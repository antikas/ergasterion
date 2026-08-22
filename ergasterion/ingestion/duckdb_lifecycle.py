"""DuckDB ``LifecycleSink``: idempotent event projection and typed evidence queries.

Lifecycle envelopes are stored once by immutable event identity. A reused
identifier with a different payload is ``event_conflict``; ordinals on one
stream are gap-free. The authorization-bound evidence query returns the closed
``EvidenceRecord`` union, never source-native values. Lifecycle read models
rebuild from this log; they do not own run truth.
"""

from __future__ import annotations

from pathlib import Path

from ergasterion.framework.bronze_contract import EvidenceKind, LifecycleEventType
from ergasterion.ingestion.duckdb_bronze import (
    DuckDBStore,
    cursor_token,
    dumps,
    encoded_size,
    identity_key,
    loads,
    parse_cursor_token,
)
from ergasterion.ingestion.records import (
    AttemptEvidenceItem,
    ContractEvidenceItem,
    DeletionEvidenceItem,
    EvidencePage,
    EvidenceQuery,
    LifecycleEvent,
    LifecycleEventBatch,
    LineageEvidenceItem,
    MetadataEvidenceItem,
    PublicationEvidenceItem,
    QualityEvidenceItem,
    QuarantineEvidenceItem,
    ReceiptEvidenceItem,
    SchemaEvidenceItem,
)
from ergasterion.ingestion.runtime import PortError, canonical_digest

_EVENT_TO_EVIDENCE = {
    LifecycleEventType.RECEIVED.value: EvidenceKind.ATTEMPT.value,
    LifecycleEventType.PREPARING.value: EvidenceKind.ATTEMPT.value,
    LifecycleEventType.VALIDATING.value: EvidenceKind.ATTEMPT.value,
    LifecycleEventType.MATERIALIZING.value: EvidenceKind.ATTEMPT.value,
    LifecycleEventType.COMMITTING.value: EvidenceKind.ATTEMPT.value,
    LifecycleEventType.COMMIT_BLOCKED.value: EvidenceKind.ATTEMPT.value,
    LifecycleEventType.COMMITTED.value: EvidenceKind.ATTEMPT.value,
    LifecycleEventType.FAILED.value: EvidenceKind.ATTEMPT.value,
    LifecycleEventType.BRONZE_CONTRACT.value: EvidenceKind.CONTRACT.value,
    LifecycleEventType.BRONZE_SCHEMA.value: EvidenceKind.SCHEMA.value,
    LifecycleEventType.BRONZE_RECEIPT.value: EvidenceKind.RECEIPT.value,
    LifecycleEventType.BRONZE_QUALITY.value: EvidenceKind.QUALITY.value,
    LifecycleEventType.BRONZE_QUARANTINE.value: EvidenceKind.QUARANTINE.value,
    LifecycleEventType.BRONZE_PUBLICATION.value: EvidenceKind.PUBLICATION.value,
    LifecycleEventType.BRONZE_DELETION_EVIDENCE.value: EvidenceKind.DELETION_EVIDENCE.value,
    LifecycleEventType.BRONZE_LINEAGE.value: EvidenceKind.LINEAGE.value,
    LifecycleEventType.BRONZE_METADATA.value: EvidenceKind.METADATA.value,
}


class DuckDBLifecycleSink:
    """``LifecycleSinkPort`` over the shared DuckDB Bronze file."""

    def __init__(self, store: DuckDBStore | str | Path) -> None:
        self.store = store if isinstance(store, DuckDBStore) else DuckDBStore(store)

    def close(self) -> None:
        self.store.close()

    def project_events(self, batch: LifecycleEventBatch) -> tuple[str, ...]:
        ids: list[str] = []
        self.store.begin()
        try:
            for event in batch.events:
                existing = self.store.fetchone(
                    "SELECT payload_digest FROM lifecycle_events WHERE event_id = ?",
                    [event.event_id],
                )
                if existing is not None:
                    if existing["payload_digest"] != event.payload_digest:
                        raise PortError("event_conflict", event.event_id)
                    ids.append(event.event_id)
                    continue
                stream = identity_key(event.logical_identity)
                ordinal = int(event.event_ordinal)
                previous = self.store.fetchone(
                    """SELECT MAX(event_ordinal) AS last FROM lifecycle_events WHERE identity_key = ?""",
                    [stream],
                )
                last = int(previous["last"]) if previous and previous["last"] is not None else None
                if last is not None and ordinal not in (last, last + 1):
                    raise PortError(
                        "event_conflict",
                        f"event ordinal {ordinal} does not follow {last}: a lifecycle envelope is missing",
                    )
                self.store.execute(
                    """INSERT INTO lifecycle_events(
                           event_id, identity_key, event_type, state_revision, event_ordinal, payload_digest, json
                       ) VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    [
                        event.event_id, stream,
                        event.event_type.value if hasattr(event.event_type, "value") else event.event_type,
                        int(event.state_revision), ordinal, event.payload_digest, dumps(event),
                    ],
                )
                self._project_read_model(event)
                ids.append(event.event_id)
            self.store.commit()
        except PortError:
            self.store.rollback()
            raise
        except Exception:
            self.store.rollback()
            raise
        return tuple(ids)

    def evidence_query(self, query: EvidenceQuery) -> EvidencePage:
        if not query.authorization_context_ref:
            raise PortError("access_denied", "authorization_context_ref is required")
        identity = identity_key(query.logical_identity)
        after = parse_cursor_token(query.after_cursor)
        sql = """SELECT json FROM lifecycle_events WHERE identity_key = ?"""
        params: list = [identity]
        if query.immutable_id is not None:
            sql += " AND event_id = ?"
            params.append(query.immutable_id)
        wanted = query.evidence_kind.value if hasattr(query.evidence_kind, "value") else query.evidence_kind
        sql += " ORDER BY state_revision, event_ordinal, event_id"
        rows = self.store.fetchall(sql, params)
        items = []
        bytes_returned = 0
        max_bytes = int(query.max_bytes)
        more = False
        next_cursor = None
        sequential = 0
        for row in rows:
            event = loads(LifecycleEvent, row["json"])
            evidence_kind = _EVENT_TO_EVIDENCE.get(
                event.event_type.value if hasattr(event.event_type, "value") else event.event_type,
            )
            if evidence_kind != wanted:
                continue
            if after is not None and sequential <= after:
                sequential += 1
                continue
            item = _evidence_item(event)
            if item is None:
                sequential += 1
                continue
            size = encoded_size(item)
            if size > max_bytes and not items:
                raise PortError("item_too_large", event.event_id)
            if len(items) >= query.max_items or bytes_returned + size > max_bytes:
                more = True
                break
            items.append(item)
            bytes_returned += size
            next_cursor = cursor_token(sequential)
            sequential += 1
            if query.immutable_id is not None:
                break
        if not more:
            next_cursor = None
        return EvidencePage(
            items=tuple(items), next_cursor=next_cursor, bytes_returned=str(bytes_returned), more=more,
        )

    def _project_read_model(self, event: LifecycleEvent) -> None:
        payload = event.payload
        kind = event.event_type.value if hasattr(event.event_type, "value") else event.event_type
        if kind == LifecycleEventType.BRONZE_CONTRACT.value:
            self.store.execute(
                """INSERT INTO contract_registry(identity_key, contract_digest, json)
                   VALUES (?, ?, ?)
                   ON CONFLICT (contract_digest) DO NOTHING""",
                [
                    identity_key(event.logical_identity),
                    canonical_digest(payload.contract.model_dump(mode="json", by_alias=True)),
                    dumps(payload.contract),
                ],
            )
        elif kind == LifecycleEventType.BRONZE_QUALITY.value:
            digest = payload.validation.validation_result_digest
            self.store.execute(
                """INSERT INTO quality_projection(validation_result_digest, json)
                   VALUES (?, ?) ON CONFLICT (validation_result_digest) DO NOTHING""",
                [digest, dumps(payload.validation)],
            )
        elif kind == LifecycleEventType.BRONZE_LINEAGE.value:
            self.store.execute(
                """INSERT INTO lineage_projection(lineage_digest, json)
                   VALUES (?, ?) ON CONFLICT (lineage_digest) DO NOTHING""",
                [payload.lineage.lineage_digest, dumps(payload)],
            )
        elif kind == LifecycleEventType.BRONZE_METADATA.value:
            self.store.execute(
                """INSERT INTO product_metadata_projection(identity_key, contract_digest, json)
                   VALUES (?, ?, ?)
                   ON CONFLICT (identity_key, contract_digest) DO UPDATE SET json = excluded.json""",
                [identity_key(event.logical_identity), payload.metadata.contract_digest, dumps(payload.metadata)],
            )
        elif kind == LifecycleEventType.BRONZE_DELETION_EVIDENCE.value:
            self.store.execute(
                """INSERT INTO deletion_evidence_projection(deletion_evidence_digest, json)
                   VALUES (?, ?) ON CONFLICT (deletion_evidence_digest) DO NOTHING""",
                [payload.evidence.deletion_evidence_digest, dumps(payload.evidence)],
            )
        elif kind == LifecycleEventType.BRONZE_PUBLICATION.value:
            pass


def _evidence_item(event: LifecycleEvent):
    payload = event.payload
    kind = event.event_type.value if hasattr(event.event_type, "value") else event.event_type
    if kind in _attempt_types():
        return AttemptEvidenceItem(kind="attempt", attempt=payload.attempt, confirmation=payload.projection_confirmation)
    if kind == LifecycleEventType.BRONZE_CONTRACT.value:
        return ContractEvidenceItem(kind="contract", contract=payload.contract)
    if kind == LifecycleEventType.BRONZE_SCHEMA.value:
        return SchemaEvidenceItem(kind="schema", metadata=payload.metadata)
    if kind == LifecycleEventType.BRONZE_RECEIPT.value:
        return ReceiptEvidenceItem(kind="receipt", receipt=payload.receipt)
    if kind == LifecycleEventType.BRONZE_QUALITY.value:
        return QualityEvidenceItem(kind="quality", validation=payload.validation)
    if kind == LifecycleEventType.BRONZE_QUARANTINE.value:
        return QuarantineEvidenceItem(kind="quarantine", validation=payload.validation, decision=payload.decision)
    if kind == LifecycleEventType.BRONZE_PUBLICATION.value:
        return PublicationEvidenceItem(kind="publication", ledger=payload.ledger, confirmation=payload.confirmation)
    if kind == LifecycleEventType.BRONZE_DELETION_EVIDENCE.value:
        return DeletionEvidenceItem(kind="deletion_evidence", evidence=payload.evidence)
    if kind == LifecycleEventType.BRONZE_LINEAGE.value:
        return LineageEvidenceItem(kind="lineage", lineage=payload.lineage, run_lineage=payload.run_lineage)
    if kind == LifecycleEventType.BRONZE_METADATA.value:
        return MetadataEvidenceItem(kind="metadata", metadata=payload.metadata)
    return None


def _attempt_types() -> set[str]:
    return {
        LifecycleEventType.RECEIVED.value, LifecycleEventType.PREPARING.value,
        LifecycleEventType.VALIDATING.value, LifecycleEventType.MATERIALIZING.value,
        LifecycleEventType.COMMITTING.value, LifecycleEventType.COMMIT_BLOCKED.value,
        LifecycleEventType.COMMITTED.value, LifecycleEventType.FAILED.value,
    }


__all__ = ["DuckDBLifecycleSink"]
