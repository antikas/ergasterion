"""DuckDB ``ProjectionPublisher``: gap-ordered publication and rebuildable read models.

The target cursor is partitioned by ``(fully_qualified_logical_identity,
projection_target)``. Exact replay of the current revision is idempotent; gaps
and older mismatches fail. One DuckDB transaction updates the cursor, stream
status, published ledger, visibility ancestry, version registry and active
alias together. Projection relations never duplicate raw payload bytes.
"""

from __future__ import annotations

from pathlib import Path

from ergasterion.framework.bronze_contract import (
    ProcessingOutcome,
    ProjectionIntentKind,
    SnapshotReconciliationStatus,
    TimelinessState,
)
from ergasterion.framework.runtime_binding import ProjectionCursor
from ergasterion.ingestion.duckdb_bronze import (
    PROJECTION_RELATIONS,
    DuckDBStore,
    dumps,
    identity_key,
    loads,
)
from ergasterion.ingestion.records import (
    LogicalIdentity,
    ProjectionConfirmation,
    ProjectionIntent,
    ProjectionReplayBatch,
    PublishedLedgerRow,
    StreamStatus,
    Token,
    VersionInterface,
    VisibilityAncestryRow,
    VisibilityIdentity,
)
from ergasterion.ingestion.runtime import PortError, canonical_digest

NOW = "2026-01-01T00:00:00.000000Z"


def _kind(value) -> str:
    return value.value if hasattr(value, "value") else str(value)


class DuckDBProjectionPublisher:
    """``ProjectionPublisherPort`` over the shared DuckDB Bronze file."""

    def __init__(
        self,
        store: DuckDBStore | str | Path,
        *,
        fail_first_n: int = 0,
        now: str = NOW,
    ) -> None:
        self.store = store if isinstance(store, DuckDBStore) else DuckDBStore(store)
        self.fail_first_n = fail_first_n
        self._calls = 0
        self.now = now

    def close(self) -> None:
        self.store.close()

    def apply_gap_ordered(self, intent: ProjectionIntent) -> ProjectionConfirmation:
        self.store.require_available()
        if intent.projection_intent_digest in self._confirmation_cache(intent):
            return self._confirmation_cache(intent)[intent.projection_intent_digest]
        if self._calls < self.fail_first_n:
            self._calls += 1
            raise PortError("target_unavailable", intent.projection_intent_digest)
        self._calls += 1
        identity = identity_key(intent.logical_identity)
        cursor = self.store.fetchone(
            """SELECT projection_revision, projection_intent_digest FROM projection_cursors
               WHERE identity_key = ? AND projection_target = ?""",
            [identity, intent.projection_target],
        )
        current = int(cursor["projection_revision"]) if cursor else 0
        current_digest = cursor["projection_intent_digest"] if cursor else None
        wanted = int(intent.projection_revision)
        if wanted == current and current_digest == intent.projection_intent_digest:
            stored = self.store.fetchone(
                """SELECT confirmation_json FROM projection_applied
                   WHERE identity_key = ? AND projection_target = ? AND projection_revision = ?""",
                [identity, intent.projection_target, wanted],
            )
            if stored is not None:
                return loads(ProjectionConfirmation, stored["confirmation_json"])
        if wanted != current + 1:
            if wanted <= current:
                raise PortError("projection_conflict", f"stale revision {wanted}, current is {current}")
            raise PortError("projection_gap", f"expected {current + 1}, got {wanted}")
        if cursor is not None and current > 0 and current_digest and wanted == current and current_digest != intent.projection_intent_digest:
            raise PortError("projection_conflict", intent.projection_revision)
        confirmation = self._confirmation_for(intent)
        self.store.begin()
        try:
            self._write_cursor(identity, intent, confirmation)
            self._apply_payload(identity, intent, confirmation)
            self.store.execute(
                """INSERT INTO projection_applied(
                       identity_key, projection_target, projection_revision, projection_intent_digest,
                       confirmation_json, intent_json
                   ) VALUES (?, ?, ?, ?, ?, ?)""",
                [
                    identity, intent.projection_target, wanted, intent.projection_intent_digest,
                    dumps(confirmation), dumps(intent),
                ],
            )
            self.store.commit()
        except PortError:
            self.store.rollback()
            raise
        except Exception:
            self.store.rollback()
            raise
        return confirmation

    def read_cursor(self, logical_identity: LogicalIdentity, projection_target: Token) -> ProjectionCursor:
        identity = identity_key(logical_identity)
        row = self.store.fetchone(
            """SELECT projection_revision, projection_intent_digest FROM projection_cursors
               WHERE identity_key = ? AND projection_target = ?""",
            [identity, projection_target],
        )
        if row is None:
            return ProjectionCursor(
                logical_identity=logical_identity, projection_target=projection_target,
                projection_revision="0", projection_intent_digest=None,
            )
        digest = row["projection_intent_digest"]
        revision = str(int(row["projection_revision"]))
        return ProjectionCursor(
            logical_identity=logical_identity, projection_target=projection_target,
            projection_revision=revision, projection_intent_digest=digest if int(revision) else None,
        )

    def rebuild_read_models(self, batch: ProjectionReplayBatch) -> ProjectionCursor:
        self.store.require_available()
        if len(batch.intents) != len(batch.confirmations):
            raise PortError("unconfirmed_revision", "intents and confirmations count mismatch")
        if not self.store._bronze_tables_ok() or self.store._lost:
            raise PortError(
                "bronze_store_restore_required",
                "bronze partitions must remain while projection relations rebuild",
            )
        self.store._ensure_schema()
        if not batch.intents:
            raise PortError("unconfirmed_revision", "rebuild requires at least one confirmed intent")
        identity_model = batch.intents[0].logical_identity
        target = batch.intents[0].projection_target
        identity = identity_key(identity_model)
        scoped = (
            "projection_cursors", "projection_applied", "stream_status", "published_ledger",
            "visibility_ancestry", "version_registry", "active_alias", "snapshot_history",
        )
        self.store.begin()
        try:
            for name in scoped:
                self.store.execute(f"DELETE FROM {name} WHERE identity_key = ?", [identity])
            self.store.commit()
        except Exception:
            self.store.rollback()
            raise
        last = None
        for intent, confirmation in zip(batch.intents, batch.confirmations):
            if intent.projection_intent_digest != confirmation.projection_intent_digest:
                raise PortError("unconfirmed_revision", intent.projection_revision)
            if dumps(intent.logical_identity) != dumps(identity_model) or intent.projection_target != target:
                raise PortError("projection_conflict", intent.projection_intent_digest)
            last = self._replay_one(intent, confirmation)
        self.store.rebuild_quarantine_projection()
        return last

    def ledger_rows(self, logical_identity: LogicalIdentity, projection_target: Token) -> tuple[PublishedLedgerRow, ...]:
        identity = identity_key(logical_identity)
        rows = self.store.fetchall(
            """SELECT json FROM published_ledger
               WHERE identity_key = ? AND projection_target = ?
               ORDER BY visibility_epoch, visibility_kind, visibility_id""",
            [identity, projection_target],
        )
        return tuple(loads(PublishedLedgerRow, row["json"]) for row in rows)

    def ancestry_rows(self, logical_identity: LogicalIdentity, projection_target: Token) -> tuple[VisibilityAncestryRow, ...]:
        identity = identity_key(logical_identity)
        rows = self.store.fetchall(
            """SELECT descendant_epoch, ancestor_epoch, projection_revision FROM visibility_ancestry
               WHERE identity_key = ? AND projection_target = ?
               ORDER BY descendant_epoch, ancestor_epoch""",
            [identity, projection_target],
        )
        return tuple(
            VisibilityAncestryRow(
                logical_identity=logical_identity, descendant_epoch=row["descendant_epoch"],
                ancestor_epoch=row["ancestor_epoch"], projection_target=projection_target,
                projection_revision=str(int(row["projection_revision"])),
            )
            for row in rows
        )

    def version_interfaces(self, logical_identity: LogicalIdentity, projection_target: Token) -> tuple[VersionInterface, ...]:
        identity = identity_key(logical_identity)
        rows = self.store.fetchall(
            """SELECT json FROM version_registry
               WHERE identity_key = ? AND projection_target = ?
               ORDER BY product_version, contract_digest""",
            [identity, projection_target],
        )
        return tuple(loads(VersionInterface, row["json"]) for row in rows)

    def active_alias(self, logical_identity: LogicalIdentity, projection_target: Token) -> str | None:
        identity = identity_key(logical_identity)
        row = self.store.fetchone(
            "SELECT relation_ref FROM active_alias WHERE identity_key = ? AND projection_target = ?",
            [identity, projection_target],
        )
        return row["relation_ref"] if row else None

    def published_visibility_set(self, logical_identity: LogicalIdentity, projection_target: Token) -> set[tuple[str, str, str]]:
        return {
            (row.visibility.epoch, row.visibility.kind, row.visibility.id)
            for row in self.ledger_rows(logical_identity, projection_target)
        }

    def drop_projection_relations(self) -> None:
        self.store.drop_relations(PROJECTION_RELATIONS)

    def _confirmation_cache(self, intent: ProjectionIntent) -> dict[str, ProjectionConfirmation]:
        identity = identity_key(intent.logical_identity)
        rows = self.store.fetchall(
            """SELECT projection_intent_digest, confirmation_json FROM projection_applied
               WHERE identity_key = ? AND projection_target = ?""",
            [identity, intent.projection_target],
        )
        return {row["projection_intent_digest"]: loads(ProjectionConfirmation, row["confirmation_json"]) for row in rows}

    def _confirmation_for(self, intent: ProjectionIntent) -> ProjectionConfirmation:
        visibility = getattr(intent.payload, "visibility", None)
        kind = _kind(intent.kind)
        committed_at = None
        release_applied_at = None
        if kind in {ProjectionIntentKind.DELIVERY_PUBLICATION.value, ProjectionIntentKind.WHOLE_DELIVERY_REPROCESSING.value}:
            prior = getattr(intent.payload, "prior_committed_at", None)
            committed_at = prior or self.now
        if kind == ProjectionIntentKind.REMEDIATION_RELEASE.value:
            release_applied_at = self.now
            committed_at = getattr(intent.payload, "prior_committed_at", None)
        if kind in {
            ProjectionIntentKind.MIGRATION.value, ProjectionIntentKind.PROCESSING.value,
            ProjectionIntentKind.TIMELINESS.value, ProjectionIntentKind.HEARTBEAT.value,
        }:
            committed_at = getattr(intent.payload, "prior_committed_at", None)
        processing = ProcessingOutcome.COMMITTED
        if kind == ProjectionIntentKind.PROCESSING.value:
            processing = intent.payload.processing
        ledger_ref = None
        if visibility is not None:
            ledger_ref = f"ledger:{visibility.epoch}:{visibility.kind}:{visibility.id}"
        deletion = None
        intent_deletion = getattr(intent.payload, "deletion_evidence", None)
        if intent_deletion is not None:
            from ergasterion.ingestion.records import DeletionEvidence
            deletion = DeletionEvidence(
                intent=intent_deletion, applied_at=self.now,
                deletion_evidence_digest=canonical_digest({
                    "schema": "ergasterion.deletion-evidence/v1",
                    "intent": intent_deletion.model_dump(mode="json", by_alias=True),
                    "applied_at": self.now,
                }),
            )
        return ProjectionConfirmation(
            schema="ergasterion.projection-confirmation/v1", logical_identity=intent.logical_identity,
            contract_digest=intent.contract_digest, projection_target=intent.projection_target, kind=intent.kind,
            projection_intent_digest=intent.projection_intent_digest, projection_revision=intent.projection_revision,
            target_applied_at=self.now, committed_at=committed_at, release_applied_at=release_applied_at,
            timeliness=getattr(intent.payload, "timeliness", None), processing=processing, visibility=visibility,
            ledger_ref=ledger_ref, deletion_evidence=deletion,
            target_result_digest=canonical_digest({"intent": intent.projection_intent_digest}),
        )

    def _write_cursor(self, identity: str, intent: ProjectionIntent, confirmation: ProjectionConfirmation) -> None:
        self.store.execute(
            """INSERT INTO projection_cursors(identity_key, projection_target, projection_revision, projection_intent_digest)
               VALUES (?, ?, ?, ?)
               ON CONFLICT (identity_key, projection_target) DO UPDATE SET
                 projection_revision = excluded.projection_revision,
                 projection_intent_digest = excluded.projection_intent_digest""",
            [identity, intent.projection_target, int(intent.projection_revision), intent.projection_intent_digest],
        )

    def _apply_payload(self, identity: str, intent: ProjectionIntent, confirmation: ProjectionConfirmation) -> None:
        kind = _kind(intent.kind)
        if kind in {
            ProjectionIntentKind.DELIVERY_PUBLICATION.value,
            ProjectionIntentKind.WHOLE_DELIVERY_REPROCESSING.value,
            ProjectionIntentKind.REMEDIATION_RELEASE.value,
        }:
            self._apply_publication(identity, intent, confirmation)
        elif kind == ProjectionIntentKind.MIGRATION.value:
            self._apply_migration(identity, intent, confirmation)
        self._upsert_stream(identity, intent, confirmation)

    def _apply_publication(self, identity: str, intent: ProjectionIntent, confirmation: ProjectionConfirmation) -> None:
        payload = intent.payload
        visibility: VisibilityIdentity = payload.visibility
        prior = self.store.fetchone(
            """SELECT payload_digest, claim_digest FROM published_ledger
               WHERE identity_key = ? AND visibility_epoch = ? AND visibility_kind = ?
                 AND visibility_id = ? AND projection_target = ?""",
            [identity, visibility.epoch, visibility.kind, visibility.id, intent.projection_target],
        )
        row = PublishedLedgerRow(
            logical_identity=intent.logical_identity, visibility=visibility,
            projection_target=intent.projection_target, product_version=payload.product_version,
            contract_digest=payload.contract_digest, source_schema_digest=payload.source_schema_digest,
            published_schema_digest=payload.published_schema_digest,
            delivery_claim_digest=getattr(payload, "delivery_claim_digest", getattr(payload, "original_delivery_claim_digest", "")),
            transport_payload_digest=payload.transport_payload_digest,
            raw_receipt_ref=payload.raw_receipt_ref, raw_receipt_digest=payload.raw_receipt_digest,
            bronze_partition_ref=payload.bronze_partition_ref, accepted_content_digest=payload.accepted_content_digest,
            ruleset_digest=payload.ruleset_digest, validation_result_digest=payload.validation_result_digest,
            accepted_count=payload.accepted_count, progress_claim=payload.progress_claim,
            execution_plan_digest=intent.execution_plan_digest, runtime_manifest_digest=intent.runtime_manifest_digest,
            committed_at=confirmation.committed_at or self.now,
            release_applied_at=confirmation.release_applied_at,
            projection_revision=intent.projection_revision,
        )
        payload_digest = canonical_digest(row.model_dump(mode="json", by_alias=True))
        claim_digest = row.delivery_claim_digest
        if prior is not None:
            if prior["payload_digest"] != payload_digest or prior["claim_digest"] != claim_digest:
                raise PortError("projection_conflict", visibility.id)
            return
        self.store.execute(
            """INSERT INTO published_ledger(
                   identity_key, visibility_epoch, visibility_kind, visibility_id, projection_target,
                   payload_digest, claim_digest, json
               ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            [
                identity, visibility.epoch, visibility.kind, visibility.id, intent.projection_target,
                payload_digest, claim_digest, dumps(row),
            ],
        )
        if confirmation.deletion_evidence is not None or _kind(intent.kind) == ProjectionIntentKind.DELIVERY_PUBLICATION.value:
            self._advance_snapshot_pointer(identity, intent, visibility)

    def _advance_snapshot_pointer(self, identity: str, intent: ProjectionIntent, visibility: VisibilityIdentity) -> None:
        current = self.store.fetchone(
            """SELECT json FROM stream_status
               WHERE identity_key = ? AND contract_digest = ? AND projection_target = ?""",
            [identity, intent.contract_digest, intent.projection_target],
        )
        if current is not None:
            status = json_status(current["json"])
            prior_vis = status.get("latest_snapshot_visibility")
            if prior_vis is not None:
                self.store.execute(
                    """INSERT INTO snapshot_history(
                           identity_key, projection_target, visibility_epoch, visibility_kind, visibility_id,
                           projection_revision, json
                       ) VALUES (?, ?, ?, ?, ?, ?, ?)
                       ON CONFLICT (identity_key, projection_target, visibility_epoch, visibility_kind, visibility_id)
                       DO NOTHING""",
                    [
                        identity, intent.projection_target, prior_vis["epoch"], prior_vis["kind"], prior_vis["id"],
                        int(intent.projection_revision), dumps(prior_vis),
                    ],
                )

    def _apply_migration(self, identity: str, intent: ProjectionIntent, confirmation: ProjectionConfirmation) -> None:
        payload = intent.payload
        version: VersionInterface = payload.version_interface
        ancestry: tuple[VisibilityAncestryRow, ...] = payload.ancestry
        carry = any(row.descendant_epoch != row.ancestor_epoch for row in ancestry)
        for row in ancestry:
            self.store.execute(
                """INSERT INTO visibility_ancestry(
                       identity_key, descendant_epoch, ancestor_epoch, projection_target, projection_revision
                   ) VALUES (?, ?, ?, ?, ?)
                   ON CONFLICT (identity_key, descendant_epoch, ancestor_epoch, projection_target)
                   DO UPDATE SET projection_revision = excluded.projection_revision""",
                [
                    identity, row.descendant_epoch, row.ancestor_epoch, intent.projection_target,
                    int(intent.projection_revision),
                ],
            )
        retired = self.store.fetchall(
            """SELECT product_version, contract_digest, json FROM version_registry
               WHERE identity_key = ? AND projection_target = ?""",
            [identity, intent.projection_target],
        )
        for row in retired:
            inactive = loads(VersionInterface, row["json"]).model_copy(update={"active": False})
            self.store.execute(
                """UPDATE version_registry SET active = FALSE, json = ?
                   WHERE identity_key = ? AND product_version = ? AND contract_digest = ?
                     AND projection_target = ?""",
                [
                    dumps(inactive), identity, row["product_version"], row["contract_digest"],
                    intent.projection_target,
                ],
            )
        stored = version.model_copy(update={"active": True})
        self.store.execute(
            """INSERT INTO version_registry(
                   identity_key, product_version, contract_digest, projection_target,
                   root_visibility_epoch, relation_ref, active, json
               ) VALUES (?, ?, ?, ?, ?, ?, TRUE, ?)
               ON CONFLICT (identity_key, product_version, contract_digest, projection_target)
               DO UPDATE SET active = TRUE, json = excluded.json""",
            [
                identity, version.product_version, version.contract_digest, intent.projection_target,
                version.root_visibility_epoch, version.relation_ref, dumps(stored),
            ],
        )
        if not carry:
            self.store.execute(
                """INSERT INTO active_alias(identity_key, projection_target, relation_ref, product_version, contract_digest)
                   VALUES (?, ?, ?, ?, ?)
                   ON CONFLICT (identity_key, projection_target) DO UPDATE SET
                     relation_ref = excluded.relation_ref,
                     product_version = excluded.product_version,
                     contract_digest = excluded.contract_digest""",
                [identity, intent.projection_target, version.relation_ref, version.product_version, version.contract_digest],
            )
        else:
            existing = self.store.fetchone(
                "SELECT relation_ref FROM active_alias WHERE identity_key = ? AND projection_target = ?",
                [identity, intent.projection_target],
            )
            if existing is None:
                self.store.execute(
                    """INSERT INTO active_alias(identity_key, projection_target, relation_ref, product_version, contract_digest)
                       VALUES (?, ?, ?, ?, ?)""",
                    [identity, intent.projection_target, version.relation_ref, version.product_version, version.contract_digest],
                )

    def _upsert_stream(self, identity: str, intent: ProjectionIntent, confirmation: ProjectionConfirmation) -> None:
        payload = intent.payload
        existing = self.store.fetchone(
            """SELECT json FROM stream_status
               WHERE identity_key = ? AND contract_digest = ? AND projection_target = ?""",
            [identity, intent.contract_digest, intent.projection_target],
        )
        prior = json_status(existing["json"]) if existing else {}
        visibility = getattr(payload, "visibility", None)
        snapshot_vis = prior.get("latest_snapshot_visibility")
        if confirmation.deletion_evidence is not None and visibility is not None:
            snapshot_vis = visibility.model_dump(mode="json")
        elif _kind(intent.kind) == ProjectionIntentKind.DELIVERY_PUBLICATION.value and visibility is not None:
            snapshot_vis = visibility.model_dump(mode="json")
        heartbeat_at = getattr(payload, "heartbeat_at", prior.get("heartbeat_at") or self.now)
        evaluated = getattr(payload, "evaluated_through_at", prior.get("evaluated_through_at") or self.now)
        timeliness = getattr(payload, "timeliness", None)
        processing = confirmation.processing
        status = StreamStatus(
            logical_identity=intent.logical_identity, contract_digest=intent.contract_digest,
            projection_target=intent.projection_target, projection_revision=intent.projection_revision,
            projected_at=confirmation.target_applied_at,
            scheduled_boundary_at=getattr(payload, "scheduled_boundary_at", prior.get("scheduled_boundary_at")),
            processing=processing,
            timeliness=timeliness or (
                TimelinessState(prior["timeliness"]) if prior.get("timeliness") else TimelinessState.NOT_DUE
            ),
            latest_attempt=getattr(payload, "attempt", None),
            committed_at=confirmation.committed_at,
            accepted_progress=prior.get("accepted_progress") or {},
            latest_snapshot_visibility=_visibility_from(snapshot_vis),
            snapshot_reconciliation=(
                SnapshotReconciliationStatus.COMPLETE if confirmation.deletion_evidence is not None
                else SnapshotReconciliationStatus.NOT_APPLICABLE
            ),
            heartbeat_at=heartbeat_at, evaluated_through_at=evaluated,
        )
        self.store.execute(
            """INSERT INTO stream_status(identity_key, contract_digest, projection_target, projection_revision, json)
               VALUES (?, ?, ?, ?, ?)
               ON CONFLICT (identity_key, contract_digest, projection_target) DO UPDATE SET
                 projection_revision = excluded.projection_revision,
                 json = excluded.json""",
            [identity, intent.contract_digest, intent.projection_target, int(intent.projection_revision), dumps(status)],
        )

    def _replay_one(self, intent: ProjectionIntent, confirmation: ProjectionConfirmation) -> ProjectionCursor:
        identity = identity_key(intent.logical_identity)
        self.store.begin()
        try:
            self._write_cursor(identity, intent, confirmation)
            self._apply_payload(identity, intent, confirmation)
            self.store.execute(
                """INSERT INTO projection_applied(
                       identity_key, projection_target, projection_revision, projection_intent_digest,
                       confirmation_json, intent_json
                   ) VALUES (?, ?, ?, ?, ?, ?)
                   ON CONFLICT (identity_key, projection_target, projection_revision) DO UPDATE SET
                     confirmation_json = excluded.confirmation_json,
                     intent_json = excluded.intent_json""",
                [
                    identity, intent.projection_target, int(intent.projection_revision),
                    intent.projection_intent_digest, dumps(confirmation), dumps(intent),
                ],
            )
            self.store.commit()
        except Exception:
            self.store.rollback()
            raise
        return ProjectionCursor(
            logical_identity=intent.logical_identity, projection_target=intent.projection_target,
            projection_revision=intent.projection_revision,
            projection_intent_digest=intent.projection_intent_digest,
        )


def json_status(text: str) -> dict:
    import json
    return json.loads(text)


def _visibility_from(value) -> VisibilityIdentity | None:
    if value is None:
        return None
    if hasattr(value, "kind"):
        return value
    kind = value.get("kind")
    from ergasterion.ingestion.records import DeliveryVisibilityIdentity, ReleaseVisibilityIdentity, ReprocessVisibilityIdentity
    if kind == "delivery":
        return DeliveryVisibilityIdentity.model_validate(value)
    if kind == "release":
        return ReleaseVisibilityIdentity.model_validate(value)
    if kind == "reprocess":
        return ReprocessVisibilityIdentity.model_validate(value)
    return None


__all__ = [
    "DuckDBProjectionPublisher",
    "NOW",
]
