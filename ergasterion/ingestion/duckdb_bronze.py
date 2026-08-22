"""DuckDB ``LandingAdapter``: typed candidate partitions and the disposition index.

One DuckDB file is the durable Bronze store for candidate frames, the immutable
disposition index and accepted partitions. Projection, remediation and lifecycle
adapters bind the same file through ``DuckDBStore``. Warehouse SQL stays here
(and in the sibling DuckDB adapters); the runtime service never imports DuckDB.

Managed rows carry ``_ergasterion_delivery_id`` plus tagged visibility identity
and original delivery identity. Typed-but-rule-invalid units stay in the
candidate/disposition relations and never enter accepted storage. Raw payload
bytes are not copied into these partitions -- they remain in the raw store.
"""

from __future__ import annotations

import base64
import json
import tempfile
from pathlib import Path
from typing import Any

from ergasterion.framework.bronze_contract import (
    BronzeProductContract,
    DispositionStatus,
    Finding,
    FindingMetadata,
    RawLocator,
)
from ergasterion.ingestion.records import (
    BronzeEvidence,
    CandidateField,
    CandidateFrame,
    CandidateFramePage,
    CandidateReadQuery,
    DeliveryVisibilityIdentity,
    Digest,
    Disposition,
    DispositionPage,
    DispositionQuery,
    DispositionQueryPage,
    ExternalReceiptInput,
    LandingPreparation,
    LogicalIdentity,
    MaterializationCompletion,
    MaterializationSession,
    MaterializedBronzeEvidence,
    RawReadHandle,
    RawReadPage,
    RawReceipt,
    ReleaseMaterializationRequest,
    ReleaseVisibilityBinding,
    SourceNativeEvidenceItem,
    SourceNativePage,
    SourceNativeQuery,
    Token,
    VisibilityIdentity,
)
from ergasterion.ingestion.runtime import PortError, canonical_digest, digest_token, utc_now_string

SCHEMA_VERSION = 1

BRONZE_RELATIONS = (
    "candidate_partitions",
    "candidate_frames",
    "dispositions",
    "accepted_partitions",
    "accepted_rows",
)
PROJECTION_RELATIONS = (
    "projection_cursors",
    "projection_applied",
    "stream_status",
    "published_ledger",
    "visibility_ancestry",
    "version_registry",
    "active_alias",
    "snapshot_history",
    "quarantine_projection",
    "contract_registry",
    "source_schema_registry",
    "published_schema_registry",
    "quality_projection",
    "lineage_projection",
    "product_metadata_projection",
    "deletion_evidence_projection",
)

_KNOWN_BRONZE: dict[str, str] = {}

_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS schema_meta (
    version INTEGER NOT NULL,
    bronze_written INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS preparations (
    preparation_id VARCHAR PRIMARY KEY,
    attempt_id VARCHAR NOT NULL,
    raw_receipt_digest VARCHAR NOT NULL,
    identity_key VARCHAR NOT NULL,
    next_offset BIGINT NOT NULL,
    closed BOOLEAN NOT NULL,
    raw_bytes BLOB NOT NULL,
    contract_json VARCHAR NOT NULL,
    visibility_json VARCHAR NOT NULL,
    receipt_json VARCHAR NOT NULL
);
CREATE TABLE IF NOT EXISTS candidate_partitions (
    candidate_ref VARCHAR PRIMARY KEY,
    identity_key VARCHAR NOT NULL,
    attempt_id VARCHAR NOT NULL,
    preparation_id VARCHAR NOT NULL,
    candidate_digest VARCHAR NOT NULL,
    frame_index_ref VARCHAR NOT NULL,
    frame_index_digest VARCHAR NOT NULL,
    visibility_json VARCHAR NOT NULL,
    evidence_json VARCHAR NOT NULL,
    delivery_id VARCHAR NOT NULL,
    visibility_epoch VARCHAR NOT NULL,
    visibility_kind VARCHAR NOT NULL,
    visibility_id VARCHAR NOT NULL
);
CREATE TABLE IF NOT EXISTS candidate_frames (
    candidate_ref VARCHAR NOT NULL,
    frame_sequence BIGINT NOT NULL,
    identity_key VARCHAR NOT NULL,
    _ergasterion_delivery_id VARCHAR NOT NULL,
    _ergasterion_visibility_epoch VARCHAR NOT NULL,
    _ergasterion_visibility_kind VARCHAR NOT NULL,
    _ergasterion_visibility_id VARCHAR NOT NULL,
    original_delivery_id VARCHAR NOT NULL,
    typed_fields_json VARCHAR,
    findings_json VARCHAR NOT NULL,
    locator_json VARCHAR NOT NULL,
    frame_json VARCHAR NOT NULL,
    PRIMARY KEY (candidate_ref, frame_sequence)
);
CREATE TABLE IF NOT EXISTS materialization_sessions (
    session_id VARCHAR PRIMARY KEY,
    attempt_id VARCHAR NOT NULL,
    evaluation_id VARCHAR NOT NULL,
    ruleset_digest VARCHAR NOT NULL,
    evidence_json VARCHAR NOT NULL,
    next_frame_sequence BIGINT NOT NULL,
    closed BOOLEAN NOT NULL,
    identity_key VARCHAR NOT NULL
);
CREATE TABLE IF NOT EXISTS dispositions (
    disposition_id VARCHAR PRIMARY KEY,
    session_id VARCHAR NOT NULL,
    identity_key VARCHAR NOT NULL,
    candidate_ref VARCHAR NOT NULL,
    frame_sequence BIGINT NOT NULL,
    status VARCHAR NOT NULL,
    json VARCHAR NOT NULL
);
CREATE TABLE IF NOT EXISTS accepted_partitions (
    accepted_ref VARCHAR PRIMARY KEY,
    identity_key VARCHAR NOT NULL,
    session_id VARCHAR NOT NULL,
    candidate_ref VARCHAR NOT NULL,
    disposition_ref VARCHAR NOT NULL,
    accepted_content_digest VARCHAR NOT NULL,
    published_visibility_json VARCHAR,
    evidence_json VARCHAR NOT NULL,
    provisional BOOLEAN NOT NULL
);
CREATE TABLE IF NOT EXISTS accepted_rows (
    accepted_ref VARCHAR NOT NULL,
    frame_sequence BIGINT NOT NULL,
    identity_key VARCHAR NOT NULL,
    _ergasterion_delivery_id VARCHAR NOT NULL,
    _ergasterion_visibility_epoch VARCHAR NOT NULL,
    _ergasterion_visibility_kind VARCHAR NOT NULL,
    _ergasterion_visibility_id VARCHAR NOT NULL,
    original_delivery_id VARCHAR NOT NULL,
    typed_fields_json VARCHAR NOT NULL,
    locator_json VARCHAR NOT NULL,
    disposition_id VARCHAR NOT NULL,
    PRIMARY KEY (accepted_ref, frame_sequence)
);
CREATE TABLE IF NOT EXISTS query_snapshots (
    snapshot_token VARCHAR PRIMARY KEY,
    kind VARCHAR NOT NULL,
    query_digest VARCHAR NOT NULL,
    identity_key VARCHAR NOT NULL,
    disposition_id VARCHAR,
    authorization_context_ref VARCHAR NOT NULL,
    high_water BIGINT NOT NULL
);
CREATE TABLE IF NOT EXISTS remediation_decisions (
    decision_id VARCHAR PRIMARY KEY,
    identity_key VARCHAR NOT NULL,
    kind VARCHAR NOT NULL,
    evaluation_id VARCHAR NOT NULL,
    decided_at VARCHAR NOT NULL,
    json VARCHAR NOT NULL
);
CREATE TABLE IF NOT EXISTS remediation_decision_dispositions (
    decision_id VARCHAR NOT NULL,
    disposition_id VARCHAR NOT NULL,
    PRIMARY KEY (decision_id, disposition_id)
);
CREATE TABLE IF NOT EXISTS release_claims (
    root_visibility_epoch VARCHAR NOT NULL,
    original_claim_digest VARCHAR NOT NULL,
    locator_key VARCHAR NOT NULL,
    decision_id VARCHAR NOT NULL,
    decision_digest VARCHAR NOT NULL,
    PRIMARY KEY (root_visibility_epoch, original_claim_digest, locator_key)
);
CREATE TABLE IF NOT EXISTS projection_cursors (
    identity_key VARCHAR NOT NULL,
    projection_target VARCHAR NOT NULL,
    projection_revision BIGINT NOT NULL,
    projection_intent_digest VARCHAR,
    PRIMARY KEY (identity_key, projection_target)
);
CREATE TABLE IF NOT EXISTS projection_applied (
    identity_key VARCHAR NOT NULL,
    projection_target VARCHAR NOT NULL,
    projection_revision BIGINT NOT NULL,
    projection_intent_digest VARCHAR NOT NULL,
    confirmation_json VARCHAR NOT NULL,
    intent_json VARCHAR NOT NULL,
    PRIMARY KEY (identity_key, projection_target, projection_revision)
);
CREATE TABLE IF NOT EXISTS stream_status (
    identity_key VARCHAR NOT NULL,
    contract_digest VARCHAR NOT NULL,
    projection_target VARCHAR NOT NULL,
    projection_revision BIGINT NOT NULL,
    json VARCHAR NOT NULL,
    PRIMARY KEY (identity_key, contract_digest, projection_target)
);
CREATE TABLE IF NOT EXISTS published_ledger (
    identity_key VARCHAR NOT NULL,
    visibility_epoch VARCHAR NOT NULL,
    visibility_kind VARCHAR NOT NULL,
    visibility_id VARCHAR NOT NULL,
    projection_target VARCHAR NOT NULL,
    payload_digest VARCHAR NOT NULL,
    claim_digest VARCHAR NOT NULL,
    json VARCHAR NOT NULL,
    PRIMARY KEY (identity_key, visibility_epoch, visibility_kind, visibility_id, projection_target)
);
CREATE TABLE IF NOT EXISTS visibility_ancestry (
    identity_key VARCHAR NOT NULL,
    descendant_epoch VARCHAR NOT NULL,
    ancestor_epoch VARCHAR NOT NULL,
    projection_target VARCHAR NOT NULL,
    projection_revision BIGINT NOT NULL,
    PRIMARY KEY (identity_key, descendant_epoch, ancestor_epoch, projection_target)
);
CREATE TABLE IF NOT EXISTS version_registry (
    identity_key VARCHAR NOT NULL,
    product_version VARCHAR NOT NULL,
    contract_digest VARCHAR NOT NULL,
    projection_target VARCHAR NOT NULL,
    root_visibility_epoch VARCHAR NOT NULL,
    relation_ref VARCHAR NOT NULL,
    active BOOLEAN NOT NULL,
    json VARCHAR NOT NULL,
    PRIMARY KEY (identity_key, product_version, contract_digest, projection_target)
);
CREATE TABLE IF NOT EXISTS active_alias (
    identity_key VARCHAR NOT NULL,
    projection_target VARCHAR NOT NULL,
    relation_ref VARCHAR NOT NULL,
    product_version VARCHAR NOT NULL,
    contract_digest VARCHAR NOT NULL,
    PRIMARY KEY (identity_key, projection_target)
);
CREATE TABLE IF NOT EXISTS snapshot_history (
    identity_key VARCHAR NOT NULL,
    projection_target VARCHAR NOT NULL,
    visibility_epoch VARCHAR NOT NULL,
    visibility_kind VARCHAR NOT NULL,
    visibility_id VARCHAR NOT NULL,
    projection_revision BIGINT NOT NULL,
    json VARCHAR NOT NULL,
    PRIMARY KEY (identity_key, projection_target, visibility_epoch, visibility_kind, visibility_id)
);
CREATE TABLE IF NOT EXISTS quarantine_projection (
    identity_key VARCHAR NOT NULL,
    disposition_id VARCHAR PRIMARY KEY,
    json VARCHAR NOT NULL
);
CREATE TABLE IF NOT EXISTS contract_registry (
    identity_key VARCHAR NOT NULL,
    contract_digest VARCHAR PRIMARY KEY,
    json VARCHAR NOT NULL
);
CREATE TABLE IF NOT EXISTS source_schema_registry (
    source_schema_digest VARCHAR PRIMARY KEY,
    json VARCHAR NOT NULL
);
CREATE TABLE IF NOT EXISTS published_schema_registry (
    published_schema_digest VARCHAR PRIMARY KEY,
    json VARCHAR NOT NULL
);
CREATE TABLE IF NOT EXISTS quality_projection (
    validation_result_digest VARCHAR PRIMARY KEY,
    json VARCHAR NOT NULL
);
CREATE TABLE IF NOT EXISTS lineage_projection (
    lineage_digest VARCHAR PRIMARY KEY,
    json VARCHAR NOT NULL
);
CREATE TABLE IF NOT EXISTS product_metadata_projection (
    identity_key VARCHAR NOT NULL,
    contract_digest VARCHAR NOT NULL,
    json VARCHAR NOT NULL,
    PRIMARY KEY (identity_key, contract_digest)
);
CREATE TABLE IF NOT EXISTS deletion_evidence_projection (
    deletion_evidence_digest VARCHAR PRIMARY KEY,
    json VARCHAR NOT NULL
);
CREATE TABLE IF NOT EXISTS lifecycle_events (
    event_id VARCHAR PRIMARY KEY,
    identity_key VARCHAR NOT NULL,
    event_type VARCHAR NOT NULL,
    state_revision BIGINT NOT NULL,
    event_ordinal BIGINT NOT NULL,
    payload_digest VARCHAR NOT NULL,
    json VARCHAR NOT NULL
);
"""


def dumps(value: object) -> str:
    return json.dumps(_json_payload(value), sort_keys=True, separators=(",", ":"))


def _json_payload(value: object) -> object:
    if hasattr(value, "model_dump"):
        payload = value.model_dump(mode="json", by_alias=True)
        _strip_omittable_nulls(value, payload)
        return payload
    if isinstance(value, dict):
        return value
    if isinstance(value, (list, tuple)):
        return [_json_payload(item) for item in value]
    raise TypeError(f"cannot dump {type(value)!r}")


def _strip_omittable_nulls(model: object, data: object) -> object:
    """Drop JSON nulls for IDL omittable-not-nullable fields, including nested records."""

    if not isinstance(data, dict):
        return data
    omit = getattr(model, "_omittable_not_nullable", frozenset())
    for name in omit:
        wire = "schema" if name == "schema_" else name
        if data.get(wire) is None:
            data.pop(wire, None)
    fields = getattr(model, "model_fields", None)
    if not fields:
        return data
    for field_name in fields:
        wire = "schema" if field_name == "schema_" else field_name
        if wire not in data:
            continue
        nested = getattr(model, field_name, None)
        if hasattr(nested, "model_fields") and isinstance(data[wire], dict):
            _strip_omittable_nulls(nested, data[wire])
        elif isinstance(nested, (list, tuple)) and isinstance(data[wire], list):
            for item, dumped in zip(nested, data[wire]):
                if hasattr(item, "model_fields") and isinstance(dumped, dict):
                    _strip_omittable_nulls(item, dumped)
    return data


def loads(cls, text: str):
    return cls.model_validate(json.loads(text))


def identity_key(identity: LogicalIdentity) -> str:
    return dumps(identity)


def encoded_size(value: object) -> int:
    return len(dumps(value).encode("utf-8"))


def b64url_decode(text: str) -> bytes:
    padding = "=" * (-len(text) % 4)
    return base64.urlsafe_b64decode(text + padding)


def query_digest(
    *,
    schema: str,
    identity: LogicalIdentity,
    disposition_id: Digest | None,
    authorization_context_ref: str,
) -> Digest:
    return canonical_digest({
        "schema": schema,
        "logical_identity": identity.model_dump(mode="json", by_alias=True),
        "disposition_id": disposition_id,
        "authorization_context_ref": authorization_context_ref,
    })


def snapshot_token_for(digest: Digest) -> Token:
    return digest_token(digest, "s")


def cursor_token(seq: int) -> Token:
    return f"c-{seq}"


def parse_cursor_token(token: Token | None) -> int | None:
    if token is None:
        return None
    if not token.startswith("c-"):
        raise PortError("not_found", token)
    try:
        return int(token[2:])
    except ValueError as exc:
        raise PortError("not_found", token) from exc


def _connect(path: Path):
    try:
        import duckdb
    except ImportError as exc:
        raise PortError("missing_extra", "duckdb is required for the local Bronze adapters") from exc
    return duckdb.connect(str(path))


def _table_names(conn) -> set[str]:
    rows = conn.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
    ).fetchall()
    return {str(row[0]).lower() for row in rows}


def _reject_finding() -> Finding:
    return Finding(
        kind="rule", field_path="/key", code="row_attribution_error", severity="error",
        metadata=FindingMetadata(
            diagnostic_code="null_not_allowed", raw_locator=None, expected_logical_type=None,
            observed_logical_type=None, observed_count=None, expected_min_count=None,
            expected_max_count=None, duplicate_group_size=None,
        ),
    )


class DuckDBStore:
    """One DuckDB file plus the closed Bronze/projection/lifecycle schema."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        resolved = str(self.path.resolve())
        existed = self.path.is_file()
        known = _KNOWN_BRONZE.get(resolved)
        try:
            self.conn = _connect(self.path)
        except Exception as exc:
            raise PortError("bronze_store_restore_required", f"duckdb file is unreadable: {exc}") from exc
        self._ensure_schema()
        self._lost = False
        if not self._bronze_tables_ok():
            self._lost = True
        elif known and not existed:
            self._lost = True
        elif known and not self.bronze_written:
            self._lost = True
        elif self.bronze_written and self._bronze_row_count() == 0:
            self._lost = True

    @property
    def bronze_written(self) -> bool:
        row = self.conn.execute("SELECT bronze_written FROM schema_meta").fetchone()
        return bool(row and int(row[0]))

    def _ensure_schema(self) -> None:
        self.conn.execute("BEGIN TRANSACTION")
        try:
            self.conn.execute(_SCHEMA_SQL)
            row = self.conn.execute("SELECT version FROM schema_meta").fetchone()
            if row is None:
                self.conn.execute(
                    "INSERT INTO schema_meta(version, bronze_written) VALUES (?, 0)",
                    [SCHEMA_VERSION],
                )
            else:
                current = int(row[0])
                if current > SCHEMA_VERSION:
                    raise PortError(
                        "integrity_error",
                        f"duckdb schema version {current} is ahead of {SCHEMA_VERSION}",
                    )
            self.conn.execute("COMMIT")
        except Exception:
            self.conn.execute("ROLLBACK")
            raise

    def _bronze_tables_ok(self) -> bool:
        names = _table_names(self.conn)
        return all(name in names for name in BRONZE_RELATIONS)

    def _bronze_row_count(self) -> int:
        if not self._bronze_tables_ok():
            return 0
        total = 0
        for relation in ("candidate_partitions", "dispositions", "accepted_partitions"):
            row = self.conn.execute(f"SELECT COUNT(*) FROM {relation}").fetchone()
            total += int(row[0]) if row else 0
        return total

    def mark_bronze_written(self) -> None:
        self.conn.execute("UPDATE schema_meta SET bronze_written = 1")
        _KNOWN_BRONZE[str(self.path.resolve())] = "1"

    def require_available(self) -> None:
        if self._lost or not self._bronze_tables_ok():
            raise PortError(
                "bronze_store_restore_required",
                "bronze partitions or the DuckDB file must be restored from backup",
            )
        if self.bronze_written and self._bronze_row_count() == 0:
            raise PortError(
                "bronze_store_restore_required",
                "bronze partitions were emptied and must be restored from backup",
            )

    def require_bronze_lookup(self) -> None:
        self.require_available()
        if self._lost:
            raise PortError(
                "bronze_store_restore_required",
                "bronze partitions or the DuckDB file must be restored from backup",
            )

    def checkpoint(self) -> None:
        self.conn.execute("CHECKPOINT")

    def close(self) -> None:
        try:
            self.checkpoint()
            self.conn.close()
        except Exception:
            pass

    def begin(self) -> None:
        self.conn.execute("BEGIN TRANSACTION")

    def commit(self) -> None:
        self.conn.execute("COMMIT")
        self.checkpoint()

    def rollback(self) -> None:
        self.conn.execute("ROLLBACK")

    def fetchall(self, sql: str, params: list[Any] | tuple[Any, ...] | None = None) -> list[dict[str, Any]]:
        result = self.conn.execute(sql, params or [])
        description = result.description
        if not description:
            return []
        cols = [col[0] for col in description]
        return [dict(zip(cols, row)) for row in result.fetchall()]

    def fetchone(self, sql: str, params: list[Any] | tuple[Any, ...] | None = None) -> dict[str, Any] | None:
        rows = self.fetchall(sql, params)
        return rows[0] if rows else None

    def execute(self, sql: str, params: list[Any] | tuple[Any, ...] | None = None):
        return self.conn.execute(sql, params or [])

    def drop_relations(self, names: tuple[str, ...]) -> None:
        self.begin()
        try:
            for name in names:
                self.execute(f"DROP TABLE IF EXISTS {name}")
            self.commit()
        except Exception:
            self.rollback()
            raise

    def rebuild_quarantine_projection(self) -> None:
        self.require_available()
        self.begin()
        try:
            self.execute("DELETE FROM quarantine_projection")
            for row in self.fetchall("SELECT identity_key, disposition_id, json FROM dispositions"):
                self.execute(
                    "INSERT INTO quarantine_projection(identity_key, disposition_id, json) VALUES (?, ?, ?)",
                    [row["identity_key"], row["disposition_id"], row["json"]],
                )
            self.commit()
        except Exception:
            self.rollback()
            raise

    def resolve_query_snapshot(
        self, kind: str, digest: Digest, identity: str, disposition_id: Digest | None,
        authorization_context_ref: str, snapshot_token: Token | None, high_water_sql: str,
        high_water_params: list[Any],
    ) -> Token:
        if snapshot_token is None:
            water = self.fetchone(high_water_sql, high_water_params)
            high_water = int(water["n"]) if water else 0
            token = snapshot_token_for(canonical_digest({
                "kind": kind, "query_digest": digest, "high_water": high_water,
            }))
            self.execute(
                """INSERT INTO query_snapshots(
                       snapshot_token, kind, query_digest, identity_key, disposition_id,
                       authorization_context_ref, high_water
                   ) VALUES (?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT (snapshot_token) DO NOTHING""",
                [token, kind, digest, identity, disposition_id, authorization_context_ref, high_water],
            )
            return token
        row = self.fetchone("SELECT * FROM query_snapshots WHERE snapshot_token = ?", [snapshot_token])
        if row is None:
            raise PortError("not_found", snapshot_token)
        if row["query_digest"] != digest:
            raise PortError("access_denied", "identity, filter or authorization does not match the snapshot")
        if row["identity_key"] != identity or row["authorization_context_ref"] != authorization_context_ref:
            raise PortError("access_denied", "identity, filter or authorization does not match the snapshot")
        stored_disp = row["disposition_id"]
        if (stored_disp or None) != (disposition_id or None):
            raise PortError("access_denied", "identity, filter or authorization does not match the snapshot")
        return snapshot_token


class DuckDBLandingAdapter:
    """``LandingAdapterPort`` over a real DuckDB file."""

    def __init__(
        self,
        store: DuckDBStore | str | Path,
        *,
        finish_prepare_fault: str | None = None,
        finish_materialization_fault: str | None = None,
    ) -> None:
        self.store = store if isinstance(store, DuckDBStore) else DuckDBStore(store)
        self.finish_prepare_fault = finish_prepare_fault
        self.finish_materialization_fault = finish_materialization_fault

    def close(self) -> None:
        self.store.close()

    def begin_prepare(
        self, attempt_id: Digest, receipt: RawReceipt, raw: RawReadHandle,
        contract: BronzeProductContract, visibility: VisibilityIdentity,
    ) -> LandingPreparation:
        self.store.require_available()
        preparation_id = canonical_digest({"attempt_id": attempt_id, "raw": receipt.raw_receipt_digest})
        existing = self.store.fetchone(
            "SELECT * FROM preparations WHERE preparation_id = ?", [preparation_id],
        )
        if existing is not None:
            stored_receipt = json.loads(existing["receipt_json"])
            if stored_receipt.get("raw_receipt_digest") != receipt.raw_receipt_digest:
                raise PortError("evidence_conflict", preparation_id)
            return LandingPreparation(
                preparation_id=preparation_id, attempt_id=attempt_id,
                raw_receipt_digest=receipt.raw_receipt_digest,
                next_offset=str(existing["next_offset"]), closed=bool(existing["closed"]),
            )
        identity = contract.logical_identity
        self.store.begin()
        try:
            self.store.execute(
                """INSERT INTO preparations(
                       preparation_id, attempt_id, raw_receipt_digest, identity_key, next_offset, closed,
                       raw_bytes, contract_json, visibility_json, receipt_json
                   ) VALUES (?, ?, ?, ?, 0, FALSE, ?, ?, ?, ?)""",
                [
                    preparation_id, attempt_id, receipt.raw_receipt_digest, identity_key(identity),
                    b"", dumps(contract), dumps(visibility), dumps(receipt),
                ],
            )
            self.store.commit()
        except Exception:
            self.store.rollback()
            raise
        return LandingPreparation(
            preparation_id=preparation_id, attempt_id=attempt_id,
            raw_receipt_digest=receipt.raw_receipt_digest, next_offset="0", closed=False,
        )

    def append_raw(self, preparation: LandingPreparation, page: RawReadPage) -> LandingPreparation:
        self.store.require_available()
        row = self.store.fetchone(
            "SELECT * FROM preparations WHERE preparation_id = ?", [preparation.preparation_id],
        )
        if row is None:
            raise PortError("not_found", preparation.preparation_id)
        if bool(row["closed"]):
            raise PortError("sequence_conflict", preparation.preparation_id)
        if int(page.offset) != int(row["next_offset"]):
            raise PortError("sequence_conflict", preparation.preparation_id)
        chunk = b64url_decode(page.bytes_base64url)
        raw_bytes = bytes(row["raw_bytes"]) + chunk
        next_offset = int(page.offset) + int(page.bytes_returned)
        closed = bool(page.eof)
        self.store.begin()
        try:
            self.store.execute(
                "UPDATE preparations SET raw_bytes = ?, next_offset = ?, closed = ? WHERE preparation_id = ?",
                [raw_bytes, next_offset, closed, preparation.preparation_id],
            )
            self.store.commit()
        except Exception:
            self.store.rollback()
            raise
        return LandingPreparation(
            preparation_id=preparation.preparation_id, attempt_id=preparation.attempt_id,
            raw_receipt_digest=preparation.raw_receipt_digest, next_offset=str(next_offset), closed=closed,
        )

    def finish_prepare(self, preparation: LandingPreparation) -> BronzeEvidence:
        self.store.require_available()
        if self.finish_prepare_fault is not None:
            raise PortError(self.finish_prepare_fault, "landing preparation failed permanently")
        existing = self.store.fetchone(
            "SELECT evidence_json FROM candidate_partitions WHERE preparation_id = ?",
            [preparation.preparation_id],
        )
        if existing is not None:
            return loads(BronzeEvidence, existing["evidence_json"])
        row = self.store.fetchone(
            "SELECT * FROM preparations WHERE preparation_id = ?", [preparation.preparation_id],
        )
        if row is None:
            raise PortError("not_found", preparation.preparation_id)
        receipt = loads(RawReceipt, row["receipt_json"])
        contract = BronzeProductContract.model_validate(json.loads(row["contract_json"]))
        frames = self._frames(bytes(row["raw_bytes"]), contract, receipt.payload.content_encoding)
        visibility = DeliveryVisibilityIdentity.model_validate(json.loads(row["visibility_json"]))
        candidate_digest = canonical_digest({"frames": [json.loads(dumps(frame)) for frame in frames]})
        frame_index_digest = canonical_digest({"index": preparation.preparation_id, "count": len(frames)})
        evidence = BronzeEvidence(
            raw_receipt=receipt, candidate_ref=preparation.preparation_id, candidate_digest=candidate_digest,
            frame_index_ref=preparation.preparation_id, frame_index_digest=frame_index_digest, visibility=visibility,
        )
        delivery_id = visibility.id
        self.store.begin()
        try:
            self.store.execute(
                """INSERT INTO candidate_partitions(
                       candidate_ref, identity_key, attempt_id, preparation_id, candidate_digest,
                       frame_index_ref, frame_index_digest, visibility_json, evidence_json,
                       delivery_id, visibility_epoch, visibility_kind, visibility_id
                   ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                [
                    preparation.preparation_id, row["identity_key"], row["attempt_id"],
                    preparation.preparation_id, candidate_digest, preparation.preparation_id,
                    frame_index_digest, dumps(visibility), dumps(evidence), delivery_id,
                    visibility.epoch, visibility.kind, visibility.id,
                ],
            )
            for frame in frames:
                typed_json = dumps(tuple(field.model_dump(mode="json") for field in frame.typed_fields)) if frame.typed_fields else None
                self.store.execute(
                    """INSERT INTO candidate_frames(
                           candidate_ref, frame_sequence, identity_key,
                           _ergasterion_delivery_id, _ergasterion_visibility_epoch,
                           _ergasterion_visibility_kind, _ergasterion_visibility_id,
                           original_delivery_id, typed_fields_json, findings_json, locator_json, frame_json
                       ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    [
                        preparation.preparation_id, int(frame.frame_sequence), row["identity_key"],
                        delivery_id, visibility.epoch, visibility.kind, visibility.id, delivery_id,
                        typed_json, dumps(tuple(f.model_dump(mode="json") for f in frame.structural_findings)),
                        dumps(frame.raw_locator), dumps(frame),
                    ],
                )
            self.store.mark_bronze_written()
            self.store.commit()
        except Exception:
            self.store.rollback()
            raise
        return evidence

    def read_candidate(self, query: CandidateReadQuery) -> CandidateFramePage:
        self.store.require_bronze_lookup()
        partition = self.store.fetchone(
            "SELECT * FROM candidate_partitions WHERE candidate_ref = ?",
            [query.evidence.candidate_ref],
        )
        if partition is None:
            if self.store.bronze_written or self.store._lost:
                raise PortError(
                    "bronze_store_restore_required",
                    "bronze candidate partition is missing and must be restored",
                )
            raise PortError("not_found", query.evidence.candidate_ref)
        after = int(query.after_sequence) if query.after_sequence is not None else -1
        rows = self.store.fetchall(
            """SELECT frame_json FROM candidate_frames
               WHERE candidate_ref = ? AND frame_sequence > ?
               ORDER BY frame_sequence""",
            [query.evidence.candidate_ref, after],
        )
        frames: list[CandidateFrame] = []
        bytes_returned = 0
        next_after: str | None = None
        more = False
        max_bytes = int(query.max_bytes)
        for row in rows:
            frame = loads(CandidateFrame, row["frame_json"])
            size = encoded_size(frame)
            if size > max_bytes and not frames:
                raise PortError("item_too_large", frame.frame_sequence)
            if len(frames) >= query.max_frames or bytes_returned + size > max_bytes:
                more = True
                next_after = frames[-1].frame_sequence if frames else query.after_sequence
                break
            frames.append(frame)
            bytes_returned += size
            next_after = frame.frame_sequence
        if not more:
            next_after = None
        return CandidateFramePage(
            frames=tuple(frames), next_after_sequence=next_after,
            bytes_returned=str(bytes_returned), more=more,
        )

    def begin_materialization(
        self, attempt_id: Digest, evidence: BronzeEvidence, evaluation_id: Digest, ruleset_digest: Digest,
    ) -> MaterializationSession:
        self.store.require_available()
        session_id = canonical_digest({"attempt": attempt_id, "evaluation": evaluation_id})
        existing = self.store.fetchone(
            "SELECT * FROM materialization_sessions WHERE session_id = ?", [session_id],
        )
        if existing is not None:
            stored = loads(BronzeEvidence, existing["evidence_json"])
            if stored.candidate_digest != evidence.candidate_digest:
                raise PortError("evidence_conflict", session_id)
            return MaterializationSession(
                session_id=session_id, attempt_id=attempt_id, evaluation_id=evaluation_id,
                ruleset_digest=ruleset_digest, next_frame_sequence=str(existing["next_frame_sequence"]),
                closed=bool(existing["closed"]),
            )
        partition = self.store.fetchone(
            "SELECT identity_key FROM candidate_partitions WHERE candidate_ref = ?",
            [evidence.candidate_ref],
        )
        identity = partition["identity_key"] if partition else ""
        self.store.begin()
        try:
            self.store.execute(
                """INSERT INTO materialization_sessions(
                       session_id, attempt_id, evaluation_id, ruleset_digest, evidence_json,
                       next_frame_sequence, closed, identity_key
                   ) VALUES (?, ?, ?, ?, ?, 0, FALSE, ?)""",
                [session_id, attempt_id, evaluation_id, ruleset_digest, dumps(evidence), identity],
            )
            self.store.commit()
        except Exception:
            self.store.rollback()
            raise
        return MaterializationSession(
            session_id=session_id, attempt_id=attempt_id, evaluation_id=evaluation_id,
            ruleset_digest=ruleset_digest, next_frame_sequence="0", closed=False,
        )

    def append_dispositions(self, session: MaterializationSession, page: DispositionPage) -> MaterializationSession:
        self.store.require_available()
        row = self.store.fetchone(
            "SELECT * FROM materialization_sessions WHERE session_id = ?", [session.session_id],
        )
        if row is None:
            raise PortError("not_found", session.session_id)
        if bool(row["closed"]):
            raise PortError("sequence_conflict", session.session_id)
        expected = int(row["next_frame_sequence"])
        if int(page.first_frame_sequence) != expected:
            raise PortError("sequence_conflict", session.session_id)
        evidence = loads(BronzeEvidence, row["evidence_json"])
        self.store.begin()
        try:
            for disposition in page.dispositions:
                prior = self.store.fetchone(
                    "SELECT json FROM dispositions WHERE disposition_id = ?", [disposition.disposition_id],
                )
                if prior is not None:
                    if prior["json"] != dumps(disposition):
                        raise PortError("evidence_conflict", disposition.disposition_id)
                    continue
                self.store.execute(
                    """INSERT INTO dispositions(
                           disposition_id, session_id, identity_key, candidate_ref, frame_sequence, status, json
                       ) VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    [
                        disposition.disposition_id, session.session_id, row["identity_key"],
                        evidence.candidate_ref, int(disposition.raw_locator.frame_sequence),
                        disposition.status.value if hasattr(disposition.status, "value") else disposition.status,
                        dumps(disposition),
                    ],
                )
            self.store.execute(
                "UPDATE materialization_sessions SET next_frame_sequence = ? WHERE session_id = ?",
                [int(page.next_frame_sequence), session.session_id],
            )
            self.store.mark_bronze_written()
            self.store.commit()
        except PortError:
            self.store.rollback()
            raise
        except Exception:
            self.store.rollback()
            raise
        return MaterializationSession(
            session_id=session.session_id, attempt_id=session.attempt_id, evaluation_id=session.evaluation_id,
            ruleset_digest=session.ruleset_digest, next_frame_sequence=page.next_frame_sequence, closed=False,
        )

    def finish_materialization(self, completion: MaterializationCompletion) -> MaterializedBronzeEvidence:
        self.store.require_available()
        if self.finish_materialization_fault is not None:
            raise PortError(self.finish_materialization_fault, "landing materialization failed permanently")
        session = completion.session
        row = self.store.fetchone(
            "SELECT * FROM materialization_sessions WHERE session_id = ?", [session.session_id],
        )
        if row is None:
            raise PortError("not_found", session.session_id)
        existing = self.store.fetchone(
            "SELECT evidence_json FROM accepted_partitions WHERE session_id = ?", [session.session_id],
        )
        if existing is not None:
            return loads(MaterializedBronzeEvidence, existing["evidence_json"])
        evidence = loads(BronzeEvidence, row["evidence_json"])
        dispositions = self.store.fetchall(
            "SELECT json FROM dispositions WHERE session_id = ? ORDER BY frame_sequence",
            [session.session_id],
        )
        accepted_records = []
        visibility = completion.output_visibility
        vis_epoch = visibility.epoch if visibility is not None else evidence.visibility.epoch
        vis_kind = visibility.kind if visibility is not None else evidence.visibility.kind
        vis_id = visibility.id if visibility is not None else evidence.visibility.id
        original_delivery = evidence.visibility.id
        self.store.begin()
        try:
            for item in dispositions:
                disposition = loads(Disposition, item["json"])
                if (disposition.status.value if hasattr(disposition.status, "value") else disposition.status) != DispositionStatus.ACCEPTED.value:
                    continue
                frame_row = self.store.fetchone(
                    "SELECT * FROM candidate_frames WHERE candidate_ref = ? AND frame_sequence = ?",
                    [evidence.candidate_ref, int(disposition.raw_locator.frame_sequence)],
                )
                if frame_row is None or frame_row["typed_fields_json"] is None:
                    continue
                accepted_records.append((disposition, frame_row))
            accepted_ref = f"{session.session_id}-accepted"
            records_digest = [
                {
                    "locator": json.loads(frame_row["locator_json"]),
                    "typed": json.loads(frame_row["typed_fields_json"]),
                    "disposition_id": disposition.disposition_id,
                    "outcome_digest": disposition.outcome_digest,
                }
                for disposition, frame_row in accepted_records
            ]
            accepted_content_digest = canonical_digest({
                "schema": "ergasterion.accepted-partition/v1",
                "session": session.session_id,
                "records": records_digest,
            })
            materialized = MaterializedBronzeEvidence(
                prepared=evidence, disposition_ref=session.session_id, accepted_ref=accepted_ref,
                accepted_content_digest=accepted_content_digest, candidate_keyset=completion.candidate_keyset,
                published_visibility=visibility,
            )
            self.store.execute(
                """INSERT INTO accepted_partitions(
                       accepted_ref, identity_key, session_id, candidate_ref, disposition_ref,
                       accepted_content_digest, published_visibility_json, evidence_json, provisional
                   ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                [
                    accepted_ref, row["identity_key"], session.session_id, evidence.candidate_ref,
                    session.session_id, accepted_content_digest,
                    dumps(visibility) if visibility is not None else None,
                    dumps(materialized), visibility is None,
                ],
            )
            for disposition, frame_row in accepted_records:
                self.store.execute(
                    """INSERT INTO accepted_rows(
                           accepted_ref, frame_sequence, identity_key,
                           _ergasterion_delivery_id, _ergasterion_visibility_epoch,
                           _ergasterion_visibility_kind, _ergasterion_visibility_id,
                           original_delivery_id, typed_fields_json, locator_json, disposition_id
                       ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    [
                        accepted_ref, int(frame_row["frame_sequence"]), row["identity_key"],
                        original_delivery, vis_epoch, vis_kind, vis_id, original_delivery,
                        frame_row["typed_fields_json"], frame_row["locator_json"], disposition.disposition_id,
                    ],
                )
            self.store.execute(
                "UPDATE materialization_sessions SET closed = TRUE WHERE session_id = ?",
                [session.session_id],
            )
            self.store.mark_bronze_written()
            self.store.commit()
        except Exception:
            self.store.rollback()
            raise
        self.store.rebuild_quarantine_projection()
        return materialized

    def bind_release_visibility(self, binding: ReleaseVisibilityBinding) -> MaterializedBronzeEvidence:
        self.store.require_available()
        materialized = binding.materialized
        visibility = binding.visibility
        bound_ref = f"{materialized.accepted_ref}-release-{visibility.id}"
        existing = self.store.fetchone(
            "SELECT evidence_json FROM accepted_partitions WHERE accepted_ref = ?", [bound_ref],
        )
        if existing is not None:
            bound = loads(MaterializedBronzeEvidence, existing["evidence_json"])
            if bound.accepted_content_digest != materialized.accepted_content_digest:
                raise PortError("row_attribution_error", bound_ref)
            return bound
        collision = self.store.fetchone(
            """SELECT ap.accepted_ref, ap.accepted_content_digest FROM accepted_partitions ap
               JOIN accepted_rows ar ON ar.accepted_ref = ap.accepted_ref
               WHERE ar._ergasterion_visibility_epoch = ?
                 AND ar._ergasterion_visibility_kind = ?
                 AND ar._ergasterion_visibility_id = ?
               LIMIT 1""",
            [visibility.epoch, visibility.kind, visibility.id],
        )
        if collision is not None and collision["accepted_content_digest"] != materialized.accepted_content_digest:
            raise PortError("row_attribution_error", visibility.id)
        source = self.store.fetchone(
            "SELECT * FROM accepted_partitions WHERE accepted_ref = ?", [materialized.accepted_ref],
        )
        if source is None:
            raise PortError("not_found", materialized.accepted_ref)
        if source["accepted_content_digest"] != materialized.accepted_content_digest:
            raise PortError("row_attribution_error", materialized.accepted_ref)
        bound = materialized.model_copy(update={"accepted_ref": bound_ref, "published_visibility": visibility})
        rows = self.store.fetchall(
            "SELECT * FROM accepted_rows WHERE accepted_ref = ? ORDER BY frame_sequence",
            [materialized.accepted_ref],
        )
        self.store.begin()
        try:
            self.store.execute(
                """INSERT INTO accepted_partitions(
                       accepted_ref, identity_key, session_id, candidate_ref, disposition_ref,
                       accepted_content_digest, published_visibility_json, evidence_json, provisional
                   ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, FALSE)""",
                [
                    bound_ref, source["identity_key"], source["session_id"], source["candidate_ref"],
                    source["disposition_ref"], source["accepted_content_digest"], dumps(visibility), dumps(bound),
                ],
            )
            for row in rows:
                self.store.execute(
                    """INSERT INTO accepted_rows(
                           accepted_ref, frame_sequence, identity_key,
                           _ergasterion_delivery_id, _ergasterion_visibility_epoch,
                           _ergasterion_visibility_kind, _ergasterion_visibility_id,
                           original_delivery_id, typed_fields_json, locator_json, disposition_id
                       ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    [
                        bound_ref, row["frame_sequence"], row["identity_key"],
                        row["original_delivery_id"], visibility.epoch, visibility.kind, visibility.id,
                        row["original_delivery_id"], row["typed_fields_json"], row["locator_json"],
                        row["disposition_id"],
                    ],
                )
            self.store.commit()
        except Exception:
            self.store.rollback()
            raise
        return bound

    def materialize_release(self, request: ReleaseMaterializationRequest) -> MaterializedBronzeEvidence:
        """Re-materialize a selected-locator remediation release into the
        published projection. Each frame's typed content was already produced
        once, at ``finish_prepare`` time -- a quarantine finding narrows
        disposition, not typing -- so this reads that existing typed content
        back by locator rather than re-deriving it, and inserts it into a
        release-scoped accepted partition distinct from the original
        delivery's, so it can never collide with rows already accepted there.
        Idempotent on ``request.release_id``: a replayed release returns the
        same accepted partition instead of inserting a second one."""

        self.store.require_available()
        session_id = canonical_digest({"release_materialize": request.release_id})
        accepted_ref = f"{session_id}-accepted"
        existing = self.store.fetchone(
            "SELECT evidence_json FROM accepted_partitions WHERE accepted_ref = ?", [accepted_ref],
        )
        if existing is not None:
            return loads(MaterializedBronzeEvidence, existing["evidence_json"])
        partition = self.store.fetchone(
            "SELECT identity_key, evidence_json FROM candidate_partitions WHERE candidate_ref = ?",
            [request.raw_ref],
        )
        if partition is None:
            raise PortError("not_found", request.raw_ref)
        prepared = loads(BronzeEvidence, partition["evidence_json"])
        frame_rows = []
        for locator in request.selected_locators:
            frame_row = self.store.fetchone(
                "SELECT * FROM candidate_frames WHERE candidate_ref = ? AND frame_sequence = ?",
                [request.raw_ref, int(locator.frame_sequence)],
            )
            if frame_row is None or frame_row["typed_fields_json"] is None:
                raise PortError("not_found", f"{request.raw_ref}:{locator.frame_sequence}")
            frame_rows.append(frame_row)
        records_digest = [
            {"locator": json.loads(row["locator_json"]), "typed": json.loads(row["typed_fields_json"])}
            for row in frame_rows
        ]
        accepted_content_digest = canonical_digest({
            "schema": "ergasterion.accepted-partition/v1", "session": session_id, "records": records_digest,
        })
        materialized = MaterializedBronzeEvidence(
            prepared=prepared, disposition_ref=session_id, accepted_ref=accepted_ref,
            accepted_content_digest=accepted_content_digest, candidate_keyset=None,
            published_visibility=request.visibility,
        )
        self.store.begin()
        try:
            self.store.execute(
                """INSERT INTO accepted_partitions(
                       accepted_ref, identity_key, session_id, candidate_ref, disposition_ref,
                       accepted_content_digest, published_visibility_json, evidence_json, provisional
                   ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, FALSE)""",
                [
                    accepted_ref, partition["identity_key"], session_id, request.raw_ref,
                    session_id, accepted_content_digest, dumps(request.visibility), dumps(materialized),
                ],
            )
            for locator, row in zip(request.selected_locators, frame_rows):
                disposition_id = canonical_digest({"release": request.release_id, "frame": locator.frame_sequence})
                self.store.execute(
                    """INSERT INTO accepted_rows(
                           accepted_ref, frame_sequence, identity_key,
                           _ergasterion_delivery_id, _ergasterion_visibility_epoch,
                           _ergasterion_visibility_kind, _ergasterion_visibility_id,
                           original_delivery_id, typed_fields_json, locator_json, disposition_id
                       ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    [
                        accepted_ref, int(locator.frame_sequence), partition["identity_key"],
                        row["original_delivery_id"], request.visibility.epoch, request.visibility.kind,
                        request.visibility.id, row["original_delivery_id"], row["typed_fields_json"],
                        row["locator_json"], disposition_id,
                    ],
                )
            self.store.mark_bronze_written()
            self.store.commit()
        except Exception:
            self.store.rollback()
            raise
        self.store.rebuild_quarantine_projection()
        return materialized

    def source_native_query(self, query: SourceNativeQuery) -> SourceNativePage:
        self.store.require_bronze_lookup()
        if not query.authorization_context_ref:
            raise PortError("access_denied", "authorization_context_ref is required")
        partition = self.store.fetchone(
            "SELECT identity_key FROM candidate_partitions WHERE candidate_ref = ?",
            [query.candidate_ref],
        )
        if partition is None:
            raise PortError("not_found", query.candidate_ref)
        if partition["identity_key"] != identity_key(query.logical_identity):
            raise PortError("access_denied", "logical_identity does not match the candidate partition")
        after = int(query.after_frame_sequence) if query.after_frame_sequence is not None else -1
        frames = self.store.fetchall(
            """SELECT frame_json, frame_sequence FROM candidate_frames
               WHERE candidate_ref = ? AND frame_sequence > ?
               ORDER BY frame_sequence""",
            [query.candidate_ref, after],
        )
        items: list[SourceNativeEvidenceItem] = []
        bytes_returned = 0
        max_bytes = int(query.max_bytes)
        more = False
        next_seq: str | None = None
        for row in frames:
            frame = loads(CandidateFrame, row["frame_json"])
            disposition_row = None
            if query.disposition_ref is not None:
                disposition_row = self.store.fetchone(
                    """SELECT json FROM dispositions
                       WHERE session_id = ? AND frame_sequence = ?""",
                    [query.disposition_ref, int(row["frame_sequence"])],
                )
            else:
                disposition_row = self.store.fetchone(
                    """SELECT json FROM dispositions
                       WHERE candidate_ref = ? AND frame_sequence = ?""",
                    [query.candidate_ref, int(row["frame_sequence"])],
                )
            disposition = loads(Disposition, disposition_row["json"]) if disposition_row else None
            item = SourceNativeEvidenceItem(kind="source_native", frame=frame, disposition=disposition)
            size = encoded_size(item)
            if size > max_bytes and not items:
                raise PortError("item_too_large", frame.frame_sequence)
            if len(items) >= query.max_items or bytes_returned + size > max_bytes:
                more = True
                break
            items.append(item)
            bytes_returned += size
            next_seq = frame.frame_sequence
        if not more:
            next_seq = None
        return SourceNativePage(
            items=tuple(items), next_frame_sequence=next_seq, bytes_returned=str(bytes_returned), more=more,
        )

    def disposition_query(self, query: DispositionQuery) -> DispositionQueryPage:
        self.store.require_bronze_lookup()
        if not query.authorization_context_ref:
            raise PortError("access_denied", "authorization_context_ref is required")
        identity = identity_key(query.logical_identity)
        digest = query_digest(
            schema="ergasterion.disposition-query/v1", identity=query.logical_identity,
            disposition_id=query.disposition_id, authorization_context_ref=query.authorization_context_ref,
        )
        snapshot = self.store.resolve_query_snapshot(
            "disposition", digest, identity, query.disposition_id, query.authorization_context_ref,
            query.snapshot_token,
            high_water_sql="SELECT COUNT(*) AS n FROM dispositions WHERE identity_key = ?",
            high_water_params=[identity],
        )
        after = parse_cursor_token(query.after_cursor)
        sql = "SELECT json FROM dispositions WHERE identity_key = ?"
        params: list[Any] = [identity]
        if query.disposition_id is not None:
            sql += " AND disposition_id = ?"
            params.append(query.disposition_id)
        sql += " ORDER BY disposition_id"
        rows = self.store.fetchall(sql, params)
        items: list[Disposition] = []
        bytes_returned = 0
        max_bytes = int(query.max_bytes)
        more = False
        next_cursor: Token | None = None
        for index, row in enumerate(rows):
            if after is not None and index <= after:
                continue
            disposition = loads(Disposition, row["json"])
            size = encoded_size(disposition)
            if size > max_bytes and not items:
                raise PortError("item_too_large", disposition.disposition_id)
            if len(items) >= query.max_items or bytes_returned + size > max_bytes:
                more = True
                break
            items.append(disposition)
            bytes_returned += size
            next_cursor = cursor_token(index)
        if not more:
            next_cursor = None
        return DispositionQueryPage(
            items=tuple(items), snapshot_token=snapshot, next_cursor=next_cursor,
            bytes_returned=str(bytes_returned), more=more,
        )

    def verify_open(self, input: ExternalReceiptInput, visibility: DeliveryVisibilityIdentity) -> BronzeEvidence:
        payload = input.receipt.payload
        from ergasterion.ingestion.records import RawManifestObject, RawPayloadObject
        receipt = RawReceipt(
            schema="ergasterion.raw-receipt/v1", claim_digest=payload.delivery_claim_digest,
            payload=RawPayloadObject(
                content_id=f"sha256:{payload.raw_digest}", algorithm="sha256", byte_length="0",
                media_type="application/x-ndjson", content_encoding="identity",
            ),
            manifest=RawManifestObject(content_id=f"sha256:{payload.manifest_digest}", algorithm="sha256", byte_length="0"),
            raw_receipt_digest=payload.raw_digest,
        )
        return BronzeEvidence(
            raw_receipt=receipt, candidate_ref=payload.candidate_ref, candidate_digest=payload.candidate_digest,
            frame_index_ref=payload.frame_index_ref, frame_index_digest=payload.frame_index_digest,
            visibility=visibility,
        )

    @staticmethod
    def _is_legacy_conformance_shape(raw: bytes) -> bool:
        """The bare JSON-array-of-``{key, accept}`` shape adapter conformance
        fixtures use in place of a real per-column CSV/JSON Lines payload --
        never produced by a real ``FileSource`` delivery."""

        try:
            parsed = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError):
            return False
        return isinstance(parsed, list) and all(isinstance(item, dict) for item in parsed)

    def _frames(self, raw: bytes, contract: BronzeProductContract, content_encoding: str) -> tuple[CandidateFrame, ...]:
        """Real codec-based framing for the contract's declared CSV/JSON Lines
        shape. A payload that fails real parsing but matches the legacy bare
        JSON-array-of-``{key, accept}`` shape falls back to
        :meth:`_frames_from_bytes` unchanged -- that shape predates per-column
        typed parsing and is never produced by a real delivery."""

        from ergasterion.ingestion.codecs import decode_transport, parse_payload

        sequence_field = (
            contract.delivery.progress.field if contract.delivery.progress.kind == "sequence" else None
        )
        try:
            inner = decode_transport(
                raw, content_encoding, max_uncompressed_bytes=1073741824, max_expansion_ratio=1024,
            )
            result = parse_payload(
                inner, contract.landing.codec, contract.landing.physical_columns, sequence_field=sequence_field,
            )
        except PortError:
            if self._is_legacy_conformance_shape(raw):
                return self._frames_from_bytes(raw)
            raise
        frames: list[CandidateFrame] = []
        for parsed in result.frames:
            typed_fields = (
                tuple(CandidateField(name=name, value=value) for name, value in parsed.fields)
                if parsed.fields else None
            )
            frames.append(CandidateFrame(
                frame_sequence=parsed.frame_sequence, raw_locator=parsed.raw_locator,
                typed_fields=typed_fields, structural_findings=parsed.findings,
            ))
        return tuple(frames)

    def _frames_from_bytes(self, raw: bytes) -> tuple[CandidateFrame, ...]:
        if not raw:
            return ()
        try:
            parsed = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            parsed = None
        if isinstance(parsed, list) and all(isinstance(item, dict) for item in parsed):
            frames = []
            for index, row in enumerate(parsed):
                findings = () if row.get("accept", True) else (_reject_finding(),)
                frames.append(CandidateFrame(
                    frame_sequence=str(index),
                    raw_locator=RawLocator(
                        frame_sequence=str(index), byte_offset=None, byte_length=None, line_number=None,
                    ),
                    typed_fields=(
                        CandidateField(name="record_key", value={"logical_type": "utf8_string", "value": row.get("key", "")}),
                    ),
                    structural_findings=findings,
                ))
            return tuple(frames)
        raise PortError("codec_error", "payload is not a typed candidate row set")


def duckdb_ports_factory(
    vector: dict, contract: BronzeProductContract, payload_handle: Token,
    *, directory: str | Path | None = None,
):
    """``run_adapter_conformance`` factory: DuckDB landing/remediation/projection/lifecycle."""

    from ergasterion.ingestion.conformance import memory_ports_factory
    from ergasterion.ingestion.duckdb_lifecycle import DuckDBLifecycleSink
    from ergasterion.ingestion.duckdb_projection import DuckDBProjectionPublisher
    from ergasterion.ingestion.duckdb_remediation import DuckDBRemediationRepository
    from ergasterion.ingestion.ports import PortSet

    directory = Path(directory) if directory is not None else Path(tempfile.mkdtemp(prefix="dpf-duckdb-"))
    directory.mkdir(parents=True, exist_ok=True)
    store = DuckDBStore(directory / f"{vector['id']}.duckdb")
    ports, state = memory_ports_factory(vector, contract, payload_handle)
    fail_first_n = int(vector.get("publisher_failures", 1 if vector.get("publisher_fault") == "fail_once" else 0))
    return PortSet(
        source_connector=ports.source_connector,
        raw_store=ports.raw_store,
        scratch_store=ports.scratch_store,
        state_store=ports.state_store,
        landing_adapter=DuckDBLandingAdapter(
            store,
            finish_prepare_fault=vector.get("landing_fault"),
            finish_materialization_fault=vector.get("materialization_fault"),
        ),
        remediation_repository=DuckDBRemediationRepository(store),
        projection_publisher=DuckDBProjectionPublisher(store, fail_first_n=fail_first_n),
        lifecycle_sink=DuckDBLifecycleSink(store),
        key_resolver=ports.key_resolver,
    ), state


__all__ = [
    "BRONZE_RELATIONS",
    "PROJECTION_RELATIONS",
    "SCHEMA_VERSION",
    "DuckDBLandingAdapter",
    "DuckDBStore",
    "cursor_token",
    "dumps",
    "duckdb_ports_factory",
    "encoded_size",
    "identity_key",
    "loads",
    "parse_cursor_token",
    "query_digest",
    "snapshot_token_for",
]
