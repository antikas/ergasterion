"""SQLite reference ``DeliveryStateStore``: operational state, trust metadata and evidence.

One file is the durable authority for stream state, contract/deployment lifecycle,
attempt and publication ledgers, optimistic revisions, per-target projection
cursors, snapshot/tombstone keysets, recoverable outboxes and retained lifecycle
logs. HMAC secrets never enter this file; keyed membership tags live only in the
protected keyset and deletion-evidence tables.

The public method set is exactly ``DeliveryStateStorePort`` (plus the matching
``KeyResolverPort`` / ``LifecycleSinkPort`` facades bound to the same file).
Behaviour that the in-memory fake omitted -- lease expiry, fencing, catch-up
cursor checks, key-commitment conflict, retention -- is implemented through the
same closed error codes and public operations the adapter contract already
names. Expired successor keysets drop as a side-effect of those operations.
"""

from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from ergasterion.framework.bronze_contract import (
    AttemptState,
    ContractLifecycleAction,
    DeleteStrategy,
    DeploymentLifecycleAction,
    MigrationKind,
    OutboxEntryKind,
    OutboxFailureDisposition,
    OutboxStatus,
    ReadinessResult,
    SnapshotReconciliationStatus,
)
from ergasterion.framework.runtime_binding import (
    DeploymentLifecycleRequest,
    ProjectionCursor,
    RuntimeDeployment,
)
from ergasterion.ingestion.evidence import (
    add_utc,
    b64url_decode,
    deletion_evidence_intent_digest,
    dump_json,
    hmac_key_commitment,
    hmac_sha256_tag,
    mac_result,
    snapshot_keyset_digest,
    tombstone_keyset_digest,
    verification_key_record,
)
from pydantic import TypeAdapter

from ergasterion.ingestion.records import (
    Attempt,
    AttemptPage,
    AttemptQuery,
    ContractLifecycleRequest,
    ContractLifecycleTransitionResult,
    DeletionEvidenceIntent,
    DeploymentLifecycleTransitionResult,
    DeliveryVisibilityIdentity,
    Digest,
    EvidenceOutboxPayload,
    EvidencePage,
    EvidenceQuery,
    KeyCommitmentRecord,
    LifecycleEvent,
    LifecycleEventBatch,
    LifecycleEventCursor,
    LifecycleEventLogPage,
    LifecycleEventLogQuery,
    LifecycleOutboxPayload,
    LogicalIdentity,
    MacResult,
    OperationalStatus,
    OutboxEntry,
    OutboxPayload,
    ProcessingOutcome,
    ProjectionConfirmation,
    ProjectionConfirmationLogPage,
    ProjectionIntent,
    ProjectionLogPage,
    ProjectionOutboxPayload,
    RecordKeyTagPage,
    SnapshotKeyset,
    SnapshotKeysetCompletion,
    SnapshotKeysetRequest,
    SnapshotReconciliation,
    SnapshotReconciliationRequest,
    SnapshotReconciliationResult,
    StateOutboxTransaction,
    StreamState,
    Token,
    TombstoneEvidenceRequest,
    TombstoneKeyset,
    TombstoneKeysetCompletion,
    TombstoneKeysetRequest,
    TombstoneTag,
    TombstoneTagPage,
    VerificationKeyRecord,
    VisibilityIdentity,
)
from ergasterion.ingestion.runtime import PortError, canonical_digest, utc_now_string

_OUTBOX_PAYLOAD = TypeAdapter(OutboxPayload)

SCHEMA_VERSION = 1

_PRE_INTENT = frozenset({
    AttemptState.RECEIVED, AttemptState.PREPARING, AttemptState.VALIDATING, AttemptState.MATERIALIZING,
})
_POST_INTENT = frozenset({AttemptState.COMMITTING, AttemptState.COMMIT_BLOCKED})
_TERMINAL = frozenset({AttemptState.COMMITTED, AttemptState.FAILED})

_SCHEMA_V1 = """
CREATE TABLE IF NOT EXISTS schema_meta (
    version INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS streams (
    identity_key TEXT PRIMARY KEY,
    state_json TEXT NOT NULL,
    candidate_contract_digest TEXT,
    candidate_migration_json TEXT,
    deployment_json TEXT,
    deployment_revision INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS attempts (
    attempt_id TEXT PRIMARY KEY,
    identity_key TEXT NOT NULL,
    claim_digest TEXT NOT NULL,
    ordinal INTEGER NOT NULL,
    state TEXT NOT NULL,
    json TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS outbox (
    outbox_id TEXT PRIMARY KEY,
    identity_key TEXT NOT NULL,
    entry_kind TEXT NOT NULL,
    payload_digest TEXT NOT NULL,
    status TEXT NOT NULL,
    dispatch_ordinal INTEGER NOT NULL,
    next_not_before TEXT NOT NULL,
    lease_owner TEXT,
    lease_expires_at TEXT,
    projection_revision INTEGER,
    json TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS outbox_payloads (
    outbox_id TEXT NOT NULL,
    payload_digest TEXT NOT NULL,
    json TEXT NOT NULL,
    PRIMARY KEY (outbox_id, payload_digest)
);
CREATE TABLE IF NOT EXISTS projection_intents (
    identity_key TEXT NOT NULL,
    projection_target TEXT NOT NULL,
    projection_revision INTEGER NOT NULL,
    intent_digest TEXT NOT NULL,
    json TEXT NOT NULL,
    PRIMARY KEY (identity_key, projection_target, projection_revision)
);
CREATE TABLE IF NOT EXISTS projection_confirmations (
    identity_key TEXT NOT NULL,
    projection_target TEXT NOT NULL,
    projection_revision INTEGER NOT NULL,
    intent_digest TEXT NOT NULL,
    json TEXT NOT NULL,
    PRIMARY KEY (identity_key, projection_target, projection_revision)
);
CREATE TABLE IF NOT EXISTS projection_cursors (
    identity_key TEXT NOT NULL,
    projection_target TEXT NOT NULL,
    projection_revision INTEGER NOT NULL,
    intent_digest TEXT,
    PRIMARY KEY (identity_key, projection_target)
);
CREATE TABLE IF NOT EXISTS lifecycle_events (
    event_id TEXT PRIMARY KEY,
    identity_key TEXT NOT NULL,
    state_revision INTEGER NOT NULL,
    event_ordinal INTEGER NOT NULL,
    payload_digest TEXT NOT NULL,
    json TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS snapshot_keysets (
    keyset_id TEXT PRIMARY KEY,
    identity_key TEXT NOT NULL,
    visibility_key TEXT NOT NULL,
    hmac_key_id TEXT NOT NULL,
    key_commitment TEXT NOT NULL,
    complete INTEGER NOT NULL,
    successor_complete INTEGER NOT NULL DEFAULT 0,
    retained_until TEXT,
    json TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS snapshot_tags (
    keyset_id TEXT NOT NULL,
    seq INTEGER NOT NULL,
    tag TEXT NOT NULL,
    PRIMARY KEY (keyset_id, seq)
);
CREATE TABLE IF NOT EXISTS tombstone_keysets (
    keyset_id TEXT PRIMARY KEY,
    identity_key TEXT NOT NULL,
    visibility_key TEXT NOT NULL,
    hmac_key_id TEXT NOT NULL,
    key_commitment TEXT NOT NULL,
    complete INTEGER NOT NULL,
    successor_complete INTEGER NOT NULL DEFAULT 0,
    retained_until TEXT,
    json TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS tombstone_tags (
    keyset_id TEXT NOT NULL,
    seq INTEGER NOT NULL,
    event_sequence TEXT NOT NULL,
    tag TEXT NOT NULL,
    json TEXT NOT NULL,
    PRIMARY KEY (keyset_id, seq)
);
CREATE TABLE IF NOT EXISTS snapshot_reconciliations (
    attempt_id TEXT PRIMARY KEY,
    identity_key TEXT NOT NULL,
    status TEXT NOT NULL,
    json TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS verification_keys (
    key_id TEXT PRIMARY KEY,
    fingerprint TEXT NOT NULL,
    json TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS key_commitments (
    key_id TEXT PRIMARY KEY,
    algorithm TEXT NOT NULL,
    commitment TEXT NOT NULL
);
"""


def _dumps(value: object) -> str:
    payload = dump_json(value)
    _strip_omittable_nulls(value, payload)
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


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


def _loads(cls, text: str):
    return cls.model_validate(json.loads(text))


def _identity_key(identity: LogicalIdentity) -> str:
    return _dumps(identity)


def _visibility_key(visibility: object) -> str:
    return _dumps(visibility)


def _encoded_size(value: object) -> int:
    return len(_dumps(value).encode("utf-8"))


def _connect(path: str | Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(path), timeout=30.0, isolation_level=None, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=DELETE")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=5000")
    return conn


def _migrate(conn: sqlite3.Connection) -> None:
    # ``executescript`` issues its own COMMIT; wrapping it in BEGIN/COMMIT fails.
    conn.executescript(_SCHEMA_V1)
    row = conn.execute("SELECT version FROM schema_meta").fetchone()
    if row is None:
        conn.execute("INSERT INTO schema_meta(version) VALUES (?)", (SCHEMA_VERSION,))
        return
    current = int(row["version"])
    if current > SCHEMA_VERSION:
        raise PortError("integrity_error", f"sqlite schema version {current} is ahead of {SCHEMA_VERSION}")


def _empty_state(identity: LogicalIdentity) -> StreamState:
    return StreamState(
        logical_identity=identity, active_contract_digest=None, visibility_epoch="0",
        accepted_progress={}, snapshot_reconciliation=None, state_revision="0",
        required_projection_revision="0",
    )


class SqliteStateStore:
    """Durable ``DeliveryStateStorePort`` implementation over one SQLite file."""

    def __init__(
        self,
        path: str | Path,
        *,
        logical_identity: LogicalIdentity | None = None,
        lease_seconds: int = 60,
        deletion_keyset_days: int = 30,
        max_wire_record_bytes: int = 1_048_576,
        now_fn=None,
    ) -> None:
        self.path = Path(path)
        self.lease_seconds = lease_seconds
        self.deletion_keyset_days = deletion_keyset_days
        self.max_wire_record_bytes = max_wire_record_bytes
        self.now_fn = now_fn or utc_now_string
        self._conn = _connect(self.path)
        _migrate(self._conn)
        if logical_identity is not None:
            self.ensure_stream(logical_identity)

    def close(self) -> None:
        try:
            self._conn.close()
        except sqlite3.Error:
            pass

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass

    def __enter__(self) -> "SqliteStateStore":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    def _expire_retained_keysets(self, conn: sqlite3.Connection, observed_at: str) -> None:
        """Drop successor-complete keysets whose retention window has elapsed.

        Invoked from existing ``DeliveryStateStorePort`` transactions, not as a
        separate public prune operation.
        """

        for table, tag_table in (
            ("snapshot_keysets", "snapshot_tags"),
            ("tombstone_keysets", "tombstone_tags"),
        ):
            rows = conn.execute(
                f"""SELECT keyset_id FROM {table}
                    WHERE successor_complete = 1 AND retained_until IS NOT NULL
                      AND retained_until <= ?""",
                (observed_at,),
            ).fetchall()
            for row in rows:
                conn.execute(f"DELETE FROM {tag_table} WHERE keyset_id = ?", (row["keyset_id"],))
                conn.execute(f"DELETE FROM {table} WHERE keyset_id = ?", (row["keyset_id"],))

    def _reap_expired_keysets(self) -> None:
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            self._expire_retained_keysets(self._conn, self.now_fn())
            self._conn.execute("COMMIT")
        except Exception:
            self._conn.execute("ROLLBACK")
            raise

    @contextmanager
    def _tx(self) -> Iterator[sqlite3.Connection]:
        self._reap_expired_keysets()
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            yield self._conn
            self._conn.execute("COMMIT")
        except Exception:
            self._conn.execute("ROLLBACK")
            raise

    def ensure_stream(self, identity: LogicalIdentity) -> StreamState:
        key = _identity_key(identity)
        with self._tx() as conn:
            row = conn.execute("SELECT state_json FROM streams WHERE identity_key = ?", (key,)).fetchone()
            if row is not None:
                return _loads(StreamState, row["state_json"])
            state = _empty_state(identity)
            conn.execute(
                "INSERT INTO streams(identity_key, state_json, deployment_revision) VALUES (?, ?, 0)",
                (key, _dumps(state)),
            )
            return state

    def _stream_row(self, conn: sqlite3.Connection, identity: LogicalIdentity) -> sqlite3.Row:
        key = _identity_key(identity)
        row = conn.execute("SELECT * FROM streams WHERE identity_key = ?", (key,)).fetchone()
        if row is None:
            raise PortError("not_found", key)
        return row

    def _load_state(self, conn: sqlite3.Connection, identity: LogicalIdentity) -> StreamState:
        return _loads(StreamState, self._stream_row(conn, identity)["state_json"])

    def _save_state(self, conn: sqlite3.Connection, state: StreamState, **stream_fields: object) -> None:
        key = _identity_key(state.logical_identity)
        assignments = ["state_json = ?"]
        values: list[object] = [_dumps(state)]
        for column, value in stream_fields.items():
            assignments.append(f"{column} = ?")
            values.append(value)
        values.append(key)
        conn.execute(f"UPDATE streams SET {', '.join(assignments)} WHERE identity_key = ?", values)

    def _check_revision(self, actual: str, expected: str) -> None:
        if expected != actual:
            raise PortError("stale_revision", f"expected {expected}, actual {actual}")

    def _put_attempt(self, conn: sqlite3.Connection, attempt: Attempt) -> None:
        encoded = _dumps(attempt)
        if len(encoded.encode("utf-8")) > self.max_wire_record_bytes:
            raise PortError("capacity_exceeded", attempt.attempt_id)
        conn.execute(
            """INSERT INTO attempts(attempt_id, identity_key, claim_digest, ordinal, state, json)
               VALUES (?, ?, ?, ?, ?, ?)
               ON CONFLICT(attempt_id) DO UPDATE SET
                 claim_digest = excluded.claim_digest, ordinal = excluded.ordinal,
                 state = excluded.state, json = excluded.json""",
            (
                attempt.attempt_id, _identity_key(attempt.logical_identity), attempt.claim_digest,
                attempt.attempt_ordinal, attempt.state.value, encoded,
            ),
        )

    def _attempts_for(self, conn: sqlite3.Connection, identity: LogicalIdentity) -> list[Attempt]:
        rows = conn.execute(
            "SELECT json FROM attempts WHERE identity_key = ? ORDER BY ordinal, attempt_id",
            (_identity_key(identity),),
        ).fetchall()
        return [_loads(Attempt, row["json"]) for row in rows]

    def _authoritative_cursor(
        self, conn: sqlite3.Connection, identity: LogicalIdentity, projection_target: Token,
    ) -> ProjectionCursor:
        row = conn.execute(
            "SELECT projection_revision, intent_digest FROM projection_cursors WHERE identity_key = ? AND projection_target = ?",
            (_identity_key(identity), projection_target),
        ).fetchone()
        if row is None:
            return ProjectionCursor(
                logical_identity=identity, projection_target=projection_target,
                projection_revision="0", projection_intent_digest=None,
            )
        return ProjectionCursor(
            logical_identity=identity, projection_target=projection_target,
            projection_revision=str(row["projection_revision"]), projection_intent_digest=row["intent_digest"],
        )

    def _fence_attempts(
        self, conn: sqlite3.Connection, identity: LogicalIdentity, reason: str, permit: bool,
    ) -> tuple[Digest, ...]:
        live = [a for a in self._attempts_for(conn, identity) if a.state not in _TERMINAL]
        blocked = [
            a for a in live
            if a.state in _POST_INTENT or a.remediation_commit_checkpoint is not None or a.projection_revision is not None
        ]
        if blocked:
            raise PortError("inflight_attempt", blocked[0].attempt_id)
        eligible = [a for a in live if a.state in _PRE_INTENT]
        if eligible and not permit:
            raise PortError("inflight_attempt", eligible[0].attempt_id)
        fenced: list[Digest] = []
        for attempt in eligible:
            updated = attempt.model_copy(update={"state": AttemptState.FAILED, "reason_code": reason})
            self._put_attempt(conn, updated)
            fenced.append(attempt.attempt_id)
        return tuple(sorted(fenced))

    def _check_catchup(self, auth: ProjectionCursor, catchup: ProjectionCursor, target: Token) -> None:
        if catchup.projection_target != target:
            raise PortError("unsupported_secondary_target", catchup.projection_target)
        if catchup.logical_identity != auth.logical_identity:
            raise PortError("integrity_error", "catch-up cursor identity does not match the stream")
        auth_rev = int(auth.projection_revision)
        got_rev = int(catchup.projection_revision)
        if auth_rev == 0:
            if got_rev != 0 or catchup.projection_intent_digest is not None:
                raise PortError("superseded_deployment", "catch-up cursor is ahead of an empty target")
            return
        if got_rev == 0 and catchup.projection_intent_digest is None:
            raise PortError("superseded_deployment", "a zero cursor cannot activate over confirmed revisions")
        if got_rev < auth_rev:
            raise PortError("superseded_deployment", "catch-up cursor is behind the authoritative target")
        if got_rev > auth_rev:
            raise PortError("superseded_deployment", "catch-up cursor is ahead of the authoritative target")
        if catchup.projection_intent_digest != auth.projection_intent_digest:
            raise PortError("superseded_deployment", "catch-up cursor digest does not match the authoritative target")

    def contract_lifecycle(self, request: ContractLifecycleRequest) -> ContractLifecycleTransitionResult:
        identity = request.contract.logical_identity
        digest = canonical_digest(dump_json(request.contract))
        with self._tx() as conn:
            row = self._stream_row(conn, identity)
            state = _loads(StreamState, row["state_json"])
            self._check_revision(state.state_revision, request.expected_state_revision)
            deployment = _loads(RuntimeDeployment, row["deployment_json"]) if row["deployment_json"] else None
            next_revision = str(int(state.state_revision) + 1)
            if request.action is ContractLifecycleAction.REGISTER:
                migration_json = _dumps(request.migration) if request.migration is not None else None
                next_state = state.model_copy(update={"state_revision": next_revision})
                self._save_state(
                    conn, next_state, candidate_contract_digest=digest, candidate_migration_json=migration_json,
                )
                return ContractLifecycleTransitionResult(state=next_state, deployment=deployment, fenced_attempt_ids=())
            if row["candidate_contract_digest"] is None:
                raise PortError("contract_conflict", "no candidate contract is registered to activate")
            if row["candidate_contract_digest"] != digest:
                raise PortError(
                    "contract_conflict",
                    f"registered candidate is {row['candidate_contract_digest']!r}, activation carries {digest!r}",
                )
            stored_migration = json.loads(row["candidate_migration_json"]) if row["candidate_migration_json"] else None
            incoming_migration = dump_json(request.migration) if request.migration is not None else None
            if stored_migration != incoming_migration:
                raise PortError("migration_conflict", "activation migration does not match the registered candidate")
            if request.migration is not None:
                if (
                    request.migration.from_contract_digest is not None
                    and state.active_contract_digest is not None
                    and request.migration.from_contract_digest != state.active_contract_digest
                ):
                    raise PortError("migration_conflict", "migration from-digest is not the active contract")
            fenced = self._fence_attempts(conn, identity, "superseded_contract", request.permit_pre_intent_fence)
            updates: dict = {"active_contract_digest": digest, "state_revision": next_revision}
            if request.migration is not None:
                updates["visibility_epoch"] = request.migration.to_visibility_epoch
                if request.migration.kind is MigrationKind.RESET:
                    updates["accepted_progress"] = {}
            next_state = state.model_copy(update=updates)
            self._save_state(conn, next_state, candidate_contract_digest=None, candidate_migration_json=None)
            return ContractLifecycleTransitionResult(state=next_state, deployment=deployment, fenced_attempt_ids=fenced)

    def deployment_lifecycle(self, request: DeploymentLifecycleRequest) -> DeploymentLifecycleTransitionResult:
        incoming = request.deployment
        identity = incoming.logical_identity
        with self._tx() as conn:
            row = self._stream_row(conn, identity)
            state = _loads(StreamState, row["state_json"])
            self._check_revision(state.state_revision, request.expected_state_revision)
            actual_rev = int(row["deployment_revision"])
            if int(request.expected_deployment_revision) != actual_rev:
                raise PortError("stale_revision", "deployment revision mismatch")
            if request.readiness.result is not ReadinessResult.READY:
                raise PortError("schema_invalid", f"deployment readiness is {request.readiness.result.value!r}")
            current = _loads(RuntimeDeployment, row["deployment_json"]) if row["deployment_json"] else None
            if current is not None and current.projection_target != incoming.projection_target:
                raise PortError("unsupported_secondary_target", incoming.projection_target)
            if request.action is DeploymentLifecycleAction.ACTIVATE:
                auth = self._authoritative_cursor(conn, identity, incoming.projection_target)
                self._check_catchup(auth, request.catchup_cursor, incoming.projection_target)
            next_deployment_revision = actual_rev + 1
            if request.action is DeploymentLifecycleAction.REGISTER:
                deployment = incoming.model_copy(update={
                    "candidate_manifest_digest": incoming.candidate_manifest_digest,
                    "active_manifest_digest": current.active_manifest_digest if current is not None else None,
                    "retired_manifest_digests": current.retired_manifest_digests if current is not None else (),
                    "deployment_revision": str(next_deployment_revision),
                })
                fenced: tuple[Digest, ...] = ()
            else:
                candidate = incoming.candidate_manifest_digest or (
                    current.candidate_manifest_digest if current is not None else None
                )
                if candidate is None:
                    raise PortError("superseded_deployment", "no candidate manifest is registered to activate")
                retired = tuple(current.retired_manifest_digests) if current is not None else ()
                previous = current.active_manifest_digest if current is not None else None
                fenced = self._fence_attempts(conn, identity, "superseded_deployment", request.permit_pre_intent_fence)
                deployment = incoming.model_copy(update={
                    "candidate_manifest_digest": None,
                    "active_manifest_digest": candidate,
                    "retired_manifest_digests": retired + ((previous,) if previous else ()),
                    "deployment_revision": str(next_deployment_revision),
                })
            next_state = state.model_copy(update={"state_revision": str(int(state.state_revision) + 1)})
            self._save_state(
                conn, next_state, deployment_json=_dumps(deployment), deployment_revision=next_deployment_revision,
            )
            return DeploymentLifecycleTransitionResult(
                state=next_state, deployment=deployment, catchup_cursor=request.catchup_cursor,
                fenced_attempt_ids=fenced,
            )

    def attempts(self, query: AttemptQuery) -> AttemptPage:
        with self._tx() as conn:
            items = self._attempts_for(conn, query.logical_identity)
        if query.claim_digest is not None:
            items = [a for a in items if a.claim_digest == query.claim_digest]
        if query.nonterminal_only:
            items = [a for a in items if a.state not in _TERMINAL]
        if query.after_attempt_id is not None:
            start = next(
                (index + 1 for index, a in enumerate(items) if a.attempt_id == query.after_attempt_id),
                len(items),
            )
            items = items[start:]
        page = items[: query.max_items]
        more = len(items) > len(page)
        return AttemptPage(
            attempts=tuple(page), next_after_attempt_id=page[-1].attempt_id if page and more else None, more=more,
        )

    def status_query(self, logical_identity: LogicalIdentity) -> OperationalStatus:
        with self._tx() as conn:
            state = self._load_state(conn, logical_identity)
            mine = self._attempts_for(conn, logical_identity)
            pending = conn.execute(
                """SELECT COUNT(*) AS n FROM outbox WHERE identity_key = ? AND status IN (?, ?, ?)""",
                (
                    _identity_key(logical_identity), OutboxStatus.PENDING.value, OutboxStatus.LEASED.value,
                    OutboxStatus.RETRYABLE.value,
                ),
            ).fetchone()["n"]
        latest = mine[-1] if mine else None
        processing = ProcessingOutcome.NONE
        if latest is not None:
            processing = {
                AttemptState.COMMITTED: ProcessingOutcome.COMMITTED,
                AttemptState.FAILED: ProcessingOutcome.FAILED,
                AttemptState.COMMIT_BLOCKED: ProcessingOutcome.BLOCKED,
            }.get(latest.state, ProcessingOutcome.IN_PROGRESS)
        return OperationalStatus(
            state=state, latest_attempt=latest, processing=processing,
            block_phase=latest.block_phase if latest else None, incomplete_outbox_count=str(pending),
        )

    def _record_intent(self, conn: sqlite3.Connection, intent: ProjectionIntent) -> None:
        key = _identity_key(intent.logical_identity)
        existing = conn.execute(
            "SELECT json FROM projection_intents WHERE identity_key = ? AND projection_target = ? AND projection_revision = ?",
            (key, intent.projection_target, int(intent.projection_revision)),
        ).fetchone()
        encoded = _dumps(intent)
        if existing is not None:
            previous = json.loads(existing["json"])
            if previous != json.loads(encoded):
                raise PortError("intent_conflict", intent.projection_intent_digest)
            return
        conn.execute(
            """INSERT INTO projection_intents(identity_key, projection_target, projection_revision, intent_digest, json)
               VALUES (?, ?, ?, ?, ?)""",
            (key, intent.projection_target, int(intent.projection_revision), intent.projection_intent_digest, encoded),
        )

    def _record_confirmation(self, conn: sqlite3.Connection, confirmation: ProjectionConfirmation) -> None:
        key = _identity_key(confirmation.logical_identity)
        encoded = _dumps(confirmation)
        existing = conn.execute(
            """SELECT json FROM projection_confirmations
               WHERE identity_key = ? AND projection_target = ? AND projection_revision = ?""",
            (key, confirmation.projection_target, int(confirmation.projection_revision)),
        ).fetchone()
        if existing is not None:
            if json.loads(existing["json"]) != json.loads(encoded):
                raise PortError("integrity_error", confirmation.projection_intent_digest)
            return
        conn.execute(
            """INSERT INTO projection_confirmations(identity_key, projection_target, projection_revision, intent_digest, json)
               VALUES (?, ?, ?, ?, ?)""",
            (
                key, confirmation.projection_target, int(confirmation.projection_revision),
                confirmation.projection_intent_digest, encoded,
            ),
        )
        current = conn.execute(
            "SELECT projection_revision, intent_digest FROM projection_cursors WHERE identity_key = ? AND projection_target = ?",
            (key, confirmation.projection_target),
        ).fetchone()
        incoming = int(confirmation.projection_revision)
        if current is None or incoming > int(current["projection_revision"]):
            conn.execute(
                """INSERT INTO projection_cursors(identity_key, projection_target, projection_revision, intent_digest)
                   VALUES (?, ?, ?, ?)
                   ON CONFLICT(identity_key, projection_target) DO UPDATE SET
                     projection_revision = excluded.projection_revision, intent_digest = excluded.intent_digest""",
                (key, confirmation.projection_target, incoming, confirmation.projection_intent_digest),
            )

    def _record_event(self, conn: sqlite3.Connection, event: LifecycleEvent) -> None:
        existing = conn.execute(
            "SELECT payload_digest FROM lifecycle_events WHERE event_id = ?", (event.event_id,),
        ).fetchone()
        if existing is not None:
            if existing["payload_digest"] != event.payload_digest:
                raise PortError("event_conflict", event.event_id)
            return
        conn.execute(
            """INSERT INTO lifecycle_events(event_id, identity_key, state_revision, event_ordinal, payload_digest, json)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (
                event.event_id, _identity_key(event.logical_identity), int(event.state_revision),
                int(event.event_ordinal), event.payload_digest, _dumps(event),
            ),
        )

    def _enqueue(self, conn: sqlite3.Connection, identity: LogicalIdentity, item) -> None:
        payload = item.payload
        encoded = _dumps(payload)
        if len(encoded.encode("utf-8")) > self.max_wire_record_bytes:
            raise PortError("capacity_exceeded", item.outbox_id)
        existing = conn.execute("SELECT payload_digest FROM outbox WHERE outbox_id = ?", (item.outbox_id,)).fetchone()
        if existing is not None:
            if existing["payload_digest"] != item.payload_digest:
                raise PortError("integrity_error", item.outbox_id)
            return
        revision = None
        if isinstance(payload, ProjectionOutboxPayload):
            self._record_intent(conn, payload.intent)
            revision = int(payload.intent.projection_revision)
        elif isinstance(payload, LifecycleOutboxPayload):
            self._record_event(conn, payload.event)
        elif isinstance(payload, EvidenceOutboxPayload):
            conn.execute(
                """INSERT INTO snapshot_reconciliations(attempt_id, identity_key, status, json)
                   VALUES (?, ?, ?, ?)
                   ON CONFLICT(attempt_id) DO UPDATE SET status = excluded.status, json = excluded.json""",
                (
                    payload.reconciliation.attempt_id, _identity_key(identity),
                    payload.reconciliation.status.value, _dumps(payload.reconciliation),
                ),
            )
        entry = OutboxEntry(
            outbox_id=item.outbox_id, logical_identity=identity, entry_kind=payload.entry_kind,
            payload_ref=item.outbox_id, payload_digest=item.payload_digest, status=OutboxStatus.PENDING,
            dispatch_ordinal=1, next_not_before=item.next_not_before, lease_owner=None, lease_expires_at=None,
            reason_code=None, completed_at=None,
        )
        conn.execute(
            """INSERT INTO outbox(outbox_id, identity_key, entry_kind, payload_digest, status, dispatch_ordinal,
                                  next_not_before, lease_owner, lease_expires_at, projection_revision, json)
               VALUES (?, ?, ?, ?, ?, ?, ?, NULL, NULL, ?, ?)""",
            (
                item.outbox_id, _identity_key(identity), payload.entry_kind, item.payload_digest,
                OutboxStatus.PENDING.value, 1, item.next_not_before, revision, _dumps(entry),
            ),
        )
        conn.execute(
            "INSERT INTO outbox_payloads(outbox_id, payload_digest, json) VALUES (?, ?, ?)",
            (item.outbox_id, item.payload_digest, encoded),
        )

    def state_transaction(self, transaction: StateOutboxTransaction) -> StreamState:
        identity = transaction.next_state.logical_identity
        with self._tx() as conn:
            state = self._load_state(conn, identity)
            self._check_revision(state.state_revision, transaction.expected_state_revision)
            for attempt in transaction.attempt_updates:
                self._put_attempt(conn, attempt)
            if transaction.deployment_update is not None:
                conn.execute(
                    "UPDATE streams SET deployment_json = ?, deployment_revision = ? WHERE identity_key = ?",
                    (
                        _dumps(transaction.deployment_update),
                        int(transaction.deployment_update.deployment_revision),
                        _identity_key(identity),
                    ),
                )
            for item in transaction.enqueue:
                self._enqueue(conn, identity, item)
            for done in transaction.complete:
                row = conn.execute(
                    "SELECT payload_digest, json FROM outbox WHERE outbox_id = ?", (done.outbox_id,),
                ).fetchone()
                if row is None:
                    raise PortError("not_found", done.outbox_id)
                if row["payload_digest"] != done.payload_digest:
                    raise PortError("integrity_error", done.outbox_id)
                entry = _loads(OutboxEntry, row["json"])
                if entry.status is OutboxStatus.COMPLETE:
                    continue
                if entry.status is OutboxStatus.LEASED and entry.lease_owner != done.lease_owner:
                    raise PortError("integrity_error", done.outbox_id)
                entry = entry.model_copy(update={
                    "status": OutboxStatus.COMPLETE, "completed_at": done.completed_at, "lease_owner": None,
                    "lease_expires_at": None,
                })
                conn.execute(
                    "UPDATE outbox SET status = ?, lease_owner = NULL, lease_expires_at = NULL, json = ? WHERE outbox_id = ?",
                    (OutboxStatus.COMPLETE.value, _dumps(entry), done.outbox_id),
                )
            if transaction.projection_confirmation is not None:
                self._record_confirmation(conn, transaction.projection_confirmation)
            self._save_state(conn, transaction.next_state)
            return transaction.next_state

    def load_outbox_payload(self, outbox_id: Digest, payload_digest: Digest) -> OutboxPayload:
        row = self._conn.execute(
            "SELECT json FROM outbox_payloads WHERE outbox_id = ? AND payload_digest = ?",
            (outbox_id, payload_digest),
        ).fetchone()
        if row is None:
            raise PortError("not_found", f"{outbox_id}:{payload_digest}")
        return _OUTBOX_PAYLOAD.validate_python(json.loads(row["json"]))

    def lease_outbox(
        self, logical_identity: LogicalIdentity, entry_kind: OutboxEntryKind, lease_owner: Token,
        observed_at: str, max_items: int,
    ) -> tuple[OutboxEntry, ...]:
        key = _identity_key(logical_identity)
        leased: list[OutboxEntry] = []
        with self._tx() as conn:
            rows = conn.execute(
                """SELECT * FROM outbox WHERE identity_key = ? AND entry_kind = ?
                   ORDER BY COALESCE(projection_revision, 1 << 30), outbox_id""",
                (key, entry_kind.value),
            ).fetchall()
            for row in rows:
                if len(leased) >= max_items:
                    break
                entry = _loads(OutboxEntry, row["json"])
                reclaimable = (
                    entry.status is OutboxStatus.LEASED
                    and entry.lease_expires_at is not None
                    and entry.lease_expires_at <= observed_at
                )
                if entry.status not in (OutboxStatus.PENDING, OutboxStatus.RETRYABLE) and not reclaimable:
                    continue
                if entry.next_not_before > observed_at:
                    continue
                if entry_kind is OutboxEntryKind.PROJECTION and leased:
                    # Only the lowest incomplete revision in the partition is leasable.
                    break
                expires = add_utc(observed_at, self.lease_seconds)
                updated = entry.model_copy(update={
                    "status": OutboxStatus.LEASED, "lease_owner": lease_owner, "lease_expires_at": expires,
                })
                conn.execute(
                    "UPDATE outbox SET status = ?, lease_owner = ?, lease_expires_at = ?, json = ? WHERE outbox_id = ?",
                    (OutboxStatus.LEASED.value, lease_owner, expires, _dumps(updated), entry.outbox_id),
                )
                leased.append(updated)
        return tuple(leased)

    def fail_outbox(self, transaction) -> StreamState:
        identity = transaction.next_state.logical_identity
        with self._tx() as conn:
            state = self._load_state(conn, identity)
            self._check_revision(state.state_revision, transaction.expected_state_revision)
            for attempt in transaction.attempt_updates:
                self._put_attempt(conn, attempt)
            row = conn.execute(
                "SELECT payload_digest, json FROM outbox WHERE outbox_id = ?", (transaction.outbox_id,),
            ).fetchone()
            if row is None:
                raise PortError("not_found", transaction.outbox_id)
            if row["payload_digest"] != transaction.payload_digest:
                raise PortError("integrity_error", transaction.outbox_id)
            entry = _loads(OutboxEntry, row["json"])
            if entry.status is OutboxStatus.LEASED and entry.lease_owner not in (None, transaction.lease_owner):
                if entry.lease_expires_at is None or entry.lease_expires_at > transaction.failure_observed_at:
                    raise PortError("concurrency_conflict", transaction.outbox_id)
            next_status = (
                OutboxStatus.DEAD_LETTER
                if transaction.disposition is OutboxFailureDisposition.DEAD_LETTER
                else OutboxStatus.RETRYABLE
            )
            updated = entry.model_copy(update={
                "status": next_status,
                "next_not_before": transaction.next_not_before or entry.next_not_before,
                "reason_code": transaction.reason_code, "lease_owner": None, "lease_expires_at": None,
                "dispatch_ordinal": entry.dispatch_ordinal + 1,
            })
            conn.execute(
                """UPDATE outbox SET status = ?, next_not_before = ?, lease_owner = NULL, lease_expires_at = NULL,
                          dispatch_ordinal = ?, json = ? WHERE outbox_id = ?""",
                (
                    next_status.value, updated.next_not_before, updated.dispatch_ordinal, _dumps(updated),
                    transaction.outbox_id,
                ),
            )
            self._save_state(conn, transaction.next_state)
            return transaction.next_state

    def _paged_log(self, rows: list, after_revision: str, max_items: int, max_bytes: str, attr: str):
        items = []
        total = 0
        more = False
        limit = int(max_bytes)
        for payload in rows:
            if int(getattr(payload, "projection_revision")) <= int(after_revision):
                continue
            size = _encoded_size(payload)
            if not items and size > limit:
                raise PortError("item_too_large", getattr(payload, "projection_intent_digest"))
            if len(items) >= max_items or total + size > limit:
                more = True
                break
            items.append(payload)
            total += size
        next_after = getattr(items[-1], "projection_revision") if items else None
        return items, next_after, str(total), more

    def projection_log(self, logical_identity, after_revision, max_items, max_bytes) -> ProjectionLogPage:
        rows = self._conn.execute(
            "SELECT json FROM projection_intents WHERE identity_key = ? ORDER BY projection_revision",
            (_identity_key(logical_identity),),
        ).fetchall()
        intents = [_loads(ProjectionIntent, row["json"]) for row in rows]
        page, next_after, nbytes, more = self._paged_log(intents, after_revision, max_items, max_bytes, "intents")
        return ProjectionLogPage(intents=tuple(page), next_after_revision=next_after, bytes_returned=nbytes, more=more)

    def projection_confirmation_log(self, logical_identity, after_revision, max_items, max_bytes) -> ProjectionConfirmationLogPage:
        rows = self._conn.execute(
            "SELECT json FROM projection_confirmations WHERE identity_key = ? ORDER BY projection_revision",
            (_identity_key(logical_identity),),
        ).fetchall()
        items = [_loads(ProjectionConfirmation, row["json"]) for row in rows]
        page, next_after, nbytes, more = self._paged_log(items, after_revision, max_items, max_bytes, "confirmations")
        return ProjectionConfirmationLogPage(
            confirmations=tuple(page), next_after_revision=next_after, bytes_returned=nbytes, more=more,
        )

    def lifecycle_event_log(self, query: LifecycleEventLogQuery) -> LifecycleEventLogPage:
        rows = self._conn.execute(
            """SELECT json FROM lifecycle_events WHERE identity_key = ?
               ORDER BY state_revision, event_ordinal, event_id""",
            (_identity_key(query.logical_identity),),
        ).fetchall()
        events = [_loads(LifecycleEvent, row["json"]) for row in rows]
        if query.after_cursor is not None:
            events = [
                event for event in events
                if (int(event.state_revision), int(event.event_ordinal), event.event_id)
                > (int(query.after_cursor.state_revision), int(query.after_cursor.event_ordinal), query.after_cursor.event_id)
            ]
        page = []
        total = 0
        more = False
        limit = int(query.max_bytes)
        for event in events:
            size = _encoded_size(event)
            if not page and size > limit:
                raise PortError("item_too_large", event.event_id)
            if len(page) >= query.max_items or total + size > limit:
                more = True
                break
            page.append(event)
            total += size
        cursor = None
        if page:
            last = page[-1]
            cursor = LifecycleEventCursor(
                state_revision=last.state_revision, event_ordinal=last.event_ordinal, event_id=last.event_id,
            )
        return LifecycleEventLogPage(events=tuple(page), next_cursor=cursor, bytes_returned=str(total), more=more)

    def _keyset_id(self, identity: LogicalIdentity, visibility, kind: str) -> Digest:
        return canonical_digest({
            "logical_identity": dump_json(identity), "visibility": dump_json(visibility), "kind": kind,
        })

    def begin_snapshot_keyset(self, request: SnapshotKeysetRequest) -> SnapshotKeyset:
        keyset_id = self._keyset_id(request.logical_identity, request.visibility, "snapshot")
        with self._tx() as conn:
            row = conn.execute("SELECT * FROM snapshot_keysets WHERE keyset_id = ?", (keyset_id,)).fetchone()
            if row is not None:
                if row["key_commitment"] != request.key_commitment or row["hmac_key_id"] != request.hmac_key_id:
                    raise PortError("key_commitment_conflict", keyset_id)
                return _loads(SnapshotKeyset, row["json"])
            keyset = SnapshotKeyset(
                keyset_id=keyset_id, logical_identity=request.logical_identity, visibility=request.visibility,
                record_key_scope=request.record_key_scope, hmac_key_id=request.hmac_key_id,
                key_commitment=request.key_commitment, keyset_ref=keyset_id, keyset_digest=None,
                key_count="0", complete=False,
            )
            conn.execute(
                """INSERT INTO snapshot_keysets(keyset_id, identity_key, visibility_key, hmac_key_id, key_commitment,
                                               complete, json)
                   VALUES (?, ?, ?, ?, ?, 0, ?)""",
                (
                    keyset_id, _identity_key(request.logical_identity), _visibility_key(request.visibility),
                    request.hmac_key_id, request.key_commitment, _dumps(keyset),
                ),
            )
            return keyset

    def append_snapshot_keyset(self, attempt_id: Digest, page: RecordKeyTagPage) -> SnapshotKeyset:
        with self._tx() as conn:
            row = conn.execute("SELECT * FROM snapshot_keysets WHERE keyset_id = ?", (page.keyset_id,)).fetchone()
            if row is None:
                raise PortError("not_found", page.keyset_id)
            if row["complete"]:
                raise PortError("integrity_error", "a complete keyset cannot take more tags")
            count = conn.execute("SELECT COUNT(*) AS n FROM snapshot_tags WHERE keyset_id = ?", (page.keyset_id,)).fetchone()["n"]
            if int(page.first_frame_sequence) != count:
                raise PortError("sequence_conflict", f"keyset holds {count} tags, page starts at {page.first_frame_sequence}")
            for offset, tag in enumerate(page.tags):
                conn.execute(
                    "INSERT INTO snapshot_tags(keyset_id, seq, tag) VALUES (?, ?, ?)",
                    (page.keyset_id, count + offset, tag),
                )
            keyset = _loads(SnapshotKeyset, row["json"]).model_copy(update={"key_count": str(count + len(page.tags))})
            conn.execute("UPDATE snapshot_keysets SET json = ? WHERE keyset_id = ?", (_dumps(keyset), page.keyset_id))
            return keyset

    def complete_snapshot_keyset(self, completion: SnapshotKeysetCompletion) -> SnapshotKeyset:
        with self._tx() as conn:
            row = conn.execute("SELECT * FROM snapshot_keysets WHERE keyset_id = ?", (completion.keyset_id,)).fetchone()
            if row is None:
                raise PortError("not_found", completion.keyset_id)
            tags = [
                r["tag"] for r in conn.execute(
                    "SELECT tag FROM snapshot_tags WHERE keyset_id = ? ORDER BY seq", (completion.keyset_id,),
                ).fetchall()
            ]
            if str(len(tags)) != completion.expected_key_count:
                raise PortError(
                    "integrity_error",
                    f"keyset holds {len(tags)} tags, completion expected {completion.expected_key_count}",
                )
            keyset = _loads(SnapshotKeyset, row["json"])
            digest = snapshot_keyset_digest(
                keyset.logical_identity, keyset.visibility, keyset.record_key_scope,
                keyset.hmac_key_id, keyset.key_commitment, tags,
            )
            if digest != completion.expected_keyset_digest:
                raise PortError("integrity_error", "keyset digest does not match the expected digest")
            keyset = keyset.model_copy(update={"complete": True, "keyset_digest": digest})
            conn.execute(
                "UPDATE snapshot_keysets SET complete = 1, json = ? WHERE keyset_id = ?",
                (_dumps(keyset), completion.keyset_id),
            )
            return keyset

    def get_snapshot_keyset(self, logical_identity: LogicalIdentity, visibility: VisibilityIdentity) -> SnapshotKeyset:
        with self._tx() as conn:
            row = conn.execute(
                """SELECT json FROM snapshot_keysets WHERE identity_key = ? AND visibility_key = ?
                   ORDER BY complete DESC, keyset_id""",
                (_identity_key(logical_identity), _visibility_key(visibility)),
            ).fetchone()
            loaded = None if row is None else _loads(SnapshotKeyset, row["json"])
        if loaded is None:
            raise PortError("not_found", "no snapshot keyset for that visibility")
        return loaded

    def reconcile_snapshot(self, request: SnapshotReconciliationRequest) -> SnapshotReconciliationResult:
        candidate = request.candidate_keyset
        if not candidate.complete:
            raise PortError("integrity_error", "an incomplete candidate keyset cannot be reconciled")
        with self._tx() as conn:
            existing = conn.execute(
                "SELECT json FROM snapshot_reconciliations WHERE attempt_id = ?", (request.attempt_id,),
            ).fetchone()
            if existing is not None:
                stored = _loads(SnapshotReconciliation, existing["json"])
                if stored.candidate_keyset_ref != candidate.keyset_ref:
                    raise PortError("concurrency_conflict", request.attempt_id)
                intent = stored.deletion_evidence
                if intent is None:
                    raise PortError("integrity_error", request.attempt_id)
                return SnapshotReconciliationResult(reconciliation=stored, deletion_evidence=intent)
            cand_row = conn.execute("SELECT * FROM snapshot_keysets WHERE keyset_id = ?", (candidate.keyset_id,)).fetchone()
            if cand_row is None:
                raise PortError("not_found", candidate.keyset_id)
            candidate_tags = {
                r["tag"] for r in conn.execute(
                    "SELECT tag FROM snapshot_tags WHERE keyset_id = ?", (candidate.keyset_id,),
                ).fetchall()
            }
            prior_tags: set[str] = set()
            if request.prior_keyset is not None:
                if (
                    request.prior_keyset.hmac_key_id != candidate.hmac_key_id
                    or request.prior_keyset.key_commitment != candidate.key_commitment
                ):
                    raise PortError("key_commitment_conflict", candidate.keyset_id)
                prior_row = conn.execute(
                    "SELECT * FROM snapshot_keysets WHERE keyset_id = ?", (request.prior_keyset.keyset_id,),
                ).fetchone()
                if prior_row is None:
                    raise PortError("not_found", request.prior_keyset.keyset_id)
                prior_tags = {
                    r["tag"] for r in conn.execute(
                        "SELECT tag FROM snapshot_tags WHERE keyset_id = ?", (request.prior_keyset.keyset_id,),
                    ).fetchall()
                }
            deleted = sorted(prior_tags - candidate_tags)
            deleted_ref = canonical_digest({"deleted": deleted, "attempt": request.attempt_id})
            deleted_digest = snapshot_keyset_digest(
                candidate.logical_identity, candidate.visibility, candidate.record_key_scope,
                candidate.hmac_key_id, candidate.key_commitment, deleted,
            ) if deleted else snapshot_keyset_digest(
                candidate.logical_identity, candidate.visibility, candidate.record_key_scope,
                candidate.hmac_key_id, candidate.key_commitment, (),
            )
            reconciliation_digest = canonical_digest({
                "candidate": candidate.keyset_id,
                "prior": request.prior_keyset.keyset_id if request.prior_keyset else None,
                "deleted": deleted,
            })
            intent_body = {
                "logical_identity": dump_json(candidate.logical_identity),
                "visibility": dump_json(candidate.visibility),
                "delete_strategy": DeleteStrategy.SNAPSHOT_DIFF.value,
                "claim_digest": request.claim_digest,
                "attempt_id": request.attempt_id,
                "event_sequence_low": None,
                "event_sequence_high": None,
                "record_key_scope": dump_json(candidate.record_key_scope),
                "hmac_key_id": candidate.hmac_key_id,
                "key_commitment": candidate.key_commitment,
                "deleted_keyset_ref": deleted_ref,
                "deleted_keyset_digest": deleted_digest,
                "deleted_key_count": str(len(deleted)),
                "reconciliation_digest": reconciliation_digest,
            }
            intent = DeletionEvidenceIntent(
                **intent_body, deletion_evidence_intent_digest=deletion_evidence_intent_digest(intent_body),
            )
            visibility = candidate.visibility
            if not isinstance(visibility, DeliveryVisibilityIdentity):
                visibility = DeliveryVisibilityIdentity(
                    epoch=getattr(visibility, "epoch"), kind="delivery",
                    id=getattr(visibility, "id"),
                )
            reconciliation = SnapshotReconciliation(
                schema="ergasterion.snapshot-reconciliation/v1",
                logical_identity=candidate.logical_identity, attempt_id=request.attempt_id,
                candidate_visibility=visibility,
                prior_visibility=request.prior_keyset.visibility if request.prior_keyset else None,
                prior_keyset_ref=request.prior_keyset.keyset_ref if request.prior_keyset else None,
                candidate_keyset_ref=candidate.keyset_ref, status=SnapshotReconciliationStatus.COMPLETE,
                attempt_count="1", next_attempt_at=None, lease_owner=None, lease_expires_at=None,
                reason_code=None, deletion_evidence=intent, reconciliation_digest=reconciliation_digest,
            )
            conn.execute(
                """INSERT INTO snapshot_reconciliations(attempt_id, identity_key, status, json)
                   VALUES (?, ?, ?, ?)""",
                (
                    request.attempt_id, _identity_key(candidate.logical_identity),
                    SnapshotReconciliationStatus.COMPLETE.value, _dumps(reconciliation),
                ),
            )
            if request.prior_keyset is not None:
                retained_until = add_utc(self.now_fn(), self.deletion_keyset_days * 86400)
                conn.execute(
                    "UPDATE snapshot_keysets SET successor_complete = 1, retained_until = ? WHERE keyset_id = ?",
                    (retained_until, request.prior_keyset.keyset_id),
                )
            return SnapshotReconciliationResult(reconciliation=reconciliation, deletion_evidence=intent)

    def begin_tombstone_keyset(self, request: TombstoneKeysetRequest) -> TombstoneKeyset:
        keyset_id = self._keyset_id(request.logical_identity, request.visibility, "tombstone")
        with self._tx() as conn:
            row = conn.execute("SELECT * FROM tombstone_keysets WHERE keyset_id = ?", (keyset_id,)).fetchone()
            if row is not None:
                if row["key_commitment"] != request.key_commitment or row["hmac_key_id"] != request.hmac_key_id:
                    raise PortError("key_commitment_conflict", keyset_id)
                return _loads(TombstoneKeyset, row["json"])
            keyset = TombstoneKeyset(
                keyset_id=keyset_id, logical_identity=request.logical_identity, visibility=request.visibility,
                record_key_scope=request.record_key_scope, hmac_key_id=request.hmac_key_id,
                key_commitment=request.key_commitment, keyset_ref=keyset_id, keyset_digest=None,
                key_count="0", event_sequence_low=None, event_sequence_high=None, complete=False,
            )
            conn.execute(
                """INSERT INTO tombstone_keysets(keyset_id, identity_key, visibility_key, hmac_key_id, key_commitment,
                                                complete, json)
                   VALUES (?, ?, ?, ?, ?, 0, ?)""",
                (
                    keyset_id, _identity_key(request.logical_identity), _visibility_key(request.visibility),
                    request.hmac_key_id, request.key_commitment, _dumps(keyset),
                ),
            )
            return keyset

    def append_tombstone_keyset(self, attempt_id: Digest, page: TombstoneTagPage) -> TombstoneKeyset:
        with self._tx() as conn:
            row = conn.execute("SELECT * FROM tombstone_keysets WHERE keyset_id = ?", (page.keyset_id,)).fetchone()
            if row is None:
                raise PortError("not_found", page.keyset_id)
            if row["complete"]:
                raise PortError("integrity_error", "a complete keyset cannot take more tags")
            last = conn.execute(
                "SELECT event_sequence FROM tombstone_tags WHERE keyset_id = ? ORDER BY seq DESC LIMIT 1",
                (page.keyset_id,),
            ).fetchone()
            last_seq = last["event_sequence"] if last is not None else None
            count = conn.execute(
                "SELECT COUNT(*) AS n FROM tombstone_tags WHERE keyset_id = ?", (page.keyset_id,),
            ).fetchone()["n"]
            for offset, item in enumerate(page.items):
                if last_seq is not None and int(item.event_sequence) <= int(last_seq):
                    raise PortError("sequence_conflict", f"event sequence {item.event_sequence} is not ahead of the keyset")
                conn.execute(
                    "INSERT INTO tombstone_tags(keyset_id, seq, event_sequence, tag, json) VALUES (?, ?, ?, ?, ?)",
                    (page.keyset_id, count + offset, item.event_sequence, item.tag, _dumps(item)),
                )
                last_seq = item.event_sequence
            tags = conn.execute(
                "SELECT event_sequence FROM tombstone_tags WHERE keyset_id = ? ORDER BY seq", (page.keyset_id,),
            ).fetchall()
            keyset = _loads(TombstoneKeyset, row["json"]).model_copy(update={
                "key_count": str(len(tags)),
                "event_sequence_low": tags[0]["event_sequence"] if tags else None,
                "event_sequence_high": tags[-1]["event_sequence"] if tags else None,
            })
            conn.execute("UPDATE tombstone_keysets SET json = ? WHERE keyset_id = ?", (_dumps(keyset), page.keyset_id))
            return keyset

    def complete_tombstone_keyset(self, completion: TombstoneKeysetCompletion) -> TombstoneKeyset:
        with self._tx() as conn:
            row = conn.execute("SELECT * FROM tombstone_keysets WHERE keyset_id = ?", (completion.keyset_id,)).fetchone()
            if row is None:
                raise PortError("not_found", completion.keyset_id)
            items = [
                _loads(TombstoneTag, r["json"])
                for r in conn.execute(
                    "SELECT json FROM tombstone_tags WHERE keyset_id = ? ORDER BY seq", (completion.keyset_id,),
                ).fetchall()
            ]
            if str(len(items)) != completion.expected_key_count:
                raise PortError(
                    "integrity_error",
                    f"keyset holds {len(items)} tags, completion expected {completion.expected_key_count}",
                )
            keyset = _loads(TombstoneKeyset, row["json"])
            digest = tombstone_keyset_digest(
                keyset.logical_identity, keyset.visibility, keyset.record_key_scope,
                keyset.hmac_key_id, keyset.key_commitment, items,
            )
            if digest != completion.expected_keyset_digest:
                raise PortError("integrity_error", "keyset digest does not match the expected digest")
            keyset = keyset.model_copy(update={
                "complete": True, "keyset_digest": digest,
                "event_sequence_low": completion.event_sequence_low,
                "event_sequence_high": completion.event_sequence_high,
            })
            conn.execute(
                "UPDATE tombstone_keysets SET complete = 1, json = ? WHERE keyset_id = ?",
                (_dumps(keyset), completion.keyset_id),
            )
            return keyset

    def finalize_tombstone_evidence(self, request: TombstoneEvidenceRequest) -> DeletionEvidenceIntent:
        with self._tx() as conn:
            row = conn.execute(
                "SELECT * FROM tombstone_keysets WHERE keyset_id = ?", (request.keyset.keyset_id,),
            ).fetchone()
            if row is None:
                raise PortError("not_found", request.keyset.keyset_id)
            keyset = _loads(TombstoneKeyset, row["json"])
            if not keyset.complete:
                raise PortError("integrity_error", "an incomplete tombstone keyset cannot be finalized")
            if (
                keyset.hmac_key_id != request.keyset.hmac_key_id
                or keyset.key_commitment != request.keyset.key_commitment
            ):
                raise PortError("key_commitment_conflict", keyset.keyset_id)
            body = {
                "logical_identity": dump_json(keyset.logical_identity),
                "visibility": dump_json(keyset.visibility),
                "delete_strategy": DeleteStrategy.EXPLICIT_TOMBSTONE.value,
                "claim_digest": request.claim_digest,
                "attempt_id": request.attempt_id,
                "event_sequence_low": keyset.event_sequence_low,
                "event_sequence_high": keyset.event_sequence_high,
                "record_key_scope": dump_json(keyset.record_key_scope),
                "hmac_key_id": keyset.hmac_key_id,
                "key_commitment": keyset.key_commitment,
                "deleted_keyset_ref": keyset.keyset_ref,
                "deleted_keyset_digest": keyset.keyset_digest,
                "deleted_key_count": keyset.key_count,
                "reconciliation_digest": None,
            }
            intent = DeletionEvidenceIntent(
                **body, deletion_evidence_intent_digest=deletion_evidence_intent_digest(body),
            )
            retained_until = add_utc(self.now_fn(), self.deletion_keyset_days * 86400)
            conn.execute(
                "UPDATE tombstone_keysets SET successor_complete = 1, retained_until = ? WHERE keyset_id = ?",
                (retained_until, keyset.keyset_id),
            )
            return intent


class SqliteKeyResolver:
    """Key resolver whose public records and commitments persist; HMAC secrets do not."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self._secrets: dict[Token, bytes] = {}
        self._conn = _connect(self.path)
        _migrate(self._conn)

    def close(self) -> None:
        self._secrets.clear()
        try:
            self._conn.close()
        except sqlite3.Error:
            pass

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass

    def put_verification_key(self, record: VerificationKeyRecord) -> VerificationKeyRecord:
        with self._tx() as conn:
            existing = conn.execute(
                "SELECT fingerprint, json FROM verification_keys WHERE key_id = ?", (record.key_id,),
            ).fetchone()
            if existing is not None and existing["fingerprint"] != record.public_key_fingerprint:
                raise PortError("key_commitment_conflict", record.key_id)
            if existing is None:
                conn.execute(
                    "INSERT INTO verification_keys(key_id, fingerprint, json) VALUES (?, ?, ?)",
                    (record.key_id, record.public_key_fingerprint, _dumps(record)),
                )
                return record
            stored = _loads(VerificationKeyRecord, existing["json"])
            revoked_at = record.revoked_at if record.revoked_at is not None else stored.revoked_at
            expires_at = record.expires_at if record.expires_at is not None else stored.expires_at
            if revoked_at == stored.revoked_at and expires_at == stored.expires_at:
                return stored
            updated = verification_key_record(
                stored.key_id,
                b64url_decode(stored.public_key_base64url),
                stored.enabled_at,
                stored.authorized_policy_refs,
                expires_at=expires_at,
                revoked_at=revoked_at,
            )
            conn.execute(
                "UPDATE verification_keys SET json = ? WHERE key_id = ?",
                (_dumps(updated), record.key_id),
            )
            return updated

    def put_hmac_secret(self, key_id: Token, secret: bytes) -> KeyCommitmentRecord:
        commitment = hmac_key_commitment(key_id, secret)
        with self._tx() as conn:
            existing = conn.execute(
                "SELECT commitment FROM key_commitments WHERE key_id = ?", (key_id,),
            ).fetchone()
            if existing is not None and existing["commitment"] != commitment:
                raise PortError("key_commitment_conflict", key_id)
            if existing is None:
                conn.execute(
                    "INSERT INTO key_commitments(key_id, algorithm, commitment) VALUES (?, ?, ?)",
                    (key_id, "HMAC-SHA-256", commitment),
                )
        self._secrets[key_id] = secret
        return KeyCommitmentRecord(key_id=key_id, algorithm="HMAC-SHA-256", commitment=commitment)

    @contextmanager
    def _tx(self) -> Iterator[sqlite3.Connection]:
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            yield self._conn
            self._conn.execute("COMMIT")
        except Exception:
            self._conn.execute("ROLLBACK")
            raise

    def resolve_verification_key(self, key_id: Token) -> VerificationKeyRecord:
        row = self._conn.execute(
            "SELECT json FROM verification_keys WHERE key_id = ?", (key_id,),
        ).fetchone()
        if row is None:
            raise PortError("key_not_found", key_id)
        record = _loads(VerificationKeyRecord, row["json"])
        if record.revoked_at is not None:
            raise PortError("key_revoked", key_id)
        return record

    def key_commitment(self, key_id: Token) -> KeyCommitmentRecord:
        row = self._conn.execute(
            "SELECT algorithm, commitment FROM key_commitments WHERE key_id = ?", (key_id,),
        ).fetchone()
        if row is None:
            raise PortError("key_not_found", key_id)
        return KeyCommitmentRecord(key_id=key_id, algorithm=row["algorithm"], commitment=row["commitment"])

    def mac(self, key_id: Token, domain: str, message_base64url: str) -> MacResult:
        secret = self._secrets.get(key_id)
        if secret is None:
            raise PortError("key_not_found", key_id)
        tag = hmac_sha256_tag(secret, domain, b64url_decode(message_base64url))
        return mac_result(key_id, tag)


class SqliteLifecycleSink:
    """Projects lifecycle envelopes into the same SQLite file the state store reads."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self._conn = _connect(self.path)
        _migrate(self._conn)

    def close(self) -> None:
        try:
            self._conn.close()
        except sqlite3.Error:
            pass

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass

    def project_events(self, batch: LifecycleEventBatch) -> tuple[Digest, ...]:
        ids: list[Digest] = []
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            for event in batch.events:
                existing = self._conn.execute(
                    "SELECT payload_digest FROM lifecycle_events WHERE event_id = ?", (event.event_id,),
                ).fetchone()
                if existing is not None:
                    if existing["payload_digest"] != event.payload_digest:
                        raise PortError("event_conflict", event.event_id)
                    ids.append(event.event_id)
                    continue
                self._conn.execute(
                    """INSERT INTO lifecycle_events(event_id, identity_key, state_revision, event_ordinal, payload_digest, json)
                       VALUES (?, ?, ?, ?, ?, ?)""",
                    (
                        event.event_id, _identity_key(event.logical_identity), int(event.state_revision),
                        int(event.event_ordinal), event.payload_digest, _dumps(event),
                    ),
                )
                ids.append(event.event_id)
            self._conn.execute("COMMIT")
        except Exception:
            self._conn.execute("ROLLBACK")
            raise
        return tuple(ids)

    def evidence_query(self, query: EvidenceQuery) -> EvidencePage:
        return EvidencePage(items=(), next_cursor=None, bytes_returned="0", more=False)


def sqlite_ports_factory(vector: dict, contract, payload_handle: Token, *, directory: str | Path | None = None):
    """``run_adapter_conformance`` factory: SQLite state store, in-memory remainder."""

    import tempfile

    from ergasterion.ingestion.conformance import memory_ports_factory
    from ergasterion.ingestion.ports import PortSet

    directory = Path(directory) if directory is not None else Path(tempfile.mkdtemp(prefix="dpf-sqlite-"))
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{vector['id']}.sqlite"
    store = SqliteStateStore(path, logical_identity=contract.logical_identity)
    ports, _ignored = memory_ports_factory(vector, contract, payload_handle)
    state = store.status_query(contract.logical_identity).state
    return PortSet(
        source_connector=ports.source_connector,
        raw_store=ports.raw_store,
        scratch_store=ports.scratch_store,
        state_store=store,
        landing_adapter=ports.landing_adapter,
        remediation_repository=ports.remediation_repository,
        projection_publisher=ports.projection_publisher,
        lifecycle_sink=ports.lifecycle_sink,
        key_resolver=ports.key_resolver,
    ), state


__all__ = [
    "SCHEMA_VERSION",
    "SqliteKeyResolver",
    "SqliteLifecycleSink",
    "SqliteStateStore",
    "sqlite_ports_factory",
]
