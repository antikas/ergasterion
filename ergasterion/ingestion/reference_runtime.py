"""Assemble the local PortSet: file connector, SQLite, DuckDB and in-process HMAC.

A session is one open SQLite file, one DuckDB file, the raw/scratch directories
and an ``IngestionRuntime``. Secrets never leave the process; they are
re-injected on every open. Execution is a separate step from compiling the
manifest. Projection faults are a test-only injection, not a production
scheduler.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone

from ergasterion.framework.bronze_contract import BronzeProductContract
from ergasterion.framework.runtime_binding import InterfaceReadiness, ReadinessResult, RuntimeBinding
from ergasterion.ingestion.duckdb_bronze import DuckDBLandingAdapter, DuckDBStore, identity_key
from ergasterion.ingestion.duckdb_lifecycle import DuckDBLifecycleSink
from ergasterion.ingestion.duckdb_projection import DuckDBProjectionPublisher
from ergasterion.ingestion.duckdb_remediation import DuckDBRemediationRepository
from ergasterion.ingestion.file_source import FileSource
from ergasterion.ingestion.local_raw_store import LocalRawStore
from ergasterion.ingestion.local_scratch_store import LocalScratchStore
from ergasterion.ingestion.ports import PortSet
from ergasterion.ingestion.records import EvidenceQuery, LifecycleEventBatch
from ergasterion.ingestion.runtime import (
    PORT_FIELD_ORDER,
    Clock,
    IngestionRuntime,
    admit,
    canonical_digest,
    utc_now_string,
)
from ergasterion.ingestion.settings import (
    LOCAL_IMPLEMENTATION_VERSION,
    SYNTHETIC_ACCESS_POLICY,
    SYNTHETIC_CLASSIFICATION,
    SYNTHETIC_HMAC_KEY_ID,
    SYNTHETIC_HMAC_SECRET,
    SYNTHETIC_PROTECTION_PROFILE,
    SYNTHETIC_RETENTION_POLICY,
    LocalLayout,
)
from ergasterion.ingestion.sqlite_store import SqliteKeyResolver, SqliteStateStore
from ergasterion.source_delivery import compute_derived_digest, compute_published_schema_digest, compute_source_schema_digest
from ergasterion.translators.local_ingestion import local_adapter_capabilities

_TEST_PROJECTION_FAIL_FIRST_N = 0
_TEST_CLOCK: Clock | None = None
LIFECYCLE_ORDINAL_FILENAME = "lifecycle-ordinal.json"


class _GapFreeLifecycleSink:
    """Assign consecutive event ordinals per logical identity at the port boundary.

    IngestionRuntime stamps ``event_ordinal`` from ``state_revision``, which
    jumps across contract/deployment transitions that emit no envelope. This
    wrapper keeps the DuckDB log gap-free without fabricating padding events
    and without operator commands reading lifecycle SQL.

    The cursor is keyed by logical identity so a second ``--source``/``--table``
    sharing the runtime root cannot advance another stream. The next ordinal is
    taken from that stream's existing envelopes at the lifecycle port, so a
    missing or corrupt sidecar cannot reset below DuckDBLifecycleSink's
    per-identity maximum.
    """

    def __init__(self, inner: DuckDBLifecycleSink, layout: LocalLayout) -> None:
        self._inner = inner
        self._path = layout.runtime_root / LIFECYCLE_ORDINAL_FILENAME

    def _sidecar_cursors(self) -> dict[str, int]:
        if not self._path.is_file():
            return {}
        try:
            payload = json.loads(self._path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError, TypeError, ValueError):
            return {}
        if not isinstance(payload, dict):
            return {}
        cursors = payload.get("cursors")
        if not isinstance(cursors, dict):
            return {}
        out: dict[str, int] = {}
        for key, value in cursors.items():
            try:
                out[str(key)] = int(value)
            except (TypeError, ValueError):
                continue
        return out

    def _envelopes_last(self, identity) -> int:
        row = self._inner.store.fetchone(
            """SELECT MAX(event_ordinal) AS last FROM lifecycle_events WHERE identity_key = ?""",
            [identity_key(identity)],
        )
        if row is None or row["last"] is None:
            return 0
        return int(row["last"])

    def _last_for(self, identity) -> int:
        # Existing envelopes are the invariant DuckDBLifecycleSink enforces.
        # A missing/corrupt sidecar, or a leftover whole-root counter, must not
        # win over the per-identity maximum already stored at the port.
        return self._envelopes_last(identity)

    def _store(self, updates: dict[str, int]) -> None:
        cursors = self._sidecar_cursors()
        cursors.update(updates)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._path.write_text(json.dumps({"cursors": cursors}, sort_keys=True) + "\n", encoding="utf-8")

    def project_events(self, batch: LifecycleEventBatch) -> tuple[str, ...]:
        last_by_stream: dict[str, int] = {}
        rewritten = []
        for event in batch.events:
            key = identity_key(event.logical_identity)
            if key not in last_by_stream:
                last_by_stream[key] = self._last_for(event.logical_identity)
            last_by_stream[key] += 1
            rewritten.append(event.model_copy(update={"event_ordinal": str(last_by_stream[key])}))
        projected = self._inner.project_events(
            LifecycleEventBatch(
                events=tuple(rewritten),
                max_items=batch.max_items,
                bytes_supplied=batch.bytes_supplied,
            )
        )
        self._store(last_by_stream)
        return projected

    def evidence_query(self, query: EvidenceQuery):
        return self._inner.evidence_query(query)

    def close(self) -> None:
        self._inner.close()


def set_projection_faults(n: int) -> None:
    """Inject a finite number of publisher failures. Zero restores the healthy path."""

    global _TEST_PROJECTION_FAIL_FIRST_N
    _TEST_PROJECTION_FAIL_FIRST_N = int(n)


def set_clock(clock: Clock | None) -> None:
    """Replace the trusted clock for a deterministic operator journey. ``None`` restores UTC now."""

    global _TEST_CLOCK
    _TEST_CLOCK = clock


def implementation_versions() -> dict[str, str]:
    return {name: LOCAL_IMPLEMENTATION_VERSION for name in PORT_FIELD_ORDER}


def contract_digest(contract: BronzeProductContract) -> str:
    return canonical_digest(contract.model_dump(mode="json", by_alias=True))


def build_readiness(
    contract: BronzeProductContract,
    runtime_manifest_digest: str,
    *,
    now: str,
    capability_digest: str,
) -> InterfaceReadiness:
    digest = contract_digest(contract)
    body = {
        "schema": "ergasterion.interface-readiness/v1",
        "logical_identity": contract.logical_identity.model_dump(mode="json", by_alias=True),
        "projection_target": "bronze",
        "runtime_manifest_digest": runtime_manifest_digest,
        "contract_digest": digest,
        "source_schema_digest": compute_source_schema_digest(contract),
        "published_schema_digest": compute_published_schema_digest(contract),
        "version_interface_ref": "bronze.v1",
        "capability_digest": capability_digest,
        "classification": SYNTHETIC_CLASSIFICATION,
        "access_policy_ref": SYNTHETIC_ACCESS_POLICY,
        "retention_policy_ref": SYNTHETIC_RETENTION_POLICY,
        "protection_profile": SYNTHETIC_PROTECTION_PROFILE,
        "result": ReadinessResult.READY.value,
    }
    readiness_digest = compute_derived_digest("InterfaceReadiness", body)
    return InterfaceReadiness(
        schema="ergasterion.interface-readiness/v1",
        logical_identity=contract.logical_identity,
        projection_target="bronze",
        runtime_manifest_digest=runtime_manifest_digest,
        contract_digest=digest,
        source_schema_digest=body["source_schema_digest"],
        published_schema_digest=body["published_schema_digest"],
        version_interface_ref="bronze.v1",
        capability_digest=capability_digest,
        classification=SYNTHETIC_CLASSIFICATION,
        access_policy_ref=SYNTHETIC_ACCESS_POLICY,
        retention_policy_ref=SYNTHETIC_RETENTION_POLICY,
        protection_profile=SYNTHETIC_PROTECTION_PROFILE,
        result=ReadinessResult.READY,
        readiness_digest=readiness_digest,
        verified_at=now,
        revoked_at=None,
    )


def aggregate_capability_digest() -> str:
    capabilities = local_adapter_capabilities()
    return canonical_digest({
        name: capabilities[name].model_dump(mode="json", by_alias=True) for name in PORT_FIELD_ORDER
    })


@dataclass
class LocalRuntimeSession:
    """One open local runtime for a single logical identity."""

    layout: LocalLayout
    contract: BronzeProductContract
    ports: PortSet
    runtime: IngestionRuntime
    clock: Clock
    duck_store: DuckDBStore
    state_store: SqliteStateStore
    key_resolver: SqliteKeyResolver
    publisher: DuckDBProjectionPublisher
    closed: bool = field(default=False, init=False)

    def now(self) -> str:
        instant = self.clock.now()
        self.publisher.now = instant
        return instant

    def checkpoint(self) -> None:
        try:
            self.state_store._conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        except Exception:
            pass
        try:
            self.key_resolver._conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        except Exception:
            pass
        self.duck_store.checkpoint()

    def close(self) -> None:
        if self.closed:
            return
        try:
            self.checkpoint()
        finally:
            self.state_store.close()
            self.key_resolver.close()
            self.duck_store.close()
            self.closed = True

    def __enter__(self) -> "LocalRuntimeSession":
        return self

    def __exit__(self, *exc) -> None:
        self.close()


def open_session(
    layout: LocalLayout,
    contract: BronzeProductContract,
    *,
    projection_fail_first_n: int | None = None,
    clock: Clock | None = None,
) -> LocalRuntimeSession:
    """Open (or create) the local runtime root and return a live session."""

    layout.runtime_root.mkdir(parents=True, exist_ok=True)
    layout.raw_root.mkdir(parents=True, exist_ok=True)
    layout.scratch_root.mkdir(parents=True, exist_ok=True)
    layout.sqlite_path.parent.mkdir(parents=True, exist_ok=True)
    layout.duckdb_path.parent.mkdir(parents=True, exist_ok=True)

    used_clock = clock or _TEST_CLOCK or Clock(lambda: datetime.now(timezone.utc))
    faults = _TEST_PROJECTION_FAIL_FIRST_N if projection_fail_first_n is None else projection_fail_first_n
    duck = DuckDBStore(layout.duckdb_path)
    state = SqliteStateStore(
        layout.sqlite_path,
        logical_identity=contract.logical_identity,
        lease_seconds=int(layout.binding.outbox.lease_seconds),
        deletion_keyset_days=int(layout.binding.retention.deletion_keyset_days),
        max_wire_record_bytes=int(layout.binding.runtime_resources.max_wire_record_bytes),
        now_fn=used_clock.now,
    )
    keys = SqliteKeyResolver(layout.sqlite_path)
    keys.put_hmac_secret(SYNTHETIC_HMAC_KEY_ID, SYNTHETIC_HMAC_SECRET)
    publisher = DuckDBProjectionPublisher(duck, fail_first_n=faults, now=used_clock.now())
    connector = FileSource(contract=contract, key_resolver=keys, now_fn=used_clock.now)
    ports = PortSet(
        source_connector=connector,
        raw_store=LocalRawStore(layout.raw_root),
        scratch_store=LocalScratchStore(layout.scratch_root, max_scratch_bytes=134217728),
        state_store=state,
        landing_adapter=DuckDBLandingAdapter(duck),
        remediation_repository=DuckDBRemediationRepository(duck),
        projection_publisher=publisher,
        lifecycle_sink=_GapFreeLifecycleSink(DuckDBLifecycleSink(duck), layout),
        key_resolver=keys,
    )
    runtime = IngestionRuntime(ports, used_clock, lease_owner="local-operator")
    return LocalRuntimeSession(
        layout=layout,
        contract=contract,
        ports=ports,
        runtime=runtime,
        clock=used_clock,
        duck_store=duck,
        state_store=state,
        key_resolver=keys,
        publisher=publisher,
    )


def admit_execution(
    session: LocalRuntimeSession,
    binding: RuntimeBinding,
    deployment,
    readiness: InterfaceReadiness,
    execution_plan_digest: str,
    runtime_manifest_digest: str,
) -> None:
    admit(
        binding,
        deployment,
        local_adapter_capabilities(),
        implementation_versions(),
        readiness,
        session.contract,
        execution_plan_digest,
        runtime_manifest_digest,
        session.now(),
    )


__all__ = [
    "LocalRuntimeSession",
    "admit_execution",
    "aggregate_capability_digest",
    "build_readiness",
    "contract_digest",
    "implementation_versions",
    "open_session",
    "set_clock",
    "set_projection_faults",
    "utc_now_string",
]
