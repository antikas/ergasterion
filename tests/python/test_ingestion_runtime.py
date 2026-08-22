"""Assert-script tests for ergasterion/ingestion/ports.py, runtime.py and
conformance.py (repo convention: no pytest).

Each test function proves one property of the Bronze runtime-ports service:

Ports and conformance seam
  - The nine port protocols (``ergasterion.ingestion.ports``) are structurally
    satisfied by the packaged in-memory reference set.
  - ``exercise_all_operations`` reaches every operation of all nine ports, so
    the surface is proven whole rather than only along one delivery's path.
  - The packaged runner accepts implementations explicitly: the caller supplies
    the factory that builds the ``PortSet`` under test and the runner resolves
    nothing from a registry.
  - Every vector in ``ergasterion/conformance/adapter-v1.json`` passes.

Admission
  - Port topology, adapter implementation versions, plan/manifest agreement,
    schema readiness and the aggregate memory/scratch budget each admit a
    correct deployment and reject a wrong one with a closed error code.

Delivery rules
  - Delivery modes: a managed payload requires a managed integration, the
    progress claim's kind must match, and a complete-snapshot delivery must
    carry its attestation.
  - Claim replay and reprocessing replay are idempotent; conflicting claims are
    rejected -- including once the stream holds more attempts than one page,
    where every scan must page to the end and no identifier may collide.
  - Quarantine release restrictions: no release under the originating ruleset,
    and one evaluation releases exactly once.
  - Scratch scopes enforce capacity, sequencing, ownership isolation and orphan
    cleanup.

Progress, order and failure
  - A pre-intent failure and a permanent failure never advance progress, and a
    failure raised after the ``materializing`` transition fails the attempt
    against the state it actually reached, carrying its own error code out.
  - Due entries replay in staged projection-revision order, not in retry-count
    order.
  - A post-intent target failure reserves accepted progress behind an invisible
    ``commit_blocked`` outbox entry, rejects a conflicting successor, replays
    the same claim, and resumes exactly the same publication.
  - Retry exhaustion dead-letters instead of retrying forever.
  - Trusted-clock catch-up evaluates every mandatory occurrence in order, and a
    target failure delays an occurrence without skipping it -- including an
    occurrence raised while a publication is ``commit_blocked``, which waits
    rather than reserving a revision behind the blocked one, and the same wait
    after that publication has exhausted retries and dead-lettered.
  - Projection revisions and lifecycle event ordinals are both gap-free.

Concurrency and crash seams
  - Concurrent contract and deployment activations resolve to one winner.
  - Candidate registration and activation are separate transitions.
  - Crash before the remediation compare-and-swap, crash after it, crash at its
    checkpoint, and crash at the outbox completion each recover exactly once.

Persistence neutrality
  - Static imports and a subprocess import both prove no SQLite, DuckDB, dbt or
    orchestrator dependency.

Usage:
    python tests/python/test_ingestion_runtime.py
"""

from __future__ import annotations

import ast
import json
import subprocess
import sys
import traceback
from datetime import datetime, timedelta
from pathlib import Path

# Allow direct execution as `python tests/python/test_ingestion_runtime.py`.
if __package__ in (None, ""):
    import os as _os, sys as _sys
    _sys.path.insert(0, _os.path.dirname(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))))

from ergasterion.framework.bronze_contract import (
    BronzeProductContract,
    DeliveryMode,
    PublicationPolicy,
    ReadinessResult,
)
from ergasterion.framework.runtime_binding import DeploymentLifecycleRequest, ProjectionCursor
from ergasterion.ingestion.conformance import (
    REFERENCE_KEY_ID,
    FakeLifecycleSink,
    FakeScratchStore,
    MemoryStateStore,
    SimulatedCrash,
    build_capabilities,
    build_deployment,
    build_memory_ports,
    build_readiness,
    build_runtime_binding,
    contract_variant,
    exercise_all_operations,
    fixed_clock,
    load_vectors,
    memory_ports_factory,
    run_adapter_conformance,
    run_all,
)
from ergasterion.ingestion.ports import PORT_PROTOCOLS, PortSet
from ergasterion.ingestion.records import (
    PORT_OPERATION_ORDER,
    Attempt,
    AttemptLifecyclePayload,
    ContractLifecycleRequest,
    DeliveryManifest,
    DeliveryVisibilityIdentity,
    HeartbeatProjectionPayload,
    LifecycleEvent,
    LifecycleEventBatch,
    ManagedPayloadInput,
    OutboxEnqueue,
    OutboxFailureDisposition,
    OutboxFailureTransaction,
    PayloadDescriptor,
    ProjectionIntent,
    ProjectionIntentKind,
    ProjectionOutboxPayload,
    RemediationEvaluation,
    ReprocessingClaim,
    ScratchChunk,
    StateOutboxTransaction,
)
from ergasterion.ingestion.runtime import (
    LEASE_ITEM_LIMIT,
    PORT_FIELD_ORDER,
    IngestionRuntime,
    PortError,
    admit,
    admit_resources,
    canonical_digest,
    check_implementation_versions,
    check_plan_and_manifest,
    check_port_topology,
    check_readiness,
    digest_token,
    parse_utc_instant,
    scheduled_occurrences,
    utc_now_string,
)
from ergasterion.source_delivery import next_boundary_after

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
INGESTION_DIR = REPO_ROOT / "ergasterion" / "ingestion"
SCHEMA_VECTORS_PATH = REPO_ROOT / "tests" / "fixtures" / "bronze_schema_vectors.json"

PLAN_DIGEST = canonical_digest({"plan": "bronze-runtime-test"})
MANIFEST_DIGEST = canonical_digest({"manifest": "bronze-runtime-test"})
IMPLEMENTATION_VERSION = "1.0.0"


# --------------------------------------------------------------------------- fixtures / helpers

def _sample_contract() -> BronzeProductContract:
    document = json.loads(SCHEMA_VECTORS_PATH.read_text(encoding="utf-8"))
    for vector in document["positive"]:
        if vector["record"] == "BronzeProductContract":
            return BronzeProductContract.model_validate(vector["payload"])
    raise AssertionError("no BronzeProductContract positive vector found in the schema fixture")


def _managed_contract(
    publication_mode: PublicationPolicy | None = None, delivery_mode: DeliveryMode | None = None,
) -> BronzeProductContract:
    return contract_variant(
        _sample_contract(), integration_kind="managed", publication_mode=publication_mode,
        delivery_mode=delivery_mode,
    )


def _b64url_json(rows: list) -> str:
    import base64
    import json as _json

    return base64.urlsafe_b64encode(_json.dumps(rows).encode("utf-8")).decode("ascii").rstrip("=")


def _manifest(contract: BronzeProductContract, delivery_id: str, rows: list) -> DeliveryManifest:
    progress_claim = (
        {"kind": "opaque_batch"} if contract.delivery.progress.kind == "opaque_batch"
        else {"kind": "sequence", "high_watermark": str(len(rows)), "event_count": str(len(rows))}
    )
    return DeliveryManifest(
        schema="ergasterion.delivery-manifest/v1", logical_identity=contract.logical_identity,
        product_version=contract.product.product_version,
        contract_digest=canonical_digest(contract.model_dump(mode="json", by_alias=True)),
        delivery_id=delivery_id, batch_id=None, scheduled_boundary_at=None, effective_boundary_at=None,
        payload=PayloadDescriptor(media_type="application/x-ndjson", content_encoding="identity", codec_version=1,
                                   byte_length=str(len(json.dumps(rows))), sha256="0" * 64),
        frame_sequence_digest=None, progress_claim=progress_claim, declared_row_count=str(len(rows)),
        snapshot_attestation=None,
    )


def _submit(runtime, contract, state, handle: str, delivery_id: str, rows: list):
    input_record = ManagedPayloadInput(
        kind="managed_payload", manifest=_manifest(contract, delivery_id, rows), payload_handle=handle,
    )
    attempt, state = runtime.submit_managed(
        state, contract, PLAN_DIGEST, MANIFEST_DIGEST, canonical_digest({"run": delivery_id}), input_record,
    )
    return attempt, state, input_record


def _deliver(runtime, ports, contract, state, handle: str, delivery_id: str, rows: list, readiness=None):
    """Submit, land, validate and publish one delivery, returning the
    ``IngestionResult`` and the stream state the store now holds."""

    attempt, state, input_record = _submit(runtime, contract, state, handle, delivery_id, rows)
    receipt = ports.raw_store.preserve(input_record)
    visibility = DeliveryVisibilityIdentity(
        epoch="0", kind="delivery", id=digest_token(attempt.attempt_id, "delivery"),
    )
    attempt, state, materialized, validation = runtime.land_and_validate(
        attempt, state, contract, receipt, visibility,
        canonical_digest({"evaluation": delivery_id}), canonical_digest({"ruleset": delivery_id}),
    )
    result = runtime.publish(
        attempt, state, contract, materialized, validation, visibility, receipt,
        readiness or build_readiness(contract, MANIFEST_DIGEST),
    )
    return result, ports.state_store.status_query(contract.logical_identity).state


def _committed_attempt(contract: BronzeProductContract) -> Attempt:
    digest = canonical_digest(contract.model_dump(mode="json", by_alias=True))
    return Attempt(
        run_id=digest, attempt_id=digest, logical_identity=contract.logical_identity, claim_digest=digest,
        scheduled_boundary_at="2026-01-01T00:00:00.000000Z", attempt_ordinal=1, state="committed",
        block_phase=None, reason_code=None, execution_plan_digest=digest, runtime_manifest_digest=digest,
        state_revision="0",
    )


def _evaluation(contract: BronzeProductContract, name: str) -> RemediationEvaluation:
    digest = canonical_digest(contract.model_dump(mode="json", by_alias=True))
    return RemediationEvaluation(
        schema="ergasterion.remediation-evaluation/v1", original_claim_digest=digest, raw_receipt_digest=digest,
        target_contract_digest=digest, target_source_schema_digest=digest, target_published_schema_digest=digest,
        target_ruleset_digest=digest, execution_plan_digest=digest, root_visibility_epoch="0",
        remediation_evaluation_id=canonical_digest({"evaluation": name}),
    )


def _expect_error(code: str, action, message: str) -> None:
    try:
        action()
    except PortError as exc:
        assert exc.code == code, f"{message}: expected {code!r}, got {exc.code!r} ({exc.detail})"
    else:
        raise AssertionError(message)


# --------------------------------------------------------------------------- ports and conformance seam

def test_port_protocols_satisfied_structurally() -> None:
    contract = _sample_contract()
    ports, _state = build_memory_ports(contract.logical_identity)
    for field_name, protocol in PORT_PROTOCOLS.items():
        candidate = getattr(ports, field_name)
        assert isinstance(candidate, protocol), f"{type(candidate).__name__} does not satisfy {protocol.__name__}"
    assert tuple(PORT_PROTOCOLS) == PORT_FIELD_ORDER, "port slot order must match the IDL PortKind order"


def test_every_port_operation_is_reached() -> None:
    contract = _managed_contract()
    ports, state = build_memory_ports(contract.logical_identity, content_by_handle={"exercise": [{"key": "a", "accept": True}]})
    reached = exercise_all_operations(ports, state, contract, "exercise")
    assert set(reached) == set(PORT_OPERATION_ORDER), "coverage must report exactly the nine declared ports"
    for field_name, expected in PORT_OPERATION_ORDER.items():
        missing = tuple(operation for operation in expected if operation not in reached[field_name])
        assert not missing, f"port {field_name!r} never reached operation(s) {', '.join(missing)}"


def test_conformance_runner_accepts_implementations_explicitly() -> None:
    contract = _sample_contract()
    vector = {v["id"]: v for v in load_vectors()}["all-accepted-commits"]
    supplied: list[str] = []

    def factory(vector_data, resolved_contract, payload_handle):
        supplied.append(payload_handle)
        ports, state = memory_ports_factory(vector_data, resolved_contract, payload_handle)
        assert isinstance(ports, PortSet)
        return ports, state

    outcome = run_adapter_conformance(vector, contract, ports_factory=factory)
    assert outcome.passed, outcome.detail
    assert supplied, "the runner must obtain its ports from the caller's factory, not from a registry"


def test_adapter_conformance_vectors_all_pass() -> None:
    contract = _sample_contract()
    vectors = load_vectors()
    assert len(vectors) >= 15, "expected at least fifteen submission-family vectors"
    identifiers = {vector["id"] for vector in vectors}
    for required in (
        "all-accepted-commits", "mixed-rows-publish-valid-rows-commits",
        "mixed-rows-all-or-nothing-rejects-delivery", "all-rejected-fails-no-progress",
        "resubmission-replays-idempotently", "conflicting-claim-same-delivery-id-rejected",
        "progress-kind-mismatch-is-invalid-manifest", "contract-digest-mismatch-is-capability-mismatch",
        "target-failure-then-retry-commits-exactly-once", "target-failure-exhausts-retries-and-dead-letters",
        "permanent-landing-failure-fails-with-no-progress", "unready-published-schema-blocks-publication",
        "revoked-readiness-blocks-publication", "managed-payload-for-external-integration-rejected",
        "materialization-failure-fails-the-attempt-it-reached",
    ):
        assert required in identifiers, f"vector {required!r} is missing from the packaged vector set"
    outcomes = run_all(vectors, contract)
    failed = [o for o in outcomes if not o.passed]
    assert not failed, "\n".join(f"{o.vector_id}: {o.detail}" for o in failed)


# --------------------------------------------------------------------------- admission

def _binding_and_capabilities(contract, **capability_overrides):
    capabilities = build_capabilities(**capability_overrides)
    binding = build_runtime_binding(contract, capabilities, PLAN_DIGEST)
    return binding, capabilities


def test_admission_admits_the_reference_deployment() -> None:
    contract = _managed_contract()
    binding, capabilities = _binding_and_capabilities(contract)
    admission = admit(
        binding, build_deployment(contract, MANIFEST_DIGEST), capabilities,
        {name: IMPLEMENTATION_VERSION for name in PORT_FIELD_ORDER},
        build_readiness(contract, MANIFEST_DIGEST), contract, PLAN_DIGEST, MANIFEST_DIGEST,
        "2026-01-01T00:00:00.000000Z",
    )
    assert admission.port_order == PORT_FIELD_ORDER
    assert admission.aggregate_memory_bytes == 2 * 67108864, "aggregate memory is parallel attempts times validation bytes"
    assert admission.aggregate_scratch_bytes == 2 * 33554432, "aggregate scratch is parallel attempts times the reservation"


def test_topology_rejects_a_missing_port_and_an_undeclared_operation() -> None:
    contract = _managed_contract()
    binding, capabilities = _binding_and_capabilities(contract)
    partial = {name: capability for name, capability in capabilities.items() if name != "key_resolver"}
    _expect_error("capability_mismatch", lambda: check_port_topology(binding, partial),
                  "expected an unbound port to fail admission")

    trimmed = dict(capabilities)
    scratch = capabilities["scratch_store"]
    trimmed["scratch_store"] = scratch.model_copy(update={"operations": scratch.operations[:-1]})
    trimmed_binding = build_runtime_binding(contract, trimmed, PLAN_DIGEST)
    try:
        check_port_topology(trimmed_binding, trimmed)
    except PortError as exc:
        assert exc.code == "capability_mismatch"
        assert "operation" in exc.detail, exc.detail
    else:
        raise AssertionError("expected a port declaring fewer operations than it has to fail admission")


def test_implementation_version_mismatch_is_rejected() -> None:
    contract = _managed_contract()
    binding, _capabilities = _binding_and_capabilities(contract)
    check_implementation_versions(binding, {name: IMPLEMENTATION_VERSION for name in PORT_FIELD_ORDER})
    _expect_error("capability_mismatch",
                  lambda: check_implementation_versions(binding, {name: "2.0.0" for name in PORT_FIELD_ORDER}),
                  "expected an adapter running another implementation version to be rejected")


def test_plan_and_manifest_mismatch_are_rejected() -> None:
    contract = _managed_contract()
    binding, _capabilities = _binding_and_capabilities(contract)
    deployment = build_deployment(contract, MANIFEST_DIGEST)
    check_plan_and_manifest(binding, deployment, PLAN_DIGEST, MANIFEST_DIGEST)
    _expect_error("superseded_contract",
                  lambda: check_plan_and_manifest(binding, deployment, canonical_digest({"plan": "stale"}), MANIFEST_DIGEST),
                  "expected a stale execution plan to be a superseded contract")
    _expect_error("superseded_deployment",
                  lambda: check_plan_and_manifest(binding, deployment, PLAN_DIGEST, canonical_digest({"manifest": "stale"})),
                  "expected a stale runtime manifest to be a superseded deployment")


def test_schema_readiness_failures_are_rejected() -> None:
    contract = _managed_contract()
    digest = canonical_digest(contract.model_dump(mode="json", by_alias=True))
    observed_at = "2026-01-01T00:00:00.000000Z"
    check_readiness(build_readiness(contract, MANIFEST_DIGEST), digest, MANIFEST_DIGEST, observed_at)
    _expect_error("schema_invalid",
                  lambda: check_readiness(build_readiness(contract, MANIFEST_DIGEST, result=ReadinessResult.REJECTED),
                                          digest, MANIFEST_DIGEST, observed_at),
                  "expected a rejected readiness result to fail closed")
    _expect_error("schema_invalid",
                  lambda: check_readiness(build_readiness(contract, MANIFEST_DIGEST, revoked_at="2025-01-01T00:00:00.000000Z"),
                                          digest, MANIFEST_DIGEST, observed_at),
                  "expected a revoked readiness to fail closed")
    _expect_error("capability_mismatch",
                  lambda: check_readiness(build_readiness(contract, canonical_digest({"manifest": "other"})),
                                          digest, MANIFEST_DIGEST, observed_at),
                  "expected readiness verified against another manifest to be rejected")


def test_aggregate_memory_and_scratch_budgets_are_admitted_or_refused() -> None:
    contract = _managed_contract()
    binding, capabilities = _binding_and_capabilities(contract)
    assert admit_resources(binding, capabilities) == (2 * 67108864, 2 * 33554432)

    crowded = build_runtime_binding(contract, capabilities, PLAN_DIGEST, max_parallel_attempts=8)
    _expect_error("capacity_exceeded", lambda: admit_resources(crowded, capabilities),
                  "expected parallel attempts over the process memory ceiling to be refused")

    small_memory = build_capabilities(max_memory_bytes="1048576")
    small_memory_binding = build_runtime_binding(contract, small_memory, PLAN_DIGEST)
    _expect_error("capacity_exceeded", lambda: admit_resources(small_memory_binding, small_memory),
                  "expected aggregate memory over an adapter's own ceiling to be refused")

    small_scratch = build_capabilities(max_scratch_bytes="1024")
    small_scratch_binding = build_runtime_binding(contract, small_scratch, PLAN_DIGEST)
    _expect_error("capacity_exceeded", lambda: admit_resources(small_scratch_binding, small_scratch),
                  "expected aggregate scratch over the scratch store's ceiling to be refused")


# --------------------------------------------------------------------------- delivery modes and replay

def test_delivery_mode_validation() -> None:
    external = _sample_contract()
    ports, state = build_memory_ports(external.logical_identity, content_by_handle={"h": [{"key": "a"}]})
    runtime = IngestionRuntime(ports, fixed_clock())
    _expect_error("invalid_manifest", lambda: _submit(runtime, external, state, "h", "external-1", [{"key": "a"}]),
                  "expected a managed payload against an external integration to be rejected")

    snapshot = _managed_contract(delivery_mode=DeliveryMode.COMPLETE_SNAPSHOT)
    ports, state = build_memory_ports(snapshot.logical_identity, content_by_handle={"h": [{"key": "a"}]})
    runtime = IngestionRuntime(ports, fixed_clock())
    _expect_error("invalid_manifest", lambda: _submit(runtime, snapshot, state, "h", "snapshot-1", [{"key": "a"}]),
                  "expected a complete_snapshot delivery with no attestation to be rejected")


def test_claim_replay_is_idempotent_and_conflict_is_rejected() -> None:
    contract = _managed_contract()
    rows = [{"key": "a", "accept": True}]
    ports, state = build_memory_ports(contract.logical_identity, content_by_handle={"h": rows, "other": [{"key": "b"}]})
    runtime = IngestionRuntime(ports, fixed_clock())
    first, state, _input = _submit(runtime, contract, state, "h", "delivery-1", rows)
    replayed, state, _input = _submit(runtime, contract, state, "h", "delivery-1", rows)
    assert replayed.attempt_id == first.attempt_id, "a resubmitted identical claim must replay, not fork"

    _expect_error("claim_conflict",
                  lambda: _submit(runtime, contract, state, "other", "delivery-1", [{"key": "b"}]),
                  "expected the same delivery_id under a different claim digest to conflict")


def _reprocessing_claim(contract: BronzeProductContract, original_claim_digest: str, name: str) -> ReprocessingClaim:
    digest = canonical_digest(contract.model_dump(mode="json", by_alias=True))
    return ReprocessingClaim(
        schema="ergasterion.reprocessing-claim/v1", original_claim_digest=original_claim_digest,
        raw_receipt_digest=digest, target_product_version=contract.product.product_version,
        target_contract_digest=digest, target_source_schema_digest=digest,
        target_published_schema_digest=digest, target_ruleset_digest=digest, execution_plan_digest=digest,
        reprocessing_id=canonical_digest({"reprocess": name}),
    )


def test_attempt_scans_page_past_the_lease_item_limit() -> None:
    """A stream outlives one page. Once it holds more attempts than a single
    ``AttemptQuery`` returns, every admission rule that scans attempts must page
    to the end -- otherwise the conflicting claim, the prior reprocessing
    attempt and the ordinal that names a new attempt all fall off the end of
    page one and the runtime admits exactly the duplicate it exists to refuse."""

    contract = _managed_contract()
    count = LEASE_ITEM_LIMIT + 3
    content = {f"h{index}": [{"key": f"k{index}", "accept": True}] for index in range(count)}
    content["conflict"] = [{"key": "conflicting", "accept": True}]
    ports, state = build_memory_ports(contract.logical_identity, content_by_handle=content)
    runtime = IngestionRuntime(ports, fixed_clock())

    attempts = []
    for index in range(count):
        attempt, state, _input = _submit(
            runtime, contract, state, f"h{index}", f"delivery-{index}", content[f"h{index}"],
        )
        attempts.append(attempt)
    assert len({a.attempt_id for a in attempts}) == count, "attempt identifiers must not collide past one page"
    assert sorted(a.attempt_ordinal for a in attempts) == list(range(1, count + 1)), \
        "attempt ordinals must keep counting past one page rather than restarting"

    last_delivery_id = f"delivery-{count - 1}"
    _expect_error("claim_conflict",
                  lambda: _submit(runtime, contract, state, "conflict", last_delivery_id, content["conflict"]),
                  "expected a conflicting claim beyond the first page to still be seen")

    state = ports.state_store.status_query(contract.logical_identity).state
    claim = _reprocessing_claim(contract, attempts[0].claim_digest, "paged")
    first = runtime.reprocess_whole_delivery(state, claim, canonical_digest({"run": "paged-1"}))
    assert first.attempt_ordinal == count + 1, "a reprocessing attempt is ordered behind every existing attempt"
    state = ports.state_store.status_query(contract.logical_identity).state
    replayed = runtime.reprocess_whole_delivery(state, claim, canonical_digest({"run": "paged-1"}))
    assert replayed.attempt_id == first.attempt_id, \
        "the reprocessing attempt sits beyond page one, and must still be found there on replay"

    stored = ports.state_store._attempts
    assert len(stored) == count + 1, "replay must not fork a second attempt"
    assert len({a.attempt_id for a in stored.values()}) == len(stored)
    assert len({a.attempt_ordinal for a in stored.values()}) == len(stored)


def test_reprocessing_claim_replays_and_conflicts() -> None:
    contract = _managed_contract()
    ports, state = build_memory_ports(contract.logical_identity)
    runtime = IngestionRuntime(ports, fixed_clock())
    digest = canonical_digest(contract.model_dump(mode="json", by_alias=True))

    def claim(original_claim_digest: str) -> ReprocessingClaim:
        return _reprocessing_claim(contract, original_claim_digest, "once")

    first = runtime.reprocess_whole_delivery(state, claim(digest), canonical_digest({"run": "reprocess-1"}))
    state = ports.state_store.status_query(contract.logical_identity).state
    replayed = runtime.reprocess_whole_delivery(state, claim(digest), canonical_digest({"run": "reprocess-1"}))
    assert replayed.attempt_id == first.attempt_id, "an identical reprocessing claim must replay"

    other = canonical_digest({"claim": "another-delivery"})
    _expect_error("claim_conflict",
                  lambda: runtime.reprocess_whole_delivery(state, claim(other), canonical_digest({"run": "reprocess-2"})),
                  "expected the same reprocessing_id over another delivery to conflict")


def test_quarantine_release_restrictions() -> None:
    contract = _managed_contract()
    ports, state = build_memory_ports(contract.logical_identity)
    runtime = IngestionRuntime(ports, fixed_clock())
    digest = canonical_digest(contract.model_dump(mode="json", by_alias=True))
    attempt = _committed_attempt(contract)
    evaluation = _evaluation(contract, "quarantine-1")
    originating_ruleset = canonical_digest({"contract": digest})

    _expect_error("decision_conflict",
                  lambda: runtime.release_quarantine(attempt, state, contract, evaluation, (), digest, originating_ruleset),
                  "expected a release under the originating ruleset to be refused")

    decision = runtime.release_quarantine(attempt, state, contract, evaluation, (), digest, None)
    assert decision.kind.value == "released"

    _expect_error("release_conflict",
                  lambda: runtime.release_quarantine(attempt, state, contract, evaluation, (), digest, None),
                  "expected a second release of the same evaluation to be refused")


def test_scratch_store_capacity_sequencing_isolation_and_cleanup() -> None:
    store = FakeScratchStore()
    attempt_id = canonical_digest({"attempt": "scratch-1"})
    other_attempt_id = canonical_digest({"attempt": "scratch-2"})
    scope = store.create_scope(attempt_id, "8")
    store.write_sequential(attempt_id, ScratchChunk(scope_id=scope.scope_id, sequence="0", bytes_base64url=_b64url_json([1, 2])))

    _expect_error("scope_owner_mismatch",
                  lambda: store.write_sequential(other_attempt_id, ScratchChunk(
                      scope_id=scope.scope_id, sequence="1", bytes_base64url=_b64url_json([3]))),
                  "expected a write from a non-owning attempt to be refused")
    _expect_error("sequence_conflict",
                  lambda: store.write_sequential(attempt_id, ScratchChunk(
                      scope_id=scope.scope_id, sequence="5", bytes_base64url=_b64url_json([3]))),
                  "expected an out-of-order sequence to be refused")
    _expect_error("capacity_exceeded",
                  lambda: store.write_sequential(attempt_id, ScratchChunk(
                      scope_id=scope.scope_id, sequence="1", bytes_base64url=_b64url_json(list(range(100))))),
                  "expected a write past the scope's declared capacity to be refused")
    _expect_error("scope_open",
                  lambda: store.read_sequential(attempt_id, scope.scope_id, "0", "1024"),
                  "expected a read of an open scope to be refused")

    other_scope = store.create_scope(other_attempt_id, "1024")
    removed = store.cleanup_orphans((attempt_id,), 8)
    assert other_scope.scope_id in removed, "cleanup must remove a scope whose owning attempt is no longer active"
    assert scope.scope_id not in removed, "cleanup must leave an active attempt's scope alone"


# --------------------------------------------------------------------------- progress, order and failure

def test_rejected_delivery_never_stages_progress_or_outbox() -> None:
    contract = _managed_contract(publication_mode=PublicationPolicy.ALL_OR_NOTHING)
    rows = [{"key": "a", "accept": False}, {"key": "b", "accept": False}]
    ports, state = build_memory_ports(contract.logical_identity, content_by_handle={"h": rows})
    runtime = IngestionRuntime(ports, fixed_clock())
    result, _state = _deliver(runtime, ports, contract, state, "h", "progress-check", rows)

    assert result.attempt.state.value == "failed"
    store: MemoryStateStore = ports.state_store
    assert store.stream_state.accepted_progress == {}, "a failed attempt must never advance accepted_progress"
    assert store._outbox == {}, "a failed attempt must never stage a projection outbox entry"


def test_permanent_landing_failure_never_stages_progress() -> None:
    contract = _managed_contract()
    rows = [{"key": "a", "accept": True}]
    ports, state = build_memory_ports(contract.logical_identity, content_by_handle={"h": rows},
                                       finish_prepare_fault="integrity_error")
    runtime = IngestionRuntime(ports, fixed_clock())
    _expect_error("integrity_error", lambda: _deliver(runtime, ports, contract, state, "h", "permanent-1", rows),
                  "expected a permanent landing failure to surface its error code")

    store: MemoryStateStore = ports.state_store
    assert store.stream_state.accepted_progress == {}, "a permanent pre-intent failure must not advance progress"
    assert store._outbox == {}, "a permanent pre-intent failure must not stage an outbox entry"
    latest = store.status_query(contract.logical_identity).latest_attempt
    assert latest is not None and latest.state.value == "failed"


def test_materialization_failure_fails_the_attempt_it_actually_reached() -> None:
    """A port failure after the ``materializing`` transition must fail the
    attempt against the state the store really holds. Failing it against the
    revision landing started from would be refused by the store's own
    compare-and-swap, and the caller would see ``stale_revision`` instead of the
    code the landing adapter raised."""

    contract = _managed_contract()
    rows = [{"key": "a", "accept": True}]
    ports, state = build_memory_ports(contract.logical_identity, content_by_handle={"h": rows},
                                       finish_materialization_fault="codec_error")
    runtime = IngestionRuntime(ports, fixed_clock())
    _expect_error("codec_error", lambda: _deliver(runtime, ports, contract, state, "h", "materialize-1", rows),
                  "expected the injected materialization failure to survive the failure path")

    store: MemoryStateStore = ports.state_store
    latest = store.status_query(contract.logical_identity).latest_attempt
    assert latest is not None and latest.state.value == "failed", \
        "a failure after the materializing transition must still drive the attempt to failed"
    assert latest.reason_code == "codec_error", "the failed attempt must record the code the port raised"
    assert store.stream_state.accepted_progress == {}, "a pre-intent failure must not advance progress"
    assert store._outbox == {}, "a pre-intent failure must not stage an outbox entry"
    assert [event.event_type.value for event in store.events][-2:] == ["materializing", "failed"], \
        "the lifecycle envelopes must show the attempt failing from the state it reached"


def _stage_heartbeat(store, contract, state, revision: str, now: str):
    """Stage one heartbeat projection intent at ``revision`` directly on the
    state store, returning the outbox identifier, the intent and the state.

    Staging by hand is what lets a test place two entries at known revisions
    with known retry counts; the runtime's own staging paths never produce that
    combination on purpose."""

    contract_digest = canonical_digest(contract.model_dump(mode="json", by_alias=True))
    payload = HeartbeatProjectionPayload(kind="heartbeat", heartbeat_at=now, evaluated_through_at=now,
                                          prior_committed_at=None)
    payload_digest = canonical_digest(payload.model_dump(mode="json", by_alias=True))
    intent = ProjectionIntent(
        schema="ergasterion.projection-intent/v1", logical_identity=contract.logical_identity,
        contract_digest=contract_digest, projection_target="bronze", projection_revision=revision,
        originating_state_revision=state.state_revision, kind=ProjectionIntentKind.HEARTBEAT,
        execution_plan_digest=PLAN_DIGEST, runtime_manifest_digest=MANIFEST_DIGEST, payload=payload,
        payload_digest=payload_digest,
        projection_intent_digest=canonical_digest({"payload": payload_digest, "revision": revision}),
    )
    outbox_id = canonical_digest({"intent": intent.projection_intent_digest})
    state = store.state_transaction(StateOutboxTransaction(
        expected_state_revision=state.state_revision,
        next_state=state.model_copy(update={"required_projection_revision": revision}),
        attempt_updates=(), deployment_update=None, projection_confirmation=None,
        enqueue=(OutboxEnqueue(outbox_id=outbox_id,
                                payload=ProjectionOutboxPayload(entry_kind="projection", intent=intent),
                                payload_digest=intent.projection_intent_digest, next_not_before=now),),
        complete=(),
    ))
    return outbox_id, intent, state


def test_run_due_replays_pending_entries_in_staged_revision_order() -> None:
    """Two entries due at the same instant must reach the target in the order
    their revisions were staged, not in the order of their retry counts. The
    entry staged first here carries the *later* revision and has been dispatched
    once; the entry staged second carries the earlier revision and has already
    failed twice, so lease order and dispatch order both disagree with staging
    order."""

    contract = _managed_contract()
    ports, state = build_memory_ports(contract.logical_identity)
    store: MemoryStateStore = ports.state_store
    clock = fixed_clock()
    runtime = IngestionRuntime(ports, clock)
    now = clock.now()

    later_id, later_intent, state = _stage_heartbeat(store, contract, state, "2", now)
    earlier_id, earlier_intent, state = _stage_heartbeat(store, contract, state, "1", now)
    for _ in range(2):
        state = store.fail_outbox(OutboxFailureTransaction(
            expected_state_revision=state.state_revision, next_state=state, attempt_updates=(),
            outbox_id=earlier_id, payload_digest=earlier_intent.projection_intent_digest,
            lease_owner=runtime.lease_owner, failure_observed_at=now, reason_code="target_unavailable",
            disposition=OutboxFailureDisposition.RETRYABLE, next_not_before=now,
        ))
    assert store._outbox[earlier_id].dispatch_ordinal > store._outbox[later_id].dispatch_ordinal, \
        "the earlier revision must carry the higher retry count for this test to mean anything"
    assert list(store._outbox) == [later_id, earlier_id], "the later revision must also be leased first"

    results = runtime.run_due(contract.logical_identity, now, 10)
    assert len(results) == 2, "both due entries must be replayed"
    applied = [result.projection_confirmation for result in results]
    assert all(confirmation is not None for confirmation in applied), \
        "replaying out of staged order would be refused as a projection gap"
    assert [confirmation.projection_revision for confirmation in applied] == ["1", "2"], \
        "pending entries must apply in the order their projection revisions were staged"
    assert ports.projection_publisher.cursor_revision == 2


def test_commit_blocked_reserves_progress_rejects_successors_and_resumes_once() -> None:
    contract = _managed_contract()
    rows = [{"key": "a", "accept": True}]
    successor_rows = [{"key": "b", "accept": True}]
    ports, state = build_memory_ports(
        contract.logical_identity, content_by_handle={"h": rows, "successor": successor_rows}, fail_first_n=1,
    )
    clock = fixed_clock()
    runtime = IngestionRuntime(ports, clock)
    result, state = _deliver(runtime, ports, contract, state, "h", "blocked-1", rows)

    assert result.attempt.state.value == "commit_blocked"
    assert result.attempt.block_phase.value == "projection_blocked"
    store: MemoryStateStore = ports.state_store
    assert store.stream_state.accepted_progress != {}, "post-intent progress must be reserved once the intent lands"
    assert ports.projection_publisher.cursor_revision == 0, "reserved progress must stay invisible at the target"
    blocked_entry = next(iter(store._outbox.values()))
    assert blocked_entry.status.value == "retryable"

    _expect_error("inflight_attempt",
                  lambda: _submit(runtime, contract, state, "successor", "blocked-2", successor_rows),
                  "expected a conflicting successor to be refused behind a commit_blocked publication")

    replayed, state, _input = _submit(runtime, contract, state, "h", "blocked-1", rows)
    assert replayed.attempt_id == result.attempt.attempt_id, "the blocked publication's own claim must still replay"

    resumed = runtime.run_due(contract.logical_identity, clock.now(), int(contract.delivery.retry.max_attempts))
    assert len(resumed) == 1 and resumed[0].attempt.state.value == "committed"
    assert ports.projection_publisher.cursor_revision == 1, "the resumed publication must apply exactly once"
    assert len(store._confirmations) == 1, "one publication yields exactly one confirmation"

    state = store.status_query(contract.logical_identity).state
    later, state, _input = _submit(runtime, contract, state, "successor", "blocked-2", successor_rows)
    assert later.attempt_id != result.attempt.attempt_id, "a successor is admitted once the blockage resolves"


def test_crash_at_outbox_completion_resumes_the_same_publication() -> None:
    contract = _managed_contract()
    rows = [{"key": "a", "accept": True}]
    ports, state = build_memory_ports(contract.logical_identity, content_by_handle={"h": rows},
                                       crash_seams=frozenset({"outbox_completion"}))
    clock = fixed_clock()
    try:
        _deliver(IngestionRuntime(ports, clock), ports, contract, state, "h", "crash-1", rows)
    except SimulatedCrash as crash:
        assert crash.seam == "outbox_completion"
    else:
        raise AssertionError("expected the outbox completion seam to lose the process")

    store: MemoryStateStore = ports.state_store
    assert store.stream_state.accepted_progress != {}, "the intent transaction committed before the crash"
    assert not store._confirmations, "no confirmation was durable when the process died"
    assert next(iter(store._outbox.values())).status.value == "pending", "the outbox entry never settled"

    resumed = IngestionRuntime(ports, clock).run_due(
        contract.logical_identity, clock.now(), int(contract.delivery.retry.max_attempts),
    )
    assert len(resumed) == 1 and resumed[0].attempt.state.value == "committed"
    assert ports.projection_publisher.cursor_revision == 1, "resumption must not apply the intent a second time"
    assert len(store._confirmations) == 1
    assert next(iter(store._outbox.values())).status.value == "complete", "resumption settles the outbox entry"


def test_crash_before_remediation_cas_is_retried() -> None:
    contract = _managed_contract()
    ports, state = build_memory_ports(contract.logical_identity, crash_seams=frozenset({"remediation_cas_before"}))
    runtime = IngestionRuntime(ports, fixed_clock())
    digest = canonical_digest(contract.model_dump(mode="json", by_alias=True))
    attempt = _committed_attempt(contract)
    evaluation = _evaluation(contract, "crash-before")

    try:
        runtime.release_quarantine(attempt, state, contract, evaluation, (), digest, None)
    except SimulatedCrash as crash:
        assert crash.seam == "remediation_cas_before"
    else:
        raise AssertionError("expected the pre-compare-and-swap seam to lose the process")

    assert not ports.remediation_repository._released_evaluations, "nothing was released before the crash"
    decision = runtime.release_quarantine(attempt, state, contract, evaluation, (), digest, None)
    assert decision.kind.value == "released", "a crash before the compare-and-swap is recovered by retrying"


def test_crash_after_remediation_cas_is_resumed_not_re_decided() -> None:
    contract = _managed_contract()
    ports, state = build_memory_ports(contract.logical_identity, crash_seams=frozenset({"remediation_cas_after"}))
    runtime = IngestionRuntime(ports, fixed_clock())
    digest = canonical_digest(contract.model_dump(mode="json", by_alias=True))
    attempt = _committed_attempt(contract)
    evaluation = _evaluation(contract, "crash-after")

    try:
        runtime.release_quarantine(attempt, state, contract, evaluation, (), digest, None)
    except SimulatedCrash as crash:
        assert crash.seam == "remediation_cas_after"
    else:
        raise AssertionError("expected the post-compare-and-swap seam to lose the process")

    _expect_error("release_conflict",
                  lambda: runtime.release_quarantine(attempt, state, contract, evaluation, (), digest, None),
                  "expected retrying a release that already happened to be refused")

    resumed = IngestionRuntime(ports, fixed_clock()).resume_release(attempt, state, contract, evaluation, digest, 0)
    assert resumed.kind.value == "released"
    assert len(ports.remediation_repository._released_evaluations) == 1, "the evaluation released exactly once"
    checkpointed = ports.state_store._attempts[attempt.attempt_id]
    assert checkpointed.remediation_commit_checkpoint is not None, "resumption writes the missing checkpoint"


def test_crash_at_remediation_checkpoint_is_resumed() -> None:
    contract = _managed_contract()
    ports, state = build_memory_ports(contract.logical_identity, crash_seams=frozenset({"remediation_checkpoint"}))
    runtime = IngestionRuntime(ports, fixed_clock())
    digest = canonical_digest(contract.model_dump(mode="json", by_alias=True))
    attempt = _committed_attempt(contract)
    evaluation = _evaluation(contract, "crash-checkpoint")

    try:
        runtime.release_quarantine(attempt, state, contract, evaluation, (), digest, None)
    except SimulatedCrash as crash:
        assert crash.seam == "remediation_checkpoint"
    else:
        raise AssertionError("expected the checkpoint seam to lose the process")

    stored = ports.state_store._attempts.get(attempt.attempt_id)
    assert stored is None or stored.remediation_commit_checkpoint is None, \
        "the checkpoint never reached the state store"

    state = ports.state_store.status_query(contract.logical_identity).state
    resumed = IngestionRuntime(ports, fixed_clock()).resume_release(attempt, state, contract, evaluation, digest, 0)
    assert resumed.kind.value == "released"
    assert ports.state_store._attempts[attempt.attempt_id].remediation_commit_checkpoint is not None


def test_resume_release_without_a_recorded_decision_is_refused() -> None:
    contract = _managed_contract()
    ports, state = build_memory_ports(contract.logical_identity)
    runtime = IngestionRuntime(ports, fixed_clock())
    digest = canonical_digest(contract.model_dump(mode="json", by_alias=True))
    _expect_error("not_found",
                  lambda: runtime.resume_release(_committed_attempt(contract), state, contract,
                                                  _evaluation(contract, "never-released"), digest, 0),
                  "expected resumption with nothing recorded to be refused rather than invented")


# --------------------------------------------------------------------------- scheduled occurrences

def _occurrence_window(contract: BronzeProductContract, minimum: int = 3):
    """The first boundary a never-evaluated stream sees, plus a later instant at
    which at least ``minimum`` further mandatory occurrences are due."""

    schedule = contract.delivery.schedule
    start = utc_now_string(parse_utc_instant(contract.delivery.schedule.anchor_at)
                            if schedule.kind == "interval" else datetime(2026, 1, 1))
    first = scheduled_occurrences(contract, None, start, 4)
    assert first, "the contract's schedule must place a current boundary"
    cursor = parse_utc_instant(first[0])
    for _ in range(64):
        cursor = next_boundary_after(schedule, cursor)
        now = utc_now_string(cursor + timedelta(seconds=1))
        occurrences = scheduled_occurrences(contract, first[0], now, 16)
        if len(occurrences) >= minimum:
            return first[0], now, occurrences
    raise AssertionError("the contract's schedule never yields enough occurrences to prove catch-up")


def test_trusted_clock_catch_up_evaluates_every_occurrence_in_order() -> None:
    contract = _managed_contract()
    since, now, expected = _occurrence_window(contract)
    assert list(expected) == sorted(expected), "occurrences must come back in ascending boundary order"

    ports, state = build_memory_ports(contract.logical_identity)
    clock = fixed_clock(parse_utc_instant(now))
    runtime = IngestionRuntime(ports, clock)
    state, projected = runtime.run_scheduled(contract, state, now, since, 16)
    assert list(projected) == list(expected), "a clock that jumps forward must catch up on every occurrence in order"
    assert runtime.last_evaluated_occurrence(contract.logical_identity) == expected[-1]
    assert ports.projection_publisher.cursor_revision == len(expected), "each occurrence projects exactly once"


def test_no_failure_skips_a_mandatory_occurrence() -> None:
    contract = _managed_contract()
    since, now, expected = _occurrence_window(contract)
    ports, state = build_memory_ports(contract.logical_identity, fail_first_n=1)
    clock = fixed_clock(parse_utc_instant(now))
    runtime = IngestionRuntime(ports, clock)
    max_attempts = int(contract.delivery.retry.max_attempts)

    state, projected = runtime.run_scheduled(contract, state, now, since, 16)
    assert projected == (), "the occurrence whose projection failed must not be recorded as evaluated"
    assert runtime.last_evaluated_occurrence(contract.logical_identity) is None

    resumed = runtime.run_due(contract.logical_identity, now, max_attempts)
    assert len(resumed) == 1 and resumed[0].projection_confirmation is not None
    assert runtime.last_evaluated_occurrence(contract.logical_identity) == expected[0], \
        "the delayed occurrence is the one that resumes, not the latest one"

    state = ports.state_store.status_query(contract.logical_identity).state
    state, remaining = runtime.run_scheduled(contract, state, now, expected[0], 16)
    assert list(remaining) == list(expected[1:]), "the occurrences behind the failure are evaluated, never skipped"
    assert runtime.last_evaluated_occurrence(contract.logical_identity) == expected[-1]


def test_a_scheduled_occurrence_waits_behind_a_commit_blocked_publication() -> None:
    """A scheduled occurrence raised while a publication is ``commit_blocked``
    must not reserve the revision behind the blocked one. Staging it there would
    either apply out of order at the target or, once the target refused the gap,
    spend the occurrence's own retries and dead-letter it for a failure that was
    never its own -- which is how a mandatory occurrence gets skipped."""

    contract = _managed_contract()
    since, now, expected = _occurrence_window(contract)
    rows = [{"key": "a", "accept": True}]
    ports, state = build_memory_ports(contract.logical_identity, content_by_handle={"h": rows}, fail_first_n=1)
    clock = fixed_clock(parse_utc_instant(now))
    runtime = IngestionRuntime(ports, clock)
    max_attempts = int(contract.delivery.retry.max_attempts)

    result, state = _deliver(runtime, ports, contract, state, "h", "blocked-occurrence", rows)
    assert result.attempt.state.value == "commit_blocked"
    store: MemoryStateStore = ports.state_store
    assert len(store._outbox) == 1, "the blocked publication is the only staged entry"

    state, projected = runtime.run_scheduled(contract, state, now, since, 16)
    assert projected == (), "no occurrence may be projected while an earlier revision is still pending"
    assert len(store._outbox) == 1, "no occurrence entry may be staged ahead of the pending publication"
    assert [entry.status.value for entry in store._outbox.values()] == ["retryable"], \
        "a waiting occurrence must not be dead-lettered for the publication's failure"
    assert ports.projection_publisher.cursor_revision == 0, "nothing reached the target out of order"

    resumed = runtime.run_due(contract.logical_identity, now, max_attempts)
    assert len(resumed) == 1 and resumed[0].attempt.state.value == "committed", \
        "the blocked publication drains first"

    state = store.status_query(contract.logical_identity).state
    state, projected = runtime.run_scheduled(contract, state, now, since, 16)
    assert list(projected) == list(expected), "every waiting occurrence is evaluated once the blockage drains"
    assert runtime.last_evaluated_occurrence(contract.logical_identity) == expected[-1]


def test_a_scheduled_occurrence_waits_behind_an_exhausted_publication() -> None:
    """A scheduled occurrence raised after a publication has exhausted retries
    must not reserve the revision behind the dead-lettered one.
    ``incomplete_outbox_count`` no longer reports work -- a store that counts
    only pending, leased and retryable entries treats the stream as idle -- so
    the gate is the publisher cursor: while ``required_projection_revision`` is
    still ahead of it, staging would enqueue at required+1, the target would
    refuse the gap, and the occurrence would burn its own retries for
    ``projection_gap``."""

    contract = _managed_contract()
    since, now, expected = _occurrence_window(contract)
    rows = [{"key": "a", "accept": True}]
    ports, state = build_memory_ports(contract.logical_identity, content_by_handle={"h": rows}, fail_first_n=99)
    clock = fixed_clock(parse_utc_instant(now))
    runtime = IngestionRuntime(ports, clock)
    max_attempts = int(contract.delivery.retry.max_attempts)

    result, state = _deliver(runtime, ports, contract, state, "h", "exhausted-occurrence", rows)
    assert result.attempt.state.value == "commit_blocked"
    directive = result.retry_directive
    while directive is not None and not directive.exhausted:
        resumed = runtime.run_due(contract.logical_identity, now, max_attempts)
        assert resumed, "the blocked publication must stay due until it exhausts"
        directive = resumed[-1].retry_directive
    assert directive is not None and directive.exhausted

    store: MemoryStateStore = ports.state_store
    status = store.status_query(contract.logical_identity)
    assert int(status.incomplete_outbox_count) == 0, \
        "a dead-lettered publication must look idle to incomplete_outbox_count"
    assert [entry.status.value for entry in store._outbox.values()] == ["dead_letter"]
    assert int(status.state.required_projection_revision) > ports.projection_publisher.cursor_revision, \
        "the reserved revision must still sit ahead of the applied publisher cursor"

    state, projected = runtime.run_scheduled(contract, state, now, since, 16)
    assert projected == (), "no occurrence may be projected into the unapplied gap"
    assert len(store._outbox) == 1, "no occurrence entry may be staged ahead of the dead-lettered publication"
    assert [entry.status.value for entry in store._outbox.values()] == ["dead_letter"], \
        "a waiting occurrence must not be dead-lettered for projection_gap"
    assert ports.projection_publisher.cursor_revision == 0, "nothing reached the target out of order"
    assert runtime.last_evaluated_occurrence(contract.logical_identity) is None
    assert runtime.run_due(contract.logical_identity, now, max_attempts) == (), \
        "an exhausted publication must not be leased, and no occurrence may have been staged behind it"
    assert expected, "the window must hold occurrences that stayed unevaluated"


def test_a_never_evaluated_stream_does_not_backfill_its_history() -> None:
    contract = _managed_contract()
    since, now, expected = _occurrence_window(contract)
    assert len(scheduled_occurrences(contract, None, now, 16)) == 1, \
        "a stream with no evaluated occurrence evaluates only the current boundary"
    assert len(expected) >= 3, "the window must hold several occurrences for catch-up to mean anything"


# --------------------------------------------------------------------------- order and concurrency

def test_projection_publisher_rejects_a_revision_gap() -> None:
    contract = _sample_contract()
    ports, stream_state = build_memory_ports(contract.logical_identity)
    digest = canonical_digest(contract.model_dump(mode="json", by_alias=True))
    payload = {"kind": "heartbeat", "heartbeat_at": "2026-01-01T00:00:00.000000Z",
               "evaluated_through_at": "2026-01-01T00:00:00.000000Z", "prior_committed_at": None}
    payload_digest = canonical_digest(payload)
    intent = ProjectionIntent(
        schema="ergasterion.projection-intent/v1", logical_identity=contract.logical_identity, contract_digest=digest,
        projection_target="bronze", projection_revision="5",  # the cursor is at 0 and expects 1
        originating_state_revision=stream_state.state_revision, kind="heartbeat",
        execution_plan_digest=digest, runtime_manifest_digest=digest, payload=payload,
        payload_digest=payload_digest, projection_intent_digest=canonical_digest({"payload": payload_digest, "rev": "5"}),
    )
    _expect_error("projection_gap", lambda: ports.projection_publisher.apply_gap_ordered(intent),
                  "expected a projection revision gap to be refused")


def test_lifecycle_envelopes_are_gap_free_and_ordered() -> None:
    contract = _managed_contract()
    rows = [{"key": "a", "accept": True}]
    ports, state = build_memory_ports(contract.logical_identity, content_by_handle={"h": rows})
    runtime = IngestionRuntime(ports, fixed_clock())
    result, _state = _deliver(runtime, ports, contract, state, "h", "envelopes-1", rows)
    assert result.attempt.state.value == "committed"

    events = ports.state_store.events
    ordinals = [int(event.event_ordinal) for event in events]
    assert ordinals == sorted(ordinals), "lifecycle envelopes must be projected in ordinal order"
    for previous, following in zip(ordinals, ordinals[1:]):
        assert following - previous <= 1, f"ordinal {following} skips past {previous}"
    assert [event.event_type.value for event in events][:3] == ["received", "preparing", "materializing"]

    sink = FakeLifecycleSink()
    template = events[0]
    sink.project_events(LifecycleEventBatch(events=(template,), max_items=1, bytes_supplied="0"))
    skipped = template.model_copy(update={
        "event_id": canonical_digest({"event": "skipped"}),
        "event_ordinal": str(int(template.event_ordinal) + 4),
    })
    _expect_error("event_conflict",
                  lambda: sink.project_events(LifecycleEventBatch(events=(skipped,), max_items=1, bytes_supplied="0")),
                  "expected a skipped lifecycle ordinal to be refused")


def test_contract_lifecycle_cas_and_candidate_activation() -> None:
    contract = _managed_contract()
    ports, stream_state = build_memory_ports(contract.logical_identity)
    digest = canonical_digest(contract.model_dump(mode="json", by_alias=True))

    def request(action: str, expected_revision: str, subject: BronzeProductContract = contract):
        return ContractLifecycleRequest(
            schema="ergasterion.contract-lifecycle-request/v1", action=action,
            expected_state_revision=expected_revision, expected_deployment_revision=None,
            contract=subject, migration=None, permit_pre_intent_fence=False,
        )

    _expect_error("contract_conflict", lambda: ports.state_store.contract_lifecycle(request("activate", "0")),
                  "expected activation with no registered candidate to be refused")

    registered = ports.state_store.contract_lifecycle(request("register", "0"))
    assert registered.state.active_contract_digest is None, "registration records a candidate, it does not activate it"
    _expect_error("stale_revision", lambda: ports.state_store.contract_lifecycle(request("register", "0")),
                  "expected a second concurrent request at the same revision to be refused")

    other = contract_variant(contract, publication_mode=PublicationPolicy.PUBLISH_VALID_ROWS)
    _expect_error("contract_conflict",
                  lambda: ports.state_store.contract_lifecycle(
                      request("activate", registered.state.state_revision, other)),
                  "expected activating a contract other than the registered candidate to be refused")

    activated = ports.state_store.contract_lifecycle(request("activate", registered.state.state_revision))
    assert activated.state.active_contract_digest == digest, "activation promotes exactly the registered candidate"


def test_deployment_lifecycle_cas_candidate_activation_and_retirement() -> None:
    contract = _managed_contract()
    ports, stream_state = build_memory_ports(contract.logical_identity)
    first_manifest = canonical_digest({"manifest": "one"})
    second_manifest = canonical_digest({"manifest": "two"})
    state = stream_state
    revision = "0"

    def request(action: str, candidate: str, expected_revision: str, expected_deployment_revision: str):
        return DeploymentLifecycleRequest(
            schema="ergasterion.deployment-lifecycle-request/v1", action=action,
            expected_state_revision=expected_revision, expected_deployment_revision=expected_deployment_revision,
            deployment=build_deployment(contract, candidate, candidate_manifest_digest=candidate),
            readiness=build_readiness(contract, candidate),
            catchup_cursor=ProjectionCursor(logical_identity=contract.logical_identity, projection_target="bronze",
                                             projection_revision="0", projection_intent_digest=None),
            permit_pre_intent_fence=False,
        )

    registered = ports.state_store.deployment_lifecycle(request("register", first_manifest, state.state_revision, revision))
    _expect_error("stale_revision",
                  lambda: ports.state_store.deployment_lifecycle(request("register", first_manifest, state.state_revision, revision)),
                  "expected a second concurrent deployment request at the same revisions to be refused")
    state, revision = registered.state, registered.deployment.deployment_revision
    assert registered.deployment.active_manifest_digest is None, "registration records a candidate manifest only"

    activated = ports.state_store.deployment_lifecycle(request("activate", first_manifest, state.state_revision, revision))
    state, revision = activated.state, activated.deployment.deployment_revision
    assert activated.deployment.active_manifest_digest == first_manifest

    registered = ports.state_store.deployment_lifecycle(request("register", second_manifest, state.state_revision, revision))
    state, revision = registered.state, registered.deployment.deployment_revision
    assert registered.deployment.active_manifest_digest == first_manifest, "registering a candidate leaves the active manifest"

    activated = ports.state_store.deployment_lifecycle(request("activate", second_manifest, state.state_revision, revision))
    assert activated.deployment.active_manifest_digest == second_manifest
    assert first_manifest in activated.deployment.retired_manifest_digests, "activation retires the manifest it replaced"


def test_unready_deployment_is_refused() -> None:
    contract = _managed_contract()
    ports, state = build_memory_ports(contract.logical_identity)
    request = DeploymentLifecycleRequest(
        schema="ergasterion.deployment-lifecycle-request/v1", action="register",
        expected_state_revision=state.state_revision, expected_deployment_revision="0",
        deployment=build_deployment(contract, MANIFEST_DIGEST, candidate_manifest_digest=MANIFEST_DIGEST),
        readiness=build_readiness(contract, MANIFEST_DIGEST, result=ReadinessResult.REJECTED),
        catchup_cursor=ProjectionCursor(logical_identity=contract.logical_identity, projection_target="bronze",
                                         projection_revision="0", projection_intent_digest=None),
        permit_pre_intent_fence=False,
    )
    _expect_error("schema_invalid", lambda: ports.state_store.deployment_lifecycle(request),
                  "expected a deployment whose readiness is not ready to be refused")


def test_key_resolver_refuses_an_unknown_key() -> None:
    contract = _managed_contract()
    ports, _state = build_memory_ports(contract.logical_identity)
    assert ports.key_resolver.resolve_verification_key(REFERENCE_KEY_ID).key_id == REFERENCE_KEY_ID
    _expect_error("key_not_found", lambda: ports.key_resolver.resolve_verification_key("absent-key"),
                  "expected an unknown key identifier to be refused")


# --------------------------------------------------------------------------- persistence neutrality

BANNED_MODULES = ("sqlite3", "duckdb", "dbt", "airflow", "dagster", "prefect", "luigi", "sqlalchemy")


def _imported_module_names(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
            names.add(node.module)
    return names


def test_static_imports_prove_no_persistence_or_orchestrator_dependency() -> None:
    modules = ("ports.py", "runtime.py", "conformance.py", "records.py")
    pending = [INGESTION_DIR / name for name in modules]
    seen: set[Path] = set()
    walked = 0
    while pending:
        path = pending.pop()
        if path in seen or not path.exists():
            continue
        seen.add(path)
        walked += 1
        for module in _imported_module_names(path):
            root = module.split(".")[0]
            assert root not in BANNED_MODULES, f"{path.name} imports {module!r}"
            if root == "ergasterion":
                candidate = REPO_ROOT / Path(*module.split(".")).with_suffix(".py")
                package_init = REPO_ROOT / Path(*module.split(".")) / "__init__.py"
                pending.extend(p for p in (candidate, package_init) if p.exists())
    assert walked > len(modules), "the import closure must reach beyond the four ingestion modules"


def test_importing_the_runtime_loads_no_persistence_or_orchestrator_module() -> None:
    program = (
        "import sys\n"
        "import ergasterion.ingestion.ports, ergasterion.ingestion.runtime, ergasterion.ingestion.conformance\n"
        f"banned = {BANNED_MODULES!r}\n"
        "loaded = sorted(name for name in sys.modules if name.split('.')[0] in banned)\n"
        "print(','.join(loaded))\n"
    )
    completed = subprocess.run(
        [sys.executable, "-c", program], cwd=str(REPO_ROOT), capture_output=True, text=True, check=False,
    )
    assert completed.returncode == 0, completed.stderr
    assert completed.stdout.strip() == "", f"importing the runtime loaded {completed.stdout.strip()!r}"


TESTS = [
    test_port_protocols_satisfied_structurally,
    test_every_port_operation_is_reached,
    test_conformance_runner_accepts_implementations_explicitly,
    test_adapter_conformance_vectors_all_pass,
    test_admission_admits_the_reference_deployment,
    test_topology_rejects_a_missing_port_and_an_undeclared_operation,
    test_implementation_version_mismatch_is_rejected,
    test_plan_and_manifest_mismatch_are_rejected,
    test_schema_readiness_failures_are_rejected,
    test_aggregate_memory_and_scratch_budgets_are_admitted_or_refused,
    test_delivery_mode_validation,
    test_claim_replay_is_idempotent_and_conflict_is_rejected,
    test_attempt_scans_page_past_the_lease_item_limit,
    test_reprocessing_claim_replays_and_conflicts,
    test_quarantine_release_restrictions,
    test_scratch_store_capacity_sequencing_isolation_and_cleanup,
    test_rejected_delivery_never_stages_progress_or_outbox,
    test_permanent_landing_failure_never_stages_progress,
    test_materialization_failure_fails_the_attempt_it_actually_reached,
    test_run_due_replays_pending_entries_in_staged_revision_order,
    test_commit_blocked_reserves_progress_rejects_successors_and_resumes_once,
    test_crash_at_outbox_completion_resumes_the_same_publication,
    test_crash_before_remediation_cas_is_retried,
    test_crash_after_remediation_cas_is_resumed_not_re_decided,
    test_crash_at_remediation_checkpoint_is_resumed,
    test_resume_release_without_a_recorded_decision_is_refused,
    test_trusted_clock_catch_up_evaluates_every_occurrence_in_order,
    test_no_failure_skips_a_mandatory_occurrence,
    test_a_scheduled_occurrence_waits_behind_a_commit_blocked_publication,
    test_a_scheduled_occurrence_waits_behind_an_exhausted_publication,
    test_a_never_evaluated_stream_does_not_backfill_its_history,
    test_projection_publisher_rejects_a_revision_gap,
    test_lifecycle_envelopes_are_gap_free_and_ordered,
    test_contract_lifecycle_cas_and_candidate_activation,
    test_deployment_lifecycle_cas_candidate_activation_and_retirement,
    test_unready_deployment_is_refused,
    test_key_resolver_refuses_an_unknown_key,
    test_static_imports_prove_no_persistence_or_orchestrator_dependency,
    test_importing_the_runtime_loads_no_persistence_or_orchestrator_module,
]


def main() -> int:
    failures = 0
    for test in TESTS:
        name = test.__name__
        try:
            test()
        except Exception:  # noqa: BLE001 - report and continue, exit code carries the signal
            failures += 1
            print(f"FAIL {name}")
            traceback.print_exc()
        else:
            print(f"PASS {name}")
    total = len(TESTS)
    print(f"{total - failures}/{total} passed")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
