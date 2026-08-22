"""Assert-script tests for the SQLite operational state store and evidence module.

The SQLite adapter is passed directly to ``ergasterion.ingestion.conformance``
and the packaged ``adapter-v1.json`` vectors. Additional cases prove restart,
rollback, race, lease expiry, retry/dead-letter, projection replay, target
cursor mismatch, contract migration, MAC/key-commitment conflict, HMAC
rotation reset, key retention, attestation time/revocation rules, the snapshot
reconciliation barrier, and that secrets and source values never persist.

Usage:
    python tests/python/test_sqlite_delivery_store.py
"""

from __future__ import annotations

import json
import tempfile
import traceback
from pathlib import Path

if __package__ in (None, ""):
    import os as _os
    import sys as _sys
    _sys.path.insert(0, _os.path.dirname(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))))

from ergasterion.framework.bronze_contract import (
    BronzeProductContract,
    FingerprintScope,
    Migration,
    MigrationKind,
    OutboxEntryKind,
    OutboxFailureDisposition,
    PublicationPolicy,
    ReadinessResult,
)
from ergasterion.framework.runtime_binding import DeploymentLifecycleRequest, ProjectionCursor
from ergasterion.ingestion.conformance import (
    build_deployment,
    build_memory_ports,
    build_readiness,
    contract_variant,
    fixed_clock,
    load_vectors,
    run_adapter_conformance,
)
from ergasterion.ingestion.evidence import (
    frame_mac,
    generate_ed25519_keypair,
    hmac_sha256_tag,
    record_key_fingerprint,
    record_key_message,
    sign_envelope,
    snapshot_keyset_digest,
    tombstone_keyset_digest,
    verification_key_record,
    verify_signed_attestation,
)
from ergasterion.ingestion.ports import PORT_PROTOCOLS, PortSet
from ergasterion.ingestion.records import (
    Attempt,
    AttemptQuery,
    ContractLifecycleRequest,
    DeliveryVisibilityIdentity,
    HeartbeatProjectionPayload,
    LifecycleEvent,
    LifecycleEventBatch,
    LifecycleEventLogQuery,
    OutboxCompletion,
    OutboxEnqueue,
    OutboxFailureTransaction,
    PayloadDescriptor,
    ProcessingOutcome,
    ProjectionConfirmation,
    ProjectionIntent,
    ProjectionIntentKind,
    ProjectionOutboxPayload,
    RecordKeyTagPage,
    SignedAttestation,
    SnapshotAttestationPayload,
    SnapshotKeysetCompletion,
    SnapshotKeysetRequest,
    SnapshotReconciliationRequest,
    StateOutboxTransaction,
    TombstoneEvidenceRequest,
    TombstoneKeysetCompletion,
    TombstoneKeysetRequest,
    TombstoneTag,
    TombstoneTagPage,
)
from ergasterion.ingestion.runtime import (
    IngestionRuntime,
    PortError,
    canonical_digest,
    digest_token,
)
from ergasterion.ingestion.sqlite_store import (
    SCHEMA_VERSION,
    SqliteKeyResolver,
    SqliteLifecycleSink,
    SqliteStateStore,
    sqlite_ports_factory,
)

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
SCHEMA_VECTORS_PATH = REPO_ROOT / "tests" / "fixtures" / "bronze_schema_vectors.json"
IDL_PATH = REPO_ROOT / "docs" / "specifications" / "bronze-portable-idl-v1.json"

HMAC_GOLDEN_KEY = bytes.fromhex("000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f")
NOW = "2026-01-01T00:00:00.000000Z"


def _sample_contract() -> BronzeProductContract:
    document = json.loads(SCHEMA_VECTORS_PATH.read_text(encoding="utf-8"))
    for vector in document["positive"]:
        if vector["record"] == "BronzeProductContract":
            return BronzeProductContract.model_validate(vector["payload"])
    raise AssertionError("no BronzeProductContract positive vector found")


def _managed_contract():
    return contract_variant(_sample_contract(), integration_kind="managed", publication_mode=PublicationPolicy.ALL_OR_NOTHING)


def _expect_error(code: str, fn, message: str) -> PortError:
    try:
        fn()
    except PortError as exc:
        assert exc.code == code, f"{message}: expected {code!r}, got {exc.code!r} ({exc.detail})"
        return exc
    raise AssertionError(message)


def _identity(contract=None):
    return (contract or _managed_contract()).logical_identity


def _visibility(attempt_id: str = "a" * 64) -> DeliveryVisibilityIdentity:
    return DeliveryVisibilityIdentity(epoch="0", kind="delivery", id=digest_token(attempt_id, "delivery"))


def _scope(contract=None) -> FingerprintScope:
    contract = contract or _managed_contract()
    scope = contract.delivery.record_key.fingerprint_scope
    assert scope is not None
    return scope


def _heartbeat_intent(identity, revision: str, state_revision: str) -> ProjectionIntent:
    payload = HeartbeatProjectionPayload(kind="heartbeat", heartbeat_at=NOW, evaluated_through_at=NOW, prior_committed_at=None)
    payload_digest = canonical_digest(payload.model_dump(mode="json", by_alias=True))
    contract_digest = "b" * 64
    intent_base = {
        "schema": "ergasterion.projection-intent/v1", "logical_identity": identity.model_dump(mode="json"),
        "contract_digest": contract_digest, "projection_target": "bronze", "projection_revision": revision,
        "originating_state_revision": state_revision, "kind": "heartbeat", "payload_digest": payload_digest,
    }
    return ProjectionIntent(
        schema="ergasterion.projection-intent/v1", logical_identity=identity, contract_digest=contract_digest,
        projection_target="bronze", projection_revision=revision, originating_state_revision=state_revision,
        kind=ProjectionIntentKind.HEARTBEAT, execution_plan_digest="c" * 64, runtime_manifest_digest="d" * 64,
        payload=payload, payload_digest=payload_digest, projection_intent_digest=canonical_digest(intent_base),
    )


def _enqueue_heartbeat(store: SqliteStateStore, identity, revision="1") -> tuple:
    state = store.status_query(identity).state
    intent = _heartbeat_intent(identity, revision, state.state_revision)
    outbox_id = canonical_digest({"intent": intent.projection_intent_digest})
    next_state = state.model_copy(update={
        "state_revision": str(int(state.state_revision) + 1),
        "required_projection_revision": revision,
    })
    state = store.state_transaction(StateOutboxTransaction(
        expected_state_revision=state.state_revision, next_state=next_state, attempt_updates=(),
        deployment_update=None, projection_confirmation=None,
        enqueue=(OutboxEnqueue(
            outbox_id=outbox_id,
            payload=ProjectionOutboxPayload(entry_kind="projection", intent=intent),
            payload_digest=intent.projection_intent_digest, next_not_before=NOW,
        ),),
        complete=(),
    ))
    return state, outbox_id, intent


def _ports_with_sqlite(contract, tmp: Path, **memory_kwargs):
    identity = contract.logical_identity
    path = tmp / "state.sqlite"
    store = SqliteStateStore(path, logical_identity=identity, lease_seconds=1, deletion_keyset_days=0, now_fn=lambda: NOW)
    ports, _state = build_memory_ports(identity, **memory_kwargs)
    bundled = PortSet(
        source_connector=ports.source_connector, raw_store=ports.raw_store, scratch_store=ports.scratch_store,
        state_store=store, landing_adapter=ports.landing_adapter,
        remediation_repository=ports.remediation_repository, projection_publisher=ports.projection_publisher,
        lifecycle_sink=ports.lifecycle_sink, key_resolver=ports.key_resolver,
    )
    return bundled, store.status_query(identity).state, store


# --------------------------------------------------------------------------- packaged conformance

def test_sqlite_state_store_satisfies_the_port_protocol() -> None:
    contract = _managed_contract()
    with tempfile.TemporaryDirectory() as tmp:
        store = SqliteStateStore(Path(tmp) / "s.sqlite", logical_identity=contract.logical_identity)
        try:
            assert isinstance(store, PORT_PROTOCOLS["state_store"])
        finally:
            store.close()


def test_adapter_conformance_vectors_pass_against_sqlite() -> None:
    contract = _sample_contract()
    vectors = load_vectors()
    assert len(vectors) >= 15
    with tempfile.TemporaryDirectory() as tmp:
        held: list = []

        def factory(vector, resolved, handle):
            ports, state = sqlite_ports_factory(vector, resolved, handle, directory=tmp)
            held.append(ports.state_store)
            return ports, state

        failed = []
        try:
            for vector in vectors:
                outcome = run_adapter_conformance(vector, contract, ports_factory=factory)
                if not outcome.passed:
                    failed.append(f"{outcome.vector_id}: {outcome.detail}")
        finally:
            for store in held:
                store.close()
        assert not failed, "\n".join(failed)


# --------------------------------------------------------------------------- restart, rollback, race

def test_restart_reloads_attempts_and_outbox() -> None:
    contract = _managed_contract()
    identity = contract.logical_identity
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "state.sqlite"
        store = SqliteStateStore(path, logical_identity=identity)
        state, outbox_id, intent = _enqueue_heartbeat(store, identity)
        store.close()
        reopened = SqliteStateStore(path, logical_identity=identity)
        try:
            status = reopened.status_query(identity)
            assert status.state.state_revision == state.state_revision
            assert status.state.required_projection_revision == "1"
            leased = reopened.lease_outbox(identity, OutboxEntryKind.PROJECTION, "owner-a", NOW, 8)
            assert len(leased) == 1
            assert leased[0].outbox_id == outbox_id
            payload = reopened.load_outbox_payload(outbox_id, intent.projection_intent_digest)
            assert payload.intent.projection_intent_digest == intent.projection_intent_digest
        finally:
            reopened.close()


def test_stale_revision_rolls_back_partial_writes() -> None:
    contract = _managed_contract()
    identity = contract.logical_identity
    with tempfile.TemporaryDirectory() as tmp:
        store = SqliteStateStore(Path(tmp) / "s.sqlite", logical_identity=identity)
        try:
            attempt = Attempt(
                run_id="a" * 64, attempt_id="b" * 64, logical_identity=identity, claim_digest="c" * 64,
                scheduled_boundary_at=NOW, attempt_ordinal=1, state="received", block_phase=None,
                reason_code=None, execution_plan_digest="d" * 64, runtime_manifest_digest="e" * 64,
                state_revision="1",
            )
            state = store.status_query(identity).state
            _expect_error(
                "stale_revision",
                lambda: store.state_transaction(StateOutboxTransaction(
                    expected_state_revision="99",
                    next_state=state.model_copy(update={"state_revision": "100"}),
                    attempt_updates=(attempt,), deployment_update=None, projection_confirmation=None,
                    enqueue=(), complete=(),
                )),
                "expected a mismatched revision to refuse the transaction",
            )
            page = store.attempts(AttemptQuery(
                logical_identity=identity, claim_digest=None, nonterminal_only=False,
                after_attempt_id=None, max_items=16,
            ))
            assert page.attempts == ()
            assert store.status_query(identity).state.state_revision == "0"
        finally:
            store.close()


def test_concurrent_lifecycle_cas_leaves_one_winner() -> None:
    contract = _managed_contract()
    identity = contract.logical_identity
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "s.sqlite"
        first = SqliteStateStore(path, logical_identity=identity)
        second = SqliteStateStore(path, logical_identity=identity)
        try:
            request = ContractLifecycleRequest(
                schema="ergasterion.contract-lifecycle-request/v1", action="register",
                expected_state_revision="0", expected_deployment_revision=None,
                contract=contract, migration=None, permit_pre_intent_fence=False,
            )
            first.contract_lifecycle(request)
            _expect_error(
                "stale_revision",
                lambda: second.contract_lifecycle(request),
                "expected the losing lifecycle CAS to see stale_revision",
            )
            assert first.status_query(identity).state.state_revision == "1"
        finally:
            first.close()
            second.close()


# --------------------------------------------------------------------------- lease, retry, dead-letter

def test_lease_expiry_makes_work_reclaimable() -> None:
    contract = _managed_contract()
    identity = contract.logical_identity
    with tempfile.TemporaryDirectory() as tmp:
        store = SqliteStateStore(Path(tmp) / "s.sqlite", logical_identity=identity, lease_seconds=1)
        try:
            _enqueue_heartbeat(store, identity)
            first = store.lease_outbox(identity, OutboxEntryKind.PROJECTION, "owner-a", NOW, 8)
            assert len(first) == 1
            assert first[0].lease_owner == "owner-a"
            still_held = store.lease_outbox(identity, OutboxEntryKind.PROJECTION, "owner-b", NOW, 8)
            assert still_held == ()
            later = "2026-01-01T00:00:02.000000Z"
            reclaimed = store.lease_outbox(identity, OutboxEntryKind.PROJECTION, "owner-b", later, 8)
            assert len(reclaimed) == 1
            assert reclaimed[0].lease_owner == "owner-b"
        finally:
            store.close()


def test_retry_then_dead_letter() -> None:
    contract = _managed_contract()
    identity = contract.logical_identity
    with tempfile.TemporaryDirectory() as tmp:
        store = SqliteStateStore(Path(tmp) / "s.sqlite", logical_identity=identity)
        try:
            state, outbox_id, intent = _enqueue_heartbeat(store, identity)
            store.lease_outbox(identity, OutboxEntryKind.PROJECTION, "owner-a", NOW, 8)
            state = store.fail_outbox(OutboxFailureTransaction(
                expected_state_revision=state.state_revision, next_state=state, attempt_updates=(),
                outbox_id=outbox_id, payload_digest=intent.projection_intent_digest, lease_owner="owner-a",
                failure_observed_at=NOW, reason_code="target_unavailable",
                disposition=OutboxFailureDisposition.RETRYABLE, next_not_before=NOW,
            ))
            retryable = store.lease_outbox(identity, OutboxEntryKind.PROJECTION, "owner-a", NOW, 8)
            assert retryable[0].status.value == "leased"
            assert retryable[0].dispatch_ordinal == 2
            store.fail_outbox(OutboxFailureTransaction(
                expected_state_revision=state.state_revision, next_state=state, attempt_updates=(),
                outbox_id=outbox_id, payload_digest=intent.projection_intent_digest, lease_owner="owner-a",
                failure_observed_at=NOW, reason_code="target_unavailable",
                disposition=OutboxFailureDisposition.DEAD_LETTER, next_not_before=None,
            ))
            assert store.lease_outbox(identity, OutboxEntryKind.PROJECTION, "owner-a", NOW, 8) == ()
            assert store.status_query(identity).incomplete_outbox_count == "0"
        finally:
            store.close()


# --------------------------------------------------------------------------- projection replay and target cursors

def test_projection_replay_batch_rebuilds_cursor() -> None:
    from ergasterion.ingestion.records import ProjectionReplayBatch

    contract = _managed_contract()
    identity = contract.logical_identity
    with tempfile.TemporaryDirectory() as tmp:
        store = SqliteStateStore(Path(tmp) / "s.sqlite", logical_identity=identity)
        ports, _ = build_memory_ports(identity)
        try:
            state, outbox_id, intent = _enqueue_heartbeat(store, identity)
            confirmation = ports.projection_publisher.apply_gap_ordered(intent)
            store.state_transaction(StateOutboxTransaction(
                expected_state_revision=state.state_revision, next_state=state, attempt_updates=(),
                deployment_update=None, projection_confirmation=confirmation, enqueue=(),
                complete=(),
            ))
            intents = store.projection_log(identity, "0", 16, "1000000").intents
            confirmations = store.projection_confirmation_log(identity, "0", 16, "1000000").confirmations
            lost, _ = build_memory_ports(identity)
            cursor = lost.projection_publisher.rebuild_read_models(ProjectionReplayBatch(
                intents=intents, confirmations=confirmations, max_items=16, max_bytes="1000000", bytes_supplied="0",
            ))
            assert cursor.projection_revision == "1"
        finally:
            store.close()


def test_projection_confirmation_replay_refuses_digest_mismatch() -> None:
    contract = _managed_contract()
    identity = contract.logical_identity
    with tempfile.TemporaryDirectory() as tmp:
        store = SqliteStateStore(Path(tmp) / "s.sqlite", logical_identity=identity)
        try:
            state, _outbox_id, intent = _enqueue_heartbeat(store, identity)
            confirmation = ProjectionConfirmation(
                schema="ergasterion.projection-confirmation/v1", logical_identity=identity,
                contract_digest=intent.contract_digest, projection_target="bronze", kind=intent.kind,
                projection_intent_digest=intent.projection_intent_digest, projection_revision="1",
                target_applied_at=NOW, committed_at=NOW, release_applied_at=None, timeliness=None,
                processing=ProcessingOutcome.COMMITTED, visibility=None, ledger_ref=None, deletion_evidence=None,
                target_result_digest="f" * 64,
            )
            store.state_transaction(StateOutboxTransaction(
                expected_state_revision=state.state_revision, next_state=state, attempt_updates=(),
                deployment_update=None, projection_confirmation=confirmation, enqueue=(), complete=(),
            ))
            state = store.status_query(identity).state
            store.state_transaction(StateOutboxTransaction(
                expected_state_revision=state.state_revision,
                next_state=state.model_copy(update={"state_revision": str(int(state.state_revision) + 1)}),
                attempt_updates=(), deployment_update=None, projection_confirmation=confirmation,
                enqueue=(), complete=(),
            ))
            logged = store.projection_confirmation_log(identity, "0", 16, "1000000").confirmations
            assert len(logged) == 1
            assert logged[0].target_result_digest == "f" * 64
            assert logged[0].projection_intent_digest == intent.projection_intent_digest
            conflicting = confirmation.model_copy(update={"target_result_digest": "0" * 64})
            state = store.status_query(identity).state
            _expect_error(
                "integrity_error",
                lambda: store.state_transaction(StateOutboxTransaction(
                    expected_state_revision=state.state_revision,
                    next_state=state.model_copy(update={"state_revision": str(int(state.state_revision) + 1)}),
                    attempt_updates=(), deployment_update=None, projection_confirmation=conflicting,
                    enqueue=(), complete=(),
                )),
                "a confirmation replay with a mismatched body must be refused",
            )
            after = store.projection_confirmation_log(identity, "0", 16, "1000000").confirmations
            assert len(after) == 1
            assert after[0].target_result_digest == "f" * 64
            assert after[0].projection_intent_digest == intent.projection_intent_digest
            assert store.status_query(identity).state.state_revision == state.state_revision
        finally:
            store.close()


def test_fail_outbox_and_complete_require_digest_and_lease_owner() -> None:
    contract = _managed_contract()
    identity = contract.logical_identity
    with tempfile.TemporaryDirectory() as tmp:
        store = SqliteStateStore(Path(tmp) / "s.sqlite", logical_identity=identity)
        try:
            state, outbox_id, intent = _enqueue_heartbeat(store, identity)
            store.lease_outbox(identity, OutboxEntryKind.PROJECTION, "owner-a", NOW, 8)
            _expect_error(
                "integrity_error",
                lambda: store.fail_outbox(OutboxFailureTransaction(
                    expected_state_revision=state.state_revision, next_state=state, attempt_updates=(),
                    outbox_id=outbox_id, payload_digest="0" * 64, lease_owner="owner-a",
                    failure_observed_at=NOW, reason_code="target_unavailable",
                    disposition=OutboxFailureDisposition.RETRYABLE, next_not_before=NOW,
                )),
                "fail_outbox must refuse a mismatched payload digest",
            )
            leased = store.lease_outbox(identity, OutboxEntryKind.PROJECTION, "owner-b", NOW, 8)
            assert leased == ()
            _expect_error(
                "integrity_error",
                lambda: store.state_transaction(StateOutboxTransaction(
                    expected_state_revision=state.state_revision, next_state=state, attempt_updates=(),
                    deployment_update=None, projection_confirmation=None, enqueue=(),
                    complete=(OutboxCompletion(
                        outbox_id=outbox_id, payload_digest="0" * 64, lease_owner="owner-a", completed_at=NOW,
                    ),),
                )),
                "complete must refuse a mismatched payload digest",
            )
            _expect_error(
                "integrity_error",
                lambda: store.state_transaction(StateOutboxTransaction(
                    expected_state_revision=state.state_revision, next_state=state, attempt_updates=(),
                    deployment_update=None, projection_confirmation=None, enqueue=(),
                    complete=(OutboxCompletion(
                        outbox_id=outbox_id, payload_digest=intent.projection_intent_digest,
                        lease_owner="owner-b", completed_at=NOW,
                    ),),
                )),
                "complete must require the lease owner when the entry is leased",
            )
            store.state_transaction(StateOutboxTransaction(
                expected_state_revision=state.state_revision, next_state=state, attempt_updates=(),
                deployment_update=None, projection_confirmation=None, enqueue=(),
                complete=(OutboxCompletion(
                    outbox_id=outbox_id, payload_digest=intent.projection_intent_digest,
                    lease_owner="owner-a", completed_at=NOW,
                ),),
            ))
            assert store.lease_outbox(identity, OutboxEntryKind.PROJECTION, "owner-a", NOW, 8) == ()
            assert store.status_query(identity).incomplete_outbox_count == "0"
        finally:
            store.close()


def test_ahead_behind_corrupt_catchup_cursor_refuses_activation() -> None:
    contract = _managed_contract()
    identity = contract.logical_identity
    with tempfile.TemporaryDirectory() as tmp:
        store = SqliteStateStore(Path(tmp) / "s.sqlite", logical_identity=identity)
        try:
            state, _outbox_id, intent = _enqueue_heartbeat(store, identity)
            from ergasterion.ingestion.records import ProjectionConfirmation, ProcessingOutcome
            confirmation = ProjectionConfirmation(
                schema="ergasterion.projection-confirmation/v1", logical_identity=identity,
                contract_digest=intent.contract_digest, projection_target="bronze", kind=intent.kind,
                projection_intent_digest=intent.projection_intent_digest, projection_revision="1",
                target_applied_at=NOW, committed_at=NOW, release_applied_at=None, timeliness=None,
                processing=ProcessingOutcome.COMMITTED, visibility=None, ledger_ref=None, deletion_evidence=None,
                target_result_digest="f" * 64,
            )
            store.state_transaction(StateOutboxTransaction(
                expected_state_revision=state.state_revision, next_state=state, attempt_updates=(),
                deployment_update=None, projection_confirmation=confirmation, enqueue=(), complete=(),
            ))
            manifest = "a" * 64
            registered = store.deployment_lifecycle(DeploymentLifecycleRequest(
                schema="ergasterion.deployment-lifecycle-request/v1", action="register",
                expected_state_revision=store.status_query(identity).state.state_revision,
                expected_deployment_revision="0",
                deployment=build_deployment(contract, manifest, candidate_manifest_digest=manifest),
                readiness=build_readiness(contract, manifest),
                catchup_cursor=ProjectionCursor(
                    logical_identity=identity, projection_target="bronze",
                    projection_revision="0", projection_intent_digest=None,
                ),
                permit_pre_intent_fence=False,
            ))

            def activate(revision: str, digest):
                return store.deployment_lifecycle(DeploymentLifecycleRequest(
                    schema="ergasterion.deployment-lifecycle-request/v1", action="activate",
                    expected_state_revision=registered.state.state_revision,
                    expected_deployment_revision=registered.deployment.deployment_revision,
                    deployment=build_deployment(contract, manifest, candidate_manifest_digest=manifest),
                    readiness=build_readiness(contract, manifest),
                    catchup_cursor=ProjectionCursor(
                        logical_identity=identity, projection_target="bronze",
                        projection_revision=revision, projection_intent_digest=digest,
                    ),
                    permit_pre_intent_fence=False,
                ))

            _expect_error("superseded_deployment", lambda: activate("0", None), "zero cursor over confirmed revisions")
            _expect_error("superseded_deployment", lambda: activate("2", intent.projection_intent_digest), "ahead cursor")
            _expect_error(
                "superseded_deployment",
                lambda: activate("1", "0" * 64),
                "corrupt cursor digest",
            )
            activated = activate("1", intent.projection_intent_digest)
            assert activated.deployment.active_manifest_digest == manifest
        finally:
            store.close()


# --------------------------------------------------------------------------- contract migration

def test_carry_and_reset_contract_migration() -> None:
    contract = _managed_contract()
    identity = contract.logical_identity
    digest = canonical_digest(contract.model_dump(mode="json", by_alias=True))
    with tempfile.TemporaryDirectory() as tmp:
        store = SqliteStateStore(Path(tmp) / "s.sqlite", logical_identity=identity)
        try:
            carry = Migration(
                migration_id="1" * 64, kind=MigrationKind.CARRY, from_contract_digest=None,
                to_contract_digest=digest, activated_at=None, from_visibility_epoch="0", to_visibility_epoch="0",
            )
            registered = store.contract_lifecycle(ContractLifecycleRequest(
                schema="ergasterion.contract-lifecycle-request/v1", action="register",
                expected_state_revision="0", expected_deployment_revision=None,
                contract=contract, migration=carry, permit_pre_intent_fence=False,
            ))
            _expect_error(
                "migration_conflict",
                lambda: store.contract_lifecycle(ContractLifecycleRequest(
                    schema="ergasterion.contract-lifecycle-request/v1", action="activate",
                    expected_state_revision=registered.state.state_revision, expected_deployment_revision=None,
                    contract=contract, migration=None, permit_pre_intent_fence=False,
                )),
                "activating without the registered migration must conflict",
            )
            activated = store.contract_lifecycle(ContractLifecycleRequest(
                schema="ergasterion.contract-lifecycle-request/v1", action="activate",
                expected_state_revision=registered.state.state_revision, expected_deployment_revision=None,
                contract=contract, migration=carry, permit_pre_intent_fence=False,
            ))
            assert activated.state.active_contract_digest == digest
            reset = Migration(
                migration_id="2" * 64, kind=MigrationKind.RESET, from_contract_digest=digest,
                to_contract_digest=digest, activated_at=None, from_visibility_epoch="0", to_visibility_epoch="1",
            )
            registered = store.contract_lifecycle(ContractLifecycleRequest(
                schema="ergasterion.contract-lifecycle-request/v1", action="register",
                expected_state_revision=activated.state.state_revision, expected_deployment_revision=None,
                contract=contract, migration=reset, permit_pre_intent_fence=False,
            ))
            reset_activated = store.contract_lifecycle(ContractLifecycleRequest(
                schema="ergasterion.contract-lifecycle-request/v1", action="activate",
                expected_state_revision=registered.state.state_revision, expected_deployment_revision=None,
                contract=contract, migration=reset, permit_pre_intent_fence=False,
            ))
            assert reset_activated.state.visibility_epoch == "1"
            assert reset_activated.state.accepted_progress == {}
        finally:
            store.close()


def test_sqlite_schema_migrates_an_empty_file() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "empty.sqlite"
        store = SqliteStateStore(path, logical_identity=_identity())
        try:
            version = store._conn.execute("SELECT version FROM schema_meta").fetchone()[0]
            assert version == SCHEMA_VERSION
            store.close()
            reopened = SqliteStateStore(path, logical_identity=_identity())
            again = reopened._conn.execute("SELECT version FROM schema_meta").fetchone()[0]
            assert again == SCHEMA_VERSION
            reopened.close()
        except Exception:
            store.close()
            raise


# --------------------------------------------------------------------------- HMAC, attestation, keysets

def test_record_key_mac_golden_vector_and_no_plaintext_in_sqlite() -> None:
    idl = json.loads(IDL_PATH.read_bytes())
    golden = idl["golden_vectors"]["record_key_mac"]
    domain = golden["domain_utf8"].encode("utf-8")
    message = golden["message_utf8"].encode("utf-8")
    assert frame_mac(domain, message).hex() == golden["framed_input_hex"]
    assert hmac_sha256_tag(HMAC_GOLDEN_KEY, golden["domain_utf8"], message) == golden["tag_hex"]
    identity = {
        "estate_namespace": "com.example.synthetic", "source": "ledger", "table": "accounts",
    }
    scope = {"scope_id": "account_population", "scope_parameters": {}}
    components = [
        {"logical_type": "utf8_string", "value": "acct-001"},
        {"logical_type": "int64", "value": "42"},
    ]
    tag = record_key_fingerprint(HMAC_GOLDEN_KEY, identity, scope, components)
    assert tag == golden["tag_hex"]
    assert record_key_message(identity, scope, components) == message

    contract = _managed_contract()
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "s.sqlite"
        store = SqliteStateStore(path, logical_identity=contract.logical_identity, deletion_keyset_days=0, now_fn=lambda: NOW)
        resolver = SqliteKeyResolver(path)
        try:
            resolver.put_hmac_secret("hmac-key-1", HMAC_GOLDEN_KEY)
            commitment = resolver.key_commitment("hmac-key-1")
            visibility = _visibility()
            keyset = store.begin_snapshot_keyset(SnapshotKeysetRequest(
                attempt_id="a" * 64, logical_identity=contract.logical_identity, visibility=visibility,
                record_key_scope=_scope(contract), hmac_key_id="hmac-key-1", key_commitment=commitment.commitment,
            ))
            store.append_snapshot_keyset("a" * 64, RecordKeyTagPage(
                keyset_id=keyset.keyset_id, first_frame_sequence="0", next_frame_sequence="1",
                tags=(tag,), bytes_supplied="0",
            ))
            expected = snapshot_keyset_digest(
                contract.logical_identity, visibility, _scope(contract), "hmac-key-1", commitment.commitment, (tag,),
            )
            completed = store.complete_snapshot_keyset(SnapshotKeysetCompletion(
                attempt_id="a" * 64, keyset_id=keyset.keyset_id, expected_key_count="1",
                expected_keyset_digest=expected,
            ))
            assert completed.complete
            raw = path.read_bytes()
            assert HMAC_GOLDEN_KEY not in raw
            assert b"acct-001" not in raw
        finally:
            store.close()
            resolver.close()


def test_opaque_mac_key_commitment_conflict_and_hmac_rotation_reset() -> None:
    contract = _managed_contract()
    identity = contract.logical_identity
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "s.sqlite"
        store = SqliteStateStore(path, logical_identity=identity, now_fn=lambda: NOW)
        resolver = SqliteKeyResolver(path)
        try:
            first = resolver.put_hmac_secret("hmac-key-1", HMAC_GOLDEN_KEY)
            _expect_error(
                "key_commitment_conflict",
                lambda: resolver.put_hmac_secret("hmac-key-1", b"\x11" * 32),
                "reusing a key id with different material must conflict",
            )
            rotated = resolver.put_hmac_secret("hmac-key-2", b"\x22" * 32)
            vis_prior = _visibility("1" * 64)
            vis_next = _visibility("2" * 64)
            prior = store.begin_snapshot_keyset(SnapshotKeysetRequest(
                attempt_id="a" * 64, logical_identity=identity, visibility=vis_prior,
                record_key_scope=_scope(contract), hmac_key_id="hmac-key-1", key_commitment=first.commitment,
            ))
            store.append_snapshot_keyset("a" * 64, RecordKeyTagPage(
                keyset_id=prior.keyset_id, first_frame_sequence="0", next_frame_sequence="1",
                tags=("aa" * 32,), bytes_supplied="0",
            ))
            prior = store.complete_snapshot_keyset(SnapshotKeysetCompletion(
                attempt_id="a" * 64, keyset_id=prior.keyset_id, expected_key_count="1",
                expected_keyset_digest=snapshot_keyset_digest(
                    identity, vis_prior, _scope(contract), "hmac-key-1", first.commitment, ("aa" * 32,),
                ),
            ))
            candidate = store.begin_snapshot_keyset(SnapshotKeysetRequest(
                attempt_id="b" * 64, logical_identity=identity, visibility=vis_next,
                record_key_scope=_scope(contract), hmac_key_id="hmac-key-2", key_commitment=rotated.commitment,
            ))
            store.append_snapshot_keyset("b" * 64, RecordKeyTagPage(
                keyset_id=candidate.keyset_id, first_frame_sequence="0", next_frame_sequence="1",
                tags=("bb" * 32,), bytes_supplied="0",
            ))
            candidate = store.complete_snapshot_keyset(SnapshotKeysetCompletion(
                attempt_id="b" * 64, keyset_id=candidate.keyset_id, expected_key_count="1",
                expected_keyset_digest=snapshot_keyset_digest(
                    identity, vis_next, _scope(contract), "hmac-key-2", rotated.commitment, ("bb" * 32,),
                ),
            ))
            _expect_error(
                "key_commitment_conflict",
                lambda: store.reconcile_snapshot(SnapshotReconciliationRequest(
                    attempt_id="b" * 64, claim_digest="c" * 64, prior_keyset=prior, candidate_keyset=candidate,
                )),
                "diffing keysets minted under different HMAC keys must conflict",
            )
            reset = store.reconcile_snapshot(SnapshotReconciliationRequest(
                attempt_id="b" * 64, claim_digest="c" * 64, prior_keyset=None, candidate_keyset=candidate,
            ))
            assert reset.deletion_evidence.deleted_key_count == "0"
            assert reset.reconciliation.status.value == "complete"
        finally:
            store.close()
            resolver.close()


def test_key_retention_keeps_tags_until_successor_and_window_elapse() -> None:
    contract = _managed_contract()
    identity = contract.logical_identity
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "s.sqlite"
        store = SqliteStateStore(
            path, logical_identity=identity, deletion_keyset_days=1, now_fn=lambda: NOW,
        )
        try:
            vis_prior = _visibility("1" * 64)
            vis_next = _visibility("2" * 64)
            commitment = "ab" * 32
            prior = store.begin_snapshot_keyset(SnapshotKeysetRequest(
                attempt_id="a" * 64, logical_identity=identity, visibility=vis_prior,
                record_key_scope=_scope(contract), hmac_key_id="hmac-key-1", key_commitment=commitment,
            ))
            store.append_snapshot_keyset("a" * 64, RecordKeyTagPage(
                keyset_id=prior.keyset_id, first_frame_sequence="0", next_frame_sequence="1",
                tags=("aa" * 32,), bytes_supplied="0",
            ))
            prior = store.complete_snapshot_keyset(SnapshotKeysetCompletion(
                attempt_id="a" * 64, keyset_id=prior.keyset_id, expected_key_count="1",
                expected_keyset_digest=snapshot_keyset_digest(
                    identity, vis_prior, _scope(contract), "hmac-key-1", commitment, ("aa" * 32,),
                ),
            ))
            candidate = store.begin_snapshot_keyset(SnapshotKeysetRequest(
                attempt_id="b" * 64, logical_identity=identity, visibility=vis_next,
                record_key_scope=_scope(contract), hmac_key_id="hmac-key-1", key_commitment=commitment,
            ))
            store.append_snapshot_keyset("b" * 64, RecordKeyTagPage(
                keyset_id=candidate.keyset_id, first_frame_sequence="0", next_frame_sequence="0",
                tags=(), bytes_supplied="0",
            ))
            candidate = store.complete_snapshot_keyset(SnapshotKeysetCompletion(
                attempt_id="b" * 64, keyset_id=candidate.keyset_id, expected_key_count="0",
                expected_keyset_digest=snapshot_keyset_digest(
                    identity, vis_next, _scope(contract), "hmac-key-1", commitment, (),
                ),
            ))
            result = store.reconcile_snapshot(SnapshotReconciliationRequest(
                attempt_id="b" * 64, claim_digest="c" * 64, prior_keyset=prior, candidate_keyset=candidate,
            ))
            assert result.deletion_evidence.deleted_key_count == "1"
            assert store.get_snapshot_keyset(identity, vis_prior).keyset_id == prior.keyset_id

            tombstones = store.begin_tombstone_keyset(TombstoneKeysetRequest(
                attempt_id="a" * 64, logical_identity=identity, visibility=vis_prior,
                record_key_scope=_scope(contract), hmac_key_id="hmac-key-1", key_commitment=commitment,
            ))
            item = TombstoneTag(event_sequence="1", tag="aa" * 32)
            store.append_tombstone_keyset("a" * 64, TombstoneTagPage(
                keyset_id=tombstones.keyset_id, items=(item,), bytes_supplied="0",
            ))
            tombstones = store.complete_tombstone_keyset(TombstoneKeysetCompletion(
                attempt_id="a" * 64, keyset_id=tombstones.keyset_id, expected_key_count="1",
                expected_keyset_digest=tombstone_keyset_digest(
                    identity, vis_prior, _scope(contract), "hmac-key-1", commitment, (item,),
                ),
                event_sequence_low="1", event_sequence_high="1",
            ))
            store.finalize_tombstone_evidence(TombstoneEvidenceRequest(
                attempt_id="a" * 64, claim_digest="c" * 64, keyset=tombstones,
            ))
            still_complete = store.begin_tombstone_keyset(TombstoneKeysetRequest(
                attempt_id="a" * 64, logical_identity=identity, visibility=vis_prior,
                record_key_scope=_scope(contract), hmac_key_id="hmac-key-1", key_commitment=commitment,
            ))
            assert still_complete.complete is True
            store.close()
            later = "2026-01-03T00:00:00.000000Z"
            reopened = SqliteStateStore(
                path, logical_identity=identity, deletion_keyset_days=1, now_fn=lambda: later,
            )
            try:
                _expect_error(
                    "not_found",
                    lambda: reopened.get_snapshot_keyset(identity, vis_prior),
                    "a retained keyset must disappear only after the window elapses",
                )
                revived = reopened.begin_tombstone_keyset(TombstoneKeysetRequest(
                    attempt_id="a" * 64, logical_identity=identity, visibility=vis_prior,
                    record_key_scope=_scope(contract), hmac_key_id="hmac-key-1", key_commitment=commitment,
                ))
                assert revived.complete is False
                assert revived.key_count == "0"
            finally:
                reopened.close()
        except Exception:
            store.close()
            raise


def test_attestation_revocation_and_clock_skew_rules() -> None:
    private, public_raw = generate_ed25519_keypair()
    key_id = "key-a"
    enabled = verification_key_record(
        key_id, public_raw, enabled_at="2026-01-01T00:00:00.000000Z",
        authorized_policy_refs=("attest-default",),
    )
    identity = _identity()
    payload = SnapshotAttestationPayload(
        logical_identity=identity, contract_digest="a" * 64, delivery_id="delivery-1",
        batch_id="batch-1", effective_boundary_at=NOW, content_fingerprint="b" * 64,
        scope=_scope(), row_count="1", issued_at=NOW,
    )
    unsigned = SignedAttestation(
        schema="ergasterion.snapshot-attestation/v1", algorithm="Ed25519", key_id=key_id,
        payload=payload, signature="AA",
    )
    signature = sign_envelope(private, unsigned)
    attestation = unsigned.model_copy(update={"signature": signature})
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "keys.sqlite"
        resolver = SqliteKeyResolver(path)
        try:
            resolver.put_verification_key(enabled)
            stored = resolver.resolve_verification_key(key_id)
            verify_signed_attestation(
                attestation, stored, now=NOW, future_clock_skew_seconds=30, policy_ref="attest-default",
            )
            future_payload = payload.model_copy(update={"issued_at": "2026-01-01T00:01:00.000000Z"})
            future_unsigned = unsigned.model_copy(update={"payload": future_payload})
            future = future_unsigned.model_copy(update={"signature": sign_envelope(private, future_unsigned)})
            _expect_error(
                "attestation_invalid",
                lambda: verify_signed_attestation(
                    future, stored, now=NOW, future_clock_skew_seconds=30, policy_ref="attest-default",
                ),
                "an attestation beyond the future-clock-skew window must be refused",
            )
            _expect_error(
                "policy_not_authorized",
                lambda: verify_signed_attestation(
                    attestation, stored, now=NOW, future_clock_skew_seconds=30, policy_ref="other-policy",
                ),
                "a key that is not authorised for the contract policy must be refused",
            )
            other_private, _ = generate_ed25519_keypair()
            forged = unsigned.model_copy(update={"signature": sign_envelope(other_private, unsigned)})
            _expect_error(
                "invalid_signature",
                lambda: verify_signed_attestation(
                    forged, stored, now=NOW, future_clock_skew_seconds=30, policy_ref="attest-default",
                ),
                "a signature from another key must be refused",
            )

            revoked_after = verification_key_record(
                key_id, public_raw, enabled_at="2026-01-01T00:00:00.000000Z",
                authorized_policy_refs=("attest-default",), revoked_at="2026-06-01T00:00:00.000000Z",
            )
            persisted_after = resolver.put_verification_key(revoked_after)
            assert persisted_after.revoked_at == "2026-06-01T00:00:00.000000Z"
            verify_signed_attestation(
                attestation, persisted_after, now="2026-07-01T00:00:00.000000Z",
                future_clock_skew_seconds=30, policy_ref="attest-default",
            )
            _expect_error(
                "key_revoked",
                lambda: resolver.resolve_verification_key(key_id),
                "resolve must refuse a stored revoked key for new acceptance",
            )
            resolver.close()
            reopened = SqliteKeyResolver(path)
            try:
                kept = reopened.put_verification_key(enabled)
                assert kept.revoked_at == "2026-06-01T00:00:00.000000Z"
                verify_signed_attestation(
                    attestation, kept, now="2026-07-01T00:00:00.000000Z",
                    future_clock_skew_seconds=30, policy_ref="attest-default",
                )
            finally:
                reopened.close()

            later_path = Path(tmp) / "keys-revoked-before.sqlite"
            before_resolver = SqliteKeyResolver(later_path)
            try:
                revoked_before = verification_key_record(
                    key_id, public_raw, enabled_at="2026-01-01T00:00:00.000000Z",
                    authorized_policy_refs=("attest-default",), revoked_at="2025-12-31T00:00:00.000000Z",
                )
                persisted_before = before_resolver.put_verification_key(revoked_before)
                _expect_error(
                    "key_revoked",
                    lambda: verify_signed_attestation(
                        attestation, persisted_before, now=NOW, future_clock_skew_seconds=30,
                        policy_ref="attest-default",
                    ),
                    "an attestation issued after revocation must be refused",
                )
                _expect_error(
                    "key_revoked",
                    lambda: before_resolver.resolve_verification_key(key_id),
                    "a key revoked before issued_at must not resolve",
                )
            finally:
                before_resolver.close()
        finally:
            resolver.close()


def test_snapshot_reconciliation_barrier_and_idempotent_evidence() -> None:
    contract = _managed_contract()
    identity = contract.logical_identity
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "s.sqlite"
        store = SqliteStateStore(path, logical_identity=identity, now_fn=lambda: NOW)
        sink = SqliteLifecycleSink(path)
        try:
            vis = _visibility()
            commitment = "cd" * 32
            keyset = store.begin_snapshot_keyset(SnapshotKeysetRequest(
                attempt_id="a" * 64, logical_identity=identity, visibility=vis,
                record_key_scope=_scope(contract), hmac_key_id="hmac-key-1", key_commitment=commitment,
            ))
            _expect_error(
                "integrity_error",
                lambda: store.reconcile_snapshot(SnapshotReconciliationRequest(
                    attempt_id="a" * 64, claim_digest="c" * 64, prior_keyset=None, candidate_keyset=keyset,
                )),
                "an incomplete candidate keyset cannot pass the publication barrier",
            )
            store.append_snapshot_keyset("a" * 64, RecordKeyTagPage(
                keyset_id=keyset.keyset_id, first_frame_sequence="0", next_frame_sequence="1",
                tags=("aa" * 32,), bytes_supplied="0",
            ))
            keyset = store.complete_snapshot_keyset(SnapshotKeysetCompletion(
                attempt_id="a" * 64, keyset_id=keyset.keyset_id, expected_key_count="1",
                expected_keyset_digest=snapshot_keyset_digest(
                    identity, vis, _scope(contract), "hmac-key-1", commitment, ("aa" * 32,),
                ),
            ))
            first = store.reconcile_snapshot(SnapshotReconciliationRequest(
                attempt_id="a" * 64, claim_digest="c" * 64, prior_keyset=None, candidate_keyset=keyset,
            ))
            second = store.reconcile_snapshot(SnapshotReconciliationRequest(
                attempt_id="a" * 64, claim_digest="c" * 64, prior_keyset=None, candidate_keyset=keyset,
            ))
            assert first.deletion_evidence.deletion_evidence_intent_digest == second.deletion_evidence.deletion_evidence_intent_digest

            tombstones = store.begin_tombstone_keyset(TombstoneKeysetRequest(
                attempt_id="a" * 64, logical_identity=identity, visibility=vis,
                record_key_scope=_scope(contract), hmac_key_id="hmac-key-1", key_commitment=commitment,
            ))
            item = TombstoneTag(event_sequence="1", tag="aa" * 32)
            store.append_tombstone_keyset("a" * 64, TombstoneTagPage(
                keyset_id=tombstones.keyset_id, items=(item,), bytes_supplied="0",
            ))
            tombstones = store.complete_tombstone_keyset(TombstoneKeysetCompletion(
                attempt_id="a" * 64, keyset_id=tombstones.keyset_id, expected_key_count="1",
                expected_keyset_digest=tombstone_keyset_digest(
                    identity, vis, _scope(contract), "hmac-key-1", commitment, (item,),
                ),
                event_sequence_low="1", event_sequence_high="1",
            ))
            evidence = store.finalize_tombstone_evidence(TombstoneEvidenceRequest(
                attempt_id="a" * 64, claim_digest="c" * 64, keyset=tombstones,
            ))
            assert evidence.delete_strategy.value == "explicit_tombstone"
            assert evidence.reconciliation_digest is None

            from ergasterion.framework.bronze_contract import LifecycleEventType
            from ergasterion.ingestion.records import AttemptLifecyclePayload, AttemptState
            attempt = Attempt(
                run_id="a" * 64, attempt_id="b" * 64, logical_identity=identity, claim_digest="c" * 64,
                scheduled_boundary_at=NOW, attempt_ordinal=1, state=AttemptState.RECEIVED, block_phase=None,
                reason_code=None, execution_plan_digest="d" * 64, runtime_manifest_digest="e" * 64,
                state_revision="1",
            )
            event = LifecycleEvent(
                event_id="f" * 64, event_type=LifecycleEventType.RECEIVED, logical_identity=identity,
                state_revision="1", event_ordinal="1", attempt_id=attempt.attempt_id,
                execution_plan_digest=attempt.execution_plan_digest, runtime_manifest_digest=attempt.runtime_manifest_digest,
                payload=AttemptLifecyclePayload(kind=AttemptState.RECEIVED, attempt=attempt, projection_confirmation=None),
                payload_digest="11" * 32, created_at=NOW,
            )
            assert sink.project_events(LifecycleEventBatch(events=(event,), max_items=1, bytes_supplied="0")) == (event.event_id,)
            assert sink.project_events(LifecycleEventBatch(events=(event,), max_items=1, bytes_supplied="0")) == (event.event_id,)
            store.close()
            reopened = SqliteStateStore(path, logical_identity=identity)
            try:
                page = reopened.lifecycle_event_log(LifecycleEventLogQuery(
                    logical_identity=identity, after_cursor=None, max_items=16, max_bytes="1000000",
                ))
                assert page.events[0].event_id == event.event_id
            finally:
                reopened.close()
        finally:
            sink.close()
            try:
                store.close()
            except Exception:
                pass


def test_runtime_resume_after_sqlite_restart() -> None:
    contract = _managed_contract()
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        ports, state, store = _ports_with_sqlite(
            contract, tmp_path, content_by_handle={"h": [{"key": "a", "accept": True}]}, fail_first_n=1,
        )
        restored = None
        try:
            runtime = IngestionRuntime(ports, fixed_clock())
            from ergasterion.ingestion.records import DeliveryManifest, ManagedPayloadInput
            digest = canonical_digest(contract.model_dump(mode="json", by_alias=True))
            rows = [{"key": "a", "accept": True}]
            kind = contract.delivery.progress.kind
            progress_claim = (
                {"kind": "opaque_batch"} if kind == "opaque_batch"
                else {"kind": "sequence", "high_watermark": str(len(rows)), "event_count": str(len(rows))}
            )
            manifest = DeliveryManifest(
                schema="ergasterion.delivery-manifest/v1", logical_identity=contract.logical_identity,
                product_version=contract.product.product_version, contract_digest=digest,
                delivery_id="resume-1", batch_id=None, scheduled_boundary_at=None, effective_boundary_at=None,
                payload=PayloadDescriptor(
                    media_type="application/x-ndjson", content_encoding="identity", codec_version=1,
                    byte_length=str(len(json.dumps(rows))), sha256="0" * 64,
                ),
                frame_sequence_digest=None, progress_claim=progress_claim,
                declared_row_count="1", snapshot_attestation=None,
            )
            input_record = ManagedPayloadInput(kind="managed_payload", manifest=manifest, payload_handle="h")
            attempt, state = runtime.submit_managed(state, contract, "a" * 64, "b" * 64, "c" * 64, input_record)
            receipt = ports.raw_store.preserve(input_record)
            visibility = DeliveryVisibilityIdentity(
                epoch="0", kind="delivery", id=digest_token(attempt.attempt_id, "delivery"),
            )
            attempt, state, materialized, validation = runtime.land_and_validate(
                attempt, state, contract, receipt, visibility, "d" * 64, "e" * 64,
            )
            result = runtime.publish(
                attempt, state, contract, materialized, validation, visibility, receipt,
                build_readiness(contract, "b" * 64),
            )
            assert result.attempt.state.value == "commit_blocked"
            path = store.path
            store.close()
            restored = SqliteStateStore(path, logical_identity=contract.logical_identity)
            resumed_ports = PortSet(
                source_connector=ports.source_connector, raw_store=ports.raw_store, scratch_store=ports.scratch_store,
                state_store=restored, landing_adapter=ports.landing_adapter,
                remediation_repository=ports.remediation_repository, projection_publisher=ports.projection_publisher,
                lifecycle_sink=ports.lifecycle_sink, key_resolver=ports.key_resolver,
            )
            resumed = IngestionRuntime(resumed_ports, fixed_clock())
            outcomes = resumed.run_due(contract.logical_identity, NOW, int(contract.delivery.retry.max_attempts))
            assert outcomes, "expected run_due to resume the blocked publication after restart"
            assert outcomes[-1].attempt.state.value == "committed"
        finally:
            store.close()
            if restored is not None:
                restored.close()


def test_sqlite_schema_file_has_no_hmac_secret_after_resolver_close() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "keys.sqlite"
        resolver = SqliteKeyResolver(path)
        secret = b"super-secret-hmac-material-32b!!"
        resolver.put_hmac_secret("hmac-key-1", secret)
        raw_open = path.read_bytes()
        assert secret not in raw_open
        resolver.close()
        assert secret not in path.read_bytes()


# --------------------------------------------------------------------------- restart of projection logs

def test_projection_log_survives_restart() -> None:
    contract = _managed_contract()
    identity = contract.logical_identity
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "s.sqlite"
        store = SqliteStateStore(path, logical_identity=identity)
        state, _outbox_id, intent = _enqueue_heartbeat(store, identity)
        store.close()
        reopened = SqliteStateStore(path, logical_identity=identity)
        try:
            page = reopened.projection_log(identity, "0", 16, "1000000")
            assert page.intents[0].projection_intent_digest == intent.projection_intent_digest
            assert page.more is False
            assert int(page.bytes_returned) > 0
        finally:
            reopened.close()
        del state


TESTS = [
    test_sqlite_state_store_satisfies_the_port_protocol,
    test_adapter_conformance_vectors_pass_against_sqlite,
    test_restart_reloads_attempts_and_outbox,
    test_stale_revision_rolls_back_partial_writes,
    test_concurrent_lifecycle_cas_leaves_one_winner,
    test_lease_expiry_makes_work_reclaimable,
    test_retry_then_dead_letter,
    test_projection_replay_batch_rebuilds_cursor,
    test_projection_confirmation_replay_refuses_digest_mismatch,
    test_fail_outbox_and_complete_require_digest_and_lease_owner,
    test_ahead_behind_corrupt_catchup_cursor_refuses_activation,
    test_carry_and_reset_contract_migration,
    test_sqlite_schema_migrates_an_empty_file,
    test_record_key_mac_golden_vector_and_no_plaintext_in_sqlite,
    test_opaque_mac_key_commitment_conflict_and_hmac_rotation_reset,
    test_key_retention_keeps_tags_until_successor_and_window_elapse,
    test_attestation_revocation_and_clock_skew_rules,
    test_snapshot_reconciliation_barrier_and_idempotent_evidence,
    test_runtime_resume_after_sqlite_restart,
    test_sqlite_schema_file_has_no_hmac_secret_after_resolver_close,
    test_projection_log_survives_restart,
]


def main() -> int:
    failures = 0
    for test in TESTS:
        name = test.__name__
        try:
            test()
        except Exception:  # noqa: BLE001
            failures += 1
            print(f"FAIL {name}")
            traceback.print_exc()
        else:
            print(f"PASS {name}")
    total = len(TESTS)
    print(f"{total - failures}/{total} passed")
    return 1 if failures else 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
