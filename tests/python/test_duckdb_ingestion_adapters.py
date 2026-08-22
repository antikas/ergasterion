"""Assert-script tests for the DuckDB Bronze bundle and operational read models.

DuckDB landing, remediation, projection and lifecycle adapters are passed to
``ergasterion.ingestion.conformance`` and the packaged ``adapter-v1.json``
vectors. Additional cases prove prepare/disposition, publication fences,
rebuild/restore, paged decision queries and restart against a real DuckDB file.

Usage:
    python tests/python/test_duckdb_ingestion_adapters.py
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
    DispositionStatus,
    EvidenceKind,
    Finding,
    FindingMetadata,
    LifecycleEventType,
    Migration,
    MigrationKind,
    PublicationPolicy,
    RawLocator,
)
from ergasterion.ingestion.conformance import (
    build_memory_ports,
    contract_variant,
    exercise_all_operations,
    load_vectors,
    run_adapter_conformance,
)
from ergasterion.ingestion.duckdb_bronze import (
    BRONZE_RELATIONS,
    DuckDBLandingAdapter,
    DuckDBStore,
    dumps,
    duckdb_ports_factory,
)
from ergasterion.ingestion.duckdb_lifecycle import DuckDBLifecycleSink
from ergasterion.ingestion.duckdb_projection import DuckDBProjectionPublisher
from ergasterion.ingestion.duckdb_remediation import DuckDBRemediationRepository, decision_query_digest
from ergasterion.ingestion.ports import PORT_PROTOCOLS, PortSet
from ergasterion.ingestion.records import (
    AttemptLifecyclePayload,
    CandidateReadQuery,
    DeliveryPublicationPayload,
    DeliveryVisibilityIdentity,
    Disposition,
    DispositionPage,
    DispositionQuery,
    EvidenceQuery,
    HeartbeatProjectionPayload,
    LifecycleEvent,
    LifecycleEventBatch,
    MaterializationCompletion,
    MigrationProjectionPayload,
    PORT_OPERATION_ORDER,
    ProjectionIntent,
    ProjectionIntentKind,
    ProjectionReplayBatch,
    RawManifestObject,
    RawPayloadObject,
    RawReadHandle,
    RawReadPage,
    RawReceipt,
    ReleaseVisibilityBinding,
    ReleaseVisibilityIdentity,
    RemediationDecision,
    RemediationDecisionKind,
    RemediationDecisionQuery,
    RemediationEvaluation,
    RemediationRelease,
    ReprocessingClaim,
    SourceNativeQuery,
    VersionInterface,
    VisibilityAncestryRow,
)
from ergasterion.ingestion.runtime import PortError, canonical_digest, digest_token

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
SCHEMA_VECTORS_PATH = REPO_ROOT / "tests" / "fixtures" / "bronze_schema_vectors.json"
NOW = "2026-01-01T00:00:00.000000Z"
DIGEST_A = "a" * 64
DIGEST_B = "b" * 64
DIGEST_C = "c" * 64
DIGEST_D = "d" * 64
DIGEST_E = "e" * 64


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


def _b64url(raw: bytes) -> str:
    import base64
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _receipt(digest: str = DIGEST_A) -> RawReceipt:
    return RawReceipt(
        schema="ergasterion.raw-receipt/v1", claim_digest=digest,
        payload=RawPayloadObject(
            content_id=f"sha256:{digest}", algorithm="sha256", byte_length="2",
            media_type="application/x-ndjson", content_encoding="identity",
        ),
        manifest=RawManifestObject(content_id=f"sha256:{digest}", algorithm="sha256", byte_length="2"),
        raw_receipt_digest=digest,
    )


def _handle(receipt: RawReceipt) -> RawReadHandle:
    return RawReadHandle(
        raw_receipt_digest=receipt.raw_receipt_digest, content_id=receipt.payload.content_id,
        byte_length=receipt.payload.byte_length, handle_ref=receipt.raw_receipt_digest,
    )


def _page(raw: bytes) -> RawReadPage:
    return RawReadPage(
        handle_ref="raw", offset="0", bytes_base64url=_b64url(raw),
        bytes_returned=str(len(raw)), next_offset=None, eof=True,
    )


def _visibility(attempt_id: str = DIGEST_A) -> DeliveryVisibilityIdentity:
    return DeliveryVisibilityIdentity(epoch="0", kind="delivery", id=digest_token(attempt_id, "delivery"))


def _prepare(landing: DuckDBLandingAdapter, contract, rows, *, attempt_id=DIGEST_A, visibility=None):
    visibility = visibility or _visibility(attempt_id)
    receipt = _receipt(canonical_digest({"attempt": attempt_id, "rows": rows}))
    raw = json.dumps(rows).encode("utf-8")
    receipt = receipt.model_copy(update={
        "payload": receipt.payload.model_copy(update={"byte_length": str(len(raw))}),
        "raw_receipt_digest": canonical_digest({"raw": attempt_id}),
    })
    preparation = landing.begin_prepare(attempt_id, receipt, _handle(receipt), contract, visibility)
    preparation = landing.append_raw(preparation, _page(raw))
    evidence = landing.finish_prepare(preparation)
    return evidence, visibility, receipt


def _disposition(evidence, frame_sequence: str, status: str, attempt_id: str = DIGEST_A) -> Disposition:
    return Disposition(
        disposition_id=canonical_digest({"frame": frame_sequence, "attempt": attempt_id}),
        raw_ref=evidence.candidate_ref, raw_locator=RawLocator(
            frame_sequence=frame_sequence, byte_offset=None, byte_length=None, line_number=None,
        ),
        delivery_id="d1", claim_digest=DIGEST_B, ruleset_digest=DIGEST_C,
        product_version="1.0.0", contract_digest=DIGEST_D, source_schema_digest=DIGEST_E,
        published_schema_digest=DIGEST_A,
        status=status, findings=() if status == "accepted" else (_finding(),),
        outcome_digest=canonical_digest({"frame": frame_sequence, "status": status}),
    )


def _finding() -> Finding:
    return Finding(
        kind="rule", field_path="/key", code="row_attribution_error", severity="error",
        metadata=FindingMetadata(
            diagnostic_code="null_not_allowed", raw_locator=None, expected_logical_type=None,
            observed_logical_type=None, observed_count=None, expected_min_count=None,
            expected_max_count=None, duplicate_group_size=None,
        ),
    )


def _materialize(landing, evidence, frames_status, *, attempt_id=DIGEST_A, visibility=None):
    session = landing.begin_materialization(attempt_id, evidence, DIGEST_C, DIGEST_D)
    dispositions = tuple(
        _disposition(evidence, str(index), status, attempt_id)
        for index, status in enumerate(frames_status)
    )
    if dispositions:
        session = landing.append_dispositions(session, DispositionPage(
            session_id=session.session_id, dispositions=dispositions,
            first_frame_sequence=dispositions[0].raw_locator.frame_sequence,
            next_frame_sequence=str(len(dispositions)), bytes_supplied="0",
        ))
    from ergasterion.ingestion.records import ValidationResult
    accepted = sum(1 for item in dispositions if item.status == DispositionStatus.ACCEPTED or item.status == "accepted")
    validation = ValidationResult(
        schema="ergasterion.validation-result/v1", evaluation_id=DIGEST_C, ruleset_digest=DIGEST_D,
        batch_findings=(), framed_count=str(len(dispositions)), accepted_count=str(accepted),
        error_count=str(len(dispositions) - accepted), warning_count="0", quarantined_count=str(len(dispositions) - accepted),
        error_numerator=str(len(dispositions) - accepted), error_denominator=str(max(len(dispositions), 1)),
        publication_decision="publish_all" if accepted == len(dispositions) else "publish_valid_rows",
        validation_result_digest=canonical_digest({"accepted": accepted}),
    )
    return landing.finish_materialization(MaterializationCompletion(
        session=session, validation=validation, candidate_keyset=None, output_visibility=visibility,
    )), dispositions


def _intent(identity, kind, payload, revision: str, *, target="bronze", contract_digest=DIGEST_B):
    payload_digest = canonical_digest(payload.model_dump(mode="json", by_alias=True))
    base = {
        "schema": "ergasterion.projection-intent/v1", "logical_identity": identity.model_dump(mode="json"),
        "contract_digest": contract_digest, "projection_target": target, "projection_revision": revision,
        "originating_state_revision": "1", "kind": kind if isinstance(kind, str) else kind.value,
        "payload_digest": payload_digest,
    }
    return ProjectionIntent(
        schema="ergasterion.projection-intent/v1", logical_identity=identity, contract_digest=contract_digest,
        projection_target=target, projection_revision=revision, originating_state_revision="1",
        kind=kind, execution_plan_digest=DIGEST_C, runtime_manifest_digest=DIGEST_D,
        payload=payload, payload_digest=payload_digest, projection_intent_digest=canonical_digest(base),
    )


def _publication_payload(visibility, *, accepted_ref="accepted-1"):
    return DeliveryPublicationPayload(
        kind="delivery_publication", attempt_id=DIGEST_A, visibility=visibility, product_version="1.0.0",
        contract_digest=DIGEST_B, source_schema_digest=DIGEST_C, published_schema_digest=DIGEST_D,
        readiness_digest=DIGEST_E, delivery_claim_digest=DIGEST_A, transport_payload_digest=DIGEST_B,
        raw_receipt_ref="raw-ref", raw_receipt_digest=DIGEST_C, bronze_partition_ref=accepted_ref,
        accepted_content_digest=DIGEST_D, ruleset_digest=DIGEST_E, validation_result_digest=DIGEST_A,
        accepted_count="1", progress_claim={"kind": "opaque_batch"}, deletion_evidence=None,
        scheduled_boundary_at=NOW, warning_deadline_at=NOW, error_deadline_at=NOW,
        prior_committed_at=None, lineage_digest=DIGEST_B,
    )


def _bundle(tmp: Path):
    store = DuckDBStore(Path(tmp) / "bronze.duckdb")
    landing = DuckDBLandingAdapter(store)
    remediation = DuckDBRemediationRepository(store)
    publisher = DuckDBProjectionPublisher(store)
    sink = DuckDBLifecycleSink(store)
    return store, landing, remediation, publisher, sink


# --------------------------------------------------------------------------- packaged conformance

def test_duckdb_adapters_satisfy_port_protocols() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        store, landing, remediation, publisher, sink = _bundle(tmp)
        try:
            assert isinstance(landing, PORT_PROTOCOLS["landing_adapter"])
            assert isinstance(remediation, PORT_PROTOCOLS["remediation_repository"])
            assert isinstance(publisher, PORT_PROTOCOLS["projection_publisher"])
            assert isinstance(sink, PORT_PROTOCOLS["lifecycle_sink"])
        finally:
            store.close()


def test_adapter_conformance_vectors_pass_against_duckdb() -> None:
    contract = _sample_contract()
    vectors = load_vectors()
    assert len(vectors) >= 15
    with tempfile.TemporaryDirectory() as tmp:
        held: list = []

        def factory(vector, resolved, handle):
            ports, state = duckdb_ports_factory(vector, resolved, handle, directory=Path(tmp) / vector["id"])
            held.append(ports.landing_adapter)
            return ports, state

        failed = []
        try:
            for vector in vectors:
                outcome = run_adapter_conformance(vector, contract, ports_factory=factory)
                if not outcome.passed:
                    failed.append(f"{outcome.vector_id}: {outcome.detail}")
        finally:
            for adapter in held:
                adapter.close()
        assert not failed, "\n".join(failed)


def test_exercise_all_operations_with_duckdb_ports() -> None:
    contract = _managed_contract()
    with tempfile.TemporaryDirectory() as tmp:
        store = DuckDBStore(Path(tmp) / "bronze.duckdb")
        ports, state = build_memory_ports(
            contract.logical_identity, content_by_handle={"exercise": [{"key": "a", "accept": True}]},
        )
        bundled = PortSet(
            source_connector=ports.source_connector, raw_store=ports.raw_store, scratch_store=ports.scratch_store,
            state_store=ports.state_store,
            landing_adapter=DuckDBLandingAdapter(store),
            remediation_repository=DuckDBRemediationRepository(store),
            projection_publisher=DuckDBProjectionPublisher(store),
            lifecycle_sink=DuckDBLifecycleSink(store),
            key_resolver=ports.key_resolver,
        )
        try:
            reached = exercise_all_operations(bundled, state, contract, "exercise")
            for field_name in ("landing_adapter", "remediation_repository", "projection_publisher", "lifecycle_sink"):
                missing = tuple(op for op in PORT_OPERATION_ORDER[field_name] if op not in reached[field_name])
                assert not missing, f"{field_name} missed {missing}"
        finally:
            store.close()


# --------------------------------------------------------------------------- landing / disposition

def test_prepare_typed_failure_and_rule_invalid_exclusion() -> None:
    contract = _managed_contract()
    with tempfile.TemporaryDirectory() as tmp:
        store, landing, *_ = _bundle(tmp)
        try:
            evidence, visibility, _receipt = _prepare(
                landing, contract,
                [{"key": "ok", "accept": True}, {"key": "bad", "accept": False}],
            )
            page = landing.read_candidate(CandidateReadQuery(
                evidence=evidence, after_sequence=None, max_frames=16, max_bytes="1000000",
            ))
            assert len(page.frames) == 2
            assert page.frames[0].typed_fields is not None
            assert page.frames[1].structural_findings
            materialized, dispositions = _materialize(
                landing, evidence, ("accepted", "rejected"), visibility=visibility,
            )
            accepted = store.fetchall("SELECT * FROM accepted_rows")
            assert len(accepted) == 1
            assert accepted[0]["_ergasterion_visibility_kind"] == "delivery"
            assert accepted[0]["_ergasterion_delivery_id"] == visibility.id
            assert json.loads(accepted[0]["typed_fields_json"])[0]["value"]["value"] == "ok"
            rejected = [item for item in dispositions if item.status == DispositionStatus.REJECTED or item.status == "rejected"]
            assert rejected
            native = landing.source_native_query(SourceNativeQuery(
                logical_identity=contract.logical_identity, candidate_ref=evidence.candidate_ref,
                disposition_ref=materialized.disposition_ref, authorization_context_ref="operator",
                after_frame_sequence=None, max_items=16, max_bytes="1000000",
            ))
            assert len(native.items) == 2
        finally:
            store.close()


def test_tagged_visibility_collision_and_remediation_release() -> None:
    contract = _managed_contract()
    with tempfile.TemporaryDirectory() as tmp:
        store, landing, remediation, *_ = _bundle(tmp)
        try:
            evidence, visibility, _receipt = _prepare(landing, contract, [{"key": "a", "accept": True}])
            materialized, dispositions = _materialize(landing, evidence, ("accepted",), visibility=None)
            assert materialized.published_visibility is None
            release_vis = ReleaseVisibilityIdentity(epoch="0", kind="release", id=DIGEST_E)
            bound = landing.bind_release_visibility(ReleaseVisibilityBinding(
                materialized=materialized, visibility=release_vis,
            ))
            assert bound.published_visibility == release_vis
            assert bound.accepted_content_digest == materialized.accepted_content_digest
            replay = landing.bind_release_visibility(ReleaseVisibilityBinding(
                materialized=materialized, visibility=release_vis,
            ))
            assert replay.accepted_ref == bound.accepted_ref
            other = materialized.model_copy(update={"accepted_content_digest": DIGEST_A})
            _expect_error(
                "row_attribution_error",
                lambda: landing.bind_release_visibility(ReleaseVisibilityBinding(materialized=other, visibility=release_vis)),
                "same tagged visibility with different content must collide",
            )
            delivery_same_id = DeliveryVisibilityIdentity(epoch="0", kind="delivery", id=digest_token(DIGEST_E, "x"))
            # delivery/release namespaces are distinct even when the id hex matches a digest
            evaluation = RemediationEvaluation(
                schema="ergasterion.remediation-evaluation/v1", original_claim_digest=DIGEST_B,
                raw_receipt_digest=DIGEST_C, target_contract_digest=DIGEST_D, target_source_schema_digest=DIGEST_E,
                target_published_schema_digest=DIGEST_A, target_ruleset_digest=DIGEST_B, execution_plan_digest=DIGEST_C,
                root_visibility_epoch="0", remediation_evaluation_id=DIGEST_D,
            )
            release = RemediationRelease(
                schema="ergasterion.remediation-release/v1", remediation_evaluation_id=DIGEST_D,
                selected_locators=(dispositions[0].raw_locator,), accepted_content_digest=materialized.accepted_content_digest,
                release_id=DIGEST_E,
            )
            decision = RemediationDecision(
                schema="ergasterion.remediation-decision/v1", decision_id=DIGEST_A, kind=RemediationDecisionKind.RELEASED,
                evaluation=evaluation, disposition_ids=(dispositions[0].disposition_id,),
                validation_result_digest=DIGEST_B, release=release, decided_at=NOW,
            )
            recorded = remediation.record_decision(decision)
            assert recorded.decision_id == decision.decision_id
            assert remediation.record_decision(decision).decision_id == decision.decision_id
            overlapping = decision.model_copy(update={"decision_id": DIGEST_B})
            _expect_error(
                "release_conflict",
                lambda: remediation.record_decision(overlapping),
                "overlapping locator claims must conflict",
            )
            _ = delivery_same_id
        finally:
            store.close()


def test_replay_conflict_gap_revision_atomic_projection_and_unpublished_exclusion() -> None:
    contract = _managed_contract()
    identity = contract.logical_identity
    with tempfile.TemporaryDirectory() as tmp:
        store, landing, _remediation, publisher, _sink = _bundle(tmp)
        try:
            evidence, visibility, _receipt = _prepare(landing, contract, [{"key": "a", "accept": True}])
            materialized, _dispositions = _materialize(landing, evidence, ("accepted",), visibility=visibility)
            payload = _publication_payload(visibility, accepted_ref=materialized.accepted_ref)
            intent = _intent(identity, ProjectionIntentKind.DELIVERY_PUBLICATION, payload, "1")
            assert publisher.ledger_rows(identity, "bronze") == ()
            confirmation = publisher.apply_gap_ordered(intent)
            assert confirmation.projection_revision == "1"
            assert publisher.apply_gap_ordered(intent).projection_intent_digest == intent.projection_intent_digest
            published = publisher.published_visibility_set(identity, "bronze")
            assert (visibility.epoch, visibility.kind, visibility.id) in published
            _expect_error(
                "projection_gap",
                lambda: publisher.apply_gap_ordered(_intent(identity, ProjectionIntentKind.DELIVERY_PUBLICATION, payload, "3")),
                "a gapped revision must be refused",
            )
            stale = _intent(identity, ProjectionIntentKind.HEARTBEAT, HeartbeatProjectionPayload(
                kind="heartbeat", heartbeat_at=NOW, evaluated_through_at=NOW, prior_committed_at=NOW,
            ), "1")
            _expect_error(
                "projection_conflict",
                lambda: publisher.apply_gap_ordered(stale),
                "a stale current revision with different bytes must conflict",
            )
            assert len(publisher.ledger_rows(identity, "bronze")) == 1
            other_vis = DeliveryVisibilityIdentity(epoch="0", kind="delivery", id=digest_token(DIGEST_B, "delivery"))
            unpublished = store.fetchall(
                """SELECT _ergasterion_visibility_id FROM accepted_rows
                   WHERE _ergasterion_visibility_id != ?""",
                [visibility.id],
            )
            assert unpublished == [] or all(
                (visibility.epoch, "delivery", row["_ergasterion_visibility_id"]) not in published
                for row in unpublished
            )
            other_payload = _publication_payload(other_vis, accepted_ref="not-published")
            # unpublished tuples stay out of the ledger until a successful apply
            assert (other_vis.epoch, other_vis.kind, other_vis.id) not in publisher.published_visibility_set(identity, "bronze")
            _ = other_payload
        finally:
            store.close()


def test_visibility_ancestry_and_versioned_interfaces_coexist_across_reset() -> None:
    contract = _managed_contract()
    identity = contract.logical_identity
    with tempfile.TemporaryDirectory() as tmp:
        store, landing, _remediation, publisher, _sink = _bundle(tmp)
        try:
            _prepare(landing, contract, [{"key": "a", "accept": True}])
            carry = MigrationProjectionPayload(
                kind="migration",
                migration=Migration(
                    migration_id=DIGEST_A, kind=MigrationKind.CARRY, from_contract_digest=DIGEST_B,
                    to_contract_digest=DIGEST_C, activated_at=NOW, from_visibility_epoch="0",
                    to_visibility_epoch="1",
                ),
                version_interface=VersionInterface(
                    logical_identity=identity, product_version="1.1.0", contract_digest=DIGEST_C,
                    root_visibility_epoch="0", relation_ref="bronze.v1_1", active=True,
                ),
                ancestry=(
                    VisibilityAncestryRow(
                        logical_identity=identity, descendant_epoch="1", ancestor_epoch="1",
                        projection_target="bronze", projection_revision="1",
                    ),
                    VisibilityAncestryRow(
                        logical_identity=identity, descendant_epoch="1", ancestor_epoch="0",
                        projection_target="bronze", projection_revision="1",
                    ),
                ),
                readiness_digest=DIGEST_D, prior_committed_at=NOW,
            )
            publisher.apply_gap_ordered(_intent(identity, ProjectionIntentKind.MIGRATION, carry, "1", contract_digest=DIGEST_C))
            carry_alias = publisher.active_alias(identity, "bronze")
            reset = MigrationProjectionPayload(
                kind="migration",
                migration=Migration(
                    migration_id=DIGEST_E, kind=MigrationKind.RESET, from_contract_digest=DIGEST_C,
                    to_contract_digest=DIGEST_D, activated_at=NOW, from_visibility_epoch="1",
                    to_visibility_epoch="2",
                ),
                version_interface=VersionInterface(
                    logical_identity=identity, product_version="2.0.0", contract_digest=DIGEST_D,
                    root_visibility_epoch="2", relation_ref="bronze.v2", active=True,
                ),
                ancestry=(
                    VisibilityAncestryRow(
                        logical_identity=identity, descendant_epoch="2", ancestor_epoch="2",
                        projection_target="bronze", projection_revision="2",
                    ),
                ),
                readiness_digest=DIGEST_A, prior_committed_at=NOW,
            )
            publisher.apply_gap_ordered(_intent(identity, ProjectionIntentKind.MIGRATION, reset, "2", contract_digest=DIGEST_D))
            versions = publisher.version_interfaces(identity, "bronze")
            assert len(versions) == 2
            refs = {item.relation_ref for item in versions}
            assert "bronze.v1_1" in refs and "bronze.v2" in refs
            assert sum(1 for item in versions if item.active) == 1
            by_ref = {item.relation_ref: item.active for item in versions}
            assert by_ref["bronze.v1_1"] is False
            assert by_ref["bronze.v2"] is True
            for row in store.fetchall("SELECT active, json FROM version_registry"):
                parsed = json.loads(row["json"])
                assert bool(row["active"]) is bool(parsed["active"])
            assert publisher.active_alias(identity, "bronze") == "bronze.v2"
            assert carry_alias in refs
            ancestry = publisher.ancestry_rows(identity, "bronze")
            assert any(row.descendant_epoch == "1" and row.ancestor_epoch == "0" for row in ancestry)
            assert any(row.descendant_epoch == "2" and row.ancestor_epoch == "2" for row in ancestry)
        finally:
            store.close()


def test_reset_retires_prior_version_interface_json_active() -> None:
    contract = _managed_contract()
    identity = contract.logical_identity
    with tempfile.TemporaryDirectory() as tmp:
        store, landing, _remediation, publisher, _sink = _bundle(tmp)
        try:
            _prepare(landing, contract, [{"key": "a", "accept": True}])
            carry = MigrationProjectionPayload(
                kind="migration",
                migration=Migration(
                    migration_id=DIGEST_A, kind=MigrationKind.CARRY, from_contract_digest=DIGEST_B,
                    to_contract_digest=DIGEST_C, activated_at=NOW, from_visibility_epoch="0",
                    to_visibility_epoch="1",
                ),
                version_interface=VersionInterface(
                    logical_identity=identity, product_version="1.1.0", contract_digest=DIGEST_C,
                    root_visibility_epoch="0", relation_ref="bronze.v1_1", active=True,
                ),
                ancestry=(
                    VisibilityAncestryRow(
                        logical_identity=identity, descendant_epoch="1", ancestor_epoch="1",
                        projection_target="bronze", projection_revision="1",
                    ),
                    VisibilityAncestryRow(
                        logical_identity=identity, descendant_epoch="1", ancestor_epoch="0",
                        projection_target="bronze", projection_revision="1",
                    ),
                ),
                readiness_digest=DIGEST_D, prior_committed_at=NOW,
            )
            publisher.apply_gap_ordered(_intent(identity, ProjectionIntentKind.MIGRATION, carry, "1", contract_digest=DIGEST_C))
            reset = MigrationProjectionPayload(
                kind="migration",
                migration=Migration(
                    migration_id=DIGEST_E, kind=MigrationKind.RESET, from_contract_digest=DIGEST_C,
                    to_contract_digest=DIGEST_D, activated_at=NOW, from_visibility_epoch="1",
                    to_visibility_epoch="2",
                ),
                version_interface=VersionInterface(
                    logical_identity=identity, product_version="2.0.0", contract_digest=DIGEST_D,
                    root_visibility_epoch="2", relation_ref="bronze.v2", active=True,
                ),
                ancestry=(
                    VisibilityAncestryRow(
                        logical_identity=identity, descendant_epoch="2", ancestor_epoch="2",
                        projection_target="bronze", projection_revision="2",
                    ),
                ),
                readiness_digest=DIGEST_A, prior_committed_at=NOW,
            )
            publisher.apply_gap_ordered(_intent(identity, ProjectionIntentKind.MIGRATION, reset, "2", contract_digest=DIGEST_D))
            versions = publisher.version_interfaces(identity, "bronze")
            assert [item.relation_ref for item in versions if item.active] == ["bronze.v2"]
            assert [item.relation_ref for item in versions if not item.active] == ["bronze.v1_1"]
            rows = store.fetchall(
                "SELECT active, json FROM version_registry WHERE identity_key = ?",
                [dumps(identity)],
            )
            assert len(rows) == 2
            assert sum(1 for row in rows if bool(row["active"])) == 1
            for row in rows:
                parsed = json.loads(row["json"])
                assert bool(row["active"]) is bool(parsed["active"])
        finally:
            store.close()


def test_release_ancestry_lookup_scopes_claim_identity_candidate() -> None:
    contract = _managed_contract()
    with tempfile.TemporaryDirectory() as tmp:
        store, landing, remediation, *_ = _bundle(tmp)
        try:
            decoy_vis = DeliveryVisibilityIdentity(epoch="99", kind="delivery", id=digest_token(DIGEST_C, "delivery"))
            decoy_evidence, _, _ = _prepare(
                landing, contract, [{"key": "decoy", "accept": True}],
                attempt_id=DIGEST_C, visibility=decoy_vis,
            )
            _, decoy_dispositions = _materialize(
                landing, decoy_evidence, ("accepted",), attempt_id=DIGEST_C, visibility=decoy_vis,
            )
            evidence, visibility, _ = _prepare(landing, contract, [{"key": "real", "accept": True}])
            materialized, dispositions = _materialize(landing, evidence, ("accepted",), visibility=visibility)
            decoy_eval = RemediationEvaluation(
                schema="ergasterion.remediation-evaluation/v1", original_claim_digest=DIGEST_B,
                raw_receipt_digest=DIGEST_C, target_contract_digest=DIGEST_D, target_source_schema_digest=DIGEST_E,
                target_published_schema_digest=DIGEST_A, target_ruleset_digest=DIGEST_B, execution_plan_digest=DIGEST_C,
                root_visibility_epoch="0", remediation_evaluation_id=DIGEST_A,
            )
            decoy_release = RemediationRelease(
                schema="ergasterion.remediation-release/v1", remediation_evaluation_id=DIGEST_A,
                selected_locators=(decoy_dispositions[0].raw_locator,), accepted_content_digest=DIGEST_C,
                release_id=DIGEST_D,
            )
            _expect_error(
                "ancestry_mismatch",
                lambda: remediation.record_decision(RemediationDecision(
                    schema="ergasterion.remediation-decision/v1", decision_id=DIGEST_B,
                    kind=RemediationDecisionKind.RELEASED, evaluation=decoy_eval,
                    disposition_ids=(decoy_dispositions[0].disposition_id,),
                    validation_result_digest=DIGEST_B, release=decoy_release, decided_at=NOW,
                )),
                "a foreign-epoch locator without ancestry must mismatch when scoped to its candidate",
            )
            evaluation = RemediationEvaluation(
                schema="ergasterion.remediation-evaluation/v1", original_claim_digest=DIGEST_B,
                raw_receipt_digest=DIGEST_C, target_contract_digest=DIGEST_D, target_source_schema_digest=DIGEST_E,
                target_published_schema_digest=DIGEST_A, target_ruleset_digest=DIGEST_B, execution_plan_digest=DIGEST_C,
                root_visibility_epoch="0", remediation_evaluation_id=DIGEST_D,
            )
            release = RemediationRelease(
                schema="ergasterion.remediation-release/v1", remediation_evaluation_id=DIGEST_D,
                selected_locators=(dispositions[0].raw_locator,), accepted_content_digest=materialized.accepted_content_digest,
                release_id=DIGEST_E,
            )
            recorded = remediation.record_decision(RemediationDecision(
                schema="ergasterion.remediation-decision/v1", decision_id=DIGEST_A, kind=RemediationDecisionKind.RELEASED,
                evaluation=evaluation, disposition_ids=(dispositions[0].disposition_id,),
                validation_result_digest=DIGEST_B, release=release, decided_at=NOW,
            ))
            assert recorded.decision_id == DIGEST_A
        finally:
            store.close()


def test_release_ancestry_lookup_scopes_evaluation_identity() -> None:
    contract = _managed_contract()
    identity = contract.logical_identity
    foreign = identity.model_copy(update={"table": "other_table"})
    with tempfile.TemporaryDirectory() as tmp:
        store, landing, remediation, publisher, _sink = _bundle(tmp)
        try:
            foreign_carry = MigrationProjectionPayload(
                kind="migration",
                migration=Migration(
                    migration_id=DIGEST_A, kind=MigrationKind.CARRY, from_contract_digest=DIGEST_B,
                    to_contract_digest=DIGEST_C, activated_at=NOW, from_visibility_epoch="99",
                    to_visibility_epoch="2",
                ),
                version_interface=VersionInterface(
                    logical_identity=foreign, product_version="1.1.0", contract_digest=DIGEST_C,
                    root_visibility_epoch="99", relation_ref="bronze.v1_1", active=True,
                ),
                ancestry=(
                    VisibilityAncestryRow(
                        logical_identity=foreign, descendant_epoch="2", ancestor_epoch="2",
                        projection_target="bronze", projection_revision="1",
                    ),
                    VisibilityAncestryRow(
                        logical_identity=foreign, descendant_epoch="2", ancestor_epoch="99",
                        projection_target="bronze", projection_revision="1",
                    ),
                ),
                readiness_digest=DIGEST_D, prior_committed_at=NOW,
            )
            publisher.apply_gap_ordered(
                _intent(foreign, ProjectionIntentKind.MIGRATION, foreign_carry, "1", contract_digest=DIGEST_C),
            )
            vis = DeliveryVisibilityIdentity(epoch="99", kind="delivery", id=digest_token(DIGEST_A, "delivery"))
            evidence, visibility, _ = _prepare(
                landing, contract, [{"key": "real", "accept": True}], visibility=vis,
            )
            materialized, dispositions = _materialize(
                landing, evidence, ("accepted",), visibility=visibility,
            )
            evaluation = RemediationEvaluation(
                schema="ergasterion.remediation-evaluation/v1", original_claim_digest=DIGEST_B,
                raw_receipt_digest=DIGEST_C, target_contract_digest=DIGEST_D, target_source_schema_digest=DIGEST_E,
                target_published_schema_digest=DIGEST_A, target_ruleset_digest=DIGEST_B, execution_plan_digest=DIGEST_C,
                root_visibility_epoch="2", remediation_evaluation_id=DIGEST_D,
            )
            release = RemediationRelease(
                schema="ergasterion.remediation-release/v1", remediation_evaluation_id=DIGEST_D,
                selected_locators=(dispositions[0].raw_locator,),
                accepted_content_digest=materialized.accepted_content_digest,
                release_id=DIGEST_E,
            )
            decision = RemediationDecision(
                schema="ergasterion.remediation-decision/v1", decision_id=DIGEST_A,
                kind=RemediationDecisionKind.RELEASED, evaluation=evaluation,
                disposition_ids=(dispositions[0].disposition_id,),
                validation_result_digest=DIGEST_B, release=release, decided_at=NOW,
            )
            _expect_error(
                "ancestry_mismatch",
                lambda: remediation.record_decision(decision),
                "another identity's epoch pair must not authorize a reset-root release",
            )
            own_carry = MigrationProjectionPayload(
                kind="migration",
                migration=Migration(
                    migration_id=DIGEST_E, kind=MigrationKind.CARRY, from_contract_digest=DIGEST_B,
                    to_contract_digest=DIGEST_C, activated_at=NOW, from_visibility_epoch="99",
                    to_visibility_epoch="2",
                ),
                version_interface=VersionInterface(
                    logical_identity=identity, product_version="1.1.0", contract_digest=DIGEST_C,
                    root_visibility_epoch="99", relation_ref="bronze.v1_1", active=True,
                ),
                ancestry=(
                    VisibilityAncestryRow(
                        logical_identity=identity, descendant_epoch="2", ancestor_epoch="2",
                        projection_target="bronze", projection_revision="1",
                    ),
                    VisibilityAncestryRow(
                        logical_identity=identity, descendant_epoch="2", ancestor_epoch="99",
                        projection_target="bronze", projection_revision="1",
                    ),
                ),
                readiness_digest=DIGEST_D, prior_committed_at=NOW,
            )
            publisher.apply_gap_ordered(
                _intent(identity, ProjectionIntentKind.MIGRATION, own_carry, "1", contract_digest=DIGEST_C),
            )
            recorded = remediation.record_decision(decision)
            assert recorded.decision_id == DIGEST_A
        finally:
            store.close()


def test_orphan_recovery_and_restart_against_same_file() -> None:
    contract = _managed_contract()
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "bronze.duckdb"
        store = DuckDBStore(path)
        landing = DuckDBLandingAdapter(store)
        try:
            receipt = _receipt()
            rows = [{"key": "a", "accept": True}]
            raw = json.dumps(rows).encode("utf-8")
            visibility = _visibility()
            preparation = landing.begin_prepare(DIGEST_A, receipt, _handle(receipt), contract, visibility)
            landing.append_raw(preparation, _page(raw))
            store.checkpoint()
        finally:
            store.close()
        reopened = DuckDBStore(path)
        landing2 = DuckDBLandingAdapter(reopened)
        try:
            again = landing2.begin_prepare(DIGEST_A, receipt, _handle(receipt), contract, visibility)
            assert again.preparation_id == preparation.preparation_id
            evidence = landing2.finish_prepare(again)
            page = landing2.read_candidate(CandidateReadQuery(
                evidence=evidence, after_sequence=None, max_frames=8, max_bytes="1000000",
            ))
            assert len(page.frames) == 1
        finally:
            reopened.close()


def test_snapshot_barrier_pointer_ordering_is_atomic() -> None:
    contract = _managed_contract()
    identity = contract.logical_identity
    with tempfile.TemporaryDirectory() as tmp:
        store, landing, _remediation, publisher, _sink = _bundle(tmp)
        try:
            _prepare(landing, contract, [{"key": "a", "accept": True}])
            first = _visibility(DIGEST_A)
            second = DeliveryVisibilityIdentity(epoch="0", kind="delivery", id=digest_token(DIGEST_B, "delivery"))
            publisher.apply_gap_ordered(_intent(identity, ProjectionIntentKind.DELIVERY_PUBLICATION, _publication_payload(first), "1"))
            status = json.loads(store.fetchone("SELECT json FROM stream_status")["json"])
            pointer = status["latest_snapshot_visibility"]["id"]
            ledger = publisher.published_visibility_set(identity, "bronze")
            assert pointer == first.id
            assert (first.epoch, first.kind, first.id) in ledger
            publisher.apply_gap_ordered(_intent(identity, ProjectionIntentKind.DELIVERY_PUBLICATION, _publication_payload(second), "2"))
            status = json.loads(store.fetchone("SELECT json FROM stream_status")["json"])
            assert status["latest_snapshot_visibility"]["id"] == second.id
            history = store.fetchall("SELECT visibility_id FROM snapshot_history")
            assert any(row["visibility_id"] == first.id for row in history)
            assert len(publisher.ledger_rows(identity, "bronze")) == 2
        finally:
            store.close()


def test_projection_corruption_rebuilds_while_bronze_partitions_remain() -> None:
    contract = _managed_contract()
    identity = contract.logical_identity
    with tempfile.TemporaryDirectory() as tmp:
        store, landing, _remediation, publisher, _sink = _bundle(tmp)
        try:
            evidence, visibility, _receipt = _prepare(landing, contract, [{"key": "a", "accept": True}])
            materialized, _dispositions = _materialize(landing, evidence, ("accepted",), visibility=visibility)
            intent = _intent(identity, ProjectionIntentKind.DELIVERY_PUBLICATION, _publication_payload(visibility, accepted_ref=materialized.accepted_ref), "1")
            confirmation = publisher.apply_gap_ordered(intent)
            bronze_before = store.fetchone("SELECT COUNT(*) AS n FROM candidate_frames")["n"]
            publisher.drop_projection_relations()
            assert store.fetchone("SELECT COUNT(*) AS n FROM candidate_frames")["n"] == bronze_before
            rebuilt = publisher.rebuild_read_models(ProjectionReplayBatch(
                intents=(intent,), confirmations=(confirmation,), max_items=16, max_bytes="1000000", bytes_supplied="0",
            ))
            assert rebuilt.projection_revision == "1"
            assert (visibility.epoch, visibility.kind, visibility.id) in publisher.published_visibility_set(identity, "bronze")
            store.execute("UPDATE quarantine_projection SET json = '{not-json}'")
            store.rebuild_quarantine_projection()
            restored = store.fetchone("SELECT json FROM quarantine_projection")
            assert restored is not None
            json.loads(restored["json"])
        finally:
            store.close()


def test_bronze_partition_or_file_loss_requires_restore_without_reprocessing_claim() -> None:
    contract = _managed_contract()
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "bronze.duckdb"
        store = DuckDBStore(path)
        landing = DuckDBLandingAdapter(store)
        evidence = None
        try:
            evidence, _visibility, _receipt = _prepare(landing, contract, [{"key": "a", "accept": True}])
            store.drop_relations(BRONZE_RELATIONS)
            _expect_error(
                "bronze_store_restore_required",
                lambda: landing.read_candidate(CandidateReadQuery(
                    evidence=evidence, after_sequence=None, max_frames=8, max_bytes="1000000",
                )),
                "bronze partition loss must demand restore",
            )
        finally:
            store.close()
        path.unlink()
        restored = DuckDBStore(path)
        landing2 = DuckDBLandingAdapter(restored)
        try:
            _expect_error(
                "bronze_store_restore_required",
                lambda: landing2.read_candidate(CandidateReadQuery(
                    evidence=evidence, after_sequence=None, max_frames=8, max_bytes="1000000",
                )),
                "whole-file loss must demand restore",
            )
            assert ReprocessingClaim.__name__ == "ReprocessingClaim"
        finally:
            restored.close()


def test_many_decisions_query_paging_snapshot_restart_and_digest_stability() -> None:
    contract = _managed_contract()
    identity = contract.logical_identity
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "bronze.duckdb"
        store = DuckDBStore(path)
        landing = DuckDBLandingAdapter(store)
        remediation = DuckDBRemediationRepository(store)
        try:
            evidence, _visibility, _receipt = _prepare(landing, contract, [{"key": "a", "accept": False}])
            _materialized, dispositions = _materialize(landing, evidence, ("rejected",))
            disposition_id = dispositions[0].disposition_id
            for index in range(8):
                evaluation = RemediationEvaluation(
                    schema="ergasterion.remediation-evaluation/v1", original_claim_digest=DIGEST_B,
                    raw_receipt_digest=DIGEST_C, target_contract_digest=DIGEST_D, target_source_schema_digest=DIGEST_E,
                    target_published_schema_digest=DIGEST_A, target_ruleset_digest=DIGEST_B,
                    execution_plan_digest=DIGEST_C, root_visibility_epoch="0",
                    remediation_evaluation_id=canonical_digest({"eval": index}),
                )
                remediation.record_decision(RemediationDecision(
                    schema="ergasterion.remediation-decision/v1",
                    decision_id=canonical_digest({"decision": index}),
                    kind=RemediationDecisionKind.EVALUATED, evaluation=evaluation,
                    disposition_ids=(disposition_id,), validation_result_digest=DIGEST_A,
                    release=None, decided_at=NOW,
                ))
            disp_page = landing.disposition_query(DispositionQuery(
                logical_identity=identity, disposition_id=disposition_id, authorization_context_ref="operator",
                snapshot_token=None, after_cursor=None, max_items=16, max_bytes="1000000",
            ))
            first = remediation.decision_query(RemediationDecisionQuery(
                logical_identity=identity, disposition_id=disposition_id, authorization_context_ref="operator",
                snapshot_token=None, after_cursor=None, max_items=3, max_bytes="8000",
            ))
            assert first.more is True
            assert len(first.items) <= 3
            digest_small = decision_query_digest(RemediationDecisionQuery(
                logical_identity=identity, disposition_id=disposition_id, authorization_context_ref="operator",
                snapshot_token=first.snapshot_token, after_cursor=None, max_items=3, max_bytes="8000",
            ))
            digest_large = decision_query_digest(RemediationDecisionQuery(
                logical_identity=identity, disposition_id=disposition_id, authorization_context_ref="operator",
                snapshot_token=first.snapshot_token, after_cursor=None, max_items=16, max_bytes="1000000",
            ))
            assert digest_small == digest_large
            store.checkpoint()
            disp_token, decision_token, cursor = disp_page.snapshot_token, first.snapshot_token, first.next_cursor
        finally:
            store.close()
        restarted = DuckDBStore(path)
        landing2 = DuckDBLandingAdapter(restarted)
        remediation2 = DuckDBRemediationRepository(restarted)
        try:
            second = remediation2.decision_query(RemediationDecisionQuery(
                logical_identity=identity, disposition_id=disposition_id, authorization_context_ref="operator",
                snapshot_token=decision_token, after_cursor=cursor, max_items=16, max_bytes="2000",
            ))
            assert second.snapshot_token == decision_token
            assert second.items
            seen = {item.decision_id for item in first.items} | {item.decision_id for item in second.items}
            while second.more:
                second = remediation2.decision_query(RemediationDecisionQuery(
                    logical_identity=identity, disposition_id=disposition_id, authorization_context_ref="operator",
                    snapshot_token=decision_token, after_cursor=second.next_cursor, max_items=3, max_bytes="8000",
                ))
                seen.update(item.decision_id for item in second.items)
            assert len(seen) == 8
            _expect_error(
                "access_denied",
                lambda: remediation2.decision_query(RemediationDecisionQuery(
                    logical_identity=identity, disposition_id=DIGEST_A, authorization_context_ref="operator",
                    snapshot_token=decision_token, after_cursor=None, max_items=3, max_bytes="8000",
                )),
                "filter mismatch must invalidate the snapshot",
            )
            _expect_error(
                "access_denied",
                lambda: remediation2.decision_query(RemediationDecisionQuery(
                    logical_identity=identity, disposition_id=disposition_id, authorization_context_ref="other",
                    snapshot_token=decision_token, after_cursor=None, max_items=3, max_bytes="8000",
                )),
                "authorization mismatch must invalidate the snapshot",
            )
            _expect_error(
                "not_found",
                lambda: remediation2.decision_query(RemediationDecisionQuery(
                    logical_identity=identity, disposition_id=disposition_id, authorization_context_ref="operator",
                    snapshot_token="s-missing", after_cursor=None, max_items=3, max_bytes="8000",
                )),
                "unknown snapshot token must fail",
            )
            landing2.disposition_query(DispositionQuery(
                logical_identity=identity, disposition_id=disposition_id, authorization_context_ref="operator",
                snapshot_token=disp_token, after_cursor=None, max_items=16, max_bytes="1000000",
            ))
        finally:
            restarted.close()


def test_lifecycle_events_and_raw_bytes_stay_out_of_projections() -> None:
    contract = _managed_contract()
    identity = contract.logical_identity
    with tempfile.TemporaryDirectory() as tmp:
        store, landing, _remediation, publisher, sink = _bundle(tmp)
        try:
            payload_bytes = json.dumps([{"key": "secret-row", "accept": True}]).encode("utf-8")
            evidence, visibility, _receipt = _prepare(landing, contract, [{"key": "secret-row", "accept": True}])
            materialized, _dispositions = _materialize(landing, evidence, ("accepted",), visibility=visibility)
            intent = _intent(identity, ProjectionIntentKind.DELIVERY_PUBLICATION, _publication_payload(visibility, accepted_ref=materialized.accepted_ref), "1")
            confirmation = publisher.apply_gap_ordered(intent)
            from ergasterion.ingestion.records import Attempt
            attempt = Attempt(
                run_id=DIGEST_A, attempt_id=DIGEST_B, logical_identity=identity, claim_digest=DIGEST_C,
                scheduled_boundary_at=NOW, attempt_ordinal=1, state="committed", block_phase=None,
                reason_code=None, execution_plan_digest=DIGEST_D, runtime_manifest_digest=DIGEST_E,
                state_revision="1",
            )
            event = LifecycleEvent(
                event_id=DIGEST_A, event_type=LifecycleEventType.COMMITTED, logical_identity=identity,
                state_revision="1", event_ordinal="1", attempt_id=attempt.attempt_id,
                execution_plan_digest=DIGEST_D, runtime_manifest_digest=DIGEST_E,
                payload=AttemptLifecyclePayload(kind="committed", attempt=attempt, projection_confirmation=confirmation),
                payload_digest=canonical_digest({"attempt": attempt.attempt_id}), created_at=NOW,
            )
            assert sink.project_events(LifecycleEventBatch(events=(event,), max_items=1, bytes_supplied="0")) == (event.event_id,)
            assert sink.project_events(LifecycleEventBatch(events=(event,), max_items=1, bytes_supplied="0")) == (event.event_id,)
            conflicted = event.model_copy(update={"payload_digest": DIGEST_B})
            _expect_error(
                "event_conflict",
                lambda: sink.project_events(LifecycleEventBatch(events=(conflicted,), max_items=1, bytes_supplied="0")),
                "reused event id with different payload must conflict",
            )
            page = sink.evidence_query(EvidenceQuery(
                logical_identity=identity, evidence_kind=EvidenceKind.ATTEMPT, immutable_id=None,
                authorization_context_ref="operator", after_cursor=None, max_items=16, max_bytes="1000000",
            ))
            assert page.items and page.items[0].kind == "attempt"
            projection_json = dumps(publisher.ledger_rows(identity, "bronze")[0])
            assert "secret-row" not in projection_json
            stream_json = store.fetchone("SELECT json FROM stream_status")["json"]
            assert payload_bytes.decode("utf-8") not in stream_json
            assert payload_bytes.decode("utf-8") not in projection_json
        finally:
            store.close()


TESTS = [
    test_duckdb_adapters_satisfy_port_protocols,
    test_adapter_conformance_vectors_pass_against_duckdb,
    test_exercise_all_operations_with_duckdb_ports,
    test_prepare_typed_failure_and_rule_invalid_exclusion,
    test_tagged_visibility_collision_and_remediation_release,
    test_replay_conflict_gap_revision_atomic_projection_and_unpublished_exclusion,
    test_visibility_ancestry_and_versioned_interfaces_coexist_across_reset,
    test_reset_retires_prior_version_interface_json_active,
    test_release_ancestry_lookup_scopes_claim_identity_candidate,
    test_release_ancestry_lookup_scopes_evaluation_identity,
    test_orphan_recovery_and_restart_against_same_file,
    test_snapshot_barrier_pointer_ordering_is_atomic,
    test_projection_corruption_rebuilds_while_bronze_partitions_remain,
    test_bronze_partition_or_file_loss_requires_restore_without_reprocessing_claim,
    test_many_decisions_query_paging_snapshot_restart_and_digest_stability,
    test_lifecycle_events_and_raw_bytes_stay_out_of_projections,
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
