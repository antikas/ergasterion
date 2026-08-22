"""Assert-script tests for Bronze validation, lineage, metadata and publication.

Proves the quality arithmetic, prohibited partial modes, streaming uniqueness
spill, metadata-only diagnostics, same-ruleset revalidation, selected-locator
remediation, whole-delivery reprocessing admission, digest binding, derived
lineage, intent-before-confirmation ordering and mandatory graph occurrence
checks. Repo convention: no pytest.

Usage:
    python tests/python/test_bronze_patterns.py
"""

from __future__ import annotations

import json
import traceback
from dataclasses import dataclass, field
from pathlib import Path

if __package__ in (None, ""):
    import os as _os
    import sys as _sys
    _sys.path.insert(0, _os.path.dirname(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))))

from ergasterion.framework.bronze_contract import (
    AcceptedValuesRule,
    BronzeProductContract,
    DeliveryMode,
    Finding,
    LifecycleEventType,
    MigrationKind,
    NotNullRule,
    OpaqueBatchProgress,
    ProcessingOutcome,
    PublicationDecision,
    PublicationPolicy,
    RawLocator,
    ReadinessResult,
    RemediationActionStatus,
    RowCountRule,
    Severity,
    TypedInt64,
    TypedString,
    UniqueKeyRule,
)
from ergasterion.framework.runtime_binding import InterfaceReadiness
from ergasterion.ingestion.conformance import contract_variant
from ergasterion.ingestion.evidence import b64url_decode
from ergasterion.ingestion.lifecycle import (
    MANDATORY_OCCURRENCE_IDS,
    bind_deletion_evidence,
    bronze_execution_plan,
    build_lineage_descriptor,
    build_product_metadata,
    build_run_lineage,
    contract_digests,
    derive_field_lineage,
    observer_event_order,
    require_mandatory_graph,
    require_observer_order,
)
from ergasterion.ingestion.publication import (
    admit_selected_locator_release,
    admit_whole_delivery_reprocessing,
    build_delivery_publication_payload,
    build_projection_intent,
    confirm_projection,
    published_ledger_row,
    records_bind_contract_and_schemas,
    release_id_for,
    require_publication_barrier,
)
from ergasterion.ingestion.records import (
    CandidateField,
    CandidateFrame,
    DeletionEvidenceIntent,
    DeliveryVisibilityIdentity,
    OpaqueProgressClaim,
    ProjectionIntentKind,
    ReprocessingClaim,
    ScratchChunk,
    ScratchReadPage,
    ScratchScope,
    UnitResult,
)
from ergasterion.ingestion.runtime import PortError, canonical_digest
from ergasterion.ingestion.validation import (
    decide_publication,
    diagnostics_are_metadata_only,
    revalidate_frames,
    validate_frames,
)

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
VECTORS_PATH = REPO_ROOT / "tests" / "fixtures" / "bronze_pattern_vectors.json"
SCHEMA_VECTORS_PATH = REPO_ROOT / "tests" / "fixtures" / "bronze_schema_vectors.json"
NOW = "2026-01-01T00:00:00.000000Z"
DIGEST_A = "a" * 64
DIGEST_B = "b" * 64


@dataclass
class PagingScratchStore:
    """Scratch store that honours ``max_bytes`` and pages sequential reads.

    A fake that ignores ``max_bytes`` and returns every remaining chunk at once
    cannot prove uniqueness merge stayed inside the declared bound.
    """

    _scopes: dict[str, ScratchScope] = field(default_factory=dict)
    _owner: dict[str, str] = field(default_factory=dict)
    _closed: set[str] = field(default_factory=set)
    _next_sequence: dict[str, int] = field(default_factory=dict)
    _used_bytes: dict[str, int] = field(default_factory=dict)
    _chunks: dict[str, list[ScratchChunk]] = field(default_factory=dict)
    page_bytes: list[int] = field(default_factory=list)
    max_read_bytes: int = 0
    paged: bool = False

    def create_scope(self, attempt_id: str, capacity_bytes: str) -> ScratchScope:
        scope_id = f"scope-{len(self._scopes):04d}"
        scope = ScratchScope(scope_id=scope_id, attempt_id=attempt_id, capacity_bytes=capacity_bytes)
        self._scopes[scope_id] = scope
        self._owner[scope_id] = attempt_id
        self._next_sequence[scope_id] = 0
        self._used_bytes[scope_id] = 0
        self._chunks[scope_id] = []
        return scope

    def write_sequential(self, attempt_id: str, chunk: ScratchChunk) -> UnitResult:
        scope_id = chunk.scope_id
        if scope_id not in self._scopes or self._owner[scope_id] != attempt_id:
            raise PortError("scope_owner_mismatch", scope_id)
        if scope_id in self._closed:
            raise PortError("scope_closed", scope_id)
        if int(chunk.sequence) != self._next_sequence[scope_id]:
            raise PortError("sequence_conflict", scope_id)
        raw_len = len(b64url_decode(chunk.bytes_base64url))
        capacity = int(self._scopes[scope_id].capacity_bytes)
        if self._used_bytes[scope_id] + raw_len > capacity:
            raise PortError("capacity_exceeded", scope_id)
        self._used_bytes[scope_id] += raw_len
        self._next_sequence[scope_id] += 1
        self._chunks[scope_id].append(chunk)
        return UnitResult(ok=True)

    def read_sequential(self, attempt_id: str, scope_id: str, after_sequence: str, max_bytes: str) -> ScratchReadPage:
        if scope_id not in self._scopes or self._owner[scope_id] != attempt_id:
            raise PortError("scope_owner_mismatch" if scope_id in self._scopes else "not_found", scope_id)
        if scope_id not in self._closed:
            raise PortError("scope_open", scope_id)
        budget = int(max_bytes)
        self.max_read_bytes = max(self.max_read_bytes, budget)
        selected: list[ScratchChunk] = []
        returned = 0
        next_sequence = None
        remaining = [chunk for chunk in self._chunks[scope_id] if int(chunk.sequence) > int(after_sequence)]
        for chunk in remaining:
            payload_len = len(b64url_decode(chunk.bytes_base64url))
            if payload_len > budget:
                raise PortError("item_too_large", scope_id)
            if returned + payload_len > budget and selected:
                next_sequence = chunk.sequence
                self.paged = True
                break
            if returned + payload_len > budget:
                raise PortError("item_too_large", scope_id)
            selected.append(chunk)
            returned += payload_len
        self.page_bytes.append(returned)
        payload: dict = {"chunks": tuple(selected), "bytes_returned": str(returned)}
        if next_sequence is not None:
            payload["next_sequence"] = next_sequence
        return ScratchReadPage.model_validate(payload)

    def close_scope(self, attempt_id: str, scope_id: str) -> UnitResult:
        if scope_id not in self._scopes or self._owner[scope_id] != attempt_id:
            raise PortError("scope_owner_mismatch", scope_id)
        self._closed.add(scope_id)
        return UnitResult(ok=True)

    def delete_scope(self, attempt_id: str, scope_id: str) -> UnitResult:
        if scope_id not in self._scopes or self._owner[scope_id] != attempt_id:
            raise PortError("scope_owner_mismatch", scope_id)
        del self._scopes[scope_id]
        return UnitResult(ok=True)

    def cleanup_orphans(self, active_attempt_ids: tuple[str, ...], max_scopes: int) -> tuple[str, ...]:
        orphans = [sid for sid, owner in self._owner.items() if owner not in active_attempt_ids and sid in self._scopes]
        removed = tuple(orphans[:max_scopes])
        for sid in removed:
            del self._scopes[sid]
        return removed


def _vectors() -> dict:
    return json.loads(VECTORS_PATH.read_text(encoding="utf-8"))


def _sample_contract() -> BronzeProductContract:
    document = json.loads(SCHEMA_VECTORS_PATH.read_text(encoding="utf-8"))
    for vector in document["positive"]:
        if vector["record"] == "BronzeProductContract":
            return BronzeProductContract.model_validate(vector["payload"])
    raise AssertionError("no BronzeProductContract positive vector found")


def _append_batch_contract(base: BronzeProductContract | None = None) -> BronzeProductContract:
    contract = contract_variant(
        base or _sample_contract(),
        integration_kind="managed",
        publication_mode=PublicationPolicy.PUBLISH_VALID_ROWS,
        delivery_mode=DeliveryMode.APPEND_ONLY,
    )
    delivery = contract.delivery.model_copy(update={
        "progress": OpaqueBatchProgress(kind="opaque_batch"),
        "delete_strategy": "none",
        "quality": contract.delivery.quality.model_copy(update={"max_error_fraction": "0.5"}),
    })
    return contract.model_copy(update={"delivery": delivery})


def _frame(sequence: int, values: dict[str, object], findings: tuple[Finding, ...] = ()) -> CandidateFrame:
    fields = []
    for name, value in values.items():
        if isinstance(value, int):
            typed = TypedInt64(logical_type="int64", value=str(value))
        elif value is None:
            typed = None
        else:
            typed = TypedString(logical_type="utf8_string", value=str(value))
        fields.append(CandidateField(name=name, value=typed))
    return CandidateFrame(
        frame_sequence=str(sequence),
        raw_locator=RawLocator(
            frame_sequence=str(sequence), byte_offset=str(sequence), byte_length="8",
            line_number=str(sequence + 1),
        ),
        typed_fields=tuple(fields),
        structural_findings=findings,
    )


def _expect_error(code: str, fn, message: str) -> PortError:
    try:
        fn()
    except PortError as exc:
        assert exc.code == code, f"{message}: expected {code!r}, got {exc.code!r} ({exc.detail})"
        return exc
    raise AssertionError(message)


def _readiness(contract: BronzeProductContract, manifest: str = DIGEST_A) -> InterfaceReadiness:
    contract_digest, source_schema_digest, published_schema_digest = contract_digests(contract)
    return InterfaceReadiness(
        schema="ergasterion.interface-readiness/v1",
        logical_identity=contract.logical_identity,
        projection_target="bronze",
        runtime_manifest_digest=manifest,
        contract_digest=contract_digest,
        source_schema_digest=source_schema_digest,
        published_schema_digest=published_schema_digest,
        version_interface_ref="bronze.v1",
        capability_digest=DIGEST_B,
        classification=contract.product.classification,
        access_policy_ref=contract.product.access_policy_ref,
        retention_policy_ref=contract.product.retention_policy_ref,
        protection_profile="synthetic-local-only",
        result=ReadinessResult.READY,
        readiness_digest=canonical_digest({
            "schema": "ergasterion.interface-readiness/v1",
            "contract_digest": contract_digest,
            "source_schema_digest": source_schema_digest,
            "published_schema_digest": published_schema_digest,
            "runtime_manifest_digest": manifest,
        }),
        verified_at=NOW,
        revoked_at=None,
    )


def test_quality_arithmetic_vectors() -> None:
    for vector in _vectors()["quality_arithmetic"]:
        decision = decide_publication(
            publication_mode=PublicationPolicy(vector["publication_mode"]),
            max_error_fraction=vector["max_error_fraction"],
            delivery_mode=DeliveryMode(vector["delivery_mode"]),
            progress_kind=vector["progress_kind"],
            framed_count=vector["framed_count"],
            error_numerator=vector["error_numerator"],
            passing_count=vector["passing_count"],
            batch_error=vector["batch_error"],
        )
        assert decision.value == vector["expected_decision"], (
            f"{vector['id']}: expected {vector['expected_decision']!r}, got {decision.value!r}"
        )


def test_prohibited_partial_modes() -> None:
    for vector in _vectors()["prohibited_partial_modes"]:
        _expect_error(
            "invalid_config",
            lambda v=vector: decide_publication(
                publication_mode=PublicationPolicy.PUBLISH_VALID_ROWS,
                max_error_fraction="0.5",
                delivery_mode=DeliveryMode(v["delivery_mode"]),
                progress_kind=v["progress_kind"],
                framed_count=4,
                error_numerator=1,
                passing_count=3,
                batch_error=False,
            ),
            vector["id"],
        )


def test_all_or_nothing_never_emits_partial_decision() -> None:
    _expect_error(
        "invalid_config",
        lambda: decide_publication(
            publication_mode=PublicationPolicy.ALL_OR_NOTHING,
            max_error_fraction="0.1",
            delivery_mode=DeliveryMode.CDC,
            progress_kind="sequence",
            framed_count=4,
            error_numerator=0,
            passing_count=4,
            batch_error=False,
        ),
        "all_or_nothing with nonzero max_error_fraction",
    )


def test_authored_rules_and_snapshot_acceptance() -> None:
    contract = _sample_contract()
    quality = contract.delivery.quality.model_copy(update={
        "rules": (
            NotNullRule(kind="not_null", field="acct_id", severity=Severity.ERROR),
            UniqueKeyRule(kind="unique_key", fields=("acct_id",), severity=Severity.ERROR),
            AcceptedValuesRule(
                kind="accepted_values", field="acct_id",
                values=(TypedString(logical_type="utf8_string", value="ok"),),
                allow_null=False, severity=Severity.WARN,
            ),
            RowCountRule(kind="row_count", min="1", max="10", severity=Severity.ERROR),
        ),
    })
    contract = contract.model_copy(update={"delivery": contract.delivery.model_copy(update={"quality": quality})})
    frames = (
        _frame(0, {"acct_id": "ok", "is_deleted": None}),
        _frame(1, {"acct_id": "ok", "is_deleted": None}),
        _frame(2, {"acct_id": None, "is_deleted": None}),
    )
    outcome = validate_frames(
        contract, frames, claim_digest=DIGEST_A, delivery_id="d1",
        evaluation_id=DIGEST_B, memory_budget_bytes=65536,
    )
    assert outcome.validation.publication_decision is PublicationDecision.REJECT_DELIVERY
    assert outcome.validation.error_numerator == "3"
    assert outcome.validation.error_denominator == "3"
    duplicate_ids = [
        item.disposition_id for item in outcome.dispositions
        if any(finding.metadata.diagnostic_code.value == "duplicate_key" for finding in item.findings)
    ]
    assert len(duplicate_ids) == 2, "every member of an in-delivery duplicate group fails"
    nulls = [
        item for item in outcome.dispositions
        if any(finding.metadata.diagnostic_code.value == "null_not_allowed" for finding in item.findings)
    ]
    assert len(nulls) == 1
    snapshot_contract = contract_variant(contract, delivery_mode=DeliveryMode.COMPLETE_SNAPSHOT)
    snapshot_delivery = snapshot_contract.delivery.model_copy(update={
        "progress": OpaqueBatchProgress(kind="opaque_batch"),
        "delete_strategy": "snapshot_diff",
        "quality": snapshot_contract.delivery.quality.model_copy(update={
            "publication_mode": PublicationPolicy.ALL_OR_NOTHING, "max_error_fraction": "0",
        }),
    })
    snapshot_contract = snapshot_contract.model_copy(update={"delivery": snapshot_delivery})
    snap = validate_frames(
        snapshot_contract, frames, claim_digest=DIGEST_A, delivery_id="d1",
        evaluation_id=DIGEST_B, memory_budget_bytes=65536,
    )
    assert snap.snapshot_acceptance is not None
    assert snap.snapshot_acceptance.source_snapshot_complete is True
    assert snap.snapshot_acceptance.accepted_snapshot_complete is False
    assert snap.validation.publication_decision is PublicationDecision.REJECT_DELIVERY


def test_warnings_do_not_quarantine() -> None:
    contract = _append_batch_contract()
    quality = contract.delivery.quality.model_copy(update={
        "rules": (
            AcceptedValuesRule(
                kind="accepted_values", field="acct_id",
                values=(TypedString(logical_type="utf8_string", value="keep"),),
                allow_null=False, severity=Severity.WARN,
            ),
        ),
        "max_error_fraction": "0.5",
    })
    contract = contract.model_copy(update={"delivery": contract.delivery.model_copy(update={"quality": quality})})
    frames = (_frame(0, {"acct_id": "keep"}), _frame(1, {"acct_id": "other"}))
    outcome = validate_frames(
        contract, frames, claim_digest=DIGEST_A, delivery_id="d1",
        evaluation_id=DIGEST_B, memory_budget_bytes=65536,
    )
    assert outcome.validation.publication_decision is PublicationDecision.PUBLISH_ALL
    assert outcome.validation.quarantined_count == "0"
    assert outcome.validation.warning_count == "1"
    assert all(item.status.value == "accepted" for item in outcome.dispositions)


def test_spillable_uniqueness_stays_within_memory_bound() -> None:
    contract = _append_batch_contract()
    quality = contract.delivery.quality.model_copy(update={
        "rules": (UniqueKeyRule(kind="unique_key", fields=("acct_id",), severity=Severity.ERROR),),
        "max_error_fraction": "0.5",
    })
    contract = contract.model_copy(update={"delivery": contract.delivery.model_copy(update={"quality": quality})})
    bound = _vectors()["memory_bound"]["budget_bytes"]
    frame_count = _vectors()["memory_bound"]["frame_count"]
    frames = tuple(_frame(index, {"acct_id": f"row-{index:04d}"}) for index in range(frame_count))
    frames = frames + (_frame(frame_count, {"acct_id": "row-0001"}),)
    scratch = PagingScratchStore()
    outcome = validate_frames(
        contract, frames, claim_digest=DIGEST_A, delivery_id="d1",
        evaluation_id=DIGEST_B, memory_budget_bytes=bound, scratch_store=scratch, attempt_id=DIGEST_A,
    )
    assert outcome.peak_memory_bytes <= bound, (
        f"uniqueness working set {outcome.peak_memory_bytes} exceeded bound {bound}"
    )
    assert outcome.peak_memory_bytes > 96, (
        "peak_memory_bytes must meter encoded working-set bytes, not a 96-byte observe slot"
    )
    assert outcome.spilled_uniqueness is True
    assert scratch.max_read_bytes <= bound, (
        f"scratch reads requested {scratch.max_read_bytes} bytes, above bound {bound}"
    )
    assert scratch.paged is True, "scratch must page uniqueness chunks instead of returning all at once"
    assert scratch.page_bytes, "scratch uniqueness merge must read paged chunks"
    assert all(nbytes <= bound for nbytes in scratch.page_bytes)
    dupes = [
        item for item in outcome.dispositions
        if any(finding.metadata.diagnostic_code.value == "duplicate_key" for finding in item.findings)
    ]
    assert len(dupes) == 2


def test_metadata_only_diagnostics_cannot_leak() -> None:
    contract = _append_batch_contract()
    secret = _vectors()["forbidden_tokens"][0]
    membership = _vectors()["forbidden_tokens"][1]
    frames = (
        _frame(0, {"acct_id": secret, "is_deleted": None}),
        _frame(1, {"acct_id": membership, "is_deleted": None}),
        _frame(2, {"acct_id": None, "is_deleted": None}),
    )
    outcome = validate_frames(
        contract, frames, claim_digest=DIGEST_A, delivery_id="d1",
        evaluation_id=DIGEST_B, memory_budget_bytes=65536,
    )
    leaks = diagnostics_are_metadata_only(outcome.dispositions, _vectors()["forbidden_tokens"])
    leaks += diagnostics_are_metadata_only(outcome.validation, _vectors()["forbidden_tokens"])
    assert leaks == [], leaks
    dumped = json.dumps([item.model_dump(mode="json") for item in outcome.dispositions])
    for token in _vectors()["forbidden_tokens"]:
        assert token not in dumped


def test_same_ruleset_revalidation_cannot_override() -> None:
    contract = _append_batch_contract()
    frames = (_frame(0, {"acct_id": None}), _frame(1, {"acct_id": "ok"}))
    first = validate_frames(
        contract, frames, claim_digest=DIGEST_A, delivery_id="d1",
        evaluation_id=DIGEST_B, memory_budget_bytes=65536,
    )
    status, second = revalidate_frames(
        contract, frames, prior_ruleset_digest=first.validation.ruleset_digest,
        prior_dispositions=first.dispositions, claim_digest=DIGEST_A, delivery_id="d1",
        evaluation_id=DIGEST_B, memory_budget_bytes=65536,
    )
    assert status is RemediationActionStatus.UNCHANGED_FINDING
    assert second.validation.ruleset_digest == first.validation.ruleset_digest
    recovered = (
        _frame(0, {"acct_id": "fixed"}),
        _frame(1, {"acct_id": "ok"}),
    )
    _expect_error(
        "decision_conflict",
        lambda: revalidate_frames(
            contract, recovered, prior_ruleset_digest=first.validation.ruleset_digest,
            prior_dispositions=first.dispositions, claim_digest=DIGEST_A, delivery_id="d1",
            evaluation_id=DIGEST_B, memory_budget_bytes=65536,
        ),
        "same-ruleset revalidation override",
    )


def test_selected_locator_remediation_releases_once() -> None:
    contract = _append_batch_contract()
    original_frames = (
        _frame(0, {"acct_id": "keep-a"}),
        _frame(1, {"acct_id": None}),
        _frame(2, {"acct_id": "keep-b"}),
        _frame(3, {"acct_id": None}),
    )
    original = validate_frames(
        contract, original_frames, claim_digest=DIGEST_A, delivery_id="d1",
        evaluation_id=DIGEST_B, memory_budget_bytes=65536,
    )
    assert original.validation.publication_decision is PublicationDecision.PUBLISH_VALID_ROWS
    recovered = (
        _frame(0, {"acct_id": "keep-a"}),
        _frame(1, {"acct_id": "fixed-c"}),
        _frame(2, {"acct_id": "keep-b"}),
        _frame(3, {"acct_id": "fixed-d"}),
    )
    post = validate_frames(
        contract, recovered, claim_digest=DIGEST_A, delivery_id="d1",
        evaluation_id=DIGEST_B, memory_budget_bytes=65536,
    )
    locators = (recovered[1].raw_locator, recovered[3].raw_locator)
    admitted = admit_selected_locator_release(
        contract=contract, prior_decision=original.validation.publication_decision,
        visibility_epoch="2", active_root_epoch="1", migration_kind=MigrationKind.CARRY,
        selected_locators=locators, max_locators=10, already_released_locators=(),
        evaluation=post.validation, dispositions=post.dispositions,
        original_denominator=original.validation.error_denominator,
    )
    assert tuple(item.frame_sequence for item in admitted) == ("1", "3")
    first_id = release_id_for(DIGEST_A, admitted, DIGEST_B)
    second_id = release_id_for(DIGEST_A, tuple(reversed(locators)), DIGEST_B)
    assert first_id == second_id, "release identity is independent of selected-locator order"
    _expect_error(
        "release_conflict",
        lambda: admit_selected_locator_release(
            contract=contract, prior_decision=PublicationDecision.PUBLISH_VALID_ROWS,
            visibility_epoch="2", active_root_epoch="1", migration_kind=MigrationKind.CARRY,
            selected_locators=locators[:1], max_locators=10, already_released_locators=admitted,
            evaluation=post.validation, dispositions=post.dispositions,
            original_denominator=original.validation.error_denominator,
        ),
        "overlapping release",
    )
    _expect_error(
        "ancestry_mismatch",
        lambda: admit_selected_locator_release(
            contract=contract, prior_decision=PublicationDecision.PUBLISH_VALID_ROWS,
            visibility_epoch="0", active_root_epoch="1", migration_kind=MigrationKind.RESET,
            selected_locators=locators, max_locators=10, already_released_locators=(),
            evaluation=post.validation, dispositions=post.dispositions,
            original_denominator=original.validation.error_denominator,
        ),
        "reset root cannot release old locators",
    )
    cdc = contract_variant(_sample_contract(), delivery_mode=DeliveryMode.CDC)
    _expect_error(
        "invalid_config",
        lambda: admit_selected_locator_release(
            contract=cdc, prior_decision=PublicationDecision.PUBLISH_VALID_ROWS,
            visibility_epoch="1", active_root_epoch="1", migration_kind=MigrationKind.CARRY,
            selected_locators=locators, max_locators=10, already_released_locators=(),
            evaluation=post.validation, dispositions=post.dispositions,
            original_denominator=original.validation.error_denominator,
        ),
        "CDC cannot selected-locator release",
    )


def test_selected_locator_cannot_game_unique_key_or_row_count() -> None:
    spec = _vectors()["selected_locator_gaming"]
    contract = _append_batch_contract()
    quality = contract.delivery.quality.model_copy(update={
        "rules": (
            UniqueKeyRule(kind="unique_key", fields=tuple(spec["unique_key_fields"]), severity=Severity.ERROR),
            RowCountRule(kind="row_count", min=str(spec["original_denominator"]), severity=Severity.ERROR),
        ),
        "max_error_fraction": "0.5",
    })
    contract = contract.model_copy(update={"delivery": contract.delivery.model_copy(update={"quality": quality})})
    frames = (
        _frame(0, {"acct_id": "dup"}),
        _frame(1, {"acct_id": "dup"}),
        _frame(2, {"acct_id": "keep-b"}),
        _frame(3, {"acct_id": "keep-c"}),
    )
    full = validate_frames(
        contract, frames, claim_digest=DIGEST_A, delivery_id="d1",
        evaluation_id=DIGEST_B, memory_budget_bytes=65536,
    )
    assert full.validation.error_denominator == str(spec["original_denominator"])
    assert full.validation.publication_decision is PublicationDecision.PUBLISH_VALID_ROWS
    subset_frames = (frames[2], frames[3])
    subset = validate_frames(
        contract, subset_frames, claim_digest=DIGEST_A, delivery_id="d1",
        evaluation_id=DIGEST_B, memory_budget_bytes=65536,
    )
    assert subset.validation.framed_count == str(spec["subset_framed_count"])
    _expect_error(
        "decision_conflict",
        lambda: admit_selected_locator_release(
            contract=contract, prior_decision=PublicationDecision.PUBLISH_VALID_ROWS,
            visibility_epoch="2", active_root_epoch="1", migration_kind=MigrationKind.CARRY,
            selected_locators=(frames[2].raw_locator, frames[3].raw_locator),
            max_locators=10, already_released_locators=(),
            evaluation=subset.validation, dispositions=subset.dispositions,
            original_denominator=full.validation.error_denominator,
        ),
        "subset evaluation cannot replace the original denominator",
    )
    admitted = admit_selected_locator_release(
        contract=contract, prior_decision=PublicationDecision.PUBLISH_VALID_ROWS,
        visibility_epoch="2", active_root_epoch="1", migration_kind=MigrationKind.CARRY,
        selected_locators=(frames[0].raw_locator, frames[2].raw_locator, frames[3].raw_locator),
        max_locators=10, already_released_locators=(),
        evaluation=full.validation, dispositions=full.dispositions,
        original_denominator=full.validation.error_denominator,
    )
    assert tuple(item.frame_sequence for item in admitted) == ("2", "3"), (
        "duplicate unique_key members must not be released even if selected"
    )


def test_whole_delivery_reprocessing_requires_unpublished_original_claim() -> None:
    claim = ReprocessingClaim(
        schema="ergasterion.reprocessing-claim/v1",
        original_claim_digest=DIGEST_A, raw_receipt_digest=DIGEST_B,
        target_product_version="1.0.0", target_contract_digest=DIGEST_A,
        target_source_schema_digest=DIGEST_A, target_published_schema_digest=DIGEST_A,
        target_ruleset_digest=DIGEST_A, execution_plan_digest=DIGEST_A,
        reprocessing_id=DIGEST_B,
    )
    admit_whole_delivery_reprocessing(
        claim, original_claim_digest=DIGEST_A, prior_publication_decisions=(PublicationDecision.REJECT_DELIVERY,),
    )
    _expect_error(
        "claim_conflict",
        lambda: admit_whole_delivery_reprocessing(
            claim, original_claim_digest=DIGEST_B, prior_publication_decisions=(),
        ),
        "wrong original claim",
    )
    _expect_error(
        "claim_conflict",
        lambda: admit_whole_delivery_reprocessing(
            claim, original_claim_digest=DIGEST_A,
            prior_publication_decisions=(PublicationDecision.PUBLISH_ALL,),
        ),
        "successful publication blocks reprocessing",
    )
    _expect_error(
        "claim_conflict",
        lambda: admit_whole_delivery_reprocessing(
            claim, original_claim_digest=DIGEST_A,
            prior_publication_decisions=(PublicationDecision.PUBLISH_VALID_ROWS,),
        ),
        "partial publication blocks reprocessing",
    )


def test_derived_lineage_is_deterministic_and_binds_digests() -> None:
    contract = _sample_contract()
    plan = bronze_execution_plan(contract)
    require_mandatory_graph(plan)
    first = build_lineage_descriptor(contract, plan.execution_plan_digest)
    second = build_lineage_descriptor(contract, plan.execution_plan_digest)
    assert first.lineage_digest == second.lineage_digest
    assert tuple(entry.source for entry in derive_field_lineage(contract)) == tuple(
        entry.source for entry in contract.projection
    )
    run = build_run_lineage(
        contract=contract, run_id=DIGEST_A, attempt_id=DIGEST_B, delivery_id="d1",
        reprocessing_id=None, remediation_evaluation_id=None, transport_payload_digest=DIGEST_A,
        delivery_claim_digest=DIGEST_A, ruleset_digest=DIGEST_A, validation_result_digest=DIGEST_A,
        accepted_count="2", quarantined_count="0", execution_plan_digest=plan.execution_plan_digest,
        runtime_manifest_digest=DIGEST_A, landing_ref="landing", confirmation=None,
        result=ProcessingOutcome.IN_PROGRESS, committed_at=None,
    )
    records_bind_contract_and_schemas(contract, (run, first, plan, build_product_metadata(contract, latest_stream_status_ref="stream")))
    _expect_error(
        "intent_conflict",
        lambda: build_run_lineage(
            contract=contract, run_id=DIGEST_A, attempt_id=DIGEST_B, delivery_id="d1",
            reprocessing_id=None, remediation_evaluation_id=None, transport_payload_digest=DIGEST_A,
            delivery_claim_digest=DIGEST_A, ruleset_digest=DIGEST_A, validation_result_digest=DIGEST_A,
            accepted_count="2", quarantined_count="0", execution_plan_digest=plan.execution_plan_digest,
            runtime_manifest_digest=DIGEST_A, landing_ref="landing", confirmation=None,
            result=ProcessingOutcome.COMMITTED, committed_at=NOW,
        ),
        "final run lineage without confirmation",
    )


def test_intent_precedes_confirmation_and_lineage_follows() -> None:
    contract = _append_batch_contract()
    plan = bronze_execution_plan(contract)
    readiness = _readiness(contract)
    frames = (_frame(0, {"acct_id": "a"}), _frame(1, {"acct_id": "b"}))
    outcome = validate_frames(
        contract, frames, claim_digest=DIGEST_A, delivery_id="d1",
        evaluation_id=DIGEST_B, memory_budget_bytes=65536,
    )
    require_publication_barrier(plan, readiness, outcome.validation, contract, DIGEST_A)
    visibility = DeliveryVisibilityIdentity(epoch="0", kind="delivery", id="delivery-a")
    payload = build_delivery_publication_payload(
        contract=contract, attempt_id=DIGEST_B, visibility=visibility,
        readiness_digest=readiness.readiness_digest, delivery_claim_digest=DIGEST_A,
        transport_payload_digest=DIGEST_A, raw_receipt_ref="raw", raw_receipt_digest=DIGEST_A,
        bronze_partition_ref="accepted", accepted_content_digest=DIGEST_A,
        ruleset_digest=outcome.validation.ruleset_digest,
        validation_result_digest=outcome.validation.validation_result_digest,
        accepted_count=outcome.validation.accepted_count, progress_claim=OpaqueProgressClaim(kind="opaque_batch"),
        deletion_evidence=None, scheduled_boundary_at=NOW, warning_deadline_at=NOW, error_deadline_at=NOW,
        prior_committed_at=None, execution_plan_digest=plan.execution_plan_digest,
    )
    intent = build_projection_intent(
        logical_identity=contract.logical_identity, contract_digest=payload.contract_digest,
        projection_target="bronze", projection_revision="1", originating_state_revision="0",
        kind=ProjectionIntentKind.DELIVERY_PUBLICATION, execution_plan_digest=plan.execution_plan_digest,
        runtime_manifest_digest=DIGEST_A, payload=payload,
    )
    confirmation = confirm_projection(
        intent, target_applied_at=NOW, committed_at=NOW, release_applied_at=None,
        processing=ProcessingOutcome.COMMITTED, visibility=visibility, ledger_ref="ledger",
        deletion_evidence=None, target_result_digest=DIGEST_A,
    )
    assert confirmation.projection_intent_digest == intent.projection_intent_digest
    ledger = published_ledger_row(intent, confirmation)
    records_bind_contract_and_schemas(contract, (intent.payload, ledger, confirmation))
    final = build_run_lineage(
        contract=contract, run_id=DIGEST_A, attempt_id=DIGEST_B, delivery_id="d1",
        reprocessing_id=None, remediation_evaluation_id=None, transport_payload_digest=DIGEST_A,
        delivery_claim_digest=DIGEST_A, ruleset_digest=outcome.validation.ruleset_digest,
        validation_result_digest=outcome.validation.validation_result_digest,
        accepted_count=outcome.validation.accepted_count, quarantined_count=outcome.validation.quarantined_count,
        execution_plan_digest=plan.execution_plan_digest, runtime_manifest_digest=DIGEST_A,
        landing_ref="landing", confirmation=confirmation, result=ProcessingOutcome.COMMITTED, committed_at=NOW,
    )
    assert final.publication_ref == intent.projection_intent_digest
    assert final.result is ProcessingOutcome.COMMITTED
    require_observer_order(observer_event_order())
    _expect_error(
        "event_conflict",
        lambda: require_observer_order((
            LifecycleEventType.BRONZE_LINEAGE, LifecycleEventType.BRONZE_PUBLICATION,
            LifecycleEventType.BRONZE_QUALITY, LifecycleEventType.BRONZE_QUARANTINE,
            LifecycleEventType.BRONZE_METADATA,
        )),
        "lineage before publication",
    )


def test_omission_and_reordering_of_mandatory_occurrences_fail() -> None:
    contract = _sample_contract()
    plan = bronze_execution_plan(contract)
    assert tuple(item.occurrence_id for item in plan.occurrences) == MANDATORY_OCCURRENCE_IDS
    checkpoint = next(item for item in plan.occurrences if item.occurrence_id == "bronze.checkpoint")
    expected_members = tuple(_vectors()["checkpoint_enclosure"]["members"])
    assert checkpoint.members == expected_members
    omitted = plan.model_copy(update={"occurrences": plan.occurrences[1:]})
    _expect_error("contract_invalid", lambda: require_mandatory_graph(omitted), "omitted occurrence")
    swapped = list(plan.occurrences)
    swapped[0], swapped[-1] = swapped[-1], swapped[0]
    reordered = plan.model_copy(update={"occurrences": tuple(swapped)})
    _expect_error("contract_invalid", lambda: require_mandatory_graph(reordered), "reordered occurrence")
    ingest = next(item for item in plan.occurrences if item.occurrence_id == "bronze.ingest")
    publish = next(item for item in plan.occurrences if item.occurrence_id == "bronze.publish")
    mutated = []
    for item in plan.occurrences:
        if item.occurrence_id == "bronze.ingest":
            mutated.append(item.model_copy(update={"phase_ordinal": publish.phase_ordinal}))
        elif item.occurrence_id == "bronze.publish":
            mutated.append(item.model_copy(update={"phase_ordinal": ingest.phase_ordinal}))
        else:
            mutated.append(item)
    _expect_error(
        "contract_invalid",
        lambda: require_mandatory_graph(plan.model_copy(update={"occurrences": tuple(mutated)})),
        "reordered phase ordinals",
    )
    stripped = [
        item.model_copy(update={"members": ()}) if item.occurrence_id == "bronze.checkpoint" else item
        for item in plan.occurrences
    ]
    _expect_error(
        "contract_invalid",
        lambda: require_mandatory_graph(plan.model_copy(update={"occurrences": tuple(stripped)})),
        "checkpoint members=() drops wrapper enclosure",
    )


def test_rejected_validation_cannot_cross_publication_barrier() -> None:
    contract = _sample_contract()
    plan = bronze_execution_plan(contract)
    readiness = _readiness(contract)
    frames = (_frame(0, {"acct_id": None}),)
    outcome = validate_frames(
        contract, frames, claim_digest=DIGEST_A, delivery_id="d1",
        evaluation_id=DIGEST_B, memory_budget_bytes=65536,
    )
    _expect_error(
        "contract_invalid",
        lambda: require_publication_barrier(plan, readiness, outcome.validation, contract, DIGEST_A),
        "rejected delivery at publication barrier",
    )
    revoked = readiness.model_copy(update={"revoked_at": NOW})
    clean = validate_frames(
        contract, (_frame(0, {"acct_id": "ok"}),), claim_digest=DIGEST_A, delivery_id="d1",
        evaluation_id=DIGEST_B, memory_budget_bytes=65536,
    )
    _expect_error(
        "schema_invalid",
        lambda: require_publication_barrier(plan, revoked, clean.validation, contract, DIGEST_A),
        "revoked readiness",
    )


def test_deletion_evidence_handoff_omits_membership_tags() -> None:
    intent = DeletionEvidenceIntent(
        logical_identity=_sample_contract().logical_identity,
        visibility=DeliveryVisibilityIdentity(epoch="0", kind="delivery", id="delivery-a"),
        delete_strategy="snapshot_diff",
        claim_digest=DIGEST_A, attempt_id=DIGEST_B, event_sequence_low=None, event_sequence_high=None,
        record_key_scope={"scope_id": "account-population", "scope_parameters": {}},
        hmac_key_id="hmac-key-1", key_commitment=DIGEST_A,
        deleted_keyset_ref="deleted-ref", deleted_keyset_digest=DIGEST_B, deleted_key_count="2",
        reconciliation_digest=DIGEST_A, deletion_evidence_intent_digest=DIGEST_A,
    )
    evidence = bind_deletion_evidence(intent, NOW)
    dumped = json.dumps(evidence.model_dump(mode="json"))
    assert _vectors()["forbidden_tokens"][1] not in dumped
    assert evidence.intent.deleted_key_count == "2"
    assert evidence.applied_at == NOW


def main() -> int:
    tests = [
        test_quality_arithmetic_vectors,
        test_prohibited_partial_modes,
        test_all_or_nothing_never_emits_partial_decision,
        test_authored_rules_and_snapshot_acceptance,
        test_warnings_do_not_quarantine,
        test_spillable_uniqueness_stays_within_memory_bound,
        test_metadata_only_diagnostics_cannot_leak,
        test_same_ruleset_revalidation_cannot_override,
        test_selected_locator_remediation_releases_once,
        test_selected_locator_cannot_game_unique_key_or_row_count,
        test_whole_delivery_reprocessing_requires_unpublished_original_claim,
        test_derived_lineage_is_deterministic_and_binds_digests,
        test_intent_precedes_confirmation_and_lineage_follows,
        test_omission_and_reordering_of_mandatory_occurrences_fail,
        test_rejected_validation_cannot_cross_publication_barrier,
        test_deletion_evidence_handoff_omits_membership_tags,
    ]
    failed = 0
    for test in tests:
        try:
            test()
            print(f"PASS {test.__name__}")
        except Exception:
            failed += 1
            print(f"FAIL {test.__name__}")
            traceback.print_exc()
    print(f"{len(tests) - failed}/{len(tests)} passed")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
