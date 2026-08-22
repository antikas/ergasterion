"""Publication barrier, projection intent/confirmation, remediation-release
admission and whole-delivery reprocessing admission.

A publication intent cannot exist until the Bronze graph, interface readiness
and validation decision all pass. Confirmation references that immutable
intent; final run lineage is a later observer. Product version, contract
digest and both schema digests bind every lineage and publication record.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from ergasterion.framework.bronze_contract import (
    BronzeProductContract,
    DeliveryMode,
    DispositionStatus,
    ExecutionPlan,
    MigrationKind,
    PublicationDecision,
    PublicationPolicy,
    ReadinessResult,
)
from ergasterion.framework.runtime_binding import InterfaceReadiness
from ergasterion.ingestion.lifecycle import (
    build_lineage_descriptor,
    contract_digests,
    require_mandatory_graph,
)
from ergasterion.ingestion.records import (
    DeliveryPublicationPayload,
    Digest,
    Disposition,
    ProgressClaim,
    ProjectionConfirmation,
    ProjectionIntent,
    ProjectionIntentKind,
    ProjectionPayload,
    PublishedLedgerRow,
    RawLocator,
    ReprocessingClaim,
    ValidationResult,
    VisibilityIdentity,
    WholeDeliveryReprocessingPayload,
)
from ergasterion.ingestion.runtime import PortError, canonical_digest
from ergasterion.ingestion.validation import batch_blocks_publication
from ergasterion.source_delivery import compute_derived_digest

RELEASE_ID_SCHEMA = "ergasterion.release-id/v1"
REMEDIATION_EVALUATION_SCHEMA = "ergasterion.remediation-evaluation/v1"


def _dump(value: Any) -> Any:
    if hasattr(value, "model_dump"):
        return value.model_dump(mode="json", by_alias=True)
    if isinstance(value, dict):
        return {key: _dump(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_dump(item) for item in value]
    return value


def require_publication_barrier(
    plan: ExecutionPlan,
    readiness: InterfaceReadiness,
    validation: ValidationResult,
    contract: BronzeProductContract,
    runtime_manifest_digest: Digest,
) -> None:
    """``bronze.schema`` accepts only a success with null ``revoked_at`` for the
    exact target/manifest before a publication intent can exist. The graph must
    be whole. A rejected delivery never crosses the barrier."""

    require_mandatory_graph(plan)
    contract_digest, source_schema_digest, published_schema_digest = contract_digests(contract)
    if plan.contract_digest != contract_digest:
        raise PortError("superseded_contract", "execution plan contract digest does not bind the contract")
    if plan.source_schema_digest != source_schema_digest or plan.published_schema_digest != published_schema_digest:
        raise PortError("schema_invalid", "execution plan schema digests do not bind the contract")
    if readiness.result is not ReadinessResult.READY or readiness.revoked_at is not None:
        raise PortError("schema_invalid", "bronze.schema requires ready interface readiness with null revoked_at")
    if readiness.contract_digest != contract_digest:
        raise PortError("capability_mismatch", "interface readiness was verified against a different contract")
    if readiness.runtime_manifest_digest != runtime_manifest_digest:
        raise PortError("capability_mismatch", "interface readiness was verified against a different manifest")
    if readiness.source_schema_digest != source_schema_digest or readiness.published_schema_digest != published_schema_digest:
        raise PortError("schema_invalid", "interface readiness schema digests do not bind the contract")
    if validation.publication_decision is PublicationDecision.REJECT_DELIVERY:
        raise PortError("contract_invalid", "rejected validation cannot cross the publication barrier")
    if (
        validation.publication_decision is PublicationDecision.PUBLISH_VALID_ROWS
        and (
            contract.delivery.quality.publication_mode is not PublicationPolicy.PUBLISH_VALID_ROWS
            or contract.delivery.mode is not DeliveryMode.APPEND_ONLY
            or contract.delivery.progress.kind != "opaque_batch"
        )
    ):
        raise PortError("invalid_config", "partial publication decision is prohibited for this delivery mode")


def lineage_digest_for(contract: BronzeProductContract, execution_plan_digest: Digest) -> Digest:
    return build_lineage_descriptor(contract, execution_plan_digest).lineage_digest


def build_projection_intent(
    *,
    logical_identity,
    contract_digest: Digest,
    projection_target: str,
    projection_revision: str,
    originating_state_revision: str,
    kind: ProjectionIntentKind,
    execution_plan_digest: Digest,
    runtime_manifest_digest: Digest,
    payload: ProjectionPayload,
) -> ProjectionIntent:
    """Intent is the durability point. Confirmation cannot be constructed
    without this record; ``projection_intent_digest`` excludes target-produced
    timestamps that do not exist yet."""

    if payload.kind != kind.value:
        raise PortError("intent_conflict", "projection payload kind must equal the enclosing intent kind")
    payload_digest = canonical_digest(_dump(payload))
    basis = {
        "schema": "ergasterion.projection-intent/v1",
        "logical_identity": _dump(logical_identity),
        "contract_digest": contract_digest,
        "projection_target": projection_target,
        "projection_revision": projection_revision,
        "originating_state_revision": originating_state_revision,
        "kind": kind.value,
        "execution_plan_digest": execution_plan_digest,
        "runtime_manifest_digest": runtime_manifest_digest,
        "payload": _dump(payload),
        "payload_digest": payload_digest,
    }
    intent_digest = compute_derived_digest("ProjectionIntent", basis)
    return ProjectionIntent(
        schema="ergasterion.projection-intent/v1",
        logical_identity=logical_identity,
        contract_digest=contract_digest,
        projection_target=projection_target,
        projection_revision=projection_revision,
        originating_state_revision=originating_state_revision,
        kind=kind,
        execution_plan_digest=execution_plan_digest,
        runtime_manifest_digest=runtime_manifest_digest,
        payload=payload,
        payload_digest=payload_digest,
        projection_intent_digest=intent_digest,
    )


def build_delivery_publication_payload(
    *,
    contract: BronzeProductContract,
    attempt_id: Digest,
    visibility: VisibilityIdentity,
    readiness_digest: Digest,
    delivery_claim_digest: Digest,
    transport_payload_digest: Digest,
    raw_receipt_ref: str,
    raw_receipt_digest: Digest,
    bronze_partition_ref: str,
    accepted_content_digest: Digest,
    ruleset_digest: Digest,
    validation_result_digest: Digest,
    accepted_count: str,
    progress_claim: ProgressClaim,
    deletion_evidence,
    scheduled_boundary_at: str,
    warning_deadline_at: str,
    error_deadline_at: str,
    prior_committed_at: str | None,
    execution_plan_digest: Digest,
) -> DeliveryPublicationPayload:
    contract_digest, source_schema_digest, published_schema_digest = contract_digests(contract)
    return DeliveryPublicationPayload(
        kind="delivery_publication",
        attempt_id=attempt_id,
        visibility=visibility,
        product_version=contract.product.product_version,
        contract_digest=contract_digest,
        source_schema_digest=source_schema_digest,
        published_schema_digest=published_schema_digest,
        readiness_digest=readiness_digest,
        delivery_claim_digest=delivery_claim_digest,
        transport_payload_digest=transport_payload_digest,
        raw_receipt_ref=raw_receipt_ref,
        raw_receipt_digest=raw_receipt_digest,
        bronze_partition_ref=bronze_partition_ref,
        accepted_content_digest=accepted_content_digest,
        ruleset_digest=ruleset_digest,
        validation_result_digest=validation_result_digest,
        accepted_count=accepted_count,
        progress_claim=progress_claim,
        deletion_evidence=deletion_evidence,
        scheduled_boundary_at=scheduled_boundary_at,
        warning_deadline_at=warning_deadline_at,
        error_deadline_at=error_deadline_at,
        prior_committed_at=prior_committed_at,
        lineage_digest=lineage_digest_for(contract, execution_plan_digest),
    )


def confirm_projection(
    intent: ProjectionIntent,
    *,
    target_applied_at: str,
    committed_at: str | None,
    release_applied_at: str | None,
    processing,
    visibility: VisibilityIdentity | None,
    ledger_ref: str | None,
    deletion_evidence,
    target_result_digest: Digest,
    timeliness=None,
) -> ProjectionConfirmation:
    """Confirmation binds the intent digest plus target-produced timestamps.
    There is no confirmation path that skips the preceding intent."""

    if intent is None:
        raise PortError("intent_conflict", "projection confirmation requires a preceding intent")
    return ProjectionConfirmation(
        schema="ergasterion.projection-confirmation/v1",
        logical_identity=intent.logical_identity,
        contract_digest=intent.contract_digest,
        projection_target=intent.projection_target,
        kind=intent.kind,
        projection_intent_digest=intent.projection_intent_digest,
        projection_revision=intent.projection_revision,
        target_applied_at=target_applied_at,
        committed_at=committed_at,
        release_applied_at=release_applied_at,
        timeliness=timeliness,
        processing=processing,
        visibility=visibility,
        ledger_ref=ledger_ref,
        deletion_evidence=deletion_evidence,
        target_result_digest=target_result_digest,
    )


def published_ledger_row(
    intent: ProjectionIntent,
    confirmation: ProjectionConfirmation,
) -> PublishedLedgerRow:
    if confirmation.projection_intent_digest != intent.projection_intent_digest:
        raise PortError("integrity_error", "ledger row must bind the confirmed intent digest")
    if confirmation.projection_revision != intent.projection_revision:
        raise PortError("projection_conflict", "confirmation revision does not match the intent")
    payload = intent.payload
    if not isinstance(payload, (DeliveryPublicationPayload, WholeDeliveryReprocessingPayload)):
        raise PortError("intent_conflict", "published ledger rows are produced only by data-changing intents")
    committed_at = confirmation.committed_at
    if committed_at is None:
        raise PortError("integrity_error", "a published ledger row requires semantic committed_at")
    return PublishedLedgerRow(
        logical_identity=intent.logical_identity,
        visibility=payload.visibility,
        projection_target=intent.projection_target,
        product_version=payload.product_version,
        contract_digest=payload.contract_digest,
        source_schema_digest=payload.source_schema_digest,
        published_schema_digest=payload.published_schema_digest,
        delivery_claim_digest=payload.delivery_claim_digest if isinstance(payload, DeliveryPublicationPayload)
        else payload.original_delivery_claim_digest,
        transport_payload_digest=payload.transport_payload_digest,
        raw_receipt_ref=payload.raw_receipt_ref,
        raw_receipt_digest=payload.raw_receipt_digest,
        bronze_partition_ref=payload.bronze_partition_ref,
        accepted_content_digest=payload.accepted_content_digest,
        ruleset_digest=payload.ruleset_digest,
        validation_result_digest=payload.validation_result_digest,
        accepted_count=payload.accepted_count,
        progress_claim=payload.progress_claim,
        execution_plan_digest=intent.execution_plan_digest,
        runtime_manifest_digest=intent.runtime_manifest_digest,
        committed_at=committed_at,
        release_applied_at=confirmation.release_applied_at,
        projection_revision=intent.projection_revision,
    )


def locator_sort_key(locator: RawLocator) -> tuple[int, str]:
    return (int(locator.frame_sequence), canonical_digest(_dump(locator)))


def remediation_evaluation_id(
    *,
    original_claim_digest: Digest,
    raw_receipt_digest: Digest,
    target_contract_digest: Digest,
    target_source_schema_digest: Digest,
    target_published_schema_digest: Digest,
    execution_plan_digest: Digest,
    root_visibility_epoch: str,
    ruleset_digest: Digest,
) -> Digest:
    return canonical_digest({
        "schema": REMEDIATION_EVALUATION_SCHEMA,
        "original_claim_digest": original_claim_digest,
        "raw_receipt_digest": raw_receipt_digest,
        "target": {
            "contract_digest": target_contract_digest,
            "source_schema_digest": target_source_schema_digest,
            "published_schema_digest": target_published_schema_digest,
            "execution_plan_digest": execution_plan_digest,
            "root_visibility_epoch": root_visibility_epoch,
        },
        "ruleset_digest": ruleset_digest,
    })


def release_id_for(
    evaluation_id: Digest, locators: Sequence[RawLocator], accepted_content_digest: Digest,
) -> Digest:
    unique = tuple(sorted(locators, key=locator_sort_key))
    return canonical_digest({
        "schema": RELEASE_ID_SCHEMA,
        "remediation_evaluation_id": evaluation_id,
        "locators": [_dump(item) for item in unique],
        "accepted_content_digest": accepted_content_digest,
    })


def admit_selected_locator_release(
    *,
    contract: BronzeProductContract,
    prior_decision: PublicationDecision,
    visibility_epoch: str,
    active_root_epoch: str,
    migration_kind: MigrationKind,
    selected_locators: Sequence[RawLocator],
    max_locators: int,
    already_released_locators: Sequence[RawLocator],
    evaluation: ValidationResult,
    dispositions: Sequence[Disposition],
    original_denominator: str,
) -> tuple[RawLocator, ...]:
    """Selected-locator remediation is allowed only for a previously partial
    published ``publish_valid_rows`` opaque append batch whose visibility epoch
    belongs to the schema-compatible active contract's carry ancestry. One
    evaluation releases the selected set once.

    Admission binds a full-delivery ``ValidationResult``, the original
    denominator and per-locator post-eval pass/fail. ``unique_key`` and
    ``row_count`` cannot be gamed by selecting a subset; only locators whose
    new unit outcomes pass may be released.
    """

    delivery = contract.delivery
    if (
        delivery.mode is not DeliveryMode.APPEND_ONLY
        or delivery.progress.kind != "opaque_batch"
        or delivery.quality.publication_mode is not PublicationPolicy.PUBLISH_VALID_ROWS
    ):
        raise PortError(
            "invalid_config",
            "selected-locator remediation is admitted only for append-only opaque publish_valid_rows",
        )
    if prior_decision is not PublicationDecision.PUBLISH_VALID_ROWS:
        raise PortError("decision_conflict", "remediation release requires a previously partial publication")
    if migration_kind is MigrationKind.RESET:
        raise PortError("ancestry_mismatch", "a reset/new-root contract cannot release locators from the old root")
    if int(visibility_epoch) < int(active_root_epoch):
        raise PortError("ancestry_mismatch", "visibility epoch is not in the active contract carry ancestry")
    if (
        evaluation.framed_count != original_denominator
        or evaluation.error_denominator != original_denominator
        or str(len(dispositions)) != original_denominator
    ):
        raise PortError(
            "decision_conflict",
            "selected-locator remediation must re-evaluate the original framed delivery",
        )
    if batch_blocks_publication(evaluation.batch_findings):
        raise PortError("contract_invalid", "a batch-level error blocks selected-locator release")
    if not selected_locators:
        raise PortError("invalid_usage", "selected-locator release requires at least one locator")
    if len(selected_locators) > max_locators:
        raise PortError("capacity_exceeded", "selected locators exceed max_remediation_locators")
    keys = [canonical_digest(_dump(item)) for item in selected_locators]
    if len(set(keys)) != len(keys):
        raise PortError("invalid_usage", "selected locators must be duplicate-free")
    released = {canonical_digest(_dump(item)) for item in already_released_locators}
    overlap = [item for item, key in zip(selected_locators, keys) if key in released]
    if overlap:
        raise PortError("release_conflict", "overlapping selected-locator release")
    by_key = {canonical_digest(_dump(item.raw_locator)): item for item in dispositions}
    passing: list[RawLocator] = []
    for locator, key in zip(selected_locators, keys):
        disposition = by_key.get(key)
        if disposition is None:
            raise PortError(
                "decision_conflict",
                "selected locator is not present in the full-delivery evaluation",
            )
        if disposition.status is DispositionStatus.ACCEPTED:
            passing.append(locator)
    if not passing:
        raise PortError(
            "contract_invalid",
            "selected-locator release requires at least one locator whose post-eval unit outcome passes",
        )
    return tuple(sorted(passing, key=locator_sort_key))


def admit_whole_delivery_reprocessing(
    claim: ReprocessingClaim,
    *,
    original_claim_digest: Digest,
    prior_publication_decisions: Sequence[PublicationDecision],
) -> None:
    """Whole-delivery reprocessing accepts only the original claim, and only
    when that claim has no successful or partial publication."""

    if claim.original_claim_digest != original_claim_digest:
        raise PortError("claim_conflict", "whole-delivery reprocessing requires the original claim digest")
    blocking = {
        PublicationDecision.PUBLISH_ALL,
        PublicationDecision.PUBLISH_VALID_ROWS,
    }
    if any(decision in blocking for decision in prior_publication_decisions):
        raise PortError(
            "claim_conflict",
            "whole-delivery reprocessing accepts only an original claim with no successful or partial publication",
        )


def records_bind_contract_and_schemas(
    contract: BronzeProductContract,
    records: Sequence[Any],
) -> None:
    """Every lineage and publication record must carry the product version,
    contract digest and both schema digests of the contract it claims."""

    contract_digest, source_schema_digest, published_schema_digest = contract_digests(contract)
    expected_version = contract.product.product_version
    for record in records:
        dumped = _dump(record)
        version = dumped.get("product_version")
        if version is not None and version != expected_version:
            raise PortError("integrity_error", "record product_version does not bind the contract")
        if "contract_digest" in dumped and dumped["contract_digest"] != contract_digest:
            raise PortError("integrity_error", "record contract_digest does not bind the contract")
        if "source_schema_digest" in dumped and dumped["source_schema_digest"] != source_schema_digest:
            raise PortError("integrity_error", "record source_schema_digest does not bind the contract")
        if "published_schema_digest" in dumped and dumped["published_schema_digest"] != published_schema_digest:
            raise PortError("integrity_error", "record published_schema_digest does not bind the contract")


__all__ = [
    "admit_selected_locator_release",
    "admit_whole_delivery_reprocessing",
    "build_delivery_publication_payload",
    "build_projection_intent",
    "confirm_projection",
    "lineage_digest_for",
    "locator_sort_key",
    "published_ledger_row",
    "records_bind_contract_and_schemas",
    "release_id_for",
    "remediation_evaluation_id",
    "require_publication_barrier",
]
