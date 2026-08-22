"""Product, field and run lineage, product metadata, deletion-evidence handoff
and the mandatory Bronze graph occurrence check.

Lineage is derived from the authored contract and the observed run, never from
a catalogue. Format normalisation is explicit in the projection mapping;
business transformation is forbidden at this layer. Final run lineage is
emitted only after projection confirmation; intent is a preceding fact.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from ergasterion.framework.bronze_contract import (
    BronzeProductContract,
    ExecutionPlan,
    GraphEdge,
    GraphOccurrence,
    GraphOccurrenceRole,
    HANDOFF_SCHEMA_BINDINGS,
    HandoffSchema,
    LifecycleEventType,
    ProcessingOutcome,
)
from ergasterion.framework.models import Layer
from ergasterion.framework.resolver import resolve
from ergasterion.ingestion.records import (
    DeletionEvidence,
    DeletionEvidenceIntent,
    Digest,
    LineageDescriptor,
    LineageLifecyclePayload,
    MetadataLifecyclePayload,
    ProductMetadata,
    ProjectionConfirmation,
    ProjectionField,
    PublicationConfirmationHandoff,
    PublicationLifecyclePayload,
    PublishedLedgerRow,
    QualityLifecyclePayload,
    QuarantineLifecyclePayload,
    RemediationDecision,
    RunLineage,
    SourceField,
    ValidationResultHandoff,
)
from ergasterion.ingestion.runtime import PortError, canonical_digest
from ergasterion.source_delivery import (
    compute_contract_digest,
    compute_derived_digest,
    compute_published_schema_digest,
    compute_source_schema_digest,
)

# The eight mandatory Bronze occurrences, in the resolver's canonical
# occurrence_id order. Omission or reordering of this sequence fails closed.
MANDATORY_OCCURRENCE_IDS: tuple[str, ...] = tuple(
    occurrence.occurrence_id for occurrence in resolve(Layer.BRONZE).occurrences
)

# Data-flow order the publication barrier walks. Reordering these relative to
# each other is a graph integrity failure even if the full sorted tuple matches.
MANDATORY_PHASE_ORDER: tuple[str, ...] = (
    "bronze.ingest",
    "bronze.validate",
    "bronze.contract",
    "bronze.schema",
    "bronze.publish",
)


def _dump(value: Any) -> Any:
    if hasattr(value, "model_dump"):
        return value.model_dump(mode="json", by_alias=True)
    return value


def require_mandatory_graph(plan: ExecutionPlan) -> tuple[str, ...]:
    """Fail closed if a mandatory occurrence is omitted or reordered.

    The wire plan must carry exactly the eight Bronze occurrences in the
    resolver's canonical order, and the five phase/barrier occurrences must
    appear in data-flow order inside that list.
    """

    actual = tuple(occurrence.occurrence_id for occurrence in plan.occurrences)
    if actual != MANDATORY_OCCURRENCE_IDS:
        raise PortError(
            "contract_invalid",
            "omission or reordering of a mandatory graph occurrence",
        )
    checkpoint = next(
        occurrence for occurrence in plan.occurrences
        if occurrence.occurrence_id == "bronze.checkpoint"
    )
    expected_members = tuple(
        occurrence_id for occurrence_id in actual if occurrence_id != "bronze.checkpoint"
    )
    if tuple(checkpoint.members) != expected_members:
        raise PortError(
            "contract_invalid",
            "checkpoint enclosure is missing or incomplete",
        )
    ordinals = {occurrence.occurrence_id: int(occurrence.phase_ordinal) for occurrence in plan.occurrences}
    phase_ranks = tuple(ordinals[occurrence_id] for occurrence_id in MANDATORY_PHASE_ORDER)
    if phase_ranks != tuple(range(len(MANDATORY_PHASE_ORDER))):
        raise PortError(
            "contract_invalid",
            "mandatory phase occurrences are reordered relative to the publication barrier",
        )
    return actual


def derive_field_lineage(contract: BronzeProductContract) -> tuple[ProjectionField, ...]:
    """Physical source field to published column. The projection mapping is the
    lineage; a published column may only rename and restate nullability/type,
    never compute a business predicate."""

    columns = {column.name: column for column in contract.landing.physical_columns}
    lineage: list[ProjectionField] = []
    for entry in contract.projection:
        source = columns.get(entry.source)
        if source is None:
            raise PortError("schema_invalid", f"projection source {entry.source!r} is not a physical column")
        lineage.append(entry)
    return tuple(lineage)


def derive_source_schema(contract: BronzeProductContract) -> tuple[SourceField, ...]:
    return tuple(contract.landing.physical_columns)


def contract_digests(contract: BronzeProductContract) -> tuple[Digest, Digest, Digest]:
    return (
        compute_contract_digest(contract),
        compute_source_schema_digest(contract),
        compute_published_schema_digest(contract),
    )


def build_lineage_descriptor(
    contract: BronzeProductContract, execution_plan_digest: Digest,
) -> LineageDescriptor:
    projection = derive_field_lineage(contract)
    basis = {
        "logical_identity": _dump(contract.logical_identity),
        "projection": [_dump(entry) for entry in projection],
        "execution_plan_digest": execution_plan_digest,
    }
    digest = compute_derived_digest("LineageDescriptor", basis)
    return LineageDescriptor(
        logical_identity=contract.logical_identity,
        projection=projection,
        execution_plan_digest=execution_plan_digest,
        lineage_digest=digest,
    )


def build_run_lineage(
    *,
    contract: BronzeProductContract,
    run_id: Digest,
    attempt_id: Digest,
    delivery_id: str | None,
    reprocessing_id: Digest | None,
    remediation_evaluation_id: Digest | None,
    transport_payload_digest: Digest,
    delivery_claim_digest: Digest,
    ruleset_digest: Digest | None,
    validation_result_digest: Digest | None,
    accepted_count: str,
    quarantined_count: str,
    execution_plan_digest: Digest,
    runtime_manifest_digest: Digest,
    landing_ref: str,
    confirmation: ProjectionConfirmation | None,
    result: ProcessingOutcome,
    committed_at: str | None,
) -> RunLineage:
    """Run lineage binds product version, contract digest and both schema
    digests. Final lineage (a publication reference and committed result)
    requires the projection confirmation that follows the intent; calling this
    with ``committed`` and no confirmation fails closed."""

    if result is ProcessingOutcome.COMMITTED and confirmation is None:
        raise PortError(
            "intent_conflict",
            "final run lineage follows projection confirmation; confirmation is missing",
        )
    contract_digest, source_schema_digest, published_schema_digest = contract_digests(contract)
    publication_ref = None
    if confirmation is not None:
        publication_ref = confirmation.projection_intent_digest
        if confirmation.contract_digest != contract_digest:
            raise PortError("integrity_error", "run lineage contract digest does not bind the confirmation")
        committed_at = confirmation.committed_at
        result = confirmation.processing
    basis = {
        "schema": "ergasterion.run-lineage/v1",
        "run_id": run_id,
        "attempt_id": attempt_id,
        "logical_identity": _dump(contract.logical_identity),
        "delivery_id": delivery_id,
        "reprocessing_id": reprocessing_id,
        "remediation_evaluation_id": remediation_evaluation_id,
        "transport_payload_digest": transport_payload_digest,
        "delivery_claim_digest": delivery_claim_digest,
        "ruleset_digest": ruleset_digest,
        "validation_result_digest": validation_result_digest,
        "accepted_count": accepted_count,
        "quarantined_count": quarantined_count,
        "product_version": contract.product.product_version,
        "contract_digest": contract_digest,
        "source_schema_digest": source_schema_digest,
        "published_schema_digest": published_schema_digest,
        "execution_plan_digest": execution_plan_digest,
        "runtime_manifest_digest": runtime_manifest_digest,
        "landing_ref": landing_ref,
        "publication_ref": publication_ref,
        "result": result.value,
        "committed_at": committed_at,
    }
    digest = compute_derived_digest("RunLineage", basis)
    return RunLineage(
        schema="ergasterion.run-lineage/v1",
        run_id=run_id,
        attempt_id=attempt_id,
        logical_identity=contract.logical_identity,
        delivery_id=delivery_id,
        reprocessing_id=reprocessing_id,
        remediation_evaluation_id=remediation_evaluation_id,
        transport_payload_digest=transport_payload_digest,
        delivery_claim_digest=delivery_claim_digest,
        ruleset_digest=ruleset_digest,
        validation_result_digest=validation_result_digest,
        accepted_count=accepted_count,
        quarantined_count=quarantined_count,
        product_version=contract.product.product_version,
        contract_digest=contract_digest,
        source_schema_digest=source_schema_digest,
        published_schema_digest=published_schema_digest,
        execution_plan_digest=execution_plan_digest,
        runtime_manifest_digest=runtime_manifest_digest,
        landing_ref=landing_ref,
        publication_ref=publication_ref,
        result=result,
        committed_at=committed_at,
        run_lineage_digest=digest,
    )


def build_product_metadata(
    contract: BronzeProductContract,
    *,
    latest_stream_status_ref: str,
    latest_publication_ref: str | None = None,
) -> ProductMetadata:
    contract_digest, source_schema_digest, published_schema_digest = contract_digests(contract)
    payload = {
        "logical_identity": contract.logical_identity,
        "product": contract.product,
        "contract_digest": contract_digest,
        "source_schema_digest": source_schema_digest,
        "published_schema_digest": published_schema_digest,
        "source_schema": derive_source_schema(contract),
        "quality": contract.delivery.quality,
        "interfaces": contract.interfaces,
        "latest_stream_status_ref": latest_stream_status_ref,
    }
    if latest_publication_ref is not None:
        payload["latest_publication_ref"] = latest_publication_ref
    return ProductMetadata(**payload)


def bind_deletion_evidence(intent: DeletionEvidenceIntent, applied_at: str) -> DeletionEvidence:
    """Target transaction turns a typed intent into final deletion evidence by
    attaching ``applied_at``. Membership identifiers stay on the protected
    keyset; this handoff carries only the opaque reference, digest and count."""

    digest = compute_derived_digest(
        "DeletionEvidence",
        {"intent": _dump(intent), "applied_at": applied_at},
    )
    return DeletionEvidence(intent=intent, applied_at=applied_at, deletion_evidence_digest=digest)


def quality_handoff(
    *,
    logical_identity,
    run_id: Digest,
    attempt_id: Digest,
    evaluation_id: Digest,
    ruleset_digest: Digest,
    validation_result_digest: Digest,
    accepted_content_digest: Digest,
    disposition_ref: str,
    accepted_ref: str,
    framed_count: str,
    accepted_count: str,
    error_count: str,
    warning_count: str,
    quarantined_count: str,
    batch_findings: tuple,
    error_numerator: str,
    error_denominator: str,
    publication_decision,
) -> ValidationResultHandoff:
    return ValidationResultHandoff(
        logical_identity=logical_identity,
        run_id=run_id,
        attempt_id=attempt_id,
        evaluation_id=evaluation_id,
        ruleset_digest=ruleset_digest,
        validation_result_digest=validation_result_digest,
        accepted_content_digest=accepted_content_digest,
        disposition_ref=disposition_ref,
        accepted_ref=accepted_ref,
        framed_count=framed_count,
        accepted_count=accepted_count,
        error_count=error_count,
        warning_count=warning_count,
        quarantined_count=quarantined_count,
        batch_findings=batch_findings,
        error_numerator=error_numerator,
        error_denominator=error_denominator,
        publication_decision=publication_decision,
    )


def lineage_payload(descriptor: LineageDescriptor, run_lineage: RunLineage) -> LineageLifecyclePayload:
    return LineageLifecyclePayload(kind="bronze.lineage", lineage=descriptor, run_lineage=run_lineage)


def metadata_payload(metadata: ProductMetadata) -> MetadataLifecyclePayload:
    return MetadataLifecyclePayload(kind="bronze.metadata", metadata=metadata)


def quality_payload(handoff: ValidationResultHandoff) -> QualityLifecyclePayload:
    return QualityLifecyclePayload(kind="bronze.quality", validation=handoff)


def quarantine_payload(
    handoff: ValidationResultHandoff, decision: RemediationDecision | None,
) -> QuarantineLifecyclePayload:
    return QuarantineLifecyclePayload(kind="bronze.quarantine", validation=handoff, decision=decision)


def publication_payload(
    run_id: Digest, attempt_id: Digest, confirmation: ProjectionConfirmation, ledger: PublishedLedgerRow,
) -> PublicationLifecyclePayload:
    return PublicationLifecyclePayload(
        kind="bronze.publication",
        confirmation=PublicationConfirmationHandoff(
            run_id=run_id, attempt_id=attempt_id, confirmation=confirmation,
        ),
        ledger=ledger,
    )


def bronze_execution_plan(
    contract: BronzeProductContract,
    *,
    execution_plan_digest: Digest | None = None,
) -> ExecutionPlan:
    """Wire ``ExecutionPlan`` for the normative Bronze graph, with contract and
    both schema digests bound on the record."""

    resolved = resolve(Layer.BRONZE)
    contract_digest, source_schema_digest, published_schema_digest = contract_digests(contract)
    wrapper_id = resolved.wrapper_id
    wrapper_members = resolved.wrapper_members
    occurrences = tuple(
        GraphOccurrence(
            occurrence_id=occurrence.occurrence_id,
            pattern_id=occurrence.pattern_id,
            roles=tuple(GraphOccurrenceRole(role.value) for role in occurrence.roles),
            phase_ordinal=MANDATORY_PHASE_ORDER.index(occurrence.occurrence_id)
            if occurrence.occurrence_id in MANDATORY_PHASE_ORDER else 0,
            members=wrapper_members if occurrence.occurrence_id == wrapper_id else (),
            execution_owner_required=occurrence.execution_owner_required,
        )
        for occurrence in resolved.occurrences
    )
    edges = tuple(
        GraphEdge(
            from_occurrence=edge.source,
            to_occurrence=edge.target,
            role=edge.edge_role.value,  # GraphEdgeRole matches EdgeRole tokens
            handoff_schema_id=edge.handoff_schema_id,
        )
        for edge in resolved.edges
    )
    seen: set[str] = set()
    handoffs: list[HandoffSchema] = []
    for edge in resolved.edges:
        schema_id = edge.handoff_schema_id
        if schema_id.value in seen:
            continue
        seen.add(schema_id.value)
        record_type = HANDOFF_SCHEMA_BINDINGS[schema_id]
        handoffs.append(HandoffSchema(
            schema_id=schema_id,
            record_type=record_type,
            schema_digest=canonical_digest({"schema_id": schema_id.value, "record_type": record_type.value}),
        ))
    basis = {
        "schema": "ergasterion.execution-plan/v1",
        "logical_identity": _dump(contract.logical_identity),
        "product_version": contract.product.product_version,
        "contract_digest": contract_digest,
        "source_schema_digest": source_schema_digest,
        "published_schema_digest": published_schema_digest,
        "occurrences": [_dump(item) for item in occurrences],
        "edges": [_dump(item) for item in edges],
        "handoffs": [_dump(item) for item in handoffs],
    }
    digest = execution_plan_digest or compute_derived_digest("ExecutionPlan", basis)
    return ExecutionPlan(
        schema="ergasterion.execution-plan/v1",
        logical_identity=contract.logical_identity,
        product_version=contract.product.product_version,
        contract_digest=contract_digest,
        source_schema_digest=source_schema_digest,
        published_schema_digest=published_schema_digest,
        occurrences=occurrences,
        edges=edges,
        handoffs=tuple(handoffs),
        execution_plan_digest=digest,
    )


def observer_event_order() -> tuple[LifecycleEventType, ...]:
    """Intent (publication) precedes confirmation-bound lineage. Quality and
    quarantine are validation observers; lineage and metadata follow publish."""

    return (
        LifecycleEventType.BRONZE_QUALITY,
        LifecycleEventType.BRONZE_QUARANTINE,
        LifecycleEventType.BRONZE_PUBLICATION,
        LifecycleEventType.BRONZE_LINEAGE,
        LifecycleEventType.BRONZE_METADATA,
    )


def require_observer_order(event_types: Sequence[LifecycleEventType]) -> None:
    expected = observer_event_order()
    filtered = tuple(event_type for event_type in event_types if event_type in expected)
    if filtered != expected:
        raise PortError(
            "event_conflict",
            "lifecycle observers omitted or reordered: intent/publication must precede final lineage",
        )


__all__ = [
    "MANDATORY_OCCURRENCE_IDS",
    "MANDATORY_PHASE_ORDER",
    "bind_deletion_evidence",
    "bronze_execution_plan",
    "build_lineage_descriptor",
    "build_product_metadata",
    "build_run_lineage",
    "contract_digests",
    "derive_field_lineage",
    "derive_source_schema",
    "lineage_payload",
    "metadata_payload",
    "observer_event_order",
    "publication_payload",
    "quality_handoff",
    "quality_payload",
    "quarantine_payload",
    "require_mandatory_graph",
    "require_observer_order",
]
