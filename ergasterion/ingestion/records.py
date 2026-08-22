"""Closed Pydantic projections of the frozen Bronze portable IDL: delivery input,
raw-receipt, reprocessing/remediation, migrations/state, validation/disposition,
lifecycle/publication/projection intent and confirmation, attestation, backup and
evidence records -- everything not already covered by
``ergasterion.framework.bronze_contract`` (vocabulary + contract declaration) or
``ergasterion.framework.runtime_binding`` (runtime binding + deployment + capabilities +
readiness). Builds on both of those modules; neither imports this one, so the
dependency chain stays one-way. See ``bronze_contract``'s module docstring for the
shared design notes (structural-only scope, no file I/O at import time, the IDL pin).

This module also carries the port declarations (``PORTS``, ``PORT_OPERATION_ORDER``) and
the schema-bundle / equivalence-report generators, since it is the one module with
visibility into every record the three-module family declares.
"""

from __future__ import annotations

import hashlib
import json
import types
import typing
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Annotated, Literal, Union

from pydantic import BaseModel, ConfigDict, Field

import ergasterion.framework.bronze_contract as bronze_contract
import ergasterion.framework.runtime_binding as runtime_binding
from ergasterion.framework.bronze_contract import (
    EXPECTED_IDL_SHA256,
    AttemptState,
    Base64Url,
    BackupAction,
    BackoffKind,
    BlockPhase,
    BronzeInterfaces,
    BronzeProductContract,
    ByteStringBase64Url,
    ClosedModel,
    ContentEncoding,
    ContentId,
    ContractActivationState,
    ContractLifecycleAction,
    DeleteStrategy,
    DeliveryInputKind,
    DeliveryMode,
    Digest,
    DispositionStatus,
    ErrorCategory,
    ErrorCode,
    EvidenceKind,
    FileMode,
    Finding,
    FingerprintScope,
    Identifier,
    IntegerString,
    JsonPointer,
    LogicalIdentity,
    MediaType,
    Migration,
    NonNegativeInteger,
    NonNegativeIntegerString,
    OpaqueRef,
    OutboxEntryKind,
    OutboxFailureDisposition,
    OutboxStatus,
    PositiveInteger,
    PositiveIntegerString,
    ProcessingOutcome,
    ProductFacts,
    ProgressKind,
    ProjectionField,
    ProjectionIntentKind,
    PublicationDecision,
    QualityPolicy,
    QuarantineAction,
    RawLocator,
    RemediationActionStatus,
    RemediationDecisionKind,
    SemVer,
    Severity,
    SnapshotReconciliationStatus,
    SourceField,
    Token,
    TimelinessState,
    TypedScalar,
    UtcInstant,
    ValidationResultHandoff,
    VisibilityKind,
)
from ergasterion.framework.runtime_binding import (
    ProjectionCursor,
    RuntimeBinding,
    RuntimeDeployment,
    RuntimeManifest,
)

__all__ = [
    "generate_equivalence_report",
    "generate_schema_bundle",
    "load_idl",
    "ALL_RECORD_MODELS",
    "ALL_ENUM_MODELS",
    "ALL_UNION_MODELS",
    "PORTS",
    "PORT_OPERATION_ORDER",
]


# --------------------------------------------------------------------------- delivery input / raw evidence

class PayloadDescriptor(ClosedModel):
    media_type: MediaType
    content_encoding: ContentEncoding
    codec_version: PositiveInteger
    byte_length: NonNegativeIntegerString
    sha256: Digest


class SequenceProgressClaim(ClosedModel):
    kind: Literal["sequence"]
    high_watermark: IntegerString
    event_count: NonNegativeIntegerString


class OpaqueProgressClaim(ClosedModel):
    kind: Literal["opaque_batch"]


ProgressClaim = Annotated[Union[OpaqueProgressClaim, SequenceProgressClaim], Field(discriminator="kind")]


class SnapshotAttestationPayload(ClosedModel):
    logical_identity: LogicalIdentity
    contract_digest: Digest
    delivery_id: Token
    batch_id: Token
    effective_boundary_at: UtcInstant
    content_fingerprint: Digest
    scope: FingerprintScope
    row_count: NonNegativeIntegerString
    issued_at: UtcInstant


class SignedAttestation(ClosedModel):
    schema_: Literal["ergasterion.snapshot-attestation/v1"] = Field(alias="schema")
    algorithm: Literal["Ed25519"]
    key_id: Token
    payload: SnapshotAttestationPayload
    signature: Base64Url

    model_config = ConfigDict(extra="forbid", frozen=True, populate_by_name=True)


class DeliveryManifest(ClosedModel):
    schema_: Literal["ergasterion.delivery-manifest/v1"] = Field(alias="schema")
    logical_identity: LogicalIdentity
    product_version: SemVer
    contract_digest: Digest
    delivery_id: Token
    batch_id: Token | None
    scheduled_boundary_at: UtcInstant | None
    effective_boundary_at: UtcInstant | None
    payload: PayloadDescriptor
    frame_sequence_digest: Digest | None
    progress_claim: ProgressClaim
    declared_row_count: NonNegativeIntegerString
    snapshot_attestation: SignedAttestation | None

    model_config = ConfigDict(extra="forbid", frozen=True, populate_by_name=True)


class ManagedPayloadInput(ClosedModel):
    kind: Literal["managed_payload"]
    manifest: DeliveryManifest
    payload_handle: OpaqueRef


class DeliveryVisibilityIdentity(ClosedModel):
    epoch: NonNegativeIntegerString
    kind: Literal["delivery"]
    id: Token


class ExternalReceiptPayload(ClosedModel):
    logical_identity: LogicalIdentity
    contract_digest: Digest
    claim: DeliveryManifest
    delivery_claim_digest: Digest
    visibility: DeliveryVisibilityIdentity
    adapter_capability_digest: Digest
    raw_ref: OpaqueRef
    raw_digest: Digest
    manifest_ref: OpaqueRef
    manifest_digest: Digest
    candidate_ref: OpaqueRef
    candidate_digest: Digest
    frame_index_ref: OpaqueRef
    frame_index_digest: Digest
    issued_at: UtcInstant


class SignedExternalReceipt(ClosedModel):
    schema_: Literal["ergasterion.external-receipt/v1"] = Field(alias="schema")
    algorithm: Literal["Ed25519"]
    key_id: Token
    payload: ExternalReceiptPayload
    signature: Base64Url

    model_config = ConfigDict(extra="forbid", frozen=True, populate_by_name=True)


class ExternalReceiptInput(ClosedModel):
    kind: Literal["external_receipt"]
    receipt: SignedExternalReceipt


DeliveryInput = Annotated[Union[ExternalReceiptInput, ManagedPayloadInput], Field(discriminator="kind")]


class RawPayloadObject(ClosedModel):
    content_id: ContentId
    algorithm: Literal["sha256"]
    byte_length: NonNegativeIntegerString
    media_type: MediaType
    content_encoding: ContentEncoding


class RawManifestObject(ClosedModel):
    content_id: ContentId
    algorithm: Literal["sha256"]
    byte_length: NonNegativeIntegerString


class RawReceipt(ClosedModel):
    schema_: Literal["ergasterion.raw-receipt/v1"] = Field(alias="schema")
    claim_digest: Digest
    payload: RawPayloadObject
    manifest: RawManifestObject
    frame_sequence_digest: Digest | None = None
    raw_receipt_digest: Digest

    _omittable_not_nullable = frozenset({"frame_sequence_digest"})
    model_config = ConfigDict(extra="forbid", frozen=True, populate_by_name=True)


class ReleaseVisibilityIdentity(ClosedModel):
    epoch: NonNegativeIntegerString
    kind: Literal["release"]
    id: Digest


class ReprocessVisibilityIdentity(ClosedModel):
    epoch: NonNegativeIntegerString
    kind: Literal["reprocess"]
    id: Digest


VisibilityIdentity = Annotated[
    Union[DeliveryVisibilityIdentity, ReleaseVisibilityIdentity, ReprocessVisibilityIdentity],
    Field(discriminator="kind"),
]


class BronzeEvidence(ClosedModel):
    raw_receipt: RawReceipt
    candidate_ref: OpaqueRef
    candidate_digest: Digest
    frame_index_ref: OpaqueRef
    frame_index_digest: Digest
    visibility: VisibilityIdentity


class RawReadHandle(ClosedModel):
    raw_receipt_digest: Digest
    content_id: ContentId
    byte_length: NonNegativeIntegerString
    handle_ref: OpaqueRef


class RawReadPage(ClosedModel):
    handle_ref: OpaqueRef
    offset: NonNegativeIntegerString
    bytes_base64url: ByteStringBase64Url
    bytes_returned: NonNegativeIntegerString
    next_offset: NonNegativeIntegerString | None
    eof: bool


class LandingPreparation(ClosedModel):
    preparation_id: Digest
    attempt_id: Digest
    raw_receipt_digest: Digest
    next_offset: NonNegativeIntegerString
    closed: bool


class CandidateField(ClosedModel):
    name: Identifier
    value: TypedScalar | None


class CandidateFrame(ClosedModel):
    frame_sequence: NonNegativeIntegerString
    raw_locator: RawLocator
    typed_fields: tuple[CandidateField, ...] | None
    structural_findings: tuple[Finding, ...]


class CandidateFramePage(ClosedModel):
    frames: tuple[CandidateFrame, ...]
    next_after_sequence: NonNegativeIntegerString | None
    bytes_returned: NonNegativeIntegerString
    more: bool


class MaterializationSession(ClosedModel):
    session_id: Digest
    attempt_id: Digest
    evaluation_id: Digest
    ruleset_digest: Digest
    next_frame_sequence: NonNegativeIntegerString
    closed: bool


class CandidateReadQuery(ClosedModel):
    evidence: BronzeEvidence
    after_sequence: NonNegativeIntegerString | None
    max_frames: PositiveInteger
    max_bytes: PositiveIntegerString


# --------------------------------------------------------------------------- validation / disposition

class Disposition(ClosedModel):
    disposition_id: Digest
    raw_ref: OpaqueRef
    raw_locator: RawLocator
    delivery_id: Token
    claim_digest: Digest
    ruleset_digest: Digest
    product_version: SemVer
    contract_digest: Digest
    source_schema_digest: Digest
    published_schema_digest: Digest
    status: DispositionStatus
    findings: tuple[Finding, ...]
    outcome_digest: Digest


class DispositionPage(ClosedModel):
    session_id: Digest
    dispositions: tuple[Disposition, ...]
    first_frame_sequence: NonNegativeIntegerString
    next_frame_sequence: NonNegativeIntegerString
    bytes_supplied: NonNegativeIntegerString


class ValidationResult(ClosedModel):
    schema_: Literal["ergasterion.validation-result/v1"] = Field(alias="schema")
    evaluation_id: Digest
    ruleset_digest: Digest
    batch_findings: tuple[Finding, ...]
    framed_count: NonNegativeIntegerString
    accepted_count: NonNegativeIntegerString
    error_count: NonNegativeIntegerString
    warning_count: NonNegativeIntegerString
    quarantined_count: NonNegativeIntegerString
    error_numerator: NonNegativeIntegerString
    error_denominator: NonNegativeIntegerString
    publication_decision: PublicationDecision
    validation_result_digest: Digest

    model_config = ConfigDict(extra="forbid", frozen=True, populate_by_name=True)


class SnapshotAcceptance(ClosedModel):
    source_snapshot_complete: Literal[True]
    accepted_snapshot_complete: bool
    framed_count: NonNegativeIntegerString
    accepted_count: NonNegativeIntegerString
    quarantined_count: NonNegativeIntegerString
    validation_result_digest: Digest
    publication_decision: PublicationDecision


class MaterializationCompletion(ClosedModel):
    session: MaterializationSession
    validation: ValidationResult
    candidate_keyset: "SnapshotKeyset | None"
    output_visibility: VisibilityIdentity | None


class MaterializedBronzeEvidence(ClosedModel):
    prepared: BronzeEvidence
    disposition_ref: OpaqueRef
    accepted_ref: OpaqueRef
    accepted_content_digest: Digest
    candidate_keyset: "SnapshotKeyset | None"
    published_visibility: VisibilityIdentity | None


class ReleaseVisibilityBinding(ClosedModel):
    materialized: MaterializedBronzeEvidence
    visibility: ReleaseVisibilityIdentity


class ReleaseMaterializationRequest(ClosedModel):
    """Re-materialize a compare-and-swap-accepted selected-locator release into
    the published projection: the frames already carry typed content from the
    original ``finish_prepare`` typing pass (a quarantine finding narrows
    disposition, not typing), so this reads that existing typed content back
    by locator rather than re-deriving it. Idempotent on ``release_id`` -- a
    replayed release must not duplicate the accepted row it already
    materialized."""

    raw_ref: OpaqueRef
    selected_locators: tuple[RawLocator, ...]
    release_id: Digest
    visibility: ReleaseVisibilityIdentity
    accepted_content_digest: Digest


# --------------------------------------------------------------------------- reprocessing / remediation

class ReprocessingClaim(ClosedModel):
    schema_: Literal["ergasterion.reprocessing-claim/v1"] = Field(alias="schema")
    original_claim_digest: Digest
    raw_receipt_digest: Digest
    target_product_version: SemVer
    target_contract_digest: Digest
    target_source_schema_digest: Digest
    target_published_schema_digest: Digest
    target_ruleset_digest: Digest
    execution_plan_digest: Digest
    reprocessing_id: Digest

    model_config = ConfigDict(extra="forbid", frozen=True, populate_by_name=True)


class RemediationEvaluation(ClosedModel):
    schema_: Literal["ergasterion.remediation-evaluation/v1"] = Field(alias="schema")
    original_claim_digest: Digest
    raw_receipt_digest: Digest
    target_contract_digest: Digest
    target_source_schema_digest: Digest
    target_published_schema_digest: Digest
    target_ruleset_digest: Digest
    execution_plan_digest: Digest
    root_visibility_epoch: NonNegativeIntegerString
    remediation_evaluation_id: Digest

    model_config = ConfigDict(extra="forbid", frozen=True, populate_by_name=True)


class RemediationRelease(ClosedModel):
    schema_: Literal["ergasterion.remediation-release/v1"] = Field(alias="schema")
    remediation_evaluation_id: Digest
    selected_locators: tuple[RawLocator, ...]
    accepted_content_digest: Digest
    release_id: Digest

    model_config = ConfigDict(extra="forbid", frozen=True, populate_by_name=True)


class RemediationDecision(ClosedModel):
    schema_: Literal["ergasterion.remediation-decision/v1"] = Field(alias="schema")
    decision_id: Digest
    kind: RemediationDecisionKind
    evaluation: RemediationEvaluation
    disposition_ids: tuple[Digest, ...]
    validation_result_digest: Digest
    release: RemediationRelease | None
    decided_at: UtcInstant

    model_config = ConfigDict(extra="forbid", frozen=True, populate_by_name=True)


class RemediationCommitCheckpoint(ClosedModel):
    schema_: Literal["ergasterion.remediation-commit-checkpoint/v1"] = Field(alias="schema")
    attempt_id: Digest
    decision: RemediationDecision
    release_projection_payload: "RemediationReleasePayload"
    checkpoint_digest: Digest

    model_config = ConfigDict(extra="forbid", frozen=True, populate_by_name=True)


# --------------------------------------------------------------------------- attempts / stream state

class Attempt(ClosedModel):
    run_id: Digest
    attempt_id: Digest
    logical_identity: LogicalIdentity
    claim_digest: Digest
    delivery_id: Token | None = None
    reprocessing_id: Digest | None = None
    remediation_evaluation_id: Digest | None = None
    scheduled_boundary_at: UtcInstant
    attempt_ordinal: PositiveInteger
    state: AttemptState
    block_phase: BlockPhase | None
    reason_code: ErrorCode | None
    execution_plan_digest: Digest
    runtime_manifest_digest: Digest
    remediation_commit_checkpoint: RemediationCommitCheckpoint | None = None
    snapshot_acceptance: SnapshotAcceptance | None = None
    state_revision: NonNegativeIntegerString
    projection_revision: NonNegativeIntegerString | None = None
    committed_at: UtcInstant | None = None

    _omittable_not_nullable = frozenset({
        "delivery_id", "reprocessing_id", "remediation_evaluation_id",
        "remediation_commit_checkpoint", "snapshot_acceptance",
        "projection_revision", "committed_at",
    })


class StreamState(ClosedModel):
    logical_identity: LogicalIdentity
    active_contract_digest: Digest | None
    visibility_epoch: NonNegativeIntegerString
    accepted_progress: dict[str, TypedScalar]
    last_committed_visibility: VisibilityIdentity | None = None
    last_committed_at: UtcInstant | None = None
    snapshot_reconciliation: "SnapshotReconciliation | None"
    state_revision: NonNegativeIntegerString
    required_projection_revision: NonNegativeIntegerString

    _omittable_not_nullable = frozenset({"last_committed_visibility", "last_committed_at"})


class OperationalStatus(ClosedModel):
    state: StreamState
    latest_attempt: Attempt | None
    processing: ProcessingOutcome
    block_phase: BlockPhase | None
    incomplete_outbox_count: NonNegativeIntegerString


class AttemptPage(ClosedModel):
    attempts: tuple[Attempt, ...]
    next_after_attempt_id: Digest | None
    more: bool


class AttemptQuery(ClosedModel):
    logical_identity: LogicalIdentity
    claim_digest: Digest | None
    nonterminal_only: bool
    after_attempt_id: Digest | None
    max_items: PositiveInteger


class ContractLifecycleRequest(ClosedModel):
    schema_: Literal["ergasterion.contract-lifecycle-request/v1"] = Field(alias="schema")
    action: ContractLifecycleAction
    expected_state_revision: NonNegativeIntegerString
    expected_deployment_revision: NonNegativeIntegerString | None
    contract: BronzeProductContract
    migration: Migration | None
    permit_pre_intent_fence: bool

    model_config = ConfigDict(extra="forbid", frozen=True, populate_by_name=True)


class ContractLifecycleTransitionResult(ClosedModel):
    state: StreamState
    deployment: RuntimeDeployment | None
    fenced_attempt_ids: tuple[Digest, ...]


class DeploymentLifecycleTransitionResult(ClosedModel):
    state: StreamState
    deployment: RuntimeDeployment
    catchup_cursor: ProjectionCursor
    fenced_attempt_ids: tuple[Digest, ...]


# --------------------------------------------------------------------------- projection intent / confirmation

class DeliveryPublicationPayload(ClosedModel):
    kind: Literal["delivery_publication"]
    attempt_id: Digest
    visibility: VisibilityIdentity
    product_version: SemVer
    contract_digest: Digest
    source_schema_digest: Digest
    published_schema_digest: Digest
    readiness_digest: Digest
    delivery_claim_digest: Digest
    transport_payload_digest: Digest
    raw_receipt_ref: OpaqueRef
    raw_receipt_digest: Digest
    bronze_partition_ref: OpaqueRef
    accepted_content_digest: Digest
    ruleset_digest: Digest
    validation_result_digest: Digest
    accepted_count: NonNegativeIntegerString
    progress_claim: ProgressClaim
    deletion_evidence: "DeletionEvidenceIntent | None"
    scheduled_boundary_at: UtcInstant
    warning_deadline_at: UtcInstant
    error_deadline_at: UtcInstant
    prior_committed_at: UtcInstant | None
    lineage_digest: Digest


class WholeDeliveryReprocessingPayload(ClosedModel):
    kind: Literal["whole_delivery_reprocessing"]
    attempt_id: Digest
    reprocessing_id: Digest
    original_delivery_claim_digest: Digest
    transport_payload_digest: Digest
    visibility: VisibilityIdentity
    product_version: SemVer
    contract_digest: Digest
    source_schema_digest: Digest
    published_schema_digest: Digest
    readiness_digest: Digest
    raw_receipt_ref: OpaqueRef
    raw_receipt_digest: Digest
    bronze_partition_ref: OpaqueRef
    accepted_content_digest: Digest
    ruleset_digest: Digest
    validation_result_digest: Digest
    accepted_count: NonNegativeIntegerString
    progress_claim: ProgressClaim
    deletion_evidence: "DeletionEvidenceIntent | None"
    scheduled_boundary_at: UtcInstant
    warning_deadline_at: UtcInstant
    error_deadline_at: UtcInstant
    prior_committed_at: UtcInstant | None
    lineage_digest: Digest


class RemediationReleasePayload(ClosedModel):
    kind: Literal["remediation_release"]
    attempt_id: Digest
    remediation_evaluation_id: Digest
    release_id: Digest
    visibility: VisibilityIdentity
    product_version: SemVer
    contract_digest: Digest
    source_schema_digest: Digest
    published_schema_digest: Digest
    readiness_digest: Digest
    delivery_claim_digest: Digest
    transport_payload_digest: Digest
    raw_receipt_ref: OpaqueRef
    raw_receipt_digest: Digest
    bronze_partition_ref: OpaqueRef
    accepted_content_digest: Digest
    ruleset_digest: Digest
    validation_result_digest: Digest
    accepted_count: NonNegativeIntegerString
    progress_claim: ProgressClaim
    deletion_evidence: "DeletionEvidenceIntent | None"
    prior_committed_at: UtcInstant | None
    lineage_digest: Digest


class VersionInterface(ClosedModel):
    logical_identity: LogicalIdentity
    product_version: SemVer
    contract_digest: Digest
    root_visibility_epoch: NonNegativeIntegerString
    relation_ref: OpaqueRef
    active: bool


class VisibilityAncestryRow(ClosedModel):
    logical_identity: LogicalIdentity
    descendant_epoch: NonNegativeIntegerString
    ancestor_epoch: NonNegativeIntegerString
    projection_target: Token
    projection_revision: NonNegativeIntegerString


class MigrationProjectionPayload(ClosedModel):
    kind: Literal["migration"]
    migration: Migration
    version_interface: VersionInterface
    ancestry: tuple[VisibilityAncestryRow, ...]
    readiness_digest: Digest
    prior_committed_at: UtcInstant | None


class ProcessingProjectionPayload(ClosedModel):
    kind: Literal["processing"]
    attempt: Attempt
    processing: ProcessingOutcome
    prior_committed_at: UtcInstant | None


class TimelinessProjectionPayload(ClosedModel):
    kind: Literal["timeliness"]
    scheduled_boundary_at: UtcInstant
    warning_deadline_at: UtcInstant
    error_deadline_at: UtcInstant
    timeliness: TimelinessState
    evaluated_through_at: UtcInstant
    prior_committed_at: UtcInstant | None


class HeartbeatProjectionPayload(ClosedModel):
    kind: Literal["heartbeat"]
    heartbeat_at: UtcInstant
    evaluated_through_at: UtcInstant
    prior_committed_at: UtcInstant | None


ProjectionPayload = Annotated[
    Union[
        DeliveryPublicationPayload, HeartbeatProjectionPayload, MigrationProjectionPayload,
        ProcessingProjectionPayload, RemediationReleasePayload, TimelinessProjectionPayload,
        WholeDeliveryReprocessingPayload,
    ],
    Field(discriminator="kind"),
]


class ProjectionIntent(ClosedModel):
    schema_: Literal["ergasterion.projection-intent/v1"] = Field(alias="schema")
    logical_identity: LogicalIdentity
    contract_digest: Digest
    projection_target: Token
    projection_revision: NonNegativeIntegerString
    originating_state_revision: NonNegativeIntegerString
    kind: ProjectionIntentKind
    execution_plan_digest: Digest
    runtime_manifest_digest: Digest
    payload: ProjectionPayload
    payload_digest: Digest
    projection_intent_digest: Digest

    model_config = ConfigDict(extra="forbid", frozen=True, populate_by_name=True)


class DeletionEvidenceIntent(ClosedModel):
    logical_identity: LogicalIdentity
    visibility: VisibilityIdentity
    delete_strategy: DeleteStrategy
    claim_digest: Digest
    attempt_id: Digest
    event_sequence_low: IntegerString | None
    event_sequence_high: IntegerString | None
    record_key_scope: FingerprintScope
    hmac_key_id: Token
    key_commitment: Digest
    deleted_keyset_ref: OpaqueRef
    deleted_keyset_digest: Digest
    deleted_key_count: NonNegativeIntegerString
    reconciliation_digest: Digest | None
    deletion_evidence_intent_digest: Digest


class DeletionEvidence(ClosedModel):
    intent: DeletionEvidenceIntent
    applied_at: UtcInstant
    deletion_evidence_digest: Digest


class ProjectionConfirmation(ClosedModel):
    schema_: Literal["ergasterion.projection-confirmation/v1"] = Field(alias="schema")
    logical_identity: LogicalIdentity
    contract_digest: Digest
    projection_target: Token
    kind: ProjectionIntentKind
    projection_intent_digest: Digest
    projection_revision: NonNegativeIntegerString
    target_applied_at: UtcInstant
    committed_at: UtcInstant | None
    release_applied_at: UtcInstant | None
    timeliness: TimelinessState | None
    processing: ProcessingOutcome
    visibility: VisibilityIdentity | None
    ledger_ref: OpaqueRef | None
    deletion_evidence: DeletionEvidence | None
    target_result_digest: Digest

    model_config = ConfigDict(extra="forbid", frozen=True, populate_by_name=True)


class ProjectionLogPage(ClosedModel):
    intents: tuple[ProjectionIntent, ...]
    next_after_revision: NonNegativeIntegerString | None
    bytes_returned: NonNegativeIntegerString
    more: bool


class ProjectionConfirmationLogPage(ClosedModel):
    confirmations: tuple[ProjectionConfirmation, ...]
    next_after_revision: NonNegativeIntegerString | None
    bytes_returned: NonNegativeIntegerString
    more: bool


class ProjectionReplayBatch(ClosedModel):
    intents: tuple[ProjectionIntent, ...]
    confirmations: tuple[ProjectionConfirmation, ...]
    max_items: PositiveInteger
    max_bytes: PositiveIntegerString
    bytes_supplied: NonNegativeIntegerString


class StreamStatus(ClosedModel):
    logical_identity: LogicalIdentity
    contract_digest: Digest
    projection_target: Token
    projection_revision: NonNegativeIntegerString
    projected_at: UtcInstant
    scheduled_boundary_at: UtcInstant | None
    processing: ProcessingOutcome
    timeliness: TimelinessState
    latest_attempt: Attempt | None
    committed_at: UtcInstant | None
    accepted_progress: dict[str, TypedScalar]
    latest_snapshot_visibility: VisibilityIdentity | None
    snapshot_reconciliation: SnapshotReconciliationStatus
    heartbeat_at: UtcInstant
    evaluated_through_at: UtcInstant


class PublishedLedgerRow(ClosedModel):
    logical_identity: LogicalIdentity
    visibility: VisibilityIdentity
    projection_target: Token
    product_version: SemVer
    contract_digest: Digest
    source_schema_digest: Digest
    published_schema_digest: Digest
    delivery_claim_digest: Digest
    transport_payload_digest: Digest
    raw_receipt_ref: OpaqueRef
    raw_receipt_digest: Digest
    bronze_partition_ref: OpaqueRef
    accepted_content_digest: Digest
    ruleset_digest: Digest
    validation_result_digest: Digest
    accepted_count: NonNegativeIntegerString
    progress_claim: ProgressClaim
    execution_plan_digest: Digest
    runtime_manifest_digest: Digest
    committed_at: UtcInstant
    release_applied_at: UtcInstant | None
    projection_revision: NonNegativeIntegerString


# --------------------------------------------------------------------------- lifecycle events

class AttemptLifecyclePayload(ClosedModel):
    kind: AttemptState
    attempt: Attempt
    projection_confirmation: ProjectionConfirmation | None


class LineageDescriptor(ClosedModel):
    logical_identity: LogicalIdentity
    projection: tuple[ProjectionField, ...]
    execution_plan_digest: Digest
    lineage_digest: Digest


class RunLineage(ClosedModel):
    schema_: Literal["ergasterion.run-lineage/v1"] = Field(alias="schema")
    run_id: Digest
    attempt_id: Digest
    logical_identity: LogicalIdentity
    delivery_id: Token | None
    reprocessing_id: Digest | None
    remediation_evaluation_id: Digest | None
    transport_payload_digest: Digest
    delivery_claim_digest: Digest
    ruleset_digest: Digest | None
    validation_result_digest: Digest | None
    accepted_count: NonNegativeIntegerString
    quarantined_count: NonNegativeIntegerString
    product_version: SemVer
    contract_digest: Digest
    source_schema_digest: Digest
    published_schema_digest: Digest
    execution_plan_digest: Digest
    runtime_manifest_digest: Digest
    landing_ref: OpaqueRef
    publication_ref: OpaqueRef | None
    result: ProcessingOutcome
    committed_at: UtcInstant | None
    run_lineage_digest: Digest

    model_config = ConfigDict(extra="forbid", frozen=True, populate_by_name=True)


class LineageLifecyclePayload(ClosedModel):
    kind: Literal["bronze.lineage"]
    lineage: LineageDescriptor
    run_lineage: RunLineage


class ProductMetadata(ClosedModel):
    logical_identity: LogicalIdentity
    product: ProductFacts
    contract_digest: Digest
    source_schema_digest: Digest
    published_schema_digest: Digest
    source_schema: tuple[SourceField, ...]
    quality: QualityPolicy
    interfaces: BronzeInterfaces
    latest_stream_status_ref: OpaqueRef
    latest_publication_ref: OpaqueRef | None = None

    _omittable_not_nullable = frozenset({"latest_publication_ref"})


class MetadataLifecyclePayload(ClosedModel):
    kind: Literal["bronze.metadata"]
    metadata: ProductMetadata


class ContractEvidenceLifecyclePayload(ClosedModel):
    kind: Literal["bronze.contract"]
    contract: BronzeProductContract


class SchemaEvidenceLifecyclePayload(ClosedModel):
    kind: Literal["bronze.schema"]
    metadata: ProductMetadata


class ReceiptLifecyclePayload(ClosedModel):
    kind: Literal["bronze.receipt"]
    receipt: RawReceipt


class QualityLifecyclePayload(ClosedModel):
    kind: Literal["bronze.quality"]
    validation: ValidationResultHandoff


class QuarantineLifecyclePayload(ClosedModel):
    kind: Literal["bronze.quarantine"]
    validation: ValidationResultHandoff
    decision: RemediationDecision | None


class PublicationConfirmationHandoff(ClosedModel):
    run_id: Digest
    attempt_id: Digest
    confirmation: ProjectionConfirmation


class PublicationLifecyclePayload(ClosedModel):
    kind: Literal["bronze.publication"]
    confirmation: PublicationConfirmationHandoff
    ledger: PublishedLedgerRow


class DeletionEvidenceLifecyclePayload(ClosedModel):
    kind: Literal["bronze.deletion_evidence"]
    evidence: DeletionEvidence


# ``AttemptLifecyclePayload.kind`` ranges over the whole ``AttemptState`` enum (eight
# possible tags), not one fixed literal like the other nine variants -- so this union
# cannot be a Pydantic discriminated union (which requires one fixed literal tag per
# variant). It is a plain ``Union``; Pydantic's smart-mode union validation still
# resolves it correctly by shape.
LifecyclePayload = Union[
    AttemptLifecyclePayload, ContractEvidenceLifecyclePayload, DeletionEvidenceLifecyclePayload,
    LineageLifecyclePayload, MetadataLifecyclePayload, PublicationLifecyclePayload,
    QualityLifecyclePayload, QuarantineLifecyclePayload, ReceiptLifecyclePayload,
    SchemaEvidenceLifecyclePayload,
]


class LifecycleEvent(ClosedModel):
    event_id: Digest
    event_type: "bronze_contract.LifecycleEventType"
    logical_identity: LogicalIdentity
    state_revision: NonNegativeIntegerString
    event_ordinal: NonNegativeIntegerString
    attempt_id: Digest | None = None
    execution_plan_digest: Digest
    runtime_manifest_digest: Digest
    payload: LifecyclePayload
    payload_digest: Digest
    created_at: UtcInstant

    _omittable_not_nullable = frozenset({"attempt_id"})


class LifecycleEventCursor(ClosedModel):
    state_revision: NonNegativeIntegerString
    event_ordinal: NonNegativeIntegerString
    event_id: Digest


class LifecycleEventLogPage(ClosedModel):
    events: tuple[LifecycleEvent, ...]
    next_cursor: LifecycleEventCursor | None
    bytes_returned: NonNegativeIntegerString
    more: bool


class LifecycleEventLogQuery(ClosedModel):
    logical_identity: LogicalIdentity
    after_cursor: LifecycleEventCursor | None
    max_items: PositiveInteger
    max_bytes: PositiveIntegerString


class LifecycleEventBatch(ClosedModel):
    events: tuple[LifecycleEvent, ...]
    max_items: PositiveInteger
    bytes_supplied: NonNegativeIntegerString


class DeliveryClaim(ClosedModel):
    schema_: Literal["ergasterion.delivery-claim/v1"] = Field(alias="schema")
    claim: DeliveryManifest
    delivery_claim_digest: Digest

    model_config = ConfigDict(extra="forbid", frozen=True, populate_by_name=True)


# --------------------------------------------------------------------------- snapshot / tombstone keysets

class SnapshotKeyset(ClosedModel):
    keyset_id: Digest
    logical_identity: LogicalIdentity
    visibility: VisibilityIdentity
    record_key_scope: FingerprintScope
    hmac_key_id: Token
    key_commitment: Digest
    keyset_ref: OpaqueRef
    keyset_digest: Digest | None
    key_count: NonNegativeIntegerString
    complete: bool


class SnapshotKeysetRequest(ClosedModel):
    attempt_id: Digest
    logical_identity: LogicalIdentity
    visibility: VisibilityIdentity
    record_key_scope: FingerprintScope
    hmac_key_id: Token
    key_commitment: Digest


class SnapshotKeysetCompletion(ClosedModel):
    attempt_id: Digest
    keyset_id: Digest
    expected_key_count: NonNegativeIntegerString
    expected_keyset_digest: Digest


class TombstoneKeyset(ClosedModel):
    keyset_id: Digest
    logical_identity: LogicalIdentity
    visibility: DeliveryVisibilityIdentity
    record_key_scope: FingerprintScope
    hmac_key_id: Token
    key_commitment: Digest
    keyset_ref: OpaqueRef
    keyset_digest: Digest | None
    key_count: NonNegativeIntegerString
    event_sequence_low: IntegerString | None
    event_sequence_high: IntegerString | None
    complete: bool


class TombstoneKeysetRequest(ClosedModel):
    attempt_id: Digest
    logical_identity: LogicalIdentity
    visibility: DeliveryVisibilityIdentity
    record_key_scope: FingerprintScope
    hmac_key_id: Token
    key_commitment: Digest


class TombstoneKeysetCompletion(ClosedModel):
    attempt_id: Digest
    keyset_id: Digest
    expected_key_count: NonNegativeIntegerString
    expected_keyset_digest: Digest
    event_sequence_low: IntegerString | None
    event_sequence_high: IntegerString | None


class TombstoneTag(ClosedModel):
    event_sequence: IntegerString
    tag: Digest


class TombstoneTagPage(ClosedModel):
    keyset_id: Digest
    items: tuple[TombstoneTag, ...]
    bytes_supplied: NonNegativeIntegerString


class RecordKeyTagPage(ClosedModel):
    keyset_id: Digest
    first_frame_sequence: NonNegativeIntegerString
    next_frame_sequence: NonNegativeIntegerString
    tags: tuple[Digest, ...]
    bytes_supplied: NonNegativeIntegerString


class SnapshotReconciliation(ClosedModel):
    schema_: Literal["ergasterion.snapshot-reconciliation/v1"] = Field(alias="schema")
    logical_identity: LogicalIdentity
    attempt_id: Digest
    candidate_visibility: DeliveryVisibilityIdentity
    prior_visibility: VisibilityIdentity | None
    prior_keyset_ref: OpaqueRef | None
    candidate_keyset_ref: OpaqueRef
    status: SnapshotReconciliationStatus
    attempt_count: NonNegativeIntegerString
    next_attempt_at: UtcInstant | None
    lease_owner: Token | None
    lease_expires_at: UtcInstant | None
    reason_code: ErrorCode | None
    deletion_evidence: DeletionEvidenceIntent | None
    reconciliation_digest: Digest

    model_config = ConfigDict(extra="forbid", frozen=True, populate_by_name=True)


class SnapshotReconciliationRequest(ClosedModel):
    attempt_id: Digest
    claim_digest: Digest
    prior_keyset: SnapshotKeyset | None
    candidate_keyset: SnapshotKeyset


class SnapshotReconciliationResult(ClosedModel):
    reconciliation: SnapshotReconciliation
    deletion_evidence: DeletionEvidenceIntent


class TombstoneEvidenceRequest(ClosedModel):
    attempt_id: Digest
    claim_digest: Digest
    keyset: TombstoneKeyset


# --------------------------------------------------------------------------- outbox

class OutboxEntry(ClosedModel):
    outbox_id: Digest
    logical_identity: LogicalIdentity
    entry_kind: OutboxEntryKind
    payload_ref: OpaqueRef
    payload_digest: Digest
    status: OutboxStatus
    dispatch_ordinal: PositiveInteger
    next_not_before: UtcInstant
    lease_owner: Token | None
    lease_expires_at: UtcInstant | None
    reason_code: ErrorCode | None
    completed_at: UtcInstant | None


class ProjectionOutboxPayload(ClosedModel):
    entry_kind: Literal["projection"]
    intent: ProjectionIntent


class LifecycleOutboxPayload(ClosedModel):
    entry_kind: Literal["lifecycle"]
    event: LifecycleEvent


class EvidenceOutboxPayload(ClosedModel):
    entry_kind: Literal["evidence"]
    reconciliation: SnapshotReconciliation


OutboxPayload = Annotated[
    Union[EvidenceOutboxPayload, LifecycleOutboxPayload, ProjectionOutboxPayload],
    Field(discriminator="entry_kind"),
]


class OutboxEnqueue(ClosedModel):
    outbox_id: Digest
    payload: OutboxPayload
    payload_digest: Digest
    next_not_before: UtcInstant


class OutboxCompletion(ClosedModel):
    outbox_id: Digest
    payload_digest: Digest
    lease_owner: Token
    completed_at: UtcInstant


class StateOutboxTransaction(ClosedModel):
    expected_state_revision: NonNegativeIntegerString
    next_state: StreamState
    attempt_updates: tuple[Attempt, ...]
    deployment_update: RuntimeDeployment | None
    projection_confirmation: ProjectionConfirmation | None
    enqueue: tuple[OutboxEnqueue, ...]
    complete: tuple[OutboxCompletion, ...]


class OutboxFailureTransaction(ClosedModel):
    expected_state_revision: NonNegativeIntegerString
    next_state: StreamState
    attempt_updates: tuple[Attempt, ...]
    outbox_id: Digest
    payload_digest: Digest
    lease_owner: Token
    failure_observed_at: UtcInstant
    reason_code: ErrorCode
    disposition: OutboxFailureDisposition
    next_not_before: UtcInstant | None


# --------------------------------------------------------------------------- keys / MAC / backup

class VerificationKeyRecord(ClosedModel):
    key_id: Token
    algorithm: Literal["Ed25519"]
    public_key_base64url: Base64Url
    public_key_fingerprint: Digest
    enabled_at: UtcInstant
    expires_at: UtcInstant | None = None
    revoked_at: UtcInstant | None = None
    authorized_policy_refs: tuple[Token, ...]
    trust_record_digest: Digest

    _omittable_not_nullable = frozenset({"expires_at", "revoked_at"})


class KeyCommitmentRecord(ClosedModel):
    key_id: Token
    algorithm: Literal["HMAC-SHA-256"]
    commitment: Digest


class MacResult(ClosedModel):
    algorithm: Literal["HMAC-SHA-256"]
    key_id: Token
    tag_hex: Digest


class BackupEntry(ClosedModel):
    relative_path: str
    mode: FileMode
    size_bytes: NonNegativeIntegerString
    sha256: Digest


class BackupEntryPage(ClosedModel):
    schema_: Literal["ergasterion.local-backup-entry-page/v1"] = Field(alias="schema")
    page_index: NonNegativeIntegerString
    previous_page_digest: Digest | None
    entries: tuple[BackupEntry, ...]
    page_digest: Digest

    model_config = ConfigDict(extra="forbid", frozen=True, populate_by_name=True)


class BackupManifest(ClosedModel):
    schema_: Literal["ergasterion.local-backup/v1"] = Field(alias="schema")
    backup_id: Digest
    runtime_binding_digest: Digest
    runtime_manifest_digest: Digest
    state_revision: NonNegativeIntegerString
    projection_revision: NonNegativeIntegerString
    created_at: UtcInstant
    entry_count: NonNegativeIntegerString
    page_count: NonNegativeIntegerString
    entry_pages_ref: OpaqueRef
    final_entry_page_digest: Digest | None
    manifest_digest: Digest

    model_config = ConfigDict(extra="forbid", frozen=True, populate_by_name=True)


# --------------------------------------------------------------------------- command envelope / results

class CommandError(ClosedModel):
    code: ErrorCode
    category: ErrorCategory
    retryable: bool
    message: str
    field_path: JsonPointer | None = None
    safe_ref: OpaqueRef | None = None

    _omittable_not_nullable = frozenset({"field_path", "safe_ref"})


class PlanCommandResult(ClosedModel):
    kind: Literal["plan"]
    execution_plan: "bronze_contract.ExecutionPlan"
    runtime_manifest: RuntimeManifest
    runtime_manifest_digest: Digest
    findings: tuple[Finding, ...]


class ContractRegisteredResult(ClosedModel):
    kind: Literal["contract_registered"]
    contract_digest: Digest
    source_schema_digest: Digest
    published_schema_digest: Digest
    execution_plan_digest: Digest
    runtime_manifest_digest: Digest
    readiness_digest: Digest
    state_revision: NonNegativeIntegerString


class ContractActivationResult(ClosedModel):
    kind: Literal["contract_activation"]
    migration: Migration
    activation_state: ContractActivationState
    candidate_contract_digest: Digest
    active_contract_digest: Digest | None
    fenced_attempt_ids: tuple[Digest, ...]
    state_revision: NonNegativeIntegerString


class DeploymentRegisteredResult(ClosedModel):
    kind: Literal["deployment_registered"]
    runtime_manifest_digest: Digest
    capability_digests: tuple[Digest, ...]
    readiness_digest: Digest
    catchup_cursor: ProjectionCursor


class DeploymentActivationResult(ClosedModel):
    kind: Literal["deployment_activation"]
    deployment: RuntimeDeployment
    previous_manifest_digest: Digest
    fenced_attempt_ids: tuple[Digest, ...]
    active_cursor: ProjectionCursor


class RetryDirective(ClosedModel):
    attempt_ordinal: PositiveInteger
    error_code: ErrorCode
    failure_observed_at: UtcInstant
    next_not_before: UtcInstant
    exhausted: bool


class IngestionResult(ClosedModel):
    kind: Literal["ingestion"]
    attempt: Attempt
    visibility: VisibilityIdentity | None
    publication: PublishedLedgerRow | None
    projection_confirmation: ProjectionConfirmation | None
    retry_directive: RetryDirective | None


class DueEvaluationResult(ClosedModel):
    kind: Literal["due_evaluation"]
    evaluated_through_at: UtcInstant
    transitions_applied: NonNegativeInteger
    state_revision: NonNegativeIntegerString
    projection_revisions: tuple[NonNegativeIntegerString, ...]
    more_due: bool
    continuation_after: UtcInstant | None = None

    _omittable_not_nullable = frozenset({"continuation_after"})


class ReconciliationResult(ClosedModel):
    kind: Literal["reconciliation"]
    target_cursors: tuple[ProjectionCursor, ...]
    actions: tuple[Token, ...]
    remaining_blocks: tuple[Digest, ...]
    confirmations: tuple[ProjectionConfirmation, ...]


class StreamStatusResult(ClosedModel):
    kind: Literal["stream_status"]
    stream_status: StreamStatus
    operational_status: OperationalStatus
    target_cursor: ProjectionCursor
    projection_lag: NonNegativeIntegerString


class EvidenceQuery(ClosedModel):
    logical_identity: LogicalIdentity
    evidence_kind: EvidenceKind
    immutable_id: OpaqueRef | None
    authorization_context_ref: OpaqueRef
    after_cursor: Token | None
    max_items: PositiveInteger
    max_bytes: PositiveIntegerString


class AttemptEvidenceItem(ClosedModel):
    kind: Literal["attempt"]
    attempt: Attempt
    confirmation: ProjectionConfirmation | None


class ContractEvidenceItem(ClosedModel):
    kind: Literal["contract"]
    contract: BronzeProductContract


class SchemaEvidenceItem(ClosedModel):
    kind: Literal["schema"]
    metadata: ProductMetadata


class ReceiptEvidenceItem(ClosedModel):
    kind: Literal["receipt"]
    receipt: RawReceipt


class QualityEvidenceItem(ClosedModel):
    kind: Literal["quality"]
    validation: ValidationResultHandoff


class LineageEvidenceItem(ClosedModel):
    kind: Literal["lineage"]
    lineage: LineageDescriptor
    run_lineage: RunLineage


class MetadataEvidenceItem(ClosedModel):
    kind: Literal["metadata"]
    metadata: ProductMetadata


class PublicationEvidenceItem(ClosedModel):
    kind: Literal["publication"]
    ledger: PublishedLedgerRow
    confirmation: PublicationConfirmationHandoff


class QuarantineEvidenceItem(ClosedModel):
    kind: Literal["quarantine"]
    validation: ValidationResultHandoff
    decision: RemediationDecision | None


class DeletionEvidenceItem(ClosedModel):
    kind: Literal["deletion_evidence"]
    evidence: DeletionEvidence


EvidenceRecord = Annotated[
    Union[
        AttemptEvidenceItem, ContractEvidenceItem, DeletionEvidenceItem, LineageEvidenceItem,
        MetadataEvidenceItem, PublicationEvidenceItem, QualityEvidenceItem, QuarantineEvidenceItem,
        ReceiptEvidenceItem, SchemaEvidenceItem,
    ],
    Field(discriminator="kind"),
]


class EvidencePage(ClosedModel):
    items: tuple[EvidenceRecord, ...]
    next_cursor: Token | None
    bytes_returned: NonNegativeIntegerString
    more: bool


class InspectionResult(ClosedModel):
    kind: Literal["inspection"]
    evidence: EvidencePage


class QuarantineSnapshot(ClosedModel):
    query_digest: Digest
    disposition_snapshot_token: Token
    remediation_snapshot_token: Token


class QuarantineCursor(ClosedModel):
    snapshot: QuarantineSnapshot
    disposition_after_cursor: Token | None
    active_disposition_id: Digest | None
    decision_after_cursor: Token | None


class QuarantineQuery(ClosedModel):
    logical_identity: LogicalIdentity
    disposition_id: Digest | None
    authorization_context_ref: OpaqueRef
    cursor: QuarantineCursor | None
    max_items: PositiveInteger
    max_bytes: PositiveIntegerString
    max_decisions_per_item: PositiveInteger
    max_decision_bytes_per_item: PositiveIntegerString


class RemediationDecisionQuery(ClosedModel):
    logical_identity: LogicalIdentity
    disposition_id: Digest | None
    authorization_context_ref: OpaqueRef
    snapshot_token: Token | None
    after_cursor: Token | None
    max_items: PositiveInteger
    max_bytes: PositiveIntegerString


class RemediationDecisionPage(ClosedModel):
    items: tuple[RemediationDecision, ...]
    snapshot_token: Token
    next_cursor: Token | None
    bytes_returned: NonNegativeIntegerString
    more: bool


class QuarantineItem(ClosedModel):
    disposition: Disposition
    decision_page: RemediationDecisionPage


class QuarantinePage(ClosedModel):
    items: tuple[QuarantineItem, ...]
    snapshot: QuarantineSnapshot
    next_cursor: QuarantineCursor | None
    bytes_returned: NonNegativeIntegerString
    more: bool


class QuarantineResult(ClosedModel):
    kind: Literal["quarantine"]
    action: QuarantineAction
    status: RemediationActionStatus
    evidence: QuarantinePage
    decision: RemediationDecision | None


class LocalBackupResult(ClosedModel):
    kind: Literal["local_backup"]
    action: BackupAction
    manifest_path: OpaqueRef
    manifest: BackupManifest
    verification_digest: Digest
    reconciliation: ReconciliationResult | None


class UnitResult(ClosedModel):
    ok: bool


CommandResult = Annotated[
    Union[
        ContractActivationResult, ContractRegisteredResult, DeploymentActivationResult,
        DeploymentRegisteredResult, DueEvaluationResult, IngestionResult, InspectionResult,
        LocalBackupResult, PlanCommandResult, QuarantineResult, ReconciliationResult,
        StreamStatusResult,
    ],
    Field(discriminator="kind"),
]


class CommandEnvelope(ClosedModel):
    schema_: Literal["ergasterion.command-result/v1"] = Field(alias="schema")
    command: Token
    status: "bronze_contract.CommandStatus"
    logical_identity: LogicalIdentity | None
    contract_digest: Digest | None
    execution_plan_digest: Digest | None
    runtime_manifest_digest: Digest | None
    result: CommandResult | None
    errors: tuple[CommandError, ...]

    model_config = ConfigDict(extra="forbid", frozen=True, populate_by_name=True)


class SourceNativeEvidenceItem(ClosedModel):
    kind: Literal["source_native"]
    frame: CandidateFrame
    disposition: Disposition | None


class SourceNativeQuery(ClosedModel):
    logical_identity: LogicalIdentity
    candidate_ref: OpaqueRef
    disposition_ref: OpaqueRef | None
    authorization_context_ref: OpaqueRef
    after_frame_sequence: NonNegativeIntegerString | None
    max_items: PositiveInteger
    max_bytes: PositiveIntegerString


class SourceNativePage(ClosedModel):
    items: tuple[SourceNativeEvidenceItem, ...]
    next_frame_sequence: NonNegativeIntegerString | None
    bytes_returned: NonNegativeIntegerString
    more: bool


class DispositionQuery(ClosedModel):
    logical_identity: LogicalIdentity
    disposition_id: Digest | None
    authorization_context_ref: OpaqueRef
    snapshot_token: Token | None
    after_cursor: Token | None
    max_items: PositiveInteger
    max_bytes: PositiveIntegerString


class DispositionQueryPage(ClosedModel):
    items: tuple[Disposition, ...]
    snapshot_token: Token
    next_cursor: Token | None
    bytes_returned: NonNegativeIntegerString
    more: bool


class ScratchScope(ClosedModel):
    scope_id: Token
    attempt_id: Digest
    capacity_bytes: PositiveIntegerString


class ScratchChunk(ClosedModel):
    scope_id: Token
    sequence: NonNegativeIntegerString
    bytes_base64url: Base64Url


class ScratchReadPage(ClosedModel):
    chunks: tuple[ScratchChunk, ...]
    next_sequence: NonNegativeIntegerString | None = None
    bytes_returned: NonNegativeIntegerString

    _omittable_not_nullable = frozenset({"next_sequence"})


# --------------------------------------------------------------------------- registry
#
# ``ALL_*`` merges this module's own records with ``bronze_contract`` and
# ``runtime_binding``'s registries -- the full, exact-153-record IDL coverage set the
# schema bundle and equivalence report are generated from.

RECORD_MODELS: dict[str, type[BaseModel]] = {
    "PayloadDescriptor": PayloadDescriptor,
    "SequenceProgressClaim": SequenceProgressClaim,
    "OpaqueProgressClaim": OpaqueProgressClaim,
    "SnapshotAttestationPayload": SnapshotAttestationPayload,
    "SignedAttestation": SignedAttestation,
    "DeliveryManifest": DeliveryManifest,
    "ManagedPayloadInput": ManagedPayloadInput,
    "DeliveryVisibilityIdentity": DeliveryVisibilityIdentity,
    "ExternalReceiptPayload": ExternalReceiptPayload,
    "SignedExternalReceipt": SignedExternalReceipt,
    "ExternalReceiptInput": ExternalReceiptInput,
    "RawPayloadObject": RawPayloadObject,
    "RawManifestObject": RawManifestObject,
    "RawReceipt": RawReceipt,
    "ReleaseVisibilityIdentity": ReleaseVisibilityIdentity,
    "ReprocessVisibilityIdentity": ReprocessVisibilityIdentity,
    "BronzeEvidence": BronzeEvidence,
    "RawReadHandle": RawReadHandle,
    "RawReadPage": RawReadPage,
    "LandingPreparation": LandingPreparation,
    "CandidateField": CandidateField,
    "CandidateFrame": CandidateFrame,
    "CandidateFramePage": CandidateFramePage,
    "MaterializationSession": MaterializationSession,
    "CandidateReadQuery": CandidateReadQuery,
    "Disposition": Disposition,
    "DispositionPage": DispositionPage,
    "ValidationResult": ValidationResult,
    "SnapshotAcceptance": SnapshotAcceptance,
    "MaterializationCompletion": MaterializationCompletion,
    "MaterializedBronzeEvidence": MaterializedBronzeEvidence,
    "ReleaseVisibilityBinding": ReleaseVisibilityBinding,
    "ReprocessingClaim": ReprocessingClaim,
    "RemediationEvaluation": RemediationEvaluation,
    "RemediationRelease": RemediationRelease,
    "RemediationDecision": RemediationDecision,
    "RemediationCommitCheckpoint": RemediationCommitCheckpoint,
    "Attempt": Attempt,
    "StreamState": StreamState,
    "OperationalStatus": OperationalStatus,
    "AttemptPage": AttemptPage,
    "AttemptQuery": AttemptQuery,
    "ContractLifecycleRequest": ContractLifecycleRequest,
    "ContractLifecycleTransitionResult": ContractLifecycleTransitionResult,
    "DeploymentLifecycleTransitionResult": DeploymentLifecycleTransitionResult,
    "DeliveryPublicationPayload": DeliveryPublicationPayload,
    "WholeDeliveryReprocessingPayload": WholeDeliveryReprocessingPayload,
    "RemediationReleasePayload": RemediationReleasePayload,
    "VersionInterface": VersionInterface,
    "VisibilityAncestryRow": VisibilityAncestryRow,
    "MigrationProjectionPayload": MigrationProjectionPayload,
    "ProcessingProjectionPayload": ProcessingProjectionPayload,
    "TimelinessProjectionPayload": TimelinessProjectionPayload,
    "HeartbeatProjectionPayload": HeartbeatProjectionPayload,
    "ProjectionIntent": ProjectionIntent,
    "DeletionEvidenceIntent": DeletionEvidenceIntent,
    "DeletionEvidence": DeletionEvidence,
    "ProjectionConfirmation": ProjectionConfirmation,
    "ProjectionLogPage": ProjectionLogPage,
    "ProjectionConfirmationLogPage": ProjectionConfirmationLogPage,
    "ProjectionReplayBatch": ProjectionReplayBatch,
    "StreamStatus": StreamStatus,
    "PublishedLedgerRow": PublishedLedgerRow,
    "AttemptLifecyclePayload": AttemptLifecyclePayload,
    "LineageDescriptor": LineageDescriptor,
    "RunLineage": RunLineage,
    "LineageLifecyclePayload": LineageLifecyclePayload,
    "ProductMetadata": ProductMetadata,
    "MetadataLifecyclePayload": MetadataLifecyclePayload,
    "ContractEvidenceLifecyclePayload": ContractEvidenceLifecyclePayload,
    "SchemaEvidenceLifecyclePayload": SchemaEvidenceLifecyclePayload,
    "ReceiptLifecyclePayload": ReceiptLifecyclePayload,
    "QualityLifecyclePayload": QualityLifecyclePayload,
    "PublicationConfirmationHandoff": PublicationConfirmationHandoff,
    "QuarantineLifecyclePayload": QuarantineLifecyclePayload,
    "PublicationLifecyclePayload": PublicationLifecyclePayload,
    "DeletionEvidenceLifecyclePayload": DeletionEvidenceLifecyclePayload,
    "LifecycleEvent": LifecycleEvent,
    "LifecycleEventCursor": LifecycleEventCursor,
    "LifecycleEventLogPage": LifecycleEventLogPage,
    "LifecycleEventLogQuery": LifecycleEventLogQuery,
    "LifecycleEventBatch": LifecycleEventBatch,
    "DeliveryClaim": DeliveryClaim,
    "SnapshotKeyset": SnapshotKeyset,
    "SnapshotKeysetRequest": SnapshotKeysetRequest,
    "SnapshotKeysetCompletion": SnapshotKeysetCompletion,
    "TombstoneKeyset": TombstoneKeyset,
    "TombstoneKeysetRequest": TombstoneKeysetRequest,
    "TombstoneKeysetCompletion": TombstoneKeysetCompletion,
    "TombstoneTag": TombstoneTag,
    "TombstoneTagPage": TombstoneTagPage,
    "RecordKeyTagPage": RecordKeyTagPage,
    "SnapshotReconciliation": SnapshotReconciliation,
    "SnapshotReconciliationRequest": SnapshotReconciliationRequest,
    "SnapshotReconciliationResult": SnapshotReconciliationResult,
    "TombstoneEvidenceRequest": TombstoneEvidenceRequest,
    "OutboxEntry": OutboxEntry,
    "ProjectionOutboxPayload": ProjectionOutboxPayload,
    "LifecycleOutboxPayload": LifecycleOutboxPayload,
    "EvidenceOutboxPayload": EvidenceOutboxPayload,
    "OutboxEnqueue": OutboxEnqueue,
    "OutboxCompletion": OutboxCompletion,
    "StateOutboxTransaction": StateOutboxTransaction,
    "OutboxFailureTransaction": OutboxFailureTransaction,
    "VerificationKeyRecord": VerificationKeyRecord,
    "KeyCommitmentRecord": KeyCommitmentRecord,
    "MacResult": MacResult,
    "BackupEntry": BackupEntry,
    "BackupEntryPage": BackupEntryPage,
    "BackupManifest": BackupManifest,
    "CommandError": CommandError,
    "CommandEnvelope": CommandEnvelope,
    "PlanCommandResult": PlanCommandResult,
    "ContractRegisteredResult": ContractRegisteredResult,
    "ContractActivationResult": ContractActivationResult,
    "DeploymentRegisteredResult": DeploymentRegisteredResult,
    "DeploymentActivationResult": DeploymentActivationResult,
    "RetryDirective": RetryDirective,
    "IngestionResult": IngestionResult,
    "DueEvaluationResult": DueEvaluationResult,
    "ReconciliationResult": ReconciliationResult,
    "StreamStatusResult": StreamStatusResult,
    "InspectionResult": InspectionResult,
    "QuarantineResult": QuarantineResult,
    "LocalBackupResult": LocalBackupResult,
    "UnitResult": UnitResult,
    "ScratchScope": ScratchScope,
    "ScratchChunk": ScratchChunk,
    "ScratchReadPage": ScratchReadPage,
    "EvidenceQuery": EvidenceQuery,
    "AttemptEvidenceItem": AttemptEvidenceItem,
    "ContractEvidenceItem": ContractEvidenceItem,
    "SchemaEvidenceItem": SchemaEvidenceItem,
    "ReceiptEvidenceItem": ReceiptEvidenceItem,
    "QualityEvidenceItem": QualityEvidenceItem,
    "LineageEvidenceItem": LineageEvidenceItem,
    "MetadataEvidenceItem": MetadataEvidenceItem,
    "PublicationEvidenceItem": PublicationEvidenceItem,
    "QuarantineEvidenceItem": QuarantineEvidenceItem,
    "QuarantineSnapshot": QuarantineSnapshot,
    "QuarantineCursor": QuarantineCursor,
    "QuarantineQuery": QuarantineQuery,
    "QuarantineItem": QuarantineItem,
    "QuarantinePage": QuarantinePage,
    "DeletionEvidenceItem": DeletionEvidenceItem,
    "SourceNativeEvidenceItem": SourceNativeEvidenceItem,
    "SourceNativeQuery": SourceNativeQuery,
    "SourceNativePage": SourceNativePage,
    "DispositionQuery": DispositionQuery,
    "DispositionQueryPage": DispositionQueryPage,
    "RemediationDecisionQuery": RemediationDecisionQuery,
    "RemediationDecisionPage": RemediationDecisionPage,
    "EvidencePage": EvidencePage,
}

UNION_MODELS: dict[str, object] = {
    "ProgressClaim": ProgressClaim,
    "DeliveryInput": DeliveryInput,
    "VisibilityIdentity": VisibilityIdentity,
    "ProjectionPayload": ProjectionPayload,
    "LifecyclePayload": LifecyclePayload,
    "OutboxPayload": OutboxPayload,
    "EvidenceRecord": EvidenceRecord,
    "CommandResult": CommandResult,
}

ALL_RECORD_MODELS: dict[str, type[BaseModel]] = {
    **bronze_contract.RECORD_MODELS,
    **runtime_binding.RECORD_MODELS,
    **RECORD_MODELS,
}
ALL_ENUM_MODELS = dict(bronze_contract.ENUM_MODELS)
ALL_UNION_MODELS: dict[str, object] = {
    **bronze_contract.UNION_MODELS,
    "LogicalType": bronze_contract.LogicalType,
    **UNION_MODELS,
}

for _model in ALL_RECORD_MODELS.values():
    _model.model_rebuild(force=True, _types_namespace={**vars(bronze_contract), **vars(runtime_binding), **globals()})
del _model

REVERSE_RECORD_NAMES: dict[type[BaseModel], str] = {cls: name for name, cls in ALL_RECORD_MODELS.items()}
"""Every record class mapped back to its exact IDL record name -- the equivalence
checker's field-type and union-variant comparisons resolve a Python annotation back to
an IDL name through this, rather than the other direction."""


# --------------------------------------------------------------------------- IDL type-expression resolution
#
# ``docs/specifications/bronze-portable-idl-v1.json``'s ``type_expression_grammar``: a
# field's ``type`` is a scalar/enum/record/union name, or ``list<T>``/``map<Token,T>``
# over one of those. This section resolves that string against the *actual* Python
# annotation a model class carries for the same field -- object identity for scalar
# aliases (``Digest`` is compared to the literal ``Annotated[str, ...]`` object bound to
# that name in ``bronze_contract``, not just "some string"), class identity for records
# and enums, structural equality for unions, and recursive unwrapping for ``tuple[X, ...]``
# / ``dict[str, X]``. It is what lets ``generate_equivalence_report`` catch a field typed
# with the wrong scalar, enum, record or union -- not merely a field with the right name.

SCALAR_ALIAS_BY_IDL_NAME: dict[str, object] = {
    "String": str,
    "Boolean": bool,
    "SafeInteger": bronze_contract.SafeInteger,
    "PositiveInteger": bronze_contract.PositiveInteger,
    "NonNegativeInteger": bronze_contract.NonNegativeInteger,
    "IntegerString": bronze_contract.IntegerString,
    "NonNegativeIntegerString": bronze_contract.NonNegativeIntegerString,
    "PositiveIntegerString": bronze_contract.PositiveIntegerString,
    "DecimalString": bronze_contract.DecimalString,
    "Digest": bronze_contract.Digest,
    "ContentId": bronze_contract.ContentId,
    "Base64Url": bronze_contract.Base64Url,
    "ByteStringBase64Url": bronze_contract.ByteStringBase64Url,
    "Token": bronze_contract.Token,
    "Identifier": bronze_contract.Identifier,
    "EstateNamespace": bronze_contract.EstateNamespace,
    "SemVer": bronze_contract.SemVer,
    "UtcInstant": bronze_contract.UtcInstant,
    "Date": bronze_contract.DateScalar,
    "FileMode": bronze_contract.FileMode,
    "JsonPointer": bronze_contract.JsonPointer,
    "OpaqueRef": bronze_contract.OpaqueRef,
    "ErrorCode": bronze_contract.ErrorCode,
}


def _strip_optional(annotation: object) -> object:
    """Drop a wrapping ``X | None``/``Optional[X]`` down to ``X``. A field the IDL marks
    nullable (either ``required: true, nullable: true`` or the omittable-not-nullable
    combination) always carries this wrapper in the Python annotation; the type identity
    the IDL cares about is the non-``None`` member."""

    origin = typing.get_origin(annotation)
    if origin is typing.Union or origin is types.UnionType:
        args = [a for a in typing.get_args(annotation) if a is not type(None)]
        if len(args) == 1:
            return args[0]
    return annotation


def _idl_type_matches(idl_type: str, annotation: object, field_meta: dict) -> tuple[bool, str]:
    """Resolve one IDL field-type expression against one Python field annotation.
    Returns ``(matches, reason)``; ``reason`` is human-readable and only meaningful when
    ``matches`` is ``False``."""

    annotation = _strip_optional(annotation)

    if idl_type.startswith("list<") and idl_type.endswith(">"):
        inner = idl_type[len("list<"):-1]
        if typing.get_origin(annotation) is not tuple:
            return False, f"expected tuple[...] for list<{inner}>, got {annotation!r}"
        args = typing.get_args(annotation)
        if len(args) != 2 or args[1] is not Ellipsis:
            return False, f"expected tuple[X, ...] shape for list<{inner}>, got {args!r}"
        return _idl_type_matches(inner, args[0], {})

    if idl_type.startswith("map<") and idl_type.endswith(">"):
        inner = idl_type[len("map<"):-1]
        key_type, _, value_type = inner.partition(",")
        if typing.get_origin(annotation) is not dict:
            return False, f"expected dict[...] for map<{inner}>, got {annotation!r}"
        args = typing.get_args(annotation)
        if len(args) != 2:
            return False, f"expected dict[K, V] shape for map<{inner}>, got {args!r}"
        if key_type != "Token" or args[0] is not str:
            return False, f"expected dict key str (Token) for map<{inner}>, got {args[0]!r}"
        return _idl_type_matches(value_type, args[1], {})

    # A field carrying an IDL ``const`` (schema-token fields, discriminator ``kind``
    # fields on non-Pydantic-discriminated unions like AttemptLifecyclePayload) is
    # projected as ``Literal[<const>]`` in Python regardless of whether the IDL names an
    # enum or the bare ``String`` scalar for it -- the enum/scalar identity check below
    # does not apply to a const field.
    if field_meta.get("const") is not None:
        if typing.get_origin(annotation) is Literal:
            largs = typing.get_args(annotation)
            if len(largs) == 1 and largs[0] == field_meta["const"]:
                return True, ""
        return False, f"expected Literal[{field_meta['const']!r}] for a const field, got {annotation!r}"

    if idl_type in SCALAR_ALIAS_BY_IDL_NAME:
        expected = SCALAR_ALIAS_BY_IDL_NAME[idl_type]
        if annotation == expected:
            return True, ""
        return False, f"expected scalar {idl_type}, got {annotation!r}"

    if idl_type in ALL_ENUM_MODELS:
        expected = ALL_ENUM_MODELS[idl_type]
        if annotation is expected:
            return True, ""
        return False, f"expected enum {idl_type}, got {annotation!r}"

    if idl_type in ALL_RECORD_MODELS:
        expected = ALL_RECORD_MODELS[idl_type]
        if annotation is expected:
            return True, ""
        return False, f"expected record {idl_type}, got {annotation!r}"

    if idl_type in ALL_UNION_MODELS:
        expected = ALL_UNION_MODELS[idl_type]
        if annotation == expected:
            return True, ""
        return False, f"expected union {idl_type}, got {annotation!r}"

    return False, f"unresolvable IDL type name {idl_type!r}"


def _union_variant_names(union_obj: object) -> set[str]:
    """The exact set of variant names a Python union object declares, resolved
    structurally (not by presence): each ``BaseModel`` variant resolves to its IDL
    record name via ``REVERSE_RECORD_NAMES``; each ``Enum`` variant (``LogicalType``'s
    bare-token branch) resolves to its own class name, which is also its IDL enum name."""

    target = union_obj
    if typing.get_origin(target) is Annotated:
        target = typing.get_args(target)[0]
    names: set[str] = set()
    for arg in typing.get_args(target):
        if isinstance(arg, type) and issubclass(arg, Enum):
            names.add(arg.__name__)
        elif arg in REVERSE_RECORD_NAMES:
            names.add(REVERSE_RECORD_NAMES[arg])
        else:
            names.add(getattr(arg, "__name__", str(arg)))
    return names


# --------------------------------------------------------------------------- ports
#
# A plain, dataclass-based transcription of the IDL's ``ports``/``port_operation_order``
# sections. It provides structural documentation and equivalence-check material; runtime
# port behaviour lives in the ingestion adapters. Every request/response type named below
# is a scalar/enum name or one of the record classes above.

@dataclass(frozen=True)
class PortMethod:
    request_fields: tuple[tuple[str, str], ...]
    response_type: str
    errors: tuple[str, ...]


@dataclass(frozen=True)
class PortDeclaration:
    kind: str
    methods: dict[str, PortMethod]


PORT_OPERATION_ORDER: dict[str, tuple[str, ...]] = {
    "source_connector": ("submit_managed", "verify_external"),
    "raw_store": ("get_receipt", "open_raw", "read_raw", "preserve", "verify_open"),
    "scratch_store": (
        "create_scope", "write_sequential", "read_sequential", "close_scope",
        "delete_scope", "cleanup_orphans",
    ),
    "state_store": (
        "contract_lifecycle", "deployment_lifecycle", "attempts", "state_transaction",
        "lease_outbox", "load_outbox_payload", "fail_outbox", "projection_log",
        "projection_confirmation_log", "lifecycle_event_log", "status_query",
        "begin_snapshot_keyset", "append_snapshot_keyset", "complete_snapshot_keyset",
        "get_snapshot_keyset", "reconcile_snapshot", "begin_tombstone_keyset",
        "append_tombstone_keyset", "complete_tombstone_keyset", "finalize_tombstone_evidence",
    ),
    "landing_adapter": (
        "begin_prepare", "append_raw", "finish_prepare", "read_candidate",
        "begin_materialization", "append_dispositions", "finish_materialization",
        "bind_release_visibility", "source_native_query", "disposition_query", "verify_open",
    ),
    "remediation_repository": ("record_decision", "decision_query"),
    "projection_publisher": ("apply_gap_ordered", "read_cursor", "rebuild_read_models"),
    "lifecycle_sink": ("project_events", "evidence_query"),
    "key_resolver": ("resolve_verification_key", "key_commitment", "mac"),
}

PORTS: dict[str, PortDeclaration] = {
    "SourceConnector": PortDeclaration("source_connector", {
        "submit_managed": PortMethod((("input", "ManagedPayloadInput"),), "DeliveryInput",
                                      ("capability_mismatch", "invalid_manifest", "integrity_error")),
        "verify_external": PortMethod((("input", "ExternalReceiptInput"),), "DeliveryInput",
                                       ("capability_mismatch", "invalid_signature", "integrity_error")),
    }),
    "RawStore": PortDeclaration("raw_store", {
        "get_receipt": PortMethod((("raw_receipt_digest", "Digest"),), "RawReceipt",
                                   ("not_found", "integrity_error")),
        "open_raw": PortMethod((("raw_receipt_digest", "Digest"),), "RawReadHandle",
                                ("not_found", "access_denied", "integrity_error")),
        "read_raw": PortMethod(
            (("handle", "RawReadHandle"), ("offset", "NonNegativeIntegerString"), ("max_bytes", "PositiveIntegerString")),
            "RawReadPage", ("not_found", "access_denied", "integrity_error"),
        ),
        "preserve": PortMethod((("input", "ManagedPayloadInput"),), "RawReceipt",
                                ("capacity_exceeded", "claim_conflict", "integrity_error")),
        "verify_open": PortMethod((("input", "ExternalReceiptInput"),), "RawReceipt",
                                   ("invalid_signature", "not_found", "integrity_error")),
    }),
    "ScratchStore": PortDeclaration("scratch_store", {
        "create_scope": PortMethod((("attempt_id", "Digest"), ("capacity_bytes", "PositiveIntegerString")),
                                    "ScratchScope", ("capacity_exceeded", "scope_conflict")),
        "write_sequential": PortMethod((("attempt_id", "Digest"), ("chunk", "ScratchChunk")), "UnitResult",
                                        ("capacity_exceeded", "scope_closed", "scope_owner_mismatch", "sequence_conflict")),
        "read_sequential": PortMethod(
            (("attempt_id", "Digest"), ("scope_id", "Token"), ("after_sequence", "NonNegativeIntegerString"),
             ("max_bytes", "PositiveIntegerString")),
            "ScratchReadPage", ("item_too_large", "not_found", "scope_open", "scope_owner_mismatch"),
        ),
        "close_scope": PortMethod((("attempt_id", "Digest"), ("scope_id", "Token")), "UnitResult",
                                   ("not_found", "scope_owner_mismatch")),
        "delete_scope": PortMethod((("attempt_id", "Digest"), ("scope_id", "Token")), "UnitResult",
                                    ("scope_owner_mismatch",)),
        "cleanup_orphans": PortMethod((("active_attempt_ids", "list<Digest>"), ("max_scopes", "PositiveInteger")),
                                       "list<Token>", ("integrity_error",)),
    }),
    "DeliveryStateStore": PortDeclaration("state_store", {
        "contract_lifecycle": PortMethod((("request", "ContractLifecycleRequest"),), "ContractLifecycleTransitionResult",
                                          ("stale_revision", "contract_conflict", "migration_conflict", "inflight_attempt")),
        "deployment_lifecycle": PortMethod((("request", "DeploymentLifecycleRequest"),),
                                            "DeploymentLifecycleTransitionResult",
                                            ("stale_revision", "capability_mismatch", "inflight_attempt",
                                             "unsupported_secondary_target", "superseded_deployment")),
        "attempts": PortMethod((("query", "AttemptQuery"),), "AttemptPage", ("not_found",)),
        "state_transaction": PortMethod((("transaction", "StateOutboxTransaction"),), "StreamState",
                                         ("capacity_exceeded", "stale_revision", "intent_conflict",
                                          "event_conflict", "integrity_error")),
        "lease_outbox": PortMethod(
            (("logical_identity", "LogicalIdentity"), ("entry_kind", "OutboxEntryKind"), ("lease_owner", "Token"),
             ("observed_at", "UtcInstant"), ("max_items", "PositiveInteger")),
            "list<OutboxEntry>", ("concurrency_conflict",),
        ),
        "load_outbox_payload": PortMethod((("outbox_id", "Digest"), ("payload_digest", "Digest")), "OutboxPayload",
                                           ("not_found", "integrity_error")),
        "fail_outbox": PortMethod((("transaction", "OutboxFailureTransaction"),), "StreamState",
                                   ("stale_revision", "concurrency_conflict", "integrity_error")),
        "projection_log": PortMethod(
            (("logical_identity", "LogicalIdentity"), ("after_revision", "NonNegativeIntegerString"),
             ("max_items", "PositiveInteger"), ("max_bytes", "PositiveIntegerString")),
            "ProjectionLogPage", ("item_too_large", "not_found"),
        ),
        "projection_confirmation_log": PortMethod(
            (("logical_identity", "LogicalIdentity"), ("after_revision", "NonNegativeIntegerString"),
             ("max_items", "PositiveInteger"), ("max_bytes", "PositiveIntegerString")),
            "ProjectionConfirmationLogPage", ("item_too_large", "not_found"),
        ),
        "lifecycle_event_log": PortMethod((("query", "LifecycleEventLogQuery"),), "LifecycleEventLogPage",
                                           ("item_too_large", "not_found")),
        "status_query": PortMethod((("logical_identity", "LogicalIdentity"),), "OperationalStatus", ("not_found",)),
        "begin_snapshot_keyset": PortMethod((("request", "SnapshotKeysetRequest"),), "SnapshotKeyset",
                                             ("key_commitment_conflict", "integrity_error")),
        "append_snapshot_keyset": PortMethod((("attempt_id", "Digest"), ("page", "RecordKeyTagPage")), "SnapshotKeyset",
                                              ("sequence_conflict", "key_commitment_conflict", "integrity_error")),
        "complete_snapshot_keyset": PortMethod((("completion", "SnapshotKeysetCompletion"),), "SnapshotKeyset",
                                                ("integrity_error", "key_commitment_conflict")),
        "get_snapshot_keyset": PortMethod(
            (("logical_identity", "LogicalIdentity"), ("visibility", "VisibilityIdentity")),
            "SnapshotKeyset", ("not_found", "key_commitment_conflict"),
        ),
        "reconcile_snapshot": PortMethod((("request", "SnapshotReconciliationRequest"),), "SnapshotReconciliationResult",
                                          ("integrity_error", "key_commitment_conflict", "concurrency_conflict")),
        "begin_tombstone_keyset": PortMethod((("request", "TombstoneKeysetRequest"),), "TombstoneKeyset",
                                              ("key_commitment_conflict", "integrity_error")),
        "append_tombstone_keyset": PortMethod((("attempt_id", "Digest"), ("page", "TombstoneTagPage")), "TombstoneKeyset",
                                               ("sequence_conflict", "key_commitment_conflict", "integrity_error")),
        "complete_tombstone_keyset": PortMethod((("completion", "TombstoneKeysetCompletion"),), "TombstoneKeyset",
                                                 ("integrity_error", "key_commitment_conflict")),
        "finalize_tombstone_evidence": PortMethod((("request", "TombstoneEvidenceRequest"),), "DeletionEvidenceIntent",
                                                    ("integrity_error", "key_commitment_conflict", "concurrency_conflict")),
    }),
    "LandingAdapter": PortDeclaration("landing_adapter", {
        "begin_prepare": PortMethod(
            (("attempt_id", "Digest"), ("receipt", "RawReceipt"), ("raw", "RawReadHandle"),
             ("contract", "BronzeProductContract"), ("visibility", "VisibilityIdentity")),
            "LandingPreparation", ("codec_error", "framing_error", "integrity_error"),
        ),
        "append_raw": PortMethod((("preparation", "LandingPreparation"), ("page", "RawReadPage")), "LandingPreparation",
                                  ("sequence_conflict", "codec_error", "framing_error", "integrity_error")),
        "finish_prepare": PortMethod((("preparation", "LandingPreparation"),), "BronzeEvidence",
                                      ("codec_error", "framing_error", "integrity_error")),
        "read_candidate": PortMethod((("query", "CandidateReadQuery"),), "CandidateFramePage",
                                      ("item_too_large", "not_found", "access_denied", "integrity_error")),
        "begin_materialization": PortMethod(
            (("attempt_id", "Digest"), ("evidence", "BronzeEvidence"), ("evaluation_id", "Digest"),
             ("ruleset_digest", "Digest")),
            "MaterializationSession", ("evidence_conflict", "capacity_exceeded"),
        ),
        "append_dispositions": PortMethod((("session", "MaterializationSession"), ("page", "DispositionPage")),
                                           "MaterializationSession",
                                           ("sequence_conflict", "evidence_conflict", "capacity_exceeded")),
        "finish_materialization": PortMethod((("completion", "MaterializationCompletion"),), "MaterializedBronzeEvidence",
                                              ("evidence_conflict", "capacity_exceeded", "integrity_error")),
        "bind_release_visibility": PortMethod((("binding", "ReleaseVisibilityBinding"),), "MaterializedBronzeEvidence",
                                               ("evidence_conflict", "row_attribution_error", "integrity_error")),
        "source_native_query": PortMethod((("query", "SourceNativeQuery"),), "SourceNativePage",
                                           ("access_denied", "item_too_large", "not_found")),
        "disposition_query": PortMethod((("query", "DispositionQuery"),), "DispositionQueryPage",
                                         ("access_denied", "item_too_large", "not_found")),
        "verify_open": PortMethod((("input", "ExternalReceiptInput"), ("visibility", "DeliveryVisibilityIdentity")),
                                   "BronzeEvidence", ("integrity_error", "row_attribution_error")),
    }),
    "RemediationRepository": PortDeclaration("remediation_repository", {
        "record_decision": PortMethod((("decision", "RemediationDecision"),), "RemediationDecision",
                                       ("capacity_exceeded", "decision_conflict", "release_conflict",
                                        "ancestry_mismatch", "integrity_error")),
        "decision_query": PortMethod((("query", "RemediationDecisionQuery"),), "RemediationDecisionPage",
                                      ("access_denied", "item_too_large", "not_found")),
    }),
    "ProjectionPublisher": PortDeclaration("projection_publisher", {
        "apply_gap_ordered": PortMethod((("intent", "ProjectionIntent"),), "ProjectionConfirmation",
                                         ("projection_gap", "projection_conflict", "target_unavailable")),
        "read_cursor": PortMethod((("logical_identity", "LogicalIdentity"), ("projection_target", "Token")),
                                   "ProjectionCursor", ("not_found",)),
        "rebuild_read_models": PortMethod((("batch", "ProjectionReplayBatch"),), "ProjectionCursor",
                                           ("capacity_exceeded", "item_too_large", "unconfirmed_revision",
                                            "projection_conflict", "target_unavailable")),
    }),
    "LifecycleSink": PortDeclaration("lifecycle_sink", {
        "project_events": PortMethod((("batch", "LifecycleEventBatch"),), "list<Digest>",
                                      ("capacity_exceeded", "item_too_large", "event_conflict", "target_unavailable")),
        "evidence_query": PortMethod((("query", "EvidenceQuery"),), "EvidencePage",
                                      ("access_denied", "item_too_large", "not_found")),
    }),
    "KeyResolver": PortDeclaration("key_resolver", {
        "resolve_verification_key": PortMethod((("key_id", "Token"),), "VerificationKeyRecord",
                                                 ("key_not_found", "key_revoked", "policy_not_authorized")),
        "key_commitment": PortMethod((("key_id", "Token"),), "KeyCommitmentRecord",
                                      ("key_not_found", "key_commitment_conflict")),
        "mac": PortMethod((("key_id", "Token"), ("domain", "String"), ("message_base64url", "Base64Url")), "MacResult",
                           ("key_not_found", "key_revoked", "policy_not_authorized")),
    }),
}


# --------------------------------------------------------------------------- generation (dev/test only)
#
# Every function below takes an explicit ``idl_path``; none is called at import time.
# ``tests/python/test_bronze_schema.py`` calls these from a repository checkout to
# regenerate ``ergasterion/schemas/bronze-product-v1.schema.json`` and
# ``ergasterion/schemas/bronze-portable-idl-equivalence.json`` and assert byte-identity
# against the committed files -- the "regenerated ... byte-identical, checks itself" gate.

def load_idl(idl_path: str | Path) -> dict:
    """Read the frozen IDL after verifying its pinned Git-blob-byte SHA-256."""

    raw = Path(idl_path).read_bytes()
    got = hashlib.sha256(raw).hexdigest()
    if got != EXPECTED_IDL_SHA256:
        raise ValueError(
            f"IDL at {idl_path} hashes to {got}, expected {EXPECTED_IDL_SHA256}: "
            "the frozen structural authority has changed; validation cannot proceed"
        )
    return json.loads(raw)


def generate_schema_bundle(idl_path: str | Path, vectors_path: str | Path | None = None) -> dict:
    """One JSON Schema bundle: every record's Pydantic-derived schema, keyed by its
    exact IDL record name, sharing one ``$defs`` section (``pydantic.json_schema.
    models_json_schema``) so a type referenced by many records (``LogicalIdentity``,
    ``Digest``-shaped scalars, ...) is defined once, not duplicated per top-level
    record. When ``vectors_path`` is given (``tests/fixtures/bronze_schema_vectors.json``
    at generation time), each record that has at least one positive vector carries an
    ``examples`` array of those vectors' payloads -- the packaged schema then ships
    worked examples alongside the shapes, not shapes alone."""

    from pydantic.json_schema import models_json_schema

    idl = load_idl(idl_path)
    names_in_order = sorted(idl["records"])
    models_in_order = [ALL_RECORD_MODELS[n] for n in names_in_order if n in ALL_RECORD_MODELS]
    inputs = [(m, "validation") for m in models_in_order]
    name_map, shared = models_json_schema(inputs, by_alias=True, ref_template="#/$defs/{model}")
    ref_by_model = {model: ref["$ref"] for (model, _mode), ref in name_map.items()}

    examples_by_record: dict[str, list] = {}
    if vectors_path is not None:
        vectors = json.loads(Path(vectors_path).read_text(encoding="utf-8"))
        for vector in vectors["positive"]:
            examples_by_record.setdefault(vector["record"], []).append(vector["payload"])

    records: dict[str, dict] = {}
    for name in names_in_order:
        model = ALL_RECORD_MODELS.get(name)
        if model is None:
            continue
        entry: dict = {"$ref": ref_by_model[model]}
        if name in examples_by_record:
            entry["examples"] = examples_by_record[name]
        records[name] = entry

    return {
        "schema": "ergasterion.bronze-product-schema-bundle/v1",
        "idl_schema": idl["schema"],
        "idl_version": idl["idl_version"],
        "idl_sha256": EXPECTED_IDL_SHA256,
        "records": records,
        "$defs": shared.get("$defs", {}),
    }


def _required_field_names(idl_record: dict) -> set[str]:
    return {f["name"] for f in idl_record["fields"] if f.get("required", True)}


def _nullable_field_names(idl_record: dict) -> set[str]:
    return {f["name"] for f in idl_record["fields"] if f.get("nullable", False)}


def _model_nullable_field_names(model: type[BaseModel]) -> set[str]:
    """Every field this model actually accepts JSON ``null`` for at validation time.

    A field whose resolved annotation admits ``None`` (``typing.get_type_hints`` with
    ``include_extras=False`` strips ``Annotated`` metadata down to the bare
    ``X | None`` shape a plain ``NoneType`` membership check can read) is nullable --
    UNLESS the class also lists it in ``_omittable_not_nullable``. That classvar is the
    ``ClosedModel`` mechanism representing ``required: false, nullable: false``: the
    field is typed ``X | None = None`` so it can be omitted, but ``ClosedModel``'s
    before-validator actively rejects an explicit null for it, so it is not nullable in
    the behavioural sense this check verifies."""

    import typing as _typing

    hints = _typing.get_type_hints(model, include_extras=False)
    omittable_not_nullable = getattr(model, "_omittable_not_nullable", frozenset())
    nullable: set[str] = set()
    for field_name in model.model_fields:
        if field_name in omittable_not_nullable:
            continue
        hint = hints.get(field_name)
        if hint is None:
            continue
        args = _typing.get_args(hint)
        if type(None) in args or hint is type(None):
            wire_name = "schema" if field_name == "schema_" else field_name
            nullable.add(wire_name)
    return nullable


_SCALAR_BASE_TYPE_BY_NAME: dict[str, object] = {"string": str, "integer": int, "boolean": bool}


def _scalar_base_type(alias: object) -> object:
    """Unwrap ``Annotated[X, ...]`` down to ``X``; a bare alias (``str``, ``bool``, or
    ``ErrorCode``'s ``Literal[...]``) is returned unchanged."""

    if typing.get_origin(alias) is Annotated:
        return typing.get_args(alias)[0]
    return alias


def _scalar_checks(idl: dict) -> dict[str, dict]:
    """Check every IDL ``scalars`` entry against the actual constraint bound to the
    same-named alias in ``SCALAR_ALIAS_BY_IDL_NAME``: base type always; pattern via
    ``bronze_contract.SCALAR_PATTERNS`` when the IDL entry carries one; numeric or
    string-length bounds via ``bronze_contract.SCALAR_BOUNDS`` when the IDL entry
    carries those; and, for ``ErrorCode``'s ``enum_ref``, that the alias is a
    ``Literal`` over exactly the IDL's own ``error_codes`` set. Presence of a
    same-named alias alone is not checked as sufficient anywhere in this function --
    every constraint the IDL states is compared against the value actually enforced."""

    checks: dict[str, dict] = {}
    for name, entry in sorted(idl["scalars"].items()):
        alias = SCALAR_ALIAS_BY_IDL_NAME.get(name)
        if alias is None:
            checks[name] = {"status": "missing_alias"}
            continue

        if "enum_ref" in entry:
            ref_ok = entry["enum_ref"] == "error_codes"
            literal_ok = (
                typing.get_origin(alias) is Literal
                and set(typing.get_args(alias)) == set(idl["error_codes"])
            )
            ok = ref_ok and literal_ok
            checks[name] = {"status": "ok" if ok else "mismatch", "enum_ref_ok": ok}
            continue

        base = _scalar_base_type(alias)
        expected_base = _SCALAR_BASE_TYPE_BY_NAME.get(entry["base"])
        base_ok = base is expected_base
        detail: dict = {"base_ok": base_ok}
        ok = base_ok

        if "pattern" in entry:
            pattern_ok = bronze_contract.SCALAR_PATTERNS.get(name) == entry["pattern"]
            detail["pattern_ok"] = pattern_ok
            ok = ok and pattern_ok

        if "minimum" in entry or "maximum" in entry:
            bounds = bronze_contract.SCALAR_BOUNDS.get(name, {})
            bounds_ok = (
                bounds.get("minimum") == entry.get("minimum")
                and bounds.get("maximum") == entry.get("maximum")
            )
            detail["bounds_ok"] = bounds_ok
            ok = ok and bounds_ok

        if "min_length" in entry or "max_length" in entry:
            bounds = bronze_contract.SCALAR_BOUNDS.get(name, {})
            length_ok = (
                bounds.get("min_length") == entry.get("min_length")
                and bounds.get("max_length") == entry.get("max_length")
            )
            detail["length_ok"] = length_ok
            ok = ok and length_ok

        checks[name] = {"status": "ok" if ok else "mismatch", **detail}
    return checks


def _port_operation_order_checks(idl: dict) -> dict[str, dict]:
    """Check every IDL ``port_operation_order`` entry against ``PORT_OPERATION_ORDER``
    element by element and in order -- a reordered or truncated transcription fails
    even though it would pass a same-set comparison."""

    checks: dict[str, dict] = {}
    for name, expected_order in sorted(idl["port_operation_order"].items()):
        actual_order = PORT_OPERATION_ORDER.get(name)
        ok = actual_order is not None and tuple(expected_order) == actual_order
        checks[name] = {"status": "ok" if ok else "mismatch", "operation_count": len(expected_order)}
    return checks


def _handoff_schema_binding_checks(idl: dict) -> dict[str, dict]:
    """Check every IDL ``handoff_schema_bindings`` entry against
    ``bronze_contract.HANDOFF_SCHEMA_BINDINGS``: the schema id resolves to a
    ``HandoffSchemaId`` member, the record type name resolves to a
    ``HandoffRecordType`` member, the record type name resolves to a real record model
    in ``ALL_RECORD_MODELS``, and the Python pairing for that schema id equals the IDL
    pairing exactly."""

    checks: dict[str, dict] = {}
    for schema_id_value, record_type_name in sorted(idl["handoff_schema_bindings"].items()):
        try:
            schema_id = bronze_contract.HandoffSchemaId(schema_id_value)
            schema_id_ok = True
        except ValueError:
            schema_id = None
            schema_id_ok = False

        try:
            record_type = bronze_contract.HandoffRecordType(record_type_name)
            record_type_ok = True
        except ValueError:
            record_type = None
            record_type_ok = False

        model_ok = record_type_name in ALL_RECORD_MODELS

        pairing_ok = (
            schema_id_ok
            and record_type_ok
            and bronze_contract.HANDOFF_SCHEMA_BINDINGS.get(schema_id) == record_type
        )

        ok = schema_id_ok and record_type_ok and model_ok and pairing_ok
        checks[schema_id_value] = {
            "status": "ok" if ok else "mismatch",
            "record_type": record_type_name,
            "schema_id_ok": schema_id_ok,
            "record_type_ok": record_type_ok,
            "model_ok": model_ok,
            "pairing_ok": pairing_ok,
        }
    return checks


def generate_equivalence_report(idl_path: str | Path) -> dict:
    """Walk every IDL record, enum, union, port, error code, scalar and
    port-operation-order entry and check it against this module family's registries:
    for records, the same field-name set, required-field set, nullable-field set AND,
    field by field, the same resolved type (scalar, enum, record or union identity,
    recursed through ``list<T>``/``map<Token,T>``) -- a field present with the right
    name and requiredness but the wrong type fails this check. For enums, the same
    member set. For unions, the same variant set, resolved structurally from the actual
    Python union object (a union whose Python projection is missing or has an extra
    variant fails). For ports, the same method set, request field name/type list,
    response type and error-code set per method -- not merely the same method names.
    For scalars, the same base type and, where the IDL states one, the same pattern or
    numeric/length bound -- not merely that a same-named alias exists. For
    port_operation_order, the same operation sequence per port, in order. For
    ``handoff_schema_bindings``, per IDL entry: the schema id resolves to a
    ``HandoffSchemaId`` member, the record type name resolves to a
    ``HandoffRecordType`` member and to a real record model in ``ALL_RECORD_MODELS``,
    and the Python pairing in ``bronze_contract.HANDOFF_SCHEMA_BINDINGS`` equals the
    IDL pairing. One verdict per IDL surface plus a summary total.

    This covers the IDL's structural sections -- the ones that state a shape a wire
    payload or a port call must match (``records``, ``enums``, ``unions``, ``ports``,
    ``error_codes``, ``scalars``, ``port_operation_order``, ``handoff_schema_bindings``)
    -- exhaustively, not a sample of them. It does not check the IDL's grammar/metadata
    sections (``schema``, ``idl_version``, ``closed``, ``purpose``,
    ``type_expression_grammar``, ``canonicalization``, ``mac_framing``,
    ``golden_vectors``), which state conventions and worked examples rather than a
    shape this module family projects."""

    idl = load_idl(idl_path)
    record_checks: dict[str, dict] = {}
    for name, idl_record in sorted(idl["records"].items()):
        model = ALL_RECORD_MODELS.get(name)
        if model is None:
            record_checks[name] = {"status": "missing_model"}
            continue
        model_fields = set(model.model_fields)
        # ``schema_``/``schema``: the alias trick means the Python attribute is
        # ``schema_`` while the wire field is ``schema``; normalise before comparing.
        wire_fields = {("schema" if f == "schema_" else f) for f in model_fields}
        idl_fields = {f["name"] for f in idl_record["fields"]}
        idl_required = _required_field_names(idl_record)
        idl_nullable = _nullable_field_names(idl_record)
        model_required = {
            ("schema" if name_ == "schema_" else name_)
            for name_, info in model.model_fields.items()
            if info.is_required()
        }
        model_nullable = _model_nullable_field_names(model)
        fields_ok = wire_fields == idl_fields
        required_ok = model_required == idl_required
        nullable_ok = model_nullable == idl_nullable

        type_mismatches: list[dict] = []
        if fields_ok:
            hints = typing.get_type_hints(model, include_extras=True)
            for field in idl_record["fields"]:
                wire_name = field["name"]
                py_name = "schema_" if wire_name == "schema" else wire_name
                matches, reason = _idl_type_matches(field["type"], hints[py_name], field)
                if not matches:
                    type_mismatches.append({"field": wire_name, "reason": reason})
        types_ok = fields_ok and not type_mismatches

        ok = fields_ok and required_ok and nullable_ok and types_ok
        record_checks[name] = {
            "status": "ok" if ok else "mismatch",
            "field_count": len(idl_fields),
            "fields_ok": fields_ok,
            "missing_in_model": sorted(idl_fields - wire_fields),
            "extra_in_model": sorted(wire_fields - idl_fields),
            "required_ok": required_ok,
            "required_count": len(idl_required),
            "required_mismatch": sorted(idl_required ^ model_required) if not required_ok else [],
            "nullable_ok": nullable_ok,
            "nullable_count": len(idl_nullable),
            "nullable_mismatch": sorted(idl_nullable ^ model_nullable) if not nullable_ok else [],
            "types_ok": types_ok,
            "type_mismatches": type_mismatches,
        }

    enum_checks: dict[str, dict] = {}
    for name, members in sorted(idl["enums"].items()):
        enum_cls = ALL_ENUM_MODELS.get(name)
        if enum_cls is None:
            enum_checks[name] = {"status": "missing_model"}
            continue
        model_values = {m.value for m in enum_cls}
        idl_values = set(members)
        enum_checks[name] = {
            "status": "ok" if model_values == idl_values else "member_set_mismatch",
            "member_count": len(idl_values),
        }

    union_checks: dict[str, dict] = {}
    for name, idl_union in sorted(idl["unions"].items()):
        union_obj = ALL_UNION_MODELS.get(name)
        if union_obj is None:
            union_checks[name] = {"status": "missing_model"}
            continue
        actual_variants = _union_variant_names(union_obj)
        if "variants" in idl_union:
            expected_variants = set(idl_union["variants"])
        else:
            expected_variants = {idl_union["token_enum"], *idl_union["object_variants"]}
        variants_ok = actual_variants == expected_variants
        union_checks[name] = {
            "status": "ok" if variants_ok else "variant_set_mismatch",
            "variant_count": len(expected_variants),
            "missing_in_model": sorted(expected_variants - actual_variants),
            "extra_in_model": sorted(actual_variants - expected_variants),
        }

    port_checks: dict[str, dict] = {}
    for name, idl_port in sorted(idl["ports"].items()):
        decl = PORTS.get(name)
        if decl is None:
            port_checks[name] = {"status": "missing_port"}
            continue
        kind_ok = decl.kind == idl_port["kind"]
        idl_methods = idl_port["methods"]
        method_checks: dict[str, dict] = {}
        for method_name, idl_method in sorted(idl_methods.items()):
            model_method = decl.methods.get(method_name)
            if model_method is None:
                method_checks[method_name] = {"status": "missing_method"}
                continue
            idl_request = [(f["name"], f["type"]) for f in idl_method["request"]]
            model_request = list(model_method.request_fields)
            request_ok = idl_request == model_request
            response_ok = idl_method["response"] == model_method.response_type
            idl_errors = set(idl_method["errors"])
            model_errors = set(model_method.errors)
            errors_ok = idl_errors == model_errors
            method_checks[method_name] = {
                "status": "ok" if request_ok and response_ok and errors_ok else "mismatch",
                "request_ok": request_ok,
                "response_ok": response_ok,
                "errors_ok": errors_ok,
            }
        method_set_ok = set(idl_methods) == set(decl.methods)
        all_methods_ok = all(v.get("status") == "ok" for v in method_checks.values())
        port_checks[name] = {
            "status": "ok" if method_set_ok and kind_ok and all_methods_ok else "mismatch",
            "method_count": len(idl_methods),
            "methods": method_checks,
        }

    error_codes_ok = set(idl["error_codes"]) == set(bronze_contract.ERROR_CODES)
    scalar_checks = _scalar_checks(idl)
    port_operation_order_checks = _port_operation_order_checks(idl)
    handoff_schema_binding_checks = _handoff_schema_binding_checks(idl)

    total_records = len(idl["records"])
    ok_records = sum(1 for v in record_checks.values() if v["status"] == "ok")
    total_enums = len(idl["enums"])
    ok_enums = sum(1 for v in enum_checks.values() if v["status"] == "ok")
    total_unions = len(idl["unions"])
    ok_unions = sum(1 for v in union_checks.values() if v["status"] == "ok")
    total_ports = len(idl["ports"])
    ok_ports = sum(1 for v in port_checks.values() if v["status"] == "ok")
    total_scalars = len(idl["scalars"])
    ok_scalars = sum(1 for v in scalar_checks.values() if v["status"] == "ok")
    total_port_operation_order = len(idl["port_operation_order"])
    ok_port_operation_order = sum(1 for v in port_operation_order_checks.values() if v["status"] == "ok")
    total_handoff_schema_bindings = len(idl["handoff_schema_bindings"])
    ok_handoff_schema_bindings = sum(1 for v in handoff_schema_binding_checks.values() if v["status"] == "ok")

    return {
        "schema": "ergasterion.bronze-portable-idl-equivalence/v1",
        "idl_schema": idl["schema"],
        "idl_version": idl["idl_version"],
        "idl_sha256": EXPECTED_IDL_SHA256,
        "records": record_checks,
        "enums": enum_checks,
        "unions": union_checks,
        "ports": port_checks,
        "scalars": scalar_checks,
        "port_operation_order": port_operation_order_checks,
        "handoff_schema_bindings": handoff_schema_binding_checks,
        "error_codes": {"status": "ok" if error_codes_ok else "mismatch", "count": len(idl["error_codes"])},
        "summary": {
            "records": {"total": total_records, "ok": ok_records},
            "enums": {"total": total_enums, "ok": ok_enums},
            "unions": {"total": total_unions, "ok": ok_unions},
            "ports": {"total": total_ports, "ok": ok_ports},
            "scalars": {"total": total_scalars, "ok": ok_scalars},
            "port_operation_order": {"total": total_port_operation_order, "ok": ok_port_operation_order},
            "handoff_schema_bindings": {
                "total": total_handoff_schema_bindings,
                "ok": ok_handoff_schema_bindings,
            },
        },
    }
