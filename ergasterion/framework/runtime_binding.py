"""Closed Pydantic projections of the frozen Bronze portable IDL, runtime-binding half:
port bindings, deployment capabilities, the runtime manifest and interface readiness.

Builds on ``ergasterion.framework.bronze_contract`` (the vocabulary and
contract-declaration half); ``ergasterion.ingestion.records`` builds on both this module
and that one. See ``bronze_contract``'s module docstring for the shared design notes
(structural-only scope, no file I/O at import time, IDL pin).
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from ergasterion.framework.bronze_contract import (
    BackoffKind,
    BackupRestoreCapability,
    CapabilityCodecKind,
    ClosedModel,
    ContentEncoding,
    DecimalString,
    DeliveryInputKind,
    DeliveryMode,
    DeploymentLifecycleAction,
    Digest,
    LogicalIdentity,
    LogicalTypeKind,
    NonNegativeIntegerString,
    OpaqueRef,
    PortKind,
    PositiveInteger,
    PositiveIntegerString,
    ProfileClass,
    ReadinessResult,
    SecretBoundary,
    SemVer,
    Token,
    TranslationRole,
    UtcInstant,
)

# --------------------------------------------------------------------------- port bindings

class PortBinding(ClosedModel):
    adapter_id: Token
    implementation_version: SemVer
    capability_digest: Digest
    endpoint_ref: OpaqueRef
    secret_resolver_refs: tuple[Token, ...]


class RuntimePortBindings(ClosedModel):
    """One binding per Bronze runtime port kind. Every field is a required
    ``PortBinding``: a deployment binds all nine ports, never a subset."""

    source_connector: PortBinding
    raw_store: PortBinding
    scratch_store: PortBinding
    state_store: PortBinding
    landing_adapter: PortBinding
    remediation_repository: PortBinding
    projection_publisher: PortBinding
    lifecycle_sink: PortBinding
    key_resolver: PortBinding


class RelationCoordinate(ClosedModel):
    database_ref: Token | None = None
    schema_ref: Token
    relation_ref: Token

    _omittable_not_nullable = frozenset({"database_ref"})


class ProjectionRelations(ClosedModel):
    database_ref: Token | None = None
    schema_ref: Token
    active_alias: Token
    stream_status: Token
    source_native_audit: Token
    quarantine: Token
    snapshot_history: Token
    published_ledger: Token
    visibility_ancestry: Token
    version_registry: Token
    contract_registry: Token
    source_schema_registry: Token
    published_schema_registry: Token
    quality: Token
    lineage: Token
    product_metadata: Token
    deletion_evidence: Token
    lifecycle_events: Token

    _omittable_not_nullable = frozenset({"database_ref"})


class SchedulerBinding(ClosedModel):
    heartbeat_seconds: PositiveInteger
    heartbeat_slo_seconds: PositiveInteger
    max_due_transitions_per_call: PositiveInteger


class OutboxBinding(ClosedModel):
    max_attempts: PositiveInteger
    lease_seconds: PositiveInteger
    backoff: BackoffKind
    base_seconds: PositiveInteger
    cap_seconds: PositiveInteger


class RuntimeResources(ClosedModel):
    process_memory_bytes: PositiveIntegerString
    validation_memory_bytes: PositiveIntegerString
    scratch_reservation_bytes: PositiveIntegerString
    max_parallel_attempts: PositiveInteger
    max_wire_record_bytes: PositiveIntegerString
    max_quarantine_disposition_bytes: PositiveIntegerString
    max_quarantine_decision_bytes: PositiveIntegerString
    max_remediation_locators: PositiveInteger
    max_visibility_ancestry_rows: PositiveInteger


class RetentionBinding(ClosedModel):
    orphan_content_hours: PositiveInteger
    deletion_keyset_days: PositiveInteger


class RuntimeBinding(ClosedModel):
    """One environment's complete deployment binding for one Bronze product: which
    adapters implement each port, which target relations the projection writes to, and
    the operating envelope (scheduler cadence, outbox retry, resource ceilings,
    retention). IDL schema token ``ergasterion.runtime-binding/v1``."""

    schema_: Literal["ergasterion.runtime-binding/v1"] = Field(alias="schema")
    binding_id: Token
    binding_version: SemVer
    environment: Token
    logical_identity: LogicalIdentity
    contract_digest: Digest
    execution_plan_digest: Digest
    projection_target: Token
    ports: RuntimePortBindings
    landing_ports: dict[str, RelationCoordinate]
    projection_relations: ProjectionRelations
    scheduler: SchedulerBinding
    outbox: OutboxBinding
    runtime_resources: RuntimeResources
    retention: RetentionBinding
    protection_profile: Token

    model_config = ConfigDict(extra="forbid", frozen=True, populate_by_name=True)


# --------------------------------------------------------------------------- capabilities

class CapabilityGuarantees(ClosedModel):
    immutable_write: bool
    compare_and_swap: bool
    atomic_projection: bool
    gap_free_revision: bool
    idempotent_replay: bool
    bounded_streaming: bool


class CapabilityLimits(ClosedModel):
    max_payload_bytes: NonNegativeIntegerString
    max_uncompressed_bytes: NonNegativeIntegerString
    max_expansion_ratio: DecimalString
    max_batch_records: NonNegativeIntegerString
    max_memory_bytes: NonNegativeIntegerString
    max_scratch_bytes: NonNegativeIntegerString


class ProtectionCapabilities(ClosedModel):
    profile_class: ProfileClass
    encryption_at_rest: bool
    transport_encryption: bool
    access_policy_binding: bool
    audit_evidence: bool
    retention_enforcement: bool
    backup_restore: BackupRestoreCapability
    secret_boundary: SecretBoundary


class AdapterCapabilities(ClosedModel):
    """The exact capability envelope an adapter/translator declares for one Bronze
    port. IDL schema token ``ergasterion.adapter-capabilities/v1``."""

    schema_: Literal["ergasterion.adapter-capabilities/v1"] = Field(alias="schema")
    port_kind: PortKind
    operations: tuple[Token, ...]
    input_kinds: tuple[DeliveryInputKind, ...]
    delivery_modes: tuple[DeliveryMode, ...]
    codecs: tuple[CapabilityCodecKind, ...]
    content_encodings: tuple[ContentEncoding, ...]
    logical_types: tuple[LogicalTypeKind, ...]
    guarantees: CapabilityGuarantees
    limits: CapabilityLimits
    protection: ProtectionCapabilities

    model_config = ConfigDict(extra="forbid", frozen=True, populate_by_name=True)


class TranslatorAssignment(ClosedModel):
    occurrence_id: Token
    role: TranslationRole
    translator_id: Token
    translator_version: SemVer


class RuntimeManifest(ClosedModel):
    """One resolved binding's execution routing table: which translator plays which
    role at every occurrence, plus the three digests (contract, execution plan, this
    binding) the manifest's own digest is derived over. IDL schema token
    ``ergasterion.runtime-manifest/v1``."""

    schema_: Literal["ergasterion.runtime-manifest/v1"] = Field(alias="schema")
    logical_identity: LogicalIdentity
    contract_digest: Digest
    execution_plan_digest: Digest
    runtime_binding_digest: Digest
    binding: RuntimeBinding
    engine_version: SemVer
    validation_version: SemVer
    codec_version: SemVer
    routes: tuple[TranslatorAssignment, ...]
    runtime_manifest_digest: Digest

    model_config = ConfigDict(extra="forbid", frozen=True, populate_by_name=True)


class InterfaceReadiness(ClosedModel):
    """The result of checking one deployment's declared adapters against the contract
    and manifest they claim to serve, before it may accept traffic. IDL schema token
    ``ergasterion.interface-readiness/v1``."""

    schema_: Literal["ergasterion.interface-readiness/v1"] = Field(alias="schema")
    logical_identity: LogicalIdentity
    projection_target: Token
    runtime_manifest_digest: Digest
    contract_digest: Digest
    source_schema_digest: Digest
    published_schema_digest: Digest
    version_interface_ref: OpaqueRef
    capability_digest: Digest
    classification: Token
    access_policy_ref: Token
    retention_policy_ref: Token
    protection_profile: Token
    result: ReadinessResult
    readiness_digest: Digest
    verified_at: UtcInstant
    revoked_at: UtcInstant | None

    model_config = ConfigDict(extra="forbid", frozen=True, populate_by_name=True)


class RuntimeDeployment(ClosedModel):
    logical_identity: LogicalIdentity
    contract_digest: Digest
    projection_target: Token
    candidate_manifest_digest: Digest | None
    active_manifest_digest: Digest | None
    retired_manifest_digests: tuple[Digest, ...]
    deployment_revision: NonNegativeIntegerString


class ProjectionCursor(ClosedModel):
    logical_identity: LogicalIdentity
    projection_target: Token
    projection_revision: NonNegativeIntegerString
    projection_intent_digest: Digest | None


class DeploymentLifecycleRequest(ClosedModel):
    """A caller's request to register or activate a runtime deployment. IDL schema
    token ``ergasterion.deployment-lifecycle-request/v1``."""

    schema_: Literal["ergasterion.deployment-lifecycle-request/v1"] = Field(alias="schema")
    action: DeploymentLifecycleAction
    expected_state_revision: NonNegativeIntegerString
    expected_deployment_revision: NonNegativeIntegerString
    deployment: RuntimeDeployment
    readiness: InterfaceReadiness
    catchup_cursor: ProjectionCursor
    permit_pre_intent_fence: bool

    model_config = ConfigDict(extra="forbid", frozen=True, populate_by_name=True)


# ``DeploymentLifecycleTransitionResult`` and ``ContractLifecycleTransitionResult`` both
# carry a ``StreamState`` field; ``StreamState`` belongs to the delivery/runtime-state
# record family and is defined in ``ergasterion.ingestion.records``, which imports this
# module -- so both transition-result records are defined there instead, keeping the
# ``bronze_contract`` -> ``runtime_binding`` -> ``ingestion.records`` chain one-way.


# --------------------------------------------------------------------------- handoff carrier that needs InterfaceReadiness

class InterfaceReadinessHandoff(ClosedModel):
    run_id: Digest
    attempt_id: Digest
    readiness: InterfaceReadiness


# --------------------------------------------------------------------------- registry

RECORD_MODELS: dict[str, type[BaseModel]] = {
    "PortBinding": PortBinding,
    "RuntimePortBindings": RuntimePortBindings,
    "RelationCoordinate": RelationCoordinate,
    "ProjectionRelations": ProjectionRelations,
    "SchedulerBinding": SchedulerBinding,
    "OutboxBinding": OutboxBinding,
    "RuntimeResources": RuntimeResources,
    "RetentionBinding": RetentionBinding,
    "RuntimeBinding": RuntimeBinding,
    "CapabilityGuarantees": CapabilityGuarantees,
    "CapabilityLimits": CapabilityLimits,
    "ProtectionCapabilities": ProtectionCapabilities,
    "AdapterCapabilities": AdapterCapabilities,
    "TranslatorAssignment": TranslatorAssignment,
    "RuntimeManifest": RuntimeManifest,
    "InterfaceReadiness": InterfaceReadiness,
    "RuntimeDeployment": RuntimeDeployment,
    "ProjectionCursor": ProjectionCursor,
    "DeploymentLifecycleRequest": DeploymentLifecycleRequest,
    "InterfaceReadinessHandoff": InterfaceReadinessHandoff,
}
