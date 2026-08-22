"""The local-ingestion translator: sole execution owner of the Bronze graph.

``ExecutionPlan + RuntimeBinding`` produces a deterministic ``RuntimeManifest``.
Execution is a separate operator step. This module never imports DuckDB, SQLite
or an orchestrator package; it only validates the closed binding against the
local adapter capability documents and emits the manifest artefact.
"""

from __future__ import annotations

import json

from ergasterion.framework.bronze_contract import (
    BackupRestoreCapability,
    CapabilityCodecKind,
    ContentEncoding,
    DeliveryInputKind,
    DeliveryMode,
    LogicalTypeKind,
    PortKind,
    ProfileClass,
    SecretBoundary,
    TranslationRole,
)
from ergasterion.framework.models import ExecutionPlan, TranslationResult, compute_plan_digest
from ergasterion.framework.runtime_binding import (
    AdapterCapabilities,
    CapabilityGuarantees,
    CapabilityLimits,
    OutboxBinding,
    PortBinding,
    ProjectionRelations,
    ProtectionCapabilities,
    RetentionBinding,
    RuntimeBinding,
    RuntimeManifest,
    RuntimePortBindings,
    RuntimeResources,
    SchedulerBinding,
    TranslatorAssignment,
)
from ergasterion.ingestion.records import PORT_OPERATION_ORDER
from ergasterion.ingestion.runtime import (
    PORT_FIELD_ORDER,
    PortError,
    admit_resources,
    canonical_digest,
    check_port_topology,
)
from ergasterion.source_delivery import compute_derived_digest
from ergasterion.translators.base import Translator

from ergasterion.ingestion.settings import (
    LOCAL_ADAPTER_IDS,
    LOCAL_ENDPOINT_REFS,
    LOCAL_IMPLEMENTATION_VERSION,
    SYNTHETIC_PROTECTION_PROFILE,
)

LOCAL_TARGET_NAME = "local-ingestion"
LOCAL_TRANSLATOR_ID = "local-ingestion"
LOCAL_TRANSLATOR_VERSION = "1.0.0"
DBT_TRANSLATOR_ID = "dbt"
DBT_TRANSLATOR_VERSION = "1.0.0"
ENGINE_VERSION = "0.4.1"
VALIDATION_VERSION = "1.0.0"
CODEC_VERSION = "1.0.0"

EXECUTION_ORDER: tuple[str, ...] = (
    "bronze.checkpoint",
    "bronze.ingest",
    "bronze.validate",
    "bronze.contract",
    "bronze.schema",
    "bronze.publish",
    "bronze.lineage",
    "bronze.metadata",
)

_OBSERVED_BY_DBT = (
    "bronze.contract",
    "bronze.schema",
    "bronze.publish",
    "bronze.lineage",
    "bronze.metadata",
)

_MEMORY = "268435456"
_SCRATCH = "134217728"
_LOCAL_GUARANTEES = CapabilityGuarantees(
    immutable_write=True,
    compare_and_swap=True,
    atomic_projection=True,
    gap_free_revision=True,
    idempotent_replay=True,
    bounded_streaming=True,
)
_LOCAL_PROTECTION = ProtectionCapabilities(
    profile_class=ProfileClass.SYNTHETIC_LOCAL_ONLY,
    encryption_at_rest=False,
    transport_encryption=False,
    access_policy_binding=False,
    audit_evidence=False,
    retention_enforcement=False,
    backup_restore=BackupRestoreCapability.OPERATOR_MANAGED,
    secret_boundary=SecretBoundary.OPAQUE_MAC,
)


def local_adapter_capabilities() -> dict[str, AdapterCapabilities]:
    """One closed capabilities document per local adapter, keyed by port slot."""

    full_inputs = (DeliveryInputKind.MANAGED_PAYLOAD, DeliveryInputKind.EXTERNAL_RECEIPT)
    full_modes = (DeliveryMode.CDC, DeliveryMode.APPEND_ONLY, DeliveryMode.COMPLETE_SNAPSHOT)
    full_codecs = (CapabilityCodecKind.CSV_V1, CapabilityCodecKind.JSONL_V1)
    full_encodings = (ContentEncoding.IDENTITY, ContentEncoding.GZIP)
    full_types = tuple(LogicalTypeKind)
    documents: dict[str, AdapterCapabilities] = {}
    for field_name in PORT_FIELD_ORDER:
        if field_name in {"source_connector", "raw_store", "landing_adapter"}:
            inputs, modes, codecs, encodings, types = (
                full_inputs, full_modes, full_codecs, full_encodings, full_types,
            )
        else:
            inputs = modes = codecs = encodings = types = ()
        scratch = _SCRATCH if field_name == "scratch_store" else "0"
        payload = "16777216" if field_name in {"source_connector", "raw_store", "landing_adapter"} else "0"
        documents[field_name] = AdapterCapabilities(
            schema="ergasterion.adapter-capabilities/v1",
            port_kind=PortKind(field_name),
            operations=PORT_OPERATION_ORDER[field_name],
            input_kinds=inputs,
            delivery_modes=modes,
            codecs=codecs,
            content_encodings=encodings,
            logical_types=types,
            guarantees=_LOCAL_GUARANTEES,
            limits=CapabilityLimits(
                max_payload_bytes=payload,
                max_uncompressed_bytes="67108864" if payload != "0" else "0",
                max_expansion_ratio="10" if payload != "0" else "0",
                max_batch_records="100000" if payload != "0" else "0",
                max_memory_bytes=_MEMORY,
                max_scratch_bytes=scratch,
            ),
            protection=_LOCAL_PROTECTION,
        )
    return documents


def capability_digest(document: AdapterCapabilities) -> str:
    return canonical_digest(document.model_dump(mode="json", by_alias=True))


def runtime_binding_digest(binding: RuntimeBinding) -> str:
    return canonical_digest({
        "schema": "ergasterion.runtime-binding/v1",
        "binding": binding.model_dump(mode="json", by_alias=True),
    })


def _routes(plan: ExecutionPlan) -> tuple[TranslatorAssignment, ...]:
    owned = tuple(
        TranslatorAssignment(
            occurrence_id=occurrence_id,
            role=TranslationRole.EXECUTION_OWNER,
            translator_id=LOCAL_TRANSLATOR_ID,
            translator_version=LOCAL_TRANSLATOR_VERSION,
        )
        for occurrence_id in EXECUTION_ORDER
    )
    observed = tuple(
        TranslatorAssignment(
            occurrence_id=occurrence_id,
            role=TranslationRole.OBSERVER,
            translator_id=DBT_TRANSLATOR_ID,
            translator_version=DBT_TRANSLATOR_VERSION,
        )
        for occurrence_id in _OBSERVED_BY_DBT
    )
    return tuple(sorted(owned + observed, key=lambda row: (row.occurrence_id, row.role.value, row.translator_id)))


def compile_runtime_manifest(plan: ExecutionPlan, binding: RuntimeBinding) -> RuntimeManifest:
    capabilities = local_adapter_capabilities()
    try:
        check_port_topology(binding, capabilities)
        admit_resources(binding, capabilities)
    except PortError as exc:
        raise ValueError(f"{exc.code}: {exc.detail}") from exc
    if binding.protection_profile != SYNTHETIC_PROTECTION_PROFILE:
        raise ValueError("production_policy_adapter_required: local translator admits synthetic-local only")
    if int(binding.runtime_resources.max_parallel_attempts) != 1:
        raise ValueError("invalid_config: the local profile fixes one parallel attempt")
    if binding.ports.scratch_store.adapter_id != LOCAL_ADAPTER_IDS["scratch_store"]:
        raise ValueError("invalid_config: bindings must include the local scratch port")
    plan_digest = compute_plan_digest(plan)
    if binding.execution_plan_digest != plan_digest:
        raise ValueError(
            f"digest_mismatch: binding execution_plan_digest {binding.execution_plan_digest} "
            f"does not match the resolved Bronze graph {plan_digest}"
        )
    binding_digest = runtime_binding_digest(binding)
    routes = _routes(plan)
    basis = {
        "schema": "ergasterion.runtime-manifest/v1",
        "logical_identity": binding.logical_identity.model_dump(mode="json", by_alias=True),
        "contract_digest": binding.contract_digest,
        "execution_plan_digest": binding.execution_plan_digest,
        "runtime_binding_digest": binding_digest,
        "binding": binding.model_dump(mode="json", by_alias=True),
        "engine_version": ENGINE_VERSION,
        "validation_version": VALIDATION_VERSION,
        "codec_version": CODEC_VERSION,
        "routes": [row.model_dump(mode="json", by_alias=True) for row in routes],
    }
    digest = compute_derived_digest("RuntimeManifest", basis)
    return RuntimeManifest(
        schema="ergasterion.runtime-manifest/v1",
        logical_identity=binding.logical_identity,
        contract_digest=binding.contract_digest,
        execution_plan_digest=binding.execution_plan_digest,
        runtime_binding_digest=binding_digest,
        binding=binding,
        engine_version=ENGINE_VERSION,
        validation_version=VALIDATION_VERSION,
        codec_version=CODEC_VERSION,
        routes=routes,
        runtime_manifest_digest=digest,
    )


DEFAULT_ENDPOINTS: dict[str, str] = dict(LOCAL_ENDPOINT_REFS)


def port_binding(field_name: str, endpoint_ref: str, capabilities: dict[str, AdapterCapabilities]) -> PortBinding:
    return PortBinding(
        adapter_id=LOCAL_ADAPTER_IDS[field_name],
        implementation_version=LOCAL_IMPLEMENTATION_VERSION,
        capability_digest=capability_digest(capabilities[field_name]),
        endpoint_ref=endpoint_ref,
        secret_resolver_refs=(),
    )


def default_projection_relations(schema_ref: str = "bronze") -> ProjectionRelations:
    relation_names = tuple(ProjectionRelations.model_fields)
    return ProjectionRelations(
        schema_ref=schema_ref,
        **{name: f"{schema_ref}.{name}" for name in relation_names if name not in ("database_ref", "schema_ref")},
    )


def build_local_binding(
    contract,
    *,
    execution_plan_digest: str,
    contract_digest: str,
    endpoints: dict[str, str] | None = None,
    schema_ref: str = "bronze",
    binding_id: str = "local-synthetic",
    binding_version: str = "1.0.0",
    environment: str = "local",
) -> RuntimeBinding:
    """The closed local profile: scratch port, one parallel attempt, non-secret refs."""

    capabilities = local_adapter_capabilities()
    tokens = dict(DEFAULT_ENDPOINTS)
    if endpoints:
        tokens.update(endpoints)
    retry = contract.delivery.retry
    return RuntimeBinding(
        schema="ergasterion.runtime-binding/v1",
        binding_id=binding_id,
        binding_version=binding_version,
        environment=environment,
        logical_identity=contract.logical_identity,
        contract_digest=contract_digest,
        execution_plan_digest=execution_plan_digest,
        projection_target="bronze",
        ports=RuntimePortBindings(
            **{name: port_binding(name, tokens[name], capabilities) for name in PORT_FIELD_ORDER}
        ),
        landing_ports={},
        projection_relations=default_projection_relations(schema_ref),
        scheduler=SchedulerBinding(
            heartbeat_seconds=60, heartbeat_slo_seconds=300, max_due_transitions_per_call=16,
        ),
        outbox=OutboxBinding(
            max_attempts=int(retry.max_attempts), lease_seconds=60, backoff=retry.backoff,
            base_seconds=int(retry.base_seconds), cap_seconds=int(retry.cap_seconds),
        ),
        runtime_resources=RuntimeResources(
            process_memory_bytes="268435456",
            validation_memory_bytes="67108864",
            scratch_reservation_bytes="33554432",
            max_parallel_attempts=1,
            max_wire_record_bytes="1048576",
            max_quarantine_disposition_bytes="262144",
            max_quarantine_decision_bytes="262144",
            max_remediation_locators=1000,
            max_visibility_ancestry_rows=1000,
        ),
        retention=RetentionBinding(orphan_content_hours=24, deletion_keyset_days=30),
        protection_profile=SYNTHETIC_PROTECTION_PROFILE,
    )


class LocalIngestionTranslator(Translator):
    """Owns every Bronze occurrence and emits the deterministic runtime manifest."""

    def __init__(
        self,
        *,
        binding: RuntimeBinding | None = None,
        plan_digest: str | None = None,
    ) -> None:
        self._binding = binding
        self._plan_digest = plan_digest

    @property
    def target_name(self) -> str:
        return LOCAL_TARGET_NAME

    def owned_occurrences(self) -> frozenset[str]:
        return frozenset(EXECUTION_ORDER)

    def observed_occurrences(self) -> frozenset[str]:
        return frozenset()

    def execution_order(self) -> tuple[str, ...]:
        return EXECUTION_ORDER

    def plan_digest(self) -> str | None:
        if self._plan_digest is not None:
            return self._plan_digest
        if self._binding is not None:
            return self._binding.execution_plan_digest
        return None

    def validate_compatibility(self, plan: ExecutionPlan) -> list[str]:
        issues: list[str] = []
        expected = compute_plan_digest(plan)
        pinned = self.plan_digest()
        if pinned is not None and pinned != expected:
            issues.append(
                f"translator was built against plan digest {pinned}, plan digest is {expected}"
            )
        if self._binding is not None:
            try:
                compile_runtime_manifest(plan, self._binding)
            except ValueError as exc:
                issues.append(str(exc))
        return issues

    def translate(self, plan: ExecutionPlan) -> TranslationResult:
        if self._binding is None:
            return TranslationResult(
                artefacts={},
                metadata={"target_name": self.target_name, "execution_plan_digest": compute_plan_digest(plan)},
                warnings=("no RuntimeBinding supplied; execution is a separate step",),
            )
        manifest = compile_runtime_manifest(plan, self._binding)
        payload = json.dumps(
            manifest.model_dump(mode="json", by_alias=True),
            sort_keys=True,
            separators=(",", ":"),
        )
        return TranslationResult(
            artefacts={"runtime-manifest.json": payload + "\n"},
            metadata={
                "target_name": self.target_name,
                "execution_plan_digest": manifest.execution_plan_digest,
                "runtime_manifest_digest": manifest.runtime_manifest_digest,
                "runtime_binding_digest": manifest.runtime_binding_digest,
            },
        )
