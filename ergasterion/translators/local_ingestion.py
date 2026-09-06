"""The local-ingestion translator: the landing route's runtime-execution half.

``ExecutionPlan + RuntimeBinding`` produces a deterministic ``RuntimeManifest``.
Execution is a separate operator step. This module never imports DuckDB, SQLite
or an orchestrator package; it only validates the closed binding against the
local adapter capability documents and emits the manifest artefact.

The landing label's eight mandatory occurrences resolve to exactly two
owners (architecture sections 9, 10; owner ruling R1; plan decision D29):
this translator for Batch Ingestion, Data Validation, Data Publish and
Checkpoint & Retries, and the publication translator
(``ergasterion.translators.publication``) for Data Contracts, Schema
Publish, Metadata Capture and Lineage Capture. Which occurrence goes to
which owner is never hard-coded here: ``route_landing`` resolves it, every
time, from the estate's translator table through the real
``TranslationRouter``, exactly as any other product's occurrences are
routed. Neither translator claims occurrence ownership in the older,
occurrence-ownership sense (``owned_occurrences()`` is empty on both): every
product it serves is resolved through the table-driven router, which reads
only ``capabilities()`` and ``translate()``.
"""

from __future__ import annotations

import json

from ergasterion.estate import EstateContext, load_estate_adapters, load_translator_table
from ergasterion.framework.landing_contract import (
    BackupRestoreCapability,
    CapabilityCodecKind,
    ContentEncoding,
    DeliveryInputKind,
    DeliveryMode,
    LogicalIdentity,
    LogicalTypeKind,
    PortKind,
    ProfileClass,
    SecretBoundary,
    TranslationRole,
)
from ergasterion.framework.adapters import ADAPTER_NAMES
from ergasterion.framework.declaration import load_estate_policy
from ergasterion.framework.models import (
    Capability,
    ExecutionPlan,
    FrameworkError,
    PatternId,
    TranslationResult,
    compute_plan_digest,
)
from ergasterion.framework.routing import RoutingResult, TranslationRouter
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
from ergasterion.source_delivery import compute_derived_digest, load_typed_declarations
from ergasterion.translators.base import Translator
from ergasterion.translators.publication import PublicationTranslator
from ergasterion.translators.publication import TRANSLATOR_VERSION as PUBLICATION_TRANSLATOR_VERSION

from ergasterion.ingestion.settings import (
    LOCAL_ADAPTER_IDS,
    LOCAL_ENDPOINT_REFS,
    LOCAL_IMPLEMENTATION_VERSION,
    SYNTHETIC_PROTECTION_PROFILE,
)

LOCAL_TARGET_NAME = "local-ingestion"
LOCAL_TRANSLATOR_ID = "local-ingestion"
LOCAL_TRANSLATOR_VERSION = "1.0.0"
ENGINE_VERSION = "0.6.0"
VALIDATION_VERSION = "1.0.0"
CODEC_VERSION = "1.0.0"

# The four patterns the landing label's translator table names this
# translator for (estate.yml, owner ruling R1, plan decision D29): the
# runtime-execution patterns of the landing composition. The remaining four
# -- data_contracts, schema_publish, metadata_capture and lineage_capture --
# are the publication translator's capabilities. This is the translator's
# own declared capability set, not the table itself: which of these
# capabilities actually gets used for a given plan is always resolved
# through the table at routing time (see ``route_landing`` below).
LOCAL_INGESTION_PATTERNS: tuple[PatternId, ...] = (
    PatternId.BATCH_INGESTION,
    PatternId.DATA_VALIDATION,
    PatternId.DATA_PUBLISH,
    PatternId.CHECKPOINT_RETRIES,
)

# The shapes this translator renders. A landing product brings its source
# in unchanged, so the relations it publishes are the ones its composition
# produces: the ``declared`` shape and no other. The estate's table names
# an owner for a product's shape as well as for each of its patterns
# (architecture section 9), and the owner of the shape is the translator
# that materialises the relations; for a landing label that is this one,
# so the capability is declared here beside the four patterns.
LOCAL_INGESTION_SHAPES: tuple[str, ...] = ("declared",)

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


def declared_landing_label(identity: LogicalIdentity, estate: EstateContext) -> str:
    """The estate layer label the landing product at ``identity`` declares.

    Owner ruling R1: ownership is declared, never inferred. The label is read off
    the product's own declaration (``declarations/<source>.yml``, the table's
    ``layer`` key) and handed to the router as a lookup key. It is never derived
    backwards from the profile the plan resolved: a profile does not identify a
    label, and an estate that declares two labels admitting the landing profile --
    one owned by the ingestion runtime, one by the SQL translator -- is a correct
    estate, not an ambiguous one.

    Fails closed with a plain ``FrameworkError`` naming the table when the estate
    declares no such landing product, or when that product declares no label.
    """

    typed = load_typed_declarations(estate)
    table = typed.tables.get((identity.source, identity.table))
    if table is None:
        raise FrameworkError(
            f"{estate.declarations_dir}: no landing product declares "
            f"({identity.source!r}, {identity.table!r}); routing needs its declared layer label"
        )
    if table.layer is None:
        raise FrameworkError(
            f"landing product ({identity.source!r}, {identity.table!r}) declares no 'layer'; "
            "routing looks the label up in estate.yml's translator table (owner ruling R1)"
        )
    return table.layer


def route_landing(
    plan: ExecutionPlan,
    estate: EstateContext | None = None,
    *,
    identity: LogicalIdentity | None = None,
    label: str | None = None,
) -> RoutingResult:
    """Resolve every occurrence of a landing-profile ``plan`` to its sole
    owner through the estate's translator table (architecture sections 9,
    11; owner ruling R1; plan decision D29), using the real
    ``LocalIngestionTranslator`` and ``PublicationTranslator`` instances and
    the real ``TranslationRouter`` -- the same mechanism any other product's
    occurrences are routed through. ``estate`` defaults to the ambient
    ``EstateContext`` (the estate co-located with the running engine or
    resolved from ``DPF_ESTATE_ROOT``); a caller operating on a different
    estate (for example an operator command bound to ``--project-dir``)
    threads its own resolved ``EstateContext`` through instead.

    This is the one place the landing route's ownership split is decided.
    ``_routes`` below only reshapes the result into the RuntimeManifest's
    evidence shape; the ``plan`` operator command calls this function
    directly to prove the same routing succeeds before it compiles a
    manifest. Raises the matching ``RoutingError`` subclass (a
    ``FrameworkError``, itself a ``ValueError``) on any routing failure,
    naming the label, the pattern and the adapter.

    The label comes from the product, not from the plan: pass ``identity`` and
    the route reads the label that product declares (``declared_landing_label``),
    or pass ``label`` directly when the caller already holds it."""

    ctx = estate if estate is not None else EstateContext.default()
    if label is None:
        if identity is None:
            raise FrameworkError(
                "route_landing needs the landing product's identity or its declared label: "
                "the label is declared, never inferred from the plan's profile (owner ruling R1)"
            )
        label = declared_landing_label(identity, ctx)
    # The estate policy still loads, so a label no policy declares fails here rather
    # than as a silent miss in the table lookup below.
    policy = load_estate_policy(ctx.estate_file)
    if label not in policy.labels:
        raise FrameworkError(
            f"{ctx.estate_file}: the landing product declares layer {label!r}, which this "
            f"estate does not declare; declared labels are {sorted(policy.labels)!r}"
        )
    translator_table = load_translator_table(ctx.estate_file)
    adapters = load_estate_adapters(ctx.estate_file).names()
    router = TranslationRouter(plan, [LocalIngestionTranslator(), PublicationTranslator()])
    return router.route(label=label, translator_table=translator_table, adapters=adapters)


def _routes(
    plan: ExecutionPlan,
    estate: EstateContext | None = None,
    *,
    identity: LogicalIdentity | None = None,
    label: str | None = None,
) -> tuple[TranslatorAssignment, ...]:
    """The RuntimeManifest's per-occurrence evidence rows, derived from
    ``route_landing`` -- never from a fixed split. The table names exactly
    one owner per occurrence, so every row is ``TranslationRole.EXECUTION_OWNER``;
    there is no separate translator-observes-occurrence concept left once
    every pattern in the landing composition has a declared table owner."""

    result = route_landing(plan, estate, identity=identity, label=label)
    versions = {
        LOCAL_TARGET_NAME: LOCAL_TRANSLATOR_VERSION,
        PublicationTranslator().target_name: PUBLICATION_TRANSLATOR_VERSION,
    }
    owner_of: dict[str, str] = {}
    for assignment in result.assignments:
        owner_of.setdefault(assignment.occurrence_id, assignment.translator_name)
    return tuple(
        TranslatorAssignment(
            occurrence_id=occurrence_id,
            role=TranslationRole.EXECUTION_OWNER,
            translator_id=translator_name,
            translator_version=versions[translator_name],
        )
        for occurrence_id, translator_name in sorted(owner_of.items())
    )


def compile_runtime_manifest(
    plan: ExecutionPlan,
    binding: RuntimeBinding,
    *,
    estate: EstateContext | None = None,
    label: str | None = None,
) -> RuntimeManifest:
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
            f"does not match the resolved Landing graph {plan_digest}"
        )
    binding_digest = runtime_binding_digest(binding)
    # The label the router keys on is the one this product declares, read through
    # its own logical identity -- never inferred from the plan's profile.
    routes = _routes(plan, estate, identity=binding.logical_identity, label=label)
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


def default_projection_relations(schema_ref: str = "landing") -> ProjectionRelations:
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
    schema_ref: str = "landing",
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
        projection_target="landing",
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
    """Renders the landing route's runtime-execution occurrences and emits
    the deterministic runtime manifest. Never claims occurrence ownership in
    the occurrence-ownership routing sense (``owned_occurrences()`` is
    always empty, matching ``PublicationTranslator``): every product this
    translator serves is resolved through the table-driven router
    (``route_landing``), which reads only ``capabilities()`` and
    ``translate()``."""

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
        return frozenset()

    def observed_occurrences(self) -> frozenset[str]:
        return frozenset()

    def capabilities(self) -> frozenset[Capability]:
        return frozenset(
            Capability(token, self.target_name, adapter)
            for token in (
                *(pattern.value for pattern in LOCAL_INGESTION_PATTERNS),
                *LOCAL_INGESTION_SHAPES,
            )
            for adapter in ADAPTER_NAMES
        )

    def execution_order(self) -> tuple[str, ...]:
        return ()

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
