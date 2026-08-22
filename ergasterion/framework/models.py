"""Typed core model for Ergasterion's platform-neutral pattern/composition graph.

This module holds the typed, immutable, digest-bearing occurrence graph that
translators consume. Occurrences, edges, roles, handoff schemas, validation
results, and translation results share one platform-neutral vocabulary.

This module imports no dbt, DuckDB, SQLite or orchestrator package: it stays
platform-neutral. It never imports ``ergasterion.translators``: translators
depend on the framework, and the dependency flows one way.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from enum import Enum

import rfc8785


# --------------------------------------------------------------------------- vocabulary


class Layer(str, Enum):
    """The closed medallion vocabulary: Bronze, Silver, and Gold."""

    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"


class PatternId(str, Enum):
    """The closed, exact registry identity for all fifteen canonical patterns.

    Identity is separate from display text: see ``PATTERN_DISPLAY_NAMES`` in
    ``patterns.py`` for the human-readable name. Callers use these exact string
    values or the enum members; aliases and case-normalisation are never
    accepted.
    """

    BATCH_INGESTION = "batch_ingestion"
    DATA_VALIDATION = "data_validation"
    DATA_CONTRACTS = "data_contracts"
    LINEAGE_CAPTURE = "lineage_capture"
    METADATA_CAPTURE = "metadata_capture"
    SCHEMA_PUBLISH = "schema_publish"
    DATA_PUBLISH = "data_publish"
    CHECKPOINT_RETRIES = "checkpoint_retries"
    BATCH_TRANSFER = "batch_transfer"
    SCHEMA_TRANSFORM = "schema_transform"
    CALCULATED_FIELDS = "calculated_fields"
    DATA_ENRICHMENT = "data_enrichment"
    DATA_FILTERING = "data_filtering"
    DATA_AGGREGATION = "data_aggregation"
    DATA_CURATION = "data_curation"


class PatternDisposition(str, Enum):
    """A pattern's classification within one layer's composition."""

    MANDATORY = "mandatory"
    OPTIONAL = "optional"
    FORBIDDEN = "forbidden"


class Role(str, Enum):
    """The closed role-token vocabulary an occurrence's ``roles`` set draws from.
    Serialised role sets sort in this exact token order."""

    PHASE = "phase"
    WRAPPER = "wrapper"
    POLICY = "policy"
    OBSERVER = "observer"
    BARRIER = "barrier"


_ROLE_SORT_INDEX = {role: index for index, role in enumerate(Role)}


class EdgeRole(str, Enum):
    """The closed edge-role vocabulary for ordinary directed edges between occurrences."""

    DATA = "data"
    VALIDATION = "validation"
    READINESS = "readiness"
    BARRIER = "barrier"
    OBSERVE = "observe"


class HandoffSchemaId(str, Enum):
    """The closed set of target-neutral handoff schema identities an edge can carry.
    Full record shapes live in the frozen IDL
    ``docs/specifications/bronze-portable-idl-v1.json``; this registry names the
    identity only, for edge typing and conformance checks."""

    RAW_EVIDENCE = "ergasterion.raw-evidence/v1"
    VALIDATION_RESULT = "ergasterion.validation-result/v1"
    CONTRACT_CONFORMANCE = "ergasterion.contract-conformance/v1"
    INTERFACE_READINESS = "ergasterion.interface-readiness/v1"
    PUBLICATION_CONFIRMATION = "ergasterion.publication-confirmation/v1"


# --------------------------------------------------------------------------- errors


class FrameworkError(ValueError):
    """Base for every framework-layer error. Always loud, always carries a stable
    ``.code`` a caller can branch on without parsing message text."""

    code: str = "framework_error"


class InvalidLayerArgumentError(FrameworkError):
    """Raised when ``resolve()`` receives a value that is not a ``Layer``
    member. A plain string such as ``"bronze"`` equals ``Layer.BRONZE.value``
    but fails the identity check ``layer is Layer.BRONZE``: this error names
    the exact failure, with its own code, so a non-``Layer`` argument is
    rejected before any ``Layer``-specific error construction runs."""

    code = "invalid_layer_argument"

    def __init__(self, layer: object) -> None:
        self.layer = layer
        super().__init__(
            f"resolve() requires a Layer member, got {layer!r} ({type(layer).__name__}); "
            "a plain string value is rejected even when it matches a Layer token"
        )


class UnsupportedLayerError(FrameworkError):
    """Raised deterministically when a layer has no executable composition yet.
    Silver and Gold raise this in version 1: they carry no hidden historical
    composition, mandatory/optional/forbidden table, or occurrence graph."""

    code = "unsupported_layer"

    def __init__(self, layer: Layer) -> None:
        self.layer = layer
        super().__init__(
            f"layer {layer.value!r} is unsupported_layer: Bronze is the only layer "
            "that resolves an executable composition"
        )


# --------------------------------------------------------------------------- graph IR


@dataclass(frozen=True)
class Occurrence:
    """One typed, identified occurrence of a pattern within a layer's execution graph.

    ``occurrence_id`` is the stable identity used by edges, wrapper membership and
    translator ownership. It stays distinct from the bare ``pattern_id`` because a
    future layer's graph could in principle place the same pattern at more than
    one occurrence. ``roles`` is duplicate-free and stored pre-sorted in ``Role``
    token order.
    """

    occurrence_id: str
    pattern_id: PatternId
    roles: tuple[Role, ...]
    execution_owner_required: bool

    def __post_init__(self) -> None:
        if len(set(self.roles)) != len(self.roles):
            raise FrameworkError(f"occurrence {self.occurrence_id!r} declares duplicate roles: {self.roles}")
        sorted_roles = tuple(sorted(self.roles, key=lambda r: _ROLE_SORT_INDEX[r]))
        if sorted_roles != self.roles:
            raise FrameworkError(
                f"occurrence {self.occurrence_id!r} roles {self.roles} are not sorted in role-token order"
            )
        if not self.roles:
            raise FrameworkError(f"occurrence {self.occurrence_id!r} declares an empty roles set")


@dataclass(frozen=True)
class Edge:
    """One ordinary directed edge between two occurrences. Wrapper enclosure is a
    separate structural fact, carried on ``ExecutionPlan.wrapper_members``: the
    wrapper's enclosure cannot be bypassed by adding an ordinary data edge."""

    source: str
    target: str
    edge_role: EdgeRole
    handoff_schema_id: HandoffSchemaId


@dataclass(frozen=True)
class ExecutionPlan:
    """The resolved, immutable, serialisable execution graph for one layer.

    ``occurrences`` and ``edges`` are stored in the plan's canonical order
    (occurrences sorted by ``occurrence_id``; edges in the normative declaration
    order). ``wrapper_id`` names the enclosing wrapper occurrence and
    ``wrapper_members`` is the sorted, duplicate-free set of the remaining
    occurrence IDs it encloses.
    """

    layer: Layer
    occurrences: tuple[Occurrence, ...]
    edges: tuple[Edge, ...]
    wrapper_id: str
    wrapper_members: tuple[str, ...]

    def __post_init__(self) -> None:
        ids = [o.occurrence_id for o in self.occurrences]
        if len(set(ids)) != len(ids):
            raise FrameworkError(f"execution plan for layer {self.layer.value!r} has duplicate occurrence IDs: {ids}")
        if sorted(ids) != ids:
            raise FrameworkError(
                f"execution plan for layer {self.layer.value!r} occurrences are not sorted by occurrence_id"
            )
        if self.wrapper_id not in ids:
            raise FrameworkError(f"wrapper_id {self.wrapper_id!r} is not a declared occurrence")
        members = list(self.wrapper_members)
        if len(set(members)) != len(members) or sorted(members) != members:
            raise FrameworkError("wrapper_members must be a sorted, duplicate-free tuple")
        if self.wrapper_id in members:
            raise FrameworkError("the wrapper occurrence cannot be its own member")
        expected_members = sorted(i for i in ids if i != self.wrapper_id)
        if members != expected_members:
            raise FrameworkError(
                f"wrapper_members {members} must be exactly the sorted non-wrapper occurrence IDs {expected_members}"
            )
        known_ids = set(ids)
        for edge in self.edges:
            if edge.source not in known_ids or edge.target not in known_ids:
                raise FrameworkError(f"edge {edge.source!r} -> {edge.target!r} references an undeclared occurrence")

    def occurrence(self, occurrence_id: str) -> Occurrence:
        for occurrence in self.occurrences:
            if occurrence.occurrence_id == occurrence_id:
                return occurrence
        raise FrameworkError(f"no such occurrence: {occurrence_id!r}")

    def edges_into(self, occurrence_id: str) -> tuple[Edge, ...]:
        return tuple(e for e in self.edges if e.target == occurrence_id)

    def edges_out_of(self, occurrence_id: str) -> tuple[Edge, ...]:
        return tuple(e for e in self.edges if e.source == occurrence_id)


# --------------------------------------------------------------------------- digest


def _plan_digest_document(plan: ExecutionPlan) -> dict:
    return {
        "schema": "ergasterion.execution-graph-shape/v1",
        "layer": plan.layer.value,
        "occurrences": [
            {
                "occurrence_id": o.occurrence_id,
                "pattern_id": o.pattern_id.value,
                "roles": [r.value for r in o.roles],
                "execution_owner_required": o.execution_owner_required,
            }
            for o in plan.occurrences
        ],
        "edges": [
            {
                "source": e.source,
                "target": e.target,
                "edge_role": e.edge_role.value,
                "handoff_schema_id": e.handoff_schema_id.value,
            }
            for e in plan.edges
        ],
        "wrapper_id": plan.wrapper_id,
        "wrapper_members": list(plan.wrapper_members),
    }


def compute_plan_digest(plan: ExecutionPlan) -> str:
    """Lowercase SHA-256 hex of the RFC 8785 (JCS) canonical bytes of the plan's
    digest document. Deterministic and platform-neutral: the same plan digests
    identically regardless of Python dict-ordering or platform.

    The digest document is tagged with the schema identity
    ``ergasterion.execution-graph-shape/v1`` and covers this framework's typed
    occurrence/edge graph shape only: ``layer``, ``occurrences``, ``edges``,
    ``wrapper_id`` and ``wrapper_members``. This identity is distinct from the
    frozen IDL's ``ExecutionPlan.execution_plan_digest``
    (``docs/specifications/bronze-portable-idl-v1.json``), which covers a wider
    record carrying ``logical_identity``, ``product_version``,
    ``contract_digest``, ``source_schema_digest``, ``published_schema_digest``,
    ``occurrences``, ``edges`` and ``handoffs``. A translator's ``plan_digest()``
    pins against this graph-shape digest to detect a stale build against the
    framework's internal graph; it carries no claim about the IDL's
    ``execution_plan_digest``, which a later projection step computes from the
    wider record."""

    canonical = rfc8785.dumps(_plan_digest_document(plan))
    return hashlib.sha256(canonical).hexdigest()


# --------------------------------------------------------------------------- translation IR
#
# These shapes live here, on the framework side of the one-way dependency, so
# the router (routing.py) can compose them across translators without importing
# ergasterion.translators. translators/base.py imports them from this module.


@dataclass(frozen=True)
class TranslationResult:
    """One translator's output: generated artefacts plus metadata.

    ``artefacts`` maps filename to content (generated code, YAML, SQL).
    ``metadata`` is translator-specific. ``warnings`` are non-blocking issues
    encountered during translation.
    """

    artefacts: dict[str, str] = field(default_factory=dict)
    metadata: dict = field(default_factory=dict)
    warnings: tuple[str, ...] = field(default_factory=tuple)


class ValidationSeverity(str, Enum):
    """Severity levels for a translator's validation/drift findings."""

    PASS = "pass"
    WARN = "warn"
    FAIL = "fail"


@dataclass(frozen=True)
class ValidationFinding:
    """One finding from a translator's ``validate()`` or ``detect_drift()``."""

    severity: ValidationSeverity
    category: str
    message: str
    location: str = ""


@dataclass(frozen=True)
class TranslatorValidationResult:
    """The result of running contract and property-based tests on a translator's
    generated artefacts. Named distinctly from any declaration-side validation
    result to avoid a same-name collision across the framework's two validation
    concerns."""

    passed: bool
    findings: tuple[ValidationFinding, ...] = field(default_factory=tuple)

    @property
    def blockers(self) -> tuple[ValidationFinding, ...]:
        return tuple(f for f in self.findings if f.severity == ValidationSeverity.FAIL)


@dataclass(frozen=True)
class DriftReport:
    """The result of comparing a translator's deployed artefacts against what the
    current plan would generate."""

    has_drift: bool
    drifted_artefacts: tuple[str, ...] = field(default_factory=tuple)
    details: tuple[ValidationFinding, ...] = field(default_factory=tuple)


@dataclass(frozen=True)
class ConventionsDocument:
    """Platform-specific conventions a translator returns for agent consumption.
    An optional capability: the default is an empty document naming only the
    target."""

    target: str
    idioms: str = ""
    testing_conventions: str = ""
    known_issues: str = ""
    examples: dict[str, str] = field(default_factory=dict)
