"""The composition resolver: turns a named profile into its resolved execution graph.

Only the ``landing`` profile has an authoring surface in this release: it
resolves the exact, normative graph that reproduces today's landing
composition, under the occurrence ids the portable IDL declares
(``landing.*``), re-issued under layer-neutral names.
The other four reference profiles
(``integration``, ``derivation``, ``consolidation``, ``serving``) are
registered as data under ``ergasterion/profiles/`` but carry no execution
graph yet: resolving one of them fails closed with ``UnsupportedProfileError``.
An unrecognised profile name fails closed with ``UnknownProfileError``.
"""

from __future__ import annotations

from ergasterion.framework.models import (
    Edge,
    EdgeRole,
    ExecutionPlan,
    HandoffSchemaId,
    Occurrence,
    PatternId,
    Role,
    UnknownProfileError,
    UnsupportedProfileError,
)
from ergasterion.framework.patterns import PROFILE_NAMES

# --------------------------------------------------------------------------- the
# normative landing graph: version 1's fixed occurrence table and edge table.
# Every occurrence's roles are pre-sorted in Role token order;
# Occurrence.__post_init__ enforces that at construction.

_LANDING_OCCURRENCES: tuple[Occurrence, ...] = (
    Occurrence("landing.checkpoint", PatternId.CHECKPOINT_RETRIES, (Role.WRAPPER, Role.POLICY), True),
    Occurrence("landing.contract", PatternId.DATA_CONTRACTS, (Role.POLICY, Role.BARRIER), True),
    Occurrence("landing.ingest", PatternId.BATCH_INGESTION, (Role.PHASE,), True),
    Occurrence("landing.lineage", PatternId.LINEAGE_CAPTURE, (Role.OBSERVER,), True),
    Occurrence("landing.metadata", PatternId.METADATA_CAPTURE, (Role.OBSERVER,), True),
    Occurrence("landing.publish", PatternId.DATA_PUBLISH, (Role.BARRIER,), True),
    Occurrence("landing.schema", PatternId.SCHEMA_PUBLISH, (Role.OBSERVER, Role.BARRIER), True),
    Occurrence("landing.validate", PatternId.DATA_VALIDATION, (Role.PHASE,), True),
)

_LANDING_EDGES: tuple[Edge, ...] = (
    Edge("landing.ingest", "landing.validate", EdgeRole.DATA, HandoffSchemaId.RAW_EVIDENCE),
    Edge("landing.validate", "landing.contract", EdgeRole.VALIDATION, HandoffSchemaId.VALIDATION_RESULT),
    Edge("landing.contract", "landing.schema", EdgeRole.READINESS, HandoffSchemaId.CONTRACT_CONFORMANCE),
    Edge("landing.validate", "landing.publish", EdgeRole.BARRIER, HandoffSchemaId.VALIDATION_RESULT),
    Edge("landing.contract", "landing.publish", EdgeRole.BARRIER, HandoffSchemaId.CONTRACT_CONFORMANCE),
    Edge("landing.schema", "landing.publish", EdgeRole.BARRIER, HandoffSchemaId.INTERFACE_READINESS),
    Edge("landing.ingest", "landing.lineage", EdgeRole.OBSERVE, HandoffSchemaId.RAW_EVIDENCE),
    Edge("landing.validate", "landing.lineage", EdgeRole.OBSERVE, HandoffSchemaId.VALIDATION_RESULT),
    Edge("landing.publish", "landing.lineage", EdgeRole.OBSERVE, HandoffSchemaId.PUBLICATION_CONFIRMATION),
    Edge("landing.contract", "landing.metadata", EdgeRole.OBSERVE, HandoffSchemaId.CONTRACT_CONFORMANCE),
    Edge("landing.validate", "landing.metadata", EdgeRole.OBSERVE, HandoffSchemaId.VALIDATION_RESULT),
    Edge("landing.publish", "landing.metadata", EdgeRole.OBSERVE, HandoffSchemaId.PUBLICATION_CONFIRMATION),
)

_LANDING_WRAPPER_ID = "landing.checkpoint"
_LANDING_WRAPPER_MEMBERS = tuple(
    sorted(o.occurrence_id for o in _LANDING_OCCURRENCES if o.occurrence_id != _LANDING_WRAPPER_ID)
)


def _resolve_landing() -> ExecutionPlan:
    return ExecutionPlan(
        profile="landing",
        occurrences=tuple(sorted(_LANDING_OCCURRENCES, key=lambda o: o.occurrence_id)),
        edges=_LANDING_EDGES,
        wrapper_id=_LANDING_WRAPPER_ID,
        wrapper_members=_LANDING_WRAPPER_MEMBERS,
    )


def resolve(profile_name: str) -> ExecutionPlan:
    """Resolve one named profile to its execution graph.

    ``landing`` always resolves the exact normative graph above. Version 1
    has no declaration input and no optional-pattern authoring surface: Batch
    Transfer and Data Filtering are classified (see
    ``ergasterion.framework.patterns.load_profile("landing")``) but neither
    ever appears as an occurrence. The other four reference profiles raise
    ``UnsupportedProfileError`` deterministically: they carry no execution
    graph to resolve against in this release. A ``profile_name`` that is not
    one of the five reference profile names raises ``UnknownProfileError``,
    checked before ``UnsupportedProfileError`` so an unrecognised name never
    reaches the unsupported-but-valid branch.
    """

    if not isinstance(profile_name, str) or profile_name not in PROFILE_NAMES:
        raise UnknownProfileError(profile_name)
    if profile_name == "landing":
        return _resolve_landing()
    raise UnsupportedProfileError(profile_name)
