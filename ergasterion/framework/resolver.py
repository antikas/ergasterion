"""The composition resolver: turns a layer into its resolved execution graph.

The Bronze graph is normative and fixed. The resolver returns that exact graph
for Bronze and fails closed for Silver or Gold because this release defines no
composition table for either layer.
"""

from __future__ import annotations

from ergasterion.framework.models import (
    Edge,
    EdgeRole,
    ExecutionPlan,
    HandoffSchemaId,
    InvalidLayerArgumentError,
    Layer,
    Occurrence,
    PatternId,
    Role,
    UnsupportedLayerError,
)

# --------------------------------------------------------------------------- the
# normative Bronze graph: version 1's fixed occurrence table and edge table.
# Every occurrence's roles are pre-sorted in Role token order;
# Occurrence.__post_init__ enforces that at construction.

_BRONZE_OCCURRENCES: tuple[Occurrence, ...] = (
    Occurrence("bronze.checkpoint", PatternId.CHECKPOINT_RETRIES, (Role.WRAPPER, Role.POLICY), True),
    Occurrence("bronze.contract", PatternId.DATA_CONTRACTS, (Role.POLICY, Role.BARRIER), True),
    Occurrence("bronze.ingest", PatternId.BATCH_INGESTION, (Role.PHASE,), True),
    Occurrence("bronze.lineage", PatternId.LINEAGE_CAPTURE, (Role.OBSERVER,), True),
    Occurrence("bronze.metadata", PatternId.METADATA_CAPTURE, (Role.OBSERVER,), True),
    Occurrence("bronze.publish", PatternId.DATA_PUBLISH, (Role.BARRIER,), True),
    Occurrence("bronze.schema", PatternId.SCHEMA_PUBLISH, (Role.OBSERVER, Role.BARRIER), True),
    Occurrence("bronze.validate", PatternId.DATA_VALIDATION, (Role.PHASE,), True),
)

_BRONZE_EDGES: tuple[Edge, ...] = (
    Edge("bronze.ingest", "bronze.validate", EdgeRole.DATA, HandoffSchemaId.RAW_EVIDENCE),
    Edge("bronze.validate", "bronze.contract", EdgeRole.VALIDATION, HandoffSchemaId.VALIDATION_RESULT),
    Edge("bronze.contract", "bronze.schema", EdgeRole.READINESS, HandoffSchemaId.CONTRACT_CONFORMANCE),
    Edge("bronze.validate", "bronze.publish", EdgeRole.BARRIER, HandoffSchemaId.VALIDATION_RESULT),
    Edge("bronze.contract", "bronze.publish", EdgeRole.BARRIER, HandoffSchemaId.CONTRACT_CONFORMANCE),
    Edge("bronze.schema", "bronze.publish", EdgeRole.BARRIER, HandoffSchemaId.INTERFACE_READINESS),
    Edge("bronze.ingest", "bronze.lineage", EdgeRole.OBSERVE, HandoffSchemaId.RAW_EVIDENCE),
    Edge("bronze.validate", "bronze.lineage", EdgeRole.OBSERVE, HandoffSchemaId.VALIDATION_RESULT),
    Edge("bronze.publish", "bronze.lineage", EdgeRole.OBSERVE, HandoffSchemaId.PUBLICATION_CONFIRMATION),
    Edge("bronze.contract", "bronze.metadata", EdgeRole.OBSERVE, HandoffSchemaId.CONTRACT_CONFORMANCE),
    Edge("bronze.validate", "bronze.metadata", EdgeRole.OBSERVE, HandoffSchemaId.VALIDATION_RESULT),
    Edge("bronze.publish", "bronze.metadata", EdgeRole.OBSERVE, HandoffSchemaId.PUBLICATION_CONFIRMATION),
)

_BRONZE_WRAPPER_ID = "bronze.checkpoint"
_BRONZE_WRAPPER_MEMBERS = tuple(
    sorted(o.occurrence_id for o in _BRONZE_OCCURRENCES if o.occurrence_id != _BRONZE_WRAPPER_ID)
)


def _resolve_bronze() -> ExecutionPlan:
    return ExecutionPlan(
        layer=Layer.BRONZE,
        occurrences=tuple(sorted(_BRONZE_OCCURRENCES, key=lambda o: o.occurrence_id)),
        edges=_BRONZE_EDGES,
        wrapper_id=_BRONZE_WRAPPER_ID,
        wrapper_members=_BRONZE_WRAPPER_MEMBERS,
    )


def resolve(layer: Layer) -> ExecutionPlan:
    """Resolve one layer to its execution graph.

    Bronze always resolves the exact normative graph above. Version 1 has no
    declaration input and no optional-pattern authoring surface: Batch Transfer
    is classified (see ``patterns.resolution_status``) but never appears as an
    occurrence. Silver and Gold raise ``UnsupportedLayerError`` deterministically:
    they carry no composition table to resolve against. A ``layer`` argument
    that is not a ``Layer`` member (for example the plain string ``"bronze"``)
    raises ``InvalidLayerArgumentError``: a typed entry point rejects an
    untyped argument with its own error code, checked before
    ``UnsupportedLayerError``, whose constructor requires a real ``Layer``
    member.
    """

    if not isinstance(layer, Layer):
        raise InvalidLayerArgumentError(layer)
    if layer is Layer.BRONZE:
        return _resolve_bronze()
    raise UnsupportedLayerError(layer)
