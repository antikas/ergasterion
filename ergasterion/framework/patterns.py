"""The canonical fifteen-pattern registry and the Bronze composition table.

The fifteen-pattern universe is closed and held in frozen module-level tables.
Bronze has an exact mandatory, optional, and forbidden classification. Silver
and Gold do not have composition tables in this release, so resolution for
those layers fails closed.
"""

from __future__ import annotations

from ergasterion.framework.models import PatternDisposition, PatternId

# Display text is separate from registry identity: identity is exact, display
# text is human-readable. All fifteen canonical patterns, exactly.
PATTERN_DISPLAY_NAMES: dict[PatternId, str] = {
    PatternId.BATCH_INGESTION: "Batch Ingestion",
    PatternId.BATCH_TRANSFER: "Batch Transfer",
    PatternId.SCHEMA_TRANSFORM: "Schema Transform",
    PatternId.CALCULATED_FIELDS: "Calculated Fields",
    PatternId.DATA_ENRICHMENT: "Data Enrichment",
    PatternId.DATA_FILTERING: "Data Filtering",
    PatternId.DATA_VALIDATION: "Data Validation",
    PatternId.DATA_AGGREGATION: "Data Aggregation",
    PatternId.DATA_CURATION: "Data Curation",
    PatternId.DATA_CONTRACTS: "Data Contracts",
    PatternId.LINEAGE_CAPTURE: "Lineage Capture",
    PatternId.METADATA_CAPTURE: "Metadata Capture",
    PatternId.SCHEMA_PUBLISH: "Schema Publish",
    PatternId.DATA_PUBLISH: "Data Publish",
    PatternId.CHECKPOINT_RETRIES: "Checkpoint & Retries",
}

assert set(PATTERN_DISPLAY_NAMES) == set(PatternId), "the registry must classify all fifteen canonical patterns"

# The Bronze composition classifies all fifteen patterns. Mandatory patterns are
# the eight occurrences the normative Bronze graph always contains. Batch
# Transfer is the sole optional pattern and has no authoring surface in version
# 1 (see `resolution_status`). The six forbidden patterns can never occur in a
# Bronze graph: business-predicate transformation would violate the
# source-aligned Bronze boundary.
BRONZE_MANDATORY: frozenset[PatternId] = frozenset(
    {
        PatternId.BATCH_INGESTION,
        PatternId.DATA_VALIDATION,
        PatternId.DATA_CONTRACTS,
        PatternId.LINEAGE_CAPTURE,
        PatternId.METADATA_CAPTURE,
        PatternId.SCHEMA_PUBLISH,
        PatternId.DATA_PUBLISH,
        PatternId.CHECKPOINT_RETRIES,
    }
)

BRONZE_OPTIONAL: frozenset[PatternId] = frozenset({PatternId.BATCH_TRANSFER})

BRONZE_FORBIDDEN: frozenset[PatternId] = frozenset(
    {
        PatternId.SCHEMA_TRANSFORM,
        PatternId.CALCULATED_FIELDS,
        PatternId.DATA_ENRICHMENT,
        PatternId.DATA_FILTERING,
        PatternId.DATA_AGGREGATION,
        PatternId.DATA_CURATION,
    }
)

assert BRONZE_MANDATORY | BRONZE_OPTIONAL | BRONZE_FORBIDDEN == set(PatternId)
assert not (BRONZE_MANDATORY & BRONZE_OPTIONAL & BRONZE_FORBIDDEN)
assert len(BRONZE_MANDATORY) == 8 and len(BRONZE_OPTIONAL) == 1 and len(BRONZE_FORBIDDEN) == 6


def classify_bronze(pattern_id: PatternId) -> PatternDisposition:
    """Return the Bronze disposition for one of the fifteen canonical patterns.
    Every pattern classifies: the classification covers the full registry."""

    if pattern_id in BRONZE_MANDATORY:
        return PatternDisposition.MANDATORY
    if pattern_id in BRONZE_OPTIONAL:
        return PatternDisposition.OPTIONAL
    if pattern_id in BRONZE_FORBIDDEN:
        return PatternDisposition.FORBIDDEN
    raise AssertionError(f"unclassified pattern: {pattern_id!r}")  # pragma: no cover - guarded by the module assertion above


class ResolutionStatus(str):
    """String-valued resolution outcomes for a pattern's presence in the resolved
    Bronze graph. These are graph-presence facts, held as a plain string-valued
    class one level up from the disposition table."""

    IN_BRONZE_GRAPH = "in_bronze_graph"
    UNSUPPORTED_OPTIONAL_PATTERN = "unsupported_optional_pattern"
    FORBIDDEN = "forbidden"


def resolution_status(pattern_id: PatternId) -> str:
    """Whether a pattern occurs in the resolved Bronze graph, and why not when it
    does not. Batch Transfer is classified optional and has no authoring surface:
    it deterministically resolves ``unsupported_optional_pattern``. Forbidden
    patterns never resolve into a Bronze graph."""

    disposition = classify_bronze(pattern_id)
    if disposition is PatternDisposition.MANDATORY:
        return ResolutionStatus.IN_BRONZE_GRAPH
    if disposition is PatternDisposition.OPTIONAL:
        return ResolutionStatus.UNSUPPORTED_OPTIONAL_PATTERN
    return ResolutionStatus.FORBIDDEN
