"""The canonical fifteen-pattern registry and the five reference profiles.

The fifteen-pattern universe is closed and held in a frozen module-level
table (``PATTERN_DISPLAY_NAMES``). Composition constraints -- which patterns
are mandatory, optional and forbidden for a product, and the ordering
constraints between them -- are no longer code: they are data, one YAML file
per reference profile under ``ergasterion/profiles/`` (architecture section
5). ``load_profile()`` reads and validates one of them; only the resolver
(``ergasterion/framework/resolver.py``) turns the ``landing`` profile into an
executable occurrence graph in this release.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

import yaml

from ergasterion.framework.models import (
    InvalidProfileDefinitionError,
    PatternDisposition,
    PatternId,
    UnknownProfileError,
)

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

# The five reference profiles (architecture section 5). Order matters nowhere
# except readability; PROFILE_NAMES is the closed set of names this engine
# ships a data file for. Only "landing" has an authoring surface (an
# executable occurrence graph) in this release; the other four are
# registered composition-constraint data with no graph behind them yet.
PROFILE_NAMES: tuple[str, ...] = ("landing", "integration", "derivation", "consolidation", "serving")

PROFILES_DIR = Path(__file__).resolve().parent.parent / "profiles"


@dataclass(frozen=True)
class Profile:
    """One profile's composition constraints: which patterns are mandatory,
    optional and forbidden, and the ordering constraints between the patterns
    this profile actually classifies (mandatory or optional).

    Architecture section 5's table does not classify every one of the fifteen
    patterns for every profile -- Schema Transform, for example, is neither
    mandatory, optional nor forbidden under derivation, consolidation or
    serving. ``mandatory``, ``optional`` and ``forbidden`` are carried
    verbatim from that table and are pairwise disjoint, but their union need
    not cover all fifteen patterns. A pattern outside all three sets is
    simply not modelled for this profile; ``disposition_of`` returns ``None``
    for it rather than forcing a fictitious classification.
    """

    name: str
    mandatory: frozenset[PatternId]
    optional: frozenset[PatternId]
    forbidden: frozenset[PatternId]
    ordering: tuple[PatternId, ...]

    def disposition_of(self, pattern_id: PatternId) -> PatternDisposition | None:
        if pattern_id in self.mandatory:
            return PatternDisposition.MANDATORY
        if pattern_id in self.optional:
            return PatternDisposition.OPTIONAL
        if pattern_id in self.forbidden:
            return PatternDisposition.FORBIDDEN
        return None


def _pattern_set(document: Mapping[str, Any], profile_name: str, key: str) -> frozenset[PatternId]:
    if key not in document:
        raise InvalidProfileDefinitionError(profile_name, f"missing required key: {key!r}")
    tokens = document[key]
    if tokens is None:
        tokens = []
    result: set[PatternId] = set()
    for token in tokens:
        try:
            result.add(PatternId(token))
        except ValueError:
            raise InvalidProfileDefinitionError(profile_name, f"{key} names an unknown pattern: {token!r}") from None
    return frozenset(result)


def parse_profile_document(profile_name: str, document: Mapping[str, Any]) -> Profile:
    """Parse and validate one profile document (already loaded from YAML, or
    built in memory by a test) into a ``Profile``.

    Fails closed with ``InvalidProfileDefinitionError`` when: the ``schema``
    key is not exactly ``"ergasterion.profile/v1"``; the ``mandatory``,
    ``optional`` or ``forbidden`` key is absent or misspelled (each is
    required, even when the set it carries is empty); a mandatory, optional,
    forbidden or ordering entry names a token that is not one of the fifteen
    canonical patterns; the mandatory/optional/forbidden sets are not
    pairwise disjoint; or an ordering entry names a pattern this profile
    does not classify as mandatory or optional. ``ordering`` must be exactly
    a permutation of ``mandatory | optional``: architecture section 5
    describes ordering constraints between the patterns a profile actually
    composes, never a pattern it forbids or leaves unmodelled.
    """

    schema = document.get("schema")
    if schema != "ergasterion.profile/v1":
        raise InvalidProfileDefinitionError(
            profile_name, f"schema must be 'ergasterion.profile/v1', got {schema!r}"
        )

    declared_name = document.get("name")
    if declared_name is not None and declared_name != profile_name:
        raise InvalidProfileDefinitionError(
            profile_name, f"document name {declared_name!r} does not match {profile_name!r}"
        )

    mandatory = _pattern_set(document, profile_name, "mandatory")
    optional = _pattern_set(document, profile_name, "optional")
    forbidden = _pattern_set(document, profile_name, "forbidden")

    if len(mandatory) + len(optional) + len(forbidden) != len(mandatory | optional | forbidden):
        raise InvalidProfileDefinitionError(profile_name, "mandatory/optional/forbidden sets are not disjoint")

    composed = mandatory | optional
    ordering_tokens = document.get("ordering") or []
    ordering: list[PatternId] = []
    for token in ordering_tokens:
        try:
            pattern_id = PatternId(token)
        except ValueError:
            raise InvalidProfileDefinitionError(profile_name, f"ordering names an unknown pattern: {token!r}") from None
        if pattern_id not in composed:
            raise InvalidProfileDefinitionError(
                profile_name,
                f"ordering names {pattern_id.value!r}, which this profile classifies as "
                "neither mandatory nor optional",
            )
        ordering.append(pattern_id)
    if len(set(ordering)) != len(ordering):
        raise InvalidProfileDefinitionError(profile_name, "ordering contains a duplicate pattern")
    if set(ordering) != composed:
        raise InvalidProfileDefinitionError(
            profile_name, "ordering must name every mandatory and optional pattern exactly once"
        )

    return Profile(
        name=profile_name,
        mandatory=mandatory,
        optional=optional,
        forbidden=forbidden,
        ordering=tuple(ordering),
    )


def load_profile(name: str) -> Profile:
    """Load and validate one reference profile from
    ``ergasterion/profiles/<name>.yml``. An unrecognised name fails closed
    with ``UnknownProfileError`` before any file access."""

    if name not in PROFILE_NAMES:
        raise UnknownProfileError(name)
    path = PROFILES_DIR / f"{name}.yml"
    if not path.is_file():
        raise UnknownProfileError(name)
    document = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    return parse_profile_document(name, document)
