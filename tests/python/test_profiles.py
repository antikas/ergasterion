"""Assert-script tests for the five reference profiles (architecture section 5)
and ergasterion/framework/patterns.py's profile loader.

Each test function proves one property of the profile layer:
  - Every one of the five reference profile files carries architecture
    section 5's mandatory/optional/forbidden table verbatim, pinned here
    against an independent transcription of that table (not against the
    loader's own output) so a regression in the loader cannot silently
    agree with itself.
  - Every profile's mandatory/optional/forbidden sets are pairwise disjoint;
    the loader is proven RED on a fixture profile whose sets overlap.
  - A profile document naming a token that is not one of the fifteen
    canonical patterns fails closed, whether the token appears in
    mandatory/optional/forbidden or in ordering.
  - Every profile's ordering constraint is exactly architecture section
    3.1's composition order, filtered to the patterns that profile
    classifies as mandatory or optional -- pinned against an independent
    transcription of that master order, not against the profile files
    themselves.
  - An unknown profile name fails closed with UnknownProfileError.
  - The landing profile's mandatory patterns are exactly the patterns that
    occur in resolve("landing")'s graph, and its optional patterns (which
    have no authoring surface in this release) occur in none of it -- the
    disposition check that used to live in test_framework_core.py against
    BRONZE_MANDATORY/BRONZE_OPTIONAL/BRONZE_FORBIDDEN, now against the
    landing profile's own data.

Usage:
    python tests/python/test_profiles.py
"""

from __future__ import annotations

import sys
import traceback
from pathlib import Path

if __package__ in (None, ""):
    import os as _os
    import sys as _sys
    _sys.path.insert(0, _os.path.dirname(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))))

from ergasterion.framework.models import InvalidProfileDefinitionError, PatternId, UnknownProfileError
from ergasterion.framework.patterns import PROFILE_NAMES, PROFILES_DIR, load_profile, parse_profile_document
from ergasterion.framework.resolver import resolve

P = PatternId

# Architecture section 5's table, transcribed independently of
# ergasterion/profiles/*.yml and of the loader: a regression in either must
# be caught here, not agreed with.
EXPECTED_TABLES: dict[str, dict[str, frozenset[PatternId]]] = {
    "landing": {
        "mandatory": frozenset({
            P.BATCH_INGESTION, P.DATA_VALIDATION, P.DATA_CONTRACTS, P.LINEAGE_CAPTURE,
            P.METADATA_CAPTURE, P.SCHEMA_PUBLISH, P.DATA_PUBLISH, P.CHECKPOINT_RETRIES,
        }),
        "optional": frozenset({P.BATCH_TRANSFER, P.DATA_FILTERING}),
        "forbidden": frozenset({
            P.SCHEMA_TRANSFORM, P.CALCULATED_FIELDS, P.DATA_ENRICHMENT, P.DATA_AGGREGATION, P.DATA_CURATION,
        }),
    },
    "integration": {
        "mandatory": frozenset({
            P.BATCH_TRANSFER, P.DATA_VALIDATION, P.SCHEMA_TRANSFORM, P.DATA_CURATION, P.DATA_CONTRACTS,
            P.LINEAGE_CAPTURE, P.METADATA_CAPTURE, P.SCHEMA_PUBLISH, P.DATA_PUBLISH, P.CHECKPOINT_RETRIES,
        }),
        "optional": frozenset({P.CALCULATED_FIELDS, P.DATA_ENRICHMENT, P.DATA_FILTERING, P.DATA_AGGREGATION}),
        "forbidden": frozenset({P.BATCH_INGESTION}),
    },
    "derivation": {
        "mandatory": frozenset({
            P.BATCH_TRANSFER, P.DATA_VALIDATION, P.CALCULATED_FIELDS, P.DATA_CONTRACTS, P.LINEAGE_CAPTURE,
            P.METADATA_CAPTURE, P.SCHEMA_PUBLISH, P.DATA_PUBLISH, P.CHECKPOINT_RETRIES,
        }),
        "optional": frozenset({P.DATA_ENRICHMENT, P.DATA_AGGREGATION, P.DATA_FILTERING}),
        "forbidden": frozenset({P.BATCH_INGESTION, P.DATA_CURATION}),
    },
    "consolidation": {
        "mandatory": frozenset({
            P.BATCH_TRANSFER, P.DATA_VALIDATION, P.DATA_CONTRACTS, P.LINEAGE_CAPTURE, P.METADATA_CAPTURE,
            P.SCHEMA_PUBLISH, P.DATA_PUBLISH, P.CHECKPOINT_RETRIES,
        }),
        "optional": frozenset({P.CALCULATED_FIELDS, P.DATA_ENRICHMENT, P.DATA_AGGREGATION, P.DATA_CURATION}),
        "forbidden": frozenset({P.BATCH_INGESTION}),
    },
    "serving": {
        "mandatory": frozenset({
            P.BATCH_TRANSFER, P.DATA_VALIDATION, P.DATA_CONTRACTS, P.LINEAGE_CAPTURE, P.METADATA_CAPTURE,
            P.DATA_PUBLISH, P.CHECKPOINT_RETRIES,
        }),
        "optional": frozenset({
            P.CALCULATED_FIELDS, P.DATA_FILTERING, P.DATA_AGGREGATION, P.DATA_CURATION, P.SCHEMA_PUBLISH,
        }),
        "forbidden": frozenset({P.BATCH_INGESTION}),
    },
}

assert set(EXPECTED_TABLES) == set(PROFILE_NAMES)

# Architecture section 3.1's composition order (the worked declaration's
# steps: block), extended to the two patterns 3.1's worked example omits
# (batch_ingestion, at the front of the ingest phase; data_filtering and
# data_aggregation, alongside the other business-logic-phase patterns) and
# to checkpoint_retries (the enclosing wrapper, placed last). Transcribed
# independently of ergasterion/profiles/*.yml's own ordering: fields, not
# re-derived from them.
MASTER_COMPOSITION_ORDER: tuple[PatternId, ...] = (
    P.BATCH_INGESTION,
    P.BATCH_TRANSFER,
    P.DATA_VALIDATION,
    P.SCHEMA_TRANSFORM,
    P.CALCULATED_FIELDS,
    P.DATA_ENRICHMENT,
    P.DATA_FILTERING,
    P.DATA_AGGREGATION,
    P.DATA_CURATION,
    P.DATA_CONTRACTS,
    P.LINEAGE_CAPTURE,
    P.METADATA_CAPTURE,
    P.SCHEMA_PUBLISH,
    P.DATA_PUBLISH,
    P.CHECKPOINT_RETRIES,
)

assert set(MASTER_COMPOSITION_ORDER) == set(PatternId)


def _expected_ordering(profile_name: str) -> tuple[PatternId, ...]:
    composed = EXPECTED_TABLES[profile_name]["mandatory"] | EXPECTED_TABLES[profile_name]["optional"]
    return tuple(p for p in MASTER_COMPOSITION_ORDER if p in composed)


def test_every_profile_matches_architecture_section_5_verbatim() -> None:
    for name in PROFILE_NAMES:
        profile = load_profile(name)
        expected = EXPECTED_TABLES[name]
        assert profile.mandatory == expected["mandatory"], (name, "mandatory", profile.mandatory)
        assert profile.optional == expected["optional"], (name, "optional", profile.optional)
        assert profile.forbidden == expected["forbidden"], (name, "forbidden", profile.forbidden)


def test_every_profile_s_three_sets_are_pairwise_disjoint() -> None:
    for name in PROFILE_NAMES:
        profile = load_profile(name)
        assert profile.mandatory.isdisjoint(profile.optional), name
        assert profile.mandatory.isdisjoint(profile.forbidden), name
        assert profile.optional.isdisjoint(profile.forbidden), name


def test_every_profile_s_ordering_matches_section_3_1_composition_order() -> None:
    for name in PROFILE_NAMES:
        profile = load_profile(name)
        expected_ordering = _expected_ordering(name)
        assert profile.ordering == expected_ordering, (name, profile.ordering, expected_ordering)
        assert set(profile.ordering) == (profile.mandatory | profile.optional), name


def test_loader_is_red_on_an_overlapping_fixture_profile() -> None:
    overlapping = {
        "schema": "ergasterion.profile/v1",
        "name": "fixture",
        "mandatory": ["batch_ingestion", "data_validation"],
        "optional": ["batch_ingestion"],  # overlaps mandatory
        "forbidden": [],
        "ordering": ["batch_ingestion", "data_validation"],
    }
    try:
        parse_profile_document("fixture", overlapping)
    except InvalidProfileDefinitionError as exc:
        assert exc.code == "invalid_profile_definition"
        assert exc.profile_name == "fixture"
    else:
        raise AssertionError("expected InvalidProfileDefinitionError for overlapping mandatory/optional sets")


def test_loader_is_red_on_a_profile_naming_an_unknown_pattern() -> None:
    schema = "ergasterion.profile/v1"
    for bad_document in (
        {"schema": schema, "name": "fixture", "mandatory": ["not_a_real_pattern"], "optional": [], "forbidden": []},
        {"schema": schema, "name": "fixture", "mandatory": [], "optional": ["also_not_real"], "forbidden": []},
        {"schema": schema, "name": "fixture", "mandatory": [], "optional": [], "forbidden": ["nope"]},
        {
            "schema": schema, "name": "fixture", "mandatory": ["batch_ingestion"], "optional": [], "forbidden": [],
            "ordering": ["batch_ingestion", "not_a_pattern"],
        },
    ):
        try:
            parse_profile_document("fixture", bad_document)
        except InvalidProfileDefinitionError as exc:
            assert exc.code == "invalid_profile_definition"
        else:
            raise AssertionError(f"expected InvalidProfileDefinitionError for {bad_document!r}")


def test_loader_is_red_on_ordering_naming_a_forbidden_or_unmodelled_pattern() -> None:
    document = {
        "schema": "ergasterion.profile/v1",
        "name": "fixture",
        "mandatory": ["batch_ingestion"],
        "optional": [],
        "forbidden": ["data_curation"],
        "ordering": ["batch_ingestion", "data_curation"],  # forbidden, not mandatory/optional
    }
    try:
        parse_profile_document("fixture", document)
    except InvalidProfileDefinitionError as exc:
        assert exc.code == "invalid_profile_definition"
    else:
        raise AssertionError("expected InvalidProfileDefinitionError for ordering naming a forbidden pattern")


def test_loader_is_red_on_ordering_missing_a_composed_pattern() -> None:
    document = {
        "schema": "ergasterion.profile/v1",
        "name": "fixture",
        "mandatory": ["batch_ingestion", "data_validation"],
        "optional": [],
        "forbidden": [],
        "ordering": ["batch_ingestion"],  # missing data_validation
    }
    try:
        parse_profile_document("fixture", document)
    except InvalidProfileDefinitionError as exc:
        assert exc.code == "invalid_profile_definition"
    else:
        raise AssertionError("expected InvalidProfileDefinitionError for incomplete ordering")


def test_profile_names_matches_the_profiles_directory_yml_files() -> None:
    on_disk = {path.stem for path in PROFILES_DIR.glob("*.yml")}
    assert on_disk == set(PROFILE_NAMES), (on_disk, PROFILE_NAMES)


def test_loader_is_red_on_a_profile_missing_a_required_key() -> None:
    schema = "ergasterion.profile/v1"
    base = {"schema": schema, "name": "fixture", "mandatory": ["batch_ingestion"], "optional": [], "forbidden": []}
    for missing_key in ("mandatory", "optional", "forbidden"):
        document = {key: value for key, value in base.items() if key != missing_key}
        try:
            parse_profile_document("fixture", document)
        except InvalidProfileDefinitionError as exc:
            assert exc.code == "invalid_profile_definition"
            assert missing_key in exc.reason, (missing_key, exc.reason)
        else:
            raise AssertionError(f"expected InvalidProfileDefinitionError for a document missing {missing_key!r}")


def test_loader_is_red_on_a_profile_with_a_misspelled_required_key() -> None:
    document = {
        "schema": "ergasterion.profile/v1",
        "name": "fixture",
        "mandatroy": ["batch_ingestion"],  # misspelled "mandatory"
        "optional": [],
        "forbidden": [],
    }
    try:
        parse_profile_document("fixture", document)
    except InvalidProfileDefinitionError as exc:
        assert exc.code == "invalid_profile_definition"
        assert "mandatory" in exc.reason, exc.reason
    else:
        raise AssertionError("expected InvalidProfileDefinitionError for a misspelled required key")


def test_loader_is_red_on_a_profile_with_the_wrong_schema() -> None:
    good = {"name": "fixture", "mandatory": ["batch_ingestion"], "optional": [], "forbidden": []}
    for bad_document in (
        dict(good),  # schema key absent entirely
        {**good, "schema": "ergasterion.profile/v2"},
        {**good, "schema": None},
    ):
        try:
            parse_profile_document("fixture", bad_document)
        except InvalidProfileDefinitionError as exc:
            assert exc.code == "invalid_profile_definition"
            assert "schema" in exc.reason, exc.reason
        else:
            raise AssertionError(f"expected InvalidProfileDefinitionError for {bad_document!r}")


def test_unknown_profile_name_fails_closed() -> None:
    for bad_name in ("bogus", "bronze", "silver", "gold", ""):
        try:
            load_profile(bad_name)
        except UnknownProfileError as exc:
            assert exc.code == "unknown_profile"
            assert exc.profile_name == bad_name
        else:
            raise AssertionError(f"expected UnknownProfileError for {bad_name!r}")


def test_landing_profile_disposition_matches_the_resolved_graph() -> None:
    profile = load_profile("landing")
    plan = resolve("landing")
    occurring_patterns = {o.pattern_id for o in plan.occurrences}

    assert occurring_patterns == profile.mandatory, (occurring_patterns, profile.mandatory)
    for optional_pattern in profile.optional:
        assert optional_pattern not in occurring_patterns, optional_pattern
    for forbidden_pattern in profile.forbidden:
        assert forbidden_pattern not in occurring_patterns, forbidden_pattern


def test_disposition_of_reflects_the_three_sets_and_none_for_unmodelled() -> None:
    from ergasterion.framework.models import PatternDisposition

    landing = load_profile("landing")
    assert landing.disposition_of(P.BATCH_INGESTION) is PatternDisposition.MANDATORY
    assert landing.disposition_of(P.BATCH_TRANSFER) is PatternDisposition.OPTIONAL
    assert landing.disposition_of(P.DATA_CURATION) is PatternDisposition.FORBIDDEN

    # Schema Transform is unmodelled (neither M/O/F) for derivation, consolidation and serving.
    for name in ("derivation", "consolidation", "serving"):
        profile = load_profile(name)
        assert profile.disposition_of(P.SCHEMA_TRANSFORM) is None, name


TESTS = [
    test_profile_names_matches_the_profiles_directory_yml_files,
    test_every_profile_matches_architecture_section_5_verbatim,
    test_every_profile_s_three_sets_are_pairwise_disjoint,
    test_every_profile_s_ordering_matches_section_3_1_composition_order,
    test_loader_is_red_on_an_overlapping_fixture_profile,
    test_loader_is_red_on_a_profile_naming_an_unknown_pattern,
    test_loader_is_red_on_ordering_naming_a_forbidden_or_unmodelled_pattern,
    test_loader_is_red_on_ordering_missing_a_composed_pattern,
    test_loader_is_red_on_a_profile_missing_a_required_key,
    test_loader_is_red_on_a_profile_with_a_misspelled_required_key,
    test_loader_is_red_on_a_profile_with_the_wrong_schema,
    test_unknown_profile_name_fails_closed,
    test_landing_profile_disposition_matches_the_resolved_graph,
    test_disposition_of_reflects_the_three_sets_and_none_for_unmodelled,
]


def main() -> int:
    failures = 0
    for test in TESTS:
        name = test.__name__
        try:
            test()
        except Exception:  # noqa: BLE001 - report and continue, exit code carries the signal
            failures += 1
            print(f"FAIL {name}")
            traceback.print_exc()
        else:
            print(f"PASS {name}")
    total = len(TESTS)
    print(f"{total - failures}/{total} passed")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
