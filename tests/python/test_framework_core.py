"""Assert-script tests for ergasterion/framework/** and ergasterion/translators/**
(repo convention: no pytest).

Each test function proves one property of the framework core:
  - The registry classifies all fifteen canonical patterns with display text
    and no legacy product-type alias anywhere. (The mandatory/optional/
    forbidden disposition itself is now profile data, pinned in
    tests/python/test_profiles.py, not asserted here.)
  - The landing profile resolves the exact normative execution graph:
    occurrence identities, roles (sorted in role-token order), the wrapper's
    membership, and every edge with its role and handoff schema, under the
    landing.* ids the portable IDL declares (re-issued under layer-neutral
    names). Its plan digest changes on exactly one axis relative to the
    pre-profile digest: the profile name in place of the layer.
  - Every reference profile other than landing raises UnsupportedProfileError
    deterministically: none of them has an execution graph in this release.
    An unrecognised profile name raises UnknownProfileError.
  - The translator conformance seam: every vector in
    tests/fixtures/translator_conformance.json produces the router outcome it
    names, covering the positive case, an observe-only translator, and all
    five required failure modes (missing/duplicate/reordered ownership,
    digest mismatch, bad handoff).
  - The router rejects an unknown occurrence reference and an owned/observed
    overlap, both undeclared_attachment, and composes translations in plan
    order, including an observe-only translator's artefacts.
  - Package-import cleanliness: ergasterion.framework and ergasterion.translators
    import without a dbt, DuckDB, SQLite or orchestrator package on the path,
    ergasterion.framework never imports ergasterion.translators (static source
    check, both directions of the one-way dependency the port map records),
    and the Translator base class's optional capabilities (deploy/conventions/
    detect_drift) carry their ported default behaviour.

Usage:
    python tests/python/test_framework_core.py
"""

from __future__ import annotations

import sys
import traceback
from pathlib import Path

# Allow direct execution as `python tests/python/test_framework_core.py`.
if __package__ in (None, ""):
    import os as _os, sys as _sys
    _sys.path.insert(0, _os.path.dirname(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))))

from ergasterion.estate import EstateContext, load_estate_adapters, load_translator_table
from ergasterion.framework import models as fw_models
from ergasterion.framework import translator_conformance as fw_conformance
from ergasterion.framework.models import (
    Capability,
    Edge,
    EdgeRole,
    ExecutionPlan,
    HandoffSchemaId,
    Occurrence,
    PatternId,
    Role,
    UnknownProfileError,
    UnsupportedProfileError,
    compute_plan_digest,
)
from ergasterion.framework.patterns import PATTERN_DISPLAY_NAMES, PROFILE_NAMES
from ergasterion.framework.resolver import resolve
from ergasterion.framework.routing import (
    DuplicateExecutionOwnerError,
    DuplicateTargetNameError,
    ForeignCapabilityError,
    MissingCapabilityError,
    MissingTranslatorTableEntryError,
    RouteAssignment,
    TableRoutingArgumentError,
    TranslationRouter,
    UndeclaredAttachmentError,
    UnregisteredTableTranslatorError,
)
from ergasterion.framework.translator_conformance import FakeTranslator, check_translator_conformance, load_vectors
from ergasterion.source_delivery import TypedDeclarations
import ergasterion.translators as ergasterion_translators
from ergasterion.translators.base import ConventionsDocument, DriftReport, Translator
from ergasterion.translators.dbt import DbtTranslator
from ergasterion.translators.local_ingestion import (
    LOCAL_INGESTION_PATTERNS,
    LOCAL_INGESTION_SHAPES,
    LocalIngestionTranslator,
)
from ergasterion.translators.publication import PublicationTranslator

FIXTURES_DIR = Path(__file__).resolve().parent.parent / "fixtures"
FRAMEWORK_DIR = Path(__file__).resolve().parent.parent.parent / "ergasterion" / "framework"

# The exact normative landing graph, under the landing.* occurrence ids the
# portable IDL declares. Kept independently of resolver.py's own literal so a
# regression in the resolver's construction cannot silently agree with itself.
EXPECTED_OCCURRENCE_ROLES = {
    "landing.checkpoint": (PatternId.CHECKPOINT_RETRIES, (Role.WRAPPER, Role.POLICY)),
    "landing.ingest": (PatternId.BATCH_INGESTION, (Role.PHASE,)),
    "landing.validate": (PatternId.DATA_VALIDATION, (Role.PHASE,)),
    "landing.contract": (PatternId.DATA_CONTRACTS, (Role.POLICY, Role.BARRIER)),
    "landing.schema": (PatternId.SCHEMA_PUBLISH, (Role.OBSERVER, Role.BARRIER)),
    "landing.publish": (PatternId.DATA_PUBLISH, (Role.BARRIER,)),
    "landing.lineage": (PatternId.LINEAGE_CAPTURE, (Role.OBSERVER,)),
    "landing.metadata": (PatternId.METADATA_CAPTURE, (Role.OBSERVER,)),
}

EXPECTED_EDGES = {
    ("landing.ingest", "landing.validate"): (EdgeRole.DATA, HandoffSchemaId.RAW_EVIDENCE),
    ("landing.validate", "landing.contract"): (EdgeRole.VALIDATION, HandoffSchemaId.VALIDATION_RESULT),
    ("landing.contract", "landing.schema"): (EdgeRole.READINESS, HandoffSchemaId.CONTRACT_CONFORMANCE),
    ("landing.validate", "landing.publish"): (EdgeRole.BARRIER, HandoffSchemaId.VALIDATION_RESULT),
    ("landing.contract", "landing.publish"): (EdgeRole.BARRIER, HandoffSchemaId.CONTRACT_CONFORMANCE),
    ("landing.schema", "landing.publish"): (EdgeRole.BARRIER, HandoffSchemaId.INTERFACE_READINESS),
    ("landing.ingest", "landing.lineage"): (EdgeRole.OBSERVE, HandoffSchemaId.RAW_EVIDENCE),
    ("landing.validate", "landing.lineage"): (EdgeRole.OBSERVE, HandoffSchemaId.VALIDATION_RESULT),
    ("landing.publish", "landing.lineage"): (EdgeRole.OBSERVE, HandoffSchemaId.PUBLICATION_CONFIRMATION),
    ("landing.contract", "landing.metadata"): (EdgeRole.OBSERVE, HandoffSchemaId.CONTRACT_CONFORMANCE),
    ("landing.validate", "landing.metadata"): (EdgeRole.OBSERVE, HandoffSchemaId.VALIDATION_RESULT),
    ("landing.publish", "landing.metadata"): (EdgeRole.OBSERVE, HandoffSchemaId.PUBLICATION_CONFIRMATION),
}

GOOD_ORDER = (
    "landing.checkpoint",
    "landing.ingest",
    "landing.validate",
    "landing.contract",
    "landing.schema",
    "landing.publish",
    "landing.lineage",
    "landing.metadata",
)


# --------------------------------------------------------------------------- tests


def test_registry_classifies_all_fifteen_with_no_legacy_alias() -> None:
    assert len(PatternId) == 15, "the registry identity enum must carry exactly fifteen tokens"
    assert set(PATTERN_DISPLAY_NAMES) == set(PatternId), "every canonical pattern must have display text"
    # No legacy product-type vocabulary anywhere in the registry namespace.
    legacy_tokens = {"origin", "foundation-base", "foundation-feature", "consumption", "foundation_base", "foundation_feature"}
    pattern_values = {p.value for p in PatternId}
    assert not (legacy_tokens & pattern_values)


def test_landing_profile_resolves_the_exact_normative_graph() -> None:
    plan = resolve("landing")
    assert plan.profile == "landing"
    assert {o.occurrence_id for o in plan.occurrences} == set(EXPECTED_OCCURRENCE_ROLES)
    for occurrence in plan.occurrences:
        expected_pattern, expected_roles = EXPECTED_OCCURRENCE_ROLES[occurrence.occurrence_id]
        assert occurrence.pattern_id is expected_pattern, occurrence.occurrence_id
        assert occurrence.roles == expected_roles, occurrence.occurrence_id
        assert occurrence.execution_owner_required is True, occurrence.occurrence_id

    got_edges = {(e.source, e.target): (e.edge_role, e.handoff_schema_id) for e in plan.edges}
    assert got_edges == EXPECTED_EDGES
    assert len(plan.edges) == 12

    assert plan.wrapper_id == "landing.checkpoint"
    assert plan.wrapper_members == tuple(sorted(i for i in EXPECTED_OCCURRENCE_ROLES if i != "landing.checkpoint"))

    # Deterministic, reproducible digest.
    digest_a = compute_plan_digest(plan)
    digest_b = compute_plan_digest(resolve("landing"))
    assert digest_a == digest_b and len(digest_a) == 64


def test_landing_plan_digest_changes_on_exactly_one_axis() -> None:
    # The digest document's shape is identical to the pre-profile Bronze
    # digest document except for one axis: "layer": "bronze" becomes
    # "profile": "landing". Everything else -- occurrences, edges,
    # wrapper_id, wrapper_members -- is unchanged.
    plan = resolve("landing")
    document = fw_models._plan_digest_document(plan)
    assert set(document) == {"schema", "profile", "occurrences", "edges", "wrapper_id", "wrapper_members"}
    assert "layer" not in document
    assert document["schema"] == "ergasterion.execution-graph-shape/v1"
    assert document["profile"] == "landing"
    assert document["wrapper_id"] == "landing.checkpoint"
    assert document["wrapper_members"] == list(plan.wrapper_members)
    assert len(document["occurrences"]) == 8
    assert len(document["edges"]) == 12


def test_non_landing_profiles_fail_unsupported_profile_deterministically() -> None:
    non_landing = [name for name in PROFILE_NAMES if name != "landing"]
    assert non_landing == ["integration", "derivation", "consolidation", "serving"]
    for name in non_landing:
        for _ in range(2):  # deterministic across repeated calls
            try:
                resolve(name)
            except UnsupportedProfileError as exc:
                assert exc.code == "unsupported_profile"
                assert exc.profile_name == name
            else:
                raise AssertionError(f"expected UnsupportedProfileError for profile {name!r}")


def test_resolve_rejects_an_unknown_profile_name() -> None:
    for bad_name in ("bogus", "bronze", "silver", "gold", 42, None):
        try:
            resolve(bad_name)
        except UnknownProfileError as exc:
            assert exc.code == "unknown_profile"
            assert exc.profile_name == bad_name
        else:
            raise AssertionError(f"expected UnknownProfileError for {bad_name!r}")


def test_translator_conformance_fixture_vectors() -> None:
    vectors = load_vectors(FIXTURES_DIR / "translator_conformance.json")
    expected_ids = {
        "valid_single_owner": None,
        "missing_execution_owner": "missing_execution_owner",
        "duplicate_execution_owner": "duplicate_execution_owner",
        "reordered_ownership": "reordered_ownership",
        "digest_mismatch": "digest_mismatch",
        "bad_handoff": "bad_handoff",
        "observe_only_translator": None,
        "table_valid_two_translators_two_adapters": None,
        "table_missing_translator_table_entry": "missing_translator_table_entry",
        "table_missing_capability": "missing_capability",
    }
    assert {v.vector_id for v in vectors} == set(expected_ids), "fixture must carry exactly the ten named vectors"

    plan = resolve("landing")
    outcomes = fw_conformance.run_all(plan, vectors)
    failures = [o for o in outcomes if not o.passed]
    assert not failures, f"conformance vectors failed: {failures}"
    for vector in vectors:
        assert vector.expected_error_code == expected_ids[vector.vector_id]


def test_router_rejects_undeclared_attachments() -> None:
    plan = resolve("landing")
    all_ids = tuple(o.occurrence_id for o in plan.occurrences)

    unknown = FakeTranslator("ghost", frozenset(all_ids) | {"landing.nonexistent"}, _order=GOOD_ORDER + ("landing.nonexistent",))
    try:
        check_translator_conformance(plan, [unknown])
    except UndeclaredAttachmentError as exc:
        assert exc.code == "undeclared_attachment"
    else:
        raise AssertionError("expected undeclared_attachment for an unknown occurrence reference")

    overlapping = FakeTranslator(
        "confused",
        frozenset(all_ids),
        _observed=frozenset({"landing.publish"}),
        _order=GOOD_ORDER,
    )
    try:
        check_translator_conformance(plan, [overlapping])
    except UndeclaredAttachmentError as exc:
        assert exc.code == "undeclared_attachment"
    else:
        raise AssertionError("expected undeclared_attachment for an owned/observed overlap")


def test_router_composes_translations_in_plan_order() -> None:
    plan = resolve("landing")
    all_ids = tuple(o.occurrence_id for o in plan.occurrences)
    translator = FakeTranslator("local_ingestion", frozenset(all_ids), _order=GOOD_ORDER)
    result = check_translator_conformance(plan, [translator])
    assert [a.occurrence_id for a in result.assignments] == sorted(all_ids)
    assert all(a.translator_name == "local_ingestion" for a in result.assignments)
    assert "local_ingestion" in result.translations


def test_router_calls_translate_for_observe_only_translator() -> None:
    # Pins the observe_only_translator fixture vector's behaviour directly:
    # a translator that owns nothing but observes an occurrence still has
    # translate() called, and its artefacts are not dropped from
    # RoutingResult.translations.
    plan = resolve("landing")
    all_ids = tuple(o.occurrence_id for o in plan.occurrences)
    owner = FakeTranslator("local_ingestion", frozenset(all_ids), _order=GOOD_ORDER)
    observer = FakeTranslator("docs_projection", frozenset(), _observed=frozenset({"landing.publish"}))
    result = check_translator_conformance(plan, [owner, observer])
    assert "local_ingestion" in result.translations
    assert "docs_projection" in result.translations
    assert result.translations["docs_projection"].metadata["target_name"] == "docs_projection"


def test_router_skips_unattached_translator_without_error() -> None:
    # A translator that owns and observes nothing is excluded from
    # RoutingResult.translations: translate() is never called on it, and the
    # router raises no error for it.
    plan = resolve("landing")
    all_ids = tuple(o.occurrence_id for o in plan.occurrences)
    owner = FakeTranslator("local_ingestion", frozenset(all_ids), _order=GOOD_ORDER)
    unattached = FakeTranslator("future_target", frozenset())
    result = check_translator_conformance(plan, [owner, unattached])
    assert "local_ingestion" in result.translations
    assert "future_target" not in result.translations
    assert [a.translator_name for a in result.assignments] == ["local_ingestion"] * len(all_ids)


def test_router_rejects_duplicate_target_name() -> None:
    plan = resolve("landing")
    all_ids = tuple(o.occurrence_id for o in plan.occurrences)
    first = FakeTranslator("local_ingestion", frozenset(all_ids), _order=GOOD_ORDER)
    second = FakeTranslator("local_ingestion", frozenset(), _observed=frozenset({"landing.publish"}))
    try:
        check_translator_conformance(plan, [first, second])
    except DuplicateTargetNameError as exc:
        assert exc.code == "duplicate_target_name"
    else:
        raise AssertionError("expected duplicate_target_name for two translators sharing a target_name")


def test_router_rejects_duplicate_owner_even_when_not_required() -> None:
    # A synthetic single-occurrence plan with execution_owner_required=False:
    # the duplicate-owner check must still fire ahead of the
    # execution_owner_required short-circuit.
    occurrence = Occurrence("solo.step", PatternId.LINEAGE_CAPTURE, (Role.OBSERVER,), False)
    plan = ExecutionPlan(
        profile="landing",
        occurrences=(occurrence,),
        edges=(),
        wrapper_id="solo.step",
        wrapper_members=(),
    )
    first = FakeTranslator("a", frozenset({"solo.step"}), _order=("solo.step",))
    second = FakeTranslator("b", frozenset({"solo.step"}), _order=("solo.step",))
    try:
        check_translator_conformance(plan, [first, second])
    except DuplicateExecutionOwnerError as exc:
        assert exc.code == "duplicate_execution_owner"
    else:
        raise AssertionError(
            "expected duplicate_execution_owner for a non-required occurrence with two owners"
        )


def test_single_owner_non_required_occurrence_still_recorded_in_assignments() -> None:
    # A single-owner occurrence with execution_owner_required=False must still
    # reach RoutingResult.assignments: ownership is legitimate, so the router
    # records it before the execution_owner_required short-circuit, rather
    # than silently dropping it.
    occurrence = Occurrence("solo.step", PatternId.LINEAGE_CAPTURE, (Role.OBSERVER,), False)
    plan = ExecutionPlan(
        profile="landing",
        occurrences=(occurrence,),
        edges=(),
        wrapper_id="solo.step",
        wrapper_members=(),
    )
    translator = FakeTranslator("a", frozenset({"solo.step"}), _order=("solo.step",))
    result = check_translator_conformance(plan, [translator])
    assert [a.occurrence_id for a in result.assignments] == ["solo.step"]
    assert result.assignments[0].translator_name == "a"


class _StubTranslator(Translator):
    """The minimal concrete Translator used to prove the ported optional-
    capability defaults (deploy/detect_drift/conventions)."""

    @property
    def target_name(self) -> str:
        return "stub"

    def owned_occurrences(self) -> frozenset[str]:
        return frozenset()

    def execution_order(self) -> tuple[str, ...]:
        return ()

    def validate_compatibility(self, plan: ExecutionPlan) -> list[str]:
        return []

    def translate(self, plan: ExecutionPlan):
        return fw_models.TranslationResult(artefacts={"stub.txt": "same"})


def test_translator_optional_capabilities_carry_ported_defaults() -> None:
    stub = _StubTranslator()
    plan = resolve("landing")

    validation = stub.validate(plan, stub.translate(plan))
    assert validation.passed is True and validation.findings == ()

    try:
        stub.deploy(stub.translate(plan), "dev")
    except NotImplementedError:
        pass
    else:
        raise AssertionError("expected NotImplementedError from the default deploy()")

    drift = stub.detect_drift(plan, {"stub.txt": "different"})
    assert isinstance(drift, DriftReport)
    assert drift.has_drift is True and drift.drifted_artefacts == ("stub.txt",)

    conventions = stub.conventions()
    assert isinstance(conventions, ConventionsDocument)
    assert conventions.target == "stub" and conventions.idioms == ""


def test_package_import_cleanliness_and_one_way_dependency() -> None:
    # ergasterion.framework and ergasterion.translators are already imported at
    # module load time above; re-importing here proves no import-time side
    # effect requires a platform package.
    assert ergasterion_translators.Translator is Translator

    # The real proof that ergasterion.framework and ergasterion.translators
    # import without a dbt, DuckDB, SQLite or orchestrator package on the path
    # is the static source scan below, over every *.py file in both packages.
    framework_files = sorted(FRAMEWORK_DIR.glob("*.py"))
    assert len(framework_files) >= 5, "expected the full framework/ module set to exist"
    translators_dir = FRAMEWORK_DIR.parent / "translators"
    translators_files = sorted(translators_dir.glob("*.py"))
    assert len(translators_files) >= 1, "expected the translators/ module set to exist"

    one_way_dependency_import_lines = (
        "import ergasterion.translators",
        "from ergasterion.translators",
    )
    platform_import_lines = (
        "import duckdb",
        "from duckdb",
        "import sqlite3",
        "from sqlite3",
        "import dbt",
        "from dbt",
        "import airflow",
        "from airflow",
    )
    for path in framework_files:
        for line in path.read_text(encoding="utf-8").splitlines():
            stripped = line.strip()
            for banned in one_way_dependency_import_lines + platform_import_lines:
                assert not stripped.startswith(banned), f"{path} has a forbidden import line: {stripped!r}"
    for path in translators_files:
        for line in path.read_text(encoding="utf-8").splitlines():
            stripped = line.strip()
            for banned in platform_import_lines:
                assert not stripped.startswith(banned), f"{path} has a forbidden import line: {stripped!r}"

    translators_base = (translators_dir / "base.py").read_text(encoding="utf-8")
    assert "ergasterion.framework" in translators_base, "translators/base.py must depend on the framework"


# --------------------------------------------------------------------------- two-axis capabilities and the
# estate translator table


def test_translator_default_capabilities_is_empty() -> None:
    # The ported optional-capability default (base.py's Translator ABC):
    # a translator that never overrides capabilities() declares none.
    stub = _StubTranslator()
    assert stub.capabilities() == frozenset()


def test_real_translators_declare_the_bronze_label_split() -> None:
    # Ground truth: local-ingestion owns the four runtime patterns and the
    # publication translator owns the four publication-shaped patterns, both
    # across both adapters -- the exact split estate.yml's bronze translator
    # table names.
    local = LocalIngestionTranslator()
    assert LOCAL_INGESTION_PATTERNS == (
        PatternId.BATCH_INGESTION,
        PatternId.DATA_VALIDATION,
        PatternId.DATA_PUBLISH,
        PatternId.CHECKPOINT_RETRIES,
    )
    # Beside the four patterns it declares the shape it renders: a landing
    # product brings its source in unchanged, so the relations it publishes
    # are the ones its composition produces, and the estate's table names
    # this translator for that shape wherever it owns the landing label.
    assert LOCAL_INGESTION_SHAPES == ("declared",)
    local_caps = local.capabilities()
    assert len(local_caps) == 10, local_caps
    for token in (*(pattern.value for pattern in LOCAL_INGESTION_PATTERNS), *LOCAL_INGESTION_SHAPES):
        for adapter in ("duckdb", "bigquery"):
            assert Capability(token, "local-ingestion", adapter) in local_caps

    # The publication translator owns all four publication-shaped patterns of
    # the landing route, on every declared adapter.
    from ergasterion.translators.publication import PUBLICATION_PATTERNS, PublicationTranslator

    publication_caps = PublicationTranslator().capabilities()
    assert PUBLICATION_PATTERNS == (
        PatternId.DATA_CONTRACTS,
        PatternId.SCHEMA_PUBLISH,
        PatternId.METADATA_CAPTURE,
        PatternId.LINEAGE_CAPTURE,
    )
    assert len(publication_caps) == 8, publication_caps
    for pattern in PUBLICATION_PATTERNS:
        for adapter in ("duckdb", "bigquery"):
            assert Capability(pattern.value, "publication", adapter) in publication_caps

    # dbt registers no capability of its own: every pattern and shape the
    # estate's table names it for belongs to the SQL-model route, which
    # registers them as it lands. The two translators therefore share none.
    dbt = DbtTranslator(typed=TypedDeclarations(tables={}, estate_namespace="fixture"), bound={})
    dbt_caps = dbt.capabilities()
    assert dbt_caps == frozenset(), dbt_caps
    assert dbt_caps.isdisjoint(publication_caps)
    assert local_caps.isdisjoint(publication_caps)

    # Every capability names its own translator (the coherence invariant the
    # router's ForeignCapabilityError enforces).
    assert all(c.translator == "local-ingestion" for c in local_caps)
    assert all(c.translator == "publication" for c in publication_caps)


def test_estate_yml_declares_duckdb_reference_bigquery_deployment_and_final_target() -> None:
    ctx = EstateContext.default()
    adapters = load_estate_adapters(ctx.estate_file)
    assert adapters.names() == ("duckdb", "bigquery") or set(adapters.names()) == {"duckdb", "bigquery"}
    assert adapters.kind_of("duckdb") == "reference"
    assert adapters.kind_of("bigquery") == "deployment"
    assert adapters.final_target == "bigquery"


def test_estate_yml_translator_table_names_the_real_bronze_split() -> None:
    ctx = EstateContext.default()
    table = load_translator_table(ctx.estate_file)
    assert table["bronze"] == {
        "batch_ingestion": "local-ingestion",
        "data_validation": "local-ingestion",
        "data_publish": "local-ingestion",
        "checkpoint_retries": "local-ingestion",
        "data_contracts": "publication",
        "schema_publish": "publication",
        "metadata_capture": "publication",
        "lineage_capture": "publication",
    }
    # Silver and gold name the publication translator for the same four
    # patterns, dbt for every remaining pattern their admitted profiles
    # carry plus the one registered shape -- present as data even though
    # dbt has not registered those capabilities yet.
    for label in ("silver", "gold"):
        assert table[label]["data_contracts"] == "publication"
        assert table[label]["schema_publish"] == "publication"
        assert table[label]["metadata_capture"] == "publication"
        assert table[label]["lineage_capture"] == "publication"
        assert table[label]["declared"] == "dbt"


def test_todays_real_landing_route_resolves_through_the_table() -> None:
    # The acceptance clause this proves: "today's real landing route must
    # keep working" -- routed through the NEW table-driven mechanism, using
    # the REAL translators and the REAL estate.yml, not fixtures standing in
    # for them. All four publication-shaped patterns, lineage_capture
    # included, route to the publication translator.
    ctx = EstateContext.default()
    table = load_translator_table(ctx.estate_file)
    adapters = load_estate_adapters(ctx.estate_file)
    plan = resolve("landing")
    local = LocalIngestionTranslator()
    dbt = DbtTranslator(typed=TypedDeclarations(tables={}, estate_namespace="fixture"), bound={})
    publication = PublicationTranslator()

    router = TranslationRouter(plan, [local, dbt, publication])
    result = router.route(label="bronze", translator_table=table, adapters=adapters.names())

    expected_owner = {
        "landing.checkpoint": "local-ingestion",
        "landing.ingest": "local-ingestion",
        "landing.validate": "local-ingestion",
        "landing.publish": "local-ingestion",
        "landing.contract": "publication",
        "landing.schema": "publication",
        "landing.metadata": "publication",
        "landing.lineage": "publication",
    }
    assert len(result.assignments) == 8 * len(adapters.names())
    seen: dict[str, set[str]] = {}
    for assignment in result.assignments:
        assert assignment.translator_name == expected_owner[assignment.occurrence_id]
        seen.setdefault(assignment.occurrence_id, set()).add(assignment.adapter)
    assert all(seen[occurrence_id] == set(adapters.names()) for occurrence_id in expected_owner)
    assert set(result.translations) == {"local-ingestion", "publication"}


def test_silver_and_gold_products_cannot_route_yet_as_the_table_expects() -> None:
    # The silver and gold labels' entries name dbt for the transformation
    # patterns and shapes, and dbt registers no capability until its
    # SQL-model route lands, so routing a product under either label fails
    # closed with missing_capability, never a silent pass-through.
    ctx = EstateContext.default()
    table = load_translator_table(ctx.estate_file)
    adapters = load_estate_adapters(ctx.estate_file)
    local = LocalIngestionTranslator()
    dbt = DbtTranslator(typed=TypedDeclarations(tables={}, estate_namespace="fixture"), bound={})

    for label, pattern in (("silver", PatternId.SCHEMA_TRANSFORM), ("gold", PatternId.DATA_CURATION)):
        occurrence = Occurrence("p.step", pattern, (Role.PHASE,), True)
        plan = ExecutionPlan(
            profile="integration" if label == "silver" else "serving",
            occurrences=(occurrence,),
            edges=(),
            wrapper_id="p.step",
            wrapper_members=(),
        )
        router = TranslationRouter(plan, [local, dbt])
        try:
            router.route(label=label, translator_table=table, adapters=adapters.names())
        except MissingCapabilityError as exc:
            assert exc.label == label
            assert exc.translator_name == "dbt"
        else:
            raise AssertionError(f"expected missing_capability routing a {label!r} product")


def test_route_assignment_ordering_is_occurrence_then_caller_adapter_order() -> None:
    # RoutingResult.assignments docstring: occurrence_id order first (the
    # plan's own canonical order), then, within one occurrence, the caller's
    # adapters sequence in the exact order given -- asserted directly here,
    # with the adapters passed in a deliberately non-alphabetical order.
    occurrence_a = Occurrence("a.step", PatternId.LINEAGE_CAPTURE, (Role.OBSERVER,), True)
    occurrence_b = Occurrence("b.step", PatternId.METADATA_CAPTURE, (Role.OBSERVER,), True)
    plan = ExecutionPlan(
        profile="landing",
        occurrences=(occurrence_a, occurrence_b),
        edges=(),
        wrapper_id="a.step",
        wrapper_members=("b.step",),
    )
    translator = FakeTranslator(
        "solo",
        frozenset(),
        _capabilities=frozenset(
            {
                Capability("lineage_capture", "solo", "zeta"),
                Capability("lineage_capture", "solo", "alpha"),
                Capability("metadata_capture", "solo", "zeta"),
                Capability("metadata_capture", "solo", "alpha"),
            }
        ),
    )
    table = {"bronze": {"lineage_capture": "solo", "metadata_capture": "solo"}}
    router = TranslationRouter(plan, [translator])
    result = router.route(label="bronze", translator_table=table, adapters=("zeta", "alpha"))
    assert result.assignments == (
        RouteAssignment("a.step", "solo", "zeta"),
        RouteAssignment("a.step", "solo", "alpha"),
        RouteAssignment("b.step", "solo", "zeta"),
        RouteAssignment("b.step", "solo", "alpha"),
    )


def test_route_rejects_partial_table_arguments() -> None:
    plan = resolve("landing")
    router = TranslationRouter(plan, [])
    for kwargs in (
        {"label": "bronze"},
        {"translator_table": {}},
        {"adapters": ("duckdb",)},
        {"label": "bronze", "adapters": ("duckdb",)},
    ):
        try:
            router.route(**kwargs)
        except TableRoutingArgumentError as exc:
            assert exc.code == "table_routing_argument_error"
        else:
            raise AssertionError(f"expected table_routing_argument_error for {kwargs!r}")


_SOLO_OCCURRENCE = Occurrence("solo.step", PatternId.LINEAGE_CAPTURE, (Role.OBSERVER,), True)
_SOLO_PLAN = ExecutionPlan(
    profile="landing", occurrences=(_SOLO_OCCURRENCE,), edges=(), wrapper_id="solo.step", wrapper_members=()
)


def test_route_by_table_fails_closed_on_missing_table_entry() -> None:
    router = TranslationRouter(_SOLO_PLAN, [FakeTranslator("a", frozenset())])
    try:
        router.route(label="bronze", translator_table={"bronze": {}}, adapters=("duckdb", "bigquery"))
    except MissingTranslatorTableEntryError as exc:
        assert exc.code == "missing_translator_table_entry"
        assert exc.label == "bronze"
        assert exc.pattern_or_shape == "lineage_capture"
        assert exc.adapters == ("duckdb", "bigquery")
        assert "bronze" in str(exc) and "lineage_capture" in str(exc) and "duckdb" in str(exc)
    else:
        raise AssertionError("expected missing_translator_table_entry")


def test_route_by_table_fails_closed_on_missing_capability() -> None:
    translator = FakeTranslator(
        "a", frozenset(), _capabilities=frozenset({Capability("lineage_capture", "a", "duckdb")})
    )
    router = TranslationRouter(_SOLO_PLAN, [translator])
    table = {"bronze": {"lineage_capture": "a"}}
    try:
        router.route(label="bronze", translator_table=table, adapters=("duckdb", "bigquery"))
    except MissingCapabilityError as exc:
        assert exc.code == "missing_capability"
        assert (exc.label, exc.pattern_or_shape, exc.translator_name, exc.adapter) == (
            "bronze",
            "lineage_capture",
            "a",
            "bigquery",
        )
    else:
        raise AssertionError("expected missing_capability")


def test_route_by_table_rejects_an_unregistered_translator() -> None:
    router = TranslationRouter(_SOLO_PLAN, [FakeTranslator("a", frozenset())])
    table = {"bronze": {"lineage_capture": "ghost"}}
    try:
        router.route(label="bronze", translator_table=table, adapters=("duckdb",))
    except UnregisteredTableTranslatorError as exc:
        assert exc.code == "unregistered_table_translator"
        assert exc.translator_name == "ghost"
    else:
        raise AssertionError("expected unregistered_table_translator")


def test_route_by_table_rejects_a_foreign_capability() -> None:
    # Translator "a" declares a capability naming translator "b" -- a
    # coherence bug the router must reject rather than trust.
    translator = FakeTranslator(
        "a", frozenset(), _capabilities=frozenset({Capability("lineage_capture", "b", "duckdb")})
    )
    router = TranslationRouter(_SOLO_PLAN, [translator])
    table = {"bronze": {"lineage_capture": "a"}}
    try:
        router.route(label="bronze", translator_table=table, adapters=("duckdb",))
    except ForeignCapabilityError as exc:
        assert exc.code == "foreign_capability"
        assert exc.translator_name == "a"
        assert exc.claimed_translator == "b"
    else:
        raise AssertionError("expected foreign_capability")


def test_changing_a_table_entry_changes_the_owner_with_no_engine_change() -> None:
    # Architecture check 12: changing a table entry changes the owner with
    # no engine change. Two translators, "a" and "b", both register the
    # SAME capability; only the table decides which one owns the occurrence.
    translator_a = FakeTranslator(
        "a", frozenset(), _capabilities=frozenset({Capability("lineage_capture", "a", "duckdb")})
    )
    translator_b = FakeTranslator(
        "b", frozenset(), _capabilities=frozenset({Capability("lineage_capture", "b", "duckdb")})
    )
    router = TranslationRouter(_SOLO_PLAN, [translator_a, translator_b])

    result_a = router.route(label="bronze", translator_table={"bronze": {"lineage_capture": "a"}}, adapters=("duckdb",))
    assert [assignment.translator_name for assignment in result_a.assignments] == ["a"]

    result_b = router.route(label="bronze", translator_table={"bronze": {"lineage_capture": "b"}}, adapters=("duckdb",))
    assert [assignment.translator_name for assignment in result_b.assignments] == ["b"]


TESTS = [
    test_registry_classifies_all_fifteen_with_no_legacy_alias,
    test_landing_profile_resolves_the_exact_normative_graph,
    test_landing_plan_digest_changes_on_exactly_one_axis,
    test_non_landing_profiles_fail_unsupported_profile_deterministically,
    test_resolve_rejects_an_unknown_profile_name,
    test_translator_conformance_fixture_vectors,
    test_router_rejects_undeclared_attachments,
    test_router_composes_translations_in_plan_order,
    test_router_calls_translate_for_observe_only_translator,
    test_router_skips_unattached_translator_without_error,
    test_router_rejects_duplicate_target_name,
    test_router_rejects_duplicate_owner_even_when_not_required,
    test_single_owner_non_required_occurrence_still_recorded_in_assignments,
    test_translator_optional_capabilities_carry_ported_defaults,
    test_package_import_cleanliness_and_one_way_dependency,
    test_translator_default_capabilities_is_empty,
    test_real_translators_declare_the_bronze_label_split,
    test_estate_yml_declares_duckdb_reference_bigquery_deployment_and_final_target,
    test_estate_yml_translator_table_names_the_real_bronze_split,
    test_todays_real_landing_route_resolves_through_the_table,
    test_silver_and_gold_products_cannot_route_yet_as_the_table_expects,
    test_route_assignment_ordering_is_occurrence_then_caller_adapter_order,
    test_route_rejects_partial_table_arguments,
    test_route_by_table_fails_closed_on_missing_table_entry,
    test_route_by_table_fails_closed_on_missing_capability,
    test_route_by_table_rejects_an_unregistered_translator,
    test_route_by_table_rejects_a_foreign_capability,
    test_changing_a_table_entry_changes_the_owner_with_no_engine_change,
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
