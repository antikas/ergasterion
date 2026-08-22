"""Assert-script tests for ergasterion/framework/** and ergasterion/translators/**
(repo convention: no pytest).

Each test function proves one property of the framework core:
  - The registry classifies all fifteen canonical patterns using exactly Bronze/
    Silver/Gold vocabulary, with the exact Bronze mandatory/optional/forbidden
    split and no legacy product-type alias anywhere.
  - Bronze resolves the exact normative execution graph: occurrence identities,
    roles (sorted in role-token order), the wrapper's membership, and every
    edge with its role and handoff schema.
  - Batch Transfer (the sole optional pattern) never appears as a Bronze
    occurrence: it has no authoring surface, and resolves
    unsupported_optional_pattern. Every forbidden pattern likewise never
    appears.
  - Silver and Gold both raise UnsupportedLayerError with code
    unsupported_layer, deterministically, on every call.
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

from ergasterion.framework import models as fw_models
from ergasterion.framework import translator_conformance as fw_conformance
from ergasterion.framework.models import (
    Edge,
    EdgeRole,
    ExecutionPlan,
    HandoffSchemaId,
    InvalidLayerArgumentError,
    Layer,
    Occurrence,
    PatternDisposition,
    PatternId,
    Role,
    UnsupportedLayerError,
    compute_plan_digest,
)
from ergasterion.framework.patterns import (
    BRONZE_FORBIDDEN,
    BRONZE_MANDATORY,
    BRONZE_OPTIONAL,
    PATTERN_DISPLAY_NAMES,
    ResolutionStatus,
    classify_bronze,
    resolution_status,
)
from ergasterion.framework.resolver import resolve
from ergasterion.framework.routing import (
    DuplicateExecutionOwnerError,
    DuplicateTargetNameError,
    UndeclaredAttachmentError,
)
from ergasterion.framework.translator_conformance import FakeTranslator, check_translator_conformance, load_vectors
import ergasterion.translators as ergasterion_translators
from ergasterion.translators.base import ConventionsDocument, DriftReport, Translator

FIXTURES_DIR = Path(__file__).resolve().parent.parent / "fixtures"
FRAMEWORK_DIR = Path(__file__).resolve().parent.parent.parent / "ergasterion" / "framework"

# The exact normative Bronze graph. Kept independently of resolver.py's own
# literal so a regression in the resolver's construction cannot silently
# agree with itself.
EXPECTED_OCCURRENCE_ROLES = {
    "bronze.checkpoint": (PatternId.CHECKPOINT_RETRIES, (Role.WRAPPER, Role.POLICY)),
    "bronze.ingest": (PatternId.BATCH_INGESTION, (Role.PHASE,)),
    "bronze.validate": (PatternId.DATA_VALIDATION, (Role.PHASE,)),
    "bronze.contract": (PatternId.DATA_CONTRACTS, (Role.POLICY, Role.BARRIER)),
    "bronze.schema": (PatternId.SCHEMA_PUBLISH, (Role.OBSERVER, Role.BARRIER)),
    "bronze.publish": (PatternId.DATA_PUBLISH, (Role.BARRIER,)),
    "bronze.lineage": (PatternId.LINEAGE_CAPTURE, (Role.OBSERVER,)),
    "bronze.metadata": (PatternId.METADATA_CAPTURE, (Role.OBSERVER,)),
}

EXPECTED_EDGES = {
    ("bronze.ingest", "bronze.validate"): (EdgeRole.DATA, HandoffSchemaId.RAW_EVIDENCE),
    ("bronze.validate", "bronze.contract"): (EdgeRole.VALIDATION, HandoffSchemaId.VALIDATION_RESULT),
    ("bronze.contract", "bronze.schema"): (EdgeRole.READINESS, HandoffSchemaId.CONTRACT_CONFORMANCE),
    ("bronze.validate", "bronze.publish"): (EdgeRole.BARRIER, HandoffSchemaId.VALIDATION_RESULT),
    ("bronze.contract", "bronze.publish"): (EdgeRole.BARRIER, HandoffSchemaId.CONTRACT_CONFORMANCE),
    ("bronze.schema", "bronze.publish"): (EdgeRole.BARRIER, HandoffSchemaId.INTERFACE_READINESS),
    ("bronze.ingest", "bronze.lineage"): (EdgeRole.OBSERVE, HandoffSchemaId.RAW_EVIDENCE),
    ("bronze.validate", "bronze.lineage"): (EdgeRole.OBSERVE, HandoffSchemaId.VALIDATION_RESULT),
    ("bronze.publish", "bronze.lineage"): (EdgeRole.OBSERVE, HandoffSchemaId.PUBLICATION_CONFIRMATION),
    ("bronze.contract", "bronze.metadata"): (EdgeRole.OBSERVE, HandoffSchemaId.CONTRACT_CONFORMANCE),
    ("bronze.validate", "bronze.metadata"): (EdgeRole.OBSERVE, HandoffSchemaId.VALIDATION_RESULT),
    ("bronze.publish", "bronze.metadata"): (EdgeRole.OBSERVE, HandoffSchemaId.PUBLICATION_CONFIRMATION),
}

GOOD_ORDER = (
    "bronze.checkpoint",
    "bronze.ingest",
    "bronze.validate",
    "bronze.contract",
    "bronze.schema",
    "bronze.publish",
    "bronze.lineage",
    "bronze.metadata",
)


# --------------------------------------------------------------------------- tests


def test_registry_classifies_all_fifteen_with_no_legacy_alias() -> None:
    assert len(PatternId) == 15, "the registry identity enum must carry exactly fifteen tokens"
    assert set(PATTERN_DISPLAY_NAMES) == set(PatternId), "every canonical pattern must have display text"
    assert len(BRONZE_MANDATORY) == 8 and len(BRONZE_OPTIONAL) == 1 and len(BRONZE_FORBIDDEN) == 6
    assert BRONZE_MANDATORY | BRONZE_OPTIONAL | BRONZE_FORBIDDEN == set(PatternId)
    assert BRONZE_MANDATORY.isdisjoint(BRONZE_OPTIONAL)
    assert BRONZE_MANDATORY.isdisjoint(BRONZE_FORBIDDEN)
    assert BRONZE_OPTIONAL.isdisjoint(BRONZE_FORBIDDEN)
    for pattern_id in PatternId:
        assert classify_bronze(pattern_id) in (
            PatternDisposition.MANDATORY,
            PatternDisposition.OPTIONAL,
            PatternDisposition.FORBIDDEN,
        )
    # No legacy product-type vocabulary anywhere in the module namespaces.
    legacy_tokens = {"origin", "foundation-base", "foundation-feature", "consumption", "foundation_base", "foundation_feature"}
    layer_values = {layer.value for layer in Layer}
    assert layer_values == {"bronze", "silver", "gold"}
    assert not (legacy_tokens & layer_values)
    pattern_values = {p.value for p in PatternId}
    assert not (legacy_tokens & pattern_values)


def test_bronze_resolves_the_exact_normative_graph() -> None:
    plan = resolve(Layer.BRONZE)
    assert plan.layer is Layer.BRONZE
    assert {o.occurrence_id for o in plan.occurrences} == set(EXPECTED_OCCURRENCE_ROLES)
    for occurrence in plan.occurrences:
        expected_pattern, expected_roles = EXPECTED_OCCURRENCE_ROLES[occurrence.occurrence_id]
        assert occurrence.pattern_id is expected_pattern, occurrence.occurrence_id
        assert occurrence.roles == expected_roles, occurrence.occurrence_id
        assert occurrence.execution_owner_required is True, occurrence.occurrence_id

    got_edges = {(e.source, e.target): (e.edge_role, e.handoff_schema_id) for e in plan.edges}
    assert got_edges == EXPECTED_EDGES
    assert len(plan.edges) == 12

    assert plan.wrapper_id == "bronze.checkpoint"
    assert plan.wrapper_members == tuple(sorted(i for i in EXPECTED_OCCURRENCE_ROLES if i != "bronze.checkpoint"))

    # Deterministic, reproducible digest.
    digest_a = compute_plan_digest(plan)
    digest_b = compute_plan_digest(resolve(Layer.BRONZE))
    assert digest_a == digest_b and len(digest_a) == 64


def test_batch_transfer_has_no_authoring_surface_and_forbidden_never_occurs() -> None:
    assert resolution_status(PatternId.BATCH_TRANSFER) == ResolutionStatus.UNSUPPORTED_OPTIONAL_PATTERN
    plan = resolve(Layer.BRONZE)
    occurring_patterns = {o.pattern_id for o in plan.occurrences}
    assert PatternId.BATCH_TRANSFER not in occurring_patterns
    for forbidden in BRONZE_FORBIDDEN:
        assert resolution_status(forbidden) == ResolutionStatus.FORBIDDEN
        assert forbidden not in occurring_patterns
    for mandatory in BRONZE_MANDATORY:
        assert resolution_status(mandatory) == ResolutionStatus.IN_BRONZE_GRAPH
        assert mandatory in occurring_patterns


def test_silver_and_gold_fail_unsupported_layer_deterministically() -> None:
    for layer in (Layer.SILVER, Layer.GOLD):
        for _ in range(2):  # deterministic across repeated calls
            try:
                resolve(layer)
            except UnsupportedLayerError as exc:
                assert exc.code == "unsupported_layer"
                assert exc.layer is layer
            else:
                raise AssertionError(f"expected UnsupportedLayerError for layer {layer.value!r}")


def test_resolve_rejects_a_non_layer_argument() -> None:
    # A plain string equal to Layer.BRONZE.value fails the `layer is
    # Layer.BRONZE` identity check in resolve(); it must raise
    # InvalidLayerArgumentError with its own code, ahead of
    # UnsupportedLayerError's constructor, which calls .value and requires a
    # real Layer member.
    try:
        resolve("bronze")
    except InvalidLayerArgumentError as exc:
        assert exc.code == "invalid_layer_argument"
        assert exc.layer == "bronze"
    else:
        raise AssertionError("expected InvalidLayerArgumentError for a plain string argument")


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
    }
    assert {v.vector_id for v in vectors} == set(expected_ids), "fixture must carry exactly the seven named vectors"

    plan = resolve(Layer.BRONZE)
    outcomes = fw_conformance.run_all(plan, vectors)
    failures = [o for o in outcomes if not o.passed]
    assert not failures, f"conformance vectors failed: {failures}"
    for vector in vectors:
        assert vector.expected_error_code == expected_ids[vector.vector_id]


def test_router_rejects_undeclared_attachments() -> None:
    plan = resolve(Layer.BRONZE)
    all_ids = tuple(o.occurrence_id for o in plan.occurrences)

    unknown = FakeTranslator("ghost", frozenset(all_ids) | {"bronze.nonexistent"}, _order=GOOD_ORDER + ("bronze.nonexistent",))
    try:
        check_translator_conformance(plan, [unknown])
    except UndeclaredAttachmentError as exc:
        assert exc.code == "undeclared_attachment"
    else:
        raise AssertionError("expected undeclared_attachment for an unknown occurrence reference")

    overlapping = FakeTranslator(
        "confused",
        frozenset(all_ids),
        _observed=frozenset({"bronze.publish"}),
        _order=GOOD_ORDER,
    )
    try:
        check_translator_conformance(plan, [overlapping])
    except UndeclaredAttachmentError as exc:
        assert exc.code == "undeclared_attachment"
    else:
        raise AssertionError("expected undeclared_attachment for an owned/observed overlap")


def test_router_composes_translations_in_plan_order() -> None:
    plan = resolve(Layer.BRONZE)
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
    plan = resolve(Layer.BRONZE)
    all_ids = tuple(o.occurrence_id for o in plan.occurrences)
    owner = FakeTranslator("local_ingestion", frozenset(all_ids), _order=GOOD_ORDER)
    observer = FakeTranslator("docs_projection", frozenset(), _observed=frozenset({"bronze.publish"}))
    result = check_translator_conformance(plan, [owner, observer])
    assert "local_ingestion" in result.translations
    assert "docs_projection" in result.translations
    assert result.translations["docs_projection"].metadata["target_name"] == "docs_projection"


def test_router_skips_unattached_translator_without_error() -> None:
    # A translator that owns and observes nothing is excluded from
    # RoutingResult.translations: translate() is never called on it, and the
    # router raises no error for it.
    plan = resolve(Layer.BRONZE)
    all_ids = tuple(o.occurrence_id for o in plan.occurrences)
    owner = FakeTranslator("local_ingestion", frozenset(all_ids), _order=GOOD_ORDER)
    unattached = FakeTranslator("future_target", frozenset())
    result = check_translator_conformance(plan, [owner, unattached])
    assert "local_ingestion" in result.translations
    assert "future_target" not in result.translations
    assert [a.translator_name for a in result.assignments] == ["local_ingestion"] * len(all_ids)


def test_router_rejects_duplicate_target_name() -> None:
    plan = resolve(Layer.BRONZE)
    all_ids = tuple(o.occurrence_id for o in plan.occurrences)
    first = FakeTranslator("local_ingestion", frozenset(all_ids), _order=GOOD_ORDER)
    second = FakeTranslator("local_ingestion", frozenset(), _observed=frozenset({"bronze.publish"}))
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
        layer=Layer.BRONZE,
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
        layer=Layer.BRONZE,
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
    plan = resolve(Layer.BRONZE)

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


TESTS = [
    test_registry_classifies_all_fifteen_with_no_legacy_alias,
    test_bronze_resolves_the_exact_normative_graph,
    test_batch_transfer_has_no_authoring_surface_and_forbidden_never_occurs,
    test_silver_and_gold_fail_unsupported_layer_deterministically,
    test_resolve_rejects_a_non_layer_argument,
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
