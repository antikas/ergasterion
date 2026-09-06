"""Assert-script tests for product declaration validation, layer 1
(architecture sections 3.1-3.4, 9).

Covers: the product-declaration and per-pattern configuration schemas exist
and are loadable JSON Schema; one valid fixture per reference profile
validates; every acceptance-named red case fails closed with the expected
rule slug; label/profile resolution through estate.yml (unknown label,
unknown profile, profile not admitted, profile required when a label admits
more than one); the neutrality gate, hermetically, for all five forbidden
markers and for the named_only "no inline expression at all" rule; a
per-product 'mode' key fails closed on schema shape; the shape registry
carries only 'declared' and fails closed on an unregistered shape name; and
the `ergasterion validate` CLI runs layer 1 over a synthetic estate's
declarations/products/ tree end to end, both green and red.

Usage:
    python tests/python/test_product_declaration.py
"""

from __future__ import annotations

import dataclasses
import shutil
import subprocess
import sys
import tempfile
import traceback
from pathlib import Path

if __package__ in (None, ""):
    import os as _os
    import sys as _sys
    _sys.path.insert(0, _os.path.dirname(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))))

import jsonschema
import yaml

from ergasterion.framework import declaration as decl
from ergasterion.framework.models import PatternId
from ergasterion.framework.neutrality import (
    MODE_NAMED_ONLY,
    MODE_SQL,
    NeutralityViolationError,
    find_markers,
    validate_expression,
)
from ergasterion.framework.shapes import UnknownShapeError, get_shape, registered_shape_names

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
FIXTURES_DIR = REPO_ROOT / "tests" / "fixtures" / "products"
VALID_DIR = FIXTURES_DIR / "valid"
RED_DIR = FIXTURES_DIR / "red"
RULES_FIXTURES_DIR = FIXTURES_DIR / "rules"
ESTATE_YML = REPO_ROOT / "estate.yml"
PATTERN_SCHEMAS_DIR = REPO_ROOT / "ergasterion" / "schemas" / "patterns"
PRODUCT_SCHEMA_PATH = REPO_ROOT / "ergasterion" / "schemas" / "product-declaration-v1.schema.json"
RULE_CATALOGUE_SCHEMA_PATH = REPO_ROOT / "ergasterion" / "schemas" / "named-rule-catalogue-v1.schema.json"

REFERENCE_PROFILES = ("landing", "integration", "derivation", "consolidation", "serving")


def _policy() -> decl.EstatePolicy:
    return decl.load_estate_policy(ESTATE_YML)


def _load_fixture(path: Path) -> dict:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


# --------------------------------------------------------------------------- schema shipping


def test_a_configuration_schema_is_shipped_for_every_one_of_the_fifteen_patterns() -> None:
    for pattern_id in PatternId:
        path = PATTERN_SCHEMAS_DIR / f"{pattern_id.value}.schema.json"
        assert path.is_file(), f"missing pattern configuration schema: {path}"
        schema = decl._load_json_schema(path)
        jsonschema.Draft202012Validator.check_schema(schema)


def test_product_declaration_and_rule_catalogue_schemas_are_valid_json_schema() -> None:
    for path in (PRODUCT_SCHEMA_PATH, RULE_CATALOGUE_SCHEMA_PATH):
        assert path.is_file(), f"missing schema: {path}"
        schema = decl._load_json_schema(path)
        jsonschema.Draft202012Validator.check_schema(schema)


def test_rule_catalogue_schema_validates_a_worked_example_rule() -> None:
    # architecture section 3.2's own example: a calculated field referencing
    # the named rule "risk_band_v2".
    schema = decl._load_json_schema(RULE_CATALOGUE_SCHEMA_PATH)
    document = {
        "schema": "ergasterion.rule-catalogue/v1",
        "rules": [
            {
                "name": "risk_band_v2",
                "version": 2,
                "description": "Bands a customer's risk score.",
                "inputs": [{"name": "risk_score", "type": "integer"}],
                "output": {"type": "string"},
            }
        ],
    }
    jsonschema.Draft202012Validator(schema).validate(document)


# --------------------------------------------------------------------------- valid fixtures


def test_one_valid_fixture_validates_for_every_reference_profile() -> None:
    policy = _policy()
    seen_profiles = set()
    for profile_name in REFERENCE_PROFILES:
        path = VALID_DIR / f"{profile_name}.yml"
        assert path.is_file(), f"missing valid fixture for profile {profile_name!r}: {path}"
        document = _load_fixture(path)
        result = decl.validate_declaration(document, policy=policy)
        assert result.profile == profile_name, (result.profile, profile_name)
        seen_profiles.add(result.profile)
    assert seen_profiles == set(REFERENCE_PROFILES)


def test_labels_with_one_admitted_profile_omit_the_profile_key() -> None:
    # bronze and gold each admit exactly one profile; the fixtures prove this
    # by never naming 'profile' at all.
    for profile_name in ("landing", "serving"):
        document = _load_fixture(VALID_DIR / f"{profile_name}.yml")
        assert "profile" not in document["product"], profile_name


def test_labels_with_more_than_one_admitted_profile_require_the_profile_key() -> None:
    for profile_name in ("integration", "derivation", "consolidation"):
        document = _load_fixture(VALID_DIR / f"{profile_name}.yml")
        assert document["product"]["profile"] == profile_name, profile_name
        assert document["product"]["layer"] == "silver", profile_name


def test_a_pattern_may_repeat_with_distinct_stages_pre_and_post() -> None:
    # architecture section 2: "a product may use a pattern more than once,
    # for example validation before and after transformation"; section
    # 3.1's worked example; section 5: "the stages a repeated pattern may
    # take". valid/integration.yml carries exactly this composition.
    document = _load_fixture(VALID_DIR / "integration.yml")
    validation_stages = [
        step.get("stage") for step in document["steps"] if step["pattern"] == "data_validation"
    ]
    assert validation_stages == ["pre", "post"], validation_stages
    policy = _policy()
    result = decl.validate_declaration(document, policy=policy)
    assert result.name == "customer"


def test_a_pattern_repeat_with_no_stage_in_its_schema_fails_closed() -> None:
    # schema_transform's own configuration schema carries no 'stage'
    # property at all, so two occurrences of it have no way to be
    # distinguished -- the repeat itself is illegal, independent of
    # duplicate_stage (which governs patterns whose schema DOES declare a
    # stage, like data_validation).
    document = _load_fixture(VALID_DIR / "integration.yml")
    mapping_step = next(s for s in document["steps"] if s["pattern"] == "schema_transform")
    document["steps"].insert(document["steps"].index(mapping_step) + 1, dict(mapping_step))
    try:
        result = decl.validate_declaration(document, policy=_policy())
    except decl.DeclarationError as exc:
        assert exc.rule == "repeated_pattern_without_stage", exc.rule
    else:
        raise AssertionError(f"expected repeated_pattern_without_stage, got {result!r}")


# --------------------------------------------------------------------------- red cases: single document


# Each red fixture is otherwise a valid declaration with exactly one injected
# violation; the rule slug pins which validation step is expected to fire.
SINGLE_DOCUMENT_RED_CASES: dict[str, str] = {
    "forbidden_pattern": "forbidden_pattern",
    "missing_mandatory_pattern": "missing_mandatory_pattern",
    "out_of_order_occurrence": "out_of_order_occurrence",
    "wrong_shape_section": "wrong_shape_section",
    "unknown_pattern_key": "unknown_pattern_key",
    "non_neutral_type": "non_neutral_type",
    "undeclared_structured_type": "undeclared_structured_type",
    "source_not_a_contract": "source_not_a_contract",
    "consolidation_needs_two_sources": "consolidation_needs_two_sources",
    "neutrality_jinja_tag": "neutrality_violation",
    "mode_key_present": "schema_violation",
    "duplicate_stage": "duplicate_stage",
    "neutrality_marker_in_product_owner": "neutrality_violation",
    "neutrality_marker_in_step_field": "neutrality_violation",
}

LABEL_RESOLUTION_RED_CASES: dict[str, str] = {
    "unknown_label": "unknown_label",
    "unknown_profile": "unknown_profile",
    "profile_not_admitted": "profile_not_admitted",
    "profile_required": "profile_required",
}


def test_every_named_acceptance_red_case_fails_closed_with_its_rule() -> None:
    policy = _policy()
    for case_name, expected_rule in SINGLE_DOCUMENT_RED_CASES.items():
        path = RED_DIR / f"{case_name}.yml"
        assert path.is_file(), f"missing red fixture: {path}"
        document = _load_fixture(path)
        try:
            result = decl.validate_declaration(document, policy=policy)
        except decl.DeclarationError as exc:
            assert exc.rule == expected_rule, (case_name, exc.rule, str(exc))
            assert exc.product, case_name  # every error names the product
        else:
            raise AssertionError(f"{case_name}: expected DeclarationError({expected_rule!r}), got {result!r}")


def test_label_and_profile_resolution_red_cases_fail_closed_naming_label_and_profile() -> None:
    policy = _policy()
    for case_name, expected_rule in LABEL_RESOLUTION_RED_CASES.items():
        path = RED_DIR / f"{case_name}.yml"
        assert path.is_file(), f"missing red fixture: {path}"
        document = _load_fixture(path)
        try:
            result = decl.validate_declaration(document, policy=policy)
        except decl.LabelResolutionError as exc:
            assert exc.rule == expected_rule, (case_name, exc.rule, str(exc))
            assert exc.product, case_name
            assert exc.label is not None or expected_rule == "unknown_label", case_name
        else:
            raise AssertionError(f"{case_name}: expected LabelResolutionError({expected_rule!r}), got {result!r}")


def test_resolve_profile_for_label_directly_covers_all_four_outcomes() -> None:
    policy = _policy()

    assert decl.resolve_profile_for_label(policy, product="p", label="bronze", profile=None) == "landing"
    assert decl.resolve_profile_for_label(policy, product="p", label="gold", profile=None) == "serving"
    assert (
        decl.resolve_profile_for_label(policy, product="p", label="silver", profile="derivation") == "derivation"
    )

    for label, profile, expected_rule in (
        ("nonexistent", None, "unknown_label"),
        ("silver", None, "profile_required"),
        ("silver", "not_a_real_profile", "unknown_profile"),
        ("bronze", "serving", "profile_not_admitted"),
    ):
        try:
            decl.resolve_profile_for_label(policy, product="p", label=label, profile=profile)
        except decl.LabelResolutionError as exc:
            assert exc.rule == expected_rule, (label, profile, exc.rule)
        else:
            raise AssertionError(f"expected LabelResolutionError({expected_rule!r}) for {label!r}/{profile!r}")


# --------------------------------------------------------------------------- duplicate published name (estate-level)


def test_duplicate_published_name_fails_closed_across_the_estate_scan() -> None:
    policy = _policy()
    with tempfile.TemporaryDirectory() as tmp:
        products_dir = Path(tmp) / "products"
        products_dir.mkdir()
        shutil.copy(RED_DIR / "duplicate_published_name_a.yml", products_dir / "a.yml")
        shutil.copy(RED_DIR / "duplicate_published_name_b.yml", products_dir / "b.yml")

        # Each file individually validates: the collision is estate-level.
        decl.validate_declaration(_load_fixture(products_dir / "a.yml"), policy=policy)
        decl.validate_declaration(_load_fixture(products_dir / "b.yml"), policy=policy)

        try:
            result = decl.validate_estate_products(products_dir, policy=policy)
        except decl.DeclarationError as exc:
            assert exc.rule == "duplicate_published_name", exc.rule
            assert "ecommerce.dup_customer" in str(exc), str(exc)
        else:
            raise AssertionError(f"expected duplicate_published_name, got {result!r}")


def test_validate_estate_products_accepts_five_distinct_valid_products() -> None:
    policy = _policy()
    with tempfile.TemporaryDirectory() as tmp:
        products_dir = Path(tmp) / "products"
        products_dir.mkdir()
        for fixture in VALID_DIR.glob("*.yml"):
            shutil.copy(fixture, products_dir / fixture.name)
        names = decl.validate_estate_products(products_dir, policy=policy)
        assert len(names) == 5, names


def test_validate_estate_products_on_a_missing_directory_is_valid_and_empty() -> None:
    policy = _policy()
    with tempfile.TemporaryDirectory() as tmp:
        result = decl.validate_estate_products(Path(tmp) / "does_not_exist", policy=policy)
        assert result == []


# --------------------------------------------------------------------------- neutrality gate: hermetic seen-RED proof


def test_neutrality_gate_is_hermetically_red_on_every_forbidden_marker() -> None:
    # One hermetic seen-RED proof per marker: an injected violation of each
    # named kind must turn the gate red, independent of any fixture file.
    cases = {
        "jinja_tag": "{{ ref('foo') }}",
        "ref_call": "ref('landing_customer')",
        "source_call": "source('landing', 'customer')",
        "url_scheme": "'s3://bucket/data.csv'",
        "filesystem_path": "'/data/exports/file.csv'",
    }
    for expected_marker, text in cases.items():
        markers = find_markers(text)
        assert expected_marker in markers, (expected_marker, markers, text)
        try:
            validate_expression(text, mode=MODE_SQL, context="hermetic-proof")
        except NeutralityViolationError as exc:
            assert expected_marker in exc.reason, (expected_marker, exc.reason)
        else:
            raise AssertionError(f"expected NeutralityViolationError for marker {expected_marker!r}")


def test_neutrality_gate_passes_clean_sql_with_no_markers() -> None:
    clean = "status_code IN ('A', 'P') AND end_date IS NULL"
    assert find_markers(clean) == ()
    validate_expression(clean, mode=MODE_SQL, context="hermetic-proof")  # must not raise
    # Ordinary division must never be mistaken for a filesystem path.
    assert find_markers("amount / total") == ()


def test_neutrality_gate_does_not_police_ordinary_sql_constructs() -> None:
    # Architecture section 3.4: the neutrality gate "does not police which
    # SQL constructs an expression uses" -- only the five named technology
    # markers. None of these ordinary constructs may ever be mistaken for
    # one, however dense the punctuation.
    ordinary_sql = {
        "window_function": "ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY created_on)",
        "case_expression": "CASE WHEN status_code = 'A' THEN 'active' ELSE 'inactive' END",
        "cast_function": "CAST(amount AS DECIMAL(18, 2))",
        "double_colon_cast": "amount::numeric",
        "arithmetic_division": "amount / total_amount",
        "string_concatenation_with_slash": "first_name || '/' || last_name",
        "subquery": (
            "(SELECT MAX(created_on) FROM customer_events "
            "WHERE customer_events.customer_id = customer.customer_id)"
        ),
        "regex_literal": "customer_id ~ '^CUST-[0-9]+$'",
    }
    for name, text in ordinary_sql.items():
        markers = find_markers(text)
        assert markers == (), (name, markers, text)
        validate_expression(text, mode=MODE_SQL, context=name)  # must not raise


def test_neutrality_gate_rejects_any_inline_expression_under_named_only_mode() -> None:
    # Reuses the integration fixture's document content (which carries an
    # inline expression) against a named_only policy, rather than a
    # dedicated fixture file: the red case here is a difference in ESTATE
    # POLICY, not in document content.
    base_policy = _policy()
    named_only_policy = dataclasses.replace(base_policy, expression_mode=MODE_NAMED_ONLY)
    document = _load_fixture(VALID_DIR / "integration.yml")
    try:
        result = decl.validate_declaration(document, policy=named_only_policy)
    except decl.DeclarationError as exc:
        assert exc.rule == "neutrality_violation", exc.rule
    else:
        raise AssertionError(f"expected neutrality_violation under named_only, got {result!r}")

    # A product with no inline expressions at all (derivation.yml uses a
    # named rule, not an expression) still validates under named_only.
    named_rule_document = _load_fixture(VALID_DIR / "derivation.yml")
    result = decl.validate_declaration(named_rule_document, policy=named_only_policy)
    assert result.name == "customer_risk_score"


def test_validate_expression_rejects_an_unknown_mode_as_a_caller_error() -> None:
    from ergasterion.framework.models import FrameworkError

    try:
        validate_expression("SUM(amount)", mode="bogus_mode", context="test")
    except FrameworkError as exc:
        assert not isinstance(exc, NeutralityViolationError)
    else:
        raise AssertionError("expected FrameworkError for an unknown expression mode")


# --------------------------------------------------------------------------- estate.yml policy loading


def test_estate_policy_loads_the_committed_estate_yml() -> None:
    policy = _policy()
    assert policy.expression_mode == MODE_SQL
    assert "array" in policy.structured_types
    assert policy.labels["bronze"] == ("landing",)
    assert policy.labels["gold"] == ("serving",)
    assert set(policy.labels["silver"]) == {"integration", "derivation", "consolidation"}


def test_estate_policy_rejects_an_unknown_expression_mode() -> None:
    from ergasterion.framework.models import FrameworkError

    with tempfile.TemporaryDirectory() as tmp:
        estate_file = Path(tmp) / "estate.yml"
        estate_file.write_text(
            "estate:\n  namespace: com.example.test\n  expression_mode: bogus\n", encoding="utf-8"
        )
        try:
            decl.load_estate_policy(estate_file)
        except FrameworkError as exc:
            assert "expression_mode" in str(exc), str(exc)
        else:
            raise AssertionError("expected FrameworkError for an unknown expression_mode")


def test_estate_policy_rejects_a_scalar_structured_types_value() -> None:
    # A bare scalar (a string written where a one-item list was meant) must
    # fail closed, not silently iterate its characters into a frozenset of
    # single letters (frozenset("array") produces {"a", "r", "y", ...}).
    from ergasterion.framework.models import FrameworkError

    with tempfile.TemporaryDirectory() as tmp:
        estate_file = Path(tmp) / "estate.yml"
        estate_file.write_text(
            "estate:\n  namespace: com.example.test\n  structured_types: array\n", encoding="utf-8"
        )
        try:
            policy = decl.load_estate_policy(estate_file)
        except FrameworkError as exc:
            assert "structured_types" in str(exc), str(exc)
        else:
            raise AssertionError(f"expected FrameworkError for a scalar structured_types, got {policy!r}")


# --------------------------------------------------------------------------- shape registry


def test_shape_registry_carries_the_engine_shape_and_every_registered_plug_in() -> None:
    # ``declared`` is the engine's own; ``canonical``, ``data_vault``,
    # ``dimensional`` and ``ods`` are plug-in packages under
    # ergasterion/shapes/ that register themselves when the registry is
    # first asked.
    assert set(registered_shape_names()) == {
        "declared",
        "canonical",
        "data_vault",
        "dimensional",
        "ods",
    }
    shape = get_shape("declared")
    # 'declared' needs nothing: shape_config must reject any property.
    jsonschema.Draft202012Validator(shape.shape_config_schema).validate({})
    try:
        jsonschema.Draft202012Validator(shape.shape_config_schema).validate({"hubs": []})
    except jsonschema.ValidationError:
        pass
    else:
        raise AssertionError("declared shape's shape_config must reject any property, including 'hubs'")


def test_unknown_shape_name_fails_closed() -> None:
    for bad_name in ("data_mesh", "wide_table", "", None, 42):
        try:
            get_shape(bad_name)
        except UnknownShapeError as exc:
            assert exc.shape_name == bad_name
        else:
            raise AssertionError(f"expected UnknownShapeError for {bad_name!r}")


def test_target_contract_is_closed_like_everywhere_else() -> None:
    # architecture section 7's element list is closed (identity, schema,
    # freshness, quality, versioning, lineage, access, support); a stray
    # key such as 'mode' there must fail closed the same way a per-product
    # or per-column mode key does everywhere else in the schema.
    document = _load_fixture(VALID_DIR / "landing.yml")
    document["target"]["contract"]["mode"] = "sql"
    try:
        result = decl.validate_declaration(document, policy=_policy())
    except decl.DeclarationError as exc:
        assert exc.rule == "schema_violation", exc.rule
        assert "target" in exc.occurrence, exc.occurrence
    else:
        raise AssertionError(f"expected schema_violation for a stray target.contract.mode key, got {result!r}")


# --------------------------------------------------------------------------- CLI end to end


def _build_synthetic_estate(root: Path) -> None:
    (root / "domains").mkdir(parents=True, exist_ok=True)
    (root / "declarations" / "products").mkdir(parents=True, exist_ok=True)
    (root / "rules").mkdir(parents=True, exist_ok=True)
    (root / "dbt_project.yml").write_text("name: synthetic\n", encoding="utf-8")
    shutil.copy(ESTATE_YML, root / "estate.yml")


def test_ergasterion_validate_cli_runs_layer_1_and_layer_2_end_to_end_green_then_red() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp) / "synthetic_estate"
        _build_synthetic_estate(root)
        products_dir = root / "declarations" / "products"
        for fixture in VALID_DIR.glob("*.yml"):
            shutil.copy(fixture, products_dir / fixture.name)
        # derivation.yml's calculated_fields step names "rule: risk_band_v2"
        # (architecture section 3.1's own worked example): a business rule
        # the engine reference catalogue does not ship, so this synthetic
        # estate carries it the way a real estate would.
        shutil.copy(RULES_FIXTURES_DIR / "risk_band_v2.yml", root / "rules" / "risk_band_v2.yml")

        proc = subprocess.run(
            [sys.executable, "-m", "ergasterion", "validate", "--estate-root", str(root)],
            cwd=str(REPO_ROOT),
            capture_output=True,
            text=True,
        )
        assert proc.returncode == 0, (proc.stdout, proc.stderr)
        assert "validate OK: 5 product declaration(s)" in proc.stdout, proc.stdout

        # Inject one red fixture; the CLI must now fail closed (exit 1) at
        # layer 1, before layer 2 ever runs.
        shutil.copy(RED_DIR / "forbidden_pattern.yml", products_dir / "forbidden_pattern.yml")
        proc = subprocess.run(
            [sys.executable, "-m", "ergasterion", "validate", "--estate-root", str(root)],
            cwd=str(REPO_ROOT),
            capture_output=True,
            text=True,
        )
        assert proc.returncode == 1, (proc.stdout, proc.stderr)
        assert "FAIL (layer 1)" in proc.stdout, proc.stdout
        assert "forbidden_pattern" in proc.stdout, proc.stdout


def test_ergasterion_validate_cli_fails_closed_at_layer_2_on_a_rule_missing_an_implementation_pair() -> None:
    # A product referencing a rule the catalogue KNOWS (so layer 1 and the
    # rule-reference resolution both succeed) but cannot resolve for every
    # declared (translator, adapter) pair must fail closed at layer 2,
    # naming the rule and the missing pair (architecture section 3.4,
    # check 9) -- distinguishably from a layer 1 failure.
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp) / "synthetic_estate_incomplete_rule"
        _build_synthetic_estate(root)
        products_dir = root / "declarations" / "products"
        shutil.copy(VALID_DIR / "derivation.yml", products_dir / "derivation.yml")
        shutil.copy(RULES_FIXTURES_DIR / "risk_band_v2_incomplete.yml", root / "rules" / "risk_band_v2.yml")

        proc = subprocess.run(
            [sys.executable, "-m", "ergasterion", "validate", "--estate-root", str(root)],
            cwd=str(REPO_ROOT),
            capture_output=True,
            text=True,
        )
        assert proc.returncode == 1, (proc.stdout, proc.stderr)
        assert "FAIL (layer 2)" in proc.stdout, proc.stdout
        assert "risk_band_v2" in proc.stdout, proc.stdout
        assert "bigquery" in proc.stdout, proc.stdout


def test_ergasterion_validate_cli_is_green_on_an_estate_with_no_product_declarations() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp) / "synthetic_estate_empty"
        _build_synthetic_estate(root)
        proc = subprocess.run(
            [sys.executable, "-m", "ergasterion", "validate", "--estate-root", str(root)],
            cwd=str(REPO_ROOT),
            capture_output=True,
            text=True,
        )
        assert proc.returncode == 0, (proc.stdout, proc.stderr)
        assert "validate OK: 0 product declaration(s)" in proc.stdout, proc.stdout


TESTS = [
    test_a_configuration_schema_is_shipped_for_every_one_of_the_fifteen_patterns,
    test_product_declaration_and_rule_catalogue_schemas_are_valid_json_schema,
    test_rule_catalogue_schema_validates_a_worked_example_rule,
    test_one_valid_fixture_validates_for_every_reference_profile,
    test_labels_with_one_admitted_profile_omit_the_profile_key,
    test_labels_with_more_than_one_admitted_profile_require_the_profile_key,
    test_a_pattern_may_repeat_with_distinct_stages_pre_and_post,
    test_a_pattern_repeat_with_no_stage_in_its_schema_fails_closed,
    test_every_named_acceptance_red_case_fails_closed_with_its_rule,
    test_label_and_profile_resolution_red_cases_fail_closed_naming_label_and_profile,
    test_resolve_profile_for_label_directly_covers_all_four_outcomes,
    test_duplicate_published_name_fails_closed_across_the_estate_scan,
    test_validate_estate_products_accepts_five_distinct_valid_products,
    test_validate_estate_products_on_a_missing_directory_is_valid_and_empty,
    test_neutrality_gate_is_hermetically_red_on_every_forbidden_marker,
    test_neutrality_gate_passes_clean_sql_with_no_markers,
    test_neutrality_gate_does_not_police_ordinary_sql_constructs,
    test_neutrality_gate_rejects_any_inline_expression_under_named_only_mode,
    test_validate_expression_rejects_an_unknown_mode_as_a_caller_error,
    test_estate_policy_loads_the_committed_estate_yml,
    test_estate_policy_rejects_an_unknown_expression_mode,
    test_estate_policy_rejects_a_scalar_structured_types_value,
    test_shape_registry_carries_the_engine_shape_and_every_registered_plug_in,
    test_unknown_shape_name_fails_closed,
    test_target_contract_is_closed_like_everywhere_else,
    test_ergasterion_validate_cli_runs_layer_1_and_layer_2_end_to_end_green_then_red,
    test_ergasterion_validate_cli_fails_closed_at_layer_2_on_a_rule_missing_an_implementation_pair,
    test_ergasterion_validate_cli_is_green_on_an_estate_with_no_product_declarations,
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
