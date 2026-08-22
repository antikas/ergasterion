"""Python-level unit tests for ergasterion/structure_gate.py (the per-target
structural gate).

Checks that every deployment target declares its structural
budgets (declarations/targets/<adapter>.yml) and generation validates the whole
models tree against them. Each budget is proven both ways: a deliberately
malformed fixture estate fails with the target, the budget, and the artefact
named, and the shipped estate passes as committed.

Same plain assert/report convention as tests/python/test_emit.py (no pytest in this
repo's .venv): each test_* raises AssertionError on failure, main() runs them
all and reports PASS/FAIL, exit code 0 = all green, 1 = any failure. Every
fixture estate lives under a tempfile.TemporaryDirectory.

Usage:
    python tests/python/test_structure_gate.py
"""

from __future__ import annotations

import sys
import tempfile
import traceback
from pathlib import Path

if __package__ in (None, ""):
    import os as _os, sys as _sys
    _sys.path.insert(0, _os.path.dirname(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))))

from ergasterion.estate import EstateContext
from ergasterion.structure_gate import check_structure, load_structure_declarations, normalise_landing

REPO_ROOT = Path(__file__).resolve().parents[2]

DEFAULT_BUDGETS = """\
adapter: {adapter}
kind: deployment
budgets:
  max_view_chain_depth: 1
  max_statement_bytes: 262144
  max_relation_identifier_chars: 255
  max_column_identifier_chars: 255
  max_description_bytes: 1024
  max_seed_rows: 10000
  max_seed_bytes: 10485760
"""


def _write(root: Path, rel: str, content: str) -> None:
    path = root / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _fixture_estate(
    root: Path,
    *,
    targets: dict[str, str] | None = None,
    interfaces: str = "view_layers:\n  - models/served\n",
) -> EstateContext:
    """Lay down a minimal estate: a project file, one table-over-view chain that
    respects the standing rule (computation table, one served view on top), and
    the target declarations the caller supplies."""
    _write(root, "dbt_project.yml", "name: fixture\nmodels:\n  fixture:\n    work:\n      +materialized: table\n")
    _write(root, "models/work/base.sql", "select 1 as id\n")
    _write(root, "models/served/iface.sql", "{{ config(materialized='view') }}\nselect * from {{ ref('base') }}\n")
    if targets is None:
        targets = {"duckplug": DEFAULT_BUDGETS.format(adapter="duckplug")}
    for name, content in targets.items():
        _write(root, f"declarations/targets/{name}.yml", content)
    if interfaces:
        _write(root, "declarations/targets/interfaces.yml", interfaces)
    return EstateContext.resolve(estate_root=root)


def _offenses_for(offenses: list, budget: str) -> list:
    return [offense for offense in offenses if offense.budget == budget]


def test_compliant_fixture_passes() -> None:
    """The baseline fixture (computation as tables, one one-deep served view)
    clears every budget."""
    with tempfile.TemporaryDirectory() as tmp:
        ctx = _fixture_estate(Path(tmp))
        offenses = check_structure(ctx)
        assert offenses == [], f"expected a clean pass, got: {offenses}"


def test_view_chain_past_ceiling_fails() -> None:
    """A view reading a view breaches the declared nesting ceiling of 1, with the
    deepest artefact and both budget and target named."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        ctx = _fixture_estate(
            root,
            interfaces="view_layers:\n  - models/served\n  - models/work\n",
        )
        _write(root, "models/work/mid.sql", "{{ config(materialized='view') }}\nselect * from {{ ref('base') }}\n")
        _write(root, "models/served/deep.sql", "{{ config(materialized='view') }}\nselect * from {{ ref('mid') }}\n")
        offenses = _offenses_for(check_structure(ctx), "max_view_chain_depth")
        assert offenses, "expected a max_view_chain_depth offense, got none"
        assert any("deep.sql" in offense.artefact for offense in offenses), (
            f"expected the deepest view named, got: {offenses}"
        )
        assert all(offense.measured == 2 for offense in offenses), (
            f"expected measured depth 2, got: {offenses}"
        )


def test_view_outside_boundary_fails() -> None:
    """A computation-layer view outside the declared interface paths breaches the
    materialisation boundary on a deployment target."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        ctx = _fixture_estate(root)
        _write(root, "models/work/sneaky.sql", "{{ config(materialized='view') }}\nselect * from {{ ref('base') }}\n")
        offenses = _offenses_for(check_structure(ctx), "view_boundary")
        assert any("sneaky.sql" in offense.artefact for offense in offenses), (
            f"expected the stray view named, got: {offenses}"
        )


def test_lane_exception_allows_view_with_reason() -> None:
    """A lane target keeps a view outside the interface paths when its own config
    states the depending lane tooling; a deployment target beside it still fails."""
    lane = (
        "adapter: laneplug\n"
        "kind: lane\n"
        "view_exceptions:\n"
        "  - path: models/work/lane_helper.sql\n"
        "    reason: lane tooling reads this relation as a view\n"
    )
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        ctx = _fixture_estate(
            root,
            targets={
                "duckplug": DEFAULT_BUDGETS.format(adapter="duckplug"),
                "laneplug": lane,
            },
        )
        _write(root, "models/work/lane_helper.sql", "{{ config(materialized='view') }}\nselect * from {{ ref('base') }}\n")
        offenses = _offenses_for(check_structure(ctx), "view_boundary")
        adapters = {offense.adapter for offense in offenses}
        assert adapters == {"duckplug"}, (
            f"expected the deployment target alone to flag the view, got: {offenses}"
        )


def test_lane_exception_without_reason_fails_loading() -> None:
    """A lane view exception with no stated reason fails declaration loading."""
    lane = (
        "adapter: laneplug\n"
        "kind: lane\n"
        "view_exceptions:\n"
        "  - path: models/work/lane_helper.sql\n"
        "    reason: ''\n"
    )
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        ctx = _fixture_estate(
            root,
            targets={
                "duckplug": DEFAULT_BUDGETS.format(adapter="duckplug"),
                "laneplug": lane,
            },
        )
        try:
            check_structure(ctx)
        except ValueError as exc:
            assert "reason" in str(exc), f"expected the reason requirement named, got: {exc}"
        else:
            raise AssertionError("expected ValueError for a reasonless view exception")


def test_statement_bytes_over_budget_fails() -> None:
    """A model statement larger than the declared byte budget is named with its
    measured size."""
    tight = DEFAULT_BUDGETS.format(adapter="duckplug").replace(
        "max_statement_bytes: 262144", "max_statement_bytes: 64"
    )
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        ctx = _fixture_estate(root, targets={"duckplug": tight})
        _write(root, "models/work/wide.sql", "select 1 as id -- " + "x" * 200 + "\n")
        offenses = _offenses_for(check_structure(ctx), "max_statement_bytes")
        assert any("wide.sql" in offense.artefact for offense in offenses), (
            f"expected the oversized statement named, got: {offenses}"
        )


def test_relation_identifier_over_budget_fails() -> None:
    """A relation identifier longer than the declared budget fails, wherever it is
    declared (a source table in a schema file here)."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        ctx = _fixture_estate(root)
        long_name = "m" * 300
        _write(
            root,
            "models/work/_sources.yml",
            f"sources:\n  - name: raw\n    tables:\n      - name: {long_name}\n",
        )
        offenses = _offenses_for(check_structure(ctx), "max_relation_identifier_chars")
        assert any(long_name in offense.artefact for offense in offenses), (
            f"expected the long relation identifier named, got: {offenses}"
        )


def test_column_identifier_over_budget_fails() -> None:
    """Column identifiers from schema files and seed CSV headers both bind."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        ctx = _fixture_estate(root)
        long_yml = "c" * 300
        long_csv = "d" * 300
        _write(
            root,
            "models/work/_work.yml",
            f"models:\n  - name: base\n    columns:\n      - name: {long_yml}\n",
        )
        _write(root, f"seeds/raw_fixture.csv", f"id,{long_csv}\n1,2\n")
        offenses = _offenses_for(check_structure(ctx), "max_column_identifier_chars")
        named = ",".join(offense.artefact for offense in offenses)
        assert long_yml in named, f"expected the schema-file column named, got: {offenses}"
        assert long_csv in named, f"expected the seed header column named, got: {offenses}"


def test_description_over_budget_fails() -> None:
    """A description string over the metadata budget is named with its file."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        ctx = _fixture_estate(root)
        _write(
            root,
            "models/work/_work.yml",
            "models:\n  - name: base\n    description: >\n      " + "long words " * 200 + "\n",
        )
        offenses = _offenses_for(check_structure(ctx), "max_description_bytes")
        assert any("_work.yml" in offense.artefact for offense in offenses), (
            f"expected the schema file named, got: {offenses}"
        )


def test_declared_seed_size_budget_fails_over_and_passes_under() -> None:
    """A declared seed breaches both size budgets when over their limits and
    clears both when the same CSV is under them."""
    tight = DEFAULT_BUDGETS.format(adapter="duckplug").replace(
        "max_seed_rows: 10000", "max_seed_rows: 2"
    ).replace("max_seed_bytes: 10485760", "max_seed_bytes: 20")
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        ctx = _fixture_estate(root, targets={"duckplug": tight})
        _write(
            root,
            "declarations/feed.yml",
            "source:\n  name: feed\ntables:\n  events:\n    raw_model: raw_feed_events\n",
        )
        _write(root, "seeds/raw_feed_events.csv", "id,value\n1,alpha\n2,beta\n3,gamma\n")
        expected_bytes = (root / "seeds/raw_feed_events.csv").stat().st_size

        row_offenses = _offenses_for(check_structure(ctx), "max_seed_rows")
        byte_offenses = _offenses_for(check_structure(ctx), "max_seed_bytes")
        for offenses, measured in ((row_offenses, 3), (byte_offenses, expected_bytes)):
            assert len(offenses) == 1, f"expected one seed-size offense, got: {offenses}"
            offense = offenses[0]
            assert offense.adapter == "duckplug", f"expected target named, got: {offense}"
            assert offense.artefact == "seeds/raw_feed_events.csv", (
                f"expected seed file named, got: {offense}"
            )
            assert offense.measured == measured, f"expected measured value {measured}, got: {offense}"

        _write(root, "seeds/raw_feed_events.csv", 'id,value\n1,"line one\nline two"\n2,b\n')
        offenses = check_structure(ctx)
        assert not _offenses_for(offenses, "max_seed_rows"), f"under-row seed must pass: {offenses}"
        assert _offenses_for(offenses, "max_seed_bytes"), (
            f"multiline fixture remains over byte budget: {offenses}"
        )

        _write(root, "seeds/raw_feed_events.csv", "id,value\n1,a\n")
        offenses = check_structure(ctx)
        assert not _offenses_for(offenses, "max_seed_rows"), f"under-row seed must pass: {offenses}"
        assert not _offenses_for(offenses, "max_seed_bytes"), f"under-byte seed must pass: {offenses}"


def test_missing_targets_directory_fails() -> None:
    """The gate is fail-closed: an estate with no declarations/targets/ fails with
    the required declaration named."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        _write(root, "dbt_project.yml", "name: fixture\n")
        _write(root, "models/work/base.sql", "select 1 as id\n")
        ctx = EstateContext.resolve(estate_root=root)
        try:
            check_structure(ctx)
        except ValueError as exc:
            assert "declarations/targets" in str(exc), f"expected the directory named, got: {exc}"
        else:
            raise AssertionError("expected ValueError for a missing targets directory")


def test_adapter_filename_mismatch_fails() -> None:
    """A budget declaration whose adapter differs from its filename stem fails
    loading, so a copy-pasted declaration cannot masquerade as another target."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        ctx = _fixture_estate(
            root, targets={"duckplug": DEFAULT_BUDGETS.format(adapter="otherplug")}
        )
        try:
            check_structure(ctx)
        except ValueError as exc:
            assert "otherplug" in str(exc), f"expected the mismatched adapter named, got: {exc}"
        else:
            raise AssertionError("expected ValueError for an adapter/filename mismatch")


def test_deployment_target_missing_budget_fails() -> None:
    """A deployment target must declare the full budget family; a missing key is
    named."""
    partial = "adapter: duckplug\nkind: deployment\nbudgets:\n  max_view_chain_depth: 1\n"
    with tempfile.TemporaryDirectory() as tmp:
        ctx = _fixture_estate(Path(tmp), targets={"duckplug": partial})
        try:
            check_structure(ctx)
        except ValueError as exc:
            assert "max_statement_bytes" in str(exc), f"expected missing budgets named, got: {exc}"
        else:
            raise AssertionError("expected ValueError for missing deployment budgets")


def test_deployment_target_rejects_view_exceptions() -> None:
    """View exceptions are lane-only config; a deployment target declaring them
    fails loading."""
    bad = DEFAULT_BUDGETS.format(adapter="duckplug") + (
        "view_exceptions:\n  - path: models/work/x.sql\n    reason: convenience\n"
    )
    with tempfile.TemporaryDirectory() as tmp:
        ctx = _fixture_estate(Path(tmp), targets={"duckplug": bad})
        try:
            check_structure(ctx)
        except ValueError as exc:
            assert "lane" in str(exc), f"expected the lane-only rule named, got: {exc}"
        else:
            raise AssertionError("expected ValueError for view_exceptions on a deployment target")


def test_in_file_config_overrides_layer_view() -> None:
    """An in-file table config caps a chain even under a layer configured view,
    matching dbt's own precedence."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        ctx = _fixture_estate(root)
        # The work layer is table-configured; give the estate a view-configured
        # layer with an in-file table override in the middle of a would-be chain.
        _write(root, "dbt_project.yml", "name: fixture\nmodels:\n  fixture:\n    work:\n      +materialized: table\n    viewy:\n      +materialized: view\n")
        _write(root, "models/viewy/lower.sql", "select * from {{ ref('base') }}\n")
        _write(root, "models/viewy/cap.sql", "{{ config(materialized='table') }}\nselect * from {{ ref('lower') }}\n")
        _write(root, "models/served/over.sql", "{{ config(materialized='view') }}\nselect * from {{ ref('cap') }}\n")
        offenses = _offenses_for(check_structure(ctx), "max_view_chain_depth")
        assert not any("over.sql" in offense.artefact for offense in offenses), (
            f"the in-file table config must cap the chain under over.sql, got: {offenses}"
        )


def test_shipped_estate_passes() -> None:
    """The committed estate clears every declared target budget as shipped."""
    ctx = EstateContext.resolve(estate_root=REPO_ROOT)
    offenses = check_structure(ctx)
    assert offenses == [], f"the shipped estate must pass its own gate, got: {offenses}"


def test_shipped_declarations_load() -> None:
    """The committed target declarations parse, both deployment targets declare the
    full budget family, and the canonical layer is a declared interface."""
    ctx = EstateContext.resolve(estate_root=REPO_ROOT)
    declarations, view_layers = load_structure_declarations(ctx)
    adapters = {decl.adapter for decl in declarations if decl.kind == "deployment"}
    assert {"bigquery", "snowflake"} <= adapters, f"expected both deployment targets, got: {adapters}"
    assert "models/canonical" in view_layers, f"expected models/canonical declared, got: {view_layers}"


BRONZE_LANDING = {
    "kind": "source",
    "source_name": "warehouse_feed",
    "identifier": "things_live",
    "integration": {"kind": "managed"},
    "content_encodings": ["identity"],
    "codec": {"kind": "jsonl", "version": 1, "charset": "utf-8", "newline": "lf"},
    "physical_columns": [{"name": "id", "logical_type": "utf8_string", "nullable": False}],
}


def test_landing_admits_the_bronze_contract_fields_on_a_source_landing() -> None:
    """normalise_landing is the single structural entry point for the landing
    discriminator. A source landing may carry the four Bronze Product Contract
    fields; a bare source landing stays the plain dbt source reference it has
    always been; a seed landing admits none of them."""
    landing = normalise_landing({"landing": dict(BRONZE_LANDING)}, "fixture:things")
    assert landing["integration"] == {"kind": "managed"}, landing
    assert landing["physical_columns"][0]["name"] == "id", landing

    legacy = normalise_landing(
        {"landing": {"kind": "source", "source_name": "feed", "identifier": "rel"}},
        "fixture:things",
    )
    assert legacy == {"kind": "source", "source_name": "feed", "identifier": "rel"}, legacy

    assert normalise_landing({}, "fixture:things") == {"kind": "seed"}, "the default stays seed"

    for bronze_field in ("integration", "content_encodings", "codec", "physical_columns"):
        try:
            normalise_landing({"landing": {"kind": "seed", bronze_field: {}}}, "fixture:things")
        except ValueError as exc:
            assert bronze_field in str(exc), str(exc)
        else:
            raise AssertionError(f"a seed landing must reject {bronze_field!r}")


def test_landing_still_rejects_a_misspelled_or_incomplete_source_landing() -> None:
    """Widening the admitted key set does not widen what passes: a misspelled
    Bronze field and a source landing missing its coordinates both fail."""
    misspelled = dict(BRONZE_LANDING)
    misspelled["physical_colums"] = []
    try:
        normalise_landing({"landing": misspelled}, "fixture:things")
    except ValueError as exc:
        assert "physical_colums" in str(exc), str(exc)
    else:
        raise AssertionError("a misspelled Bronze landing field must fail")

    try:
        normalise_landing({"landing": {"kind": "source", "integration": {"kind": "managed"}}}, "fixture:things")
    except ValueError as exc:
        assert "source_name" in str(exc) and "identifier" in str(exc), str(exc)
    else:
        raise AssertionError("a source landing without its coordinates must fail")


def test_the_structural_gate_owns_no_bronze_semantics() -> None:
    """The structural gate validates the shape of the discriminator alone.
    Whether a codec, encoding or column set is a valid Bronze Product Contract is
    decided by ergasterion.source_delivery, so a structurally well-formed landing
    carrying nonsense Bronze content still passes here."""
    nonsense = dict(BRONZE_LANDING)
    nonsense["codec"] = {"kind": "not-a-codec"}
    nonsense["content_encodings"] = ["identity", "identity"]
    nonsense["physical_columns"] = []
    landing = normalise_landing({"landing": nonsense}, "fixture:things")
    assert landing["codec"] == {"kind": "not-a-codec"}, landing

    from ergasterion import source_delivery

    try:
        source_delivery.LandingContract.model_validate(nonsense)
    except Exception as exc:
        assert "codec" in str(exc), str(exc)
    else:
        raise AssertionError("the Bronze wire schema must reject the codec this gate passes")


TESTS = [
    test_landing_admits_the_bronze_contract_fields_on_a_source_landing,
    test_landing_still_rejects_a_misspelled_or_incomplete_source_landing,
    test_the_structural_gate_owns_no_bronze_semantics,
    test_compliant_fixture_passes,
    test_view_chain_past_ceiling_fails,
    test_view_outside_boundary_fails,
    test_lane_exception_allows_view_with_reason,
    test_lane_exception_without_reason_fails_loading,
    test_statement_bytes_over_budget_fails,
    test_relation_identifier_over_budget_fails,
    test_column_identifier_over_budget_fails,
    test_description_over_budget_fails,
    test_declared_seed_size_budget_fails_over_and_passes_under,
    test_missing_targets_directory_fails,
    test_adapter_filename_mismatch_fails,
    test_deployment_target_missing_budget_fails,
    test_deployment_target_rejects_view_exceptions,
    test_in_file_config_overrides_layer_view,
    test_shipped_estate_passes,
    test_shipped_declarations_load,
]


def main() -> int:
    failures = 0
    for test in TESTS:
        try:
            test()
        except Exception:
            failures += 1
            print(f"FAIL {test.__name__}")
            traceback.print_exc(file=sys.stdout)
        else:
            print(f"PASS {test.__name__}")
    if failures:
        print(f"{failures} of {len(TESTS)} tests failed")
        return 1
    print(f"all {len(TESTS)} tests passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
