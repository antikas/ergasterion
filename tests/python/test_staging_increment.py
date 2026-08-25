"""Unit and golden-file tests for watermark increments: the staging increment block,
the natural key, the derived effective column, every emit-time gate, and the generated
SQL for both window shapes.

Vocabulary, used here exactly as the emitter and its errors use it:

  effective column  the one staging output column a table's bridge maps to
                    effective_from.
  staging increment block  the declared per-table configuration for watermark
                    increments: the lookback and the acknowledgment.
  consumption watermark  the point on the effective column up to which every satellite
                    fed by a table has absorbed history.
  delta window  the interval [consumption watermark minus lookback, infinity), entered
                    with a >= comparison at the floor.
  replay suppression  the satellite guard discarding a candidate row whose (business
                    key, hashdiff, effective time) already exists in the target.

No pytest dependency in this repo's .venv, so this follows the plain
assert-and-report convention already used by tests/python/test_emit.py: each test_*
function raises AssertionError on failure, main() runs them all and reports PASS/FAIL,
exit code 0 = all green, 1 = any failure.

Usage:
    python tests/python/test_staging_increment.py
"""

from __future__ import annotations

import contextlib
import copy
import datetime
import io
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import traceback
from pathlib import Path
from types import SimpleNamespace

import yaml

# Allow direct execution as `python tests/python/test_staging_increment.py`.
if __package__ in (None, ""):
    import os as _os, sys as _sys
    _sys.path.insert(0, _os.path.dirname(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))))
    _sys.path.insert(0, _os.path.dirname(_os.path.abspath(__file__)))

from ergasterion import cli, emit, structure_gate
from ergasterion.translators import dbt as dbt_translator

from test_emit import FIXTURE_DOMAIN, _fixture_declaration

GOLDEN_DIR = emit.REPO_ROOT / "tests" / "fixtures" / "staging_increment"

LOOKBACK_MINUTES = 1440
EFFECTIVE_STAGING_COLUMN = "as_of_ts"
NATURAL_KEY = ["source_id"]


# ---------------------------------------------------------------------------
# A scratch estate built from the toy fixture domain, with one table carrying a
# staging increment block. Nothing here reads the committed declarations, so the
# committed estate keeps regenerating byte-identically alongside these fixtures.
# ---------------------------------------------------------------------------


def _windowed_declaration() -> dict:
    """The toy fixture declaration with an as-of timestamp and a declared block."""
    declaration = _fixture_declaration()
    table = declaration["tables"]["things"]
    table["projection"].append(
        {"name": EFFECTIVE_STAGING_COLUMN, "expression": f"cast({EFFECTIVE_STAGING_COLUMN} as timestamp)"}
    )
    for vault in table["vault_entities"]:
        for column in vault["bridge"]["select"]:
            if column["name"] == emit.EFFECTIVE_COLUMN:
                column["expression"] = f"source.{EFFECTIVE_STAGING_COLUMN}"
    table["model_tests"] = [{"name": "source_id", "data_tests": ["unique", "not_null"]}]
    table["natural_key"] = list(NATURAL_KEY)
    table["staging_increment"] = {
        "lookback_minutes": LOOKBACK_MINUTES,
        "effective_advances_on_redelivery": True,
    }
    return declaration


def _null_effective_sibling(declaration: dict) -> None:
    """A second table on the same source whose satellite is fed a null effective time."""
    declaration["tables"]["snapshots"] = {
        "raw_model": "raw_toysrc_snapshots",
        "description": "Toy sibling table whose bridge maps a null constant to effective_from.",
        "projection": [
            {"name": "source_system", "expression": "'toysrc'"},
            {"name": "source_id", "expression": "cast(id as string)"},
            {"name": "beta_name", "expression": "cast(beta_name as string)"},
        ],
        "vault_entities": [
            {
                "name": "beta_snapshot",
                "entity": "beta",
                "bridge_model": "br_dv_toysrc_beta_snapshot",
                "stage_model": "stg_dv_toysrc_beta_snapshot",
                "satellite_model": "sat_beta_snapshot_toysrc",
                "bridge": {
                    "resolutions": [],
                    "select": [
                        {"name": "source_id", "expression": "source.source_id"},
                        {"name": "beta_name", "expression": "source.beta_name"},
                        {"name": emit.EFFECTIVE_COLUMN, "expression": "cast(null as timestamp)"},
                        {"name": "golden_beta_key", "expression": "source.source_id"},
                    ]
                },
            }
        ],
    }


def _write_estate(root: Path, declaration: dict) -> "emit.EstateContext":
    domains_dir = root / "domains"
    domains_dir.mkdir(parents=True, exist_ok=True)
    (domains_dir / "fixture.yml").write_text(
        yaml.safe_dump(FIXTURE_DOMAIN, sort_keys=False), encoding="utf-8"
    )
    declarations_dir = root / "declarations"
    declarations_dir.mkdir(parents=True, exist_ok=True)
    (declarations_dir / "toysrc.yml").write_text(
        yaml.safe_dump(declaration, sort_keys=False), encoding="utf-8"
    )
    return emit.EstateContext.resolve(
        estate_root=root, domains_dir=domains_dir, declarations_dir=declarations_dir
    )


def _load(root: Path, declaration: dict) -> list[dict]:
    ctx = _write_estate(root, declaration)
    return emit.load_declarations(emit.load_domains(root / "domains"), ctx=ctx)


def _watermark(declaration: dict | None = None) -> dict:
    """The table-local satellite layout the toy fixture's floor is resolved from."""
    with tempfile.TemporaryDirectory() as tmp:
        loaded = _load(Path(tmp), declaration or _windowed_declaration())
    return emit.source_satellite_layout(loaded[0], "things", "toysrc.yml")


def _generate(
    root: Path, declaration: dict, *, typed: object | None = None
) -> tuple[dict[str, str], str]:
    """Generate the whole scratch estate; return the files by name and what emit printed."""
    ctx = _write_estate(root, declaration)
    domain = emit.load_domains(root / "domains")
    declarations = emit.load_declarations(domain, ctx=ctx)
    printed = io.StringIO()
    with contextlib.redirect_stdout(printed):
        files = emit.generate_files(
            declarations, emit.template_env(), domain, ctx=ctx, typed=typed
        )
    return {file.path.name: file.content for file in files}, printed.getvalue()


def _raises(callable_, *, where: str) -> str:
    try:
        callable_()
    except ValueError as error:
        return str(error)
    raise AssertionError(f"{where}: expected a ValueError, none raised")


# ---------------------------------------------------------------------------
# The closed block
# ---------------------------------------------------------------------------


def test_the_block_is_closed_and_rejects_an_unknown_key() -> None:
    """The block carries the lookback and the acknowledgment, nothing else: the unique
    key and the window column are derived, so an authored key here is a mistake."""
    declaration = _windowed_declaration()
    declaration["tables"]["things"]["staging_increment"]["unique_key"] = ["source_id"]
    with tempfile.TemporaryDirectory() as tmp:
        message = _raises(
            lambda: _load(Path(tmp), declaration), where="unknown staging_increment key"
        )
    assert "unknown field(s): unique_key" in message, message
    assert "closed" in message, message


def test_a_non_positive_lookback_fails_with_a_named_error() -> None:
    """The lookback is a positive integer number of minutes."""
    for value in (0, -30, "1440", 1.5, None):
        declaration = _windowed_declaration()
        declaration["tables"]["things"]["staging_increment"]["lookback_minutes"] = value
        with tempfile.TemporaryDirectory() as tmp:
            message = _raises(
                lambda: _load(Path(tmp), declaration), where=f"lookback {value!r}"
            )
        assert "lookback_minutes" in message, message
        assert "positive integer" in message, message


def test_a_missing_or_false_acknowledgment_fails_with_a_named_error() -> None:
    """The acknowledgment records the author's judgment that the effective column
    advances on redelivery. Without it the block is refused: a static or one-off
    effective date silently loses a redelivered update below the lookback floor."""
    for mutate in (
        lambda block: block.pop("effective_advances_on_redelivery"),
        lambda block: block.update({"effective_advances_on_redelivery": False}),
    ):
        declaration = _windowed_declaration()
        mutate(declaration["tables"]["things"]["staging_increment"])
        with tempfile.TemporaryDirectory() as tmp:
            message = _raises(
                lambda: _load(Path(tmp), declaration), where="acknowledgment"
            )
        assert "effective_advances_on_redelivery" in message, message
        assert "advances on redelivery" in message, message


# ---------------------------------------------------------------------------
# The natural key and the derived staging key
# ---------------------------------------------------------------------------


def test_a_declared_block_without_a_natural_key_fails_with_a_named_error() -> None:
    """natural_key is the source of truth for the derived staging key, so a table
    carrying the block declares it and never leans on inference."""
    declaration = _windowed_declaration()
    declaration["tables"]["things"].pop("natural_key")
    with tempfile.TemporaryDirectory() as tmp:
        message = _raises(lambda: _load(Path(tmp), declaration), where="missing natural_key")
    assert "natural_key" in message, message
    assert "staging_increment" in message, message


def test_the_unique_key_is_the_natural_key_plus_the_effective_column() -> None:
    """The declaration authors only the lookback; the key is derived."""
    with tempfile.TemporaryDirectory() as tmp:
        files, _ = _generate(Path(tmp), _windowed_declaration())
    staging = files["stg_toysrc_things.sql"]
    assert f"unique_key=['source_id', '{EFFECTIVE_STAGING_COLUMN}']" in staging, staging
    schema = files["_sources.yml"]
    assert "combination_of_columns: ['source_id', 'as_of_ts']" in schema, schema
    staging_schema = schema.split("- name: stg_toysrc_things", 1)[1]
    source_id = staging_schema.split("- name: source_id", 1)[1].split("- name:", 1)[0]
    assert "unique" not in source_id, (
        "the natural key is no longer the incremental relation's grain"
    )

    configured = _windowed_declaration()
    configured_table = configured["tables"]["things"]
    configured_table["model_tests"][0]["data_tests"] = [
        {"unique": {"config": {"severity": "warn"}}},
        "not_null",
    ]
    with tempfile.TemporaryDirectory() as tmp:
        declarations = _load(Path(tmp), configured)
        emit.apply_staging_increments(declarations)
    configured_tests = declarations[0]["tables"]["things"]["model_tests"][0]["data_tests"]
    assert configured_tests == [
        {"not_null": {"config": {"severity": "error"}}}
    ], configured_tests


def test_the_staging_key_is_the_source_natural_key_a_resolution_join_consumes() -> None:
    """For a resolution-derived entity the staging key stays the per-source natural key
    the bridge's resolution joins consume: the golden hub key is bridge-computed and
    absent from staging."""
    with tempfile.TemporaryDirectory() as tmp:
        declarations = _load(Path(tmp), _windowed_declaration())
    table = declarations[0]["tables"]["things"]
    assert table["natural_key"] == NATURAL_KEY, table["natural_key"]
    conditions = table["vault_entities"][0]["bridge"]["resolutions"][0]["conditions"]
    joined = " ".join(conditions)
    for column in NATURAL_KEY:
        assert f"source.{column}" in joined, (
            f"the staging key member {column!r} must be a column the resolution join "
            f"consumes, got: {conditions}"
        )
    assert "golden_alpha_key" not in " ".join(table["natural_key"])


def test_the_natural_key_is_inferred_only_for_a_table_without_the_block() -> None:
    """A single unambiguous unique model test names the key for an undeclared table."""
    declaration = _windowed_declaration()
    table = declaration["tables"]["things"]
    table.pop("staging_increment")
    table.pop("natural_key")
    with tempfile.TemporaryDirectory() as tmp:
        declarations = _load(Path(tmp), declaration)
    assert declarations[0]["tables"]["things"]["natural_key"] == ["source_id"]


def test_two_candidate_keys_fail_with_a_named_error() -> None:
    """Two unique model tests leave the key ambiguous; the author declares it."""
    declaration = _windowed_declaration()
    table = declaration["tables"]["things"]
    table.pop("staging_increment")
    table.pop("natural_key")
    table["model_tests"].append({"name": "alpha_code", "data_tests": ["unique"]})
    with tempfile.TemporaryDirectory() as tmp:
        message = _raises(lambda: _load(Path(tmp), declaration), where="two candidate keys")
    assert "candidate natural keys" in message, message
    assert "alpha_code" in message and "source_id" in message, message


def test_a_multi_column_unique_test_fails_with_a_named_error() -> None:
    """A multi-column unique test is an ambiguity, not an inferred key."""
    declaration = _windowed_declaration()
    table = declaration["tables"]["things"]
    table.pop("staging_increment")
    table.pop("natural_key")
    table["model_tests"] = [
        {
            "name": "source_id",
            "data_tests": [
                {
                    "dbt_utils.unique_combination_of_columns": {
                        "arguments": {"combination_of_columns": ["source_id", "alpha_code"]}
                    }
                }
            ],
        }
    ]
    with tempfile.TemporaryDirectory() as tmp:
        message = _raises(lambda: _load(Path(tmp), declaration), where="multi-column unique")
    assert "multi-column unique test" in message, message
    assert "natural_key" in message, message


def test_generated_non_null_assertions_cover_every_key_member_at_error_severity() -> None:
    """A null key member under delete+insert strands or duplicates rows, so every
    derived key member -- the effective column included -- is pinned not null at error
    severity and halts the build before the satellites persist the batch."""
    with tempfile.TemporaryDirectory() as tmp:
        files, _ = _generate(Path(tmp), _windowed_declaration())
    document = yaml.safe_load(files["_sources.yml"])
    model = next(
        entry for entry in document["models"] if entry["name"] == "stg_toysrc_things"
    )
    tests_by_column = {column["name"]: column["data_tests"] for column in model["columns"]}
    for member in NATURAL_KEY + [EFFECTIVE_STAGING_COLUMN]:
        assert member in tests_by_column, f"{member} carries no generated assertion"
        assertions = [
            test
            for test in tests_by_column[member]
            if isinstance(test, dict) and "not_null" in test
        ]
        assert len(assertions) == 1, f"{member}: {tests_by_column[member]}"
        assert assertions[0]["not_null"]["config"]["severity"] == "error", assertions
        assert "not_null" not in tests_by_column[member], (
            f"{member}: exactly one not-null test stands, got {tests_by_column[member]}"
        )


# ---------------------------------------------------------------------------
# The derived effective column
# ---------------------------------------------------------------------------


def test_an_absent_effective_mapping_fails() -> None:
    declaration = _windowed_declaration()
    select = declaration["tables"]["things"]["vault_entities"][0]["bridge"]["select"]
    declaration["tables"]["things"]["vault_entities"][0]["bridge"]["select"] = [
        column for column in select if column["name"] != emit.EFFECTIVE_COLUMN
    ]
    with tempfile.TemporaryDirectory() as tmp:
        message = _raises(lambda: _generate(Path(tmp), declaration), where="absent mapping")
    assert emit.EFFECTIVE_COLUMN in message, message


def test_an_ambiguous_effective_mapping_fails() -> None:
    """Two bridges mapping different staging columns leave no single window column."""
    declaration = _windowed_declaration()
    for column in declaration["tables"]["things"]["vault_entities"][1]["bridge"]["select"]:
        if column["name"] == emit.EFFECTIVE_COLUMN:
            column["expression"] = "source.source_id"
    with tempfile.TemporaryDirectory() as tmp:
        message = _raises(lambda: _generate(Path(tmp), declaration), where="ambiguous mapping")
    assert "ambiguous" in message, message
    assert EFFECTIVE_STAGING_COLUMN in message and "source_id" in message, message


def test_a_constant_or_expression_effective_mapping_fails() -> None:
    """The window needs a column to prune on, so a constant and a transform both fail."""
    for expression in ("cast(null as timestamp)", "'2026-01-01'", "greatest(source.as_of_ts, source.source_id)"):
        declaration = _windowed_declaration()
        for column in declaration["tables"]["things"]["vault_entities"][0]["bridge"]["select"]:
            if column["name"] == emit.EFFECTIVE_COLUMN:
                column["expression"] = expression
        with tempfile.TemporaryDirectory() as tmp:
            message = _raises(
                lambda: _generate(Path(tmp), declaration), where=f"mapping {expression!r}"
            )
        assert "not a bare staging column" in message, message


def test_a_null_constant_on_a_sibling_table_cannot_hold_this_tables_watermark() -> None:
    """An independent table on the same source is absent from this table's watermark."""
    declaration = _windowed_declaration()
    _null_effective_sibling(declaration)
    with tempfile.TemporaryDirectory() as tmp:
        _, printed = _generate(Path(tmp), declaration)
    assert "sat_beta_snapshot_toysrc" not in printed, printed
    layout = _watermark(declaration)
    assert "sat_beta_snapshot_toysrc" not in {
        entry["relation"] for entry in layout["satellites"]
    }, layout


def _typed_stub(event_field: str, physical_columns: tuple = ()) -> SimpleNamespace:
    """A typed-declaration stand-in carrying one table's contract facts.

    It exposes exactly what generation reads: the tables map, the contract's
    timestamps.event_field, the landing's physical columns, and an empty production
    contract list (the fixture domain publishes no Bronze product).
    """
    return SimpleNamespace(
        tables={
            ("toysrc", "things"): SimpleNamespace(
                contract=SimpleNamespace(
                    delivery=SimpleNamespace(
                        timestamps=SimpleNamespace(event_field=event_field)
                    ),
                    landing=SimpleNamespace(physical_columns=physical_columns),
                )
            )
        },
        production_contracts=lambda: [],
    )


def test_a_bronze_event_field_mismatch_fails() -> None:
    """The delivery contract and the estate filter carry one time fact."""
    typed = _typed_stub("landed_at")
    with tempfile.TemporaryDirectory() as tmp:
        message = _raises(
            lambda: _generate(Path(tmp), _windowed_declaration(), typed=typed),
            where="event_field mismatch",
        )
    assert "timestamps.event_field" in message, message
    assert "landed_at" in message and EFFECTIVE_STAGING_COLUMN in message, message


def test_a_matching_bronze_event_field_passes() -> None:
    typed = _typed_stub(EFFECTIVE_STAGING_COLUMN)
    with tempfile.TemporaryDirectory() as tmp:
        files, _ = _generate(Path(tmp), _windowed_declaration(), typed=typed)
    assert "materialized='incremental'" in files["stg_toysrc_things.sql"]


def test_a_hand_authored_model_reading_a_windowed_relation_fails_naming_it() -> None:
    """A window-filtered stage or bridge holds the delta window only, so a hand-authored
    reader loses every row outside it."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        marts = root / "models" / "marts"
        marts.mkdir(parents=True)
        (marts / "mart_alpha_latest.sql").write_text(
            "select * from {{ ref('stg_dv_toysrc_alpha') }}\n", encoding="utf-8"
        )
        message = _raises(
            lambda: _generate(root, _windowed_declaration()), where="hand-authored reference"
        )
    assert "mart_alpha_latest" in message, message
    assert "stg_dv_toysrc_alpha" in message, message
    assert "window-filtered" in message, message


def test_a_generated_model_reading_a_windowed_relation_passes_the_gate() -> None:
    """The gate names hand-authored readers only: every generated layer carries the
    same window by construction."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        generated = root / "models" / "marts"
        generated.mkdir(parents=True)
        (generated / "mart_generated.sql").write_text(
            f"-- {emit.SQL_HEADER.split('-- ')[1]}\n"
            "select * from {{ ref('stg_dv_toysrc_alpha') }}\n",
            encoding="utf-8",
        )
        files, _ = _generate(root, _windowed_declaration())
    assert "stg_toysrc_things.sql" in files


def test_the_reference_gate_is_inert_without_a_declared_table() -> None:
    assert structure_gate.hand_authored_window_references(set()) == []


# ---------------------------------------------------------------------------
# The evolution ledger's type facts
# ---------------------------------------------------------------------------


def _typed_with_physical_type(logical_type: str) -> SimpleNamespace:
    return _typed_stub(
        EFFECTIVE_STAGING_COLUMN,
        (SimpleNamespace(name="alpha_name", logical_type=logical_type),),
    )


def test_a_physical_type_change_on_a_typed_column_grades_as_a_redefinition() -> None:
    """The ledger records a normalised logical output type per payload column wherever a
    type fact exists -- the Bronze contract's source-native schema included -- and a
    later change to it raises the estate migration requirement."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        declaration = _windowed_declaration()
        ctx = _write_estate(root, declaration)
        domain = emit.load_domains(root / "domains")
        declarations = emit.load_declarations(domain, ctx=ctx)
        files, _ = emit.grade_estate_evolution(
            domain, declarations, ctx=ctx, typed=_typed_with_physical_type("utf8_string")
        )
        emit.write_files(files, root=root)
        ledger = yaml.safe_load(
            emit.evolution_ledger_path("fixture", ctx=ctx).read_text(encoding="utf-8")
        )
        recorded = ledger["entities"]["alpha"]["column_types"]
        assert recorded["alpha_name"] == "utf8_string", recorded

        domain = emit.load_domains(root / "domains")
        declarations = emit.load_declarations(domain, ctx=ctx)
        try:
            emit.grade_estate_evolution(
                domain, declarations, ctx=ctx, typed=_typed_with_physical_type("int64")
            )
        except emit.EstateMigrationRequirement as error:
            message = str(error)
        else:
            raise AssertionError("a physical type change must raise the migration requirement")
    assert "redefinition" in message, message
    assert "alpha_name" in message, message
    assert "rebaseline" in message, message


# ---------------------------------------------------------------------------
# The generated SQL: both window shapes
# ---------------------------------------------------------------------------

GOLDEN_MODELS = (
    "stg_toysrc_things.sql",
    "br_dv_toysrc_alpha.sql",
    "stg_dv_toysrc_alpha.sql",
)


def golden_path(model_file_name: str) -> Path:
    """The golden file pinning one generated model.

    The goldens carry a .golden suffix in place of .sql: they are pinned expectations a
    Python test reads, and the estate's executable warehouse SQL is the only thing that
    carries a .sql name.
    """
    return GOLDEN_DIR / (model_file_name.removesuffix(".sql") + ".golden")


def test_golden_files_pin_the_generated_sql_for_both_window_shapes() -> None:
    """The incremental shape (the staging model) and the full-refresh-inert shape (its
    stage and bridge) are pinned byte-for-byte."""
    with tempfile.TemporaryDirectory() as tmp:
        files, _ = _generate(Path(tmp), _windowed_declaration())
    for name in GOLDEN_MODELS:
        golden = golden_path(name)
        assert golden.is_file(), f"missing golden file {golden}"
        expected = golden.read_text(encoding="utf-8")
        assert files[name] == expected, (
            f"{name} drifted from its golden file:\n--- golden ---\n{expected}"
            f"\n--- generated ---\n{files[name]}"
        )


def test_the_staging_model_carries_the_incremental_configuration() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        files, _ = _generate(Path(tmp), _windowed_declaration())
    staging = files["stg_toysrc_things.sql"]
    assert "materialized='incremental'" in staging, staging
    assert "incremental_strategy='delete+insert'" in staging, staging
    assert "on_schema_change='append_new_columns'" in staging, staging
    assert "incremental_predicates=[" in staging, staging
    assert "{% if is_incremental() %}" in staging, staging


def test_the_delete_predicate_is_built_at_execution_from_the_declared_window() -> None:
    """dbt captures a model's configuration while it parses the project, and parsing
    reads no warehouse state, so a floor resolved there is always the sentinel and a
    predicate built from it prunes nothing. The generated configuration states the window
    instead, and the incremental strategy builds the comparison at execution -- naming
    the destination alias each adapter picks for itself, dbt-core's DBT_INTERNAL_DEST or
    dbt-duckdb's DBT_INCREMENTAL_TARGET."""
    with tempfile.TemporaryDirectory() as tmp:
        files, _ = _generate(Path(tmp), _windowed_declaration())
    staging = files["stg_toysrc_things.sql"]
    assert (
        f"incremental_predicates=[{dbt_translator.DELETE_PREDICATE_MACRO}("
        f"'toysrc', 'things', '{EFFECTIVE_STAGING_COLUMN}', "
        f"'{dbt_translator.WINDOW_FLOOR_OPERATOR}', {LOOKBACK_MINUTES}, "
        in staging
    ), staging
    for spelling in ("DBT_INTERNAL_DEST", "DBT_INCREMENTAL_TARGET"):
        assert spelling not in staging, staging
    macros = (emit.REPO_ROOT / "macros" / "estate_evolution.sql").read_text(encoding="utf-8")
    for arm, alias in (
        ("default__dpf_incremental_target_alias", "DBT_INTERNAL_DEST"),
        ("duckdb__dpf_incremental_target_alias", "DBT_INCREMENTAL_TARGET"),
    ):
        assert f"macro {arm}() -%}}{alias}" in macros, arm
    assert "macro default__get_incremental_delete_insert_sql(arg_dict)" in macros, (
        "the delete-and-insert strategy resolves the declared window at execution"
    )
    assert "get_delete_insert_merge_sql(" in macros, (
        "the strategy delegates dbt's own delete and insert SQL unchanged"
    )


def test_the_staging_model_reports_its_floor_and_cumulative_window_rows() -> None:
    """A normal run names the cumulative relation metrics its post-hook measures."""
    with tempfile.TemporaryDirectory() as tmp:
        files, _ = _generate(Path(tmp), _windowed_declaration())
    staging = files["stg_toysrc_things.sql"]
    assert f"post_hook=[\"{{{{ {dbt_translator.WINDOW_ROWS_MACRO}(" in staging, staging
    macros = (emit.REPO_ROOT / "macros" / "estate_evolution.sql").read_text(encoding="utf-8")
    assert f"macro {dbt_translator.WINDOW_ROWS_MACRO}(" in macros, macros[:200]
    assert emit.WINDOW_ROWS_LOG_MARKER in macros
    assert '"relation_rows_total"' in macros
    assert '"relation_rows_in_window"' in macros
    assert '"rows_processed"' not in macros
    assert emit.WINDOW_FLOOR_LOG_MARKER in macros
    assert emit.WINDOW_AUDIT_RESULT_MARKER in macros


def test_the_stage_and_bridge_windows_are_inert_under_the_full_refresh_flag() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        files, _ = _generate(Path(tmp), _windowed_declaration())
    stage = files["stg_dv_toysrc_alpha.sql"]
    bridge = files["br_dv_toysrc_alpha.sql"]
    assert "{% if not flags.FULL_REFRESH %}" in stage, stage
    assert f"where {emit.EFFECTIVE_COLUMN} >= " in stage, stage
    assert "{% if flags.FULL_REFRESH %}true{% else %}" in bridge, bridge
    floor = dbt_translator.window_floor_expression(
        "toysrc", "things", EFFECTIVE_STAGING_COLUMN, LOOKBACK_MINUTES, _watermark()
    )
    assert stage.count(floor) == 1 and bridge.count(floor) == 1, (stage, bridge)


def test_the_watermark_constant_is_normalised_and_the_pruning_column_is_bare() -> None:
    """Every pruning column stands bare -- no cast and no function wrapper -- and the
    floor call names that same column as the type the constant is normalised to."""
    with tempfile.TemporaryDirectory() as tmp:
        files, _ = _generate(Path(tmp), _windowed_declaration())
    call = dbt_translator.window_floor_call(
        "toysrc", "things", EFFECTIVE_STAGING_COLUMN, LOOKBACK_MINUTES, _watermark()
    )
    assert f"'{EFFECTIVE_STAGING_COLUMN}'" in call, call
    for name, pruning_column in (
        ("stg_toysrc_things.sql", EFFECTIVE_STAGING_COLUMN),
        ("br_dv_toysrc_alpha.sql", f"source.{EFFECTIVE_STAGING_COLUMN}"),
        ("stg_dv_toysrc_alpha.sql", emit.EFFECTIVE_COLUMN),
    ):
        text = files[name]
        assert call in text, f"{name} must call the floor macro: {text}"
        marker = f"{pruning_column} {dbt_translator.WINDOW_FLOOR_OPERATOR} "
        assert marker in text, f"{name} must compare the bare column: {text}"
        before = text.split(marker)[0]
        tail = before[-len(pruning_column) - 8:]
        assert "cast(" not in tail.lower(), (
            f"{name}: the pruning column is left bare, got {tail!r}"
        )


def test_the_floor_comparison_operator_is_pinned() -> None:
    """The delta window is closed at the floor: the emitter stamps >= and nothing else."""
    assert dbt_translator.WINDOW_FLOOR_OPERATOR == ">="
    with tempfile.TemporaryDirectory() as tmp:
        files, _ = _generate(Path(tmp), _windowed_declaration())
    for name in GOLDEN_MODELS:
        text = files[name]
        for line in text.splitlines():
            if dbt_translator.WINDOW_FLOOR_MACRO not in line:
                continue
            assert ">=" in line, f"{name}: {line}"
            assert " > " not in line and " <" not in line, f"{name}: {line}"


def test_an_undeclared_table_regenerates_without_a_window() -> None:
    """A table without the block keeps today's full-recompute shape exactly."""
    declaration = _windowed_declaration()
    declaration["tables"]["things"].pop("staging_increment")
    with tempfile.TemporaryDirectory() as tmp:
        files, _ = _generate(Path(tmp), declaration)
    for name in GOLDEN_MODELS:
        text = files[name]
        assert dbt_translator.WINDOW_FLOOR_MACRO not in text, f"{name}: {text}"
        assert "is_incremental()" not in text, f"{name}: {text}"
        assert "FULL_REFRESH" not in text, f"{name}: {text}"
    assert "config(" not in files["stg_toysrc_things.sql"]


def test_the_committed_estate_regenerates_byte_identically() -> None:
    """Every committed table is undeclared, so emit --check stays clean."""
    old_argv = sys.argv
    out = io.StringIO()
    try:
        sys.argv = ["emit.py", "--check"]
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(io.StringIO()):
            exit_code = emit.main()
    finally:
        sys.argv = old_argv
    printed = out.getvalue()
    assert exit_code == 0, f"expected a clean --check, got {exit_code}:\n{printed}"
    changed = [line for line in printed.splitlines() if line.startswith("would change ")]
    assert changed and changed[-1].startswith("would change 0 of "), printed
    assert "ORPHANS=0" in printed, printed


def test_no_template_decides_a_window() -> None:
    """Semantic validation never lives in a template: the .j2 files render text emit
    already decided, and carry no lookback, key derivation or floor construction."""
    templates = sorted((emit.REPO_ROOT / "ergasterion" / "templates").glob("*.j2"))
    assert templates, "expected the packaged Jinja templates to exist"
    for template in templates:
        text = template.read_text(encoding="utf-8")
        for token in (
            "lookback_minutes",
            "effective_advances_on_redelivery",
            dbt_translator.WINDOW_FLOOR_MACRO,
            "incremental_predicates",
            "natural_key",
        ):
            assert token not in text, (
                f"{template.name} carries {token!r}; the window decision belongs to "
                "ergasterion.emit"
            )



# ---------------------------------------------------------------------------
# The consumption watermark: what the macro is, and what it never does
#
# The macro file is the source of truth for the resolution, so the contract clauses it
# has to keep are read from it here. A live lane further down executes the whole thing.
# ---------------------------------------------------------------------------

MACRO_PATH = emit.REPO_ROOT / "macros" / "estate_evolution.sql"
WATERMARK_SECTION_HEADING = "Watermark increments: the consumption watermark"


def _macro_text() -> str:
    return MACRO_PATH.read_text(encoding="utf-8")


def _watermark_section() -> str:
    """The half of the macro file the delta window lives in."""
    text = _macro_text()
    index = text.find(WATERMARK_SECTION_HEADING)
    assert index > 0, f"{MACRO_PATH} carries no {WATERMARK_SECTION_HEADING!r} section"
    # From the comment block that opens the divider, so a comment is never half-read.
    return text[text.rindex("{#", 0, index) :]


def test_the_consumption_watermark_is_the_least_satellite_maximum_coalesced_to_the_sentinel() -> None:
    """The watermark for a source table is the least value, across that table's
    satellites, of each satellite's maximum effective time, coalesced to the
    initial-load sentinel."""
    section = _watermark_section()
    assert 'macro dpf_window_sentinel() -%}1900-01-01' in section, section[:400]
    assert 'least(" ~ (maxima | join(", ")) ~ ")"' in section, section
    assert '"(select coalesce(max(" ~ effective ~ "), " ~ sentinel ~ ") from "' in section, section


def test_a_missing_satellite_resolves_the_watermark_to_the_sentinel() -> None:
    """A satellite that does not exist yet has absorbed nothing, so the whole watermark
    falls to the sentinel and the window is unbounded below."""
    section = _watermark_section()
    resolve = section.split("macro dpf_resolve_window_floor(")[1].split("{%- endmacro -%}")[0]
    assert "{%- if relation is none -%}" in resolve, resolve
    assert "{%- do return(sentinel) -%}" in resolve, resolve


def test_the_watermark_resolves_every_satellite_through_the_adapter_under_an_execute_guard() -> None:
    """Every satellite is resolved through adapter.get_relation under an execute guard,
    on the coordinates the manifest already carries, and read through run_query."""
    section = _watermark_section()
    resolver = section.split("macro dpf_manifest_relation(model_name)")[1].split(
        "{%- endmacro -%}"
    )[0]
    assert "{%- if execute -%}" in resolver, resolver
    assert "adapter.get_relation(" in resolver, resolver
    assert "graph.nodes.values()" in resolver, resolver
    assert "run_query(query)" in section, section


def test_the_watermark_builds_no_relation_name_and_takes_no_ref_to_a_satellite() -> None:
    """A ref() from a windowed layer to a satellite would run from a staging layer to a
    satellite built from it, and dbt would refuse the project with a dependency cycle. No
    relation name is built by hand either: every one comes from the manifest and the
    adapter."""
    section = _watermark_section()
    # The comments name the rule, so the code is read without them.
    resolution = re.sub(r"\{#.*?#\}", "", section.split("The audit invocation")[0], flags=re.S)
    assert "ref(" not in resolution, (
        "the watermark resolution must take no ref(): that edge is a dependency cycle"
    )
    for built_by_hand in ("api.Relation.create", "target.schema ~", '~ "_raw_vault"'):
        assert built_by_hand not in resolution, built_by_hand


def test_the_per_satellite_maximum_carries_a_record_source_predicate() -> None:
    """Each satellite's maximum is taken under its own record-source literal, so a
    satellite that ever held a sibling source's rows could never advance this source's
    watermark."""
    section = _watermark_section()
    assert '" where record_source = " ~ dbt.string_literal(entry[\'record_source\'])' in section, section
    layout = _watermark()
    assert [entry["record_source"] for entry in layout["satellites"]] == [
        "TOYSRC_ALPHA",
        "TOYSRC_BETA",
    ], layout


def test_the_layout_carries_the_literal_the_satellite_actually_stores() -> None:
    """AutomateDV reads a leading '!' on a derived column as the marker for a constant
    and writes the text after it, so the predicate binds to that text and not to the
    declared value with its marker."""
    layout = _watermark()
    for entry in layout["satellites"]:
        assert not entry["record_source"].startswith(emit.AUTOMATE_DV_CONSTANT_PREFIX), entry
    assert layout["effective_column"] == emit.EFFECTIVE_COLUMN, layout


def test_emit_asserts_the_table_local_satellite_layout_for_a_declared_table() -> None:
    """A declared table carries at least one enabled satellite, every satellite names a
    model and a record source, and no satellite is listed twice."""
    with tempfile.TemporaryDirectory() as tmp:
        loaded = _load(Path(tmp), _windowed_declaration())
    declaration = loaded[0]
    layout = emit.source_satellite_layout(declaration, "things", "toysrc.yml")
    assert [entry["relation"] for entry in layout["satellites"]] == [
        "sat_alpha_toysrc",
        "sat_beta_toysrc",
    ], layout

    stripped = copy.deepcopy(declaration)
    stripped["tables"]["things"]["vault_entities"][0].pop("record_source")
    message = _raises(
        lambda: emit.source_satellite_layout(stripped, "things", "toysrc.yml"),
        where="satellite without a record source",
    )
    assert "record_source" in message and "satellite_model" in message, message

    columnar = copy.deepcopy(declaration)
    columnar["tables"]["things"]["vault_entities"][0]["record_source"] = "record_source"
    message = _raises(
        lambda: emit.source_satellite_layout(columnar, "things", "toysrc.yml"),
        where="record source naming a column",
    )
    assert "names a column" in message, message

    doubled = copy.deepcopy(declaration)
    doubled["tables"]["things"]["vault_entities"][1]["satellite_model"] = "sat_alpha_toysrc"
    message = _raises(
        lambda: emit.source_satellite_layout(doubled, "things", "toysrc.yml"),
        where="satellite declared twice",
    )
    assert "declared twice" in message, message

    emptied = copy.deepcopy(declaration)
    for vault in emptied["tables"]["things"]["vault_entities"]:
        vault["enabled"] = False
    message = _raises(
        lambda: emit.source_satellite_layout(emptied, "things", "toysrc.yml"),
        where="source with no enabled satellite",
    )
    assert "at least one enabled satellite" in message, message


def test_a_satellite_fed_by_two_sources_fails_the_isolation_assert() -> None:
    """The satellites are per-source by construction, and that is the primary guarantee
    of source isolation. A satellite named by two sources breaks it, so emit stops."""
    declaration = _windowed_declaration()
    sibling = copy.deepcopy(declaration)
    sibling["source"] = {"name": "othersrc", "display_name": "OTHERSRC"}
    sibling["tables"]["things"]["vault_entities"][0]["satellite_model"] = "sat_alpha_toysrc"
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        ctx = _write_estate(root, declaration)
        (root / "declarations" / "othersrc.yml").write_text(
            yaml.safe_dump(sibling, sort_keys=False), encoding="utf-8"
        )
        domain = emit.load_domains(root / "domains")
        declarations = emit.load_declarations(domain, ctx=ctx)
        message = _raises(
            lambda: emit.apply_staging_increments(declarations),
            where="satellite fed by two sources",
        )
    assert "is fed by table routes" in message, message
    assert "sat_alpha_toysrc" in message, message
    assert "consumption watermark" in message, message


def test_the_replay_suppression_bounds_its_existing_side_scan_with_the_window_floor() -> None:
    """A candidate row collides only with a stored row carrying the same effective time,
    so stored rows below the lowest candidate effective time can never collide. Where the
    table carries a window, that lowest candidate effective time is the window floor."""
    shared = (emit.REPO_ROOT / "macros" / "automate_dv_snowflake.sql").read_text(
        encoding="utf-8"
    )
    assert "macro dpf_sat_window_floor(src_eff, source_model)" in shared, shared[:400]
    assert '"(select min(" ~ column ~ ") from " ~ staged ~ ")"' in shared, shared
    suppression = shared.split("macro dpf_sat_replay_suppression(")[1]
    assert "{%- set window_floor = dpf_sat_window_floor(src_eff, source_model) -%}" in suppression
    assert "{%- if window_floor is not none %}" in suppression, suppression
    bound = shared.split("macro dpf_sat_window_floor(")[1].split("{%- endmacro -%}")[0]
    assert "{%- if execute -%}" in bound, bound
    assert "null_effective == 0" in bound, bound
    assert "effective_columns | length == 1" in bound, bound


def test_a_declared_table_whose_projection_omits_the_effective_column_fails() -> None:
    """Every layer of the window filters on the effective column, so a declaration whose
    staging projection never outputs it would emit clean, parse clean and fail on the
    executed build with a missing column. It fails at emit instead, naming the column."""
    declaration = _windowed_declaration()
    table = declaration["tables"]["things"]
    table["projection"] = [
        column for column in table["projection"] if column["name"] != EFFECTIVE_STAGING_COLUMN
    ]
    with tempfile.TemporaryDirectory() as tmp:
        message = _raises(
            lambda: _generate(Path(tmp), declaration), where="effective column not projected"
        )
    assert EFFECTIVE_STAGING_COLUMN in message, message
    assert "staging projection" in message, message


# ---------------------------------------------------------------------------
# The audit invocation
# ---------------------------------------------------------------------------


def test_the_audit_plan_names_the_landing_relation_and_its_columns() -> None:
    """The audit scans the full source, which is the landing relation, so it resolves the
    landing column behind each derived key member and behind the effective column."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        ctx = _write_estate(root, _windowed_declaration())
        plan = emit.window_audit_plan("toysrc", "things", ctx=ctx)
    assert plan["landing"] == {"kind": "model", "model": "raw_toysrc_things"}, plan
    assert plan["landing_key"] == ["id"], plan
    assert plan["landing_effective_column"] == EFFECTIVE_STAGING_COLUMN, plan
    assert plan["effective_column"] == EFFECTIVE_STAGING_COLUMN, plan
    assert plan["lookback_minutes"] == LOOKBACK_MINUTES, plan
    assert plan["watermark"] == _watermark(), plan


def test_the_audit_plan_refuses_a_table_without_a_declared_window() -> None:
    declaration = _windowed_declaration()
    declaration["tables"]["things"].pop("staging_increment")
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        ctx = _write_estate(root, declaration)
        message = _raises(
            lambda: emit.window_audit_plan("toysrc", "things", ctx=ctx),
            where="table without a window",
        )
    assert "no window to audit" in message, message


def test_the_audit_plan_refuses_a_projection_it_cannot_follow() -> None:
    """A projection expression reading more than one landing column leaves the audit with
    no column to group or compare on, so it stops naming the column."""
    declaration = _windowed_declaration()
    for column in declaration["tables"]["things"]["projection"]:
        if column["name"] == EFFECTIVE_STAGING_COLUMN:
            column["expression"] = "greatest(as_of_ts, landed_at)"
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        ctx = _write_estate(root, declaration)
        message = _raises(
            lambda: emit.window_audit_plan("toysrc", "things", ctx=ctx),
            where="projection the audit cannot follow",
        )
    assert "no single landing column" in message, message
    assert EFFECTIVE_STAGING_COLUMN in message, message


def test_the_landing_column_extractor_reads_the_three_shapes_it_accepts() -> None:
    for expression, expected in (
        ("sourced_date", "sourced_date"),
        ("cast(deal_id as string)", "deal_id"),
        ("{{ dpf_safe_cast('sourced_date', 'date') }}", "sourced_date"),
        ("{{ blank_to_null('external_deal_id') }}", "external_deal_id"),
    ):
        assert emit.landing_column_for(expression, "where", "column") == expected, expression


def test_the_audit_invocation_is_a_named_operator_command() -> None:
    """`ergasterion evolve audit-window <source> <table>` is the command an operator
    runs, and the CLI routes it beside the re-baseline operations."""
    parser = emit._evolve_parser()
    parsed = parser.parse_args(["audit-window", "toysrc", "things"])
    assert parsed.operation == "audit-window"
    assert parsed.source == "toysrc" and parsed.table == "things"
    assert parsed.sample == 20
    assert "audit-window" in cli._usage() or "audit-window" in (cli.__doc__ or ""), cli.__doc__


# ---------------------------------------------------------------------------
# The live lane: the windowed build executed on DuckDB
#
# One scratch copy of the committed estate declares a staging increment block on one
# table and carries every phase below, in order: the initial build, the windowed second
# build, the full-refresh reading, the sibling-source proof and the audit invocation. The
# copy has its own DuckDB file, its own dbt target directory and its own declarations, so
# nothing here touches the committed tree or the engine's own warehouse.
# ---------------------------------------------------------------------------

LIVE_SOURCE = "origo"
LIVE_TABLE = "deals"
LIVE_EFFECTIVE_COLUMN = "sourced_date"
LIVE_NATURAL_KEY = "source_record_id"
LIVE_LANDING_KEY = "deal_id"
LIVE_SATELLITES = (
    "sat_deal_origo",
    "sat_deal_target_company_origo",
    "sat_deal_fund_conversion_origo",
)
LIVE_TABLE_SATELLITES = ("sat_deal_origo",)
LIVE_LINK_TABLE = "deal_links"
LIVE_LINK_SATELLITES = (
    "sat_deal_target_company_origo",
    "sat_deal_fund_conversion_origo",
)
LIVE_LINK_ANCHOR_SATELLITE = "sat_deal_target_company_origo"
LIVE_LINK_ANCHOR_RECORD_SOURCE = "ORIGO_DEAL_TARGET_COMPANY"
# The satellite whose maximum effective time is the least of the three, so it is the one
# the consumption watermark is anchored on.
LIVE_ANCHOR_SATELLITE = "sat_deal_origo"
LIVE_ANCHOR_RECORD_SOURCE = "ORIGO_DEALS"
LIVE_SELECTION = tuple(f"+{name}" for name in LIVE_SATELLITES)
LIVE_STAGING_MODEL = "stg_origo_deals"
LIVE_WINDOWED_MODELS = ("stg_origo_deals", "br_dv_origo_deals", "stg_dv_origo_deals")
LIVE_LINK_STAGING_MODEL = "stg_origo_deal_links"

LIVE: dict = {}

LIVE_COPY_DIRS = (
    "models",
    "seeds",
    "macros",
    "declarations",
    "domains",
    "profiles",
    "contracts",
    "dbt_packages",
)
LIVE_COPY_FILES = ("dbt_project.yml", "packages.yml", "package-lock.yml", "estate.yml")


def _live_estate() -> Path:
    """The scratch estate copy, created on first use and reused by every phase."""
    if "root" in LIVE:
        return LIVE["root"]
    holder = tempfile.mkdtemp(prefix="dpf-watermark-")
    LIVE["holder"] = holder
    root = Path(holder) / "estate"
    root.mkdir(parents=True)
    for name in LIVE_COPY_DIRS:
        shutil.copytree(emit.REPO_ROOT / name, root / name)
    for name in LIVE_COPY_FILES:
        shutil.copy2(emit.REPO_ROOT / name, root / name)
    (root / "tests").mkdir()
    for assertion in sorted((emit.REPO_ROOT / "tests").glob("*.sql")):
        shutil.copy2(assertion, root / "tests" / assertion.name)
    LIVE["root"] = root
    LIVE["duckdb"] = root / "scratch.duckdb"
    return root


def _live_cleanup() -> None:
    holder = LIVE.pop("holder", None)
    LIVE.clear()
    if holder:
        shutil.rmtree(holder, ignore_errors=True)


def _live_env() -> dict:
    environment = dict(os.environ)
    environment["DPF_DUCKDB_PATH"] = str(LIVE["duckdb"])
    return environment


def _declare_the_window() -> None:
    """Declare two independent windowed table chains on one source."""
    path = _live_estate() / "declarations" / f"{LIVE_SOURCE}.yml"
    document = yaml.safe_load(path.read_text(encoding="utf-8"))
    deals = document["tables"][LIVE_TABLE]
    links = copy.deepcopy(deals)
    links["description"] = "Independent link chain over the same ORIGO delivery."
    links["vault_entities"] = deals["vault_entities"][1:]
    deals["vault_entities"] = deals["vault_entities"][:1]
    for table in (deals, links):
        table[emit.NATURAL_KEY_FIELD] = LIVE_NATURAL_KEY
        table[emit.STAGING_INCREMENT_KEY] = {
            "lookback_minutes": LOOKBACK_MINUTES,
            "effective_advances_on_redelivery": True,
        }
    document["tables"][LIVE_LINK_TABLE] = links
    path.write_text(yaml.safe_dump(document, sort_keys=False), encoding="utf-8", newline="\n")


def _live_emit() -> None:
    root = _live_estate()
    old_argv = sys.argv
    out = io.StringIO()
    try:
        sys.argv = ["emit.py", "--estate-root", str(root)]
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(io.StringIO()):
            exit_code = emit.main()
    finally:
        sys.argv = old_argv
    assert exit_code == 0, f"emit on the scratch estate failed:\n{out.getvalue()}"


def _run_dbt(*arguments: str) -> str:
    """One dbt invocation against the scratch estate, on its own DuckDB file."""
    root = _live_estate()
    log = root / "logs" / "dbt.log"
    if log.exists():
        log.unlink()
    command = [
        emit.resolve_dbt_executable(),
        "--no-use-colors",
        *arguments,
        "--project-dir",
        str(root),
        "--profiles-dir",
        str(root / "profiles"),
        "-t",
        "duckdb",
    ]
    completed = subprocess.run(
        command,
        cwd=str(root),
        env=_live_env(),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    output = f"{completed.stdout}\n{completed.stderr}"
    if completed.returncode != 0:
        raise AssertionError(
            f"dbt {' '.join(arguments)} failed with exit code {completed.returncode}:\n"
            + "\n".join(output.splitlines()[-30:])
        )
    return log.read_text(encoding="utf-8", errors="replace") if log.exists() else output


def _logged(log: str, marker: str) -> list[dict]:
    """Every record the run logged on one marker line."""
    records = []
    for line in log.splitlines():
        index = line.find(marker)
        if index >= 0:
            records.append(json.loads(line[index + len(marker) :].strip()))
    return records


def _compiled(model: str) -> str:
    root = _live_estate()
    matches = sorted((root / "target" / "compiled").rglob(f"{model}.sql"))
    assert matches, f"the scratch estate compiled no {model}"
    return matches[0].read_text(encoding="utf-8")


def _live_connection(read_only: bool = True):
    import duckdb

    return duckdb.connect(str(LIVE["duckdb"]), read_only=read_only)


def test_live_the_declared_estate_emits_and_builds_on_duckdb() -> None:
    """The initial load: satellites hold nothing, so the watermark is the sentinel, the
    window is unbounded below and all history builds."""
    _declare_the_window()
    _live_emit()
    log = _run_dbt("build", "--select", *LIVE_SELECTION, "--exclude", "test_type:singular")
    floors = _logged(log, emit.WINDOW_FLOOR_LOG_MARKER)
    assert floors, "the initial build logged no window floor"
    assert all(record["source"] == LIVE_SOURCE for record in floors), floors
    assert all("1900-01-01" in record["floor"] for record in floors), (
        "an empty satellite has absorbed nothing, so the initial window floor is the "
        f"sentinel: {floors}"
    )
    connection = _live_connection()
    try:
        counts = {
            name: connection.execute(
                f"select count(*) from main_raw_vault.{name}"
            ).fetchone()[0]
            for name in LIVE_SATELLITES
        }
    finally:
        connection.close()
    assert all(count > 0 for count in counts.values()), counts
    LIVE["initial_counts"] = counts


def test_live_one_invocation_stamps_one_floor_inside_each_independent_table_chain() -> None:
    """Each table's staging, bridge and stage models resolve one table-local floor."""
    log = _run_dbt("build", "--select", *LIVE_SELECTION, "--exclude", "test_type:singular")
    LIVE["second_log"] = log
    floors = _logged(log, emit.WINDOW_FLOOR_LOG_MARKER)
    by_model: dict[str, set] = {}
    for record in floors:
        by_model.setdefault(record["model"], set()).add(record["floor"])
    for model in LIVE_WINDOWED_MODELS:
        assert model in by_model, (f"{model} stamped no window floor", sorted(by_model))
    by_table: dict[str, set] = {}
    for record in floors:
        by_table.setdefault(record["table"], set()).add(record["floor"])
    assert set(by_table) == {LIVE_TABLE, LIVE_LINK_TABLE}, by_table
    assert all(len(values) == 1 for values in by_table.values()), (
        f"each independent chain must stamp one floor, got {by_table}"
    )
    floor = next(iter(by_table[LIVE_TABLE]))
    assert "1900-01-01" not in floor, (
        f"the satellites carry history now, so the floor is a resolved watermark: {floor}"
    )
    LIVE["floor"] = floor
    LIVE["link_floor"] = next(iter(by_table[LIVE_LINK_TABLE]))


def test_live_the_floor_resolution_is_cached_under_the_source_table() -> None:
    """The cache is keyed on the source table, so the floor calls inside one render share
    one resolution. The staging model calls for the floor three times -- in its window, in
    its bounded delete predicate and in its run report -- and resolves it fewer times than
    that. dbt renders a model's body and its materialization separately, which is what
    makes per-model resolution the standing fallback, and the fallback's cost is bounded:
    one resolution per windowed render, and one lookup per satellite of the source inside
    it."""
    section = _watermark_section()
    assert "'dpf_window_floor:' ~ source_name ~ '.' ~ table_name" in section, section
    assert "load_result(key)" in section and "store_result(key," in section, section

    generated = sorted((_live_estate() / "models").rglob(f"{LIVE_STAGING_MODEL}.sql"))
    assert generated, f"the scratch estate generated no {LIVE_STAGING_MODEL}"
    text = generated[0].read_text(encoding="utf-8")
    calls = sum(
        text.count(f"{macro}(")
        for macro in (
            dbt_translator.WINDOW_FLOOR_MACRO,
            dbt_translator.DELETE_PREDICATE_MACRO,
            dbt_translator.WINDOW_ROWS_MACRO,
        )
    )
    assert calls == 3, (calls, text[:400])

    floors = _logged(LIVE["second_log"], emit.WINDOW_FLOOR_LOG_MARKER)
    resolutions = [
        record for record in floors if record["model"] == LIVE_STAGING_MODEL
    ]
    assert 1 <= len(resolutions) < calls, (
        f"{LIVE_STAGING_MODEL} calls for the floor {calls} times and must resolve it "
        f"fewer times than that, got {len(resolutions)}"
    )
    assert all(
        record["satellites"]
        == (len(LIVE_TABLE_SATELLITES) if record["table"] == LIVE_TABLE else len(LIVE_LINK_SATELLITES))
        for record in floors
    ), ("one resolution reads only that table's satellites", floors)


def test_live_each_floor_is_the_least_table_satellite_maximum_minus_the_lookback() -> None:
    """Each table derives its floor only from its own downstream satellites."""
    connection = _live_connection()
    try:
        maxima = {
            name: connection.execute(
                f"select max(effective_from) from main_raw_vault.{name}"
            ).fetchone()[0]
            for name in LIVE_TABLE_SATELLITES
        }
    finally:
        connection.close()
    watermark = min(maxima.values())
    expected = watermark - datetime.timedelta(minutes=LOOKBACK_MINUTES)
    assert f"'{expected.isoformat()}'" in LIVE["floor"], (maxima, LIVE["floor"])
    assert maxima[LIVE_ANCHOR_SATELLITE] == watermark, maxima


def test_live_the_three_layers_carry_the_window_predicate() -> None:
    """The staging model filters its source, and its bridge and stage filter the same
    window, so the vault layer receives only the delta."""
    for model, pruning_column in (
        ("stg_origo_deals", LIVE_EFFECTIVE_COLUMN),
        ("br_dv_origo_deals", f"source.{LIVE_EFFECTIVE_COLUMN}"),
        ("stg_dv_origo_deals", emit.EFFECTIVE_COLUMN),
    ):
        text = _compiled(model)
        predicate = f"{pruning_column} {dbt_translator.WINDOW_FLOOR_OPERATOR} {LIVE['floor']}"
        assert predicate in text, (model, predicate, text[-600:])


def test_live_the_replay_suppression_scans_only_the_window() -> None:
    """The satellite's existing-side scan is bounded by the window floor on the executed
    build: the stage the satellite is built from is a windowed recompute, so its lowest
    effective time is the window floor, and a stored row below it can never collide with
    a candidate."""
    existing_bound = (
        f"existing.{emit.EFFECTIVE_COLUMN} {dbt_translator.WINDOW_FLOOR_OPERATOR}"
    )
    for satellite in LIVE_SATELLITES:
        text = _compiled(satellite)
        assert "AS existing" in text, (satellite, text[-400:])
        assert existing_bound in text, (satellite, text[-800:])
        floor_index = text.find(f"(select min({emit.EFFECTIVE_COLUMN}) from ")
        assert floor_index > 0, (satellite, text[-800:])
        assert f"stg_dv_{LIVE_SOURCE}" in text[floor_index : floor_index + 200], (
            "the bound reads the windowed stage the satellite is built from",
            satellite,
            text[floor_index : floor_index + 200],
        )


def test_live_the_delete_side_is_bounded_by_the_window_floor() -> None:
    """dbt captures a model's configuration while it parses the project, so the bound the
    delete side carries is built at execution from the window the configuration states."""
    log = LIVE["second_log"]
    alias = "DBT_INCREMENTAL_TARGET"
    predicate = (
        f"{alias}.{LIVE_EFFECTIVE_COLUMN} "
        f"{dbt_translator.WINDOW_FLOOR_OPERATOR} {LIVE['floor']}"
    )
    assert predicate in log, (
        "the delete side must scan the window rather than cumulative history; the run "
        f"carried no {predicate!r}"
    )
    assert emit.WINDOW_FLOOR_LOG_MARKER in log


def test_live_the_run_reports_the_floor_and_cumulative_relation_rows() -> None:
    """A normal run reports honestly named relation counts for each declared table."""
    reports = _logged(LIVE["second_log"], emit.WINDOW_ROWS_LOG_MARKER)
    assert {report["table"] for report in reports} == {LIVE_TABLE, LIVE_LINK_TABLE}, reports
    report = next(report for report in reports if report["table"] == LIVE_TABLE)
    assert report["model"] == LIVE_STAGING_MODEL, report
    assert report["source"] == LIVE_SOURCE and report["table"] == LIVE_TABLE, report
    assert report["floor"] == LIVE["floor"], report
    assert report["relation_rows_total"] > 0 and report["relation_rows_in_window"] > 0, report


def test_live_a_sibling_source_cannot_advance_the_watermark() -> None:
    """The per-satellite maximum is taken under that satellite's own record-source
    literal, so rows written by another source are invisible to this source's watermark.
    The same row under the satellite's own record source moves the floor, which is what
    makes the predicate the thing holding it."""
    connection = _live_connection(read_only=False)
    try:
        columns = [
            row[0]
            for row in connection.execute(
                f"describe main_raw_vault.{LIVE_ANCHOR_SATELLITE}"
            ).fetchall()
        ]
        projection = ", ".join(
            "date '2099-01-01' as effective_from"
            if column == emit.EFFECTIVE_COLUMN
            else ("'DPF_SIBLING_SOURCE' as record_source" if column == "record_source" else column)
            for column in columns
        )
        connection.execute(
            f"insert into main_raw_vault.{LIVE_ANCHOR_SATELLITE} "
            f"select {projection} from main_raw_vault.{LIVE_ANCHOR_SATELLITE} limit 1"
        )
    finally:
        connection.close()

    log = _run_dbt("compile", "--select", LIVE_STAGING_MODEL)
    held = _logged(log, emit.WINDOW_FLOOR_LOG_MARKER)
    assert held and held[-1]["floor"] == LIVE["floor"], (
        "a sibling source's rows must not advance this source's watermark", held
    )

    connection = _live_connection(read_only=False)
    try:
        connection.execute(
            f"update main_raw_vault.{LIVE_ANCHOR_SATELLITE} "
            f"set record_source = '{LIVE_ANCHOR_RECORD_SOURCE}' "
            f"where record_source = 'DPF_SIBLING_SOURCE'"
        )
    finally:
        connection.close()
    log = _run_dbt("compile", "--select", LIVE_STAGING_MODEL)
    advanced = _logged(log, emit.WINDOW_FLOOR_LOG_MARKER)
    assert advanced and advanced[-1]["floor"] != LIVE["floor"], (
        "the same row under the satellite's own record source must advance the floor",
        advanced,
    )

    connection = _live_connection(read_only=False)
    try:
        connection.execute(
            f"delete from main_raw_vault.{LIVE_ANCHOR_SATELLITE} "
            f"where {emit.EFFECTIVE_COLUMN} = date '2099-01-01'"
        )
    finally:
        connection.close()


def test_live_an_independent_table_chain_cannot_advance_this_tables_floor() -> None:
    """Advancing a satellite on the sibling table moves only that table's floor."""
    connection = _live_connection(read_only=False)
    try:
        columns = [
            row[0]
            for row in connection.execute(
                f"describe main_raw_vault.{LIVE_LINK_ANCHOR_SATELLITE}"
            ).fetchall()
        ]
        projection = ", ".join(
            "date '2098-01-01' as effective_from"
            if column == emit.EFFECTIVE_COLUMN
            else (
                f"'{LIVE_LINK_ANCHOR_RECORD_SOURCE}' as record_source"
                if column == "record_source"
                else column
            )
            for column in columns
        )
        connection.execute(
            f"insert into main_raw_vault.{LIVE_LINK_ANCHOR_SATELLITE} "
            f"select {projection} from main_raw_vault.{LIVE_LINK_ANCHOR_SATELLITE} limit 1"
        )
    finally:
        connection.close()

    log = _run_dbt("compile", "--select", LIVE_STAGING_MODEL, LIVE_LINK_STAGING_MODEL)
    records = _logged(log, emit.WINDOW_FLOOR_LOG_MARKER)
    by_table = {record["table"]: record["floor"] for record in records}
    assert by_table[LIVE_TABLE] == LIVE["floor"], by_table
    assert by_table[LIVE_LINK_TABLE] != LIVE["link_floor"], by_table

    connection = _live_connection(read_only=False)
    try:
        connection.execute(
            f"delete from main_raw_vault.{LIVE_LINK_ANCHOR_SATELLITE} "
            f"where {emit.EFFECTIVE_COLUMN} = date '2098-01-01'"
        )
    finally:
        connection.close()


def test_live_the_windows_go_inert_under_the_full_refresh_flag() -> None:
    """Under the full-refresh flag the stage and bridge windows render inert and the
    staging model leaves is_incremental() false, so a full-refresh build regenerates the
    whole candidate set."""
    _run_dbt("compile", "--full-refresh", "--select", *LIVE_WINDOWED_MODELS)
    for model in LIVE_WINDOWED_MODELS:
        text = _compiled(model)
        assert dbt_translator.WINDOW_FLOOR_MACRO not in text, (model, text[-400:])
        assert f"{dbt_translator.WINDOW_FLOOR_OPERATOR} cast(" not in text, (model, text[-400:])
    assert "and true" in _compiled("br_dv_origo_deals")
    # The normal reading comes back on the next invocation without the flag.
    _run_dbt("compile", "--select", *LIVE_WINDOWED_MODELS)
    assert LIVE["floor"] in _compiled("stg_origo_deals")


def test_live_the_audit_invocation_reports_keys_wholly_outside_the_window() -> None:
    """A normal run scans nothing outside the window, so a key whose rows all fall below
    the floor is invisible to it. The audit invocation scans the full source on demand
    and reports those keys."""
    root = _live_estate()
    ctx = emit.EstateContext.resolve(estate_root=root)
    payload = emit.window_audit_plan(LIVE_SOURCE, LIVE_TABLE, ctx=ctx)
    assert payload["landing_key"] == [LIVE_LANDING_KEY], payload
    runner = emit.DbtRunOperation(
        project_dir=root,
        profiles_dir=root / "profiles",
        target="duckdb",
        echo=False,
        env=_live_env(),
        marker=emit.WINDOW_AUDIT_RESULT_MARKER,
    )
    baseline = runner(emit.WINDOW_AUDIT_MACRO, payload)
    assert baseline["floor"] == LIVE["floor"], baseline
    assert baseline["keys_total"] > 0, baseline

    connection = _live_connection(read_only=False)
    try:
        connection.execute(
            "insert into main_raw.raw_origo_deals select "
            "'D-OUTSIDE', 'ORIGO-EXT-OUTSIDE', 'Project Outside', null, 'buyout', "
            "'deal_desk_alpha', 'proprietary', '2020-05-05', 'spv', null, null"
        )
    finally:
        connection.close()
    reported = runner(emit.WINDOW_AUDIT_MACRO, payload)
    assert reported["keys_outside_window"] == baseline["keys_outside_window"] + 1, reported
    assert reported["keys_total"] == baseline["keys_total"] + 1, reported
    named = [entry[LIVE_LANDING_KEY] for entry in reported["sample"]]
    assert "D-OUTSIDE" in named, reported

    connection = _live_connection(read_only=False)
    try:
        connection.execute("delete from main_raw.raw_origo_deals where deal_id = 'D-OUTSIDE'")
    finally:
        connection.close()


def test_live_the_scratch_estate_is_removed() -> None:
    """The live lane leaves nothing behind."""
    holder = LIVE.get("holder")
    _live_cleanup()
    if holder:
        assert not Path(holder).exists(), holder

TESTS = [
    test_the_block_is_closed_and_rejects_an_unknown_key,
    test_a_non_positive_lookback_fails_with_a_named_error,
    test_a_missing_or_false_acknowledgment_fails_with_a_named_error,
    test_a_declared_block_without_a_natural_key_fails_with_a_named_error,
    test_the_unique_key_is_the_natural_key_plus_the_effective_column,
    test_the_staging_key_is_the_source_natural_key_a_resolution_join_consumes,
    test_the_natural_key_is_inferred_only_for_a_table_without_the_block,
    test_two_candidate_keys_fail_with_a_named_error,
    test_a_multi_column_unique_test_fails_with_a_named_error,
    test_generated_non_null_assertions_cover_every_key_member_at_error_severity,
    test_an_absent_effective_mapping_fails,
    test_an_ambiguous_effective_mapping_fails,
    test_a_constant_or_expression_effective_mapping_fails,
    test_a_null_constant_on_a_sibling_table_cannot_hold_this_tables_watermark,
    test_a_bronze_event_field_mismatch_fails,
    test_a_matching_bronze_event_field_passes,
    test_a_hand_authored_model_reading_a_windowed_relation_fails_naming_it,
    test_a_generated_model_reading_a_windowed_relation_passes_the_gate,
    test_the_reference_gate_is_inert_without_a_declared_table,
    test_a_physical_type_change_on_a_typed_column_grades_as_a_redefinition,
    test_golden_files_pin_the_generated_sql_for_both_window_shapes,
    test_the_staging_model_carries_the_incremental_configuration,
    test_the_stage_and_bridge_windows_are_inert_under_the_full_refresh_flag,
    test_the_watermark_constant_is_normalised_and_the_pruning_column_is_bare,
    test_the_floor_comparison_operator_is_pinned,
    test_an_undeclared_table_regenerates_without_a_window,
    test_the_committed_estate_regenerates_byte_identically,
    test_no_template_decides_a_window,
    test_the_delete_predicate_is_built_at_execution_from_the_declared_window,
    test_the_staging_model_reports_its_floor_and_cumulative_window_rows,
    test_the_consumption_watermark_is_the_least_satellite_maximum_coalesced_to_the_sentinel,
    test_a_missing_satellite_resolves_the_watermark_to_the_sentinel,
    test_the_watermark_resolves_every_satellite_through_the_adapter_under_an_execute_guard,
    test_the_watermark_builds_no_relation_name_and_takes_no_ref_to_a_satellite,
    test_the_per_satellite_maximum_carries_a_record_source_predicate,
    test_the_layout_carries_the_literal_the_satellite_actually_stores,
    test_emit_asserts_the_table_local_satellite_layout_for_a_declared_table,
    test_a_satellite_fed_by_two_sources_fails_the_isolation_assert,
    test_the_replay_suppression_bounds_its_existing_side_scan_with_the_window_floor,
    test_a_declared_table_whose_projection_omits_the_effective_column_fails,
    test_the_audit_plan_names_the_landing_relation_and_its_columns,
    test_the_audit_plan_refuses_a_table_without_a_declared_window,
    test_the_audit_plan_refuses_a_projection_it_cannot_follow,
    test_the_landing_column_extractor_reads_the_three_shapes_it_accepts,
    test_the_audit_invocation_is_a_named_operator_command,
    # The live lane runs in order on one scratch estate copy.
    test_live_the_declared_estate_emits_and_builds_on_duckdb,
    test_live_one_invocation_stamps_one_floor_inside_each_independent_table_chain,
    test_live_the_floor_resolution_is_cached_under_the_source_table,
    test_live_each_floor_is_the_least_table_satellite_maximum_minus_the_lookback,
    test_live_the_three_layers_carry_the_window_predicate,
    test_live_the_replay_suppression_scans_only_the_window,
    test_live_the_delete_side_is_bounded_by_the_window_floor,
    test_live_the_run_reports_the_floor_and_cumulative_relation_rows,
    test_live_a_sibling_source_cannot_advance_the_watermark,
    test_live_an_independent_table_chain_cannot_advance_this_tables_floor,
    test_live_the_windows_go_inert_under_the_full_refresh_flag,
    test_live_the_audit_invocation_reports_keys_wholly_outside_the_window,
    test_live_the_scratch_estate_is_removed,
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
