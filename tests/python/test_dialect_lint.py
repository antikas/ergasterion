"""Plain-script regression tests for the two-adapter dialect lint (owner
ruling R10: DuckDB reference, BigQuery deployment, no Snowflake surface
anywhere).

The DuckDB rule set is exercised rule-by-rule through the public linter, rather
than by testing its regular expressions in isolation. That proves an illegal
model/test surface is named and scanned. No warehouse connection is made.

Usage:
    python tests/python/test_dialect_lint.py
"""

from __future__ import annotations

import subprocess
import sys
import tempfile
import traceback
from pathlib import Path

if __package__ in (None, ""):
    import os as _os

    sys.path.insert(0, _os.path.dirname(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))))

from ergasterion.dialect_lint import DENY_LISTS, DUCKDB_DENY, _rules_for, lint_models
from ergasterion.estate import EstateContext

REPO_ROOT = Path(__file__).resolve().parents[2]

# One minimal line per DuckDB deny rule (owner ruling R10 retired every
# entry that guarded a retired adapter: object_construct, object_agg,
# to_variant, regexp_substr, regexp_instr, editdistance, datediff,
# array_construct, dateadd, table_generator and timestamp_ltz are gone, not
# renamed; float64, format_date, safe_divide, raw_string and select_except are
# restored from the former BigQuery-guard table since each genuinely
# breaks on DuckDB). These examples intentionally use direct SQL rather
# than dpf_* dispatch calls: the latter are the sanctioned, adapter-neutral
# translation layer and must not be caught by this gate.
ILLEGAL_DUCKDB_SQL = {
    "safe_cast": "select safe_cast(value as string) as bad",
    "safe_parse_json": "select safe.parse_json(value) as bad",
    "json_type": "select json_type(value) as bad",
    "json_value": "select json_value(value) as bad",
    "to_hex": "select to_hex(md5(value)) as bad",
    "to_json_string": "select to_json_string(json_object('key', value)) as bad",
    "struct": "select struct(value as key) as bad",
    "regexp_replace_global": "select regexp_replace(value, 'a', 'b') as bad",
    "regexp_contains": "select regexp_contains(value, 'a') as bad",
    "regexp_extract": "select regexp_extract(value, '(a)') as bad",
    "array_type": "select cast([] as array<string>) as bad",
    "date_trunc_bigquery_order": "select date_trunc(event_date, month) as bad",
    "generate_date_array": "select generate_date_array(start_date, end_date) as bad",
    "edit_distance": "select edit_distance(left_value, right_value) as bad",
    "date_diff_bigquery_order": "select date_diff(end_date, start_date, day) as bad",
    "float64": "select cast(value as float64) as bad",
    "format_date": "select format_date('%Y', event_date) as bad",
    "safe_divide": "select safe_divide(numerator, denominator) as bad",
    "raw_string": "select r'pattern' as bad",
    "select_except": "select * except(value) as bad",
}

# One minimal line per BigQuery deny rule: these guard the reference
# adapter's exclusive DuckDB syntax from leaking into BigQuery-targeted SQL
# (ergasterion/adapters/bigquery/conventions.yml).
ILLEGAL_BIGQUERY_SQL = {
    "postgres_cast_operator": "select value::int as bad",
    "duckdb_file_scan": "select * from read_parquet('foo.parquet')",
    "struct_pack": "select struct_pack(a := 1, b := 2) as bad",
}

# Kept independent of DUCKDB_DENY so an implementation omission cannot certify
# itself by making the fixture dictionary and rule list agree on the same gap.
EXPECTED_DUCKDB_RULE_TOKENS = frozenset(
    {
        "safe_cast",
        "safe_parse_json",
        "json_type",
        "json_value",
        "to_hex",
        "to_json_string",
        "struct",
        "regexp_replace_global",
        "regexp_contains",
        "regexp_extract",
        "array_type",
        "date_trunc_bigquery_order",
        "generate_date_array",
        "edit_distance",
        "date_diff_bigquery_order",
        "float64",
        "format_date",
        "safe_divide",
        "raw_string",
        "select_except",
    }
)

EXPECTED_BIGQUERY_RULE_TOKENS = frozenset({"postgres_cast_operator", "duckdb_file_scan", "struct_pack"})


def _write(root: Path, relative: Path, content: str) -> Path:
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content + "\n", encoding="utf-8")
    return path


def _fixture_context(root: Path) -> EstateContext:
    return EstateContext.resolve(estate_root=root)


def test_every_duckdb_rule_catches_its_family() -> None:
    expected_tokens = {rule.token for rule in DUCKDB_DENY}
    assert expected_tokens == EXPECTED_DUCKDB_RULE_TOKENS, (
        "DuckDB rules drifted from the independently declared census: "
        f"expected={sorted(EXPECTED_DUCKDB_RULE_TOKENS)}, actual={sorted(expected_tokens)}"
    )
    assert set(ILLEGAL_DUCKDB_SQL) == EXPECTED_DUCKDB_RULE_TOKENS, (
        "Every independently declared DuckDB family needs an explicit RED example; "
        f"examples={sorted(ILLEGAL_DUCKDB_SQL)}"
    )

    for token, sql in ILLEGAL_DUCKDB_SQL.items():
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            injected = _write(root, Path("models/injected.sql"), sql)
            offenses = lint_models("duckdb", ctx=_fixture_context(root))
            matching = [offense for offense in offenses if offense.token == token]
            assert matching, f"expected {token!r} to be caught for {sql!r}"
            assert matching[0].path == injected, (
                f"expected injected path {injected}, got {matching[0].path}"
            )


def test_sanctioned_and_neutral_forms_are_clean() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        _write(
            root,
            Path("models/clean.sql"),
            "\n".join(
                [
                    "select current_timestamp as generated_at",
                    "select dpf_regexp_replace(value, 'a', 'b') as normalised",
                    "select regexp_replace(value, 'a', 'b', 'g') as duckdb_global",
                    "select regexp_replace(coalesce(value, ''), '(a,b)', '', 'g') as nested_duckdb_global",
                    "select json_object('key', value) as duckdb_json_object",
                    "select cast(value as timestamp) as reviewed_at",
                ]
            ),
        )
        assert lint_models("duckdb", ctx=_fixture_context(root)) == []


def test_every_bigquery_rule_catches_its_family() -> None:
    expected_tokens = {rule.token for rule in DENY_LISTS["bigquery"]}
    assert expected_tokens == EXPECTED_BIGQUERY_RULE_TOKENS, (
        "BigQuery rules drifted from the independently declared census: "
        f"expected={sorted(EXPECTED_BIGQUERY_RULE_TOKENS)}, actual={sorted(expected_tokens)}"
    )
    assert set(ILLEGAL_BIGQUERY_SQL) == EXPECTED_BIGQUERY_RULE_TOKENS, (
        "Every independently declared BigQuery family needs an explicit RED example; "
        f"examples={sorted(ILLEGAL_BIGQUERY_SQL)}"
    )

    for token, sql in ILLEGAL_BIGQUERY_SQL.items():
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            injected = _write(root, Path("models/injected.sql"), sql)
            offenses = lint_models("bigquery", ctx=_fixture_context(root))
            matching = [offense for offense in offenses if offense.token == token]
            assert matching, f"expected {token!r} to be caught for {sql!r}"
            assert matching[0].path == injected, (
                f"expected injected path {injected}, got {matching[0].path}"
            )


def test_no_snowflake_surface_remains_in_any_deny_list() -> None:
    """Owner ruling R10: no Snowflake target declaration, dialect rule or
    conventions file remains. Neither registered adapter's deny-list, and no
    adapter name itself, carries the word."""
    assert "snowflake" not in DENY_LISTS, sorted(DENY_LISTS)
    for adapter_name, rules in DENY_LISTS.items():
        for rule in rules:
            haystack = f"{rule.token} {rule.message}".lower()
            assert "snowflake" not in haystack, f"{adapter_name}/{rule.token}: {rule.message!r}"


def test_regexp_replace_detects_nested_and_multiline_three_argument_forms() -> None:
    cases = [
        "select regexp_replace(coalesce(value, ''), '(a,b)', '') as bad",
        """select regexp_replace(
    coalesce(value, ''),
    '([a-z]+)',
    ''
) as bad""",
    ]
    for sql in cases:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _write(root, Path("models/injected.sql"), sql)
            offenses = lint_models("duckdb", ctx=_fixture_context(root))
            assert [offense.token for offense in offenses] == ["regexp_replace_global"], offenses


def test_committed_ddl_pin_and_estate_are_clean_for_duckdb() -> None:
    offenses = lint_models("duckdb")
    assert offenses == [], f"expected committed estate clean for DuckDB, got: {offenses}"


def test_committed_estate_is_clean_for_bigquery() -> None:
    offenses = lint_models("bigquery")
    assert offenses == [], f"expected committed estate clean for BigQuery, got: {offenses}"


def test_unknown_target_fails_loud() -> None:
    try:
        _rules_for("not-a-real-adapter")
    except ValueError as error:
        assert "unknown target" in str(error)
        assert "not-a-real-adapter" in str(error)
    else:
        raise AssertionError("unknown target must fail loud")

    result = subprocess.run(
        [sys.executable, "ergasterion/dialect_lint.py", "--target", "not-a-real-adapter"],
        cwd=REPO_ROOT,
        text=True,
        capture_output=True,
        check=False,
    )
    assert result.returncode != 0, "the gate must reject an unknown adapter"
    assert "not-a-real-adapter" in result.stdout + result.stderr, result.stdout + result.stderr


def test_the_gate_runs_clean_for_every_declared_adapter() -> None:
    """The standalone gate, driven for each adapter the estate declares, reports the
    emitted tree clean and names the adapter it ran for. Only these two are declared
    (owner ruling R10), and a third would have to be declared in estate.yml before the
    gate would accept it at all."""

    for adapter in ("bigquery", "duckdb"):
        result = subprocess.run(
            [sys.executable, "ergasterion/dialect_lint.py", "--target", adapter],
            cwd=REPO_ROOT,
            text=True,
            capture_output=True,
            check=False,
        )
        assert result.returncode == 0, result.stdout + result.stderr
        assert f"dialect-lint OK [{adapter}]" in result.stdout, result.stdout


TESTS = [
    test_every_duckdb_rule_catches_its_family,
    test_every_bigquery_rule_catches_its_family,
    test_no_snowflake_surface_remains_in_any_deny_list,
    test_sanctioned_and_neutral_forms_are_clean,
    test_regexp_replace_detects_nested_and_multiline_three_argument_forms,
    test_committed_ddl_pin_and_estate_are_clean_for_duckdb,
    test_committed_estate_is_clean_for_bigquery,
    test_unknown_target_fails_loud,
    test_the_gate_runs_clean_for_every_declared_adapter,
]


def main() -> int:
    failures = 0
    for test in TESTS:
        try:
            test()
            print(f"PASS {test.__name__}")
        except Exception:
            failures += 1
            print(f"FAIL {test.__name__}")
            traceback.print_exc()
    print(f"{len(TESTS) - failures}/{len(TESTS)} dialect-lint tests passed")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
