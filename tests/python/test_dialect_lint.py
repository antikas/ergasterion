"""Plain-script regression tests for the three-adapter dialect lint.

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

from ergasterion.dialect_lint import DUCKDB_DENY, _rules_for, lint_models
from ergasterion.estate import EstateContext

REPO_ROOT = Path(__file__).resolve().parents[2]
DDL_PIN_RELATIVE = Path("tests/assert_er_decisions_log_ddl_unchanged.sql")

# One minimal line per E2/E3 rule. These examples intentionally use direct SQL
# rather than dpf_* dispatch calls: the latter are the sanctioned, adapter-neutral
# translation layer and must not be caught by this gate.
ILLEGAL_DUCKDB_SQL = {
    "safe_cast": "select safe_cast(value as string) as bad",
    "safe_parse_json": "select safe.parse_json(value) as bad",
    "json_type": "select json_type(value) as bad",
    "json_value": "select json_value(value) as bad",
    "to_hex": "select to_hex(md5(value)) as bad",
    "to_json_string": "select to_json_string(json_object('key', value)) as bad",
    "struct": "select struct(value as key) as bad",
    "object_construct": "select object_construct_keep_null('key', value) as bad",
    "object_agg": "select object_agg(key, value) as bad",
    "to_variant": "select to_variant(value) as bad",
    "regexp_replace_global": "select regexp_replace(value, 'a', 'b') as bad",
    "regexp_contains": "select regexp_contains(value, 'a') as bad",
    "regexp_extract": "select regexp_extract(value, '(a)') as bad",
    "regexp_substr": "select regexp_substr(value, '(a)', 1, 1, 'e', 1) as bad",
    "regexp_instr": "select regexp_instr(value, 'a') > 0 as bad",
    "array_type": "select cast([] as array<string>) as bad",
    "date_trunc_bigquery_order": "select date_trunc(event_date, month) as bad",
    "generate_date_array": "select generate_date_array(start_date, end_date) as bad",
    "edit_distance": "select edit_distance(left_value, right_value) as bad",
    "editdistance": "select editdistance(left_value, right_value) as bad",
    "date_diff_bigquery_order": "select date_diff(end_date, start_date, day) as bad",
    "datediff": "select datediff(day, start_date, end_date) as bad",
    "array_construct": "select array_construct(value) as bad",
    "dateadd": "select dateadd(day, 1, event_date) as bad",
    "table_generator": "select * from table(generator(rowcount => 1))",
    "timestamp_ltz": "select cast(value as timestamp_ltz) as bad",
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
        "object_construct",
        "object_agg",
        "to_variant",
        "regexp_replace_global",
        "regexp_contains",
        "regexp_extract",
        "regexp_substr",
        "regexp_instr",
        "array_type",
        "date_trunc_bigquery_order",
        "generate_date_array",
        "edit_distance",
        "editdistance",
        "date_diff_bigquery_order",
        "datediff",
        "array_construct",
        "dateadd",
        "table_generator",
        "timestamp_ltz",
    }
)


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
        "DuckDB rules drifted from the independently declared E2/E3 census: "
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


def test_ddl_pin_exempts_only_the_expected_snowflake_token() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        committed_pin = (REPO_ROOT / DDL_PIN_RELATIVE).read_text(encoding="utf-8")
        mutated_duckdb_branch = committed_pin.replace(
            "reviewed_at timestamp,", "reviewed_at timestamp_ltz,", 1
        )
        assert mutated_duckdb_branch != committed_pin, "fixture must mutate the DuckDB branch"
        pin = _write(
            root,
            DDL_PIN_RELATIVE,
            mutated_duckdb_branch,
        )
        offenses = lint_models("duckdb", ctx=_fixture_context(root))
        matching = [offense for offense in offenses if offense.token == "timestamp_ltz"]
        assert len(matching) == 1, f"expected exactly the injected token, got {matching}"
        assert matching[0].path == pin
        assert matching[0].line.strip().lower() == "reviewed_at timestamp_ltz,"


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


def test_unknown_target_fails_loud() -> None:
    try:
        _rules_for("not-a-real-adapter")
    except ValueError as error:
        assert "unknown target" in str(error)
        assert "not-a-real-adapter" in str(error)
    else:
        raise AssertionError("unknown target must fail loud")

    result = subprocess.run(
        [sys.executable, "ergasterion/emit.py", "--check", "--lint-target", "not-a-real-adapter"],
        cwd=REPO_ROOT,
        text=True,
        capture_output=True,
        check=False,
    )
    assert result.returncode != 0, "emit --lint-target must reject an unknown adapter"
    assert "unknown adapter 'not-a-real-adapter'" in result.stdout, result.stdout


def test_emit_check_lints_duckdb_and_declared_adapters() -> None:
    result = subprocess.run(
        [sys.executable, "ergasterion/emit.py", "--check", "--lint-target", "duckdb"],
        cwd=REPO_ROOT,
        text=True,
        capture_output=True,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    for adapter in ("bigquery", "snowflake", "duckdb"):
        assert f"dialect-lint OK [{adapter}]" in result.stdout, result.stdout


TESTS = [
    test_every_duckdb_rule_catches_its_family,
    test_sanctioned_and_neutral_forms_are_clean,
    test_ddl_pin_exempts_only_the_expected_snowflake_token,
    test_regexp_replace_detects_nested_and_multiline_three_argument_forms,
    test_committed_ddl_pin_and_estate_are_clean_for_duckdb,
    test_unknown_target_fails_loud,
    test_emit_check_lints_duckdb_and_declared_adapters,
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
