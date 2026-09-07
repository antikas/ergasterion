"""Plain-script regression tests for ergasterion/framework/adapters.py: the
per-adapter conventions loader's fail-closed branches, and a green proof
that the shipped adapter packages load clean.

``load_adapter_conventions`` resolves ``ADAPTERS_DIR`` (package-relative)
at call time, so a fixture swaps the module's ``ADAPTERS_DIR`` global to a
temporary directory carrying a deliberately malformed ``conventions.yml``,
calls the loader, restores the original directory in a ``finally``, and
asserts the exact ``FrameworkError`` raised. A failed load is never cached
(the cache is only populated on success), so reusing one temporary adapter
name across cases carries no risk of a stale result.

Usage:
    python tests/python/test_adapters.py
"""

from __future__ import annotations

import sys
import tempfile
import traceback
from pathlib import Path
from typing import Callable

if __package__ in (None, ""):
    import os as _os

    sys.path.insert(0, _os.path.dirname(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))))

from ergasterion.framework import adapters as fw_adapters
from ergasterion.framework.adapters import (
    ADAPTER_KINDS,
    ADAPTER_NAMES,
    NEUTRAL_TYPE_TOKENS,
    UnknownAdapterError,
    discover_adapters,
    load_adapter_conventions,
)
from ergasterion.framework.models import FrameworkError

# A structurally complete conventions document: one deny rule, every neutral
# type token mapped, a mapping-shaped identifier_rules block. Each RED case
# mutates exactly one part of it.
_TYPE_MAPPING_YAML = "\n".join(f"  {token}: TOKEN_{token.upper()}" for token in sorted(NEUTRAL_TYPE_TOKENS))

_DENY_RULE_BLOCK = "deny_rules:\n  - token: sample\n    pattern: '\\bsample\\s*\\('\n    message: \"sample message\"\n"

_GOOD_TEMPLATE = (
    "adapter: {adapter}\n"
    "kind: reference\n"
    "dialect: fixturedb\n"
    "incremental_strategy: fixture+insert\n"
    "{deny_rule_block}"
    "type_mapping:\n"
    "{type_mapping}\n"
    "identifier_rules:\n"
    "  quote_character: '\"'\n"
    "  case_comparison: insensitive\n"
)


def _good_yaml(adapter_name: str) -> str:
    return _GOOD_TEMPLATE.format(adapter=adapter_name, deny_rule_block=_DENY_RULE_BLOCK, type_mapping=_TYPE_MAPPING_YAML)


def _write(root: Path, adapter_name: str, body: str) -> None:
    directory = root / adapter_name
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "conventions.yml").write_text(body, encoding="utf-8")


def _load_with_temp_adapters_dir(root: Path, adapter_name: str):
    original = fw_adapters.ADAPTERS_DIR
    fw_adapters.ADAPTERS_DIR = root
    try:
        return load_adapter_conventions(adapter_name)
    finally:
        fw_adapters.ADAPTERS_DIR = original


def _assert_red(body: str, expected_substrings: tuple[str, ...], adapter_name: str = "fixture") -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        _write(root, adapter_name, body)
        try:
            _load_with_temp_adapters_dir(root, adapter_name)
        except FrameworkError as exc:
            message = str(exc)
            for substring in expected_substrings:
                assert substring in message, f"expected {substring!r} in error, got: {message!r}"
        else:
            raise AssertionError(f"expected a FrameworkError, conventions loaded clean: {body!r}")


def test_unknown_adapter_fails_closed() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        try:
            _load_with_temp_adapters_dir(root, "does-not-exist")
        except UnknownAdapterError as exc:
            assert exc.code == "unknown_adapter"
            assert exc.adapter_name == "does-not-exist"
        else:
            raise AssertionError("expected UnknownAdapterError for a directory that does not exist")


def test_missing_required_key_fails_closed() -> None:
    body = "adapter: fixture\nkind: reference\n"  # deny_rules, type_mapping, identifier_rules all absent
    _assert_red(body, ("missing required key",))


def test_missing_dialect_fails_closed() -> None:
    body = _good_yaml("fixture").replace("dialect: fixturedb\n", "")
    _assert_red(body, ("missing required key", "dialect"))


def test_empty_dialect_fails_closed() -> None:
    body = _good_yaml("fixture").replace("dialect: fixturedb", "dialect: ''")
    _assert_red(body, ("'dialect' must be a non-empty SQL dialect name",))


def test_missing_incremental_strategy_fails_closed() -> None:
    body = _good_yaml("fixture").replace("incremental_strategy: fixture+insert\n", "")
    _assert_red(body, ("missing required key", "incremental_strategy"))


def test_empty_incremental_strategy_fails_closed() -> None:
    body = _good_yaml("fixture").replace("incremental_strategy: fixture+insert", "incremental_strategy: ''")
    _assert_red(body, ("'incremental_strategy' must be a non-empty strategy name",))


def test_adapter_filename_mismatch_fails_closed() -> None:
    body = _good_yaml("some-other-name")
    _assert_red(body, ("must match its directory name", "some-other-name", "fixture"))


def test_kind_outside_registered_kinds_fails_closed() -> None:
    body = _good_yaml("fixture").replace("kind: reference", "kind: bogus")
    _assert_red(body, ("kind must be one of", *ADAPTER_KINDS))


def test_deny_rules_must_be_a_non_empty_list_fails_closed() -> None:
    body = _good_yaml("fixture").replace(_DENY_RULE_BLOCK, "deny_rules: []\n")
    _assert_red(body, ("deny_rules", "non-empty list"))


def test_malformed_deny_entry_fails_closed() -> None:
    body = _good_yaml("fixture").replace('    message: "sample message"\n', "")
    _assert_red(body, ("deny_rules entry needs", "token", "pattern", "message"))


def test_invalid_deny_regex_fails_closed() -> None:
    body = _good_yaml("fixture").replace("pattern: '\\bsample\\s*\\('", "pattern: '(unclosed'")
    _assert_red(body, ("invalid pattern", "sample"))


def test_type_mapping_must_be_a_mapping_fails_closed() -> None:
    body = _good_yaml("fixture").replace(f"type_mapping:\n{_TYPE_MAPPING_YAML}\n", "type_mapping: not-a-mapping\n")
    _assert_red(body, ("type_mapping",))


def test_type_mapping_missing_neutral_token_fails_closed() -> None:
    dropped = sorted(NEUTRAL_TYPE_TOKENS)[0]
    mapping_lines = "\n".join(
        line for line in _TYPE_MAPPING_YAML.splitlines() if not line.strip().startswith(f"{dropped}:")
    )
    body = _good_yaml("fixture").replace(_TYPE_MAPPING_YAML, mapping_lines)
    _assert_red(body, ("type_mapping is missing neutral type token", dropped))


def test_identifier_rules_must_be_a_mapping_fails_closed() -> None:
    body = _good_yaml("fixture").replace(
        "identifier_rules:\n  quote_character: '\"'\n  case_comparison: insensitive\n",
        "identifier_rules: not-a-mapping\n",
    )
    _assert_red(body, ("identifier_rules", "mapping"))


def test_missing_quote_character_fails_closed() -> None:
    # P7: a stored name is written through the adapter's own quoting, so an
    # adapter that declares no quote character has nothing to quote with.
    body = _good_yaml("fixture").replace("  quote_character: '\"'\n", "")
    _assert_red(body, ("identifier_rules.quote_character",))


def test_a_multi_character_quote_character_fails_closed() -> None:
    body = _good_yaml("fixture").replace("  quote_character: '\"'\n", "  quote_character: '[]'\n")
    _assert_red(body, ("identifier_rules.quote_character", "single"))


def test_missing_case_comparison_fails_closed() -> None:
    # The duplicate rules judge two declared stored names under this answer,
    # so an adapter that declares none leaves them with nothing to judge by.
    body = _good_yaml("fixture").replace("  case_comparison: insensitive\n", "")
    _assert_red(body, ("identifier_rules.case_comparison",))


def test_an_unknown_case_comparison_fails_closed() -> None:
    body = _good_yaml("fixture").replace(
        "  case_comparison: insensitive\n", "  case_comparison: whichever\n"
    )
    _assert_red(body, ("identifier_rules.case_comparison", "whichever"))


def test_discover_adapters_finds_the_two_shipped_packages() -> None:
    assert set(discover_adapters()) == {"duckdb", "bigquery"}, discover_adapters()
    assert set(ADAPTER_NAMES) == {"duckdb", "bigquery"}, ADAPTER_NAMES


def test_shipped_duckdb_and_bigquery_conventions_load_clean() -> None:
    duckdb = load_adapter_conventions("duckdb")
    assert duckdb.adapter == "duckdb"
    assert duckdb.kind == "reference"
    assert len(duckdb.deny_rules) > 0
    assert set(duckdb.type_mapping) == NEUTRAL_TYPE_TOKENS
    assert isinstance(duckdb.identifier_rules, dict) and duckdb.identifier_rules

    bigquery = load_adapter_conventions("bigquery")
    assert bigquery.adapter == "bigquery"
    assert bigquery.kind == "deployment"
    assert len(bigquery.deny_rules) > 0
    assert set(bigquery.type_mapping) == NEUTRAL_TYPE_TOKENS
    assert isinstance(bigquery.identifier_rules, dict) and bigquery.identifier_rules


TESTS: list[Callable[[], None]] = [
    test_unknown_adapter_fails_closed,
    test_missing_required_key_fails_closed,
    test_missing_dialect_fails_closed,
    test_empty_dialect_fails_closed,
    test_missing_incremental_strategy_fails_closed,
    test_empty_incremental_strategy_fails_closed,
    test_adapter_filename_mismatch_fails_closed,
    test_kind_outside_registered_kinds_fails_closed,
    test_deny_rules_must_be_a_non_empty_list_fails_closed,
    test_malformed_deny_entry_fails_closed,
    test_invalid_deny_regex_fails_closed,
    test_type_mapping_must_be_a_mapping_fails_closed,
    test_type_mapping_missing_neutral_token_fails_closed,
    test_identifier_rules_must_be_a_mapping_fails_closed,
    test_missing_quote_character_fails_closed,
    test_a_multi_character_quote_character_fails_closed,
    test_missing_case_comparison_fails_closed,
    test_an_unknown_case_comparison_fails_closed,
    test_discover_adapters_finds_the_two_shipped_packages,
    test_shipped_duckdb_and_bigquery_conventions_load_clean,
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
    print(f"{len(TESTS) - failures}/{len(TESTS)} adapters tests passed")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
