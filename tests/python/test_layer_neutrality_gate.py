"""Assert-script tests for ergasterion/framework/layer_neutrality.py.

The gate scans every .py file under the paths named in
scripts/layer_neutrality_scope.txt (currently ergasterion/) for the three
forbidden layer words (architecture check 8), case-insensitive, in an
identifier, a string literal, a comment or the file's own name, and fails
closed on any finding not named in scripts/layer_neutrality_allowlist.txt
(committed empty and never populated). A match is suppressed only when the
exact token segment carrying it -- segments split on underscore, hyphen,
whitespace, dot and camelCase boundaries -- is one of NON_LAYER_VOCABULARY:
unrelated vocabulary from another domain that happens to carry a forbidden
word as a plain substring. The one exempt term today is "golden", the Data
Vault "golden record" survivorship term (architecture section 4, Data
Curation row; D30/R4), which is not a layer name. This is a matcher
correction, not an allowlist entry: it applies wherever the exempt
vocabulary occurs, never to one file or line.

These tests prove the SCANNING MECHANISM itself is red on a real violation,
independent of the shipped scope: they build a temporary fixture tree
containing a forbidden word in an identifier, a string literal, a comment and
a file name, point scan_scope() directly at it, and assert every one of
those categories is caught. Critically, they also drive run_check() and
main() end to end against an injected scope file, allowlist file and
repository root naming that same fixture tree (main()'s --scope/--allowlist/
--root arguments, added for exactly this) -- not just scan_scope() in
isolation -- so a mutant that made run_check() or main() ignore what
scan_scope() found (for example always returning [] / exit 0) is caught
here even where the shipped-files test alone would not catch it. They also
pin the shipped scope/allowlist files to their required state, prove
run_check()/main() are green against them today, prove the gate module's
own source carries no forbidden word (so its own path sits inside the scope
it scans with no self-exclusion needed), and prove the NON_LAYER_VOCABULARY
exemption is narrow -- only the exact segment, never a compound merely
containing the exempt term -- driving the real --check verdict path over a
mixed scratch tree.

Usage:
    python tests/python/test_layer_neutrality_gate.py
"""

from __future__ import annotations

import sys
import tempfile
import traceback
from pathlib import Path

if __package__ in (None, ""):
    import os as _os
    import sys as _sys
    _sys.path.insert(0, _os.path.dirname(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))))

from ergasterion.framework import layer_neutrality

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
SCOPE_FILE = REPO_ROOT / "scripts" / "layer_neutrality_scope.txt"
ALLOWLIST_FILE = REPO_ROOT / "scripts" / "layer_neutrality_allowlist.txt"
GATE_MODULE = REPO_ROOT / "ergasterion" / "framework" / "layer_neutrality.py"


def test_shipped_scope_is_widened_and_allowlist_stays_empty() -> None:
    # The scope starts empty (report mode) and widens to ergasterion/ once
    # the wire-format surface is re-issued under layer-neutral names. The
    # allowlist is committed empty and stays empty at every stage.
    assert SCOPE_FILE.is_file(), "scripts/layer_neutrality_scope.txt must be committed"
    assert ALLOWLIST_FILE.is_file(), "scripts/layer_neutrality_allowlist.txt must be committed"
    assert layer_neutrality._read_list_file(SCOPE_FILE) == ("ergasterion/",), (
        "the scope must be ergasterion/ (widened from the initial empty scope)"
    )
    assert layer_neutrality._read_list_file(ALLOWLIST_FILE) == (), "the allowlist must be empty and stay empty"


def test_read_list_file_fails_closed_on_a_missing_file() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        missing = Path(tmp) / "does-not-exist.txt"
        try:
            layer_neutrality._read_list_file(missing)
        except FileNotFoundError as exc:
            assert str(missing) in str(exc), exc
        else:
            raise AssertionError("expected FileNotFoundError for a missing scope/allowlist file")


def test_report_mode_is_green_against_the_shipped_files() -> None:
    violations = layer_neutrality.run_check()
    assert violations == [], f"report-mode run_check() must be green with an empty scope, got: {violations}"
    assert layer_neutrality.main(["--check"]) == 0
    assert layer_neutrality.main([]) == 0


def test_empty_scope_scans_nothing() -> None:
    assert layer_neutrality.scan_scope(()) == []


def test_gate_is_red_on_identifier_string_and_comment_violations() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        fixture_dir = root / "fixture_pkg"
        fixture_dir.mkdir()
        (fixture_dir / "__init__.py").write_text("", encoding="utf-8")
        (fixture_dir / "sample.py").write_text(
            "BRONZE_MANDATORY = 1  # bronze occurrence marker\n"
            "message = 'this references the Silver layer'\n"
            "gold_threshold = 2\n",
            encoding="utf-8",
        )
        violations = layer_neutrality.scan_scope(("fixture_pkg",), repo_root=root)

        categories = {v.category for v in violations}
        assert "identifier" in categories, violations
        assert "comment" in categories, violations
        assert "string" in categories, violations

        words = {v.word for v in violations}
        assert words == {"bronze", "silver", "gold"}, words

        identifier_words = {v.word for v in violations if v.category == "identifier"}
        assert identifier_words == {"bronze", "gold"}, identifier_words


def test_gate_is_red_on_a_layer_word_in_a_file_name() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        fixture_dir = root / "fixture_pkg"
        fixture_dir.mkdir()
        (fixture_dir / "bronze_contract.py").write_text("value = 1\n", encoding="utf-8")
        violations = layer_neutrality.scan_scope(("fixture_pkg",), repo_root=root)
        assert any(v.category == "filename" and v.word == "bronze" for v in violations), violations


def test_gate_is_green_on_a_clean_fixture_tree() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        fixture_dir = root / "fixture_pkg"
        fixture_dir.mkdir()
        (fixture_dir / "sample.py").write_text(
            "LANDING_MANDATORY = 1  # a clean occurrence marker\n"
            "message = 'this references the serving profile'\n",
            encoding="utf-8",
        )
        assert layer_neutrality.scan_scope(("fixture_pkg",), repo_root=root) == []


def test_gate_reports_an_unreadable_file_rather_than_skipping_it() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        fixture_dir = root / "fixture_pkg"
        fixture_dir.mkdir()
        (fixture_dir / "broken.py").write_text("def broken(:\n    pass\n", encoding="utf-8")
        violations = layer_neutrality.scan_scope(("fixture_pkg",), repo_root=root)
        assert any(v.category == "unreadable" for v in violations), violations


def test_filter_allowed_drops_only_named_entries() -> None:
    violations = [
        layer_neutrality.Violation("a.py", "identifier", 1, "gold", "GOLD"),
        layer_neutrality.Violation("b.py", "identifier", 2, "silver", "SILVER"),
    ]
    filtered = layer_neutrality.filter_allowed(violations, ("a.py",))
    assert filtered == [violations[1]]
    assert layer_neutrality.filter_allowed(violations, ()) == violations


def test_scope_entry_naming_a_single_file_is_scanned() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        fixture_dir = root / "fixture_pkg"
        fixture_dir.mkdir()
        (fixture_dir / "sample.py").write_text("BRONZE = 1\n", encoding="utf-8")
        (fixture_dir / "other.py").write_text("SILVER = 1\n", encoding="utf-8")
        violations = layer_neutrality.scan_scope(("fixture_pkg/sample.py",), repo_root=root)
        assert {v.path for v in violations} == {"fixture_pkg/sample.py"}


def _write_dirty_fixture(root: Path) -> None:
    fixture_dir = root / "fixture_pkg"
    fixture_dir.mkdir()
    (fixture_dir / "__init__.py").write_text("", encoding="utf-8")
    (fixture_dir / "sample.py").write_text(
        "BRONZE_MANDATORY = 1  # bronze occurrence marker\n"
        "message = 'this references the Silver layer'\n",
        encoding="utf-8",
    )
    (root / "scope.txt").write_text("fixture_pkg\n", encoding="utf-8")
    (root / "allowlist.txt").write_text("", encoding="utf-8")


def _write_clean_fixture(root: Path) -> None:
    fixture_dir = root / "fixture_pkg"
    fixture_dir.mkdir()
    (fixture_dir / "sample.py").write_text(
        "LANDING_MANDATORY = 1  # a clean occurrence marker\n"
        "message = 'this references the serving profile'\n",
        encoding="utf-8",
    )
    (root / "scope.txt").write_text("fixture_pkg\n", encoding="utf-8")
    (root / "allowlist.txt").write_text("", encoding="utf-8")


def test_run_check_is_red_end_to_end_on_an_injected_dirty_scope() -> None:
    # This is the mutant-proof test: it never calls scan_scope() directly.
    # A mutant that made run_check() drop scan_scope()'s findings (for
    # example always returning []) fails this test even though the shipped,
    # currently-empty scope would still make report_mode_is_green pass.
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        _write_dirty_fixture(root)
        violations = layer_neutrality.run_check(
            scope_file=root / "scope.txt", allowlist_file=root / "allowlist.txt", repo_root=root,
        )
        assert violations, "run_check() must report the injected fixture's findings, not an empty list"
        categories = {v.category for v in violations}
        assert categories == {"identifier", "comment", "string"}, categories


def test_main_is_red_end_to_end_on_an_injected_dirty_scope() -> None:
    # A mutant that made main() ignore run_check()'s return value (for
    # example always returning 0) fails this test.
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        _write_dirty_fixture(root)
        code = layer_neutrality.main(
            ["--check", "--scope", str(root / "scope.txt"), "--allowlist", str(root / "allowlist.txt"), "--root", str(root)]
        )
        assert code == 1, f"main() must exit 1 on an injected fixture with real findings, got {code}"


def test_run_check_and_main_are_green_end_to_end_on_an_injected_clean_scope() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        _write_clean_fixture(root)
        violations = layer_neutrality.run_check(
            scope_file=root / "scope.txt", allowlist_file=root / "allowlist.txt", repo_root=root,
        )
        assert violations == [], violations
        code = layer_neutrality.main(
            ["--check", "--scope", str(root / "scope.txt"), "--allowlist", str(root / "allowlist.txt"), "--root", str(root)]
        )
        assert code == 0, f"main() must exit 0 on an injected fixture with no findings, got {code}"


def test_gate_module_itself_carries_no_forbidden_word() -> None:
    # No self-exclusion and no allowlist entry: this is a plain scan of the
    # gate's own real path, proving the module earns its green the same way
    # any other file would, now that the scope includes it.
    assert GATE_MODULE.is_file(), GATE_MODULE
    violations = layer_neutrality.scan_file(GATE_MODULE, relative_to=REPO_ROOT)
    assert violations == [], violations


def test_non_layer_vocabulary_exempts_only_its_own_exact_segment() -> None:
    # The Data Vault "golden record" survivorship term (architecture
    # section 4, Data Curation row; D30/R4) is not a layer name, so the
    # matcher -- not the allowlist -- exempts it, and only where the whole
    # segment carrying the match equals the exempt term exactly. Segments
    # split on underscore, hyphen, whitespace, dot and camelCase boundaries.
    for clean in (
        "golden_record",
        "golden_key",
        "GoldenRecord",
        "golden-record",
        "the golden vectors pin",
        "the golden-hash parity spot check",
    ):
        assert layer_neutrality._contains_forbidden_word(clean) is None, clean

    # A segment that IS a bare forbidden word (any case, or standing alone
    # inside a camelCase compound), or that merely glues an exempt term to
    # unrelated text without being exactly equal to it, still fires -- and a
    # compound carrying the exempt term as a separate segment of its own
    # still fires on its other, non-exempt segment.
    for dirty, expected_word in (
        ("gold", "gold"),
        ("gold_layer", "gold"),
        ("GoldProduct", "gold"),
        ("goldrecord", "gold"),
        ("gold_golden", "gold"),
    ):
        assert layer_neutrality._contains_forbidden_word(dirty) == expected_word, dirty


def test_non_layer_vocabulary_is_a_matcher_fix_not_an_allowlist_entry() -> None:
    # None of the exempt terms is itself a forbidden word (the exemption
    # narrows what fires, it never widens FORBIDDEN_WORDS), and the shipped
    # allowlist stays empty: this correction lives in the matcher, not in a
    # grandfathered file or line.
    for term in layer_neutrality.NON_LAYER_VOCABULARY:
        assert layer_neutrality._contains_forbidden_word(term) is None, term
    assert layer_neutrality._read_list_file(ALLOWLIST_FILE) == (), (
        "the allowlist must still be empty after the matcher correction"
    )


def test_check_verdict_over_a_mixed_scratch_tree_golden_key_green_gold_key_red() -> None:
    # Drives the real --check verdict path (run_check()/main(), not
    # scan_scope() directly): one scratch tree carries a golden_key
    # identifier (Data Vault vocabulary, must not fire) alongside a
    # gold_key identifier (a real layer word, must fire), so the fix is
    # proven to discriminate between them within the same run.
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        fixture_dir = root / "fixture_pkg"
        fixture_dir.mkdir()
        (fixture_dir / "__init__.py").write_text("", encoding="utf-8")
        (fixture_dir / "survivorship.py").write_text("golden_key = 1\n", encoding="utf-8")
        (fixture_dir / "sample.py").write_text("gold_key = 1\n", encoding="utf-8")
        (root / "scope.txt").write_text("fixture_pkg\n", encoding="utf-8")
        (root / "allowlist.txt").write_text("", encoding="utf-8")

        violations = layer_neutrality.run_check(
            scope_file=root / "scope.txt", allowlist_file=root / "allowlist.txt", repo_root=root,
        )
        assert len(violations) == 1, violations
        assert violations[0].path == "fixture_pkg/sample.py", violations
        assert violations[0].word == "gold", violations

        code = layer_neutrality.main(
            ["--check", "--scope", str(root / "scope.txt"), "--allowlist", str(root / "allowlist.txt"), "--root", str(root)]
        )
        assert code == 1, "the gold_key finding must still turn the real --check verdict red"


TESTS = [
    test_shipped_scope_is_widened_and_allowlist_stays_empty,
    test_read_list_file_fails_closed_on_a_missing_file,
    test_report_mode_is_green_against_the_shipped_files,
    test_empty_scope_scans_nothing,
    test_gate_is_red_on_identifier_string_and_comment_violations,
    test_gate_is_red_on_a_layer_word_in_a_file_name,
    test_gate_is_green_on_a_clean_fixture_tree,
    test_gate_reports_an_unreadable_file_rather_than_skipping_it,
    test_filter_allowed_drops_only_named_entries,
    test_scope_entry_naming_a_single_file_is_scanned,
    test_run_check_is_red_end_to_end_on_an_injected_dirty_scope,
    test_main_is_red_end_to_end_on_an_injected_dirty_scope,
    test_run_check_and_main_are_green_end_to_end_on_an_injected_clean_scope,
    test_gate_module_itself_carries_no_forbidden_word,
    test_non_layer_vocabulary_exempts_only_its_own_exact_segment,
    test_non_layer_vocabulary_is_a_matcher_fix_not_an_allowlist_entry,
    test_check_verdict_over_a_mixed_scratch_tree_golden_key_green_gold_key_red,
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
