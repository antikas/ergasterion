"""Per-target dialect lint for BigQuery and DuckDB SQL.

No dbt model or singular test -- whether emitted or hand-authored -- may carry SQL
that is incompatible with its selected adapter. Each registered adapter's
deny-list names constructs exclusive to another dialect, and every registered
adapter must declare at least one rule so a new target cannot silently skip linting.

The scanner covers generated and hand-authored SQL under ``models/`` and ``tests/``.
Generated violations are fixed in their template or declaration; hand-authored
violations are fixed in the SQL file itself.

Dialect-specific SQL is legitimate in ONE place only: the adapter-dispatch macros
under `macros/` (e.g. cross_db.sql). Those are the sanctioned translation layer and
are never scanned here.

This module owns no deny-list data of its own (owner ruling R10, plan decision
D35, ground truth 5): every deny rule is adapter package data, loaded once
through ``ergasterion.framework.adapters.load_adapter_conventions`` from
``ergasterion/adapters/<adapter>/conventions.yml``. Registering a platform
means adding its conventions.yml; this module never changes for it.

Usage:
    python ergasterion/dialect_lint.py                 # lint all model SQL for bigquery
    python ergasterion/dialect_lint.py --target bigquery
    python ergasterion/dialect_lint.py --target duckdb

Exit code 0 = clean, 1 = offenders found. Importable via `lint_models()` (all models)
and `lint_emitted()` (emitted only) so the emitter can run it as a post-emit gate.
"""

from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass
from pathlib import Path

# Support installed-command and direct-script execution.
if __package__ in (None, ""):
    import os as _os, sys as _sys
    _sys.path.insert(0, _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__))))

from ergasterion.estate import EstateContext
from ergasterion.framework.adapters import ADAPTER_NAMES, load_adapter_conventions
from ergasterion.framework.generated import GENERATED_MARKER

# Ambient estate context; scan functions default to it, a caller threads its own via `ctx=`.
_DEFAULT_CTX = EstateContext.default()

# ESTATE paths -- ride the context. Aliases kept for back-compat reads.
REPO_ROOT = _DEFAULT_CTX.root
MODELS_DIR = _DEFAULT_CTX.models_dir
# Singular tests are executable warehouse SQL and use the same deny-list as models.
TESTS_DIR = _DEFAULT_CTX.tests_dir

# The marker the SQL translator writes into every generated file, re-exported
# here because callers have always read it from this module. Its one definition
# lives in ergasterion/framework/generated.py, beside the two header forms the
# translator writes, so the text a writer emits and the text this reader looks
# for can never drift apart. Only files carrying it are linted -- this is the
# precise, self-maintaining definition of "emitted SQL".


@dataclass(frozen=True)
class DenyRule:
    token: str
    pattern: re.Pattern[str]
    message: str


def _deny_rules_for_adapter(adapter_name: str) -> list[DenyRule]:
    conventions = load_adapter_conventions(adapter_name)
    return [
        DenyRule(token=spec.token, pattern=spec.pattern, message=spec.message)
        for spec in conventions.deny_rules
    ]


# Every registered adapter's deny-list, sourced only through
# ergasterion.framework.adapters (owner ruling R10, D35): this module
# carries no deny-list data of its own. DUCKDB_DENY is kept as a named
# export -- tests/python/test_dialect_lint.py exercises the DuckDB family
# rule-by-rule through it -- as a read of adapter package data, not a
# Python literal. Indexed, not .get()-defaulted: a missing reference
# adapter package fails closed here rather than silently linting with an
# empty deny-list.
DENY_LISTS: dict[str, list[DenyRule]] = {name: _deny_rules_for_adapter(name) for name in ADAPTER_NAMES}
DUCKDB_DENY: list[DenyRule] = DENY_LISTS["duckdb"]


@dataclass(frozen=True)
class Offense:
    path: Path
    line_no: int
    token: str
    message: str
    line: str


def _is_generated(path: Path) -> bool:
    try:
        head = path.read_text(encoding="utf-8")[:400]
    except OSError:
        return False
    return GENERATED_MARKER in head


def emitted_sql_files(ctx: EstateContext | None = None) -> list[Path]:
    return [p for p in sorted((ctx or _DEFAULT_CTX).models_dir.rglob("*.sql")) if _is_generated(p)]


def test_sql_files(ctx: EstateContext | None = None) -> list[Path]:
    """Return hand-authored singular tests under ``tests/*.sql``.

    Never carries the emitted marker (the emitter does not write tests/), so every
    file here is hand-authored by definition -- same class as a hand-authored model:
    dbt executes it straight against the warehouse and it must be dialect-neutral at
    its own SSOT (the test file itself).
    """
    tests_dir = (ctx or _DEFAULT_CTX).tests_dir
    return sorted(tests_dir.rglob("*.sql")) if tests_dir.is_dir() else []


def hand_authored_sql_files(ctx: EstateContext | None = None) -> list[Path]:
    """Model SQL WITHOUT the generated marker -- edited directly at its own SSOT.

    These carry the same adapter-specific risk as generated SQL.
    They are neutralised via the cross-db macros (macros/cross_db.sql), which are the
    one sanctioned home for dialect-specific SQL and are never scanned here. Does NOT
    include tests/ -- see test_sql_files(), scanned as its own hand-authored class.
    """
    return [p for p in sorted((ctx or _DEFAULT_CTX).models_dir.rglob("*.sql")) if not _is_generated(p)]


def model_sql_files(ctx: EstateContext | None = None) -> list[Path]:
    """Every model SQL file -- emitted and hand-authored alike (models/ only)."""
    return sorted((ctx or _DEFAULT_CTX).models_dir.rglob("*.sql"))


def _quoted_or_nested_call_end(text: str, opening_paren: int) -> int | None:
    """Find the matching close paren while respecting SQL quotes and nested calls."""
    depth = 1
    quote: str | None = None
    index = opening_paren + 1
    while index < len(text):
        char = text[index]
        if quote:
            if char == quote:
                if quote == "'" and index + 1 < len(text) and text[index + 1] == "'":
                    index += 2
                    continue
                quote = None
            elif char == "\\" and index + 1 < len(text):
                index += 2
                continue
        elif char in ("'", '"'):
            quote = char
        elif char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
            if depth == 0:
                return index
        index += 1
    return None


def _top_level_call_arguments(body: str) -> list[str]:
    """Split one function body at commas outside quoted/nested expressions."""
    arguments: list[str] = []
    start = 0
    depth = 0
    quote: str | None = None
    index = 0
    while index < len(body):
        char = body[index]
        if quote:
            if char == quote:
                if quote == "'" and index + 1 < len(body) and body[index + 1] == "'":
                    index += 2
                    continue
                quote = None
            elif char == "\\" and index + 1 < len(body):
                index += 2
                continue
        elif char in ("'", '"'):
            quote = char
        elif char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
        elif char == "," and depth == 0:
            arguments.append(body[start:index].strip())
            start = index + 1
        index += 1
    arguments.append(body[start:].strip())
    return arguments


def _duckdb_regexp_replace_offenses(path: Path, text: str, rule: DenyRule) -> list[Offense]:
    """Catch direct regexp_replace calls unless they carry DuckDB's global flag.

    Global replacement is the factory semantic. DuckDB's default
    three-argument call replaces only the first match, so this small delimiter-aware
    scanner intentionally supports nested expressions, capture-pattern strings, and
    multiline formatting that a line-level regular expression cannot classify.
    """
    offenses: list[Offense] = []
    for match in re.finditer(r"\bregexp_replace\s*\(", text, re.IGNORECASE):
        opening_paren = text.find("(", match.start(), match.end())
        closing_paren = _quoted_or_nested_call_end(text, opening_paren)
        if closing_paren is None:
            continue
        arguments = _top_level_call_arguments(text[opening_paren + 1 : closing_paren])
        if len(arguments) == 4 and arguments[-1].strip().lower() == "'g'":
            continue
        line_no = text.count("\n", 0, match.start()) + 1
        line = text.splitlines()[line_no - 1].strip()
        offenses.append(
            Offense(
                path=path,
                line_no=line_no,
                token=rule.token,
                message=rule.message,
                line=line,
            )
        )
    return offenses


def scan_text(path: Path, text: str, rules: list[DenyRule]) -> list[Offense]:
    """Every deny-rule offense in one artefact's text. ``path`` names the
    artefact in the reported offense and is never read: the one scan
    implementation, shared by the on-disk sweep and by a caller holding
    generated text it has not written yet."""

    offenses: list[Offense] = []
    regexp_replace_rule = next((rule for rule in rules if rule.token == "regexp_replace_global"), None)
    if regexp_replace_rule is not None:
        offenses.extend(_duckdb_regexp_replace_offenses(path, text, regexp_replace_rule))
    for line_no, line in enumerate(text.splitlines(), start=1):
        for rule in rules:
            if rule.token == "regexp_replace_global":
                continue
            if rule.pattern.search(line):
                offenses.append(
                    Offense(
                        path=path,
                        line_no=line_no,
                        token=rule.token,
                        message=rule.message,
                        line=line.strip(),
                    )
                )
    return offenses


def scan(files: list[Path], rules: list[DenyRule]) -> list[Offense]:
    offenses: list[Offense] = []
    for path in files:
        offenses.extend(scan_text(path, path.read_text(encoding="utf-8"), rules))
    return offenses


def lint_artefacts(artefacts: dict[str, str], target: str) -> list[Offense]:
    """Scan generated artefact text, keyed by the path it will be written
    to, against ``target``'s deny-list. The gate an emitter runs before it
    writes anything, so check mode enforces exactly what write mode does."""

    rules = _rules_for(target)
    offenses: list[Offense] = []
    for name in sorted(artefacts):
        if not name.endswith(".sql"):
            continue
        offenses.extend(scan_text(Path(name), artefacts[name], rules))
    return offenses


def _rules_for(target: str) -> list[DenyRule]:
    empty_adapters = sorted(adapter for adapter, rules in DENY_LISTS.items() if not rules)
    if empty_adapters:
        raise ValueError(
            "dialect_lint: empty deny-list for registered adapter(s): "
            f"{', '.join(empty_adapters)} -- every registered adapter must declare "
            "at least one portability rule"
        )
    rules = DENY_LISTS.get(target)
    if rules is None:
        raise ValueError(f"dialect_lint: unknown target {target!r} (known: {sorted(DENY_LISTS)})")
    return rules


def lint_emitted(target: str = "bigquery", *, ctx: EstateContext | None = None) -> list[Offense]:
    """Scan emitted SQL for BigQuery-only constructs for the given emit target.

    Returns the list of offenses (empty == clean). Importable by the emitter so the
    deny-list lives in exactly one place.
    """
    return scan(emitted_sql_files(ctx), _rules_for(target))


def lint_models(target: str = "bigquery", *, ctx: EstateContext | None = None) -> list[Offense]:
    """Scan ALL model SQL and test SQL -- emitted AND hand-authored -- for the target.

    This is the comprehensive gate. Dialect-specific SQL is legitimate only inside the
    cross-db macros (macros/cross_db.sql), which are never scanned; every model and
    every test, whether the emitter wrote it or a human did, must be dialect-neutral
    at its own source file.
    """
    return scan(model_sql_files(ctx) + test_sql_files(ctx), _rules_for(target))


def main() -> int:
    parser = argparse.ArgumentParser(description="Per-adapter dialect lint over emitted SQL.")
    parser.add_argument(
        "--target",
        default="bigquery",
        choices=sorted(DENY_LISTS),
        help="Emit target whose dialect deny-list to enforce (default: bigquery).",
    )
    parser.add_argument(
        "--estate-root",
        type=Path,
        default=None,
        help="Estate root to lint (resolved from the environment or working directory when omitted).",
    )
    args = parser.parse_args()

    ctx = EstateContext.resolve(estate_root=args.estate_root)
    emitted = emitted_sql_files(ctx)
    hand_authored = hand_authored_sql_files(ctx)
    tests = test_sql_files(ctx)
    offenses = lint_models(args.target, ctx=ctx)

    if offenses:
        print(f"dialect-lint FAIL [{args.target}]: {len(offenses)} offender(s) in model/test SQL")
        for off in offenses:
            rel = off.path.relative_to(ctx.root).as_posix()
            kind = "emitted" if _is_generated(off.path) else "hand-authored"
            print(f"  {rel}:{off.line_no}  [{off.token}]  ({kind})  {off.message}")
            print(f"      > {off.line}")
        return 1

    print(
        f"dialect-lint OK [{args.target}]: "
        f"{len(emitted)} emitted + {len(hand_authored)} hand-authored model + "
        f"{len(tests)} test SQL file(s) clean"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
