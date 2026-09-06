"""The neutrality gate: no forbidden legacy-vocabulary word survives in engine source.

Scans every ``.py`` file under the paths named in
``scripts/layer_neutrality_scope.txt`` for the three words architecture
check 8 and ``docs/items/CONSTRAINTS.md`` name (a closed, historical
three-tier vocabulary this engine no longer carries -- see those documents
for the exact words and why), any case, appearing in an identifier, a string
literal, a comment or the file's own name -- and fails closed on any finding
that is not named in ``scripts/layer_neutrality_allowlist.txt``.

The scope file starts empty (report mode). The portable IDL and the code
generated from it still carry identifiers and a file name drawn from that
vocabulary, so scanning ``ergasterion/`` would be red until that surface is
re-issued under neutral names; the scope is widened to ``ergasterion/`` at
that point. The allowlist is committed empty and stays empty: a real finding
is fixed by renaming, never by allow-listing it away.

This module's own source carries none of the three forbidden words in
scannable form (not as an identifier, a string literal or a comment): the
word list below is assembled at import time from character codepoints,
never spelled out, precisely so that widening the scope to include this
file's own path finds nothing to flag here and needs no
self-exclusion.

A forbidden word is matched as a plain, case-insensitive substring,
deliberately not word-bounded (see below) -- except where the exact token
segment carrying it is itself unrelated vocabulary from another domain
(``NON_LAYER_VOCABULARY``): a segment that merely contains an exempt term
without being exactly equal to it still fires. This is a matcher
correction, not an allowlist entry -- it applies everywhere that
vocabulary occurs, not to one file or line.

Usage:
    python -m ergasterion.framework.layer_neutrality --check
    python -m ergasterion.framework.layer_neutrality --check --scope PATH --allowlist PATH --root PATH
"""

from __future__ import annotations

import argparse
import io
import re
import tokenize
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent.parent

DEFAULT_SCOPE_FILE = REPO_ROOT / "scripts" / "layer_neutrality_scope.txt"
DEFAULT_ALLOWLIST_FILE = REPO_ROOT / "scripts" / "layer_neutrality_allowlist.txt"

# The three forbidden words, built from character codepoints rather than
# spelled out as string literals: a literal spelling here would itself be a
# finding the moment this file's own path enters scope. Case-insensitive
# substring match, deliberately not word-bounded: the words this gate forbids
# show up glued into identifiers by naming convention throughout the
# pre-profile codebase (see docs/items/CONSTRAINTS.md and architecture check
# 8), so a whole-word check would silently pass exactly the cases the gate
# exists to catch.
_FORBIDDEN_WORD_CODEPOINTS: tuple[tuple[int, ...], ...] = (
    (98, 114, 111, 110, 122, 101),
    (115, 105, 108, 118, 101, 114),
    (103, 111, 108, 100),
)

FORBIDDEN_WORDS: tuple[str, ...] = tuple(
    "".join(chr(codepoint) for codepoint in codepoints) for codepoints in _FORBIDDEN_WORD_CODEPOINTS
)

# Non-layer vocabulary: whole words from another domain that happen to carry
# a forbidden word as a plain substring, exempted everywhere they occur as
# their own exact token segment. "golden" is the Data Vault "golden record"
# survivorship term (architecture section 4, Data Curation row; D30/R4) --
# target vocabulary the engine keeps forever, unrelated to the layer name it
# happens to start with. Not an allowlist entry: an allowlist entry
# grandfathers one file or line; this corrects the detector for the term
# itself, wherever it appears.
NON_LAYER_VOCABULARY: frozenset[str] = frozenset({"golden"})

# A run of letters/digits: any other character (underscore, hyphen,
# whitespace, dot, quotes, punctuation) already breaks a raw token, so this
# pattern alone isolates every delimiter the segmentation rule names except
# camelCase.
_RAW_TOKEN = re.compile(r"[A-Za-z0-9]+")

# A camelCase boundary: a lowercase letter or digit immediately followed by
# an uppercase letter. Splitting a raw token on this pattern turns
# "GoldenRecord" into ["Golden", "Record"] and leaves an all-caps or
# all-lowercase compound (no internal case change) as one unsplit segment.
_CAMEL_BOUNDARY = re.compile(r"(?<=[a-z0-9])(?=[A-Z])")


def _segments(text: str) -> list[str]:
    """Break ``text`` into the segments the gate reasons about: every raw
    letters/digits run (already isolated from underscore, hyphen,
    whitespace, dot and any other punctuation), each further split at every
    camelCase boundary."""

    segments: list[str] = []
    for raw in _RAW_TOKEN.findall(text):
        segments.extend(_CAMEL_BOUNDARY.split(raw))
    return segments


@dataclass(frozen=True)
class Violation:
    """One finding: which file, what kind of token, which line, which
    forbidden word, and the exact text it was found in."""

    path: str
    category: str  # "filename" | "identifier" | "string" | "comment" | "unreadable"
    line: int
    word: str
    text: str

    def render(self) -> str:
        return f"{self.path}:{self.line}: {self.category} carries a forbidden word {self.word!r}: {self.text!r}"


def _contains_forbidden_word(text: str) -> str | None:
    """The first forbidden word carried by ``text``, or ``None``.

    Still a case-insensitive substring match, not word-bounded -- but a
    match is suppressed when the exact segment carrying it (see
    ``_segments``) is, case-insensitively, one of ``NON_LAYER_VOCABULARY``.
    ``golden_key``, ``GoldenRecord`` and ``golden`` standing alone in a
    comment do not fire. A segment that is the bare forbidden word itself
    (in any case or camelCase compound), or that merely contains an exempt
    term glued to unrelated text without being exactly equal to it, still
    fires -- including a compound that also carries the exempt term as a
    separate segment of its own.
    """

    segments = _segments(text)
    for word in FORBIDDEN_WORDS:
        for segment in segments:
            lowered_segment = segment.lower()
            if word in lowered_segment and lowered_segment not in NON_LAYER_VOCABULARY:
                return word
    return None


def _read_list_file(path: Path) -> tuple[str, ...]:
    """Every non-blank, non-comment line of a scope/allowlist file, in file
    order. A missing file fails closed: the committed files always exist,
    and a caller pointing at a path that is not there has misconfigured the
    gate, not asked for an empty scope or allowlist."""

    if not path.is_file():
        raise FileNotFoundError(f"layer-neutrality list file is missing: {path}")
    lines = []
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        lines.append(line)
    return tuple(lines)


def scan_file(path: Path, *, relative_to: Path) -> list[Violation]:
    """Every finding in one .py file: its own file name, and every
    identifier, string literal and comment token in its source. A file the
    gate cannot tokenize is reported as a finding, never silently skipped."""

    relative = path.relative_to(relative_to).as_posix()
    violations: list[Violation] = []

    filename_word = _contains_forbidden_word(path.name)
    if filename_word:
        violations.append(Violation(relative, "filename", 0, filename_word, path.name))

    try:
        text = path.read_text(encoding="utf-8")
        tokens = list(tokenize.generate_tokens(io.StringIO(text).readline))
    except (SyntaxError, tokenize.TokenError, UnicodeDecodeError, IndentationError) as exc:
        violations.append(Violation(relative, "unreadable", 0, "", f"could not tokenize: {exc}"))
        return violations

    for tok in tokens:
        if tok.type == tokenize.NAME:
            word = _contains_forbidden_word(tok.string)
            if word:
                violations.append(Violation(relative, "identifier", tok.start[0], word, tok.string))
        elif tok.type == tokenize.STRING:
            word = _contains_forbidden_word(tok.string)
            if word:
                violations.append(Violation(relative, "string", tok.start[0], word, tok.string))
        elif tok.type == tokenize.COMMENT:
            word = _contains_forbidden_word(tok.string)
            if word:
                violations.append(Violation(relative, "comment", tok.start[0], word, tok.string))

    return violations


def scan_scope(scope_entries: tuple[str, ...], *, repo_root: Path = REPO_ROOT) -> list[Violation]:
    """Every violation across every .py file under every scoped path, sorted
    for deterministic output. An empty scope scans nothing and returns no
    violations: this is the gate's report-mode behaviour."""

    files: set[Path] = set()
    for entry in scope_entries:
        base = (repo_root / entry).resolve()
        if base.is_file() and base.suffix == ".py":
            files.add(base)
        elif base.is_dir():
            files.update(base.rglob("*.py"))

    violations: list[Violation] = []
    for file_path in sorted(files):
        violations.extend(scan_file(file_path, relative_to=repo_root))
    return sorted(violations, key=lambda v: (v.path, v.line, v.category, v.word))


def filter_allowed(violations: list[Violation], allowlist: tuple[str, ...]) -> list[Violation]:
    """Drop any violation named in the allowlist, by exact file path or by
    ``path:line``. The shipped allowlist is empty and stays empty (no legacy
    compatibility anywhere); this function exists so the gate's contract is
    complete and testable even though nothing ships in it."""

    allowed = set(allowlist)
    return [v for v in violations if v.path not in allowed and f"{v.path}:{v.line}" not in allowed]


def run_check(
    *,
    scope_file: Path = DEFAULT_SCOPE_FILE,
    allowlist_file: Path = DEFAULT_ALLOWLIST_FILE,
    repo_root: Path = REPO_ROOT,
) -> list[Violation]:
    """The gate's whole evaluation: read the scope and allowlist named by the
    arguments (the committed files by default), scan, and filter. Returns
    the unallowed violations; an empty list is a pass. Every argument is a
    real injection point: a caller (a test, or main()'s own CLI arguments)
    can point the whole evaluation at a scratch scope, allowlist and
    repository root, never only at the shipped, currently-empty scope."""

    scope_entries = _read_list_file(scope_file)
    allowlist = _read_list_file(allowlist_file)
    violations = scan_scope(scope_entries, repo_root=repo_root)
    return filter_allowed(violations, allowlist)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check",
        action="store_true",
        help="Evaluate the gate and exit non-zero on any unallowed finding (the gate has no write mode).",
    )
    parser.add_argument(
        "--scope",
        type=Path,
        default=DEFAULT_SCOPE_FILE,
        help="Scope list file (default: the committed scripts/layer_neutrality_scope.txt).",
    )
    parser.add_argument(
        "--allowlist",
        type=Path,
        default=DEFAULT_ALLOWLIST_FILE,
        help="Allowlist file (default: the committed scripts/layer_neutrality_allowlist.txt).",
    )
    parser.add_argument(
        "--root",
        type=Path,
        default=REPO_ROOT,
        help="Repository root scope entries resolve against (default: the repository root).",
    )
    args = parser.parse_args(argv)
    violations = run_check(scope_file=args.scope, allowlist_file=args.allowlist, repo_root=args.root)
    if violations:
        print(f"layer-neutrality FAIL: {len(violations)} finding(s)")
        for violation in violations:
            print("  " + violation.render())
        return 1
    print("layer-neutrality OK: no forbidden word found in scope")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
