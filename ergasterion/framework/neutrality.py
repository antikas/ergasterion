"""The neutrality gate: rejects technology syntax in an inline SQL expression.

Architecture sections 3.2 and 3.4, owner rulings R2 and R3. This gate is
deliberately narrow: it never parses or polices which SQL constructs an
expression uses (sqlglot is the real parser). It only rejects
the technology markers that would tie a declaration to one translator or
platform:

  * a Jinja/dbt tag, ``{{``;
  * a dbt ``ref(`` call;
  * a dbt ``source(`` call;
  * a URL scheme (``s3://``, ``https://``, and so on);
  * a filesystem path (a POSIX path with two or more segments, or a
    Windows drive-letter path).

These five markers are rejected in both estate expression modes (``sql``
and ``named_only``). Under ``named_only`` a stricter rule also applies:
any inline expression at all is rejected, content aside, because the
estate has declared that every business rule is a named rule.
"""

from __future__ import annotations

import re

from ergasterion.framework.models import FrameworkError

MODE_SQL = "sql"
MODE_NAMED_ONLY = "named_only"
EXPRESSION_MODES: tuple[str, ...] = (MODE_SQL, MODE_NAMED_ONLY)


class NeutralityViolationError(FrameworkError):
    """Raised when an inline expression carries a forbidden technology
    marker, or when any inline expression at all appears under
    ``named_only`` mode. ``context`` is a caller-supplied string
    identifying where the expression came from (for example
    ``"customer:steps[3].fields[0].expression"``); ``expression_text`` is
    the exact text that failed; ``reason`` is a short human explanation."""

    code = "neutrality_violation"

    def __init__(self, context: str, expression_text: str, reason: str) -> None:
        self.context = context
        self.expression_text = expression_text
        self.reason = reason
        super().__init__(f"{context}: {reason}: {expression_text!r}")


# Each marker is checked independently and all findings are reported, in this
# fixed order, so a caller sees every technology marker an expression carries
# rather than only the first one a short-circuit happened to hit.
_JINJA_TAG = "{{"
_REF_CALL = re.compile(r"\bref\s*\(", re.IGNORECASE)
_SOURCE_CALL = re.compile(r"\bsource\s*\(", re.IGNORECASE)
_URL_SCHEME = re.compile(r"\b[A-Za-z][A-Za-z0-9+.\-]*://")
# A path needs at least two "/"-separated segments so ordinary SQL division
# (`amount / total`) never matches; a Windows drive-letter path is matched
# separately since it never uses "/".
_UNIX_PATH = re.compile(r"(?:^|[\s(\'\"=,])(\.{0,2}/[\w.\-]+(?:/[\w.\-]+)+)")
_WINDOWS_PATH = re.compile(r"[A-Za-z]:\\[\w\\.\-]*")


def find_markers(text: str) -> tuple[str, ...]:
    """Every forbidden marker found in ``text``, in the stable order
    ``jinja_tag``, ``ref_call``, ``source_call``, ``url_scheme``,
    ``filesystem_path``. An empty tuple means the text carries none of
    them. This function never judges the SQL itself: only these five
    technology markers architecture section 3.4 and check 7 name."""

    markers: list[str] = []
    if _JINJA_TAG in text:
        markers.append("jinja_tag")
    if _REF_CALL.search(text):
        markers.append("ref_call")
    if _SOURCE_CALL.search(text):
        markers.append("source_call")
    if _URL_SCHEME.search(text):
        markers.append("url_scheme")
    if _UNIX_PATH.search(text) or _WINDOWS_PATH.search(text):
        markers.append("filesystem_path")
    return tuple(markers)


def validate_expression(text: str, *, mode: str, context: str) -> None:
    """Raise ``NeutralityViolationError`` when ``text`` is not permitted
    under ``mode``.

    ``mode`` must be ``MODE_SQL`` or ``MODE_NAMED_ONLY``: any other value
    is a caller programming error, not a declaration failure, and raises
    plain ``FrameworkError`` rather than being silently treated as one
    mode or the other.

    Under ``MODE_NAMED_ONLY``, any non-blank inline expression is rejected
    outright. Under both modes, the five technology markers are rejected
    unconditionally: the "no inline SQL at all" rule is mode-specific, the
    neutrality gate itself is not.
    """

    if mode not in EXPRESSION_MODES:
        raise FrameworkError(f"unknown expression mode: {mode!r}; must be one of {EXPRESSION_MODES!r}")

    if mode == MODE_NAMED_ONLY and text.strip():
        raise NeutralityViolationError(
            context, text, "inline expression present under named_only estate policy"
        )

    markers = find_markers(text)
    if markers:
        raise NeutralityViolationError(context, text, f"technology syntax found: {', '.join(markers)}")
