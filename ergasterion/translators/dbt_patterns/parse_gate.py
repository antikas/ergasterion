"""The per-adapter parse gate for generated dbt SQL.

Architecture section 12's evidence item 3 requires every artefact to parse
for every declared adapter. A generated dbt model is not plain SQL: it
carries the model's configuration and every relation reference and dispatch
macro call as Jinja. This module resolves that Jinja into the SQL one named
adapter would compile, then parses the result in that adapter's own dialect
through the engine's parser port.

Resolution is a closed set, and anything outside it fails closed naming the
artefact and the construct. There is no pass-through and no best-effort
strip: a construct this gate does not understand is a construct it cannot
honestly claim to have parsed.

  * a ``config(...)`` call is the model's dbt configuration, not SQL, and is
    removed;
  * ``ref`` and ``source`` become a relation name quoted the way the
    adapter's own ``identifier_rules`` quote one;
  * ``dpf_quote`` becomes its declared physical name wrapped in the same
    adapter's own quote character -- the real per-adapter text, because
    quoting is exactly where the two platforms diverge and the generated
    model deliberately carries none of it;
  * ``dpf_type`` and ``dpf_decimal_type`` become the concrete physical type
    the adapter's own ``type_mapping`` declares -- the real per-adapter
    text, because a cast target is exactly where two dialects diverge. A
    declared decimal takes that adapter's decimal type name and the
    precision and scale the declaration states;
  * every other dispatch or named-rule macro call keeps its name and its
    arguments and loses only the Jinja braces, so the columns it reads stay
    visible to the parser. The macro's own body is fixed SQL in ``macros/``
    rather than generated per product, and the reference adapter's build is
    what proves it;
  * a Jinja statement (``{% ... %}``) or an expression that is not one such
    call fails closed.
"""

from __future__ import annotations

import re
from typing import Sequence

from ergasterion.framework.adapters import IDENTIFIER_QUOTE_CHARACTER, load_adapter_conventions
from ergasterion.framework.expressions import parse_statement
from ergasterion.framework.models import FrameworkError

CONFIG_CALL = "config"
REF_CALLS: frozenset[str] = frozenset({"ref", "source"})
TYPE_CALL = "dpf_type"
QUOTE_CALL = "dpf_quote"
DECIMAL_TYPE_CALL = "dpf_decimal_type"
# The neutral token whose adapter mapping names the decimal type family.
DECIMAL_TYPE_TOKEN = "numeric"

_STATEMENT_TAG = re.compile(r"\{%.*?%\}", re.DOTALL)
_EXPRESSION_TAG = re.compile(r"\{\{(.*?)\}\}", re.DOTALL)
_CALL = re.compile(r"^\s*([a-z_][a-z0-9_]*)\s*\((.*)\)\s*$", re.DOTALL)
_STRING_ARGUMENT = re.compile(r"""^\s*(?:'([^']*)'|"([^"]*)")\s*$""")


class ArtefactParseError(FrameworkError):
    """A generated artefact does not resolve or does not parse for one
    declared adapter. Names the artefact, the adapter and the reason."""

    code = "artefact_parse_error"

    def __init__(self, *, artefact: str, adapter: str, detail: str) -> None:
        self.artefact = artefact
        self.adapter = adapter
        self.detail = detail
        super().__init__(f"artefact {artefact!r} for adapter {adapter!r}: {detail}")


def _arguments(body: str) -> list[str]:
    """Split a call's argument list on top-level commas."""

    arguments: list[str] = []
    depth = 0
    quote: str | None = None
    current: list[str] = []
    for character in body:
        if quote is not None:
            current.append(character)
            if character == quote:
                quote = None
            continue
        if character in "'\"":
            quote = character
            current.append(character)
            continue
        if character in "([":
            depth += 1
        elif character in ")]":
            depth -= 1
        if character == "," and depth == 0:
            arguments.append("".join(current))
            current = []
            continue
        current.append(character)
    tail = "".join(current)
    if tail.strip() or arguments:
        arguments.append(tail)
    return arguments


def _string_argument(argument: str) -> str | None:
    match = _STRING_ARGUMENT.match(argument)
    if match is None:
        return None
    return match.group(1) if match.group(1) is not None else match.group(2)


def _resolve_expression(
    expression: str, *, artefact: str, adapter: str, quote: str, type_mapping: dict[str, str]
) -> str:
    match = _CALL.match(expression)
    if match is None:
        raise ArtefactParseError(
            artefact=artefact,
            adapter=adapter,
            detail=f"unresolvable template expression {expression.strip()!r}: it is not a macro call",
        )
    name, body = match.group(1), match.group(2)
    arguments = _arguments(body)

    if name == CONFIG_CALL:
        return ""
    if name in REF_CALLS:
        parts = [_string_argument(argument) for argument in arguments]
        if not parts or any(part is None for part in parts):
            raise ArtefactParseError(
                artefact=artefact,
                adapter=adapter,
                detail=f"{name}(...) takes plain string arguments, found {body.strip()!r}",
            )
        return f"{quote}{'.'.join(str(part) for part in parts)}{quote}"
    if name == QUOTE_CALL:
        declared = _string_argument(arguments[0]) if len(arguments) == 1 else None
        if declared is None:
            raise ArtefactParseError(
                artefact=artefact,
                adapter=adapter,
                detail=f"{QUOTE_CALL}(...) takes one plain string argument, found {body.strip()!r}",
            )
        if quote in declared:
            raise ArtefactParseError(
                artefact=artefact,
                adapter=adapter,
                detail=(
                    f"{QUOTE_CALL}({declared!r}) carries this adapter's own quote character, so "
                    "the quoting it resolves to would not close"
                ),
            )
        return f"{quote}{declared}{quote}"
    if name == TYPE_CALL:
        token = _string_argument(arguments[0]) if arguments else None
        physical = type_mapping.get(str(token))
        if physical is None:
            raise ArtefactParseError(
                artefact=artefact,
                adapter=adapter,
                detail=f"{TYPE_CALL}({token!r}) names no type this adapter's conventions map",
            )
        return physical
    if name == DECIMAL_TYPE_CALL:
        if len(arguments) != 2:
            raise ArtefactParseError(
                artefact=artefact,
                adapter=adapter,
                detail=f"{DECIMAL_TYPE_CALL} takes a precision and a scale, found {body.strip()!r}",
            )
        # The adapter's own decimal type name, from the same conventions
        # table dpf_type resolves through, carrying the declared precision
        # and scale rather than whichever default the mapping records.
        physical = type_mapping.get(DECIMAL_TYPE_TOKEN)
        if physical is None:
            raise ArtefactParseError(
                artefact=artefact,
                adapter=adapter,
                detail=(
                    f"{DECIMAL_TYPE_CALL} needs the {DECIMAL_TYPE_TOKEN!r} type this adapter's "
                    "conventions map, and they map none"
                ),
            )
        base = physical.split("(", 1)[0].strip()
        return f"{base}({arguments[0].strip()}, {arguments[1].strip()})"
    return f"{name}({body})"


def resolve_for_adapter(text: str, *, artefact: str, adapter: str) -> str:
    """The SQL ``adapter`` would compile from generated dbt model ``text``,
    with every template construct resolved as the module docstring
    describes."""

    # ``load_adapter_conventions`` owns what an adapter's identifier rules
    # must state and fails closed on a conventions file that states no quote
    # character, so this reads the answer rather than checking it again.
    conventions = load_adapter_conventions(adapter)
    quote = str(conventions.identifier_rules[IDENTIFIER_QUOTE_CHARACTER])
    statement = _STATEMENT_TAG.search(text)
    if statement is not None:
        raise ArtefactParseError(
            artefact=artefact,
            adapter=adapter,
            detail=f"unresolvable template statement {statement.group(0).strip()!r}",
        )

    def replace(match: re.Match[str]) -> str:
        return _resolve_expression(
            match.group(1),
            artefact=artefact,
            adapter=adapter,
            quote=quote,
            type_mapping=conventions.type_mapping,
        )

    return _EXPRESSION_TAG.sub(replace, text)


def assert_parses(artefacts: dict[str, str], *, adapters: Sequence[str]) -> None:
    """Fail closed unless every ``.sql`` artefact resolves and parses for
    every declared adapter, naming the artefact, the adapter and the
    position the parser reported."""

    for adapter in adapters:
        dialect = load_adapter_conventions(adapter).dialect
        for artefact in sorted(artefacts):
            if not artefact.endswith(".sql"):
                continue
            resolved = resolve_for_adapter(artefacts[artefact], artefact=artefact, adapter=adapter)
            if not resolved.strip():
                raise ArtefactParseError(
                    artefact=artefact, adapter=adapter, detail="resolves to no SQL at all"
                )
            parse_statement(
                resolved, product=artefact, occurrence=f"adapter {adapter}", dialect=dialect
            )
