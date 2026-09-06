"""The inline-SQL parser port (architecture sections 3.2 and 3.4; owner
ruling R3, plan decision D22).

Inline SQL is the declaration language: no closed subset, no fixed function
list, no transpilation. A real SQL parser sits behind this port so the
engine's own code (``ergasterion/framework/expressions.py``) depends on a
small, stable, dialect-neutral contract rather than on any one parser
library's own API. The shipped adapter is sqlglot, in
``ergasterion/framework/expression_sqlglot.py``; a future estate could ship
a different ``ExpressionParserPort`` implementation without any change
above this port.

This module carries the port's data shapes and its one error type only. It
knows nothing about a product, an occurrence, a declared schema, or a group
set: those are ``expressions.py``'s job, one layer up, where a parser's raw
facts are turned into the fail-closed rules architecture section 3.2 and
check 11 name (unresolved column, non-aggregated column outside the group
set, product/occurrence/position identity on a syntax error).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True)
class SourcePosition:
    """A 1-based line/column position inside an inline expression's own
    text, exactly as the parser reports it. Never a position inside the
    surrounding YAML document: the parser knows only the expression string
    it was handed, not where that string sits in a product declaration
    file."""

    line: int
    column: int

    def __str__(self) -> str:
        return f"line {self.line}, column {self.column}"


class ParserSyntaxError(Exception):
    """Raised by a port implementation when text does not parse as exactly
    one well-formed SQL expression or statement in the requested dialect:
    invalid syntax, an empty or blank expression, or more than one
    statement smuggled into one expression position (a lone trailing
    semicolon is not a second statement). ``position`` is the first
    reported failure point when the underlying parser can name one, else
    ``None``; ``detail`` is the parser's own message, always non-empty.

    This is a port-level error: it names no product or occurrence.
    ``expressions.py`` catches it and re-raises ``ExpressionError`` with
    that identity attached."""

    def __init__(self, detail: str, *, position: SourcePosition | None = None) -> None:
        self.detail = detail
        self.position = position
        super().__init__(detail)


@dataclass(frozen=True)
class ParsedExpression:
    """One parsed inline SQL expression's dialect-neutral facts.

    ``text`` is the caller's exact original source string. Architecture
    section 3.2 renders inline SQL VERBATIM into generated artefacts, never
    a parser's own re-serialisation, so this is never a parser's ``.sql()``
    output -- a port implementation must return the text it was handed,
    unchanged.

    ``columns`` are the column identifiers the expression reads: every leaf
    column reference found anywhere in the parse tree, in source order,
    duplicate-free, exactly as written (case preserved, any table qualifier
    dropped -- this port models one flat visible-schema namespace, the same
    model ``ergasterion/framework/declaration.py`` and the
    ``sources[].expect.fields`` list already use).

    ``non_aggregated_columns`` is the subset of ``columns`` that are NOT
    nested inside an aggregate function call anywhere in the tree (for
    example, in ``SUM(amount) + fee``, ``fee`` is non-aggregated and
    ``amount`` is not). This is the raw fact the aggregation rule needs:
    ``expressions.py`` checks every name in this tuple against a group set
    and fails closed on anything outside it. It is always a subset of
    ``columns``, in the same source order.

    ``is_aggregate`` is true when the expression contains at least one
    aggregate function call anywhere in the tree (for example ``SUM(...)``,
    ``COUNT(...)``). A window function such as ``SUM(x) OVER (...)`` is NOT
    an aggregate by this definition: it does not collapse rows the way a
    GROUP BY aggregate does, so a bare column beside it is still ordinary,
    row-level SQL."""

    text: str
    columns: tuple[str, ...]
    non_aggregated_columns: tuple[str, ...]
    is_aggregate: bool


@dataclass(frozen=True)
class ParsedSelect:
    """One parsed whole SELECT statement's dialect-neutral facts.

    ``output_columns`` are the select list's output names in declared
    order: each entry is a bare column's own name, its explicit ``AS``
    alias, or the literal token ``"*"`` where the select list carries a
    star projection (``expressions.py`` expands ``"*"`` against the input
    relation schema it is given; this port does not know that schema).

    ``columns_read`` is every input column the statement's projections,
    filters, and grouping reference anywhere in the tree -- the same flat,
    case-preserving, qualifier-dropped model as
    ``ParsedExpression.columns`` -- used for field lineage exactly as a
    calculated field's columns are.

    ``has_from`` is true when the statement names its own FROM source. A
    caller that binds the input relation itself -- as a translator does
    when it renders a declared select body over the composition's input --
    reads this to reject a body that names one instead.

    ``text`` is the caller's exact original source, verbatim, as with
    ``ParsedExpression.text``."""

    text: str
    output_columns: tuple[str, ...]
    columns_read: tuple[str, ...]
    has_from: bool = False


class ExpressionParserPort(Protocol):
    """The port every parser adapter implements. The shipped adapter is
    ``ergasterion.framework.expression_sqlglot.SqlglotExpressionParser``.
    Both methods raise ``ParserSyntaxError`` on anything that is not
    exactly one well-formed SQL expression or statement in ``dialect``.
    Neither method ever transpiles: ``dialect`` controls what the parser
    ACCEPTS, never what it emits, since nothing here re-serialises the
    source text (owner ruling R3: "no transpilation in the engine")."""

    def parse_expression(self, text: str, *, dialect: str) -> ParsedExpression:
        """Parse ``text`` as one scalar or aggregate SQL expression: a
        calculated field, a filter predicate, or an aggregate expression,
        never a whole SELECT statement (a port implementation fails closed
        with ``ParserSyntaxError`` when ``text`` is a whole select body;
        use ``parse_select`` for that)."""
        ...

    def parse_select(self, text: str, *, dialect: str) -> ParsedSelect:
        """Parse ``text`` as one whole SQL SELECT statement."""
        ...

    def parse_statement(self, text: str, *, dialect: str) -> None:
        """Parse ``text`` as exactly one SQL statement of any shape, for the
        syntax proof alone: a query built from set operations, common table
        expressions or anything else the dialect accepts. Returns nothing --
        no columns are resolved and no output shape is derived, because a
        caller proving that a generated artefact parses for an adapter is
        asking only whether the dialect accepts it. Raises
        ``ParserSyntaxError`` when it does not."""
        ...
