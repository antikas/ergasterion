"""Inline SQL expression validation (architecture sections 3.2, 3.4, 9;
owner ruling R3, plan decision D22).

This is the layer above the parser port
(``ergasterion/framework/expression_port.py``) and its shipped sqlglot
adapter (``ergasterion/framework/expression_sqlglot.py``). It turns a
port's raw parse facts into the fail-closed, named rules architecture
section 3.2 and check 11 describe:

  * a syntax error fails closed naming product, occurrence and position;
  * an unresolved column (one the schema visible at the occurrence does
    not carry) fails closed;
  * a non-aggregated column outside the group set fails closed (the
    aggregation rule, for an aggregate-context expression such as a
    ``data_aggregation`` aggregate);
  * the columns an expression reads are exposed for field lineage;
  * a whole select body parses and its output columns are derived from the
    input relation schema it is given.

Every expression is parsed in the reference adapter's dialect, DuckDB
(``REFERENCE_DIALECT``): the reference adapter is the engine's executable
truth (architecture section 10), so this is the one dialect the engine
itself ever needs to understand an inline expression's shape in. The text
is never transpiled and never re-rendered: what a caller gets back
(``ParsedExpression.text`` / ``ParsedSelect.text``) is byte-identical to
what it passed in, ready to render verbatim into a generated artefact.

The parser is injected (``parser: ExpressionParserPort | None``), never a
hidden module global a caller cannot substitute: the default is the
shipped sqlglot adapter, but a test or a future estate can pass any other
``ExpressionParserPort`` implementation."""

from __future__ import annotations

from typing import Iterable

from ergasterion.framework.expression_port import (
    ExpressionParserPort,
    ParsedExpression,
    ParsedSelect,
    ParserSyntaxError,
    SourcePosition,
)
from ergasterion.framework.expression_sqlglot import SqlglotExpressionParser
from ergasterion.framework.models import FrameworkError

# Re-exported so a caller needs only this module for the common case.
__all__ = [
    "REFERENCE_DIALECT",
    "ExpressionError",
    "ParsedExpression",
    "ParsedSelect",
    "SourcePosition",
    "parse_expression",
    "parse_statement",
    "resolve_expression_columns",
    "check_aggregation_rule",
    "parse_and_validate_expression",
    "parse_select_body",
]

# The reference adapter's dialect (architecture section 10: "One adapter is
# the reference adapter that executes the whole estate locally"; owner
# ruling R10: DuckDB is that adapter for this estate).
REFERENCE_DIALECT = "duckdb"

_DEFAULT_PARSER: ExpressionParserPort = SqlglotExpressionParser()


class ExpressionError(FrameworkError):
    """One inline-expression validation failure. Always names the product,
    the occurrence, and a stable rule slug (``"syntax_error"``,
    ``"unresolved_column"`` or ``"non_aggregated_column"``), mirroring
    ``ergasterion.framework.declaration.DeclarationError``'s identity so a
    caller composing both layers reports failures the same way. ``position``
    is set only for ``"syntax_error"``, when the parser could name one."""

    code = "expression_error"

    def __init__(
        self,
        *,
        product: str,
        occurrence: str,
        rule: str,
        detail: str,
        position: SourcePosition | None = None,
    ) -> None:
        self.product = product
        self.occurrence = occurrence
        self.rule = rule
        self.detail = detail
        self.position = position
        position_text = f" at {position}" if position is not None else ""
        super().__init__(f"product {product!r}: rule {rule!r} occurrence {occurrence!r}{position_text}: {detail}")


def parse_expression(
    text: str,
    *,
    product: str,
    occurrence: str,
    dialect: str = REFERENCE_DIALECT,
    parser: ExpressionParserPort | None = None,
) -> ParsedExpression:
    """Parse ``text`` as one scalar or aggregate SQL expression. Raises
    ``ExpressionError(rule="syntax_error")`` naming ``product``,
    ``occurrence`` and, where the parser can name one, the failure
    position, on anything that does not parse as exactly one well-formed
    expression."""

    active_parser = parser if parser is not None else _DEFAULT_PARSER
    try:
        return active_parser.parse_expression(text, dialect=dialect)
    except ParserSyntaxError as exc:
        raise ExpressionError(
            product=product,
            occurrence=occurrence,
            rule="syntax_error",
            detail=exc.detail,
            position=exc.position,
        ) from exc


def parse_statement(
    text: str,
    *,
    product: str,
    occurrence: str,
    dialect: str,
    parser: ExpressionParserPort | None = None,
) -> None:
    """Prove that ``text`` parses as exactly one SQL statement in
    ``dialect``. Used for the syntax half of architecture section 12's
    evidence item 3, "every artefact parses for every declared adapter":
    the caller names the artefact as the ``occurrence`` and the adapter
    picks the dialect, so a failure reads exactly like every other
    expression failure. Unlike ``parse_expression`` and
    ``parse_select_body``, ``dialect`` has no default here: proving an
    artefact parses is always a claim about one named adapter."""

    active_parser = parser if parser is not None else _DEFAULT_PARSER
    try:
        active_parser.parse_statement(text, dialect=dialect)
    except ParserSyntaxError as exc:
        raise ExpressionError(
            product=product,
            occurrence=occurrence,
            rule="syntax_error",
            detail=exc.detail,
            position=exc.position,
        ) from exc


def resolve_expression_columns(
    parsed: ParsedExpression, schema: Iterable[str], *, product: str, occurrence: str
) -> None:
    """Fail closed with ``ExpressionError(rule="unresolved_column")`` on the
    first column ``parsed`` reads that ``schema`` (the columns visible at
    this occurrence) does not carry."""

    visible = frozenset(schema)
    for column in parsed.columns:
        if column not in visible:
            raise ExpressionError(
                product=product,
                occurrence=occurrence,
                rule="unresolved_column",
                detail=f"column {column!r} is not in the schema visible at this occurrence: {sorted(visible)!r}",
            )


def check_aggregation_rule(
    parsed: ParsedExpression, group_columns: Iterable[str], *, product: str, occurrence: str
) -> None:
    """The aggregation rule for an aggregate-context expression (for
    example a ``data_aggregation`` aggregate): fail closed with
    ``ExpressionError(rule="non_aggregated_column")`` on the first column
    ``parsed`` references outside any aggregate function call that is not
    also in ``group_columns``. Never called for a non-aggregate-context
    expression (a calculated field, a filter predicate): those pass no
    group set, because they have none."""

    group_set = frozenset(group_columns)
    for column in parsed.non_aggregated_columns:
        if column not in group_set:
            raise ExpressionError(
                product=product,
                occurrence=occurrence,
                rule="non_aggregated_column",
                detail=(
                    f"column {column!r} is referenced outside an aggregate function and is "
                    f"not in the group set {sorted(group_set)!r}"
                ),
            )


def parse_and_validate_expression(
    text: str,
    *,
    schema: Iterable[str],
    product: str,
    occurrence: str,
    dialect: str = REFERENCE_DIALECT,
    group_columns: Iterable[str] | None = None,
    parser: ExpressionParserPort | None = None,
) -> ParsedExpression:
    """The full inline-expression pipeline: parse, resolve every column
    against ``schema``, and -- only when ``group_columns`` is given, that
    is, only in an aggregate context -- apply the aggregation rule.
    Returns the ``ParsedExpression`` on success, so a caller can still read
    ``.columns`` for field lineage. Raises the first ``ExpressionError``
    any of the three steps finds."""

    parsed = parse_expression(text, product=product, occurrence=occurrence, dialect=dialect, parser=parser)
    resolve_expression_columns(parsed, schema, product=product, occurrence=occurrence)
    if group_columns is not None:
        check_aggregation_rule(parsed, group_columns, product=product, occurrence=occurrence)
    return parsed


def parse_select_body(
    text: str,
    *,
    input_schema: Iterable[str],
    product: str,
    occurrence: str,
    dialect: str = REFERENCE_DIALECT,
    parser: ExpressionParserPort | None = None,
) -> ParsedSelect:
    """Parse ``text`` as one whole SELECT statement, resolve every column it
    reads against ``input_schema`` (fail closed with
    ``ExpressionError(rule="unresolved_column")`` on the first one that is
    not there), and derive its output columns from ``input_schema``: a
    literal ``"*"`` projection expands to every column of ``input_schema``,
    in the schema's own order, at the position the star appeared; every
    other projection keeps its own name or ``AS`` alias unchanged. Raises
    ``ExpressionError(rule="syntax_error")`` on anything that does not
    parse as exactly one well-formed SELECT statement."""

    active_parser = parser if parser is not None else _DEFAULT_PARSER
    try:
        parsed = active_parser.parse_select(text, dialect=dialect)
    except ParserSyntaxError as exc:
        raise ExpressionError(
            product=product,
            occurrence=occurrence,
            rule="syntax_error",
            detail=exc.detail,
            position=exc.position,
        ) from exc

    schema_tuple = tuple(input_schema)
    schema_set = frozenset(schema_tuple)
    for column in parsed.columns_read:
        if column not in schema_set:
            raise ExpressionError(
                product=product,
                occurrence=occurrence,
                rule="unresolved_column",
                detail=f"column {column!r} is not in the input relation schema {sorted(schema_set)!r}",
            )

    expanded: list[str] = []
    for name in parsed.output_columns:
        if name == "*":
            expanded.extend(schema_tuple)
        else:
            expanded.append(name)

    return ParsedSelect(
        text=parsed.text,
        output_columns=tuple(expanded),
        columns_read=parsed.columns_read,
        has_from=parsed.has_from,
    )
