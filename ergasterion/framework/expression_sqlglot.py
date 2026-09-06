"""The shipped parser adapter: sqlglot behind the parser port.

Architecture section 3.2, owner ruling R3 (plan decision D22): sqlglot is
the engine's parser, used for parse, column resolution, and field lineage,
never for transpilation. This module is the one place ``import sqlglot``
appears in the framework; everything above this file
(``ergasterion/framework/expressions.py``) depends on
``ergasterion.framework.expression_port.ExpressionParserPort`` only, so a
future estate could ship a different adapter without changing anything
above this file.

sqlglot is pinned exactly (``sqlglot==30.18.0``) as a runtime dependency in
``pyproject.toml``, not an optional extra: parsing, column resolution and
lineage are core engine behaviour under the default ``sql`` estate mode,
not an add-on.
"""

from __future__ import annotations

import sqlglot
from sqlglot import exp
from sqlglot.errors import ParseError as SqlglotParseError

from ergasterion.framework.expression_port import (
    ParsedExpression,
    ParsedSelect,
    ParserSyntaxError,
    SourcePosition,
)


def _first_position(error: SqlglotParseError) -> SourcePosition | None:
    for entry in error.errors:
        line = entry.get("line")
        column = entry.get("col")
        if isinstance(line, int) and isinstance(column, int):
            return SourcePosition(line=line, column=column)
    return None


def _detail(error: SqlglotParseError) -> str:
    descriptions = [entry.get("description") for entry in error.errors if entry.get("description")]
    return "; ".join(descriptions) if descriptions else str(error)


def _parse_one_statement(text: str, *, dialect: str) -> exp.Expression:
    """Parse ``text`` as exactly one non-empty SQL statement/expression in
    ``dialect``. Raises ``ParserSyntaxError`` on invalid syntax, an empty
    or blank expression, or more than one statement (a lone trailing
    semicolon is not a second statement: sqlglot's own splitter already
    drops the resulting empty tail, so ``"amount + 1;"`` parses as one
    statement, while ``"amount + 1; drop table x"`` fails closed as two)."""

    try:
        statements = sqlglot.parse(text, read=dialect)
    except SqlglotParseError as exc:
        raise ParserSyntaxError(_detail(exc), position=_first_position(exc)) from exc
    except ValueError as exc:
        # An unrecognised dialect name is a caller/programming error, not a
        # text-shaped syntax failure. It still fails closed here rather
        # than silently falling back to a different dialect.
        raise ParserSyntaxError(str(exc), position=None) from exc

    non_empty = [statement for statement in statements if statement is not None]
    if not non_empty:
        raise ParserSyntaxError("the expression is empty or blank", position=None)
    if len(non_empty) > 1:
        raise ParserSyntaxError(
            f"expected exactly one SQL expression or statement, found {len(non_empty)}", position=None
        )
    return non_empty[0]


def _columns_of(node: exp.Expression) -> tuple[str, ...]:
    """Every distinct column name referenced anywhere in ``node``, in
    source order, table qualifier dropped (the flat visible-schema
    model)."""

    seen: list[str] = []
    known: set[str] = set()
    for column in node.find_all(exp.Column):
        name = column.name
        if name not in known:
            known.add(name)
            seen.append(name)
    return tuple(seen)


def _is_windowed(agg_func: exp.Expression) -> bool:
    """Whether ``agg_func`` (an ``AggFunc`` node) sits inside a ``Window``
    (an ``OVER (...)`` clause). A windowed aggregate does not collapse
    rows the way a GROUP BY aggregate does, so it counts as an ordinary,
    row-level construct for both ``_is_aggregate`` and
    ``_non_aggregated_columns_of``."""

    return agg_func.find_ancestor(exp.Window) is not None


def _non_aggregated_columns_of(node: exp.Expression) -> tuple[str, ...]:
    """The subset of ``_columns_of(node)`` for columns with no
    non-windowed ``AggFunc`` ancestor anywhere between the column and the
    root -- the columns an aggregate-context caller must find in its group
    set."""

    seen: list[str] = []
    known: set[str] = set()
    for column in node.find_all(exp.Column):
        agg_ancestor = column.find_ancestor(exp.AggFunc)
        if agg_ancestor is not None and not _is_windowed(agg_ancestor):
            continue
        name = column.name
        if name not in known:
            known.add(name)
            seen.append(name)
    return tuple(seen)


def _is_aggregate(node: exp.Expression) -> bool:
    return any(not _is_windowed(agg_func) for agg_func in node.find_all(exp.AggFunc))


class SqlglotExpressionParser:
    """The shipped ``ExpressionParserPort`` implementation."""

    def parse_expression(self, text: str, *, dialect: str) -> ParsedExpression:
        statement = _parse_one_statement(text, dialect=dialect)
        if not isinstance(statement, exp.Condition):
            raise ParserSyntaxError(
                "expected one scalar or aggregate SQL expression, found a "
                f"{type(statement).__name__} statement (not a scalar or boolean expression); "
                "use parse_select for a whole select body",
                position=None,
            )
        return ParsedExpression(
            text=text,
            columns=_columns_of(statement),
            non_aggregated_columns=_non_aggregated_columns_of(statement),
            is_aggregate=_is_aggregate(statement),
        )

    def parse_statement(self, text: str, *, dialect: str) -> None:
        _parse_one_statement(text, dialect=dialect)

    def parse_select(self, text: str, *, dialect: str) -> ParsedSelect:
        statement = _parse_one_statement(text, dialect=dialect)
        if not isinstance(statement, exp.Select):
            raise ParserSyntaxError("expected a whole SELECT statement", position=None)
        output_columns = tuple(projection.alias_or_name for projection in statement.expressions)
        return ParsedSelect(
            text=text,
            output_columns=output_columns,
            columns_read=_columns_of(statement),
            has_from=statement.find(exp.From) is not None,
        )
