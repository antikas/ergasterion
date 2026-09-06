"""Assert-script tests for the inline SQL expression parser and its port
(architecture sections 3.2, 3.4, 9).

Covers: the parser port and the shipped sqlglot adapter
(``ergasterion.framework.expression_sqlglot.SqlglotExpressionParser``)
behind ``ergasterion.framework.expressions``; every worked-domain
expression the product fixtures actually carry parses in the reference
adapter's dialect (DuckDB) and renders verbatim; a syntax error fails
closed naming product, occurrence and position; an unresolved column and a
non-aggregated column outside the group set each fail closed; the columns
an expression reads are returned for field lineage; a whole select body
parses and its output columns are derived from the input relation schema,
including a ``*`` projection's expansion.

Usage:
    python tests/python/test_expressions.py
"""

from __future__ import annotations

import sys
import traceback
from pathlib import Path

if __package__ in (None, ""):
    import os as _os
    import sys as _sys

    _sys.path.insert(0, _os.path.dirname(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))))

import yaml

from ergasterion.framework import expressions as em
from ergasterion.framework.expression_port import ParsedExpression, ParsedSelect, ParserSyntaxError, SourcePosition
from ergasterion.framework.expression_sqlglot import SqlglotExpressionParser

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
FIXTURES_DIR = REPO_ROOT / "tests" / "fixtures" / "products" / "valid"


# --------------------------------------------------------------------------- worked-domain expressions


def _worked_domain_expressions() -> list[tuple[str, str]]:
    """Every literal ``expression:`` string the valid product fixtures
    carry, as ``(product_name, expression_text)``: the Stop condition's
    "worked-domain expression" set."""

    found: list[tuple[str, str]] = []
    for path in sorted(FIXTURES_DIR.glob("*.yml")):
        document = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        product_name = (document.get("product") or {}).get("name", path.stem)
        for step in document.get("steps") or []:
            for field in step.get("fields") or []:
                if "expression" in field:
                    found.append((product_name, field["expression"]))
            for predicate in step.get("predicates") or []:
                if "expression" in predicate:
                    found.append((product_name, predicate["expression"]))
            for aggregate in step.get("aggregates") or []:
                if "expression" in aggregate:
                    found.append((product_name, aggregate["expression"]))
    return found


def test_every_worked_domain_expression_parses_in_the_reference_dialect_and_renders_verbatim() -> None:
    expressions = _worked_domain_expressions()
    assert expressions, "expected at least one worked-domain expression fixture to exercise"
    for product_name, text in expressions:
        parsed = em.parse_expression(text, product=product_name, occurrence="worked-domain-proof")
        assert parsed.text == text, (product_name, text, parsed.text)


# --------------------------------------------------------------------------- parse_expression: syntax, columns, aggregation


def test_parse_expression_returns_columns_for_field_lineage() -> None:
    parsed = em.parse_expression(
        "status_code IN ('A', 'P') AND end_date IS NULL",
        product="customer",
        occurrence="steps[3]:calculated_fields.fields[0]",
    )
    assert parsed.columns == ("status_code", "end_date"), parsed.columns
    assert parsed.is_aggregate is False
    assert parsed.text == "status_code IN ('A', 'P') AND end_date IS NULL"


def test_parse_expression_detects_an_aggregate() -> None:
    parsed = em.parse_expression("SUM(amount) / COUNT(*)", product="p", occurrence="o")
    assert parsed.is_aggregate is True
    assert parsed.columns == ("amount",)
    assert parsed.non_aggregated_columns == (), parsed.non_aggregated_columns


def test_parse_expression_a_window_function_is_not_an_aggregate() -> None:
    parsed = em.parse_expression("SUM(amount) OVER (PARTITION BY region)", product="p", occurrence="o")
    assert parsed.is_aggregate is False, "a window function must not be treated as an aggregate"


def test_parse_expression_renders_verbatim_never_a_reparsed_form() -> None:
    # sqlglot's own .sql() would lower-case/re-space this; the port must
    # hand back the caller's exact original text unchanged.
    text = "Status_Code   IN ('A','P')"
    parsed = em.parse_expression(text, product="p", occurrence="o")
    assert parsed.text == text


def test_a_syntax_error_fails_closed_naming_product_occurrence_and_position() -> None:
    try:
        em.parse_expression("status_code IN (", product="customer", occurrence="steps[3]:calculated_fields.fields[0]")
    except em.ExpressionError as exc:
        assert exc.rule == "syntax_error", exc.rule
        assert exc.product == "customer", exc.product
        assert exc.occurrence == "steps[3]:calculated_fields.fields[0]", exc.occurrence
        assert exc.position is not None, "expected a source position for this syntax error"
        assert exc.position.line == 1
        assert "position" in str(exc.position) or True  # __str__ smoke check below
        assert str(exc.position) == f"line {exc.position.line}, column {exc.position.column}"
    else:
        raise AssertionError("expected ExpressionError(rule='syntax_error')")


def test_an_empty_expression_fails_closed_as_a_syntax_error() -> None:
    for text in ("", "   ", ";"):
        try:
            em.parse_expression(text, product="p", occurrence="o")
        except em.ExpressionError as exc:
            assert exc.rule == "syntax_error", (text, exc.rule)
        else:
            raise AssertionError(f"expected ExpressionError for blank expression {text!r}")


def test_multiple_statements_smuggled_into_one_expression_fail_closed() -> None:
    try:
        em.parse_expression("amount + 1; drop table x", product="p", occurrence="o")
    except em.ExpressionError as exc:
        assert exc.rule == "syntax_error", exc.rule
    else:
        raise AssertionError("expected ExpressionError for a smuggled second statement")


def test_a_whole_select_given_to_parse_expression_fails_closed() -> None:
    try:
        em.parse_expression("SELECT a FROM t", product="p", occurrence="o")
    except em.ExpressionError as exc:
        assert exc.rule == "syntax_error", exc.rule
    else:
        raise AssertionError("expected ExpressionError: parse_expression is not parse_select")


def test_non_scalar_statements_given_to_parse_expression_all_fail_closed() -> None:
    # An inline expression must be one scalar or aggregate SQL expression
    # (a sqlglot exp.Condition), never a whole statement. parse_expression
    # must reject every one of these the same way it rejects a bare SELECT,
    # not just render them verbatim into generated artefacts.
    statements = [
        "DROP TABLE customers",
        "DELETE FROM t WHERE a = 1",
        "INSERT INTO t VALUES (1)",
        "UPDATE t SET a = 1",
        "CREATE TABLE z (a INT)",
        "VACUUM",
        "PRAGMA foo",
        "install httpfs",
        "SET x = 1",
        "CALL foo()",
    ]
    for text in statements:
        try:
            em.parse_expression(text, product="p", occurrence="o")
        except em.ExpressionError as exc:
            assert exc.rule == "syntax_error", (text, exc.rule)
        else:
            raise AssertionError(f"expected ExpressionError(rule='syntax_error') for statement {text!r}")


def test_parse_select_body_still_accepts_a_select_and_rejects_the_same_non_select_statements() -> None:
    # The select-body parse path is the one place a whole SELECT is
    # expected: it must keep accepting exp.Select while still rejecting
    # every non-select statement parse_expression rejects above.
    parsed = em.parse_select_body(
        "SELECT customer_id FROM customer",
        input_schema=["customer_id"],
        product="customer_view",
        occurrence="declared-select",
    )
    assert parsed.output_columns == ("customer_id",)

    statements = [
        "DROP TABLE customers",
        "DELETE FROM t WHERE a = 1",
        "INSERT INTO t VALUES (1)",
        "UPDATE t SET a = 1",
        "CREATE TABLE z (a INT)",
        "VACUUM",
        "PRAGMA foo",
        "install httpfs",
        "SET x = 1",
        "CALL foo()",
    ]
    for text in statements:
        try:
            em.parse_select_body(text, input_schema=["customer_id"], product="p", occurrence="o")
        except em.ExpressionError as exc:
            assert exc.rule == "syntax_error", (text, exc.rule)
        else:
            raise AssertionError(f"expected ExpressionError(rule='syntax_error') for statement {text!r}")


# --------------------------------------------------------------------------- column resolution


def test_an_unresolved_column_fails_closed() -> None:
    try:
        em.parse_and_validate_expression(
            "unknown_column = 1",
            schema=["customer_id", "status_code"],
            product="customer",
            occurrence="steps[3]:calculated_fields.fields[0]",
        )
    except em.ExpressionError as exc:
        assert exc.rule == "unresolved_column", exc.rule
        assert "unknown_column" in exc.detail
    else:
        raise AssertionError("expected ExpressionError(rule='unresolved_column')")


def test_every_column_resolving_against_the_schema_passes() -> None:
    parsed = em.parse_and_validate_expression(
        "status_code IN ('A', 'P')",
        schema=["customer_id", "status_code", "email"],
        product="customer",
        occurrence="o",
    )
    assert parsed.columns == ("status_code",)


# --------------------------------------------------------------------------- the aggregation rule


def test_a_non_aggregated_column_outside_the_group_set_fails_closed() -> None:
    try:
        em.parse_and_validate_expression(
            "SUM(amount) + fee",
            schema=["region", "amount", "fee"],
            product="fact_orders",
            occurrence="steps[2]:data_aggregation.aggregates[0]",
            group_columns=["region"],
        )
    except em.ExpressionError as exc:
        assert exc.rule == "non_aggregated_column", exc.rule
        assert "fee" in exc.detail
    else:
        raise AssertionError("expected ExpressionError(rule='non_aggregated_column')")


def test_a_non_aggregated_column_inside_the_group_set_passes() -> None:
    parsed = em.parse_and_validate_expression(
        "SUM(amount) + region_weight",
        schema=["region", "amount", "region_weight"],
        product="fact_orders",
        occurrence="o",
        group_columns=["region", "region_weight"],
    )
    assert parsed.is_aggregate is True


def test_columns_inside_an_aggregate_function_need_not_be_in_the_group_set() -> None:
    # `amount` is only ever referenced inside SUM(...); the aggregation rule
    # must not demand it also sit in the group set.
    parsed = em.parse_and_validate_expression(
        "SUM(amount)",
        schema=["region", "amount"],
        product="fact_orders",
        occurrence="o",
        group_columns=["region"],
    )
    assert parsed.non_aggregated_columns == ()


def test_no_group_columns_given_skips_the_aggregation_rule_entirely() -> None:
    # calculated_fields / data_filtering are never an aggregate context: no
    # group_columns is passed, so a bare non-aggregate expression is fine
    # even though it "aggregates nothing" and has no group set at all.
    parsed = em.parse_and_validate_expression(
        "is_active AND NOT is_deleted",
        schema=["is_active", "is_deleted"],
        product="customer",
        occurrence="o",
    )
    assert parsed.is_aggregate is False


# --------------------------------------------------------------------------- parse_select_body


def test_parse_select_body_derives_output_columns_from_the_input_schema() -> None:
    parsed = em.parse_select_body(
        "SELECT customer_id, upper(email) AS email_upper FROM customer WHERE status_code = 'A'",
        input_schema=["customer_id", "email", "status_code"],
        product="customer_view",
        occurrence="declared-select",
    )
    assert parsed.output_columns == ("customer_id", "email_upper"), parsed.output_columns
    assert set(parsed.columns_read) == {"customer_id", "email", "status_code"}, parsed.columns_read
    assert parsed.text == "SELECT customer_id, upper(email) AS email_upper FROM customer WHERE status_code = 'A'"


def test_parse_select_body_expands_a_star_projection_from_the_input_schema() -> None:
    parsed = em.parse_select_body(
        "SELECT * FROM customer",
        input_schema=["customer_id", "email", "status_code"],
        product="customer_view",
        occurrence="declared-select",
    )
    assert parsed.output_columns == ("customer_id", "email", "status_code"), parsed.output_columns


def test_parse_select_body_unresolved_column_fails_closed() -> None:
    try:
        em.parse_select_body(
            "SELECT customer_id, missing_column FROM customer",
            input_schema=["customer_id", "email"],
            product="customer_view",
            occurrence="declared-select",
        )
    except em.ExpressionError as exc:
        assert exc.rule == "unresolved_column", exc.rule
        assert "missing_column" in exc.detail
    else:
        raise AssertionError("expected ExpressionError(rule='unresolved_column')")


def test_parse_select_body_syntax_error_fails_closed() -> None:
    try:
        em.parse_select_body(
            "SELECT customer_id FROM",
            input_schema=["customer_id"],
            product="customer_view",
            occurrence="declared-select",
        )
    except em.ExpressionError as exc:
        assert exc.rule == "syntax_error", exc.rule
    else:
        raise AssertionError("expected ExpressionError(rule='syntax_error')")


def test_parse_select_body_rejects_a_bare_expression_not_a_select() -> None:
    try:
        em.parse_select_body("customer_id + 1", input_schema=["customer_id"], product="p", occurrence="o")
    except em.ExpressionError as exc:
        assert exc.rule == "syntax_error", exc.rule
    else:
        raise AssertionError("expected ExpressionError: parse_select_body needs a whole SELECT")


# --------------------------------------------------------------------------- the port contract directly


def test_the_shipped_adapter_satisfies_the_port_protocol_directly() -> None:
    parser = SqlglotExpressionParser()
    parsed = parser.parse_expression("a + b", dialect=em.REFERENCE_DIALECT)
    assert isinstance(parsed, ParsedExpression)
    assert parsed.columns == ("a", "b")

    selected = parser.parse_select("SELECT a, b FROM t", dialect=em.REFERENCE_DIALECT)
    assert isinstance(selected, ParsedSelect)
    assert selected.output_columns == ("a", "b")


def test_a_caller_may_inject_a_different_parser_implementation() -> None:
    class _StubParser:
        def parse_expression(self, text: str, *, dialect: str) -> ParsedExpression:
            return ParsedExpression(text=text, columns=("stub_column",), non_aggregated_columns=(), is_aggregate=False)

        def parse_select(self, text: str, *, dialect: str) -> ParsedSelect:
            raise NotImplementedError

    parsed = em.parse_expression("anything at all", product="p", occurrence="o", parser=_StubParser())
    assert parsed.columns == ("stub_column",)


def test_an_unrecognised_dialect_fails_closed_as_a_syntax_error_not_a_silent_fallback() -> None:
    try:
        em.parse_expression("a + 1", product="p", occurrence="o", dialect="not_a_real_dialect")
    except em.ExpressionError as exc:
        assert exc.rule == "syntax_error", exc.rule
    else:
        raise AssertionError("expected ExpressionError: an unknown dialect must never silently fall back")


TESTS = [
    test_every_worked_domain_expression_parses_in_the_reference_dialect_and_renders_verbatim,
    test_parse_expression_returns_columns_for_field_lineage,
    test_parse_expression_detects_an_aggregate,
    test_parse_expression_a_window_function_is_not_an_aggregate,
    test_parse_expression_renders_verbatim_never_a_reparsed_form,
    test_a_syntax_error_fails_closed_naming_product_occurrence_and_position,
    test_an_empty_expression_fails_closed_as_a_syntax_error,
    test_multiple_statements_smuggled_into_one_expression_fail_closed,
    test_a_whole_select_given_to_parse_expression_fails_closed,
    test_non_scalar_statements_given_to_parse_expression_all_fail_closed,
    test_parse_select_body_still_accepts_a_select_and_rejects_the_same_non_select_statements,
    test_an_unresolved_column_fails_closed,
    test_every_column_resolving_against_the_schema_passes,
    test_a_non_aggregated_column_outside_the_group_set_fails_closed,
    test_a_non_aggregated_column_inside_the_group_set_passes,
    test_columns_inside_an_aggregate_function_need_not_be_in_the_group_set,
    test_no_group_columns_given_skips_the_aggregation_rule_entirely,
    test_parse_select_body_derives_output_columns_from_the_input_schema,
    test_parse_select_body_expands_a_star_projection_from_the_input_schema,
    test_parse_select_body_unresolved_column_fails_closed,
    test_parse_select_body_syntax_error_fails_closed,
    test_parse_select_body_rejects_a_bare_expression_not_a_select,
    test_the_shipped_adapter_satisfies_the_port_protocol_directly,
    test_a_caller_may_inject_a_different_parser_implementation,
    test_an_unrecognised_dialect_fails_closed_as_a_syntax_error_not_a_silent_fallback,
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
