"""Python-level unit tests for ergasterion/emit.py's validate_er_branch_coverage() gate.

Checks emit-time entity-resolution branch coverage. A source that
vaults an entity whose bridge inner-joins res_<X> (with `where golden_<X>_key is
not null`) but omits the top-level entity_resolution.<X> branch emits clean SQL,
builds green, and silently drops every record of that source downstream (no branch
=> no rows in res_<X> => null golden key => inner join drops all). The gate must
fail loudly at emit time instead.

Same plain assert/report convention as tests/python/test_emit.py (no pytest in this
repo's .venv): each test_* raises AssertionError on failure, main() runs them all
and reports PASS/FAIL, exit code 0 = all green, 1 = any failure.

Usage:
    python tests/python/test_er_coverage_gate.py
"""

from __future__ import annotations

import io
import sys
import traceback
from contextlib import redirect_stderr

# Allow direct execution as `python tests/python/test_er_coverage_gate.py`.
if __package__ in (None, ""):
    import os as _os, sys as _sys
    _sys.path.insert(0, _os.path.dirname(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))))

from ergasterion import emit


def _bridge(model: str, join_type: str = "inner") -> dict:
    return {
        "resolutions": [
            {
                "alias": f"{model}_res",
                "model": model,
                "join_type": join_type,
                "conditions": [f"{model}_res.source_system = source.source_system"],
            }
        ]
    }


def _declaration(name: str, entity_resolution: dict, vault_entities: list[dict]) -> dict:
    return {
        "source": {"name": name},
        "entity_resolution": entity_resolution,
        "tables": {"t": {"vault_entities": vault_entities}},
    }


def test_missing_branch_raises_named_value_error() -> None:
    """A bridge resolving against res_fund with no entity_resolution.fund branch must
    fail, naming the source and the missing resolution target."""
    decl = _declaration(
        "bad_src",
        entity_resolution={},  # fund branch deliberately omitted
        vault_entities=[{"entity": "fund", "name": "funds", "bridge": _bridge("res_fund")}],
    )
    try:
        emit.validate_er_branch_coverage([decl])
    except ValueError as exc:
        message = str(exc)
        assert "bad_src" in message, f"expected source named, got: {message}"
        assert "entity_resolution.fund" in message, f"expected target named, got: {message}"
        assert "funds" in message, f"expected offending vault entity named, got: {message}"
    else:
        raise AssertionError("expected ValueError for missing entity_resolution.fund, none raised")


def test_rider_entity_satisfied_by_parent_branch() -> None:
    """False-positive guard: a fund_cash_flow-style entity inner-joins res_fund but has
    no entity_resolution key of its own -- it rides the parent fund branch. Declaring
    entity_resolution.fund must satisfy it (gate keys off res MODEL, not entity name)."""
    decl = _declaration(
        "rider_src",
        entity_resolution={"fund": {"model": "stg_rider_funds", "columns": {}}},
        vault_entities=[
            {"entity": "fund", "name": "funds", "bridge": _bridge("res_fund")},
            # fund_cash_flow + fund_valuation both inner-join res_fund, no ER key of
            # their own -- must NOT trigger a missing entity_resolution.fund_cash_flow
            # or .fund_valuation error.
            {"entity": "fund_cash_flow", "name": "cash_flows", "bridge": _bridge("res_fund")},
            {"entity": "fund_valuation", "name": "valuations", "bridge": _bridge("res_fund")},
        ],
    )
    # No raise expected.
    emit.validate_er_branch_coverage([decl])


def test_orphan_branch_warns_not_errors() -> None:
    """An entity_resolution.<X> branch no bridge resolves against is a soft warning on
    stderr, not a fatal error. A branch that IS consumed must not warn."""
    decl = _declaration(
        "orphan_src",
        entity_resolution={
            "fund": {"model": "stg_orphan_funds", "columns": {}},  # consumed -> no warn
            "gp": {"model": "stg_orphan_gps", "columns": {}},  # orphan -> warn
        },
        vault_entities=[{"entity": "fund", "name": "funds", "bridge": _bridge("res_fund")}],
    )
    captured = io.StringIO()
    with redirect_stderr(captured):
        emit.validate_er_branch_coverage([decl])  # must not raise
    warnings = captured.getvalue()
    assert "WARN orphan_src" in warnings, f"expected an orphan warning, got: {warnings!r}"
    assert "entity_resolution.gp" in warnings, f"expected gp named as orphan, got: {warnings!r}"
    assert "res_gp" in warnings, f"expected res_gp named as orphan, got: {warnings!r}"
    assert "entity_resolution.fund" not in warnings, (
        f"consumed fund branch must not warn, got: {warnings!r}"
    )


def test_committed_declarations_pass_the_gate() -> None:
    """Regression guard (acceptance criterion 2): every currently-committed declaration
    already feeds every res model its bridges consume, so the live tree must clear the
    new gate with no error and no orphan warning."""
    declarations = emit.load_declarations()
    captured = io.StringIO()
    with redirect_stderr(captured):
        emit.validate_er_branch_coverage(declarations)
    assert "WARN" not in captured.getvalue(), (
        f"live declarations should raise no orphan warning, got: {captured.getvalue()!r}"
    )


TESTS = [
    test_missing_branch_raises_named_value_error,
    test_rider_entity_satisfied_by_parent_branch,
    test_orphan_branch_warns_not_errors,
    test_committed_declarations_pass_the_gate,
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
