"""Python-level unit tests for ergasterion/emit.py's Jinja environment strictness.

Checks that the emitter's Jinja environment uses
default Undefined, so res_entity.sql.j2's `branch.columns[column.name]` dereference
on a branch missing an expected column rendered as an empty expression --
`  as source_system,` -- instead of raising. template_env() now sets StrictUndefined,
so the same missing-column branch must fail loudly at render time instead.

The flip side of the same gap: a template can LEGITIMATELY rely on a key being
absent (optional fields). Those spots must guard explicitly (default() filter, or
an if-guard on a setdefault'd key) rather than ride the old undefined-renders-empty
loophole -- covered here for res_entity.sql.j2's `res.transitive_merge` (only the
fund res config sets it; gp and portfolio_company omit the key entirely) and
bridge.sql.j2's `vault.bridge.where` (schema-optional, per-declaration).

No pytest in this repo's .venv, so this follows the plain assert/report convention
already used by tests/python/test_emit.py and tests/python/test_er_coverage_gate.py: each
test_* raises AssertionError (or lets jinja2's own exception propagate) on failure,
main() runs them all and reports PASS/FAIL, exit code 0 = all green, 1 = any failure.

Usage:
    python tests/python/test_strict_undefined.py
"""

from __future__ import annotations

import sys
import traceback

import jinja2

# Allow direct execution as `python tests/python/test_strict_undefined.py`.
if __package__ in (None, ""):
    import os as _os, sys as _sys
    _sys.path.insert(0, _os.path.dirname(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))))

from ergasterion import emit


# A trimmed res config with the same shape as a domain's res_configs entry -- just
# enough columns to exercise the candidates-select loop that dereferences
# branch.columns[column.name]. No tier0 key: the optional config-driven tier-0 block
# is skipped (guarded by `res.tier0 | default(none)` in res_entity.sql.j2).
_MINIMAL_RES = {
    "path": "models/entity_resolution/res_test.sql",
    "stable_key_entity": "test",
    "golden_key_column": "golden_test_key",
    "columns": ["source_system", "source_id"],
    "match_keys": [
        {"column": "source_id", "expression": "source_id", "type": "source_id"},
    ],
    "output_columns": ["source_system", "source_id"],
    "trailing_columns": [],
}


def _res_context(**overrides) -> dict:
    context = dict(_MINIMAL_RES)
    context.update(overrides)
    context["columns"] = emit.as_template_columns(context["columns"])
    context["output_columns"] = emit.as_template_columns(context["output_columns"])
    return context


def test_missing_branch_column_raises_undefined_error() -> None:
    """A branch dict missing an expected res column must fail loudly at render time
    (jinja2.exceptions.UndefinedError), not emit an empty expression -- this is the
    silent mis-emit this regression test covers."""
    env = emit.template_env()
    branch = {
        "model": "stg_bad_source",
        # source_id deliberately omitted to make the fixture invalid.
        "columns": {"source_system": "source.source_system"},
    }
    try:
        emit.render(
            env,
            "res_entity.sql.j2",
            generated_header="-- test",
            branches=[branch],
            res=_res_context(),
        )
    except jinja2.exceptions.UndefinedError as exc:
        assert "source_id" in str(exc), f"expected the missing column named, got: {exc}"
    else:
        raise AssertionError(
            "expected jinja2.exceptions.UndefinedError for a branch missing an "
            "expected res column, none raised"
        )


def test_complete_branch_columns_render_clean() -> None:
    """A branch that supplies every expected res column must still render fine --
    StrictUndefined must not break the well-formed case."""
    env = emit.template_env()
    branch = {
        "model": "stg_good_source",
        "columns": {
            "source_system": "source.source_system",
            "source_id": "source.source_id",
        },
    }
    content = emit.render(
        env,
        "res_entity.sql.j2",
        generated_header="-- test",
        branches=[branch],
        res=_res_context(),
    )
    assert "source.source_system as source_system" in content
    assert "source.source_id as source_id" in content


def test_res_config_without_transitive_merge_key_still_renders() -> None:
    """gp and portfolio_company omit `transitive_merge` from RES_CONFIGS entirely
    (only fund sets it) -- res_entity.sql.j2's `{% if res.transitive_merge |
    default(false) %}` guard must render the non-transitive branch cleanly instead
    of raising on the missing key."""
    env = emit.template_env()
    res_context = _res_context()
    assert "transitive_merge" not in res_context, "test setup must actually omit the key"
    branch = {
        "model": "stg_source",
        "columns": {"source_system": "source.source_system", "source_id": "source.source_id"},
    }
    content = emit.render(
        env,
        "res_entity.sql.j2",
        generated_header="-- test",
        branches=[branch],
        res=res_context,
    )
    assert "TRANSITIVE ENTITY RESOLUTION" not in content


def test_bridge_without_where_key_still_renders() -> None:
    """A bridge declaration that omits the optional `where` key entirely must
    still render (bridge.sql.j2's `{% if vault.bridge.where | default([]) %}`
    guard), not raise on the missing key."""
    env = emit.template_env()
    vault = {
        "source_model": "stg_source",
        "bridge": {
            "resolutions": [],
            "select": [{"name": "source_system", "expression": "source.source_system"}],
            # "where" deliberately omitted -- schema-optional.
        },
    }
    content = emit.render(env, "bridge.sql.j2", generated_header="-- test", vault=vault)
    assert "where" not in content.lower()


def test_committed_res_and_vault_context_render_clean() -> None:
    """Regression guard: the live declarations tree's res/vault contexts render
    clean under StrictUndefined (acceptance criterion 1's real-tree emit already
    proves this end to end; this pins the res_entity.sql.j2 / bridge.sql.j2
    render step in isolation)."""
    domain = emit.load_domains()
    declarations = emit.load_declarations(domain)
    env = emit.template_env()
    for entity_name, res in domain["res_configs"].items():
        branches = [
            declaration["entity_resolution"][entity_name]
            for declaration in declarations
            if declaration.get("entity_resolution", {}).get(entity_name)
        ]
        res_context = dict(res)
        res_context["columns"] = emit.as_template_columns(res["columns"])
        res_context["output_columns"] = emit.as_template_columns(res["output_columns"])
        emit.render(
            env,
            "res_entity.sql.j2",
            generated_header="-- test",
            branches=branches,
            res=res_context,
        )
    for vault in emit.select_vaults(declarations):
        emit.render(env, "bridge.sql.j2", generated_header="-- test", vault=vault)


TESTS = [
    test_missing_branch_column_raises_undefined_error,
    test_complete_branch_columns_render_clean,
    test_res_config_without_transitive_merge_key_still_renders,
    test_bridge_without_where_key_still_renders,
    test_committed_res_and_vault_context_render_clean,
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
