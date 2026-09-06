"""Render one shipped macro's SQL outside dbt, for the golden-hash parity
lane.

``dpf_vault_hash`` (macros/data_vault.sql) is the construction every hub,
link and satellite key in a vault is computed by. Nothing about it is
derivable from a declaration: a changed sentinel, separator, case rule or
dispatch arm produces different keys for the same rows, and because every
test estate is built from scratch every suite would stay green while a live
estate silently orphaned every row it had already stored.

This module renders that macro the way the running adapter renders it, so
the parity lane executes the shipped text rather than a restatement of it:

  * the macro bodies come from ``macros/data_vault.sql`` and
    ``macros/cross_db.sql`` as they ship;
  * ``adapter.dispatch`` resolves to the named adapter's own arm in those
    files, exactly as dbt's dispatch does, so the duckdb hash arm is the
    one executed;
  * ``dbt.type_*`` resolves through the adapter's declared physical type
    mapping (``ergasterion.framework.adapters``), which is the same table
    the per-adapter parse gate resolves a cast target through;
  * ``return`` prints its argument, which is what dbt's ``return`` hands
    the caller for a macro whose whole body is one ``return``.

Anything a macro needs that is not one of those four fails loudly here
rather than being stubbed: a stub would be a second implementation, and a
second implementation is exactly what this lane exists to catch.
"""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import jinja2

from ergasterion.framework.adapters import load_adapter_conventions

REPO_ROOT = Path(__file__).resolve().parents[2]
MACRO_FILES = ("data_vault.sql", "cross_db.sql")

# The dpf_type() tokens the cross-database macros resolve through dbt's own
# type helpers. Mapped here to the adapter's declared physical type so the
# rendered cast target is the adapter's, never a literal typed out twice.
TYPE_HELPERS = {
    "type_int": "int",
    "type_float": "float",
    "type_numeric": "numeric",
    "type_string": "string",
    "type_timestamp": "timestamp",
}


def macro_module(adapter: str) -> jinja2.environment.TemplateModule:
    """Every macro in the shipped files, rendered for one adapter."""

    source = "\n".join(
        (REPO_ROOT / "macros" / name).read_text(encoding="utf-8") for name in MACRO_FILES
    )
    environment = jinja2.Environment(
        extensions=["jinja2.ext.do"], undefined=jinja2.StrictUndefined
    )
    template = environment.from_string(source)
    mapping = load_adapter_conventions(adapter).type_mapping
    # The dispatch resolves against the module it is part of, which does not
    # exist until the module is built, so it reads the module back out of
    # this holder rather than closing over a half-built one.
    holder: dict = {}

    def dispatch(name: str, package: str):
        module = holder["module"]
        arm = getattr(module, f"{adapter}__{name}", None)
        return arm if arm is not None else getattr(module, f"default__{name}")

    context: dict = {
        "return": lambda value: value,
        "adapter": SimpleNamespace(dispatch=dispatch),
        "dbt": SimpleNamespace(
            **{
                helper: (lambda token=token: mapping[token])
                for helper, token in TYPE_HELPERS.items()
            }
        ),
    }
    holder["module"] = template.make_module(context)
    return holder["module"]


def render_vault_hash(columns, *, adapter: str) -> str:
    """The SQL ``dpf_vault_hash`` renders over ``columns`` on ``adapter``."""

    return str(macro_module(adapter).dpf_vault_hash(list(columns))).strip()


__all__ = ["MACRO_FILES", "REPO_ROOT", "macro_module", "render_vault_hash"]
