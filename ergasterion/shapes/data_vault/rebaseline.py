"""``ergasterion vault-rebaseline``: the declared operation that moves one
satellite's hashdiff basis.

A satellite's hashdiff basis is frozen once versions are stored under it
(``ergasterion.shapes.data_vault.evolution``). Changing it is therefore an
operation with three declared steps rather than an edit that takes effect
silently:

    1. ``--stage`` records the currently declared basis as the satellite's
       pending one. Emission then stops on the pending-basis gate, so no
       build can land versions while the estate carries two answers to
       what change detection means;
    2. ``--promote`` adopts the pending basis. The recorded basis becomes
       the declared one and the basis version advances;
    3. the next ``ergasterion emit-products`` and build store exactly one
       version per entity under the new basis version, and leave every
       version stored under the old one exactly as it was.

``--abort`` abandons a staged re-baseline. The recorded basis is
unchanged, so the declared change that prompted it fails closed again on
the next emission and the estate converges back to where it was.

Nothing here rewrites a stored fingerprint. A fingerprint is a fact about
the basis it was computed under, every stored row carries that basis
version beside it, and a satellite's change detection is scoped to its own
basis version -- so history stays readable as what it was, rather than
being restated as what a later basis would have made of it.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any, Callable

import yaml

from ergasterion.estate import EstateContext
from ergasterion.framework import contract as contract_mod
from ergasterion.framework.models import FrameworkError
from ergasterion.shapes.data_vault import SHAPE_NAME, payload_types
from ergasterion.shapes.data_vault import evolution as evolution_mod

PRODUCTS_SUBDIRECTORY = "products"

STAGE = "stage"
PROMOTE = "promote"
ABORT = "abort"

OPERATIONS: dict[str, Callable[..., dict]] = {
    STAGE: evolution_mod.stage,
    PROMOTE: evolution_mod.promote,
    ABORT: evolution_mod.abort,
}


class RebaselineError(FrameworkError):
    """One failure of this operation that belongs to the operation itself
    rather than to the grading it drives."""

    code = "vault_rebaseline_error"


def _declaration(ctx: EstateContext, product: str) -> dict:
    """The product declaration this operation acts on, found by the
    published name it carries. Read from the declaration tree rather than
    from a resolved plan: staging a basis is a fact about the words the
    declaration uses, and the declared change that prompted the
    re-baseline is exactly what stops the route that resolves plans."""

    directory = ctx.declarations_dir / PRODUCTS_SUBDIRECTORY
    found: list[str] = []
    for path in sorted(directory.glob("*.yml")):
        document = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        block = document.get("product") or {}
        published = f"{block.get('domain')}.{block.get('name')}"
        found.append(published)
        if published != product:
            continue
        shape = (document.get("target") or {}).get("shape")
        if shape != SHAPE_NAME:
            raise RebaselineError(
                f"product {product!r} declares shape {shape!r}; a hashdiff basis belongs to a "
                f"{SHAPE_NAME!r} product's satellite"
            )
        return document
    raise RebaselineError(
        f"no product declaration under {directory} publishes {product!r}: {sorted(found)}"
    )


def _relations(ctx: EstateContext, product: str):
    """The relations this product's shape renders, resolved from the
    estate's declarations alone. Read through the route's own registry
    resolution rather than the planning pass, because the planning pass is
    where the declared change being re-baselined fails closed.

    Imported where it is used: this module is loaded by the command table,
    and the emission route loads the shape registry this module is part
    of."""

    from ergasterion import emit_products

    _entries, _schemas, resolved = emit_products.resolve_estate_relations(ctx)
    return resolved.get(product, ())


def run(
    *, ctx: EstateContext, product: str, satellite: str, operation: str
) -> tuple[Path, dict, str]:
    """Apply one operation to one satellite's record. Returns the ledger
    path, the document written and the line describing what changed."""

    document = _declaration(ctx, product)
    block = document.get("product") or {}
    relative = evolution_mod.ledger_relative_path(
        domain=str(block.get("domain")), name=str(block.get("name"))
    )
    path = ctx.root / relative
    if not path.is_file():
        raise RebaselineError(
            f"the estate carries no evolution ledger at {relative}; emit the product once so its "
            "hashdiff basis is recorded before re-baselining it"
        )
    recorded = yaml.safe_load(path.read_text(encoding="utf-8")) or {}

    keywords: dict[str, Any] = {"document": recorded, "satellite": satellite, "product": product}
    if operation == STAGE:
        shape_config = (document.get("target") or {}).get("shape_config") or {}
        declared = evolution_mod.declared_records(
            shape_config.get("satellites") or [],
            types=payload_types(_relations(ctx, product), shape_config=shape_config),
        )
        entry = declared.get(satellite)
        if entry is None:
            raise RebaselineError(
                f"product {product!r} declares no satellite {satellite!r}: {sorted(declared)}"
            )
        keywords["declared"] = entry
    written = OPERATIONS[operation](**keywords)
    path.write_text(contract_mod.dump_yaml(dict(written)), encoding="utf-8")

    entry = (written.get("satellites") or {}).get(satellite) or {}
    if operation == STAGE:
        basis = (entry.get("pending") or {}).get("basis") or []
        line = (
            f"staged: {product} satellite {satellite!r} pending basis over "
            f"{', '.join(basis)}; emission is gated until it is promoted or aborted"
        )
    elif operation == PROMOTE:
        line = (
            f"promoted: {product} satellite {satellite!r} basis version "
            f"{entry.get('basis_version')} over {', '.join(entry.get('basis') or [])}"
        )
    else:
        line = (
            f"aborted: {product} satellite {satellite!r} keeps basis version "
            f"{entry.get('basis_version')} over {', '.join(entry.get('basis') or [])}"
        )
    return path, written, line


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Stage, promote or abandon a re-baseline of one data_vault satellite's hashdiff "
            "basis."
        )
    )
    parser.add_argument("--product", required=True, help="The published product name.")
    parser.add_argument("--satellite", required=True, help="The declared satellite name.")
    parser.add_argument(
        "--estate-root",
        type=Path,
        default=None,
        help="Root of the estate to act on (resolved from the environment or cwd when omitted).",
    )
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument(
        "--stage",
        action="store_true",
        help="Record the declared basis as this satellite's pending one.",
    )
    mode.add_argument(
        "--promote", action="store_true", help="Adopt the pending basis and advance its version."
    )
    mode.add_argument("--abort", action="store_true", help="Abandon the staged re-baseline.")
    args = parser.parse_args(argv)
    operation = STAGE if args.stage else PROMOTE if args.promote else ABORT

    ctx = EstateContext.resolve(estate_root=args.estate_root)
    try:
        path, _document, line = run(
            ctx=ctx, product=args.product, satellite=args.satellite, operation=operation
        )
    except (FrameworkError, ValueError) as error:
        sys.stderr.write(f"FAIL: {error}\n")
        return 1
    print(line)
    print(path.relative_to(ctx.root).as_posix())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
