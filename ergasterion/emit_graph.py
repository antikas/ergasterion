"""Deterministic estate product graph emitter.

`ergasterion product-graph` emits one estate product graph from the product
declarations under declarations/products/, into graphs/products/: node and edge
tables, product-level and field-level lineage, the declared validation
occurrences, and the description document. Nodes are products and edges are
contract dependencies; the classification comes from
ergasterion/framework/graph.py, which reads product declarations and nothing else.

This module serialises the already-built representation only.

Usage:
    python ergasterion/emit_graph.py            # write every product-graph artefact
    python ergasterion/emit_graph.py --check    # fail if any artefact drifted
"""

from __future__ import annotations

import argparse
from pathlib import Path

# Support installed-command and direct-script execution.
# Runs either installed (`ergasterion product-graph`) or script-mode. In script-mode
# the running module's own repo root is inserted at sys.path[0] ahead of site-packages
# so `import ergasterion.*` binds to THIS tree (see tests/python/test_package_mode.py).
if __package__ in (None, ""):
    import os as _os, sys as _sys
    _sys.path.insert(0, _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__))))

from ergasterion.estate import EstateContext
from ergasterion.framework import graph as product_graph_model
from ergasterion.framework.contract import check_generated_files, write_generated_files
from ergasterion.framework.declaration import load_estate_policy
from ergasterion.framework.models import FrameworkError
from ergasterion.translators.publication import PublicationTranslator

# Ambient estate context; functions default to it, a caller threads its own via `ctx=`.
_DEFAULT_CTX = EstateContext.default()

# Estate paths ride the context. Aliases remain for compatible library use.
REPO_ROOT = _DEFAULT_CTX.root
GRAPHS_DIR = _DEFAULT_CTX.graphs_dir


PRODUCT_GRAPH_DIRNAME = "products"
PRODUCT_PRUNE_PATTERNS: tuple[str, ...] = ("product-*.csv", "product-graph.json")


def product_graph_translators() -> tuple[product_graph_model.AuxiliaryRelationProvider, ...]:
    """The translators this route constructs and asks for the private
    auxiliary relations they render under a product's namespace
    (architecture section 10). The publication translator renders the graph
    itself and holds no private relation of its own; a translator whose
    rendering does register one through the same API contributes it here
    with no change to this route."""

    return (PublicationTranslator(),)


def build_estate_product_graph(
    ctx: EstateContext | None = None,
    *,
    products_dir: Path | None = None,
    translators: Iterable[product_graph_model.AuxiliaryRelationProvider] | None = None,
) -> product_graph_model.ProductGraph | None:
    """Resolve the estate's product graph from its product declarations.
    Returns ``None`` when the estate declares no product yet, the same
    convention every other declaration-driven route uses."""

    ctx = ctx or _DEFAULT_CTX
    directory = products_dir if products_dir is not None else ctx.declarations_dir / "products"
    policy = load_estate_policy(ctx.estate_file)
    entries = product_graph_model.load_product_entries(directory, policy=policy)
    if not entries:
        return None
    providers = product_graph_translators() if translators is None else tuple(translators)
    auxiliary = product_graph_model.collect_auxiliary_relations(providers)
    return product_graph_model.build_product_graph(entries, auxiliary=auxiliary)


def generate_products(
    ctx: EstateContext | None = None,
    *,
    products_dir: Path | None = None,
    translators: Iterable[product_graph_model.AuxiliaryRelationProvider] | None = None,
) -> dict[Path, str]:
    """The full {path: text} set of product-graph artefacts for an estate."""

    ctx = ctx or _DEFAULT_CTX
    graph = build_estate_product_graph(ctx, products_dir=products_dir, translators=translators)
    if graph is None:
        return {}
    directory = ctx.graphs_dir / PRODUCT_GRAPH_DIRNAME
    return {directory / name: text for name, text in product_graph_model.graph_artefacts(graph).items()}


def write_product_files(files: dict[Path, str], *, ctx: EstateContext | None = None) -> list[Path]:
    ctx = ctx or _DEFAULT_CTX
    return write_generated_files(
        files,
        directory=ctx.graphs_dir / PRODUCT_GRAPH_DIRNAME,
        prune_patterns=PRODUCT_PRUNE_PATTERNS,
    )


def check_product_files(files: dict[Path, str], *, ctx: EstateContext | None = None) -> list[str]:
    ctx = ctx or _DEFAULT_CTX
    return check_generated_files(
        files,
        directory=ctx.graphs_dir / PRODUCT_GRAPH_DIRNAME,
        root=ctx.root,
        prune_patterns=PRODUCT_PRUNE_PATTERNS,
    )


def main(argv: list[str] | None = None) -> int:
    """`ergasterion product-graph`: resolve and emit the estate product
    graph, or, with --check, report drift against what is on disk."""

    parser = argparse.ArgumentParser(
        prog="ergasterion product-graph",
        description=(
            "Emit the estate product graph from declarations/products/: node and edge tables, "
            "product-level and field-level lineage, declared validation occurrences, and the "
            "graph description."
        ),
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Validate + fail if emitted product-graph artefacts drifted or were hand-edited.",
    )
    parser.add_argument(
        "--estate-root", type=Path, default=None,
        help="Estate root to emit the product graph against (resolved from the environment or working directory when omitted).",
    )
    args = parser.parse_args(argv)

    ctx = EstateContext.resolve(estate_root=args.estate_root)
    try:
        graph = build_estate_product_graph(ctx)
    except FrameworkError as exc:
        # The failures this gate owns and reports by name: estate policy,
        # declaration validation, expression parsing, shape resolution and
        # graph resolution all raise a FrameworkError. Anything else is a
        # defect in this program and surfaces as a traceback.
        print(f"product-graph gate FAIL: {exc}")
        return 1

    if graph is None:
        print("product-graph OK: this estate declares no product yet, so there is no graph to emit")
        return 0

    files = {
        ctx.graphs_dir / PRODUCT_GRAPH_DIRNAME / name: text
        for name, text in product_graph_model.graph_artefacts(graph).items()
    }
    for node in graph.nodes:
        print(
            f"  {node.published_name}: layer={node.layer} profile={node.profile} shape={node.shape} "
            f"generation={node.generation} relations={';'.join(node.relations)} "
            f"materialisation={node.materialisation}"
        )

    if args.check:
        problems = check_product_files(files, ctx=ctx)
        if problems:
            print(f"product-graph gate FAIL: {len(problems)} on-disk divergence(s):")
            for problem in problems:
                print(problem)
            return 1
        print(
            f"product-graph gate OK: {len(files)} artefact(s) byte-match the generated set "
            f"for {len(graph.nodes)} product(s)"
        )
        return 0

    changed = write_product_files(files, ctx=ctx)
    print(f"generated/updated {len([p for p in changed if p in files])} of {len(files)} product-graph artefact(s)")
    for path in sorted(changed):
        marker = "wrote" if path in files else "pruned"
        print(f"  {marker} {path.relative_to(ctx.root).as_posix()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
