"""``ergasterion validate-canonical``: check a canonical product's declared
mappings against a reference model checkout (architecture section 14, check
5's neighbour: the canonical shape is the estate's interface, and a mapping
onto an agreed external model is a claim about that interface).

A canonical product publishes one relation per entity. An entity may also
declare which entity of an external reference model it is the estate's
reading of, and which of that model's attributes each of its own columns
carries:

    target:
      shape: canonical
      shape_config:
        entities:
          - name: deal
            key: [deal_id]
            columns: [deal_id, deal_name, sourced_date]
            reference:
              entity: PM-15
              attributes:
                deal_name: deal_name
                sourced_date: sourced_date

The declaration names the reference entity, never a file path: where the
model is checked out is an operator's fact, supplied to this command, and a
declaration carrying a path would fail the neutrality gate
(architecture section 13).

What the command proves, per declared mapping:

  * the reference model carries an entity of that identifier, in exactly one
    file;
  * every attribute the mapping names is one that model's attribute schema
    declares.

That the mapped column is one the entity publishes is a shape constraint,
proved at emission by ``ergasterion.shapes.canonical`` without any checkout.

Where the checkout is comes from one resolution, ``estate.resolve_openim_root``:
the ``--openim-root`` flag, then the ``DPF_OPENIM_ROOT`` environment
variable, then a checkout named ``openim`` beside the estate root. Without a
checkout the command cannot prove anything, and says so: it prints the reason
it skipped, names all three places it looked, and exits 0. A wrong mapping
with a checkout present exits 1 naming the product, the entity and the
attribute.

Usage:
    ergasterion validate-canonical --openim-root <dir>
    ergasterion validate-canonical --estate-root <dir> --openim-root <dir>
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

# Runs either installed (`ergasterion validate-canonical`) or script-mode. In
# script-mode the running module's own repo root is inserted at sys.path[0]
# ahead of site-packages so `import ergasterion.*` binds to THIS tree.
if __package__ in (None, ""):
    import os as _os, sys as _sys
    _sys.path.insert(0, _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__))))

from ergasterion.estate import (
    OPENIM_ROOT_ENV,
    OPENIM_SIBLING_NAME,
    REFERENCE_MODEL_DIRECTORY,
    EstateContext,
)
from ergasterion.framework import graph as graph_mod
from ergasterion.framework.declaration import load_estate_policy
from ergasterion.framework.models import FrameworkError
from ergasterion.shapes.canonical import (
    REFERENCE_ATTRIBUTES_KEY,
    REFERENCE_ENTITY_KEY,
    REFERENCE_KEY,
    SHAPE_NAME as CANONICAL_SHAPE,
)

# The heading the attribute table sits under in a reference entity document,
# and the column header its first column carries.
ATTRIBUTE_SECTION = re.compile(r"^##\s+Attribute schema\b")
ATTRIBUTE_HEADER = "column"


class CanonicalMappingError(FrameworkError):
    """A declared canonical mapping does not hold against the reference
    model. Names the product, the entity and what is wrong."""

    code = "canonical_mapping"

    def __init__(self, *, product: str, entity: str, detail: str) -> None:
        self.product = product
        self.entity = entity
        self.detail = detail
        super().__init__(f"product {product!r} entity {entity!r}: {detail}")


def reference_attributes(path: Path) -> tuple[str, ...]:
    """Every attribute one reference entity document declares, read off the
    table under its attribute-schema heading, in the order it states them.

    The table is markdown: a header row naming its first column, a separator
    row, then one row per attribute whose first cell is the attribute name,
    optionally in backticks. A document with no such section, or one whose
    table parses to nothing, is not something to validate against and fails
    closed rather than passing vacuously."""

    lines = path.read_text(encoding="utf-8").splitlines()
    start = None
    for index, line in enumerate(lines):
        if ATTRIBUTE_SECTION.match(line.strip()):
            start = index + 1
            break
    if start is None:
        raise ValueError(f"{path}: no attribute-schema section")
    attributes: list[str] = []
    seen_header = False
    for line in lines[start:]:
        stripped = line.strip()
        if stripped.startswith("## "):
            break
        if not stripped.startswith("|"):
            continue
        cells = [cell.strip() for cell in stripped.strip("|").split("|")]
        if not cells:
            continue
        if not seen_header:
            if cells[0].lower() == ATTRIBUTE_HEADER:
                seen_header = True
            continue
        if set(cells[0]) <= {"-", ":", " "}:
            continue
        attributes.append(cells[0].strip("`").strip())
    if not attributes:
        raise ValueError(f"{path}: the attribute-schema table declares no attribute")
    return tuple(attributes)


def reference_document(root: Path, entity: str) -> Path:
    """The one document of the reference model that carries ``entity``.
    Files are named for the entity they carry, so the identifier is the
    prefix of the file name. None, or more than one, fails closed rather
    than picking."""

    directory = root / REFERENCE_MODEL_DIRECTORY
    matches = sorted(
        set(directory.rglob(f"{entity}-*.md")) | set(directory.rglob(f"{entity}.md"))
    )
    if len(matches) != 1:
        raise ValueError(
            f"{entity}: {len(matches)} document(s) under {directory.as_posix()} carry this "
            "entity; a reference entity is carried by exactly one"
        )
    return matches[0]


def declared_mappings(ctx: EstateContext) -> list[tuple[str, str, dict]]:
    """Every reference mapping the estate's canonical products declare, as
    (published name, entity name, the reference block), in a deterministic
    order."""

    policy = load_estate_policy(ctx.estate_file)
    entries = graph_mod.load_product_entries(ctx.declarations_dir / "products", policy=policy)
    mappings: list[tuple[str, str, dict]] = []
    for name in sorted(entries):
        document, validated = entries[name]
        if validated.shape != CANONICAL_SHAPE:
            continue
        shape_config = (document.get("target") or {}).get("shape_config") or {}
        for entity in shape_config.get("entities") or []:
            reference = entity.get(REFERENCE_KEY)
            if reference:
                mappings.append((name, str(entity["name"]), dict(reference)))
    return mappings


def validate(ctx: EstateContext) -> list[str]:
    """Validate every declared mapping against the checkout the context
    carries and return one report line per mapping. Raises
    ``CanonicalMappingError`` on the first mapping that does not hold."""

    root = ctx.openim_root
    assert root is not None  # the caller decides whether to skip
    report: list[str] = []
    schemas: dict[str, tuple[str, ...]] = {}
    for product, entity, reference in declared_mappings(ctx):
        identifier = str(reference[REFERENCE_ENTITY_KEY])
        if identifier not in schemas:
            try:
                document = reference_document(root, identifier)
                schemas[identifier] = reference_attributes(document)
            except ValueError as error:
                raise CanonicalMappingError(
                    product=product, entity=entity, detail=str(error)
                ) from error
        declared = schemas[identifier]
        attributes = reference[REFERENCE_ATTRIBUTES_KEY]
        for column in sorted(attributes):
            attribute = str(attributes[column])
            if attribute not in declared:
                raise CanonicalMappingError(
                    product=product,
                    entity=entity,
                    detail=(
                        f"column {column!r} is mapped onto attribute {attribute!r}, which "
                        f"{identifier} does not declare: {list(declared)}"
                    ),
                )
        report.append(
            f"validated {product} entity {entity}: {len(attributes)} attribute(s) against "
            f"{identifier}"
        )
    return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--estate-root",
        type=Path,
        default=None,
        help="Estate root whose canonical products are validated (resolved from the "
        "environment or working directory when omitted).",
    )
    parser.add_argument(
        "--openim-root",
        type=Path,
        default=None,
        help="Root of the reference model checkout. Without it the DPF_OPENIM_ROOT "
        "environment variable is read, then a checkout named 'openim' beside the estate "
        "root; with none of the three the command has nothing to validate against and "
        "says so.",
    )
    arguments = parser.parse_args(argv)
    # One resolution owns where the checkout is (``estate.resolve_openim_root``):
    # the flag first, the environment variable second, the sibling third. This
    # command never looks anywhere itself.
    ctx = EstateContext.resolve(
        estate_root=arguments.estate_root, openim_root=arguments.openim_root
    )

    if ctx.openim_root is None:
        print(
            "skipped: no reference model checkout resolved. Pass --openim-root, set "
            f"{OPENIM_ROOT_ENV}, or check the model out as {OPENIM_SIBLING_NAME!r} beside "
            f"{ctx.root.as_posix()}"
        )
        return 0
    if not (ctx.openim_root / REFERENCE_MODEL_DIRECTORY).is_dir():
        print(
            f"skipped: {ctx.openim_root.as_posix()} carries no {REFERENCE_MODEL_DIRECTORY}/ directory, "
            "so it is not a reference model checkout"
        )
        return 0

    try:
        report = validate(ctx)
    except (CanonicalMappingError, FrameworkError, ValueError) as error:
        print(f"FAIL: {error}", file=sys.stderr)
        return 1
    for line in report:
        print(line)
    print(f"canonical mappings: {len(report)} validated against {ctx.openim_root.as_posix()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
