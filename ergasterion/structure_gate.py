"""Per-target structural gate over the estate's model tree.

Every deployment target the emitters serve declares its structural budgets as
data, one file per target under ``declarations/targets/<adapter>.yml``, keyed
by the target's dbt adapter name. The reserved ``interfaces.yml`` in the same
directory declares the estate's interface boundaries: the model paths whose
artefacts may materialise as views. This gate validates the whole models tree
-- emitted and hand-authored alike -- against every declared target:

  * materialisation boundary -- on a deployment target, a view sits under a
    declared interface path; on a lane target, a view outside those paths
    needs an entry in the lane's own ``view_exceptions`` with a stated reason;
  * relation nesting -- ``view_depth`` counts the consecutive view levels a
    statement expands (a table caps the chain); every model stays within the
    target's declared ceiling;
  * statement size -- each model file's source byte count stays within budget;
  * identifier length -- relation identifiers (model, seed, and source table
    names) and column identifiers (schema-file columns and seed CSV headers)
    stay within budget;
  * metadata payload -- each description string shipped in the models tree
    stays within budget.

A breach fails generation, naming the target, the budget, the artefact, and
the measured value against the declared limit. A missing or empty
``declarations/targets/`` directory is itself a failure: the gate is
fail-closed, and ``ergasterion init`` scaffolds the directory for new estates.

Runs standalone (``python ergasterion/structure_gate.py`` or ``ergasterion
structure``) and as the post-emit gate inside ``ergasterion/emit.py``.
"""

from __future__ import annotations

import argparse
import csv
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

if __package__ in (None, ""):
    import os as _os, sys as _sys
    _sys.path.insert(0, _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__))))

from ergasterion.estate import EstateContext
from ergasterion.dialect_lint import _is_generated

INTERFACES_FILE = "interfaces.yml"
TARGET_KINDS = ("deployment", "lane")

# Budget keys a deployment target must declare, each a positive integer.
REQUIRED_BUDGETS = (
    "max_view_chain_depth",
    "max_statement_bytes",
    "max_relation_identifier_chars",
    "max_column_identifier_chars",
    "max_description_bytes",
    "max_seed_rows",
    "max_seed_bytes",
)

_REF_RE = re.compile(r"""ref\(\s*['"]([^'"]+)['"]\s*(?:,\s*['"]([^'"]+)['"]\s*)?\)""")
_MAT_RE = re.compile(r"""materialized\s*=\s*['"](\w+)['"]""")


def normalise_landing(table: dict[str, Any], where: str) -> dict[str, Any]:
    """Apply and validate the declaration landing discriminator in one place."""
    landing = table.setdefault("landing", {"kind": "seed"})
    if not isinstance(landing, dict):
        raise ValueError(f"{where}.landing: expected a mapping")
    kind = landing.get("kind")
    if kind not in {"seed", "source"}:
        raise ValueError(
            f"{where}.landing.kind: expected 'seed' or 'source', got {kind!r}"
        )
    # The Bronze-contract fields (integration, content_encodings, codec,
    # physical_columns) are OPTIONAL here: this structural gate stays the single
    # entry point for the landing discriminator, but semantic validation of the
    # full Bronze shape -- required once a table also carries a `delivery`
    # block -- belongs to ergasterion.source_delivery (the "no template owns
    # semantic validation" split). A bare {kind: source, source_name,
    # identifier} landing with no delivery block stays a legacy dbt source()
    # reference, unchanged from before this module existed.
    allowed_keys = (
        {"kind"}
        if kind == "seed"
        else {
            "kind",
            "source_name",
            "identifier",
            "integration",
            "content_encodings",
            "codec",
            "physical_columns",
        }
    )
    unknown_keys = sorted(set(landing) - allowed_keys)
    if unknown_keys:
        raise ValueError(
            f"{where}.landing: unknown field(s) for kind {kind!r}: "
            f"{', '.join(unknown_keys)}"
        )
    if kind == "source":
        missing_keys = sorted({"source_name", "identifier"} - set(landing))
        if missing_keys:
            raise ValueError(
                f"{where}.landing: kind 'source' needs {', '.join(missing_keys)}"
            )
        if "raw_model" in table:
            raise ValueError(
                f"{where}: kind 'source' cannot declare raw_model; remove the obsolete "
                "seed relation and CSV"
            )
    return landing


@dataclass(frozen=True)
class TargetDeclaration:
    adapter: str
    kind: str
    budgets: dict[str, int]
    view_exceptions: dict[str, str]  # repo-relative posix path -> reason
    path: Path


@dataclass(frozen=True)
class Offense:
    adapter: str
    budget: str
    artefact: str
    measured: int
    limit: int
    message: str


@dataclass
class ModelInfo:
    name: str
    rel_path: str  # repo-relative posix
    materialisation: str
    refs: list[str]
    size_bytes: int
    view_depth: int = 0


@dataclass
class EstateScan:
    models: dict[str, ModelInfo]
    relation_identifiers: dict[str, str]  # identifier -> where declared
    column_identifiers: dict[str, str]
    descriptions: list[tuple[str, int]]  # (where, byte length)
    seed_sizes: dict[str, tuple[str, int, int]]  # seed name -> (path, rows, bytes)


# --- declaration loading -------------------------------------------------------------

def targets_dir(ctx: EstateContext) -> Path:
    return ctx.declarations_dir / "targets"


def load_structure_declarations(
    ctx: EstateContext | None = None,
) -> tuple[list[TargetDeclaration], list[str]]:
    """Load every target budget declaration plus the declared interface paths.

    Raises ValueError, naming the file, on a malformed declaration: unknown kind,
    adapter/filename mismatch, a deployment target missing a budget, a budget
    value outside the positive integers, or a lane view exception without a
    stated reason.
    """
    ctx = ctx or EstateContext.default()
    directory = targets_dir(ctx)
    if not directory.is_dir():
        raise ValueError(
            f"{directory}: declarations/targets/ is missing -- every deployment "
            f"target declares its structural budgets there (one <adapter>.yml per "
            f"target, plus {INTERFACES_FILE} for the declared view boundaries)"
        )

    declarations: list[TargetDeclaration] = []
    view_layers: list[str] = []
    for path in sorted(directory.glob("*.yml")):
        data = yaml.safe_load(path.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            raise ValueError(f"{path}: expected a YAML mapping")
        if path.name == INTERFACES_FILE:
            layers = data.get("view_layers", [])
            if not isinstance(layers, list) or not all(isinstance(p, str) for p in layers):
                raise ValueError(f"{path}: view_layers must be a list of model paths")
            view_layers = [layer.replace("\\", "/").rstrip("/") for layer in layers]
            continue

        adapter = data.get("adapter")
        if not isinstance(adapter, str) or adapter != path.stem:
            raise ValueError(
                f"{path}: adapter {adapter!r} must match the filename stem {path.stem!r}"
            )
        kind = data.get("kind")
        if kind not in TARGET_KINDS:
            raise ValueError(f"{path}: kind must be one of {TARGET_KINDS}, got {kind!r}")

        budgets_raw = data.get("budgets", {})
        if not isinstance(budgets_raw, dict):
            raise ValueError(f"{path}: budgets must be a mapping")
        budgets: dict[str, int] = {}
        for key, value in budgets_raw.items():
            if not isinstance(value, int) or isinstance(value, bool) or value < 1:
                raise ValueError(f"{path}: budget {key} must be a positive integer, got {value!r}")
            budgets[key] = value
        if kind == "deployment":
            missing = [key for key in REQUIRED_BUDGETS if key not in budgets]
            if missing:
                raise ValueError(
                    f"{path}: deployment target {adapter} is missing budget(s): "
                    f"{', '.join(missing)}"
                )

        exceptions_raw = data.get("view_exceptions", [])
        if kind == "deployment" and exceptions_raw:
            raise ValueError(
                f"{path}: view_exceptions belong to lane targets only; on a deployment "
                f"target a view lives under a declared interface path"
            )
        view_exceptions: dict[str, str] = {}
        for entry in exceptions_raw:
            if (
                not isinstance(entry, dict)
                or not isinstance(entry.get("path"), str)
                or not isinstance(entry.get("reason"), str)
                or not entry["reason"].strip()
            ):
                raise ValueError(
                    f"{path}: each view exception needs a path and a stated reason "
                    f"(the lane tooling that depends on the view)"
                )
            view_exceptions[entry["path"].replace("\\", "/")] = entry["reason"]

        declarations.append(
            TargetDeclaration(
                adapter=str(adapter),
                kind=str(kind),
                budgets=budgets,
                view_exceptions=view_exceptions,
                path=path,
            )
        )

    if not any(decl.kind == "deployment" for decl in declarations):
        raise ValueError(
            f"{directory}: no deployment target declares structural budgets -- add one "
            f"<adapter>.yml per deployment target the estate builds on"
        )
    return declarations, view_layers


# --- estate scan ---------------------------------------------------------------------

def _project_model_config(ctx: EstateContext) -> dict[str, Any]:
    project_path = ctx.root / "dbt_project.yml"
    if not project_path.is_file():
        return {}
    project = yaml.safe_load(project_path.read_text(encoding="utf-8")) or {}
    name = project.get("name")
    models = project.get("models", {})
    return models.get(name, {}) if isinstance(models, dict) else {}


def _layer_materialisation(config: dict[str, Any], rel_dir_parts: tuple[str, ...]) -> str | None:
    materialisation = config.get("+materialized")
    node: Any = config
    for part in rel_dir_parts:
        node = node.get(part) if isinstance(node, dict) else None
        if not isinstance(node, dict):
            break
        if "+materialized" in node:
            materialisation = node["+materialized"]
    return materialisation


def scan_estate(ctx: EstateContext | None = None) -> EstateScan:
    """Scan the models tree and seeds for the facts the budgets bind.

    Materialisation resolves the way dbt resolves it: an in-file ``config()``
    wins, then the ``dbt_project.yml`` models block for the model's directory,
    then dbt's default of view. ``view_depth`` is computed per model over the
    ``ref()`` graph; a reference outside the models tree (seed or source) is
    materialised by definition and caps the chain.
    """
    ctx = ctx or EstateContext.default()
    layer_config = _project_model_config(ctx)

    models: dict[str, ModelInfo] = {}
    for path in sorted(ctx.models_dir.rglob("*.sql")):
        text = path.read_text(encoding="utf-8")
        in_file = _MAT_RE.search(text)
        if in_file:
            materialisation = in_file.group(1)
        else:
            rel_parts = path.relative_to(ctx.models_dir).parts[:-1]
            materialisation = _layer_materialisation(layer_config, rel_parts) or "view"
        refs = sorted({match.group(2) or match.group(1) for match in _REF_RE.finditer(text)})
        models[path.stem] = ModelInfo(
            name=path.stem,
            rel_path=path.relative_to(ctx.root).as_posix(),
            materialisation=materialisation,
            refs=refs,
            size_bytes=len(text.encode("utf-8")),
        )

    memo: dict[str, int] = {}
    resolving: set[str] = set()

    def view_depth(name: str) -> int:
        if name not in models:
            return 0
        if name in memo:
            return memo[name]
        if name in resolving:  # a ref cycle; dbt rejects it, the gate stays total
            return 0
        info = models[name]
        if info.materialisation != "view":
            memo[name] = 0
            return 0
        resolving.add(name)
        below = max((view_depth(ref) for ref in info.refs), default=0)
        resolving.discard(name)
        memo[name] = below + 1
        return memo[name]

    for name, info in models.items():
        info.view_depth = view_depth(name)

    relation_identifiers: dict[str, str] = {
        info.name: info.rel_path for info in models.values()
    }
    column_identifiers: dict[str, str] = {}
    descriptions: list[tuple[str, int]] = []
    seed_sizes: dict[str, tuple[str, int, int]] = {}

    declared_seed_names: set[str] = set()
    for declaration_path in sorted(ctx.declarations_dir.glob("*.yml")):
        declaration = yaml.safe_load(declaration_path.read_text(encoding="utf-8")) or {}
        if not isinstance(declaration, dict):
            continue
        for table_name, table in declaration.get("tables", {}).items():
            if not isinstance(table, dict):
                continue
            landing = normalise_landing(
                table, f"{declaration_path}:{table_name}"
            )
            if landing["kind"] == "seed":
                raw_model = table.get("raw_model")
                if isinstance(raw_model, str):
                    declared_seed_names.add(raw_model)

    seeds_dir = ctx.root / "seeds"
    if seeds_dir.is_dir():
        for csv_path in sorted(seeds_dir.glob("*.csv")):
            rel = csv_path.relative_to(ctx.root).as_posix()
            relation_identifiers.setdefault(csv_path.stem, rel)
            with csv_path.open(encoding="utf-8", newline="") as handle:
                records = csv.reader(handle)
                header = next(records, [])
                row_count = sum(1 for _ in records)
            if csv_path.stem in declared_seed_names:
                seed_sizes[csv_path.stem] = (rel, row_count, csv_path.stat().st_size)
            for column in header:
                column = column.strip()
                if column:
                    column_identifiers.setdefault(column, rel)

    def walk(node: Any, where: str, parent_key: str | None) -> None:
        if isinstance(node, dict):
            name = node.get("name")
            if isinstance(name, str):
                if parent_key == "columns":
                    column_identifiers.setdefault(name, where)
                elif parent_key in {"models", "seeds", "sources", "tables", "snapshots"}:
                    relation_identifiers.setdefault(name, where)
            description = node.get("description")
            if isinstance(description, str):
                descriptions.append((where, len(description.encode("utf-8"))))
            for key, value in node.items():
                walk(value, where, key)
        elif isinstance(node, list):
            for item in node:
                walk(item, where, parent_key)

    for yml_path in sorted(ctx.models_dir.rglob("*.yml")):
        rel = yml_path.relative_to(ctx.root).as_posix()
        for document in yaml.safe_load_all(yml_path.read_text(encoding="utf-8")):
            walk(document, rel, None)

    return EstateScan(
        models=models,
        relation_identifiers=relation_identifiers,
        column_identifiers=column_identifiers,
        descriptions=descriptions,
        seed_sizes=seed_sizes,
    )


# --- the gate ------------------------------------------------------------------------

def hand_authored_window_references(
    windowed_models: set[str] | frozenset[str], ctx: EstateContext | None = None
) -> list[tuple[str, str, str]]:
    """Every hand-authored model that reads a window-filtered relation.

    A window-filtered stage or bridge holds the delta window only, so a hand-authored
    model reading it sees the delta rather than the whole history and silently loses
    rows. Returns ``(model name, repo-relative path, referenced windowed model)``, one
    entry per offending reference, sorted.
    """
    if not windowed_models:
        return []
    ctx = ctx or EstateContext.default()
    if not ctx.models_dir.is_dir():
        return []
    offenders: list[tuple[str, str, str]] = []
    for path in sorted(ctx.models_dir.rglob("*.sql")):
        if _is_generated(path):
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except OSError:
            continue
        refs = {match.group(2) or match.group(1) for match in _REF_RE.finditer(text)}
        rel_path = path.relative_to(ctx.root).as_posix()
        for referenced in sorted(refs & set(windowed_models)):
            offenders.append((path.stem, rel_path, referenced))
    return sorted(offenders)


def _under_layer(rel_path: str, layers: list[str]) -> bool:
    return any(rel_path == layer or rel_path.startswith(layer + "/") for layer in layers)


def check_structure(ctx: EstateContext | None = None) -> list[Offense]:
    """Validate the estate against every declared target; return every offense."""
    ctx = ctx or EstateContext.default()
    declarations, view_layers = load_structure_declarations(ctx)
    scan = scan_estate(ctx)
    offenses: list[Offense] = []

    for target in declarations:
        budgets = target.budgets

        for info in scan.models.values():
            if info.materialisation != "view":
                continue
            if _under_layer(info.rel_path, view_layers):
                continue
            if target.kind == "lane" and info.rel_path in target.view_exceptions:
                continue
            offenses.append(
                Offense(
                    adapter=target.adapter,
                    budget="view_boundary",
                    artefact=info.rel_path,
                    measured=1,
                    limit=0,
                    message=(
                        f"view outside the declared interface boundaries "
                        f"(view_layers: {', '.join(view_layers) or 'none declared'}) -- "
                        f"computation layers materialise as tables"
                    ),
                )
            )

        depth_limit = budgets.get("max_view_chain_depth")
        if depth_limit is not None:
            for info in scan.models.values():
                if info.view_depth > depth_limit:
                    offenses.append(
                        Offense(
                            adapter=target.adapter,
                            budget="max_view_chain_depth",
                            artefact=info.rel_path,
                            measured=info.view_depth,
                            limit=depth_limit,
                            message=(
                                f"view chain {info.view_depth} deep exceeds the declared "
                                f"ceiling of {depth_limit}"
                            ),
                        )
                    )

        statement_limit = budgets.get("max_statement_bytes")
        if statement_limit is not None:
            for info in scan.models.values():
                if info.size_bytes > statement_limit:
                    offenses.append(
                        Offense(
                            adapter=target.adapter,
                            budget="max_statement_bytes",
                            artefact=info.rel_path,
                            measured=info.size_bytes,
                            limit=statement_limit,
                            message=f"model statement is {info.size_bytes} bytes, budget {statement_limit}",
                        )
                    )

        relation_limit = budgets.get("max_relation_identifier_chars")
        if relation_limit is not None:
            for identifier, where in scan.relation_identifiers.items():
                if len(identifier) > relation_limit:
                    offenses.append(
                        Offense(
                            adapter=target.adapter,
                            budget="max_relation_identifier_chars",
                            artefact=f"{where}:{identifier}",
                            measured=len(identifier),
                            limit=relation_limit,
                            message=f"relation identifier is {len(identifier)} chars, budget {relation_limit}",
                        )
                    )

        column_limit = budgets.get("max_column_identifier_chars")
        if column_limit is not None:
            for identifier, where in scan.column_identifiers.items():
                if len(identifier) > column_limit:
                    offenses.append(
                        Offense(
                            adapter=target.adapter,
                            budget="max_column_identifier_chars",
                            artefact=f"{where}:{identifier}",
                            measured=len(identifier),
                            limit=column_limit,
                            message=f"column identifier is {len(identifier)} chars, budget {column_limit}",
                        )
                    )

        description_limit = budgets.get("max_description_bytes")
        if description_limit is not None:
            for where, size in scan.descriptions:
                if size > description_limit:
                    offenses.append(
                        Offense(
                            adapter=target.adapter,
                            budget="max_description_bytes",
                            artefact=where,
                            measured=size,
                            limit=description_limit,
                            message=f"description is {size} bytes, budget {description_limit}",
                        )
                    )

        seed_rows_limit = budgets.get("max_seed_rows")
        if seed_rows_limit is not None:
            for _seed_name, (where, rows, _size_bytes) in scan.seed_sizes.items():
                if rows > seed_rows_limit:
                    offenses.append(
                        Offense(
                            adapter=target.adapter,
                            budget="max_seed_rows",
                            artefact=where,
                            measured=rows,
                            limit=seed_rows_limit,
                            message=f"seed has {rows} rows, budget {seed_rows_limit}",
                        )
                    )

        seed_bytes_limit = budgets.get("max_seed_bytes")
        if seed_bytes_limit is not None:
            for _seed_name, (where, _rows, size_bytes) in scan.seed_sizes.items():
                if size_bytes > seed_bytes_limit:
                    offenses.append(
                        Offense(
                            adapter=target.adapter,
                            budget="max_seed_bytes",
                            artefact=where,
                            measured=size_bytes,
                            limit=seed_bytes_limit,
                            message=f"seed is {size_bytes} bytes, budget {seed_bytes_limit}",
                        )
                    )

    return offenses


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--estate-root",
        type=Path,
        default=None,
        help="Root of the estate to validate (resolved from the environment or working directory when omitted).",
    )
    args = parser.parse_args()
    ctx = EstateContext.resolve(estate_root=args.estate_root)

    try:
        offenses = check_structure(ctx)
    except ValueError as error:
        print(f"structure-gate FAIL: {error}")
        print("STRUCTURE_OFFENSES=1")
        return 1

    print(f"STRUCTURE_OFFENSES={len(offenses)}")
    if offenses:
        print(f"structure-gate FAIL: {len(offenses)} budget offense(s):")
        for offense in offenses:
            print(
                f"  [{offense.adapter}] {offense.budget}: {offense.artefact} -- "
                f"{offense.message}"
            )
        return 1

    declarations, view_layers = load_structure_declarations(ctx)
    scan = scan_estate(ctx)
    max_depth = max((info.view_depth for info in scan.models.values()), default=0)
    adapters = ", ".join(decl.adapter for decl in declarations)
    print(
        f"structure-gate OK: {len(scan.models)} models within budget for [{adapters}]; "
        f"max view chain depth {max_depth}; view layers: {', '.join(view_layers) or 'none'}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
