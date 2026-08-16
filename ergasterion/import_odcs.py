"""Create a source-declaration skeleton from an ODCS v3 contract.

The command transcribes the contract's schema, keys, and constraints into projection and
test stubs. It does not guess vault mappings, entity-resolution rules, or survivorship
policy. Review and complete the generated declaration before emitting a pipeline.

The generated file is an editable starting point. Re-running with ``--force`` overwrites
the file; Ergasterion does not merge it with later hand edits.

Usage:
    python ergasterion/import_odcs.py <path-to-contract.odcs.yml> --source <name>
    python ergasterion/import_odcs.py <path> --source <name> --out declarations/<name>.yml --force
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path
from typing import Any

import yaml

# Support installed-command and direct-script execution.
if __package__ in (None, ""):
    import os as _os, sys as _sys
    _sys.path.insert(0, _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__))))

from ergasterion import emit
from ergasterion.dialect_lint import DENY_LISTS
from ergasterion.estate import EstateContext

# Ambient estate context; main() resolves its own (honouring --estate-root). Aliases kept
# for back-compat reads over the committed tree.
_DEFAULT_CTX = EstateContext.default()
REPO_ROOT = _DEFAULT_CTX.root
DECLARATIONS_DIR = _DEFAULT_CTX.declarations_dir
TEMPLATE_NAME = "declaration_seed.yml.j2"

MIN_SUPPORTED_MAJOR = 3
ODCS_UPGRADE_NOTE = (
    "ODCS v3.0.0 was a breaking rewrite over v2 (uuid->id, quantumName->dataProduct, "
    "datasetDomain->domain, columns->properties with nested/array support, stakeholders->team, "
    "plaintext username/password removed). Upgrade the contract to ODCS v3.x before importing -- "
    "see https://bitol-io.github.io/open-data-contract-standard/latest/ ."
)

# ODCS v3 logicalType -> a safe, cross-dialect cast expression using this factory's own
# blessed conventions (macros/cross_db.sql's dpf_safe_cast and dpf_json_cast dispatch
# macros for conversions that can fail; a plain `cast(... as string)` for text/identifiers). A supplier's
# warehouse-native physicalType (e.g. "VARCHAR(50)", "NUMBER(18,2)") is intentionally never
# compiled into the expression -- it rides as an informational trailing comment instead
# (ergasterion/dialect_lint.py's deny-list is the reason: only dpf_* dispatch calls + plain
# neutral `cast` are blessed in model-adjacent SQL, this repo's SSOT for what is safe to emit).
_LOGICAL_TYPE_CAST: dict[str, str] = {
    "string": "cast({col} as string)",
    "integer": "{{{{ dpf_safe_cast('{col}', 'int') }}}}",
    "number": "{{{{ dpf_safe_cast('{col}', 'numeric') }}}}",
    "date": "{{{{ dpf_safe_cast('{col}', 'date') }}}}",
    "boolean": "{{{{ dpf_safe_cast('{col}', 'boolean') }}}}",
    "object": "{{{{ dpf_json_cast('{col}') }}}}",
    "array": "{{{{ dpf_json_cast('{col}') }}}}",
}

_CROSS_ADAPTER_CAST_TYPES = frozenset({
    "boolean",
    "date",
    "decimal",
    "int",
    "integer",
    "numeric",
    "string",
    "timestamp",
})
_NEUTRAL_CAST_TYPES = frozenset(
    type_name
    for type_name in _CROSS_ADAPTER_CAST_TYPES
    if not any(
        rule.pattern.search(f"cast(value as {type_name})")
        for rules in DENY_LISTS.values()
        for rule in rules
    )
)
_DPF_DISPATCH_CALL_RE = re.compile(r"\{\{\s+dpf_[a-z0-9_]+\([^(){}]*\)\s+\}\}")
_WAREHOUSE_NATIVE_TYPE_RE = re.compile(r"(?<![a-z0-9_])(variant|object|struct)(?![a-z0-9_])", re.IGNORECASE)
_PLAIN_SQL_IDENTIFIER_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
_CAST_GATE_COLUMN_SENTINEL = "__dpf_seed_column__"


class OdcsImportError(ValueError):
    """A supplied contract failed the ODCS v3.x well-formedness gate. The message always
    names the specific problem (malformed field, wrong kind, pre-v3 apiVersion, ...)."""


def _major_version(api_version: Any, path: Path) -> int:
    if not isinstance(api_version, str) or not api_version.strip():
        raise OdcsImportError(
            f"{path}: missing required 'apiVersion' field -- not a well-formed ODCS contract "
            f"(every ODCS document declares apiVersion, e.g. apiVersion: v3.1.0)."
        )
    match = re.match(r"^v?(\d+)\.\d+\.\d+$", api_version.strip())
    if not match:
        raise OdcsImportError(
            f"{path}: cannot parse apiVersion {api_version!r} -- expected the form "
            f"'v3.1.0' or '3.1.0'."
        )
    return int(match.group(1))


def load_and_validate_contract(path: Path) -> dict[str, Any]:
    """Load an ODCS contract and run the well-formedness gate. Every rejection names the
    specific problem (malformed structure, wrong kind, or a pre-v3 apiVersion with an
    explicit says-upgrade message) -- never a silent best-effort parse."""
    if not path.exists():
        raise OdcsImportError(f"{path}: file not found")
    try:
        doc = yaml.safe_load(path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        raise OdcsImportError(f"{path}: not valid YAML -- {exc}") from exc
    if not isinstance(doc, dict):
        raise OdcsImportError(f"{path}: expected a YAML mapping at the document root, got {type(doc).__name__}")

    major = _major_version(doc.get("apiVersion"), path)
    if major < MIN_SUPPORTED_MAJOR:
        raise OdcsImportError(
            f"{path}: ODCS {doc['apiVersion']} is not supported -- this is a pre-v3 contract. "
            f"{ODCS_UPGRADE_NOTE}"
        )

    kind = doc.get("kind")
    if kind != "DataContract":
        raise OdcsImportError(
            f"{path}: expected kind: DataContract, got {kind!r} -- not a data contract document."
        )

    schema = doc.get("schema")
    if not isinstance(schema, list) or not schema:
        raise OdcsImportError(
            f"{path}: missing or empty 'schema' section -- an ODCS schema is a list of "
            f"table/object definitions with properties; there is nothing to seed from."
        )
    for i, obj in enumerate(schema):
        if not isinstance(obj, dict) or not obj.get("name"):
            raise OdcsImportError(f"{path}: schema[{i}] is missing a 'name' -- cannot name the table to seed.")
        properties = obj.get("properties")
        if not isinstance(properties, list) or not properties:
            raise OdcsImportError(
                f"{path}: schema[{i}] ({obj.get('name')!r}) has no 'properties' -- nothing to "
                f"seed a projection from."
            )
        for j, prop in enumerate(properties):
            if not isinstance(prop, dict) or not prop.get("name"):
                raise OdcsImportError(
                    f"{path}: schema[{i}] ({obj.get('name')!r}) property[{j}] is missing a 'name'."
                )
    return doc


def _description_text(value: Any) -> str:
    if isinstance(value, dict):
        return " ".join(str(value.get("purpose", "")).split())
    if isinstance(value, str):
        return " ".join(value.split())
    return ""


def _cast_expression(column: str, logical_type: str | None) -> str:
    if not _PLAIN_SQL_IDENTIFIER_RE.fullmatch(column):
        raise OdcsImportError(f"column {column!r}: expected a plain SQL identifier")
    template = _LOGICAL_TYPE_CAST.get((logical_type or "").lower(), _LOGICAL_TYPE_CAST["string"])
    expression = template.format(col=column)
    gate_expression = template.format(col=_CAST_GATE_COLUMN_SENTINEL)
    plain_cast = re.fullmatch(
        rf"cast\({_CAST_GATE_COLUMN_SENTINEL} as (?P<type>[a-z]+)\)",
        gate_expression,
    )
    if not (
        (plain_cast and plain_cast.group("type") in _NEUTRAL_CAST_TYPES)
        or (
            _DPF_DISPATCH_CALL_RE.fullmatch(gate_expression)
            and not _WAREHOUSE_NATIVE_TYPE_RE.search(gate_expression)
        )
    ):
        raise OdcsImportError(
            f"column {column!r}: seeded cast expression is not dialect-neutral: {expression}"
        )
    return expression


def _column_data_tests(prop: dict[str, Any]) -> list[str]:
    tests: list[str] = []
    if prop.get("required"):
        tests.append("not_null")
    if prop.get("unique") or prop.get("primaryKey"):
        tests.append("unique")
    return tests


def build_tables(contract: dict[str, Any]) -> list[dict[str, Any]]:
    """Mechanically transcribe the contract's schema section into table dicts: one per
    ODCS schema object, straight passthrough of column names, types -> cast expressions,
    required/unique/primaryKey -> seed_tests + model_tests. No vault/entity-resolution/
    survivorship content -- see module docstring."""
    contract_description = _description_text(contract.get("description"))
    tables: list[dict[str, Any]] = []
    for schema_object in contract["schema"]:
        table_name = schema_object["name"]
        description = _description_text(schema_object.get("description")) or contract_description
        projection: list[dict[str, Any]] = []
        column_tests: list[dict[str, Any]] = []
        for prop in schema_object["properties"]:
            column = prop["name"]
            logical_type = prop.get("logicalType")
            projection.append({
                "name": column,
                "expression": _cast_expression(column, logical_type),
                "physical_type": prop.get("physicalType"),
            })
            data_tests = _column_data_tests(prop)
            if data_tests:
                column_tests.append({"name": column, "data_tests": data_tests})
        tables.append({
            "name": table_name,
            # raw_model is filled in by build_context() once source_name is known.
            "raw_model": None,
            "description": description,
            "seed_tests": list(column_tests),
            "model_tests": list(column_tests),
            "projection": projection,
        })
    return tables


def _slugify(text: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "_", text.strip().lower()).strip("_")
    return slug or "source"


def default_source_name(contract: dict[str, Any]) -> str:
    contract_id = contract.get("id")
    if isinstance(contract_id, str) and contract_id.strip():
        return _slugify(contract_id.split(":")[-1])
    name = contract.get("name")
    if isinstance(name, str) and name.strip():
        return _slugify(name)
    return "source"


def build_context(
    contract: dict[str, Any],
    contract_path: Path,
    source_name: str,
    display_name: str | None,
    priority: int,
) -> dict[str, Any]:
    display_name = display_name or source_name.upper()
    tables = build_tables(contract)
    for table in tables:
        table["raw_model"] = f"raw_{source_name}_{table['name']}"

    try:
        rel_path = contract_path.relative_to(REPO_ROOT).as_posix()
    except ValueError:
        rel_path = contract_path.as_posix()

    header_lines = [
        f"# Seeded by ergasterion/import_odcs.py from ODCS contract: {rel_path}",
        f"#   id={contract.get('id')!r}  version={contract.get('version')!r}  domain={contract.get('domain')!r}",
        "#",
        "# This is a STARTING POINT, not regenerated output -- unlike ergasterion/emit.py's and",
        "# ergasterion/emit_contracts.py's outputs, this file is meant to be hand-edited. It",
        "# mechanically transcribes the contract's schema section (properties, logical/",
        "# physical types, keys) into projection stubs plus a seed_tests/model_tests",
        "# skeleton. What no ODCS contract carries -- vault_entities mapping,",
        "# entity_resolution config, survivorship stance -- is left as an explicit TODO",
        "# below and never guessed. Run ergasterion/emit.py once those TODOs are filled in.",
    ]

    return {
        "header": "\n".join(header_lines),
        "source": {
            "name": source_name,
            "display_name": display_name,
            "system": f"TODO: describe the {display_name} source system",
            "priority": priority,
        },
        "tables": tables,
    }


def render(context: dict[str, Any]) -> str:
    env = emit.template_env()
    return env.get_template(TEMPLATE_NAME).render(**context)


def seed_declaration(
    contract_path: Path,
    source_name: str | None = None,
    display_name: str | None = None,
    priority: int = 100,
) -> tuple[str, str]:
    """Pure function: load+validate the contract, build the seeded YAML text. Returns
    (source_name, yaml_text). Does not touch disk -- callers (main(), tests) decide
    where the text lands."""
    contract = load_and_validate_contract(contract_path)
    resolved_source = source_name or default_source_name(contract)
    context = build_context(contract, contract_path, resolved_source, display_name, priority)
    return resolved_source, render(context)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("contract", type=Path, help="Path to the supplier's ODCS v3.x contract YAML.")
    parser.add_argument(
        "--source", default=None,
        help="Declaration source name (declarations/<source>.yml). Defaults to a slug derived "
             "from the contract's id/name -- always confirm it matches the supplier's actual "
             "system name before committing.",
    )
    parser.add_argument("--display-name", default=None, help="Source display_name. Defaults to SOURCE.upper().")
    parser.add_argument(
        "--priority", type=int, default=100,
        help="Initial source.priority (default 100). No contract carries survivorship intent -- "
             "confirm this against the domain's other sources before relying on it.",
    )
    parser.add_argument(
        "--out", type=Path, default=None,
        help="Output path. Defaults to declarations/<source>.yml.",
    )
    parser.add_argument("--force", action="store_true", help="Overwrite an existing destination file.")
    parser.add_argument(
        "--estate-root", type=Path, default=None,
        help="Estate root whose declarations/ receives the seed (resolved from the environment or working directory when omitted).",
    )
    args = parser.parse_args()

    ctx = EstateContext.resolve(estate_root=args.estate_root)

    try:
        source_name, text = seed_declaration(args.contract, args.source, args.display_name, args.priority)
    except OdcsImportError as exc:
        print(f"FAIL: {exc}", file=sys.stderr)
        return 1

    out_path = args.out or (ctx.declarations_dir / f"{source_name}.yml")
    if out_path.exists() and not args.force:
        print(f"FAIL: {out_path} already exists -- pass --force to overwrite (never merges with hand edits).", file=sys.stderr)
        return 1

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(text, encoding="utf-8")
    print(f"seeded {out_path.relative_to(ctx.root).as_posix() if out_path.is_relative_to(ctx.root) else out_path}")
    print("Next: fill in the vault_entities / entity_resolution TODOs, then run ergasterion/emit.py.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
