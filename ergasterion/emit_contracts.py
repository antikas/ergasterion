"""Emit deterministic ODCS v3.1 data contracts for the estate's declared products.

Every contract is generated from a product declaration under
``declarations/products/`` and, for a bound production landing product, from its
Landing Product Contract. Nothing is read from a compiled dbt manifest and nothing is
hand-written: a contract is the interface a product publishes (architecture section 7).

Inputs (all read-only; the declarations stay SSOT):
  - declarations/products/**.yml  one product declaration per product.
  - declarations/*.yml            the source declarations a production landing
                                  delivery is compiled from.

Outputs:
  - contracts/products/<domain>/<name>/**   the ODCS documents, the product contract
                                  and its compliance check, one set per declared
                                  product, regenerated-never-hand-edited;
  - contracts/landing/<...>/landing.odcs.yml  one projection per bound production
                                  landing product.

Nothing here carries a timestamp, a UUID or an environment-derived fact, so the same
declarations regenerate byte-identical output. Every emitted contract is validated
in-process against the vendored ODCS v3.1 JSON Schema
(schemas/odcs-json-schema-v3.1.0.json) without network access.

Usage:
    python ergasterion/emit_contracts.py            # write + validate every contract
    python ergasterion/emit_contracts.py --check    # validate + fail if on-disk drifted / hand-edited
"""

from __future__ import annotations

import argparse
import difflib
import json
import re
import sys
from pathlib import Path
from typing import Any

import yaml
from jsonschema import Draft201909Validator, Draft202012Validator

# Support installed-command and direct-script execution.
if __package__ in (None, ""):
    import os as _os, sys as _sys
    _sys.path.insert(0, _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__))))

from ergasterion.estate import EstateContext
from ergasterion.framework.generated import YAML_HEADER
from ergasterion.framework import contract as contract_mod
from ergasterion.framework import declaration as declaration_mod
from ergasterion.source_delivery import load_typed_declarations
from ergasterion.translators.dbt import (
    bind_production_sources,
    landing_binding_dir,
    landing_odcs_id,
    landing_plan_digest,
    graph_contract_identity,
    landing_handle,
    load_runtime_bindings,
    reject_draft_generation,
)

# Ambient estate context; functions default to it, a caller threads its own via `ctx=`.
_DEFAULT_CTX = EstateContext.default()

# ESTATE paths -- ride the context. Aliases kept for back-compat reads.
REPO_ROOT = _DEFAULT_CTX.root
CONTRACTS_DIR = _DEFAULT_CTX.contracts_dir
# ENGINE DATA (the vendored ODCS JSON Schema) resolves package-relative -- shipped as
# package data inside ergasterion/, never estate-anchored (two-class principle).
SCHEMA_PATH = Path(__file__).resolve().parent / "schemas" / "odcs-json-schema-v3.1.0.json"
# ENGINE DATA (this engine's own product-contract projection schema).
PRODUCT_CONTRACT_SCHEMA_PATH = Path(__file__).resolve().parent / "schemas" / "product-contract-v1.schema.json"

API_VERSION = "v3.1.0"
KIND = "DataContract"
# The generated marker has one text and one owner
# (``ergasterion.framework.generated``), so a contract file and a model file
# say the same thing and every reader classifies both the same way. The line
# after it names the standard this file is written in, which is a fact about
# the format rather than about who wrote it.
GENERATED_HEADER = f"{YAML_HEADER}\n# ODCS v3.1.0 data contract.\n"


def load_schema_validator() -> Draft201909Validator:
    schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
    return Draft201909Validator(schema)


def dump_yaml(contract: dict[str, Any]) -> str:
    body = yaml.safe_dump(
        contract,
        sort_keys=False,
        default_flow_style=False,
        allow_unicode=True,
        width=100,
    )
    return GENERATED_HEADER + body


_ODCS_LOGICAL = {
    "boolean": "boolean",
    "int64": "integer",
    "utf8_string": "string",
    "date": "date",
    "utc_instant": "timestamp",
    "binary": "string",
    "decimal": "number",
    "local_datetime": "timestamp",
}


def _odcs_logical_type(logical_type: Any) -> str:
    from ergasterion.source_delivery import logical_type_token

    return _ODCS_LOGICAL[logical_type_token(logical_type)]


def _library_metric(metric: str, dimension: str, description: str) -> dict[str, Any]:
    return {
        "type": "library",
        "metric": metric,
        "mustBe": 0,
        "dimension": dimension,
        "description": description,
    }


def _custom_rule(implementation: str, dimension: str, description: str) -> dict[str, Any]:
    return {
        "type": "custom",
        "engine": "dbt",
        "implementation": implementation,
        "dimension": dimension,
        "description": description,
    }


def _landing_quality(contract: Any) -> list[dict[str, Any]]:
    """Product-intent quality policy. Run telemetry never enters this projection."""
    quality: list[dict[str, Any]] = []
    for rule in contract.delivery.quality.rules:
        kind = rule.kind
        if kind == "not_null":
            quality.append(_library_metric("nullValues", "completeness", f"not_null on {rule.field}"))
        elif kind == "unique_key":
            quality.append(
                _library_metric("duplicateValues", "uniqueness", f"unique_key on {list(rule.fields)}")
            )
        else:
            quality.append(
                _custom_rule(
                    f"landing quality rule {kind}",
                    "conformity",
                    f"Authored Landing quality rule kind={kind}.",
                )
            )
    return quality


def build_landing_contract(
    typed_table: Any, binding: Any, *, plan_digest: str
) -> dict[str, Any]:
    """Static ODCS v3.1 projection of one Landing Product Contract. Coordinates
    come only from the selected RuntimeBinding; no operational telemetry."""
    contract = typed_table.contract
    identity = graph_contract_identity(typed_table, plan_digest=plan_digest)
    landing = binding.landing_ports[landing_handle(contract)]
    relations = binding.projection_relations
    properties = []
    for field in contract.projection:
        prop: dict[str, Any] = {
            "name": field.name,
            "logicalType": _odcs_logical_type(field.logical_type),
        }
        if not field.nullable:
            prop["required"] = True
        properties.append(prop)

    schema_object: dict[str, Any] = {
        "name": contract.interfaces.published,
        "physicalName": relations.active_alias,
        "logicalType": "object",
        "physicalType": "table",
        "properties": properties,
    }
    quality = _landing_quality(contract)
    if quality:
        schema_object["quality"] = quality

    server_entry: dict[str, Any] = {
        "server": binding.binding_id,
        "type": "custom",
        "environment": binding.environment,
        "schema": relations.schema_ref,
    }
    database = relations.database_ref or landing.database_ref
    if database:
        server_entry["database"] = database

    lineage = [{"source": field.source, "name": field.name} for field in contract.projection]
    product = contract.product
    doc: dict[str, Any] = {
        "apiVersion": API_VERSION,
        "kind": KIND,
        "id": landing_odcs_id(identity),
        "version": product.product_version,
        "status": "active",
        "name": identity["table"],
        "domain": product.domain,
        "dataProduct": landing_odcs_id(identity),
        "description": {"purpose": product.description},
        "servers": [server_entry],
        "schema": [schema_object],
        "team": {"name": product.owner, "description": f"Owner {product.owner}."},
        "support": [
            {
                "channel": product.support,
                "url": f"https://example.invalid/support/{product.support}",
                "description": f"Support contact {product.support}.",
            }
        ],
        "customProperties": [
            {"property": "dpf.identity", "value": identity},
            {"property": "dpf.classification", "value": product.classification},
            {"property": "dpf.accessPolicyRef", "value": product.access_policy_ref},
            {"property": "dpf.retentionPolicyRef", "value": product.retention_policy_ref},
            {"property": "dpf.fieldLineage", "value": lineage},
        ],
    }
    return doc


def generate_landing(
    ctx: EstateContext | None = None,
    *,
    binding_path: Path | None = None,
    environment: str | None = None,
) -> dict[Path, str]:
    """Binding-specific ODCS projections under contracts/landing/**."""
    ctx = ctx or _DEFAULT_CTX
    typed = load_typed_declarations(ctx)
    reject_draft_generation(typed)
    if not typed.production_contracts():
        return {}
    if binding_path is None or environment is None:
        raise ValueError(
            "production Landing ODCS generation needs --binding PATH and --environment NAME"
        )
    bindings = load_runtime_bindings(binding_path, environment)
    plan_digest = landing_plan_digest()
    bound = bind_production_sources(typed, bindings)
    files: dict[Path, str] = {}
    for key, typed_table in typed.tables.items():
        if typed_table.kind != "production":
            continue
        binding = bound[key]
        path = (
            landing_binding_dir(ctx.contracts_dir, typed_table.contract.logical_identity, binding.binding_id)
            / "landing.odcs.yml"
        )
        files[path] = dump_yaml(build_landing_contract(typed_table, binding, plan_digest=plan_digest))
    return files


def _is_landing_path(path: Path, contracts_dir: Path) -> bool:
    landing = (contracts_dir / "landing").resolve()
    try:
        return path.resolve().is_relative_to(landing)
    except (OSError, ValueError):
        return False


def validate_all(
    files: dict[Path, str], validator: Draft201909Validator, *, ctx: EstateContext | None = None
) -> list[str]:
    """Validate every generated contract against the vendored ODCS v3.1 JSON Schema."""
    root = (ctx or _DEFAULT_CTX).root
    errors: list[str] = []
    for path in sorted(files):
        doc = yaml.safe_load(files[path])
        schema_errors = sorted(validator.iter_errors(doc), key=lambda e: list(e.absolute_path))
        for err in schema_errors:
            location = "/".join(str(p) for p in err.absolute_path) or "<root>"
            errors.append(f"{path.relative_to(root).as_posix()}: {location}: {err.message}")
    return errors


def write_landing_files(files: dict[Path, str], *, ctx: EstateContext | None = None) -> list[Path]:
    ctx = ctx or _DEFAULT_CTX
    landing_dir = ctx.contracts_dir / "landing"
    changed: list[Path] = []
    live = set(files)
    if landing_dir.exists():
        for existing in landing_dir.rglob("landing.odcs.yml"):
            if existing not in live:
                changed.append(existing)
                existing.unlink()
    for path in sorted(files):
        content = files[path]
        current = path.read_text(encoding="utf-8") if path.exists() else None
        if current == content:
            continue
        changed.append(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content.encode("utf-8"))
    return changed


def check_landing_files(files: dict[Path, str], *, ctx: EstateContext | None = None) -> list[str]:
    ctx = ctx or _DEFAULT_CTX
    landing_dir = ctx.contracts_dir / "landing"
    root = ctx.root
    problems: list[str] = []
    for path in sorted(files):
        rel = path.relative_to(root).as_posix()
        if not path.exists():
            problems.append(f"MISSING (never generated on disk): {rel}")
            continue
        current = path.read_text(encoding="utf-8")
        if current != files[path]:
            diff = "\n".join(
                difflib.unified_diff(
                    current.splitlines(), files[path].splitlines(),
                    fromfile=f"{rel} (on disk)", tofile=f"{rel} (regenerated)", lineterm="",
                )
            )
            problems.append(f"DRIFT (hand-edited or stale): {rel}\n{diff}")
    if landing_dir.exists():
        live = set(files)
        for existing in sorted(landing_dir.rglob("landing.odcs.yml")):
            if existing not in live:
                problems.append(
                    f"ORPHAN (on disk, no longer generated): {existing.relative_to(root).as_posix()}"
                )
    return problems


# ============================================================================ product declarations
#
# The contract pipe for the product-declaration format (architecture
# sections 4, 7; ergasterion.framework.declaration/.contract): every
# declared product under `declarations/products/` (or a caller-supplied
# directory, for fixture-driven tests) gets one generated contract, one
# ODCS document per relation its shape renders, and the contract
# compliance check. Distinct from the manifest-driven route above: that
# route stays the sole generator for dbt-rendered served tables until a
# later item wires dbt's transformation patterns through; this route reads
# only the declaration, the estate's ownership policy and, for a landing
# product, the Landing Product Contract the ingestion side already builds.


def load_product_contract_schema_validator() -> Draft202012Validator:
    schema = json.loads(PRODUCT_CONTRACT_SCHEMA_PATH.read_text(encoding="utf-8"))
    return Draft202012Validator(schema)


class DuplicateProductError(ValueError):
    """Two product declarations under the same ``products_dir`` claim the
    same published name (``domain.name``). Names both files."""

    def __init__(self, *, published_name: str, first: Path, second: Path) -> None:
        self.published_name = published_name
        self.first = first
        self.second = second
        super().__init__(
            f"published name {published_name!r} is claimed by both {first} and {second}"
        )


def _iter_product_documents(products_dir: Path) -> list[tuple[Path, dict]]:
    if not products_dir.is_dir():
        return []
    paths = sorted(set(products_dir.rglob("*.yml")) | set(products_dir.rglob("*.yaml")))
    return [(path, yaml.safe_load(path.read_text(encoding="utf-8")) or {}) for path in paths]


def landing_schemas_from_typed_declarations(ctx: EstateContext) -> dict[str, contract_mod.RelationSchema]:
    """Every production landing contract's typed schema, keyed by the
    published name (``domain.published-interface-name``) a landing
    product declaration under ``declarations/products/`` names to claim
    it -- the ingestion side's own typed schema, never re-derived here
    (``ergasterion.framework.contract.relation_schema_from_landing_
    projection``)."""

    typed = load_typed_declarations(ctx)
    schemas: dict[str, contract_mod.RelationSchema] = {}
    for landing_contract in typed.production_contracts():
        published_name = f"{landing_contract.product.domain}.{landing_contract.interfaces.published}"
        schemas[published_name] = contract_mod.relation_schema_from_landing_projection(
            domain=landing_contract.product.domain,
            name=landing_contract.interfaces.published,
            projection=landing_contract.projection,
        )
    return schemas


def build_product_contracts(
    ctx: EstateContext | None = None,
    *,
    products_dir: Path | None = None,
    landing_schemas: dict[str, contract_mod.RelationSchema] | None = None,
) -> dict[str, contract_mod.ProductContract]:
    """Every declared product's contract, keyed by its published name
    (``domain.name``). Each document is validated against layer 1
    (``ergasterion.framework.declaration.validate_declaration``); two
    documents claiming the same published name fail closed with
    ``DuplicateProductError`` naming both files. Every product's relation
    schema is then resolved together, in dependency order
    (``ergasterion.framework.contract.resolve_relation_registry``): a
    landing product's schema comes from ``landing_schemas`` (computed from
    the estate's real Landing Product Contracts when not supplied -- a test
    may inject a synthetic mapping directly instead), a fixture-bound
    source's schema comes from the fields its binding declares, every other
    product's schema is propagated through its composition from the schemas
    of the contracts its ``sources`` name, and each product's shape then
    says which relations that schema publishes. The contract lists every one
    of them. A missing
    ``products_dir`` yields no products (a fresh estate with none declared
    yet is valid, the same convention
    ``ergasterion.framework.declaration.validate_estate_products`` uses)."""
    ctx = ctx or _DEFAULT_CTX
    directory = products_dir if products_dir is not None else ctx.declarations_dir / "products"
    policy = declaration_mod.load_estate_policy(ctx.estate_file)
    ownership = contract_mod.load_estate_ownership(ctx.estate_file)
    if landing_schemas is None:
        landing_schemas = landing_schemas_from_typed_declarations(ctx)

    entries: dict[str, tuple[dict, declaration_mod.ValidatedProduct]] = {}
    claimed_by: dict[str, Path] = {}
    for path, document in _iter_product_documents(directory):
        validated = declaration_mod.validate_declaration(document, policy=policy)
        published_name = f"{validated.domain}.{validated.name}"
        if published_name in claimed_by:
            raise DuplicateProductError(published_name=published_name, first=claimed_by[published_name], second=path)
        claimed_by[published_name] = path
        entries[published_name] = (document, validated)

    # Every schema a composition can start from that no product in this
    # estate derives: the fixture-bound sources' declared fields beside the
    # ingestion side's landing schemas (architecture section 12). The product
    # route opens its compositions from exactly the same union, so a
    # fixture-bound product resolves identically here and there.
    opening_schemas = dict(contract_mod.fixture_relation_schemas(entries))
    opening_schemas.update(landing_schemas)
    relation_registry = contract_mod.resolve_relation_registry(entries, landing_schemas=opening_schemas)

    result: dict[str, contract_mod.ProductContract] = {}
    for published_name, (document, validated) in entries.items():
        result[published_name] = contract_mod.build_product_contract(
            document,
            validated,
            ownership=ownership,
            relations=tuple(
                relation.schema for relation in relation_registry[published_name]
            ),
        )
    return result


def _product_base_dir(ctx: EstateContext, contract: contract_mod.ProductContract) -> Path:
    return ctx.contracts_dir / "products" / contract.identity.domain / contract.identity.name


_PRODUCT_PRUNE_PATTERNS = ("*.odcs.yml", "contract.json", "contract-compliance.json")


def generate_products(
    ctx: EstateContext | None = None,
    *,
    products_dir: Path | None = None,
    landing_schemas: dict[str, contract_mod.RelationSchema] | None = None,
) -> dict[Path, str]:
    """Build the full {path: text} set of product-declaration-driven
    artefacts: the contract's own JSON projection, one ODCS document per
    relation, and the contract compliance check."""
    ctx = ctx or _DEFAULT_CTX
    contracts = build_product_contracts(ctx, products_dir=products_dir, landing_schemas=landing_schemas)
    return _product_files_from_contracts(ctx, contracts)


def validate_products(
    files: dict[Path, str],
    odcs_validator: Draft201909Validator,
    contract_validator: Draft202012Validator,
    *,
    ctx: EstateContext | None = None,
) -> list[str]:
    """Validate every generated product-declaration-driven artefact: the
    ``contract.json`` files against ``product-contract-v1.schema.json``,
    the ``*.odcs.yml`` files against the vendored ODCS v3.1 schema. The
    compliance-check description carries no schema of its own -- it is a
    plain artefact, not a served contract."""
    root = (ctx or _DEFAULT_CTX).root
    errors: list[str] = []
    for path in sorted(files):
        if path.name == "contract.json":
            doc = json.loads(files[path])
            validator = contract_validator
        elif path.name.endswith(".odcs.yml"):
            doc = yaml.safe_load(files[path])
            validator = odcs_validator
        else:
            continue
        schema_errors = sorted(validator.iter_errors(doc), key=lambda e: list(e.absolute_path))
        for err in schema_errors:
            location = "/".join(str(p) for p in err.absolute_path) or "<root>"
            errors.append(f"{path.relative_to(root).as_posix()}: {location}: {err.message}")
    return errors


def write_product_files(files: dict[Path, str], *, ctx: EstateContext | None = None) -> list[Path]:
    ctx = ctx or _DEFAULT_CTX
    return contract_mod.write_generated_files(
        files, directory=ctx.contracts_dir / "products", prune_patterns=_PRODUCT_PRUNE_PATTERNS
    )


def check_product_files(files: dict[Path, str], *, ctx: EstateContext | None = None) -> list[str]:
    ctx = ctx or _DEFAULT_CTX
    return contract_mod.check_generated_files(
        files, directory=ctx.contracts_dir / "products", root=ctx.root, prune_patterns=_PRODUCT_PRUNE_PATTERNS
    )


def _product_files_from_contracts(
    ctx: EstateContext, contracts: dict[str, contract_mod.ProductContract]
) -> dict[Path, str]:
    files: dict[Path, str] = {}
    for product_contract in contracts.values():
        base = _product_base_dir(ctx, product_contract)
        files[base / "contract.json"] = contract_mod.dump_json(contract_mod.contract_document(product_contract))
        for relation in product_contract.relations:
            relation_short_name = relation.name.rsplit(".", 1)[-1]
            files[base / f"{relation_short_name}.odcs.yml"] = contract_mod.dump_yaml(
                contract_mod.build_odcs_document(product_contract, relation)
            )
        files[base / "contract-compliance.json"] = contract_mod.dump_json(
            contract_mod.build_compliance_check(product_contract)
        )
    return files


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check", action="store_true",
        help="Validate + fail if any on-disk contract drifted / was hand-edited (no writes).",
    )
    parser.add_argument(
        "--estate-root", type=Path, default=None,
        help="Estate root to emit contracts against (resolved from the environment or working directory when omitted).",
    )
    parser.add_argument(
        "--binding", type=Path, default=None,
        help="RuntimeBinding YAML file or directory for production Landing ODCS projections.",
    )
    parser.add_argument(
        "--environment", default=None,
        help="Mandatory matching assertion against RuntimeBinding.environment.",
    )
    args = parser.parse_args(argv)

    ctx = EstateContext.resolve(estate_root=args.estate_root)
    validator = load_schema_validator()
    try:
        landing_files = generate_landing(ctx, binding_path=args.binding, environment=args.environment)
    except ValueError as error:
        print(f"FAIL: {error}")
        return 2

    try:
        product_contracts = build_product_contracts(ctx)
    except ValueError as error:
        print(f"FAIL: {error}")
        return 2
    product_files = _product_files_from_contracts(ctx, product_contracts)

    contract_validator = load_product_contract_schema_validator()
    landing_schema_errors = validate_all(landing_files, validator, ctx=ctx) if landing_files else []
    product_schema_errors = validate_products(product_files, validator, contract_validator, ctx=ctx)
    schema_errors = landing_schema_errors + product_schema_errors
    if schema_errors:
        print(f"ODCS schema-validation FAIL: {len(schema_errors)} error(s) against {SCHEMA_PATH.name}:")
        for err in schema_errors:
            print(f"  {err}")
        return 1
    print(
        f"ODCS schema-validation OK: {len(product_contracts)} declared product contract(s)"
        + (f" + {len(landing_files)} landing contract(s)" if landing_files else "")
        + f" valid against {SCHEMA_PATH.name}"
    )

    if args.check:
        problems = check_landing_files(landing_files, ctx=ctx) + check_product_files(product_files, ctx=ctx)
        if problems:
            print(f"ODCS contract gate FAIL: {len(problems)} on-disk divergence(s):")
            for problem in problems:
                print(problem)
            return 1
        print(
            f"ODCS contract gate OK: {len(product_contracts)} on-disk declared product "
            "contract(s) byte-match"
            + (f" and {len(landing_files)} landing contract(s) byte-match" if landing_files else "")
        )
        return 0

    landing_changed = write_landing_files(landing_files, ctx=ctx)
    product_changed = write_product_files(product_files, ctx=ctx)
    if landing_files or landing_changed:
        print(
            f"generated/updated {len([p for p in landing_changed if p in landing_files])} "
            f"of {len(landing_files)} landing contract(s)"
        )
        for path in sorted(landing_changed):
            marker = "wrote" if path in landing_files else "pruned"
            print(f"  {marker} {path.relative_to(ctx.root).as_posix()}")
    if product_files or product_changed:
        print(
            f"generated/updated {len([p for p in product_changed if p in product_files])} "
            f"of {len(product_files)} declared product artefact(s)"
        )
        for path in sorted(product_changed):
            marker = "wrote" if path in product_files else "pruned"
            print(f"  {marker} {path.relative_to(ctx.root).as_posix()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
