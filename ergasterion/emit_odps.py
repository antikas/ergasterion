"""Emit one deterministic Bitol ODPS v1 data-product descriptor per declared product.

The Open Data Product Standard descriptor links each output port to a generated ODCS
contract. Support details and ownership come from declared repository configuration.

Outputs:
  - contracts/products/<domain>/<name>/<name>.odps.yml   one ODPS (Bitol) v1.0.0
                                        descriptor per declared product,
                                        regenerated-never-hand-edited, sibling to
                                        that product's ODCS contract documents;
  - contracts/landing/<...>/landing.odps.yml             one projection per bound
                                        production landing product.

Nothing here carries a timestamp, a UUID or an environment-derived fact: the same
declarations regenerate byte-identical output. Every emitted descriptor is validated
in-process against the vendored ODPS v1.0.0 JSON Schema
(schemas/odps-json-schema-v1.0.0.json) without network access.

Usage:
    python ergasterion/emit_odps.py            # write + validate every descriptor
    python ergasterion/emit_odps.py --check     # validate + fail if on-disk drifted / hand-edited
"""

from __future__ import annotations

import argparse
import difflib
import json
import re
from pathlib import Path
from typing import Any

import yaml
from jsonschema import Draft201909Validator

# Support installed-command and direct-script execution.
if __package__ in (None, ""):
    import os as _os, sys as _sys
    _sys.path.insert(0, _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__))))

from ergasterion import emit_contracts
from ergasterion.estate import EstateContext
from ergasterion.framework.generated import YAML_HEADER
from ergasterion.framework import contract as contract_mod
from ergasterion.source_delivery import load_typed_declarations
from ergasterion.translators.dbt import (
    bind_production_sources,
    landing_binding_dir,
    landing_odcs_id,
    landing_odps_id,
    landing_plan_digest,
    graph_contract_identity,
    load_runtime_bindings,
    reject_draft_generation,
)

# Ambient estate context; functions default to it, a caller threads its own via `ctx=`.
_DEFAULT_CTX = EstateContext.default()

# ESTATE paths -- ride the context. Aliases kept for back-compat reads.
REPO_ROOT = _DEFAULT_CTX.root
CONTRACTS_DIR = _DEFAULT_CTX.contracts_dir
LICENSE_PATH = _DEFAULT_CTX.license_path
# ENGINE DATA (the vendored ODPS JSON Schema) resolves package-relative -- shipped as
# package data inside ergasterion/, never estate-anchored (two-class principle).
SCHEMA_PATH = Path(__file__).resolve().parent / "schemas" / "odps-json-schema-v1.0.0.json"


API_VERSION = "v1.0.0"
KIND = "DataProduct"
# The generated marker has one text and one owner
# (``ergasterion.framework.generated``). The lines after it name the standard
# this file is written in and separate it from a similarly named one, which
# are facts about the format rather than about who wrote it.
GENERATED_HEADER = (
    f"{YAML_HEADER}\n"
    "# ODPS (Bitol) v1.0.0 data-product descriptor -- Open Data Product STANDARD\n"
    "# (github.com/bitol-io), unrelated to opendataproducts.org's Open Data Product\n"
    "# Specification.\n"
)

# Stable public source and support endpoint for generated descriptors.
REPO_URL = "https://github.com/antikas/ergasterion"


SUPPORT_CHANNELS: list[dict[str, Any]] = [
    {
        "channel": "GitHub Issues",
        "url": f"{REPO_URL}/issues",
        "description": "Repository issue tracker for questions, bugs, and source-onboarding requests.",
        "tool": "other",
        "scope": "issues",
    },
]

# The import seeder (ergasterion/import_odcs.py:build_context) writes exactly this two-line
# header onto a declaration file it seeds from an ODCS contract: a free-text "Seeded by"
# line, then id/version/domain in Python repr() form. This regex reads the second line
# back; see module docstring for why this is the sole "does this source have an
# imported/derived contract" signal today.
_SEEDED_HEADER_RE = re.compile(
    r"^#\s+id=(?P<id>'[^']*'|\"[^\"]*\")\s+version=(?P<version>'[^']*'|\"[^\"]*\")\s+domain=",
    re.MULTILINE,
)

# The holder is the text after the year, stopping before any trailing rights clause --
# both licence line shapes parse to the same holder ("Copyright (c) 2026 Jane Doe" and
# "Copyright (c) 2026 Jane Doe. All rights reserved." both yield "Jane Doe"), so the
# emitted team attribution is stable across a licence change.
_LICENSE_COPYRIGHT_RE = re.compile(
    r"Copyright \(c\) \d{4} (?P<holder>.+?)(?:\.\s*All rights reserved\.?)?\s*$",
    re.MULTILINE,
)


def load_schema_validator() -> Draft201909Validator:
    schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
    return Draft201909Validator(schema)


def team_from_license(ctx: EstateContext | None = None) -> dict[str, Any]:
    license_path = (ctx or _DEFAULT_CTX).license_path
    text = license_path.read_text(encoding="utf-8")
    match = _LICENSE_COPYRIGHT_RE.search(text)
    if not match:
        raise ValueError(f"{license_path}: no 'Copyright (c) <year> <holder>' line found -- team metadata source missing")
    holder = match.group("holder").strip()
    return {
        "name": "Ergasterion maintainers",
        "description": "Maintainer team, per this repository's LICENSE.",
        "members": [{"username": holder, "name": holder, "role": "maintainer"}],
    }


def dump_yaml(descriptor: dict[str, Any]) -> str:
    body = yaml.safe_dump(
        descriptor,
        sort_keys=False,
        default_flow_style=False,
        allow_unicode=True,
        width=100,
    )
    return GENERATED_HEADER + body


def validate_all(
    files: dict[Path, str], validator: Draft201909Validator, *, ctx: EstateContext | None = None
) -> list[str]:
    root = (ctx or _DEFAULT_CTX).root
    errors: list[str] = []
    for path in sorted(files):
        doc = yaml.safe_load(files[path])
        schema_errors = sorted(validator.iter_errors(doc), key=lambda e: list(e.absolute_path))
        for err in schema_errors:
            location = "/".join(str(p) for p in err.absolute_path) or "<root>"
            errors.append(f"{path.relative_to(root).as_posix()}: {location}: {err.message}")
    return errors


def build_landing_descriptor(typed_table, _binding, *, plan_digest: str) -> dict[str, Any]:
    """Static ODPS projection of one Landing product. No operational telemetry."""
    contract = typed_table.contract
    identity = graph_contract_identity(typed_table, plan_digest=plan_digest)
    odcs_id = landing_odcs_id(identity)
    product = contract.product
    return {
        "apiVersion": API_VERSION,
        "kind": KIND,
        "id": landing_odps_id(identity),
        "name": product.display_name,
        "version": product.product_version,
        "status": "active",
        "domain": product.domain,
        "description": {"purpose": product.description},
        "inputPorts": [
            {
                "name": contract.landing.source_name,
                "version": product.product_version,
                "contractId": odcs_id,
            }
        ],
        "outputPorts": [
            {
                "name": contract.interfaces.published,
                "description": product.description,
                "type": "tables",
                "version": product.product_version,
                "contractId": odcs_id,
            }
        ],
        "support": [
            {
                "channel": product.support,
                "url": f"https://example.invalid/support/{product.support}",
                "description": f"Support contact {product.support}.",
            }
        ],
        "team": {
            "name": product.owner,
            "description": f"Owner {product.owner}.",
            "members": [{"username": product.owner, "name": product.owner, "role": "owner"}],
        },
        "customProperties": [
            {"property": "dpf.identity", "value": identity},
            {"property": "dpf.classification", "value": product.classification},
            {"property": "dpf.accessPolicyRef", "value": product.access_policy_ref},
            {"property": "dpf.retentionPolicyRef", "value": product.retention_policy_ref},
        ],
    }


def generate_landing(
    ctx: EstateContext | None = None,
    *,
    binding_path: Path | None = None,
    environment: str | None = None,
) -> dict[Path, str]:
    ctx = ctx or _DEFAULT_CTX
    typed = load_typed_declarations(ctx)
    reject_draft_generation(typed)
    if not typed.production_contracts():
        return {}
    if binding_path is None or environment is None:
        raise ValueError(
            "production Landing ODPS generation needs --binding PATH and --environment NAME"
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
            / "landing.odps.yml"
        )
        files[path] = dump_yaml(build_landing_descriptor(typed_table, binding, plan_digest=plan_digest))
    return files


def write_landing_files(files: dict[Path, str], *, ctx: EstateContext | None = None) -> list[Path]:
    ctx = ctx or _DEFAULT_CTX
    landing_dir = ctx.contracts_dir / "landing"
    changed: list[Path] = []
    live = set(files)
    if landing_dir.exists():
        for existing in landing_dir.rglob("landing.odps.yml"):
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
        for existing in sorted(landing_dir.rglob("landing.odps.yml")):
            if existing not in live:
                problems.append(
                    f"ORPHAN (on disk, no longer generated): {existing.relative_to(root).as_posix()}"
                )
    return problems


# ============================================================================ product declarations
#
# One ODPS descriptor per declared product (architecture section 7,
# Metadata Capture at product level), built from the same product
# contracts ergasterion.emit_contracts.build_product_contracts() builds --
# never re-derived from a compiled dbt manifest.


_PRODUCT_PRUNE_PATTERNS = ("*.odps.yml",)


def generate_products(
    ctx: EstateContext | None = None,
    *,
    products_dir=None,
    landing_schemas: dict[str, contract_mod.RelationSchema] | None = None,
) -> dict[Path, str]:
    ctx = ctx or _DEFAULT_CTX
    contracts = emit_contracts.build_product_contracts(
        ctx, products_dir=products_dir, landing_schemas=landing_schemas
    )
    files: dict[Path, str] = {}
    for product_contract in contracts.values():
        base = ctx.contracts_dir / "products" / product_contract.identity.domain / product_contract.identity.name
        path = base / f"{product_contract.identity.name}.odps.yml"
        files[path] = contract_mod.dump_yaml(contract_mod.build_odps_document(product_contract))
    return files


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


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check", action="store_true",
        help="Validate + fail if any on-disk descriptor drifted / was hand-edited (no writes).",
    )
    parser.add_argument(
        "--estate-root", type=Path, default=None,
        help="Estate root to emit descriptors against (resolved from the environment or working directory when omitted).",
    )
    parser.add_argument(
        "--binding", type=Path, default=None,
        help="RuntimeBinding YAML file or directory for production Landing ODPS projections.",
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
        product_files = generate_products(ctx)
    except ValueError as error:
        print(f"FAIL: {error}")
        return 2

    landing_schema_errors = validate_all(landing_files, validator, ctx=ctx) if landing_files else []
    product_schema_errors = validate_all(product_files, validator, ctx=ctx) if product_files else []
    schema_errors = landing_schema_errors + product_schema_errors
    if schema_errors:
        print(f"ODPS (Bitol) schema-validation FAIL: {len(schema_errors)} error(s) against {SCHEMA_PATH.name}:")
        for err in schema_errors:
            print(f"  {err}")
        return 1
    print(
        f"ODPS (Bitol) schema-validation OK: {len(product_files)} declared product descriptor(s)"
        + (f" + {len(landing_files)} landing descriptor(s)" if landing_files else "")
        + f" valid against {SCHEMA_PATH.name}"
    )

    if args.check:
        problems = check_landing_files(landing_files, ctx=ctx) + check_product_files(product_files, ctx=ctx)
        if problems:
            print(f"ODPS (Bitol) descriptor gate FAIL: {len(problems)} on-disk divergence(s):")
            for problem in problems:
                print(problem)
            return 1
        print(
            f"ODPS (Bitol) descriptor gate OK: {len(product_files)} on-disk declared product "
            "descriptor(s) byte-match"
            + (f" and {len(landing_files)} landing descriptor(s) byte-match" if landing_files else "")
        )
        return 0

    landing_changed = write_landing_files(landing_files, ctx=ctx)
    product_changed = write_product_files(product_files, ctx=ctx)
    if landing_files or landing_changed:
        print(
            f"generated/updated {len([p for p in landing_changed if p in landing_files])} "
            f"of {len(landing_files)} landing descriptor(s)"
        )
        for path in sorted(landing_changed):
            marker = "wrote" if path in landing_files else "pruned"
            print(f"  {marker} {path.relative_to(ctx.root).as_posix()}")
    if product_files or product_changed:
        print(
            f"generated/updated {len([p for p in product_changed if p in product_files])} "
            f"of {len(product_files)} declared product descriptor(s)"
        )
        for path in sorted(product_changed):
            marker = "wrote" if path in product_files else "pruned"
            print(f"  {marker} {path.relative_to(ctx.root).as_posix()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
