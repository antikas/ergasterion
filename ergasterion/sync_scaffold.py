"""Sync the engine's packaged scaffold data from its in-tree sources.

``ergasterion init`` resolves every asset it copies from ``ergasterion/scaffold/``,
package-relative, so a non-editable install finds them inside its own wheel.
The files under ``ergasterion/scaffold/`` are GENERATED artefacts of this engine
tree -- the same byte-stable discipline as ``contracts/`` and ``graphs/`` --
projected from the live sources by this tool:

  dbt_project.yml            <- derived from the estate's own committed
                                dbt_project.yml: the engine-generic structural
                                top only (estate-only blocks stripped), with
                                target-path/log-path/packages-install-path/
                                clean-targets fixed under runtime/data/dbt/
  packages.yml               <- packages.yml, verbatim
  profiles.yml                <- derived from profiles/profiles.yml: the DuckDB
                                target's default path fixed at
                                runtime/data/ergasterion.duckdb
  targets/<name>.yml         <- declarations/targets/<name>.yml, verbatim
  macros/<name>.sql          <- the engine-generic macro allow-set, verbatim
  runtime/local.yml          <- derived: a real RuntimeBinding for the one
                                reference example (source=reference,
                                table=orders) GETTING-STARTED.md walks through --
                                all nine local ports, one parallel attempt, the
                                deterministic resource envelope, computed (never
                                hand-typed) contract/execution-plan digests
  .gitignore                 <- derived: ignores only runtime/data/, never the
                                binding itself
  estate.yml                  <- derived: a placeholder estate namespace
                                matching the reference example above, with
                                the two labels, adapters and translator
                                table the shipped examples route through
  products/<name>.yml        <- rendered from ergasterion/templates/
                                product_seed.yml.j2: the one product
                                declaration a new estate is seeded with,
                                which validates and emits through
                                `ergasterion emit-products` as it stands

Edit the SOURCE (the estate file, the macros tree, the target declarations) and
re-run this tool; never edit ``ergasterion/scaffold/`` by hand. The offline
validation chain runs ``--check`` so a drifted or hand-edited scaffold fails
the lane. The tool is anchored on the engine SOURCE tree and is meaningful only
in a checkout; an installed wheel carries the synced result.

Usage:
    python ergasterion/sync_scaffold.py            # regenerate ergasterion/scaffold/
    python ergasterion/sync_scaffold.py --check    # fail (exit 1) on any drift
"""

from __future__ import annotations

import argparse
import difflib
from pathlib import Path
from typing import Any

import yaml

if __package__ in (None, ""):
    import os as _os, sys as _sys
    _sys.path.insert(0, _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__))))

from ergasterion._repo_root import REPO_ROOT
from ergasterion.framework.landing_contract import (
    BackoffKind,
    LandingProductContract,
    ContentEncoding,
    DeleteStrategy,
    DeliveryMode,
    DeliveryPolicy,
    IntervalSchedule,
    JsonlCodec,
    LandingContract,
    LogicalIdentity,
    ManagedIntegration,
    NewlineKind,
    NotNullRule,
    OpaqueBatchProgress,
    ProductFacts,
    ProjectionField,
    QualityPolicy,
    RecordKeyContract,
    RetryPolicy,
    SimpleLogicalType,
    SlaContract,
    SourceField,
    TimestampContract,
)
from ergasterion.framework.models import compute_plan_digest
from ergasterion.framework.resolver import resolve
from ergasterion.ingestion.reference_runtime import contract_digest as runtime_contract_digest
from ergasterion.source_delivery import derive_interfaces
from ergasterion.translators.dbt import template_env
from ergasterion.translators.local_ingestion import build_local_binding

SCAFFOLD_DIR = Path(__file__).resolve().parent / "scaffold"

# The one worked example GETTING-STARTED.md walks a reader through end to end, and the
# identity `ergasterion/scaffold/runtime/local.yml`'s binding is computed against --
# never a real product; the placeholder namespace matches `estate.yml`'s own so the
# shipped scaffold is internally consistent out of the box.
REFERENCE_NAMESPACE = "com.example.ergasterion"
REFERENCE_SOURCE = "reference"
REFERENCE_TABLE = "orders"
# The estate layer label the reference example sits in. A production-classed table
# declares its label (source_delivery.validate_declared_layer); the runtime route looks
# that key up in estate.yml's translator table below, never infers it (owner ruling R1).
REFERENCE_LABEL = "reference"

# The one product declaration the scaffold seeds, rendered from
# ergasterion/templates/product/product_seed.yml.j2 (the product route's
# template directory). It reads the same reference
# identity the runtime binding above is computed against, so a fresh estate
# is internally consistent: the seeded product's fixture-bound source names
# the reference source and table, and the `derived` label estate.yml declares
# below owns every pattern its composition carries.
SEED_LABEL = "derived"
SEED_PROFILE = "derivation"
SEED_PRODUCT_DOMAIN = REFERENCE_SOURCE
SEED_PRODUCT_NAME = f"{REFERENCE_TABLE}_summary"
SEED_SOURCE_CONTRACT = f"{REFERENCE_SOURCE}.{REFERENCE_TABLE}@1"
SEED_SOURCE_RELATION = f"raw_{REFERENCE_SOURCE}_{REFERENCE_TABLE}"
SEED_KEY_FIELD = "order_id"
SEED_LOAD_FIELD = "loaded_at"
SEED_DERIVED_FIELD = "loaded_on"
SEED_OWNER = "replace-with-your-owning-team"
SEED_DESCRIPTION = "Scaffold walkthrough example -- replace with your own product before production use."
PRODUCT_SEED_TEMPLATE = "product/product_seed.yml.j2"
PRODUCT_SEED_DIRNAME = "products"

RUNTIME_BINDING_NOTE = (
    "# Generated by `ergasterion init`. This binding is computed for the ONE reference\n"
    "# example GETTING-STARTED.md walks through (source=reference, table=orders) -- once\n"
    "# you author your own Landing product contract, `ergasterion plan`/`contract register`\n"
    "# report its real contract_digest; copy that value (and logical_identity) in here.\n"
    "# The nine port bindings, scheduler, outbox, runtime_resources and retention stay --\n"
    "# they are the closed local reference profile, not product-specific.\n"
)

SCAFFOLD_GITIGNORE = (
    "# The runtime data root (SQLite, DuckDB, raw objects/receipts, scratch, dbt's own\n"
    "# target/logs/packages) -- never the binding above it. See runtime/local.yml.\n"
    "runtime/data/\n"
)

SCAFFOLD_ESTATE_YML = (
    "# The target-neutral estate root. `estate.namespace` is the mandatory production\n"
    "# qualifier for every Landing Product Contract's globally qualified identity\n"
    "# (estate_namespace, source.name, tables.<key>). Replace this placeholder with your\n"
    "# own reverse-DNS-style namespace before authoring a production contract -- a\n"
    "# seed-only estate never reads this file (ergasterion.source_delivery's\n"
    "# load_estate_namespace() only matters once a table's landing.kind is 'source').\n"
    "# See docs/specifications/landing-product-v1.md.\n"
    "#\n"
    "# `labels`, `profiles`, `adapters`, `final_target` and `translators` are this\n"
    "# estate's routing policy: for each layer label, the profiles a product may\n"
    "# compose under it and the translator that renders each pattern and each shape,\n"
    "# on which adapters. Two labels ship. The `reference` label covers exactly the\n"
    "# landing composition the shipped reference example uses, so `ergasterion plan`\n"
    "# and the other ingestion commands work out of the box. The label below it\n"
    "# covers the seeded product declaration under declarations/products/, so\n"
    "# `ergasterion emit-products` works out of the box too. A label is a key, never\n"
    "# a meaning: rename either, or add your own, and the engine follows the table.\n"
    "# `profiles` is optional and absent here -- both labels admit a profile the\n"
    "# engine ships; declare your own composition there when none of the five fits.\n"
    "estate:\n"
    f"  namespace: {REFERENCE_NAMESPACE}\n"
    "  # `support` and `team` are the estate's declared ownership: every product contract\n"
    "  # this estate publishes carries them, and the engine supplies no default. Replace\n"
    "  # both placeholders with your own support channel and owning team.\n"
    "  support: replace-with-your-support-channel\n"
    "  team: replace-with-your-owning-team\n"
    "  labels:\n"
    f"    {REFERENCE_LABEL}:\n"
    "      profiles: [landing]\n"
    f"    {SEED_LABEL}:\n"
    f"      profiles: [{SEED_PROFILE}]\n"
    "  adapters:\n"
    "    duckdb:\n"
    "      kind: reference\n"
    "    bigquery:\n"
    "      kind: deployment\n"
    "  final_target: bigquery\n"
    "  translators:\n"
    f"    {REFERENCE_LABEL}:\n"
    "      batch_ingestion: local-ingestion\n"
    "      data_validation: local-ingestion\n"
    "      data_publish: local-ingestion\n"
    "      checkpoint_retries: local-ingestion\n"
    "      data_contracts: publication\n"
    "      schema_publish: publication\n"
    "      metadata_capture: publication\n"
    "      lineage_capture: publication\n"
    f"    {SEED_LABEL}:\n"
    "      batch_transfer: dbt\n"
    "      data_validation: dbt\n"
    "      calculated_fields: dbt\n"
    "      data_publish: dbt\n"
    "      checkpoint_retries: dbt\n"
    "      declared: dbt\n"
    "      data_contracts: publication\n"
    "      schema_publish: publication\n"
    "      metadata_capture: publication\n"
    "      lineage_capture: publication\n"
)

GENERATED_NOTE = (
    "# Generated by `ergasterion init`: the engine-generic structural top only.\n"
    "# Add your own `models:` overrides as you introduce layers this estate doesn't have,\n"
    "# and a `seeds:` block once you add a seed -- seed `column_types` are authored\n"
    "# config with no generator (see GETTING-STARTED.md).\n"
)

# The project/profile name is fixed: macros/cross_db.sql dispatches every
# cross-adapter macro under this literal package name.
PROJECT_NAME = "ergasterion"

# The macros a scaffolded estate needs to build what the product route writes for it,
# and nothing else. The set is the audited union of every macro the route's rendered
# models and generated tests call, plus the source-delivery route's own two files:
#
#   cross_db.sql                the adapter-dispatch layer every rendered expression
#                               resolves its type and function tokens through;
#   data_vault.sql              the Data Vault shape's hash, watermark and
#                               unstored-key macros;
#   entity_resolution_scoring.sql  the curation pattern's match scoring;
#   filter_log.sql              the filtering occurrence's excluded-row count;
#   identifiers.sql             the adapter's own identifier quoting and the
#                               schema resolution a declared physical schema needs;
#   product_tests.sql           every generated test the route attaches;
#   publish.sql                 the publication timestamp and the incremental strategy;
#   quarantine.sql              the quarantine relation's generated tests;
#   survivorship.sql            the consolidation pattern's golden-record selection;
#   source_delivery.sql         the landing route's freshness and integrity SQL;
#   estate_evolution.sql        the landing route's delta window and its floor.
#
# One entry is here for a different reason. automate_dv_duckdb.sql calls nothing and is
# called by nothing in this engine: it supplies the DuckDB dispatch arms the AutomateDV
# package resolves through, and packages.yml declares that package, so a scaffolded
# estate that installs it cannot parse without them. It leaves the set on the day
# packages.yml stops declaring the package.
#
# Nothing this engine emits calls an AutomateDV macro any more, so that day is close.
# Dropping the package is not a documentation change, though: the offline lane, the
# wheel lane and the shared package cache all name it, and the cache's own lock has to
# agree with packages.yml, so the package, the two lane scripts and the cache move
# together or the offline lanes stop resolving.
#
# An estate's own named rules, its decision-log provisioning and any other estate-owned
# macro stay in the estate that declares them and are never copied by
# ``ergasterion init``.
SCAFFOLD_MACROS = (
    "automate_dv_duckdb.sql",
    "cross_db.sql",
    "data_vault.sql",
    "entity_resolution_scoring.sql",
    "estate_evolution.sql",
    "filter_log.sql",
    "identifiers.sql",
    "product_tests.sql",
    "publish.sql",
    "quarantine.sql",
    "source_delivery.sql",
    "survivorship.sql",
)

# Blocks the estate's own dbt_project.yml carries that are estate-specific: authored
# per-seed column types and the estate's own append-only-log bootstrap hooks.
ESTATE_ONLY_TOP_KEYS = ("seeds", "on-run-start")
ESTATE_ONLY_MODEL_LEAVES: tuple[str, ...] = ()

# The scaffold's own runtime data root (ergasterion/scaffold/.gitignore ignores it, never
# the tracked runtime/local.yml binding beside it). Every dbt working-directory path a
# scaffolded estate's commands touch is fixed under here, so partial-parse state, dbt's
# own logs and DPF_DBT_PACKAGE_CACHE-materialised Hub packages never land at the estate
# root or leak outside this one ignored directory.
RUNTIME_TARGET_PATH = "runtime/data/dbt/target"
RUNTIME_LOG_PATH = "runtime/data/dbt/logs"
RUNTIME_PACKAGES_PATH = "runtime/data/dbt/packages"
RUNTIME_DUCKDB_DEFAULT = "runtime/data/ergasterion.duckdb"
_ENGINE_DUCKDB_DEFAULT = "target/ergasterion.duckdb"


def derive_dbt_project_top() -> str:
    """The scaffold's dbt_project.yml, derived from the estate's committed file
    (the live SSOT) with the estate-only blocks stripped and every dbt working-directory
    path fixed under the tracked runtime/local.yml binding's data root, so a consumer
    starts from exactly the engine-generic layer config the emitters write into."""
    raw = yaml.safe_load((REPO_ROOT / "dbt_project.yml").read_text(encoding="utf-8"))
    for key in ESTATE_ONLY_TOP_KEYS:
        raw.pop(key, None)
    models_block = (raw.get("models") or {}).get(PROJECT_NAME)
    if isinstance(models_block, dict):
        for leaf in ESTATE_ONLY_MODEL_LEAVES:
            models_block.pop(leaf, None)
    if "target-path" not in raw:
        raise ValueError("dbt_project.yml: expected an existing 'target-path' key to override")
    raw["target-path"] = RUNTIME_TARGET_PATH
    raw["clean-targets"] = [RUNTIME_TARGET_PATH, RUNTIME_PACKAGES_PATH, RUNTIME_LOG_PATH]
    ordered: dict[str, Any] = {}
    for key, value in raw.items():
        ordered[key] = value
        if key == "target-path":
            ordered["log-path"] = RUNTIME_LOG_PATH
            ordered["packages-install-path"] = RUNTIME_PACKAGES_PATH
    body = yaml.safe_dump(ordered, sort_keys=False, default_flow_style=False, width=100)
    return GENERATED_NOTE + "\n" + body


def derive_scaffold_profiles() -> bytes:
    """The scaffold's profiles.yml, derived from the engine's own committed
    profiles/profiles.yml with the DuckDB target's default path repointed at
    runtime/data/ergasterion.duckdb (still DPF_DUCKDB_PATH-overridable) -- everything
    else (every other target, every comment) is byte-identical."""
    text = (REPO_ROOT / "profiles" / "profiles.yml").read_text(encoding="utf-8")
    old = f"'{_ENGINE_DUCKDB_DEFAULT}'"
    new = f"'{RUNTIME_DUCKDB_DEFAULT}'"
    if old not in text:
        raise ValueError(
            f"profiles/profiles.yml: expected the DuckDB default path {old} to override"
        )
    return text.replace(old, new, 1).encode("utf-8")


def _omit_nulls(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _omit_nulls(item) for key, item in value.items() if item is not None}
    if isinstance(value, list):
        return [_omit_nulls(item) for item in value]
    return value


def reference_contract() -> LandingProductContract:
    """The one worked reference example: a small, valid, production-classed Landing
    Product Contract (append-only, opaque_batch progress, no deletes) that
    ``ergasterion/scaffold/runtime/local.yml``'s binding is computed against and that
    GETTING-STARTED.md walks a reader through end to end. Never a real product -- every
    field is a documented placeholder."""
    return LandingProductContract(
        schema="ergasterion.landing-product/v1",
        logical_identity=LogicalIdentity(
            estate_namespace=REFERENCE_NAMESPACE, source=REFERENCE_SOURCE, table=REFERENCE_TABLE,
        ),
        product=ProductFacts(
            product_version="1.0.0",
            display_name="Reference orders",
            description="Scaffold walkthrough example -- replace with your own source before production use.",
            owner="local-process-user",
            domain="reference",
            support="local-process-user",
            classification="synthetic",
            access_policy_ref="local-process-user",
            retention_policy_ref="local-ephemeral",
        ),
        landing=LandingContract(
            kind="source",
            source_name=REFERENCE_SOURCE,
            identifier=REFERENCE_TABLE,
            integration=ManagedIntegration(kind="managed"),
            content_encodings=(ContentEncoding.IDENTITY,),
            codec=JsonlCodec(
                kind="jsonl", version=1, charset="utf-8", newline=NewlineKind.LF,
                top_level="object", duplicate_keys="reject", number_mode="exact_decimal",
                allow_blank_lines=False,
            ),
            physical_columns=(
                SourceField(name="order_id", logical_type=SimpleLogicalType.UTF8_STRING, nullable=False),
                SourceField(name="loaded_at", logical_type=SimpleLogicalType.UTC_INSTANT, nullable=False),
            ),
        ),
        delivery=DeliveryPolicy(
            kind="production",
            mode=DeliveryMode.APPEND_ONLY,
            progress=OpaqueBatchProgress(kind="opaque_batch"),
            delete_strategy=DeleteStrategy.NONE,
            schedule=IntervalSchedule(kind="interval", every_minutes=60, anchor_at="2026-01-01T00:00:00.000000Z"),
            schedule_lateness=SlaContract(warn_after_minutes=15, error_after_minutes=60),
            timestamps=TimestampContract(load_field="loaded_at"),
            record_key=RecordKeyContract(fields=("order_id",)),
            quality=QualityPolicy(
                publication_mode="publish_valid_rows", max_error_fraction="0.01",
                rules=(NotNullRule(kind="not_null", field="order_id", severity="error"),),
            ),
            retry=RetryPolicy(max_attempts=4, backoff=BackoffKind.EXPONENTIAL, base_seconds=5, cap_seconds=300),
        ),
        projection=(
            ProjectionField(source="order_id", name="order_id", logical_type=SimpleLogicalType.UTF8_STRING, nullable=False),
            ProjectionField(source="loaded_at", name="loaded_at", logical_type=SimpleLogicalType.UTC_INSTANT, nullable=False),
        ),
        interfaces=derive_interfaces(REFERENCE_SOURCE, REFERENCE_TABLE),
    )


def derive_runtime_local_binding() -> bytes:
    """``ergasterion/scaffold/runtime/local.yml``: a real, valid ``RuntimeBinding`` for
    ``reference_contract()``, computed through the same local-ingestion translator and
    contract-digest functions the runtime itself uses -- never a hand-typed digest.

    The binding's ``contract_digest`` field is compared, at ingest time, against
    ``ergasterion.ingestion.reference_runtime.contract_digest()`` (a plain
    ``canonical_digest(contract.model_dump(...))``) -- a DIFFERENT function from
    ``ergasterion.source_delivery.compute_contract_digest()`` (which wraps the same
    canonicalisation in a ``{schema, contract}`` envelope for the contract compiler's own
    derived-digest family). Using the wrong one here would make every `ingest file`
    against the shipped binding fail closed with `superseded_contract` on a byte-identical
    contract -- so this module reads the runtime's own function, never the compiler's."""
    contract = reference_contract()
    plan = resolve("landing")
    binding = build_local_binding(
        contract,
        execution_plan_digest=compute_plan_digest(plan),
        contract_digest=runtime_contract_digest(contract),
        binding_id="scaffold-reference",
    )
    document = _omit_nulls(binding.model_dump(mode="json", by_alias=True))
    body = yaml.safe_dump(document, sort_keys=False, default_flow_style=False, width=100)
    return (RUNTIME_BINDING_NOTE + "\n" + body).encode("utf-8")


def derive_product_seed() -> bytes:
    """``ergasterion/scaffold/products/<name>.yml``: the one product
    declaration ``ergasterion init`` seeds a new estate's
    ``declarations/products/`` with, rendered from
    ``ergasterion/templates/product/product_seed.yml.j2`` through the engine's
    own Jinja environment over the templates directory.

    Every value comes from the constants above, so the seeded product, the
    scaffold estate.yml's second label and the reference identity the runtime
    binding is computed against cannot drift apart."""

    template = template_env().get_template(PRODUCT_SEED_TEMPLATE)
    rendered = template.render(
        label=SEED_LABEL,
        profile=SEED_PROFILE,
        product_domain=SEED_PRODUCT_DOMAIN,
        product_name=SEED_PRODUCT_NAME,
        source_contract=SEED_SOURCE_CONTRACT,
        source_relation=SEED_SOURCE_RELATION,
        key_field=SEED_KEY_FIELD,
        load_field=SEED_LOAD_FIELD,
        derived_field=SEED_DERIVED_FIELD,
        owner=SEED_OWNER,
        description=SEED_DESCRIPTION,
    )
    return rendered.encode("utf-8")


def generated_set() -> dict[Path, bytes]:
    """Every scaffold file as (path under ergasterion/scaffold/ -> exact bytes)."""
    files: dict[Path, bytes] = {
        Path("dbt_project.yml"): derive_dbt_project_top().encode("utf-8"),
        Path("packages.yml"): (REPO_ROOT / "packages.yml").read_bytes(),
        Path("profiles.yml"): derive_scaffold_profiles(),
        Path("runtime/local.yml"): derive_runtime_local_binding(),
        Path(".gitignore"): SCAFFOLD_GITIGNORE.encode("utf-8"),
        Path("estate.yml"): SCAFFOLD_ESTATE_YML.encode("utf-8"),
        Path(PRODUCT_SEED_DIRNAME) / f"{SEED_PRODUCT_NAME}.yml": derive_product_seed(),
    }
    for target_file in sorted((REPO_ROOT / "declarations" / "targets").glob("*.yml")):
        files[Path("targets") / target_file.name] = target_file.read_bytes()
    for name in SCAFFOLD_MACROS:
        macro_file = REPO_ROOT / "macros" / name
        if not macro_file.is_file():
            raise FileNotFoundError(f"required scaffold macro is missing: {macro_file}")
        files[Path("macros") / name] = macro_file.read_bytes()
    return files


def sync(check: bool = False) -> int:
    files = generated_set()
    drifted: list[str] = []

    for rel, content in sorted(files.items()):
        dest = SCAFFOLD_DIR / rel
        existing = dest.read_bytes() if dest.exists() else None
        if existing == content:
            continue
        drifted.append(rel.as_posix())
        if check:
            before = (existing or b"").decode("utf-8", errors="replace").splitlines()
            after = content.decode("utf-8", errors="replace").splitlines()
            print("\n".join(difflib.unified_diff(before, after, fromfile=str(dest), tofile=str(dest))))
        else:
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_bytes(content)

    orphans: list[str] = []
    if SCAFFOLD_DIR.is_dir():
        expected = {SCAFFOLD_DIR / rel for rel in files}
        for path in sorted(SCAFFOLD_DIR.rglob("*")):
            if path.is_file() and path not in expected:
                orphans.append(path.relative_to(SCAFFOLD_DIR).as_posix())
                if not check:
                    path.unlink()

    action = "would change" if check else "synced"
    print(f"{action} {len(drifted)} of {len(files)} scaffold file(s)")
    for name in drifted:
        print(f"  {name}")
    print(f"SCAFFOLD_ORPHANS={len(orphans)}")
    for name in orphans:
        print(f"  orphan: {name}")

    if check and (drifted or orphans):
        print(
            "scaffold-sync FAIL: ergasterion/scaffold/ does not match its sources -- "
            "edit the source and run `python ergasterion/sync_scaffold.py`"
        )
        return 1
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="Fail on drift; write nothing.")
    args = parser.parse_args()
    return sync(check=args.check)


if __name__ == "__main__":
    raise SystemExit(main())
