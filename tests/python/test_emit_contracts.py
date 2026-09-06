"""Self-tests for ergasterion/emit_contracts.py (the ODCS v3.1.0 contract adapter).

No pytest in this repo's .venv, so each test_* raises AssertionError on failure and
main() runs them all and reports PASS/FAIL (exit 0 = all green, 1 = any failure).

Covers the two routes this module carries:
  - the landing route: identity, schema validity and the fail-closed branches of an
    ODCS projection of a bound production Landing product;
  - the product route: contracts built from declarations/products/ alone, estate
    ownership failing closed, duplicate product names failing closed, and the
    write-then-check cycle leaving no orphan behind.

Usage:
    python tests/python/test_emit_contracts.py
"""

from __future__ import annotations

import copy
import json
import re
import sys
import tempfile
import traceback
from pathlib import Path

import yaml

# Allow direct execution as `python tests/python/test_emit_contracts.py`.
if __package__ in (None, ""):
    import os as _os, sys as _sys
    _sys.path.insert(0, _os.path.dirname(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))))

from ergasterion import emit_contracts as ec
from ergasterion.estate import EstateContext
from ergasterion.source_delivery import load_typed_declarations
from ergasterion.translators.dbt import (
    bind_production_sources,
    landing_odcs_id,
    landing_plan_digest,
    graph_contract_identity,
    landing_handle,
    load_runtime_bindings,
)


_FIXTURES = Path(__file__).resolve().parents[1] / "fixtures"
_TELEMETRY = ("heartbeat", "committed_at", "attempt_id", "run_id", "evaluated_through")


def _vector_payload(case: str) -> dict:
    data = json.loads((_FIXTURES / "source_delivery_vectors.json").read_text(encoding="utf-8"))
    for entry in data["positive"]:
        if entry["case"] == case:
            return copy.deepcopy(entry["payload"])
    raise AssertionError(case)


def _binding_template() -> dict:
    data = json.loads((_FIXTURES / "landing_schema_vectors.json").read_text(encoding="utf-8"))
    for entry in data["positive"]:
        if entry.get("record") == "RuntimeBinding":
            return copy.deepcopy(entry["payload"])
    raise AssertionError("RuntimeBinding")


def _write_production_estate(root: Path, payload: dict):
    ident = payload["logical_identity"]
    (root / "estate.yml").write_text(
        yaml.safe_dump({"estate": {"namespace": ident["estate_namespace"]}}, sort_keys=False),
        encoding="utf-8",
    )
    decls = root / "declarations"
    decls.mkdir()
    # The table's domain is one of the product block's own declared facts.
    product = dict(payload["product"])
    (decls / f"{ident['source']}.yml").write_text(
        yaml.safe_dump(
            {
                "source": {"name": ident["source"], "display_name": ident["source"].upper(), "priority": 10},
                "tables": {
                    ident["table"]: {
                        "layer": "bronze",
                        "landing": payload["landing"],
                        "product": product,
                        "delivery": payload["delivery"],
                        "projection": payload["projection"],
                    }
                },
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    return EstateContext.resolve(estate_root=root)


def _bind(root: Path, ctx, environment: str = "local"):
    typed = load_typed_declarations(ctx)
    table = next(iter(typed.tables.values()))
    payload = _binding_template()
    ident = table.contract.logical_identity
    handle = landing_handle(table.contract)
    payload["logical_identity"] = {
        "estate_namespace": ident.estate_namespace,
        "source": ident.source,
        "table": ident.table,
    }
    payload["contract_digest"] = table.contract_digest
    payload["execution_plan_digest"] = landing_plan_digest()
    payload["environment"] = environment
    payload["landing_ports"] = {handle: payload["landing_ports"]["raw"]}
    binding_path = root / "runtime.yml"
    binding_path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    bound = bind_production_sources(typed, load_runtime_bindings(binding_path, environment))
    return typed, bound, binding_path


def test_landing_odcs_identity_and_schema_for_managed_and_external() -> None:
    validator = ec.load_schema_validator()
    for case in ("append_only_managed_opaque_batch", "csv_external_append_only"):
        payload = _vector_payload(case)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            ctx = _write_production_estate(root, payload)
            typed, _bound, binding_path = _bind(root, ctx)
            files = ec.generate_landing(ctx, binding_path=binding_path, environment="local")
            assert len(files) == 1, files
            errors = ec.validate_all(files, validator, ctx=ctx)
            assert not errors, "\n".join(errors)
            path, text = next(iter(files.items()))
            assert path.as_posix().endswith("/landing.odcs.yml"), path
            assert "/contracts/landing/" in path.as_posix().replace("\\", "/")
            for token in _TELEMETRY:
                assert token not in text, f"{case} leaked {token}"
            doc = yaml.safe_load(text)
            table = next(iter(typed.tables.values()))
            identity = graph_contract_identity(table)
            assert doc["id"] == landing_odcs_id(identity)
            found = next(item["value"] for item in doc["customProperties"] if item["property"] == "dpf.identity")
            assert found == identity
            assert doc["customProperties"]
            assert all(
                item["property"] in {
                    "dpf.identity",
                    "dpf.classification",
                    "dpf.accessPolicyRef",
                    "dpf.retentionPolicyRef",
                    "dpf.fieldLineage",
                }
                for item in doc["customProperties"]
            )


def _write_draft_estate(root: Path, *, with_seed: bool = False):
    (root / "estate.yml").write_text(
        yaml.safe_dump({"estate": {"namespace": "scratch.estate"}}, sort_keys=False),
        encoding="utf-8",
    )
    domains = root / "domains"
    domains.mkdir()
    (domains / "ops.yml").write_text("{}\n", encoding="utf-8")
    decls = root / "declarations"
    decls.mkdir()
    tables = {
        "live_things": {
            "landing": {
                "kind": "source",
                "source_name": "warehouse_feed",
                "identifier": "things_live",
            },
            "delivery": {"kind": "draft", "reason": "delivery_contract_required"},
        }
    }
    if with_seed:
        tables["seed_things"] = {
            "raw_model": "raw_scratch_seed_things",
            "landing": {"kind": "seed"},
            "projection": [{"name": "id", "expression": "id"}],
        }
    (decls / "scratch.yml").write_text(
        yaml.safe_dump(
            {
                "source": {"name": "scratch", "display_name": "SCRATCH", "priority": 10},
                "tables": tables,
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    return EstateContext.resolve(estate_root=root)


def test_bronze_odcs_requires_binding_when_production_exists() -> None:
    payload = _vector_payload("append_only_managed_opaque_batch")
    with tempfile.TemporaryDirectory() as tmp:
        ctx = _write_production_estate(Path(tmp), payload)
        try:
            ec.generate_landing(ctx)
        except ValueError as exc:
            assert "--binding" in str(exc), str(exc)
        else:
            raise AssertionError("production ODCS generation must require a binding")


def test_bronze_odcs_draft_only_fails_closed() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        ctx = _write_draft_estate(Path(tmp))
        try:
            files = ec.generate_landing(ctx)
        except ValueError as exc:
            assert "delivery_contract_required" in str(exc), str(exc)
            assert "draft delivery cannot generate" in str(exc), str(exc)
        else:
            raise AssertionError(f"draft-only ODCS generation must fail closed, got {files!r}")


def test_bronze_odcs_seed_plus_draft_fails_closed() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        ctx = _write_draft_estate(Path(tmp), with_seed=True)
        try:
            files = ec.generate_landing(ctx)
        except ValueError as exc:
            assert "delivery_contract_required" in str(exc), str(exc)
            assert "draft delivery cannot generate" in str(exc), str(exc)
        else:
            raise AssertionError(f"seed-plus-draft ODCS generation must fail closed, got {files!r}")


def test_bronze_odcs_mismatched_plan_digest_fails_bind() -> None:
    payload = _vector_payload("append_only_managed_opaque_batch")
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        ctx = _write_production_estate(root, payload)
        _typed, _bound, binding_path = _bind(root, ctx)
        data = yaml.safe_load(binding_path.read_text(encoding="utf-8"))
        data["execution_plan_digest"] = "0" * 64
        binding_path.write_text(yaml.safe_dump(data, sort_keys=False), encoding="utf-8")
        try:
            files = ec.generate_landing(ctx, binding_path=binding_path, environment="local")
        except ValueError as exc:
            assert "execution_plan_digest" in str(exc), str(exc)
            assert "resolved Landing graph" in str(exc), str(exc)
        else:
            raise AssertionError(
                f"mismatched execution_plan_digest must fail Landing ODCS bind, got {files!r}"
            )


_PRODUCT_FIXTURES_DIR = _FIXTURES / "products" / "valid"

# The typed landing schema `ecommerce.crm_customer_landing` needs to resolve
# (its real source would be a Landing Product Contract's projection --
# ergasterion.emit_contracts.landing_schemas_from_typed_declarations); this
# fixture-only estate injects it directly rather than authoring a full
# legacy Landing declaration just to exercise the product-declaration route.
from ergasterion.framework.contract import RelationField, RelationSchema  # noqa: E402

_PRODUCT_LANDING_SCHEMAS = {
    "ecommerce.crm_customer_landing": RelationSchema(
        "ecommerce.crm_customer_landing",
        (
            RelationField("customer_id", "string", True),
            RelationField("email", "string", False),
            RelationField("status_code", "string", False),
            RelationField("cust_nm", "string", False),
            RelationField("crtd_dt", "string", False),
            RelationField("tag_list", "string", False),
        ),
    ),
    "ecommerce.customer_segment": RelationSchema(
        "ecommerce.customer_segment",
        (
            RelationField("segment_code", "string", False),
            RelationField("segment_name", "string", False),
        ),
    ),
    "ecommerce.storefront_customer": RelationSchema(
        "ecommerce.storefront_customer",
        (
            RelationField("customer_id", "string", True),
            RelationField("email", "string", False),
            RelationField("channel", "string", False),
        ),
    ),
}


def test_generate_products_estate_ownership_fails_closed_without_support_and_team() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        (root / "estate.yml").write_text(
            yaml.safe_dump({"estate": {"namespace": "io.antikas.scratch"}}, sort_keys=False), encoding="utf-8"
        )
        ctx = EstateContext.resolve(estate_root=root)
        try:
            ec.generate_products(ctx, products_dir=_PRODUCT_FIXTURES_DIR, landing_schemas=_PRODUCT_LANDING_SCHEMAS)
        except ValueError as exc:
            assert "support" in str(exc) and "team" in str(exc)
        else:
            raise AssertionError("expected estate ownership to fail closed without support/team")


def test_build_product_contracts_rejects_two_files_claiming_the_same_name() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        products_dir = Path(tmp)
        document = yaml.safe_load((_PRODUCT_FIXTURES_DIR / "landing.yml").read_text(encoding="utf-8"))
        (products_dir / "one.yml").write_text(yaml.safe_dump(document, sort_keys=False), encoding="utf-8")
        (products_dir / "two.yml").write_text(yaml.safe_dump(document, sort_keys=False), encoding="utf-8")
        try:
            ec.build_product_contracts(products_dir=products_dir, landing_schemas=_PRODUCT_LANDING_SCHEMAS)
        except ec.DuplicateProductError as exc:
            assert exc.published_name == "ecommerce.crm_customer_landing"
        else:
            raise AssertionError("expected DuplicateProductError for two files claiming one published name")


def test_main_wires_the_declared_product_route_through_check() -> None:
    """Blind-review correction: main() must actually call the declaration-
    driven route (generate_products/validate_products/check_product_files/
    cross_check_products_against_manifest), never leave it reachable only
    from tests. Proven with --check (read-only, safe against the real
    committed contracts/ tree) against the real committed estate (which
    already carries a parsed dbt manifest for the legacy route);
    build_product_contracts is patched to return one concrete contract so
    main()'s own wiring has something to check on disk -- the gate must
    fail closed on the missing product artefacts, and the summary line
    must name the declared product contract."""
    import io
    import contextlib
    import sys

    ctx = ec.EstateContext.default()
    ownership = ec.contract_mod.load_estate_ownership(ctx.estate_file)
    policy = ec.declaration_mod.load_estate_policy(ctx.estate_file)
    document = yaml.safe_load((_PRODUCT_FIXTURES_DIR / "landing.yml").read_text(encoding="utf-8"))
    validated = ec.declaration_mod.validate_declaration(document, policy=policy)
    relation = _PRODUCT_LANDING_SCHEMAS["ecommerce.crm_customer_landing"]
    contract = ec.contract_mod.build_product_contract(
        document, validated, ownership=ownership, relations=(relation,)
    )

    original = ec.build_product_contracts
    ec.build_product_contracts = lambda *a, **k: {"ecommerce.crm_customer_landing": contract}
    original_argv = sys.argv
    captured = io.StringIO()
    try:
        sys.argv = ["emit_contracts", "--check"]
        with contextlib.redirect_stdout(captured):
            rc = ec.main()
    finally:
        ec.build_product_contracts = original
        sys.argv = original_argv

    output = captured.getvalue()
    assert rc == 1, output
    assert "declared product contract" in output, output
    assert "MISSING" in output, output


def test_landing_schemas_from_typed_declarations_join_key_matches_a_source_reference() -> None:
    """The fold: landing_schemas_from_typed_declarations had no test and
    returns {} on the committed estate. Proves the join key
    f"{domain}.{interfaces.published}" it computes from a real production
    Landing Product Contract is exactly the name a landing product's own
    consumer would reference as its source contract."""
    payload = _vector_payload("append_only_managed_opaque_batch")
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        ctx = _write_production_estate(root, payload)
        schemas = ec.landing_schemas_from_typed_declarations(ctx)
        assert len(schemas) == 1, schemas
        published_name, relation = next(iter(schemas.items()))

        typed = load_typed_declarations(ctx)
        table = next(iter(typed.tables.values()))
        expected_key = f"{table.contract.product.domain}.{table.contract.interfaces.published}"
        assert published_name == expected_key, (published_name, expected_key)

        # A consumer would reference this exact key, pinned to a major
        # version, as its own "sources[].contract" entry.
        consumer_source_reference = f"{published_name}@1"
        assert consumer_source_reference.rsplit("@", 1)[0] == published_name

        field_names = {f.name for f in relation.fields}
        assert {f.name for f in table.contract.projection} == field_names


def test_landing_schemas_from_typed_declarations_empty_on_the_committed_estate_fails_closed_naming_the_product() -> None:
    """The committed estate declares no production Landing Product Contract
    yet, so the real function returns {}; feeding that straight into the
    registry for a landing product must fail closed naming the product,
    never publish an empty schema silently."""
    ctx = ec.EstateContext.default()
    schemas = ec.landing_schemas_from_typed_declarations(ctx)
    assert schemas == {}, schemas

    document = yaml.safe_load((_PRODUCT_FIXTURES_DIR / "landing.yml").read_text(encoding="utf-8"))
    policy = ec.declaration_mod.load_estate_policy(ctx.estate_file)
    validated = ec.declaration_mod.validate_declaration(document, policy=policy)
    entries = {"ecommerce.crm_customer_landing": (document, validated)}
    try:
        ec.contract_mod.resolve_relation_registry(entries, landing_schemas=schemas)
    except ec.contract_mod.MissingLandingSchemaError as exc:
        assert exc.product == "ecommerce.crm_customer_landing"
    else:
        raise AssertionError("expected MissingLandingSchemaError when the join key is absent")


def test_main_write_then_check_on_the_product_route_is_orphan_free() -> None:
    """Blind-review correction: write_files/check_files' orphan scan must
    exclude contracts/products/** the same way it already excludes
    contracts/landing/**, or a clean write-then-check cycle on the
    declaration-driven route reports its own fresh output as orphaned.
    Drives the real main() control flow with five real fixture
    declarations: generate()/generate_landing() are stubbed to a no-op (this
    scratch estate carries no served models and no legacy Landing
    declarations, so there is genuinely nothing for them to do, and this
    keeps the proof from needing a live dbt manifest for models this test
    never declares) and landing_schemas_from_typed_declarations is
    substituted for the same reason a real Landing estate is not built here
    -- but build_product_contracts, generate_products, validate_products,
    write_product_files and check_product_files all run for real, through
    main()'s own wiring, against real fixture declarations."""
    import contextlib
    import io
    import sys

    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        (root / "estate.yml").write_text(
            yaml.safe_dump(
                {
                    "estate": {
                        "namespace": "io.antikas.scratchtest",
                        "support": "desk",
                        "team": "data",
                        "structured_types": ["array"],
                        "labels": {
                            "bronze": {"profiles": ["landing"]},
                            "silver": {"profiles": ["integration", "derivation", "consolidation"]},
                            "gold": {"profiles": ["serving"]},
                        },
                    }
                },
                sort_keys=False,
            ),
            encoding="utf-8",
        )
        products_dir = root / "declarations" / "products"
        products_dir.mkdir(parents=True)
        for fixture in _PRODUCT_FIXTURES_DIR.glob("*.yml"):
            (products_dir / fixture.name).write_text(fixture.read_text(encoding="utf-8"), encoding="utf-8")
        original_generate_landing = ec.generate_landing
        original_landing_schemas = ec.landing_schemas_from_typed_declarations
        ec.generate_landing = lambda ctx=None, *, binding_path=None, environment=None: {}
        ec.landing_schemas_from_typed_declarations = lambda ctx: dict(_PRODUCT_LANDING_SCHEMAS)

        original_argv = sys.argv
        write_out = io.StringIO()
        check_out = io.StringIO()
        try:
            sys.argv = ["emit_contracts", "--estate-root", str(root)]
            with contextlib.redirect_stdout(write_out):
                rc_write = ec.main()
            sys.argv = ["emit_contracts", "--check", "--estate-root", str(root)]
            with contextlib.redirect_stdout(check_out):
                rc_check = ec.main()
        finally:
            ec.generate_landing = original_generate_landing
            ec.landing_schemas_from_typed_declarations = original_landing_schemas
            sys.argv = original_argv

        assert rc_write == 0, write_out.getvalue()
        assert "5 declared product contract" in write_out.getvalue(), write_out.getvalue()
        assert rc_check == 0, check_out.getvalue()
        assert "ORPHAN" not in check_out.getvalue(), check_out.getvalue()
        assert "byte-match" in check_out.getvalue(), check_out.getvalue()


def main() -> int:
    tests = [obj for name, obj in sorted(globals().items()) if name.startswith("test_") and callable(obj)]
    failures = 0
    for test in tests:
        try:
            test()
            print(f"PASS {test.__name__}")
        except Exception:  # noqa: BLE001 -- report-and-continue harness
            failures += 1
            print(f"FAIL {test.__name__}")
            traceback.print_exc()
    print(f"\n{len(tests) - failures}/{len(tests)} passed")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())


def test_the_committed_estate_generates_no_landing_contract() -> None:
    """No table in the committed estate declares a production landing delivery, so
    the landing route emits nothing and says so rather than guessing a binding."""
    assert ec.generate_landing() == {}


def test_generate_products_reads_no_dbt_manifest() -> None:
    """The product route resolves declarations and never opens a compiled dbt
    manifest: this module carries no manifest reader at all."""
    assert not hasattr(ec, "load_manifest")
    assert not hasattr(ec, "served_models")
    files = ec.generate_products(products_dir=_PRODUCT_FIXTURES_DIR, landing_schemas=_PRODUCT_LANDING_SCHEMAS)
    assert files, "expected at least one product-declaration-driven artefact"
