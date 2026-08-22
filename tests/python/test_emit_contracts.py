"""Self-tests for ergasterion/emit_contracts.py (the ODCS v3.1.0 contract adapter).

No pytest in this repo's .venv, so this follows the plain assert-and-report convention of
tests/python/test_emit.py: each test_* raises AssertionError on failure, main() runs them all
and reports PASS/FAIL (exit 0 = all green, 1 = any failure). Requires target/manifest.json
(the build validator runs `dbt parse` before this).

Covers the item's acceptance beyond the in-emitter schema-validation + byte-stable gate:
  - determinism: two generations of the same manifest are byte-identical (Collibra: no
    spurious version pushes) and carry no timestamp/UUID volatility;
  - completeness: every served table is claimed by exactly one domain (loud-fail gate);
  - agnosticism: the investment domain cites the external OpenIM model via
    authoritativeDefinitions; the e-commerce domain carries none and leaks no investment
    vocabulary -- the same emitter, two unrelated domains;
  - schema validity: every emitted contract validates against the vendored ODCS v3.1 schema.

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

from ergasterion import emit
from ergasterion import emit_contracts as ec
from ergasterion.source_delivery import load_typed_declarations
from ergasterion.translators.dbt import (
    bind_production_sources,
    bronze_odcs_id,
    bronze_plan_digest,
    graph_contract_identity,
    landing_handle,
    load_runtime_bindings,
)


def test_determinism_byte_identical() -> None:
    first = ec.generate()
    second = ec.generate()
    assert set(first) == set(second), "contract path set changed between two generations"
    for path in first:
        assert first[path] == second[path], f"non-deterministic output for {path}"


def test_no_volatile_tokens() -> None:
    """No timestamps or UUIDs anywhere in the emitted contracts (byte-stability + Collibra)."""
    uuid_re = re.compile(r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}")
    ts_re = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}")
    for path, text in ec.generate().items():
        assert not uuid_re.search(text), f"UUID-like token in {path}"
        assert not ts_re.search(text), f"timestamp-like token in {path}"


def test_deterministic_ids_and_versions() -> None:
    for path, text in ec.generate().items():
        doc = yaml.safe_load(text)
        assert doc["id"] == f"dpf:{doc['domain']}:{doc['name']}", f"non-deterministic id in {path}"
        assert re.fullmatch(r"\d+\.\d+\.\d+", str(doc["version"])), f"non-semver version in {path}"


def test_completeness_gate_fires_on_unclaimed() -> None:
    """Removing a served table from its domain's product map must fail the completeness gate."""
    original = ec.load_domain_odcs
    manifest = ec.load_manifest()
    served = set(ec.served_models(manifest))

    def patched(ctx=None):
        blocks = original(ctx=ctx)
        # Drop one real served product from whichever domain owns it.
        victim = "canonical_fund"
        for odcs in blocks.values():
            odcs["products"].pop(victim, None)
        return blocks

    ec.load_domain_odcs = patched
    try:
        raised = False
        try:
            ec.generate()
        except ValueError as exc:
            raised = "canonical_fund" in str(exc)
        assert raised, "completeness gate did not fire on an unclaimed served table"
    finally:
        ec.load_domain_odcs = original
    # Sanity: the served set is non-trivial so the gate has something to guard.
    assert len(served) >= 2, "expected multiple served tables"


def test_investment_cites_openim_ecommerce_does_not() -> None:
    files = ec.generate()
    inv = [yaml.safe_load(t) for p, t in files.items() if "/investment/" in p.as_posix()]
    eco = [yaml.safe_load(t) for p, t in files.items() if "/ecommerce/" in p.as_posix()]
    assert inv and eco, "expected both domains to emit contracts"
    for doc in inv:
        assert doc.get("authoritativeDefinitions"), (
            f"investment contract {doc['name']} lacks an authoritativeDefinitions link"
        )
    for doc in eco:
        assert "authoritativeDefinitions" not in doc, (
            f"e-commerce contract {doc['name']} carries authoritativeDefinitions (breaks the "
            f"agnosticism story -- e-commerce validates against no external model)"
        )


def test_ecommerce_data_isolated_from_investment() -> None:
    """Structural/data isolation: e-commerce contracts are namespaced to their own domain
    and their survivorship provenance names only e-commerce sources -- no investment source
    feeds an e-commerce golden record. (Prose descriptions may still MENTION OpenIM to
    explain its deliberate absence; that is the agnosticism story, not a data leak.)"""
    ecommerce_sources = {"CARTIVO", "MERCARO", "RELATIO"}
    for path, text in ec.generate().items():
        if "/ecommerce/" not in path.as_posix():
            continue
        doc = yaml.safe_load(text)
        assert doc["domain"] == "ecommerce", f"{path.name}: wrong domain {doc['domain']}"
        assert doc["id"].startswith("dpf:ecommerce:"), f"{path.name}: id not ecommerce-namespaced"
        for prop in doc.get("customProperties", []):
            if prop["property"] == "dpf.survivorship.sourcePriority":
                stray = set(prop["value"]) - ecommerce_sources
                assert not stray, f"{path.name}: e-commerce golden record fed by non-e-commerce source(s) {stray}"


def test_all_contracts_schema_valid() -> None:
    validator = ec.load_schema_validator()
    errors = ec.validate_all(ec.generate(), validator)
    assert not errors, "schema-invalid contract(s):\n" + "\n".join(errors)


def test_every_served_table_has_one_contract() -> None:
    manifest = ec.load_manifest()
    served = set(ec.served_models(manifest))
    files = ec.generate()
    emitted = {yaml.safe_load(t)["name"] for t in files.values()}
    assert emitted == served, (
        f"served/emitted mismatch: missing={sorted(served - emitted)} extra={sorted(emitted - served)}"
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
    data = json.loads((_FIXTURES / "bronze_schema_vectors.json").read_text(encoding="utf-8"))
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
    domains = root / "domains"
    domains.mkdir()
    (domains / "ops.yml").write_text(
        yaml.safe_dump(
            {
                "bronze": {
                    "domain": {"name": "operations", "display_name": "Operations"},
                    "products": [{"source": ident["source"], "table": ident["table"]}],
                }
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    decls = root / "declarations"
    decls.mkdir()
    product = {key: value for key, value in payload["product"].items() if key != "domain"}
    (decls / f"{ident['source']}.yml").write_text(
        yaml.safe_dump(
            {
                "source": {"name": ident["source"], "display_name": ident["source"].upper(), "priority": 10},
                "tables": {
                    ident["table"]: {
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
    return emit.EstateContext.resolve(estate_root=root)


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
    payload["execution_plan_digest"] = bronze_plan_digest()
    payload["environment"] = environment
    payload["landing_ports"] = {handle: payload["landing_ports"]["raw"]}
    binding_path = root / "runtime.yml"
    binding_path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    bound = bind_production_sources(typed, load_runtime_bindings(binding_path, environment))
    return typed, bound, binding_path


def test_committed_generate_does_not_touch_bronze_or_served_layout() -> None:
    files = ec.generate()
    for path in files:
        rel = path.as_posix()
        assert "/bronze/" not in rel, rel
        assert "/odps/" not in rel, rel
    assert ec.generate_bronze() == {}


def test_bronze_odcs_identity_and_schema_for_managed_and_external() -> None:
    validator = ec.load_schema_validator()
    for case in ("append_only_managed_opaque_batch", "csv_external_append_only"):
        payload = _vector_payload(case)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            ctx = _write_production_estate(root, payload)
            typed, _bound, binding_path = _bind(root, ctx)
            files = ec.generate_bronze(ctx, binding_path=binding_path, environment="local")
            assert len(files) == 1, files
            errors = ec.validate_all(files, validator, ctx=ctx)
            assert not errors, "\n".join(errors)
            path, text = next(iter(files.items()))
            assert path.as_posix().endswith("/bronze.odcs.yml"), path
            assert "/contracts/bronze/" in path.as_posix().replace("\\", "/")
            for token in _TELEMETRY:
                assert token not in text, f"{case} leaked {token}"
            doc = yaml.safe_load(text)
            table = next(iter(typed.tables.values()))
            identity = graph_contract_identity(table)
            assert doc["id"] == bronze_odcs_id(identity)
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
    return emit.EstateContext.resolve(estate_root=root)


def test_bronze_odcs_requires_binding_when_production_exists() -> None:
    payload = _vector_payload("append_only_managed_opaque_batch")
    with tempfile.TemporaryDirectory() as tmp:
        ctx = _write_production_estate(Path(tmp), payload)
        try:
            ec.generate_bronze(ctx)
        except ValueError as exc:
            assert "--binding" in str(exc), str(exc)
        else:
            raise AssertionError("production ODCS generation must require a binding")


def test_bronze_odcs_draft_only_fails_closed() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        ctx = _write_draft_estate(Path(tmp))
        try:
            files = ec.generate_bronze(ctx)
        except ValueError as exc:
            assert "delivery_contract_required" in str(exc), str(exc)
            assert "draft delivery cannot generate" in str(exc), str(exc)
        else:
            raise AssertionError(f"draft-only ODCS generation must fail closed, got {files!r}")


def test_bronze_odcs_seed_plus_draft_fails_closed() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        ctx = _write_draft_estate(Path(tmp), with_seed=True)
        try:
            files = ec.generate_bronze(ctx)
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
            files = ec.generate_bronze(ctx, binding_path=binding_path, environment="local")
        except ValueError as exc:
            assert "execution_plan_digest" in str(exc), str(exc)
            assert "resolved Bronze graph" in str(exc), str(exc)
        else:
            raise AssertionError(
                f"mismatched execution_plan_digest must fail Bronze ODCS bind, got {files!r}"
            )


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
