"""Self-tests for ergasterion/emit_odps.py (the ODPS (Bitol) v1.0.0 product-descriptor
emitter).

Same plain assert-and-report convention as tests/python/test_emit_contracts.py (no pytest in
this repo's .venv): each test_* raises AssertionError on failure, main() runs them all and
reports PASS/FAIL. Requires target/manifest.json (the build validator runs `dbt parse`
before this, same precondition as test_emit_contracts.py).

Covers the item's acceptance beyond the in-emitter schema-validation + byte-stable gate:
  - determinism: two generations are byte-identical, no timestamp/UUID volatility;
  - both worked domains emit exactly one descriptor each, schema-valid;
  - NAMED TEST (acceptance criterion 2): every output port's contractId+version resolves
    to an actually-emitted ODCS contract file -- never a dangling reference;
  - the investment/e-commerce agnosticism story extends to the descriptor level
    (authoritativeDefinitions present only where the domain's own ODCS contracts carry it);
  - the imported/derived input-port mechanism, exercised via a fixture (none of today's
    real, committed declarations/*.yml trip it -- see module docstring and README) plus
    a same-run assertion that today's real descriptors are input-port-free, so the fixture
    test and the real-output test can never silently drift apart without one of them
    failing first.

Usage:
    python tests/python/test_emit_odps.py
"""

from __future__ import annotations

import copy
import json
import re
import tempfile
import traceback
from pathlib import Path

import yaml

# Allow direct execution as `python tests/python/test_emit_odps.py`.
if __package__ in (None, ""):
    import os as _os, sys as _sys
    _sys.path.insert(0, _os.path.dirname(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))))

from ergasterion import emit
from ergasterion import emit_contracts as ec
from ergasterion import emit_odps as eo
from ergasterion.source_delivery import load_typed_declarations
from ergasterion.translators.dbt import (
    bind_production_sources,
    bronze_odcs_id,
    bronze_odps_id,
    bronze_plan_digest,
    graph_contract_identity,
    landing_handle,
    load_runtime_bindings,
)


def test_determinism_byte_identical() -> None:
    first = eo.generate()
    second = eo.generate()
    assert set(first) == set(second), "descriptor path set changed between two generations"
    for path in first:
        assert first[path] == second[path], f"non-deterministic output for {path}"


def test_no_volatile_tokens() -> None:
    uuid_re = re.compile(r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}")
    ts_re = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}")
    for path, text in eo.generate().items():
        assert not uuid_re.search(text), f"UUID-like token in {path}"
        assert not ts_re.search(text), f"timestamp-like token in {path}"


def test_two_domains_one_descriptor_each() -> None:
    files = eo.generate()
    names = {yaml.safe_load(t)["domain"] for t in files.values()}
    assert names == {"investment", "ecommerce"}, f"expected exactly investment+ecommerce, got {names}"
    assert len(files) == 2, f"expected one descriptor per domain, got {len(files)}"


def test_all_descriptors_schema_valid() -> None:
    validator = eo.load_schema_validator()
    errors = eo.validate_all(eo.generate(), validator)
    assert not errors, "schema-invalid descriptor(s):\n" + "\n".join(errors)


def test_output_ports_resolve_to_emitted_contract_files() -> None:
    """Acceptance criterion 2: every output-port contract ref resolves to an actually-
    emitted contract file on this same run -- never a dangling name or a stale id/version."""
    contract_files = ec.generate()
    contract_docs = {p: yaml.safe_load(t) for p, t in contract_files.items()}
    contract_by_id = {doc["id"]: doc for doc in contract_docs.values()}

    files = eo.generate()
    checked = 0
    for path, text in files.items():
        doc = yaml.safe_load(text)
        domain = doc["domain"]
        for port in doc["outputPorts"]:
            contract_path = eo.CONTRACTS_DIR / domain / f"{port['name']}.odcs.yml"
            assert contract_path in contract_docs, (
                f"{path.name}: output port {port['name']!r} contractId {port['contractId']!r} "
                f"names no file at {contract_path.relative_to(eo.REPO_ROOT).as_posix()}"
            )
            contract_doc = contract_docs[contract_path]
            assert contract_doc["id"] == port["contractId"], (
                f"{path.name}: output port {port['name']!r} contractId {port['contractId']!r} "
                f"!= emitted contract id {contract_doc['id']!r} at "
                f"{contract_path.relative_to(eo.REPO_ROOT).as_posix()}"
            )
            assert contract_doc["version"] == port["version"], (
                f"{path.name}: output port {port['name']!r} version {port['version']!r} "
                f"!= emitted contract version {contract_doc['version']!r}"
            )
            assert port["contractId"] in contract_by_id, (
                f"{path.name}: output port {port['name']!r} contractId {port['contractId']!r} "
                f"is not any emitted contract's id"
            )
            checked += 1
    assert checked >= 2, "expected multiple output ports checked across both domains"


def test_investment_cites_openim_ecommerce_does_not() -> None:
    files = eo.generate()
    docs = {yaml.safe_load(t)["domain"]: yaml.safe_load(t) for t in files.values()}
    assert docs["investment"].get("authoritativeDefinitions"), (
        "investment descriptor lacks an authoritativeDefinitions link"
    )
    assert "authoritativeDefinitions" not in docs["ecommerce"], (
        "e-commerce descriptor carries authoritativeDefinitions -- breaks the agnosticism "
        "story (e-commerce validates against no external model, same as its ODCS contracts)"
    )


def test_current_real_descriptors_have_no_input_ports() -> None:
    """Documents today's honest state (see module + emit_odps.py docstrings): none of the
    committed declarations/*.yml were seeded from an ODCS contract yet, so neither real
    descriptor carries inputPorts today. This is EXPECTED to start failing the moment a
    source is imported via ergasterion/import_odcs.py -- at which point flip this assertion to
    check the new port's shape, don't delete it; it is proving the mechanism wires
    end-to-end into real output, not that it stays permanently empty."""
    for path, text in eo.generate().items():
        doc = yaml.safe_load(text)
        assert "inputPorts" not in doc, (
            f"{path.name}: unexpectedly has inputPorts -- a source was imported; update "
            f"this test's assertion to check the new port instead of removing it"
        )


def test_input_port_from_seeded_declaration() -> None:
    """The imported/derived contract-pointer mechanism itself, exercised directly since no
    committed declaration trips it (see test_current_real_descriptors_have_no_input_ports).
    A declaration file carrying the import seeder's header (ergasterion/import_odcs.py's own
    convention) must produce exactly one input port pointing at the contract it names; a
    declaration with no such header must produce none."""
    header = (
        "# Seeded by ergasterion/import_odcs.py from ODCS contract: "
        "contracts/ecommerce/dim_customer_segment.odcs.yml\n"
        "#   id='dpf:ecommerce:dim_customer_segment'  version='1.0.0'  domain='ecommerce'\n"
        "# (further prose lines, as the real seeder writes, are irrelevant to the parse)\n"
    )
    plain = "source:\n  name: unseeded\n  display_name: UNSEEDED\n  priority: 5\n"

    with tempfile.TemporaryDirectory() as tmp:
        tmp_dir = Path(tmp)
        (tmp_dir / "seeded_source.yml").write_text(header, encoding="utf-8")
        (tmp_dir / "unseeded_source.yml").write_text(plain, encoding="utf-8")

        # Context construction, not global monkeypatching: a context whose
        # declarations/ points at the temp dir carrying the seeded/unseeded files.
        ctx = eo.EstateContext.resolve(estate_root=eo.REPO_ROOT, declarations_dir=tmp_dir)
        seeded_decl = {
            "source": {"name": "seeded_source", "declaration_file": "seeded_source.yml", "priority": 5},
            "tables": {"widgets": {"vault_entities": [{"entity": "customer", "enabled": True}]}},
        }
        unseeded_decl = {
            "source": {"name": "unseeded_source", "declaration_file": "unseeded_source.yml", "priority": 10},
            "tables": {"widgets": {"vault_entities": [{"entity": "customer", "enabled": True}]}},
        }

        ref = eo.imported_contract_ref(seeded_decl, ctx=ctx)
        assert ref == {"id": "dpf:ecommerce:dim_customer_segment", "version": "1.0.0"}, ref

        assert eo.imported_contract_ref(unseeded_decl, ctx=ctx) is None, "unseeded declaration must resolve to no contract"

        ports = eo.build_input_ports({"customer"}, [seeded_decl, unseeded_decl], ctx=ctx)
        assert ports == [
            {"name": "seeded_source", "version": "1.0.0", "contractId": "dpf:ecommerce:dim_customer_segment"}
        ], ports

        # A domain whose entity footprint doesn't include this source's fed entity
        # gets no input port from it, seeded or not.
        assert eo.build_input_ports({"unrelated_entity"}, [seeded_decl], ctx=ctx) == []


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


def test_committed_odps_generate_stays_on_contracts_odps() -> None:
    files = eo.generate()
    for path in files:
        rel = path.as_posix().replace("\\", "/")
        assert "/contracts/odps/" in rel, rel
        assert "/contracts/bronze/" not in rel, rel
    assert eo.generate_bronze() == {}


def test_bronze_odps_identity_matches_odcs_for_managed_and_external() -> None:
    validator = eo.load_schema_validator()
    odcs_validator = ec.load_schema_validator()
    for case in ("append_only_managed_opaque_batch", "csv_external_append_only"):
        payload = _vector_payload(case)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            ctx = _write_production_estate(root, payload)
            typed, _bound, binding_path = _bind(root, ctx)
            odps_files = eo.generate_bronze(ctx, binding_path=binding_path, environment="local")
            odcs_files = ec.generate_bronze(ctx, binding_path=binding_path, environment="local")
            assert len(odps_files) == 1 and len(odcs_files) == 1
            errors = eo.validate_all(odps_files, validator, ctx=ctx)
            assert not errors, "\n".join(errors)
            odcs_errors = ec.validate_all(odcs_files, odcs_validator, ctx=ctx)
            assert not odcs_errors, "\n".join(odcs_errors)
            text = next(iter(odps_files.values()))
            for token in _TELEMETRY:
                assert token not in text, f"{case} leaked {token}"
            doc = yaml.safe_load(text)
            table = next(iter(typed.tables.values()))
            identity = graph_contract_identity(table)
            assert doc["id"] == bronze_odps_id(identity)
            odcs_id = bronze_odcs_id(identity)
            assert doc["outputPorts"][0]["contractId"] == odcs_id
            found = next(item["value"] for item in doc["customProperties"] if item["property"] == "dpf.identity")
            assert found == identity
            odcs_doc = yaml.safe_load(next(iter(odcs_files.values())))
            assert odcs_doc["id"] == odcs_id
            odcs_identity = next(
                item["value"] for item in odcs_doc["customProperties"] if item["property"] == "dpf.identity"
            )
            assert odcs_identity == identity


def test_bronze_odps_draft_only_fails_closed() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        ctx = _write_draft_estate(Path(tmp))
        try:
            files = eo.generate_bronze(ctx)
        except ValueError as exc:
            assert "delivery_contract_required" in str(exc), str(exc)
            assert "draft delivery cannot generate" in str(exc), str(exc)
        else:
            raise AssertionError(f"draft-only ODPS generation must fail closed, got {files!r}")


def test_bronze_odps_seed_plus_draft_fails_closed() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        ctx = _write_draft_estate(Path(tmp), with_seed=True)
        try:
            files = eo.generate_bronze(ctx)
        except ValueError as exc:
            assert "delivery_contract_required" in str(exc), str(exc)
            assert "draft delivery cannot generate" in str(exc), str(exc)
        else:
            raise AssertionError(f"seed-plus-draft ODPS generation must fail closed, got {files!r}")


def test_bronze_odps_mismatched_plan_digest_fails_bind() -> None:
    payload = _vector_payload("append_only_managed_opaque_batch")
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        ctx = _write_production_estate(root, payload)
        _typed, _bound, binding_path = _bind(root, ctx)
        data = yaml.safe_load(binding_path.read_text(encoding="utf-8"))
        data["execution_plan_digest"] = "0" * 64
        binding_path.write_text(yaml.safe_dump(data, sort_keys=False), encoding="utf-8")
        try:
            files = eo.generate_bronze(ctx, binding_path=binding_path, environment="local")
        except ValueError as exc:
            assert "execution_plan_digest" in str(exc), str(exc)
            assert "resolved Bronze graph" in str(exc), str(exc)
        else:
            raise AssertionError(
                f"mismatched execution_plan_digest must fail Bronze ODPS bind, got {files!r}"
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
