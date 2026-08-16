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

import re
import sys
import traceback

import yaml

# Allow direct execution as `python tests/python/test_emit_contracts.py`.
if __package__ in (None, ""):
    import os as _os, sys as _sys
    _sys.path.insert(0, _os.path.dirname(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))))

from ergasterion import emit_contracts as ec


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
