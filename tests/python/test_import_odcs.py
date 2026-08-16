"""Self-tests for ergasterion/import_odcs.py (the ODCS v3.x import seeder).

No pytest in this repo's .venv, so this follows the plain assert-and-report convention
of tests/python/test_emit.py and tests/python/test_emit_contracts.py: each test_* raises
AssertionError on failure, main() runs them all and reports PASS/FAIL (exit 0 = all
green, 1 = any failure). All declarations/*.yml written by these tests live under a
tempfile.TemporaryDirectory and ergasterion/emit.DECLARATIONS_DIR is monkeypatched to point
at it -- never at the real declarations/ directory (the real dir is this repo's live
SSOT and must not gain a stray fixture file from a test run).

Covers the item's acceptance:
  1. a well-formed ODCS v3.x contract seeds a skeleton that ergasterion/emit.py's
     load_declarations() validator accepts as-is (vault_entities: [] is a legitimate,
     already-valid state -- the TODOs are for a human to act on, not blockers to load).
  2. round-trip: seeding from one of the engine's generated ODCS contracts
     (ergasterion/emit_contracts.py's output under contracts/ecommerce) reproduces the same
     column set, in the same order, as that contract's schema.properties.
  3. malformed and v2.x contracts are rejected with a message naming the problem; v2.2.2
     specifically gets an explicit says-upgrade message.
  4. determinism: seeding the same contract twice produces byte-identical output.

Usage:
    python tests/python/test_import_odcs.py
"""

from __future__ import annotations

import sys
import tempfile
import traceback
from pathlib import Path

import yaml

# Allow direct execution as `python tests/python/test_import_odcs.py`.
if __package__ in (None, ""):
    import os as _os, sys as _sys
    _sys.path.insert(0, _os.path.dirname(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))))

from ergasterion import emit
from ergasterion import import_odcs as io_mod

REPO_ROOT = emit.REPO_ROOT
# A generated ODCS v3.1.0 contract from ergasterion/emit_contracts.py.
# the round-trip fixture named in the item's acceptance criterion 2. Self-contained (no
# customProperties survivorship block), so it also exercises the "contract carries no
# vault/survivorship info" path cleanly.
ROUND_TRIP_FIXTURE = REPO_ROOT / "contracts" / "ecommerce" / "dim_customer_segment.odcs.yml"


def _write(tmp_path: Path, name: str, text: str) -> Path:
    path = tmp_path / name
    path.write_text(text, encoding="utf-8")
    return path


def test_v2_contract_rejected_with_upgrade_message() -> None:
    """v2.2.2 is explicitly named + told to upgrade (acceptance criterion 3)."""
    doc = {
        "apiVersion": "v2.2.2",
        "kind": "DataContract",
        "schema": [{"name": "t", "properties": [{"name": "c"}]}],
    }
    with tempfile.TemporaryDirectory() as tmp:
        path = _write(Path(tmp), "v2.yml", yaml.safe_dump(doc))
        try:
            io_mod.load_and_validate_contract(path)
        except io_mod.OdcsImportError as exc:
            message = str(exc)
            assert "v2.2.2" in message, f"expected the offending apiVersion named, got: {message}"
            assert "upgrade" in message.lower(), f"expected an explicit says-upgrade message, got: {message}"
        else:
            raise AssertionError("expected OdcsImportError for a v2.2.2 contract, none raised")


def test_missing_apiversion_rejected() -> None:
    doc = {"kind": "DataContract", "schema": [{"name": "t", "properties": [{"name": "c"}]}]}
    with tempfile.TemporaryDirectory() as tmp:
        path = _write(Path(tmp), "malformed.yml", yaml.safe_dump(doc))
        try:
            io_mod.load_and_validate_contract(path)
        except io_mod.OdcsImportError as exc:
            assert "apiVersion" in str(exc), f"expected the missing field named, got: {exc}"
        else:
            raise AssertionError("expected OdcsImportError for a missing apiVersion, none raised")


def test_missing_schema_rejected() -> None:
    doc = {"apiVersion": "v3.1.0", "kind": "DataContract"}
    with tempfile.TemporaryDirectory() as tmp:
        path = _write(Path(tmp), "no_schema.yml", yaml.safe_dump(doc))
        try:
            io_mod.load_and_validate_contract(path)
        except io_mod.OdcsImportError as exc:
            assert "schema" in str(exc).lower(), f"expected the missing 'schema' section named, got: {exc}"
        else:
            raise AssertionError("expected OdcsImportError for a missing schema section, none raised")


def test_wrong_kind_rejected() -> None:
    doc = {
        "apiVersion": "v3.1.0",
        "kind": "DataProduct",  # an ODPS descriptor, not a contract
        "schema": [{"name": "t", "properties": [{"name": "c"}]}],
    }
    with tempfile.TemporaryDirectory() as tmp:
        path = _write(Path(tmp), "wrong_kind.yml", yaml.safe_dump(doc))
        try:
            io_mod.load_and_validate_contract(path)
        except io_mod.OdcsImportError as exc:
            assert "DataProduct" in str(exc), f"expected the offending kind named, got: {exc}"
        else:
            raise AssertionError("expected OdcsImportError for kind != DataContract, none raised")


def test_empty_properties_rejected() -> None:
    doc = {"apiVersion": "v3.1.0", "kind": "DataContract", "schema": [{"name": "t", "properties": []}]}
    with tempfile.TemporaryDirectory() as tmp:
        path = _write(Path(tmp), "empty_props.yml", yaml.safe_dump(doc))
        try:
            io_mod.load_and_validate_contract(path)
        except io_mod.OdcsImportError as exc:
            assert "t" in str(exc) and "properties" in str(exc).lower(), (
                f"expected the offending table + missing properties named, got: {exc}"
            )
        else:
            raise AssertionError("expected OdcsImportError for empty properties, none raised")


def test_well_formed_contract_seeds_skeleton_validator_accepts() -> None:
    """Acceptance 1: a well-formed ODCS v3.x contract seeds a skeleton that
    ergasterion/emit.py's load_declarations() accepts (vault_entities: [] loads clean;
    the TODOs are for a human, not a load-time blocker)."""
    source_name, text = io_mod.seed_declaration(ROUND_TRIP_FIXTURE, source_name="dpf_round_trip_fixture")
    assert source_name == "dpf_round_trip_fixture"

    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        _write(tmp_path, f"{source_name}.yml", text)
        # Context construction, not global monkeypatching: declarations/ points at the
        # temp dir; domains/ still resolves against the committed estate root.
        ctx = emit.EstateContext.resolve(estate_root=emit.REPO_ROOT, declarations_dir=tmp_path)
        declarations = emit.load_declarations(ctx=ctx)
        assert len(declarations) == 1, "expected exactly the one seeded declaration to load"
        assert declarations[0]["source"]["name"] == source_name


def test_round_trip_schema_matches_source_contract() -> None:
    """Round-trip test: seed from an engine-generated ODCS
    contract and assert the seeded skeleton's projection column set, in order, matches
    the source contract's schema.properties column set."""
    contract = yaml.safe_load(ROUND_TRIP_FIXTURE.read_text(encoding="utf-8"))
    schema_object = contract["schema"][0]
    contract_columns = [prop["name"] for prop in schema_object["properties"]]

    _, text = io_mod.seed_declaration(ROUND_TRIP_FIXTURE, source_name="dpf_round_trip_fixture")
    seeded = yaml.safe_load(text)
    table = seeded["tables"][schema_object["name"]]
    seeded_columns = [column["name"] for column in table["projection"]]

    assert seeded_columns == contract_columns, (
        f"seeded projection columns {seeded_columns} do not match source contract "
        f"schema.properties columns {contract_columns}"
    )

    # The mechanical constraint transcription round-trips too: every contract property
    # marked required/unique/primaryKey produced a matching seed_tests/model_tests entry.
    tests_by_name = {t["name"]: set(t["data_tests"]) for t in table["seed_tests"]}
    for prop in schema_object["properties"]:
        expected: set[str] = set()
        if prop.get("required"):
            expected.add("not_null")
        if prop.get("unique") or prop.get("primaryKey"):
            expected.add("unique")
        if expected:
            assert tests_by_name.get(prop["name"]) == expected, (
                f"{prop['name']}: expected data_tests {expected}, got {tests_by_name.get(prop['name'])}"
            )
        else:
            assert prop["name"] not in tests_by_name, (
                f"{prop['name']}: unexpected data_tests entry for a column with no "
                f"required/unique/primaryKey constraint"
            )


def test_seeding_is_deterministic() -> None:
    _, first = io_mod.seed_declaration(ROUND_TRIP_FIXTURE, source_name="dpf_round_trip_fixture")
    _, second = io_mod.seed_declaration(ROUND_TRIP_FIXTURE, source_name="dpf_round_trip_fixture")
    assert first == second, "seed_declaration produced non-identical output for the same input"


def test_json_family_columns_seed_dispatched_json_cast() -> None:
    doc = {
        "apiVersion": "v3.1.0",
        "kind": "DataContract",
        "schema": [{
            "name": "events",
            "properties": [
                {"name": "payload", "logicalType": "object"},
                {"name": "items", "logicalType": "array"},
            ],
        }],
    }
    with tempfile.TemporaryDirectory() as tmp:
        path = _write(Path(tmp), "json.odcs.yml", yaml.safe_dump(doc))
        _, text = io_mod.seed_declaration(path, source_name="json_feed")
        seeded = yaml.safe_load(text)
        expressions = {
            column["name"]: column["expression"]
            for column in seeded["tables"]["events"]["projection"]
        }
        assert expressions == {
            "payload": "{{ dpf_json_cast('payload') }}",
            "items": "{{ dpf_json_cast('items') }}",
        }
        assert all("variant" not in expression.lower() for expression in expressions.values())


def test_supplier_column_named_variant_passes_seed_gate() -> None:
    doc = {
        "apiVersion": "v3.1.0",
        "kind": "DataContract",
        "schema": [{
            "name": "events",
            "properties": [
                {"name": "variant", "logicalType": "object", "physicalType": "VARCHAR"},
                {"name": "object", "logicalType": "string"},
            ],
        }],
    }
    with tempfile.TemporaryDirectory() as tmp:
        path = _write(Path(tmp), "named-types.odcs.yml", yaml.safe_dump(doc))
        _, text = io_mod.seed_declaration(path, source_name="named_types")
        seeded = yaml.safe_load(text)
        expressions = {
            column["name"]: column["expression"]
            for column in seeded["tables"]["events"]["projection"]
        }
        assert expressions == {
            "variant": "{{ dpf_json_cast('variant') }}",
            "object": "cast(object as string)",
        }


def test_supplier_column_with_sql_punctuation_fails_seed_gate() -> None:
    column = "payload) as variant, cast(x"
    doc = {
        "apiVersion": "v3.1.0",
        "kind": "DataContract",
        "schema": [{"name": "events", "properties": [{"name": column, "logicalType": "string"}]}],
    }
    with tempfile.TemporaryDirectory() as tmp:
        path = _write(Path(tmp), "punctuated.odcs.yml", yaml.safe_dump(doc))
        try:
            io_mod.seed_declaration(path, source_name="punctuated_feed")
        except io_mod.OdcsImportError as exc:
            assert repr(column) in str(exc), str(exc)
            assert "plain SQL identifier" in str(exc), str(exc)
        else:
            raise AssertionError("expected SQL punctuation in an ODCS column name to fail the import")


def test_planted_warehouse_native_cast_fails_seed_gate() -> None:
    doc = {
        "apiVersion": "v3.1.0",
        "kind": "DataContract",
        "schema": [{"name": "events", "properties": [{"name": "payload", "logicalType": "object"}]}],
    }
    original = io_mod._LOGICAL_TYPE_CAST["object"]
    planted = (
        "cast({col} as variant)",
        "{{{{ dpf_safe_cast('{col}::variant', 'int') }}}}",
    )
    try:
        with tempfile.TemporaryDirectory() as tmp:
            path = _write(Path(tmp), "planted.odcs.yml", yaml.safe_dump(doc))
            for defect in planted:
                io_mod._LOGICAL_TYPE_CAST["object"] = defect
                emitted = defect.format(col="payload")
                try:
                    io_mod.seed_declaration(path, source_name="planted_feed")
                except io_mod.OdcsImportError as exc:
                    message = str(exc)
                    assert "payload" in message, message
                    assert emitted in message, message
                else:
                    raise AssertionError(
                        f"expected planted warehouse-native expression to fail the import: {emitted}"
                    )
    finally:
        io_mod._LOGICAL_TYPE_CAST["object"] = original


def test_v2_never_touches_disk() -> None:
    """A rejected contract must not produce a partial/half-written seed file anywhere --
    load_and_validate_contract raises before any output is built."""
    doc = {"apiVersion": "v2.2.2", "kind": "DataContract", "schema": [{"name": "t", "properties": [{"name": "c"}]}]}
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        path = _write(tmp_path, "v2.yml", yaml.safe_dump(doc))
        try:
            io_mod.seed_declaration(path, source_name="never_written")
        except io_mod.OdcsImportError:
            pass
        else:
            raise AssertionError("expected OdcsImportError, none raised")
        assert not (tmp_path / "never_written.yml").exists()


TESTS = [
    test_v2_contract_rejected_with_upgrade_message,
    test_missing_apiversion_rejected,
    test_missing_schema_rejected,
    test_wrong_kind_rejected,
    test_empty_properties_rejected,
    test_well_formed_contract_seeds_skeleton_validator_accepts,
    test_round_trip_schema_matches_source_contract,
    test_seeding_is_deterministic,
    test_json_family_columns_seed_dispatched_json_cast,
    test_supplier_column_named_variant_passes_seed_gate,
    test_supplier_column_with_sql_punctuation_fails_seed_gate,
    test_planted_warehouse_native_cast_fails_seed_gate,
    test_v2_never_touches_disk,
]


def main() -> int:
    failures = 0
    for test in TESTS:
        try:
            test()
        except Exception:
            failures += 1
            print(f"FAIL {test.__name__}")
            traceback.print_exc()
        else:
            print(f"PASS {test.__name__}")
    total = len(TESTS)
    print(f"{total - failures}/{total} passed")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
