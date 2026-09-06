"""Self-tests for ergasterion/import_ddl.py (the DDL import seeder).

No pytest in this repo's .venv, so this follows the plain assert-and-report convention
of tests/python/test_import_odcs.py: each test_* raises AssertionError on failure, main() runs
them all and reports PASS/FAIL (exit 0 = all green, 1 = any failure). All declarations/
domains written by these tests live under a tempfile.TemporaryDirectory, never the real
declarations/ or domains/. The test run cannot add fixture files to the repository's
authored source definitions.

Exercises:
  1. feed DDL seeds a declarations/<source>.yml skeleton that the typed
     load_declarations() accepts as-is (vault_entities: [] is a legitimate, already-valid
     state -- the TODOs are for a human to act on, not blockers to load).
  2. model DDL seeds a domains/<name>.yml skeleton whose entity_configs/hub_configs/
     link_configs are correctly derived from PK/FK structure, and that
     ergasterion/emit.py's load_domains() accepts as-is (survivorship/ER/relations/odcs are
     left as TODO comments, never guessed, and never block the load).
  3. malformed DDL is rejected with a message naming the specific problem.
  4. determinism: seeding the same DDL twice produces byte-identical output.
  5. provenance header matches import_odcs.py's convention (same load-bearing sentences).

Usage:
    python tests/python/test_import_ddl.py
"""

from __future__ import annotations

import sys
import tempfile
import traceback
from pathlib import Path

import yaml

# Allow direct execution as `python tests/python/test_import_ddl.py`.
if __package__ in (None, ""):
    import os as _os, sys as _sys
    _sys.path.insert(0, _os.path.dirname(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))))

from ergasterion import import_ddl as idd
from ergasterion import import_odcs as io_mod
from ergasterion.estate import EstateContext
from ergasterion.source_delivery import load_typed_declarations

REPO_ROOT = Path(__file__).resolve().parents[2]

# --- fixtures (ergasterion/ test assets -- never under declarations/ or domains/) -----------

FEED_DDL = """
-- CUSTOMERS staging feed, exported from a (fictional) legacy CRM.
CREATE TABLE customers (
    id INTEGER PRIMARY KEY,
    email VARCHAR(255) NOT NULL,
    full_name VARCHAR(200),
    signup_date DATE NOT NULL,
    is_active BOOLEAN
);
"""

def _write(tmp_path: Path, name: str, text: str) -> Path:
    path = tmp_path / name
    path.write_text(text, encoding="utf-8")
    return path


# --- parser well-formedness gate ---------------------------------------------------------

def test_no_create_table_rejected() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        path = _write(Path(tmp), "empty.sql", "-- just a comment, no DDL\n")
        try:
            idd.parse_ddl(path.read_text(encoding="utf-8"))
        except idd.DdlImportError as exc:
            assert "no CREATE TABLE" in str(exc), f"expected the missing-statement problem named, got: {exc}"
        else:
            raise AssertionError("expected DdlImportError for DDL with no CREATE TABLE, none raised")


def test_no_columns_rejected() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        path = _write(Path(tmp), "empty_table.sql", "CREATE TABLE empty_table (\n);\n")
        try:
            idd.parse_ddl(path.read_text(encoding="utf-8"))
        except idd.DdlImportError as exc:
            assert "empty_table" in str(exc) and "no column definitions" in str(exc), (
                f"expected the offending table + missing-columns problem named, got: {exc}"
            )
        else:
            raise AssertionError("expected DdlImportError for a columnless CREATE TABLE, none raised")


# --- (a) feed DDL -> declarations/<source>.yml -------------------------------------------

def test_feed_ddl_seeds_skeleton_validator_accepts() -> None:
    """Acceptance 1: feed DDL seeds a declaration the typed loader reads without a
    complaint (a seed-backed table carries no Landing contract, and the TODOs are for a
    human rather than a load-time blocker)."""
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        ddl_path = _write(tmp_path, "feed.sql", FEED_DDL)
        source_name, text = idd.seed_declaration_from_ddl(ddl_path, source_name="testfeed")
        assert source_name == "testfeed"

        decls_dir = tmp_path / "declarations"
        decls_dir.mkdir()
        _write(decls_dir, f"{source_name}.yml", text)
        seeded = yaml.safe_load(text)
        assert seeded["source"]["name"] == source_name

        ctx = EstateContext.resolve(estate_root=tmp_path, declarations_dir=decls_dir)
        typed = load_typed_declarations(ctx)
        assert typed.tables == {}, "a seed-backed declaration carries no Landing contract"


def test_feed_ddl_projection_and_tests_match_column_constraints() -> None:
    """Column list, cast expression, and NOT NULL/PRIMARY KEY -> data_tests transcription
    are mechanical and exact: 'id' is the sole PRIMARY KEY (not_null+unique), 'email' and
    'signup_date' are NOT NULL (not_null only), 'full_name'/'is_active' carry no tests."""
    with tempfile.TemporaryDirectory() as tmp:
        ddl_path = _write(Path(tmp), "feed.sql", FEED_DDL)
        _, text = idd.seed_declaration_from_ddl(ddl_path, source_name="testfeed")
        seeded = yaml.safe_load(text)
        table = seeded["tables"]["customers"]

        columns = [c["name"] for c in table["projection"]]
        assert columns == ["id", "email", "full_name", "signup_date", "is_active"], columns

        tests_by_name = {t["name"]: set(t["data_tests"]) for t in table["seed_tests"]}
        assert tests_by_name["id"] == {"not_null", "unique"}, tests_by_name["id"]
        assert tests_by_name["email"] == {"not_null"}, tests_by_name["email"]
        assert tests_by_name["signup_date"] == {"not_null"}, tests_by_name["signup_date"]
        assert "full_name" not in tests_by_name
        assert "is_active" not in tests_by_name

        # dpf_safe_cast dispatch for the typed columns; plain cast for the string column.
        exprs = {c["name"]: c["expression"] for c in table["projection"]}
        assert "dpf_safe_cast('id', 'int')" in exprs["id"]
        assert "dpf_safe_cast('signup_date', 'date')" in exprs["signup_date"]
        assert "dpf_safe_cast('is_active', 'boolean')" in exprs["is_active"]
        assert exprs["email"] == "cast(email as string)"


def test_feed_ddl_seeding_is_deterministic() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        ddl_path = _write(Path(tmp), "feed.sql", FEED_DDL)
        _, first = idd.seed_declaration_from_ddl(ddl_path, source_name="testfeed")
        _, second = idd.seed_declaration_from_ddl(ddl_path, source_name="testfeed")
        assert first == second, "seed_declaration_from_ddl produced non-identical output for the same input"


def test_json_family_columns_seed_dispatched_json_cast() -> None:
    json_ddl = """
CREATE TABLE events (
    payload JSON,
    payload_binary JSONB,
    flexible_value VARIANT,
    nested_record STRUCT,
    items ARRAY
);
"""
    with tempfile.TemporaryDirectory() as tmp:
        ddl_path = _write(Path(tmp), "json.sql", json_ddl)
        _, text = idd.seed_declaration_from_ddl(ddl_path, source_name="json_feed")
        seeded = yaml.safe_load(text)
        expressions = {
            column["name"]: column["expression"]
            for column in seeded["tables"]["events"]["projection"]
        }
        expected = {
            column: f"{{{{ dpf_json_cast('{column}') }}}}"
            for column in ("payload", "payload_binary", "flexible_value", "nested_record", "items")
        }
        assert expressions == expected
        assert all("variant" not in expression.lower() for expression in expressions.values())


def test_planted_warehouse_native_cast_fails_seed_gate() -> None:
    original = io_mod._LOGICAL_TYPE_CAST["object"]
    planted = (
        "cast({col} as variant)",
        "{{{{ dpf_safe_cast('{col}::variant', 'int') }}}}",
    )
    try:
        with tempfile.TemporaryDirectory() as tmp:
            ddl_path = _write(Path(tmp), "planted.sql", "CREATE TABLE events (payload JSON);\n")
            for defect in planted:
                io_mod._LOGICAL_TYPE_CAST["object"] = defect
                emitted = defect.format(col="payload")
                try:
                    idd.seed_declaration_from_ddl(ddl_path, source_name="planted_feed")
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


def test_feed_provenance_header_matches_import_odcs_convention() -> None:
    """The header carries the same load-bearing sentences as import_odcs.py's (the
    STARTING-POINT / never-guessed / run-emit-once-filled-in posture), verbatim."""
    with tempfile.TemporaryDirectory() as tmp:
        ddl_path = _write(Path(tmp), "feed.sql", FEED_DDL)
        _, text = idd.seed_declaration_from_ddl(ddl_path, source_name="testfeed")
        assert "Seeded by ergasterion/import_ddl.py from DDL:" in text
        assert "This is a STARTING POINT, not regenerated output" in text
        assert "is left" in text and "never guessed" in text
        assert "never guessed" in text and "TODOs are filled in." in text


# --- (c) feed DDL --landing source -> a Bronze landing/delivery draft --------------------

SOURCE_MODE_DDL = """
CREATE TABLE Orders (
    ID VARCHAR(36) PRIMARY KEY,
    Amount NUMERIC(10,2) NOT NULL,
    Created_At TIMESTAMPTZ NOT NULL,
    Note VARCHAR(200)
);
"""


def test_landing_seed_is_still_the_default() -> None:
    """seed importer defaults are preserved: calling seed_declaration_from_ddl() with no
    --landing argument stays byte-identical to before this module gained a second mode."""
    with tempfile.TemporaryDirectory() as tmp:
        ddl_path = _write(Path(tmp), "feed.sql", FEED_DDL)
        _, text = idd.seed_declaration_from_ddl(ddl_path, source_name="testfeed")
        seeded = yaml.safe_load(text)
        table = seeded["tables"]["customers"]
        assert "raw_model" in table
        assert "landing" not in table and "delivery" not in table


def test_landing_source_carries_physical_schema_and_draft_status() -> None:
    """Acceptance: source mode carries the physical schema and draft status without
    guessing owner, support, access, schedule or progress."""
    with tempfile.TemporaryDirectory() as tmp:
        ddl_path = _write(Path(tmp), "orders.sql", SOURCE_MODE_DDL)
        source_name, text = idd.seed_declaration_from_ddl(
            ddl_path, source_name="acme", landing_kind="source", codec_kind="jsonl",
        )
        seeded = yaml.safe_load(text)
        table = seeded["tables"]["orders"]

        assert "raw_model" not in table, "landing.kind: source forbids raw_model"
        assert "seed_tests" not in table and "model_tests" not in table
        assert "vault_entities" not in table

        landing = table["landing"]
        assert landing["kind"] == "source"
        assert landing["source_name"] == "acme"
        assert landing["identifier"] == "orders"
        assert landing["integration"] == {"kind": "managed"}
        assert landing["codec"]["kind"] == "jsonl"

        columns = {c["name"]: c for c in landing["physical_columns"]}
        assert columns["id"]["logical_type"] == "utf8_string"
        assert columns["id"]["nullable"] is False, "PRIMARY KEY implies NOT NULL"
        assert columns["amount"]["logical_type"] == {"kind": "decimal", "precision": 10, "scale": 2}
        assert columns["created_at"]["logical_type"] == "utc_instant"
        assert columns["note"]["logical_type"] == "utf8_string"
        assert columns["note"]["nullable"] is True

        assert table["delivery"] == {"kind": "draft", "reason": "delivery_contract_required"}
        for guessed in ("product", "owner", "support", "schedule", "progress", "access_policy_ref"):
            assert guessed not in table and guessed not in table["delivery"], (
                f"source mode must never guess {guessed!r}"
            )


def test_landing_source_maps_every_bronze_physical_shape() -> None:
    """Every DDL type family this module reads maps onto Bronze's LogicalType
    vocabulary: bare SimpleLogicalType tokens, a parameterised decimal (explicit and
    default precision/scale), and a parameterised local_datetime (default timezone)."""
    ddl = """
CREATE TABLE things (
    a INTEGER,
    b VARCHAR(10),
    c DATE,
    d BOOLEAN,
    e BLOB,
    f NUMERIC,
    g NUMERIC(6,3),
    h TIMESTAMP,
    i TIMESTAMPTZ
);
"""
    with tempfile.TemporaryDirectory() as tmp:
        ddl_path = _write(Path(tmp), "things.sql", ddl)
        _, text = idd.seed_declaration_from_ddl(ddl_path, source_name="things_src", landing_kind="source")
        seeded = yaml.safe_load(text)
        columns = {c["name"]: c["logical_type"] for c in seeded["tables"]["things"]["landing"]["physical_columns"]}
        assert columns["a"] == "int64"
        assert columns["b"] == "utf8_string"
        assert columns["c"] == "date"
        assert columns["d"] == "boolean"
        assert columns["e"] == "binary"
        assert columns["f"] == {"kind": "decimal", "precision": 38, "scale": 9}, "no precision/scale -> the documented default"
        assert columns["g"] == {"kind": "decimal", "precision": 6, "scale": 3}
        assert columns["h"] == {"kind": "local_datetime", "timezone": "UTC"}
        assert columns["i"] == "utc_instant"


def test_landing_source_table_key_and_identifier_are_lowercase_identifiers() -> None:
    """Bronze's Identifier grammar is lowercase-only; a mixed-case DDL table/column name
    is folded so the draft is already conformant -- no rename needed to go to production."""
    with tempfile.TemporaryDirectory() as tmp:
        ddl_path = _write(Path(tmp), "orders.sql", SOURCE_MODE_DDL)
        _, text = idd.seed_declaration_from_ddl(ddl_path, source_name="acme", landing_kind="source")
        seeded = yaml.safe_load(text)
        assert "orders" in seeded["tables"] and "Orders" not in seeded["tables"]
        assert seeded["tables"]["orders"]["landing"]["identifier"] == "orders"


def test_landing_source_loads_as_a_draft_through_source_delivery() -> None:
    """The seeded landing/delivery block round-trips through
    ergasterion.source_delivery's typed loader, which resolves it to an explicit draft
    placeholder rather than a guessed production contract."""
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        ddl_path = _write(tmp_path, "orders.sql", SOURCE_MODE_DDL)
        source_name, text = idd.seed_declaration_from_ddl(ddl_path, source_name="acme", landing_kind="source")

        decls_dir = tmp_path / "declarations"
        decls_dir.mkdir()
        _write(decls_dir, f"{source_name}.yml", text)

        typed_ctx = EstateContext.resolve(estate_root=tmp_path, declarations_dir=decls_dir)
        typed = load_typed_declarations(typed_ctx)
        table = typed.tables[(source_name, "orders")]
        assert table.kind == "draft"
        assert table.draft_reason == "delivery_contract_required"
        assert table.contract is None


def test_landing_source_is_deterministic() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        ddl_path = _write(Path(tmp), "orders.sql", SOURCE_MODE_DDL)
        _, first = idd.seed_declaration_from_ddl(ddl_path, source_name="acme", landing_kind="source")
        _, second = idd.seed_declaration_from_ddl(ddl_path, source_name="acme", landing_kind="source")
        assert first == second, "source-mode seeding produced non-identical output for the same input"


TESTS = [
    test_no_create_table_rejected,
    test_no_columns_rejected,
    test_feed_ddl_seeds_skeleton_validator_accepts,
    test_feed_ddl_projection_and_tests_match_column_constraints,
    test_feed_ddl_seeding_is_deterministic,
    test_json_family_columns_seed_dispatched_json_cast,
    test_planted_warehouse_native_cast_fails_seed_gate,
    test_feed_provenance_header_matches_import_odcs_convention,
    test_landing_seed_is_still_the_default,
    test_landing_source_carries_physical_schema_and_draft_status,
    test_landing_source_maps_every_bronze_physical_shape,
    test_landing_source_table_key_and_identifier_are_lowercase_identifiers,
    test_landing_source_loads_as_a_draft_through_source_delivery,
    test_landing_source_is_deterministic,
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
