"""Self-tests for ergasterion/import_ddl.py (the DDL import seeder).

No pytest in this repo's .venv, so this follows the plain assert-and-report convention
of tests/python/test_import_odcs.py: each test_* raises AssertionError on failure, main() runs
them all and reports PASS/FAIL (exit 0 = all green, 1 = any failure). All declarations/
domains written by these tests live under a tempfile.TemporaryDirectory, never the real
declarations/ or domains/. The test run cannot add fixture files to the repository's
authored source definitions.

Exercises:
  1. feed DDL seeds a declarations/<source>.yml skeleton that ergasterion/emit.py's
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

from ergasterion import emit
from ergasterion import import_ddl as idd
from ergasterion import import_odcs as io_mod

REPO_ROOT = emit.REPO_ROOT

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

MODEL_DDL = """
CREATE TABLE customer (
    customer_id INTEGER PRIMARY KEY,
    email VARCHAR(255) NOT NULL,
    full_name VARCHAR(200)
);

CREATE TABLE orders (
    order_id INTEGER PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    order_date DATE NOT NULL,
    order_status VARCHAR(20),
    FOREIGN KEY (customer_id) REFERENCES customer(customer_id)
);

CREATE TABLE product (
    product_id INTEGER PRIMARY KEY,
    product_name VARCHAR(200) NOT NULL
);

-- order_line is a pure junction (PK == the union of its two FK columns) that ALSO
-- carries its own payload (quantity, unit_price) -- the satellite-on-link pattern
-- (domains/ecommerce.yml's order_line/order_line_product, verbatim shape).
CREATE TABLE order_line (
    order_id INTEGER NOT NULL,
    product_id INTEGER NOT NULL,
    quantity INTEGER NOT NULL,
    unit_price NUMERIC(10,2) NOT NULL,
    PRIMARY KEY (order_id, product_id),
    FOREIGN KEY (order_id) REFERENCES orders(order_id),
    FOREIGN KEY (product_id) REFERENCES product(product_id)
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
    """Acceptance 1: feed DDL seeds a skeleton that ergasterion/emit.py's load_declarations()
    accepts (vault_entities: [] loads clean; the TODOs are for a human, not a load-time
    blocker)."""
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        ddl_path = _write(tmp_path, "feed.sql", FEED_DDL)
        source_name, text = idd.seed_declaration_from_ddl(ddl_path, source_name="testfeed")
        assert source_name == "testfeed"

        decls_dir = tmp_path / "declarations"
        decls_dir.mkdir()
        _write(decls_dir, f"{source_name}.yml", text)
        # Context construction, not global monkeypatching: declarations/ points at the
        # temp dir; domains/ still resolves against the committed estate root.
        ctx = emit.EstateContext.resolve(estate_root=emit.REPO_ROOT, declarations_dir=decls_dir)
        declarations = emit.load_declarations(ctx=ctx)
        assert len(declarations) == 1, "expected exactly the one seeded declaration to load"
        assert declarations[0]["source"]["name"] == source_name


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
        assert "Seeded by ergasterion/import_ddl.py --mode feed from DDL:" in text
        assert "This is a STARTING POINT, not regenerated output" in text
        assert "is left" in text and "never guessed" in text
        assert "Run ergasterion/emit.py once those" in text and "TODOs are filled in." in text


# --- (b) model DDL -> domains/<name>.yml --------------------------------------------------

def test_model_ddl_seed_loads_through_emit_load_domains() -> None:
    """Acceptance 2: model DDL seeds a skeleton that ergasterion/emit.py's load_domains()
    accepts as-is (entity/hub/link content is complete and mechanical; bv_configs/
    res_configs/relations/odcs are TODO comments only, never blocking the load)."""
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        ddl_path = _write(tmp_path, "model.sql", MODEL_DDL)
        domain_name, text = idd.seed_domain_from_ddl(ddl_path, domain_name="testmodel")
        assert domain_name == "testmodel"

        domains_dir = tmp_path / "domains"
        domains_dir.mkdir()
        _write(domains_dir, f"{domain_name}.yml", text)
        domain = emit.load_domains(domains_dir)

        assert set(domain["entity_configs"]) == {"customer", "orders", "product", "order_line"}
        assert set(domain["hub_configs"]) == {"customer", "orders", "product"}, (
            "order_line is a link-with-payload (satellite-on-link) -- it must NOT get its "
            "own hub_configs entry, it is carried through link_configs instead"
        )
        assert set(domain["link_configs"]) == {"orders_customer", "order_line"}

        # hashdiff derivation ran through the real loader: hashdiff columns == payload.
        orders = domain["entity_configs"]["orders"]
        assert orders["hashed_columns"]["orders_hashdiff"]["columns"] == orders["payload"]
        order_line = domain["entity_configs"]["order_line"]
        assert order_line["hashed_columns"]["order_line_hashdiff"]["columns"] == order_line["payload"]
        assert order_line["payload"] == ["order_id", "product_id", "quantity", "unit_price"]


def test_model_ddl_entity_hub_derived_from_pk_structure() -> None:
    """A table with a PRIMARY KEY and no FOREIGN KEY becomes a plain hub-worthy entity
    with no links -- 'customer' and 'product' in the fixture."""
    with tempfile.TemporaryDirectory() as tmp:
        ddl_path = _write(Path(tmp), "model.sql", MODEL_DDL)
        _, text = idd.seed_domain_from_ddl(ddl_path, domain_name="testmodel")
        seeded = yaml.safe_load(text)

        for name in ("customer", "product"):
            entity = seeded["entity_configs"][name]
            assert entity["links"] == [], f"{name} declares no FK -- expected no links"
            assert entity["src_pk"] == f"{name}_hk"
            assert entity["hashed_columns"][f"{name}_hk"] == f"golden_{name}_key"
            hub = seeded["hub_configs"][name]
            assert hub["src_pk"] == f"{name}_hk"


def test_model_ddl_link_derived_from_foreign_key() -> None:
    """A table with its own PRIMARY KEY plus a FOREIGN KEY (orders -> customer) gets a
    link to the referenced entity, named <table>_<ref_table>, with a composite lhk
    hashed_columns entry -- domains/ecommerce.yml's order/order_customer shape."""
    with tempfile.TemporaryDirectory() as tmp:
        ddl_path = _write(Path(tmp), "model.sql", MODEL_DDL)
        _, text = idd.seed_domain_from_ddl(ddl_path, domain_name="testmodel")
        seeded = yaml.safe_load(text)

        orders = seeded["entity_configs"]["orders"]
        assert orders["links"] == ["orders_customer"]
        assert orders["hashed_columns"]["customer_hk"] == "golden_customer_key"
        assert orders["hashed_columns"]["orders_customer_lhk"] == ["golden_orders_key", "golden_customer_key"]

        link = seeded["link_configs"]["orders_customer"]
        assert link["src_pk"] == "orders_customer_lhk"
        assert link["src_fk"] == ["orders_hk", "customer_hk"]


def test_model_ddl_pure_junction_with_payload_becomes_link_and_entity() -> None:
    """order_line: PK == the union of its two FK columns (a pure junction) but it also
    carries its own payload (quantity, unit_price) beyond the keys -- it must therefore
    register as BOTH a link_configs entry (the relationship) AND an entity_configs entry
    (the satellite-on-link payload), the domains/ecommerce.yml order_line pattern."""
    with tempfile.TemporaryDirectory() as tmp:
        ddl_path = _write(Path(tmp), "model.sql", MODEL_DDL)
        _, text = idd.seed_domain_from_ddl(ddl_path, domain_name="testmodel")
        seeded = yaml.safe_load(text)

        assert "order_line" in seeded["link_configs"]
        assert "order_line" not in seeded["hub_configs"], "a link-entity must not also get a hub_configs entry"
        link = seeded["link_configs"]["order_line"]
        assert link["src_fk"] == ["orders_hk", "product_hk"]

        entity = seeded["entity_configs"]["order_line"]
        assert entity["links"] == ["order_line"]
        assert entity["payload"] == ["order_id", "product_id", "quantity", "unit_price"]


def test_model_ddl_pure_junction_without_payload_is_link_only() -> None:
    """A pure junction table with NO extra columns beyond its keys gets only a
    link_configs entry -- no satellite is needed, so no entity_configs entry either."""
    ddl = """
CREATE TABLE alpha (
    alpha_id INTEGER PRIMARY KEY
);
CREATE TABLE beta (
    beta_id INTEGER PRIMARY KEY
);
CREATE TABLE alpha_beta (
    alpha_id INTEGER NOT NULL,
    beta_id INTEGER NOT NULL,
    PRIMARY KEY (alpha_id, beta_id),
    FOREIGN KEY (alpha_id) REFERENCES alpha(alpha_id),
    FOREIGN KEY (beta_id) REFERENCES beta(beta_id)
);
"""
    with tempfile.TemporaryDirectory() as tmp:
        ddl_path = _write(Path(tmp), "model.sql", ddl)
        _, text = idd.seed_domain_from_ddl(ddl_path, domain_name="testmodel")
        seeded = yaml.safe_load(text)
        assert "alpha_beta" in seeded["link_configs"]
        assert "alpha_beta" not in seeded["entity_configs"]


def test_model_ddl_never_guesses_survivorship_er_relations() -> None:
    """Acceptance: survivorship (bv_configs) / entity-resolution (res_configs) /
    relations / odcs are NEVER guessed -- left as explicit TODO comments, absent as real
    YAML keys."""
    with tempfile.TemporaryDirectory() as tmp:
        ddl_path = _write(Path(tmp), "model.sql", MODEL_DDL)
        _, text = idd.seed_domain_from_ddl(ddl_path, domain_name="testmodel")
        seeded = yaml.safe_load(text)
        for section in ("bv_configs", "res_configs", "relations", "odcs"):
            assert section not in seeded, f"{section} must be left TODO, never guessed onto the seed"
            assert f"TODO {section}" in text, f"expected an explicit TODO comment block for {section}"


def test_model_ddl_seeding_is_deterministic() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        ddl_path = _write(Path(tmp), "model.sql", MODEL_DDL)
        _, first = idd.seed_domain_from_ddl(ddl_path, domain_name="testmodel")
        _, second = idd.seed_domain_from_ddl(ddl_path, domain_name="testmodel")
        assert first == second, "seed_domain_from_ddl produced non-identical output for the same input"


def test_model_provenance_header_matches_import_odcs_convention() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        ddl_path = _write(Path(tmp), "model.sql", MODEL_DDL)
        _, text = idd.seed_domain_from_ddl(ddl_path, domain_name="testmodel")
        assert "Seeded by ergasterion/import_ddl.py --mode model from DDL:" in text
        assert "This is a STARTING POINT, not regenerated output" in text
        assert "is left" in text and "never guessed" in text
        assert "Run ergasterion/emit.py once those" in text and "TODOs are filled in." in text


TESTS = [
    test_no_create_table_rejected,
    test_no_columns_rejected,
    test_feed_ddl_seeds_skeleton_validator_accepts,
    test_feed_ddl_projection_and_tests_match_column_constraints,
    test_feed_ddl_seeding_is_deterministic,
    test_json_family_columns_seed_dispatched_json_cast,
    test_planted_warehouse_native_cast_fails_seed_gate,
    test_feed_provenance_header_matches_import_odcs_convention,
    test_model_ddl_seed_loads_through_emit_load_domains,
    test_model_ddl_entity_hub_derived_from_pk_structure,
    test_model_ddl_link_derived_from_foreign_key,
    test_model_ddl_pure_junction_with_payload_becomes_link_and_entity,
    test_model_ddl_pure_junction_without_payload_is_link_only,
    test_model_ddl_never_guesses_survivorship_er_relations,
    test_model_ddl_seeding_is_deterministic,
    test_model_provenance_header_matches_import_odcs_convention,
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
