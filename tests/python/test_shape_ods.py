"""Self-tests for the ``ods`` shape: entity relations with declared keys
and effectivity, one cross-reference relation and one audit tail per
declared entity (architecture sections 6, 9, 10, 12, 13).

Same plain assert-and-report convention as the rest of this repo (no
pytest in the .venv): each ``test_*`` raises ``AssertionError`` on
failure, ``main()`` runs them all and reports PASS/FAIL.

Everything runs against ``tests/fixtures/estates/ods_one``, copied into a
scratch directory so a red case can mutate one file without touching the
fixture. Every red case drives the real ``ergasterion emit-products``
command, or a real dbt build of what that command wrote, and asserts what
the failure names.

Covers:
  - the command emits both products, is byte-stable across two runs, and
    ``--check`` is clean on what it wrote;
  - the executed DuckDB build: two entities survive resolution from six
    raw change events over four source keys, one of which (CRM-100)
    is re-pointed from one entity to the other by its latest event; the
    entity relation carries adjacent, half-open effective ranges, the
    last version of each entity open; the cross-reference relation
    still carries exactly one row per distinct source key, resolved to
    the entity its latest event names, not a plain distinct over every
    entity a key was ever seen under; the audit tail carries one row
    per delivered change, with its timestamp and operation; a second
    build changes nothing;
  - every published column carries its declared type on the built
    adapter, and a changed declared type follows into the built one;
  - the contract lists every relation the shape renders, with an ODCS
    document per relation;
  - every generated test is written in the nested arguments form;
  - the generated tests go red on what they name: a duplicated entity
    version, a duplicated cross-reference key, a duplicated audit
    change, and a broken effective range;
  - the fail-closed branches: an entity naming a column the composition
    does not carry, keyed on a column it does not publish, declared
    twice, a change column that is not an instant, a column doing two
    roles at once, and an attribute already named as the effective range.

Usage:
    python tests/python/test_shape_ods.py
"""

from __future__ import annotations

import contextlib
import io
import os
import shutil
import subprocess
import sys
import tempfile
import traceback
from pathlib import Path

import yaml

if __package__ in (None, ""):
    import os as _os
    import sys as _sys

    _sys.path.insert(0, _os.path.dirname(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))))

from ergasterion import cli
from ergasterion import emit_contracts as ec
from ergasterion import emit_products as ep
from ergasterion import structure_gate
from ergasterion.estate import EstateContext
from ergasterion.framework import contract as contract_mod
from ergasterion.framework.adapters import load_adapter_conventions
from ergasterion.shapes.ods import EFFECTIVE_FROM, EFFECTIVE_TO
from ergasterion.translators.dbt_patterns import MODELS_ROOT, parse_gate
from ergasterion.translators.dbt_patterns import sql as sql_mod

REPO_ROOT = Path(__file__).resolve().parents[2]
FIXTURE_ESTATE = REPO_ROOT / "tests" / "fixtures" / "estates" / "ods_one"
ENGINE_MACROS = (
    "cross_db.sql",
    "product_tests.sql",
    "publish.sql",
    "quarantine.sql",
    "survivorship.sql",
)

MODELS_DIR = f"{MODELS_ROOT}/crm"
FEED_MODEL = f"{MODELS_DIR}/crm__customer_events.sql"
FEED_SCHEMA = f"{MODELS_DIR}/crm__customer_events.yml"
ODS_SCHEMA = f"{MODELS_DIR}/crm__customer_ods.yml"
ENTITY_MODEL = f"{MODELS_DIR}/crm__customer_ods__customer.sql"
XREF_MODEL = f"{MODELS_DIR}/crm__customer_ods__customer_xref.sql"
AUDIT_MODEL = f"{MODELS_DIR}/crm__customer_ods__customer_audit.sql"

EXPECTED_ARTEFACTS = {
    "manifests/products/crm/customer_events.json",
    "manifests/products/crm/customer_ods.json",
    FEED_MODEL,
    FEED_SCHEMA,
    f"{MODELS_DIR}/crm__customer_events__checked.sql",
    f"{MODELS_DIR}/crm__customer_events__current.sql",
    f"{MODELS_DIR}/crm__customer_events__quarantine.sql",
    f"{MODELS_DIR}/crm__customer_events__sla.sql",
    ODS_SCHEMA,
    f"{MODELS_DIR}/crm__customer_ods__base.sql",
    f"{MODELS_DIR}/crm__customer_ods__checked.sql",
    ENTITY_MODEL,
    AUDIT_MODEL,
    f"{MODELS_DIR}/crm__customer_ods__customer_audit_current.sql",
    f"{MODELS_DIR}/crm__customer_ods__customer_audit_sla.sql",
    f"{MODELS_DIR}/crm__customer_ods__customer_current.sql",
    f"{MODELS_DIR}/crm__customer_ods__customer_sla.sql",
    XREF_MODEL,
    f"{MODELS_DIR}/crm__customer_ods__customer_xref_current.sql",
    f"{MODELS_DIR}/crm__customer_ods__customer_xref_sla.sql",
    f"{MODELS_DIR}/crm__customer_ods__quarantine.sql",
}


# --------------------------------------------------------------------------- harness


def _scratch_estate(root: Path) -> Path:
    """The fixture estate copied into ``root``, with the engine's own macros
    beside the estate's. The macros are copied rather than duplicated in the
    fixture so the estate always builds against the macros the engine
    ships."""

    estate = root / "estate"
    shutil.copytree(FIXTURE_ESTATE, estate)
    (estate / "macros").mkdir(exist_ok=True)
    for name in ENGINE_MACROS:
        shutil.copy(REPO_ROOT / "macros" / name, estate / "macros" / name)
    return estate


def _run(estate: Path, *arguments: str) -> tuple[int, str]:
    out, err = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        code = cli.main(["emit-products", "--estate-root", str(estate), *arguments])
    return code, out.getvalue() + err.getvalue()


def _emit(estate: Path) -> str:
    code, output = _run(estate)
    assert code == 0, f"emit-products failed:\n{output}"
    return output


def _fails(estate: Path, *expected: str) -> str:
    code, output = _run(estate)
    assert code == 1, f"expected the command to fail closed, it exited {code}:\n{output}"
    for token in expected:
        assert token in output, f"the failure does not name {token!r}:\n{output}"
    return output


def _read(estate: Path, relative: str) -> str:
    return (estate / relative).read_text(encoding="utf-8")


def _tree(estate: Path) -> dict[str, bytes]:
    files: dict[str, bytes] = {}
    for root in (MODELS_ROOT, "manifests/products"):
        for path in sorted((estate / root).rglob("*")):
            if path.is_file():
                files[path.relative_to(estate).as_posix()] = path.read_bytes()
    return files


def _edit_declaration(estate: Path, name: str, mutate) -> None:
    path = estate / "declarations" / "products" / name
    document = yaml.safe_load(path.read_text(encoding="utf-8"))
    mutate(document)
    path.write_text(yaml.safe_dump(document, sort_keys=False), encoding="utf-8")


def _shape_config(document: dict) -> dict:
    return document["target"]["shape_config"]


def _append_seed_row(estate: Path, name: str, row: str) -> None:
    path = estate / "seeds" / f"{name}.csv"
    path.write_text(path.read_text(encoding="utf-8") + row + "\n", encoding="utf-8")


def _dbt_executable() -> Path:
    candidate = Path(sys.executable).resolve().parent / ("dbt.exe" if os.name == "nt" else "dbt")
    assert candidate.is_file(), (
        f"the executed DuckDB lane needs the dbt executable beside the interpreter: {candidate}"
    )
    return candidate


def _dbt(estate: Path) -> subprocess.CompletedProcess:
    return subprocess.run(
        [
            str(_dbt_executable()),
            "build",
            "--profiles-dir",
            "profiles",
            "--project-dir",
            str(estate),
        ],
        cwd=estate,
        capture_output=True,
        text=True,
    )


def _dbt_build(estate: Path) -> str:
    result = _dbt(estate)
    assert result.returncode == 0, result.stdout + result.stderr
    return result.stdout + result.stderr


def _dbt_fails(estate: Path, *expected: str) -> str:
    result = _dbt(estate)
    output = result.stdout + result.stderr
    assert result.returncode != 0, f"expected the dbt build to fail:\n{output}"
    for token in expected:
        assert token in output, f"the dbt failure does not name {token!r}:\n{output}"
    return output


def _query(estate: Path, statement: str):
    import duckdb

    connection = duckdb.connect(str(estate / "target" / "ods_one.duckdb"), read_only=True)
    try:
        return connection.execute(statement).fetchall()
    finally:
        connection.close()


def _built_types(estate: Path, relation: str) -> dict[str, str]:
    rows = _query(
        estate,
        "select column_name, data_type from information_schema.columns "
        f"where table_name = '{relation}' order by ordinal_position",
    )
    return {name: str(kind) for name, kind in rows}


def _declared_types(estate: Path) -> dict[str, object]:
    """The neutral type the delivered declaration states for every column
    it publishes. Read from the declaration rather than restated here, so
    a type assertion is against what was declared."""

    document = yaml.safe_load(_read(estate, "declarations/products/customer_events.yml"))
    step = next(entry for entry in document["steps"] if entry["pattern"] == "schema_transform")
    return {entry["to"]: entry["type"] for entry in step["mapping"] if "to" in entry}


def _physical_type(estate: Path, declared: object) -> str:
    """The type the reference adapter reports for one declared neutral
    type."""

    mapping = load_adapter_conventions("duckdb").type_mapping
    physical = mapping[sql_mod.SCALAR_TYPE_TOKENS[str(declared)]]
    return str(_query(estate, f"select typeof(cast(null as {physical}))")[0][0])


def _emitted_and_built(root: Path) -> Path:
    estate = _scratch_estate(root)
    _emit(estate)
    _dbt_build(estate)
    return estate


# --------------------------------------------------------------------------- green: the command


def test_the_command_emits_the_ods_product_and_reemission_is_byte_identical() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))
        summary = _emit(estate)
        assert "shape=ods" in summary, summary
        first = _tree(estate)
        assert EXPECTED_ARTEFACTS <= set(first), sorted(EXPECTED_ARTEFACTS - set(first))

        _emit(estate)
        assert _tree(estate) == first, "a second emission is not byte-identical"

        code, output = _run(estate, "--check")
        assert code == 0, output
        assert "0 problem(s)" in output, output

        parse_gate.assert_parses(
            {
                path: text.decode("utf-8")
                for path, text in first.items()
                if path.endswith(".sql")
            },
            adapters=("duckdb", "bigquery"),
        )


def test_the_ods_relations_render_as_tables_inside_the_declared_budget() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))
        _emit(estate)
        for model in (ENTITY_MODEL, XREF_MODEL, AUDIT_MODEL):
            assert "materialized='table'" in _read(estate, model), model

        # check_structure() reports every artefact against the declared
        # view boundaries, and this estate declares none, so it also
        # names the generic checked and current-pointer views every
        # product carries -- pre-existing, shape-independent machinery,
        # not something this test judges. What this assertion checks is
        # narrower and is what this shape owns: of everything the gate
        # reports, none of it names the entity, cross-reference or audit
        # relation themselves -- an operational data store computes, it
        # publishes no view of its own.
        primary = {ENTITY_MODEL, XREF_MODEL, AUDIT_MODEL}
        offenses = [
            offense
            for offense in structure_gate.check_structure(EstateContext.resolve(estate_root=estate))
            if offense.artefact in primary
        ]
        assert not offenses, [str(offense) for offense in offenses]


# --------------------------------------------------------------------------- green: known answers


def test_the_entity_relation_carries_adjacent_half_open_versions_after_resolution() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _emitted_and_built(Path(tmp))
        rows = _query(
            estate,
            "select entity_id, customer_name, customer_segment, loyalty_points, "
            "effective_from, effective_to from crm__customer_ods__customer "
            "order by entity_id, effective_from",
        )
        assert [(row[0], row[1], row[2], row[3], str(row[4]), None if row[5] is None else str(row[5])) for row in rows] == [
            ("E-001", "Ada Byron", "standard", 10, "2026-01-05 09:00:00", "2026-02-10 09:00:00"),
            ("E-001", "Ada Byron", "premium", 10, "2026-02-10 09:00:00", "2026-03-15 09:00:00"),
            ("E-001", "Ada B. Byron", "premium", 15, "2026-03-15 09:00:00", None),
            ("E-002", "Grace Hopper", "premium", 20, "2026-01-20 09:00:00", "2026-05-01 09:00:00"),
            ("E-002", "Grace Hopper", "premium", 25, "2026-05-01 09:00:00", None),
        ], rows
        entities = _query(estate, "select count(distinct entity_id) from crm__customer_ods__customer")
        assert entities == [(2,)], entities


def test_the_cross_reference_relation_resolves_to_the_latest_entity_per_source_key() -> None:
    # CRM-100 is delivered against E-001 twice and then, by its latest
    # event, against E-002: a plain select distinct over every entity a
    # source key was ever seen under would carry two rows for it. The
    # type 1 derivation this relation reads instead keeps the row count
    # at one per source key and resolves each to the entity its latest
    # change named.
    with tempfile.TemporaryDirectory() as tmp:
        estate = _emitted_and_built(Path(tmp))
        rows = _query(
            estate,
            "select source_system, source_key, entity_id from crm__customer_ods__customer_xref "
            "order by source_system, source_key",
        )
        assert rows == [
            ("billing", "BILL-200", "E-001"),
            ("billing", "BILL-201", "E-002"),
            ("crm", "CRM-100", "E-002"),
            ("crm", "CRM-101", "E-002"),
        ], rows
        # Four distinct source keys were delivered; the row count above is
        # already one per key by construction (the generated grain test
        # holds it there), so what this line adds is the row count against
        # the number of distinct keys the seed actually carries, not a
        # second read of the same relation.
        distinct_keys = _query(
            estate,
            "select count(*) from (select distinct source_system, source_key "
            "from crm__customer_ods__customer_audit) as keys",
        )
        assert distinct_keys == [(4,)] == [(len(rows),)], (distinct_keys, rows)


def test_the_audit_tail_carries_one_row_per_change() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _emitted_and_built(Path(tmp))
        rows = _query(
            estate,
            "select entity_id, source_system, source_key, changed_at, operation "
            "from crm__customer_ods__customer_audit order by changed_at",
        )
        assert [(row[0], row[1], row[2], str(row[3]), row[4]) for row in rows] == [
            ("E-001", "crm", "CRM-100", "2026-01-05 09:00:00", "insert"),
            ("E-002", "crm", "CRM-101", "2026-01-20 09:00:00", "insert"),
            ("E-001", "billing", "BILL-200", "2026-02-10 09:00:00", "update"),
            ("E-001", "crm", "CRM-100", "2026-03-15 09:00:00", "update"),
            ("E-002", "billing", "BILL-201", "2026-04-01 09:00:00", "update"),
            ("E-002", "crm", "CRM-100", "2026-05-01 09:00:00", "update"),
        ], rows


def test_a_second_build_of_the_same_declarations_changes_nothing() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _emitted_and_built(Path(tmp))
        relations = (
            "crm__customer_ods__customer",
            "crm__customer_ods__customer_xref",
            "crm__customer_ods__customer_audit",
        )
        before = {relation: _query(estate, f"select * from {relation}") for relation in relations}
        _dbt_build(estate)
        after = {relation: _query(estate, f"select * from {relation}") for relation in relations}
        for relation in relations:
            assert sorted(map(str, before[relation])) == sorted(map(str, after[relation])), relation


# --------------------------------------------------------------------------- green: types, contracts, gates


def test_every_published_column_carries_its_declared_type_on_the_built_adapter() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _emitted_and_built(Path(tmp))
        declared = _declared_types(estate)

        entity = _built_types(estate, "crm__customer_ods__customer")
        for column, built in entity.items():
            source = "changed_at" if column in (EFFECTIVE_FROM, EFFECTIVE_TO) else column
            assert built == _physical_type(estate, declared[source]), (column, built)

        xref = _built_types(estate, "crm__customer_ods__customer_xref")
        for column, built in xref.items():
            assert built == _physical_type(estate, declared[column]), (column, built)

        audit = _built_types(estate, "crm__customer_ods__customer_audit")
        for column, built in audit.items():
            assert built == _physical_type(estate, declared[column]), (column, built)


def test_a_changed_declared_type_follows_into_the_built_type() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _emitted_and_built(Path(tmp))
        before = _built_types(estate, "crm__customer_ods__customer")["loyalty_points"]
        assert before == _physical_type(estate, _declared_types(estate)["loyalty_points"]), before

        def restate(document: dict) -> None:
            for entry in document["steps"]:
                if entry["pattern"] != "schema_transform":
                    continue
                for mapping_entry in entry["mapping"]:
                    if mapping_entry.get("to") == "loyalty_points":
                        mapping_entry["type"] = "string"

        _edit_declaration(estate, "customer_events.yml", restate)
        _emit(estate)
        _dbt_build(estate)
        after = _built_types(estate, "crm__customer_ods__customer")["loyalty_points"]
        assert after == _physical_type(estate, "string"), after
        assert after != before, after


def test_the_contract_lists_every_relation_the_shape_renders() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))
        _emit(estate)
        ctx = EstateContext.resolve(estate_root=estate)
        _plans, entries, _graph = ep.build_product_plans(ctx)
        landing = contract_mod.fixture_relation_schemas(entries)
        contracts = ec.build_product_contracts(ctx, landing_schemas=landing)

        ods = contracts["crm.customer_ods"]
        assert [relation.name for relation in ods.relations] == [
            "crm.customer_ods__customer",
            "crm.customer_ods__customer_xref",
            "crm.customer_ods__customer_audit",
        ], ods.relations

        files = ec.generate_products(ctx, landing_schemas=landing)
        odcs = sorted(
            path.name for path in files if path.name.endswith(".odcs.yml") and "customer_ods" in str(path)
        )
        assert len(odcs) == 3, odcs


def test_every_generated_test_is_written_in_the_nested_arguments_form() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))
        _emit(estate)
        document = yaml.safe_load(_read(estate, ODS_SCHEMA))
        for model in document["models"]:
            for entry in model.get("data_tests", []):
                for name, body in entry.items():
                    assert set(body) <= {"name", "arguments", "config"}, (name, body)
                    assert "config" in body and "severity" in body["config"], name

        grain_tests = {
            model["name"]: test["dpf_unique_grain"]["arguments"]["columns"]
            for model in document["models"]
            for test in model.get("data_tests", [])
            if "dpf_unique_grain" in test
        }
        assert grain_tests["crm__customer_ods__customer"] == ["entity_id", EFFECTIVE_FROM]
        assert grain_tests["crm__customer_ods__customer_xref"] == ["source_system", "source_key"]
        assert grain_tests["crm__customer_ods__customer_audit"] == [
            "source_system",
            "source_key",
            "changed_at",
        ]


# --------------------------------------------------------------------------- red: the generated tests


def test_a_duplicated_entity_version_turns_the_key_test_red() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _emitted_and_built(Path(tmp))
        _append_seed_row(
            estate,
            "customer_events",
            "EV-06,crm,CRM-100,E-001,Ada Lovelace,standard,10,2026-01-05 09:00:00,update",
        )
        _dbt_fails(estate, "dpf_unique_grain_crm__customer_ods__customer")


def test_a_duplicated_change_turns_the_audit_grain_test_red() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _emitted_and_built(Path(tmp))
        _append_seed_row(
            estate,
            "customer_events",
            "EV-07,crm,CRM-100,E-001,Ada Byron,standard,10,2026-01-05 09:00:00,insert",
        )
        _dbt_fails(estate, "dpf_unique_grain_crm__customer_ods__customer_audit")


def test_a_duplicated_cross_reference_key_turns_the_grain_test_red() -> None:
    # No seeded data can organically duplicate a cross-reference key: the
    # type 1 derivation ranks every row for one source key and keeps only
    # the top rank, so exactly one row survives per key regardless of how
    # many historical events that key carries. Proving the generated
    # uniqueness test actually catches a duplicate therefore means
    # breaking the derivation itself, the same way the contiguity red
    # case below breaks the range derivation: widen the rank filter so a
    # source key with more than one historical event -- CRM-100 carries
    # three -- publishes two rows instead of one.
    with tempfile.TemporaryDirectory() as tmp:
        estate = _emitted_and_built(Path(tmp))
        model = estate / XREF_MODEL
        text = model.read_text(encoding="utf-8")
        broken = text.replace("where dpf_version_rank = 1", "where dpf_version_rank <= 2")
        assert broken != text, "the derivation this test breaks is not in the generated model"
        model.write_text(broken, encoding="utf-8")
        _dbt_fails(estate, "dpf_unique_grain_crm__customer_ods__customer_xref")


def test_a_broken_effective_range_turns_the_contiguity_test_red() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _emitted_and_built(Path(tmp))
        model = estate / ENTITY_MODEL
        text = model.read_text(encoding="utf-8")
        broken = text.replace(
            "lead(effective_from) over (partition by entity_id order by effective_from) "
            "as effective_to",
            "cast(null as timestamp) as effective_to",
        )
        assert broken != text, "the derivation this test breaks is not in the generated model"
        model.write_text(broken, encoding="utf-8")
        _dbt_fails(estate, "dpf_effective_range_contiguity_crm__customer_ods__customer")


# --------------------------------------------------------------------------- red: shape constraints


def test_an_entity_naming_a_column_the_composition_does_not_carry_fails_closed() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))

        def mutate(document: dict) -> None:
            _shape_config(document)["entities"][0]["attributes"].append("no_such_column")

        _edit_declaration(estate, "customer_ods.yml", mutate)
        _fails(estate, "no_such_column", "customer_ods", "ods")


def test_an_entity_keyed_on_a_column_it_does_not_publish_fails_closed() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))

        def mutate(document: dict) -> None:
            _shape_config(document)["entities"][0]["key"] = ["no_such_key"]

        _edit_declaration(estate, "customer_ods.yml", mutate)
        _fails(estate, "no_such_key", "customer_ods")


def test_an_entity_declared_twice_fails_closed() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))

        def mutate(document: dict) -> None:
            entities = _shape_config(document)["entities"]
            entities.append(dict(entities[0]))

        _edit_declaration(estate, "customer_ods.yml", mutate)
        _fails(estate, "declared twice", "customer")


def test_a_change_column_that_is_not_an_instant_fails_closed() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))

        def mutate(document: dict) -> None:
            # event_id is a plain string the composition carries but no
            # role in this entity names, so this reaches the instant check
            # rather than the earlier role-clash check.
            _shape_config(document)["entities"][0]["change_column"] = "event_id"

        _edit_declaration(estate, "customer_ods.yml", mutate)
        _fails(estate, "event_id", "instant")


def test_a_column_doing_two_roles_at_once_fails_closed() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))

        def mutate(document: dict) -> None:
            _shape_config(document)["entities"][0]["source_system_column"] = "customer_name"

        _edit_declaration(estate, "customer_ods.yml", mutate)
        _fails(estate, "customer_name", "distinct roles")


def test_an_attribute_already_named_as_the_effective_range_fails_closed() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))

        def mutate(document: dict) -> None:
            _shape_config(document)["entities"][0]["attributes"].append(EFFECTIVE_FROM)

        _edit_declaration(estate, "customer_ods.yml", mutate)
        _fails(estate, EFFECTIVE_FROM, "effective range")


# --------------------------------------------------------------------------- runner

TESTS = [
    test_the_command_emits_the_ods_product_and_reemission_is_byte_identical,
    test_the_ods_relations_render_as_tables_inside_the_declared_budget,
    test_the_entity_relation_carries_adjacent_half_open_versions_after_resolution,
    test_the_cross_reference_relation_resolves_to_the_latest_entity_per_source_key,
    test_the_audit_tail_carries_one_row_per_change,
    test_a_second_build_of_the_same_declarations_changes_nothing,
    test_every_published_column_carries_its_declared_type_on_the_built_adapter,
    test_a_changed_declared_type_follows_into_the_built_type,
    test_the_contract_lists_every_relation_the_shape_renders,
    test_every_generated_test_is_written_in_the_nested_arguments_form,
    test_a_duplicated_entity_version_turns_the_key_test_red,
    test_a_duplicated_change_turns_the_audit_grain_test_red,
    test_a_duplicated_cross_reference_key_turns_the_grain_test_red,
    test_a_broken_effective_range_turns_the_contiguity_test_red,
    test_an_entity_naming_a_column_the_composition_does_not_carry_fails_closed,
    test_an_entity_keyed_on_a_column_it_does_not_publish_fails_closed,
    test_an_entity_declared_twice_fails_closed,
    test_a_change_column_that_is_not_an_instant_fails_closed,
    test_a_column_doing_two_roles_at_once_fails_closed,
    test_an_attribute_already_named_as_the_effective_range_fails_closed,
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
    print(f"\n{len(TESTS) - failures}/{len(TESTS)} passed")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
