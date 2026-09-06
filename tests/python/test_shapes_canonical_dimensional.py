"""Self-tests for the two shapes that impose a modelling of their own:
``canonical``, one relation per entity over a curated product at an
interface boundary, and ``dimensional``, facts at a declared grain with
type 1 and type 2 dimensions and the semantic layer over them
(architecture sections 6, 9, 10, 12, 13).

Same plain assert-and-report convention as the rest of this repo (no pytest
in the .venv): each ``test_*`` raises ``AssertionError`` on failure,
``main()`` runs them all and reports PASS/FAIL.

Everything runs against ``tests/fixtures/estates/two_shapes``, copied into a
scratch directory so a red case can mutate one file without touching the
fixture. Every red case drives the real ``ergasterion emit-products``
command, or a real dbt build of what that command wrote, and asserts what
the failure names.

Covers:
  - the command emits all four products, is byte-stable across two runs,
    and ``--check`` is clean on what it wrote;
  - one delivered declaration feeds two products of different shapes and
    its own artefacts are byte-identical whether or not the second shape is
    declared;
  - the canonical product renders one view per declared entity, all of them
    under the interface boundary the estate declares, and the structural
    gate reports no offence against anything that product rendered;
  - the dimensional product renders one relation per fact at its declared
    grain and one per dimension, and two facts naming one conformed
    dimension read a single dimension relation;
  - two dimensional products in one estate, each with its own semantic
    layer, over the one time spine the estate registers as its own
    auxiliary relation and both of them aggregate over;
  - the executed DuckDB build: the type 2 dimension's half-open effective
    ranges, adjacent and open only at the end; the type 1 dimension's
    latest version per key; both facts joining to the same conformed
    dimension; a second build changing nothing; and a newly delivered
    attribute closing the old version and opening a new one;
  - every published column carries its declared type on the built adapter,
    and a changed declared type follows into the built one;
  - both shapes' contracts list every relation the shape renders, with an
    ODCS document per relation;
  - every generated artefact parses for both declared adapters and every
    generated test is written in the nested arguments form;
  - the generated tests go red on what they name: a duplicated dimension
    version, a duplicated fact grain, and a broken effective range;
  - which relation a consumer reads: two consumers name relations of a
    producer that publishes several, as that producer names them, on a
    source and on an enrichment lookup; their rendered models read those
    relations and no others; they build their known answers on DuckDB with
    the types their own contracts declare; the estate graph's edges name
    the relations read; and the fail-closed branches -- a source or a
    lookup naming none of several, naming a relation the producer does not
    publish, repeating the product prefix, naming anything on a producer
    that publishes only its own relation, expecting a sibling relation's
    field, and a fixture-bound source carrying the key;
  - the fail-closed branches: a canonical product whose upstream is not a
    curated product, whose upstream is not a product at all, or which reads
    more than one source; an entity naming a column the composition does
    not carry or keyed on a column it does not publish; a fact naming an
    undeclared dimension; a dimension keyed on two columns; a change column
    that is not an instant, or that is also declared an attribute; a measure
    two facts declare; a metric naming no declared measure; a semantic layer
    with no declared time-spine window; a working column the derivation needs
    for itself; a shape publishing incrementally; a second semantic layer in
    one estate; and an interface boundary the estate does not declare.

Usage:
    python tests/python/test_shapes_canonical_dimensional.py
"""

from __future__ import annotations

import contextlib
import csv
import io
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import traceback
from datetime import date
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
from ergasterion.framework import graph as product_graph
from ergasterion.framework.adapters import load_adapter_conventions
from ergasterion.framework.models import RelationSchema
from ergasterion.framework.shapes import DERIVATION_DIMENSION_TYPE_2, ShapeRelation
from ergasterion.shapes.canonical import INTERFACE_PATH
from ergasterion.shapes.dimensional import EFFECTIVE_FROM, EFFECTIVE_TO
from ergasterion.translators.dbt_patterns import MODELS_ROOT, parse_gate
from ergasterion.translators.dbt_patterns import semantic as semantic_mod
from ergasterion.translators.dbt_patterns import shape_relations as shape_relations_mod
from ergasterion.translators.dbt_patterns import sql as sql_mod
from ergasterion.translators.dbt_patterns.sql import RenderingError

REPO_ROOT = Path(__file__).resolve().parents[2]
FIXTURE_ESTATE = REPO_ROOT / "tests" / "fixtures" / "estates" / "two_shapes"
ENGINE_MACROS = (
    "cross_db.sql",
    "product_tests.sql",
    "publish.sql",
    "quarantine.sql",
    "survivorship.sql",
)

CANONICAL_DIR = f"{MODELS_ROOT}/{INTERFACE_PATH}/crm"
STAR_DIR = f"{MODELS_ROOT}/sales"

CUSTOMER_ENTITY_MODEL = f"{CANONICAL_DIR}/crm__customer_interface__customer.sql"
SEGMENTATION_ENTITY_MODEL = f"{CANONICAL_DIR}/crm__customer_interface__customer_segmentation.sql"
DIM_CUSTOMER_MODEL = f"{STAR_DIR}/sales__order_star__dim_customer.sql"
DIM_CHANNEL_MODEL = f"{STAR_DIR}/sales__order_star__dim_channel.sql"
FACT_ORDER_LINE_MODEL = f"{STAR_DIR}/sales__order_star__fact_order_line.sql"
FACT_ORDER_TAX_MODEL = f"{STAR_DIR}/sales__order_star__fact_order_line_tax.sql"
STAR_SCHEMA = f"{STAR_DIR}/sales__order_star.yml"
STAR_SEMANTIC = f"{STAR_DIR}/sales__order_star__semantic.yml"
VOLUME_STAR_SEMANTIC = f"{STAR_DIR}/sales__order_volume_star__semantic.yml"
SPINE_DIR = f"{MODELS_ROOT}/{semantic_mod.TIME_SPINE_NAMESPACE}"
SPINE_MODEL = f"{SPINE_DIR}/{semantic_mod.TIME_SPINE_MODEL}.sql"
SPINE_SCHEMA = f"{SPINE_DIR}/{semantic_mod.TIME_SPINE_MODEL}.yml"
INTERFACE_SCHEMA = f"{CANONICAL_DIR}/crm__customer_interface.yml"
FEED_MODEL = f"{STAR_DIR}/sales__order_feed.sql"

EXPECTED_CANONICAL_ARTEFACTS = {
    CUSTOMER_ENTITY_MODEL,
    SEGMENTATION_ENTITY_MODEL,
    INTERFACE_SCHEMA,
    f"{CANONICAL_DIR}/crm__customer_interface__base.sql",
    f"{CANONICAL_DIR}/crm__customer_interface__checked.sql",
    f"{CANONICAL_DIR}/crm__customer_interface__quarantine.sql",
    f"{CANONICAL_DIR}/crm__customer_interface__customer_sla.sql",
    f"{CANONICAL_DIR}/crm__customer_interface__customer_segmentation_sla.sql",
}

EXPECTED_STAR_ARTEFACTS = {
    DIM_CUSTOMER_MODEL,
    DIM_CHANNEL_MODEL,
    FACT_ORDER_LINE_MODEL,
    FACT_ORDER_TAX_MODEL,
    STAR_SCHEMA,
    STAR_SEMANTIC,
    f"{STAR_DIR}/sales__order_star__base.sql",
    f"{STAR_DIR}/sales__order_star__checked.sql",
    f"{STAR_DIR}/sales__order_star__quarantine.sql",
    f"{STAR_DIR}/sales__order_star__dim_customer_current.sql",
    f"{STAR_DIR}/sales__order_star__dim_customer_sla.sql",
    f"{STAR_DIR}/sales__order_star__dim_channel_current.sql",
    f"{STAR_DIR}/sales__order_star__dim_channel_sla.sql",
    f"{STAR_DIR}/sales__order_star__fact_order_line_current.sql",
    f"{STAR_DIR}/sales__order_star__fact_order_line_sla.sql",
    f"{STAR_DIR}/sales__order_star__fact_order_line_tax_current.sql",
    f"{STAR_DIR}/sales__order_star__fact_order_line_tax_sla.sql",
}

# The estate's second star, and the one spine both of them aggregate over.
# The spine sits under the estate's own namespace rather than either
# product's, because a dbt project carries one spine per granularity.
EXPECTED_SECOND_STAR_ARTEFACTS = {
    VOLUME_STAR_SEMANTIC,
    f"{STAR_DIR}/sales__order_volume_star.yml",
    f"{STAR_DIR}/sales__order_volume_star__base.sql",
    f"{STAR_DIR}/sales__order_volume_star__dim_order_channel.sql",
    f"{STAR_DIR}/sales__order_volume_star__fact_order_line_volume.sql",
}

EXPECTED_SPINE_ARTEFACTS = {SPINE_MODEL, SPINE_SCHEMA}


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


def _edit_estate(estate: Path, mutate) -> None:
    path = estate / "estate.yml"
    document = yaml.safe_load(path.read_text(encoding="utf-8"))
    mutate(document["estate"])
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

    connection = duckdb.connect(str(estate / "target" / "two_shapes.duckdb"), read_only=True)
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
    it publishes. Read from the declaration rather than restated here, so a
    type assertion is against what was declared."""

    document = yaml.safe_load(_read(estate, "declarations/products/order_feed.yml"))
    step = next(entry for entry in document["steps"] if entry["pattern"] == "schema_transform")
    return {entry["to"]: entry["type"] for entry in step["mapping"] if "to" in entry}


def _physical_type(estate: Path, declared: object) -> str:
    """The type the reference adapter reports for one declared neutral
    type. The adapter's own conventions say which physical type a neutral
    one maps to, and the adapter itself is asked how it spells that type,
    so an alias (DuckDB stores TEXT as VARCHAR) is compared as it is
    reported rather than as a second table in this file claims."""

    mapping = load_adapter_conventions("duckdb").type_mapping
    if isinstance(declared, dict):
        base = mapping[parse_gate.DECIMAL_TYPE_TOKEN].split("(")[0]
        physical = f"{base}({declared['precision']},{declared['scale']})"
    else:
        physical = mapping[sql_mod.SCALAR_TYPE_TOKENS[str(declared)]]
    return str(_query(estate, f"select typeof(cast(null as {physical}))")[0][0])


def _emitted_and_built(root: Path) -> Path:
    estate = _scratch_estate(root)
    _emit(estate)
    _dbt_build(estate)
    return estate


# --------------------------------------------------------------------------- green: the command


def test_the_command_emits_both_shapes_and_re_emission_is_byte_identical() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))
        summary = _emit(estate)
        assert "shape=canonical" in summary and "shape=dimensional" in summary, summary
        first = _tree(estate)
        assert EXPECTED_CANONICAL_ARTEFACTS <= set(first), sorted(
            EXPECTED_CANONICAL_ARTEFACTS - set(first)
        )
        assert EXPECTED_STAR_ARTEFACTS <= set(first), sorted(EXPECTED_STAR_ARTEFACTS - set(first))
        assert EXPECTED_SECOND_STAR_ARTEFACTS <= set(first), sorted(
            EXPECTED_SECOND_STAR_ARTEFACTS - set(first)
        )
        assert EXPECTED_SPINE_ARTEFACTS <= set(first), sorted(
            EXPECTED_SPINE_ARTEFACTS - set(first)
        )

        _emit(estate)
        assert _tree(estate) == first, "a second emission is not byte-identical"

        code, output = _run(estate, "--check")
        assert code == 0, output
        assert "0 problem(s)" in output, output

        # Every emitted model resolves and parses for both declared
        # adapters. The command gates on this before writing anything; this
        # runs the same gate over what it wrote.
        parse_gate.assert_parses(
            {
                path: text.decode("utf-8")
                for path, text in first.items()
                if path.endswith(".sql")
            },
            adapters=("duckdb", "bigquery"),
        )


def test_one_delivered_declaration_feeds_two_shapes_without_changing() -> None:
    # Architecture check 6: one landing product feeds two products of
    # different shapes with no change to it. Proven by emitting the estate
    # twice, once without the dimensional product, and comparing the
    # delivered product's own artefacts byte for byte.
    with tempfile.TemporaryDirectory() as tmp:
        both = _scratch_estate(Path(tmp) / "both")
        _emit(both)
        feed_with_both = {
            path: text for path, text in _tree(both).items() if "order_feed" in path
        }

        one = _scratch_estate(Path(tmp) / "one")
        # The star's own consumer goes with it: a product reads a contract,
        # so removing the producer removes what it published.
        (one / "declarations" / "products" / "order_star.yml").unlink()
        (one / "declarations" / "products" / "order_volume_star.yml").unlink()
        (one / "declarations" / "products" / "order_line_revenue.yml").unlink()
        _emit(one)
        feed_with_one = {path: text for path, text in _tree(one).items() if "order_feed" in path}

        assert feed_with_both == feed_with_one, sorted(
            set(feed_with_both) ^ set(feed_with_one)
        )
        assert _read(both, "declarations/products/order_feed.yml") == _read(
            one, "declarations/products/order_feed.yml"
        )
        shapes = {
            yaml.safe_load(_read(both, f"declarations/products/{name}"))["target"]["shape"]
            for name in ("customer.yml", "order_star.yml")
        }
        assert shapes == {"declared", "dimensional"}, shapes


# --------------------------------------------------------------------------- green: canonical


def test_the_canonical_product_renders_one_view_per_entity_at_the_declared_boundary() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))
        _emit(estate)

        boundary = f"{MODELS_ROOT}/{INTERFACE_PATH}"
        declared = yaml.safe_load(_read(estate, "declarations/targets/interfaces.yml"))
        assert boundary in declared["view_layers"], declared

        canonical_paths = [
            path for path in _tree(estate) if path.startswith(f"{boundary}/")
        ]
        assert canonical_paths, "the canonical product rendered nothing under the boundary"
        for entity_model in (CUSTOMER_ENTITY_MODEL, SEGMENTATION_ENTITY_MODEL):
            assert "materialized='view'" in _read(estate, entity_model), entity_model

        # The structural gate is what judges a view against the declared
        # boundaries, and it reports none against anything this shape
        # rendered: the entity views sit inside the boundary the estate
        # declares, and the shape renders no view outside it.
        offenses = [
            offense
            for offense in structure_gate.check_structure(
                EstateContext.resolve(estate_root=estate)
            )
            if offense.artefact.startswith(f"{boundary}/")
            or offense.artefact.startswith(f"{MODELS_ROOT}/crm/crm__customer_interface")
        ]
        assert not offenses, [str(offense) for offense in offenses]


def test_the_canonical_entities_publish_their_declared_columns_on_duckdb() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _emitted_and_built(Path(tmp))
        customers = _query(
            estate, "select customer_id, customer_name from crm__customer_interface__customer order by customer_id"
        )
        assert customers == [("C-001", "Ada Byron"), ("C-002", "Grace Hopper")], customers
        segmentation = _query(
            estate,
            "select customer_id, customer_segment from crm__customer_interface__customer_segmentation "
            "order by customer_id",
        )
        assert segmentation == [("C-001", "premium"), ("C-002", "premium")], segmentation


# --------------------------------------------------------------------------- green: dimensional


def test_the_dimensional_product_renders_one_relation_per_fact_and_dimension() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))
        _emit(estate)
        manifest = json.loads(_read(estate, "manifests/products/sales/order_star.json"))
        assert manifest["relations"]["published"] == [
            "sales.order_star__dim_customer",
            "sales.order_star__dim_channel",
            "sales.order_star__fact_order_line",
            "sales.order_star__fact_order_line_tax",
        ], manifest["relations"]

        schema = yaml.safe_load(_read(estate, STAR_SCHEMA))
        grain_tests = {
            model["name"]: test["dpf_unique_grain"]["arguments"]["columns"]
            for model in schema["models"]
            for test in model.get("data_tests", [])
            if "dpf_unique_grain" in test
        }
        assert grain_tests["sales__order_star__fact_order_line"] == ["order_id", "line_number"]
        assert grain_tests["sales__order_star__fact_order_line_tax"] == ["order_id", "line_number"]
        assert grain_tests["sales__order_star__dim_customer"] == ["customer_id", EFFECTIVE_FROM]
        assert grain_tests["sales__order_star__dim_channel"] == ["channel_code"]


def test_two_facts_sharing_one_conformed_dimension_read_a_single_relation() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _emitted_and_built(Path(tmp))
        star_models = [
            path
            for path in _tree(estate)
            if path.startswith(f"{STAR_DIR}/") and "dim_customer" in path
        ]
        published = [path for path in star_models if path.endswith("__dim_customer.sql")]
        assert len(published) == 1, star_models

        # Both facts carry the conformed dimension's key, and both join to
        # the rows of that one relation.
        for relation in ("fact_order_line", "fact_order_line_tax"):
            joined = _query(
                estate,
                f"select count(*) from sales__order_star__{relation} as f "
                "join sales__order_star__dim_customer as d on f.customer_id = d.customer_id "
                "and f.order_date >= d.effective_from "
                "and (d.effective_to is null or f.order_date < d.effective_to)",
            )
            assert joined == [(5,)], (relation, joined)


def test_the_type_2_dimension_carries_adjacent_half_open_ranges() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _emitted_and_built(Path(tmp))
        rows = _query(
            estate,
            "select customer_id, customer_segment, effective_from, effective_to "
            "from sales__order_star__dim_customer order by customer_id, effective_from",
        )
        assert [(row[0], row[1], str(row[2]), None if row[3] is None else str(row[3])) for row in rows] == [
            ("C-001", "standard", "2026-01-05", "2026-03-10"),
            ("C-001", "premium", "2026-03-10", None),
            ("C-002", "premium", "2026-02-01", None),
        ], rows


def test_the_type_1_dimension_carries_the_version_the_change_column_ranks_last() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _emitted_and_built(Path(tmp))
        rows = _query(
            estate,
            "select channel_code, channel_name from sales__order_star__dim_channel order by channel_code",
        )
        assert rows == [("CH-APP", "Mobile app"), ("CH-WEB", "Web store")], rows


def test_a_changed_attribute_closes_the_old_version_and_opens_a_new_one() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _emitted_and_built(Path(tmp))
        before = _query(
            estate,
            "select count(*) from sales__order_star__dim_customer where customer_id = 'C-002'",
        )
        assert before == [(1,)], before

        _append_seed_row(
            estate,
            "customer_orders",
            "R-06,orders,O-1005,1,2026-05-04,C-002,Grace Hopper,standard,CH-APP,Mobile app,2,120.00,24.00,2026-05-04",
        )
        _dbt_build(estate)
        after = _query(
            estate,
            "select customer_segment, effective_from, effective_to from sales__order_star__dim_customer "
            "where customer_id = 'C-002' order by effective_from",
        )
        assert [(row[0], str(row[1]), None if row[2] is None else str(row[2])) for row in after] == [
            ("premium", "2026-02-01", "2026-05-04"),
            ("standard", "2026-05-04", None),
        ], after


def test_a_second_build_of_the_same_declarations_changes_nothing() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _emitted_and_built(Path(tmp))
        relations = (
            "sales__order_star__dim_customer",
            "sales__order_star__dim_channel",
            "sales__order_star__fact_order_line",
            "sales__order_star__fact_order_line_tax",
            "crm__customer_interface__customer",
            "crm__customer_interface__customer_segmentation",
            "sales__order_line_revenue",
            "crm__customer_segment_report",
        )
        before = {
            relation: _query(estate, f"select * from {relation}") for relation in relations
        }
        _dbt_build(estate)
        after = {relation: _query(estate, f"select * from {relation}") for relation in relations}
        for relation in relations:
            assert sorted(map(str, before[relation])) == sorted(map(str, after[relation])), relation


# --------------------------------------------------------------------------- green: types, contracts, gates


def test_every_published_column_carries_its_declared_type_on_the_built_adapter() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _emitted_and_built(Path(tmp))
        declared = _declared_types(estate)

        fact = _built_types(estate, "sales__order_star__fact_order_line")
        assert set(fact) == {
            "order_id",
            "line_number",
            "customer_id",
            "channel_code",
            "order_date",
            "quantity",
            "net_amount",
        }, fact
        for column, built in fact.items():
            assert built == _physical_type(estate, declared[column]), (column, built)

        dimension = _built_types(estate, "sales__order_star__dim_customer")
        for column, built in dimension.items():
            # The two effective-range columns publish as the change column
            # the dimension is versioned by.
            source = "changed_at" if column in (EFFECTIVE_FROM, EFFECTIVE_TO) else column
            assert built == _physical_type(estate, declared[source]), (column, built)

        entity = _built_types(estate, "crm__customer_interface__customer")
        for column, built in entity.items():
            assert built == _physical_type(estate, declared[column]), (column, built)


def test_a_changed_declared_type_follows_into_the_built_type() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _emitted_and_built(Path(tmp))
        before = _built_types(estate, "sales__order_star__fact_order_line")["net_amount"]
        assert before == _physical_type(estate, _declared_types(estate)["net_amount"]), before

        def restate(document: dict) -> None:
            for entry in document["steps"]:
                if entry["pattern"] != "schema_transform":
                    continue
                for mapping_entry in entry["mapping"]:
                    if mapping_entry.get("to") == "net_amount":
                        mapping_entry["type"] = "string"

        _edit_declaration(estate, "order_feed.yml", restate)
        _emit(estate)
        _dbt_build(estate)
        after = _built_types(estate, "sales__order_star__fact_order_line")["net_amount"]
        assert after == _physical_type(estate, "string"), after
        assert after != before, after


def test_both_shapes_publish_a_contract_listing_every_relation_they_render() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))
        _emit(estate)
        ctx = EstateContext.resolve(estate_root=estate)
        # The schemas this estate opens with are its fixture-bound source's
        # declared fields, exactly as the product route resolves them.
        _plans, entries, _graph = ep.build_product_plans(ctx)
        landing = contract_mod.fixture_relation_schemas(entries)
        contracts = ec.build_product_contracts(ctx, landing_schemas=landing)

        canonical = contracts["crm.customer_interface"]
        assert [relation.name for relation in canonical.relations] == [
            "crm.customer_interface__customer",
            "crm.customer_interface__customer_segmentation",
        ], canonical.relations

        dimensional = contracts["sales.order_star"]
        assert [relation.name for relation in dimensional.relations] == [
            "sales.order_star__dim_customer",
            "sales.order_star__dim_channel",
            "sales.order_star__fact_order_line",
            "sales.order_star__fact_order_line_tax",
        ], dimensional.relations

        files = ec.generate_products(ctx, landing_schemas=landing)
        odcs = sorted(
            path.name for path in files if path.name.endswith(".odcs.yml") and "order_star" in str(path)
        )
        assert len(odcs) == 4, odcs


def test_every_generated_test_is_written_in_the_nested_arguments_form() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))
        _emit(estate)
        for path in (STAR_SCHEMA, INTERFACE_SCHEMA):
            document = yaml.safe_load(_read(estate, path))
            for model in document["models"]:
                for entry in model.get("data_tests", []):
                    for name, body in entry.items():
                        assert set(body) <= {"name", "arguments", "config"}, (path, name, body)
                        assert "config" in body and "severity" in body["config"], (path, name)


def test_the_semantic_document_declares_the_measures_metrics_and_time_spine() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        # dbt parses the semantic layer as part of the build; a document
        # dbt could not parse would fail here rather than being asserted
        # against a copy of itself.
        estate = _emitted_and_built(Path(tmp))
        document = yaml.safe_load(_read(estate, STAR_SEMANTIC))
        # A product's own document names only its own relations: the spine
        # belongs to the estate and is described once, beside itself.
        assert "models" not in document, document
        spine_document = yaml.safe_load(_read(estate, SPINE_SCHEMA))
        assert spine_document["models"][0]["time_spine"] == {
            "standard_granularity_column": "date_day"
        }, spine_document["models"]
        measures = {
            measure["name"]
            for model in document["semantic_models"]
            for measure in model.get("measures", [])
        }
        assert measures == {"quantity", "net_amount", "tax_amount"}, measures
        metrics = {metric["name"]: metric["type_params"]["measure"] for metric in document["metrics"]}
        assert metrics == {
            "total_net_amount": "net_amount",
            "total_tax_amount": "tax_amount",
        }, metrics
        validity = [
            dimension["type_params"]["validity_params"]
            for model in document["semantic_models"]
            for dimension in model.get("dimensions", [])
            if "validity_params" in dimension.get("type_params", {})
        ]
        assert validity == [{"is_start": True}, {"is_end": True}], validity
        # One spine, over the window covering both stars' declared windows:
        # 2026-01-01 to 2026-04-30 and 2026-05-01 to 2026-06-30.
        spine = _query(
            estate,
            f"select count(*), min(date_day), max(date_day) from {semantic_mod.TIME_SPINE_MODEL}",
        )
        assert spine == [(181, date(2026, 1, 1), date(2026, 6, 30))], spine


# --------------------------------------------------------------------------- red: the generated tests


def test_a_duplicated_dimension_version_turns_the_key_test_red() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _emitted_and_built(Path(tmp))
        _append_seed_row(
            estate,
            "customer_orders",
            "R-07,orders,O-1006,1,2026-01-05,C-001,Ada Lovelace,standard,CH-WEB,Web,1,10.00,2.00,2026-01-05",
        )
        _dbt_fails(estate, "dpf_unique_grain_sales__order_star__dim_customer")


def test_a_duplicated_fact_grain_turns_the_grain_test_red() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _emitted_and_built(Path(tmp))
        _append_seed_row(
            estate,
            "customer_orders",
            "R-08,orders,O-1001,1,2026-01-05,C-001,Ada Byron,standard,CH-WEB,Web,9,900.00,180.00,2026-01-05",
        )
        _dbt_fails(estate, "dpf_unique_grain_sales__order_star__fact_order_line")


def test_a_broken_effective_range_turns_the_contiguity_test_red() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _emitted_and_built(Path(tmp))
        model = estate / DIM_CUSTOMER_MODEL
        text = model.read_text(encoding="utf-8")
        broken = text.replace(
            "lead(effective_from) over (partition by customer_id order by effective_from) as effective_to",
            "cast(null as date) as effective_to",
        )
        assert broken != text, "the derivation this test breaks is not in the generated model"
        model.write_text(broken, encoding="utf-8")
        _dbt_fails(estate, "dpf_effective_range_contiguity_sales__order_star__dim_customer")


# --------------------------------------------------------------------------- red: canonical constraints


def test_a_canonical_product_over_an_uncurated_upstream_fails_closed() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))
        _edit_declaration(
            estate,
            "customer_interface.yml",
            lambda document: document.update(
                sources=[{"contract": "sales.order_feed@1"}],
            ),
        )
        _fails(estate, "crm.customer_interface", "canonical", "sales.order_feed", "data_curation")


def test_a_canonical_product_over_a_source_no_product_publishes_fails_closed() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))
        _edit_declaration(
            estate,
            "customer_interface.yml",
            lambda document: document.update(
                sources=[
                    {
                        "contract": "crm.customer_extract@1",
                        "kind": "fixture",
                        "fixture": {
                            "relation": "customer_orders",
                            "fields": [
                                {"name": "customer_id", "type": "string"},
                                {"name": "customer_name", "type": "string"},
                                {"name": "customer_segment", "type": "string"},
                                {"name": "changed_at", "type": "date"},
                            ],
                        },
                    }
                ],
            ),
        )
        _fails(estate, "crm.customer_interface", "canonical", "crm.customer_extract")


def test_a_canonical_product_reading_two_sources_fails_closed() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))
        _edit_declaration(
            estate,
            "customer_interface.yml",
            lambda document: document.update(
                sources=[{"contract": "crm.customer@1"}, {"contract": "sales.order_feed@1"}],
                combine={"method": "merge", "keys": ["customer_id"], "join": "outer"},
            ),
        )
        _fails(estate, "crm.customer_interface", "canonical", "one curated product")


def test_an_entity_naming_a_column_the_composition_does_not_carry_fails_closed() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))

        def mutate(document: dict) -> None:
            _shape_config(document)["entities"][0]["columns"].append("loyalty_tier")

        _edit_declaration(estate, "customer_interface.yml", mutate)
        _fails(estate, "crm.customer_interface", "canonical", "loyalty_tier")


def test_an_entity_keyed_on_a_column_it_does_not_publish_fails_closed() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))

        def mutate(document: dict) -> None:
            _shape_config(document)["entities"][0]["key"] = ["customer_segment"]

        _edit_declaration(estate, "customer_interface.yml", mutate)
        _fails(estate, "crm.customer_interface", "canonical", "customer_segment")


def test_an_interface_boundary_the_estate_does_not_declare_fails_closed() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))
        path = estate / "declarations" / "targets" / "interfaces.yml"
        path.write_text("view_layers: []\n", encoding="utf-8")
        _fails(
            estate,
            "crm.customer_interface",
            "canonical",
            f"{MODELS_ROOT}/{INTERFACE_PATH}",
        )


# --------------------------------------------------------------------------- red: dimensional constraints


def test_a_fact_naming_an_undeclared_dimension_fails_closed() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))

        def mutate(document: dict) -> None:
            _shape_config(document)["facts"][0]["dimensions"].append("supplier")

        _edit_declaration(estate, "order_star.yml", mutate)
        _fails(estate, "sales.order_star", "dimensional", "supplier")


def test_a_dimension_keyed_on_two_columns_fails_closed() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))

        def mutate(document: dict) -> None:
            _shape_config(document)["dimensions"][0]["key"] = ["customer_id", "source_system"]

        _edit_declaration(estate, "order_star.yml", mutate)
        _fails(estate, "sales.order_star", "dimensional", "one key column")


def test_a_change_column_that_is_not_an_instant_fails_closed() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))

        def mutate(document: dict) -> None:
            _shape_config(document)["dimensions"][0]["change_column"] = "customer_name"

        _edit_declaration(estate, "order_star.yml", mutate)
        _fails(estate, "sales.order_star", "dimensional", "customer_name")


def test_one_measure_declared_by_two_facts_fails_closed() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))

        def mutate(document: dict) -> None:
            _shape_config(document)["facts"][1]["measures"].append(
                {"name": "net_amount", "aggregation": "sum"}
            )

        _edit_declaration(estate, "order_star.yml", mutate)
        _fails(estate, "sales.order_star", "dimensional", "net_amount", "order_line")


def test_a_semantic_layer_with_no_declared_time_spine_fails_closed() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))

        def mutate(document: dict) -> None:
            _shape_config(document).pop("time_spine")

        _edit_declaration(estate, "order_star.yml", mutate)
        # Layer 1 names the product by its declared name, and the shape's
        # own schema names the key it requires.
        _fails(estate, "order_star", "time_spine", "required property")


def test_a_metric_naming_no_declared_measure_fails_closed() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))

        def mutate(document: dict) -> None:
            _shape_config(document)["metrics"][0]["measure"] = "margin"

        _edit_declaration(estate, "order_star.yml", mutate)
        _fails(estate, "sales.order_star", "dimensional", "margin")


def test_a_dimension_carrying_its_change_column_as_an_attribute_fails_closed() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))

        def mutate(document: dict) -> None:
            _shape_config(document)["dimensions"][0]["attributes"].append("changed_at")

        _edit_declaration(estate, "order_star.yml", mutate)
        _fails(estate, "sales.order_star", "customer", "changed_at")


def test_a_composition_column_clashing_with_a_working_column_fails_closed() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))
        # The occurrence this red case adds needs an owner like any other,
        # so the estate's table gives it one.
        _edit_estate(
            estate,
            lambda block: block["translators"]["published"].update(calculated_fields="dbt"),
        )

        def mutate(document: dict) -> None:
            document["steps"].insert(
                2,
                {
                    "pattern": "calculated_fields",
                    "fields": [
                        {
                            "name": "dpf_version_rank",
                            "type": "string",
                            "expression": "upper(customer_id)",
                        }
                    ],
                },
            )
            _shape_config(document)["dimensions"][0]["attributes"].append("dpf_version_rank")

        _edit_declaration(estate, "order_star.yml", mutate)
        _fails(estate, "sales.order_star", "dpf_version_rank")


def test_a_shape_publishing_incrementally_fails_closed() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))

        def mutate(document: dict) -> None:
            for step in document["steps"]:
                if step["pattern"] == "data_publish":
                    step["publication_mode"] = "incremental"
                    step["unique_key"] = ["order_id"]

        _edit_declaration(estate, "order_star.yml", mutate)
        _fails(estate, "sales.order_star", "atomic", "incremental")


def test_a_consumer_of_a_product_publishing_several_relations_fails_closed() -> None:
    # The consumer here carries the declared shape, which adds no
    # constraint of its own, so the failure this case is about is the one
    # the read itself produces.
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))
        _edit_declaration(
            estate,
            "customer_segment_report.yml",
            lambda document: document.update(sources=[{"contract": "sales.order_star@1"}]),
        )
        _fails(
            estate,
            "crm.customer_segment_report",
            "sales.order_star",
            "sales.order_star__dim_customer",
            "sales.order_star__fact_order_line_tax",
            "which relation it reads",
            "dim_customer, dim_channel, fact_order_line, fact_order_line_tax",
        )


def test_a_derivation_this_translator_has_no_arm_for_fails_closed() -> None:
    # Every registered shape declares only derivations this translator
    # renders, so no declaration reaches this branch. It is the guard that
    # keeps a shape registered later from rendering as a guess, and it is
    # driven here through the real rendering rather than a mock.
    relation = ShapeRelation(
        suffix="pivoted",
        schema=RelationSchema(name="sales.order_star__pivoted", fields=()),
        derivation="pivot",
        source_columns=("order_id",),
    )
    try:
        shape_relations_mod.render_relation(
            relation,
            shape_relations_mod.RelationContext(
                base_model="sales__order_star__base", product="sales.order_star"
            ),
        )
    except RenderingError as error:
        assert "pivot" in str(error), str(error)
    else:
        raise AssertionError("expected a rendering failure for a derivation with no arm")

    without_range = ShapeRelation(
        suffix="dim_customer",
        schema=RelationSchema(name="sales.order_star__dim_customer", fields=()),
        derivation=DERIVATION_DIMENSION_TYPE_2,
        source_columns=("customer_id",),
        key=("customer_id",),
        change_column="changed_at",
    )
    try:
        shape_relations_mod.render_relation(
            without_range,
            shape_relations_mod.RelationContext(
                base_model="sales__order_star__base", product="sales.order_star"
            ),
        )
    except RenderingError as error:
        assert "effective range" in str(error), str(error)
    else:
        raise AssertionError("expected a rendering failure for a type 2 relation with no range")


def test_two_stars_in_one_estate_share_one_time_spine() -> None:
    """Two dimensional products in one estate emit, build on the reference
    adapter and both parse their semantic layers, over one spine registered
    once for the estate.

    A dbt project carries one time spine per granularity. The spine is
    therefore the estate's shared relation, rendered over the window that
    covers every window its products declared, and no product renders one
    of its own."""

    with tempfile.TemporaryDirectory() as tmp:
        estate = _emitted_and_built(Path(tmp))

        # Exactly one spine relation in the whole tree, under the estate's
        # namespace and not under either product's.
        spines = sorted(
            path.relative_to(estate).as_posix()
            for path in (estate / MODELS_ROOT).rglob(
                f"*{semantic_mod.TIME_SPINE_SUFFIX}*.sql"
            )
        )
        assert spines == [SPINE_MODEL], spines

        # Both stars publish a semantic layer, and each reads only its own
        # relations.
        for path, expected in (
            (STAR_SEMANTIC, {"total_net_amount", "total_tax_amount"}),
            (VOLUME_STAR_SEMANTIC, {"total_line_count"}),
        ):
            document = yaml.safe_load(_read(estate, path))
            assert {metric["name"] for metric in document["metrics"]} == expected, path
            assert "models" not in document, path

        # dbt resolved one spine for the project and both stars' metrics
        # against it.
        manifest = json.loads(_read(estate, "target/semantic_manifest.json"))
        spine_relations = [
            entry["node_relation"]["alias"]
            for entry in manifest["project_configuration"]["time_spines"]
        ]
        assert spine_relations == [semantic_mod.TIME_SPINE_MODEL], spine_relations
        assert {metric["name"] for metric in manifest["metrics"]} == {
            "total_net_amount",
            "total_tax_amount",
            "total_line_count",
        }, manifest["metrics"]

        # The spine is registered once, as an auxiliary relation of the
        # estate rather than of either star, and the graph places it: the
        # emission above re-resolves the graph with these registrations and
        # fails closed on one it cannot place.
        ctx = EstateContext.resolve(estate_root=estate)
        plans, entries, _graph, _ledgers = ep.plan_estate(ctx)
        translator = ep.DbtProductTranslator(
            products=tuple(plan for plan in plans if plan.shape_owner == "dbt")
        )
        auxiliary = [
            entry
            for entry in translator.auxiliary_relations()
            if entry.relation == semantic_mod.TIME_SPINE_RELATION
        ]
        assert len(auxiliary) == 1, translator.auxiliary_relations()
        assert auxiliary[0].product == product_graph.ESTATE_SCOPE, auxiliary[0]
        placed = product_graph.build_product_graph(
            entries, auxiliary=product_graph.collect_auxiliary_relations([translator])
        )
        assert semantic_mod.TIME_SPINE_RELATION in {
            entry.relation for entry in placed.auxiliary
        }, placed.auxiliary

        # The second star's known answer on the built adapter: one row per
        # delivered order line, carrying net plus tax.
        rows = _query(
            estate,
            "select count(*), sum(line_count) from sales__order_volume_star__fact_order_line_volume",
        )
        assert rows == [(5, 5)], rows


# --------------------------------------------------------------------------- green: which relation a consumer reads


def _refs(text: str) -> set[str]:
    """Every model this artefact reads through dbt's ref()."""

    return set(re.findall(r'ref\("([^"]+)"\)', text))


def _product_refs(estate: Path, prefix: str) -> set[str]:
    """Every model the artefacts of one product read, minus the ones that
    product renders for itself: what is left is what it reads from the rest
    of the estate."""

    own: set[str] = set()
    read: set[str] = set()
    for path in sorted((estate / MODELS_ROOT).rglob("*.sql")):
        name = path.stem
        if not name.startswith(prefix):
            continue
        own.add(name)
        read |= _refs(path.read_text(encoding="utf-8"))
    return read - own


def test_a_consumer_reads_the_relation_its_source_names_and_no_other() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))
        summary = _emit(estate)
        assert "emitted sales.order_line_revenue" in summary, summary
        assert "emitted crm.customer_segment_report" in summary, summary

        # The dimensional product publishes four relations; the consumer
        # reads the one its source names and the one its lookup names, and
        # nothing else of that product.
        assert _product_refs(estate, "sales__order_line_revenue") == {
            "sales__order_star__fact_order_line",
            "sales__order_star__dim_channel",
        }, _product_refs(estate, "sales__order_line_revenue")
        # The source read and the lookup join are each rendered over their
        # own relation, in the model that declares them.
        assert 'ref("sales__order_star__fact_order_line")' in _read(
            estate, f"{STAR_DIR}/sales__order_line_revenue__checked.sql"
        )
        revenue = _read(estate, f"{STAR_DIR}/sales__order_line_revenue.sql")
        assert 'ref("sales__order_star__dim_channel")' in revenue, revenue
        assert "dim_customer" not in revenue and "fact_order_line_tax" not in revenue, revenue
        # The canonical product publishes one relation per entity; the
        # consumer reads the segmentation entity, not the sibling view.
        assert _product_refs(estate, "crm__customer_segment_report") == {
            "crm__customer_interface__customer_segmentation"
        }, _product_refs(estate, "crm__customer_segment_report")

        parse_gate.assert_parses(
            {
                name: (estate / name).read_text(encoding="utf-8")
                for name in _tree(estate)
                if name.endswith(".sql")
            },
            adapters=("duckdb", "bigquery"),
        )
        code, output = _run(estate, "--check")
        assert code == 0 and "0 problem(s)" in output, output


def test_the_consumers_carry_their_known_answers_and_declared_types_on_duckdb() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _emitted_and_built(Path(tmp))

        revenue = _query(
            estate,
            "select order_id, line_number, quantity, net_amount, multi_unit_line, channel_name "
            "from sales__order_line_revenue order by order_id, line_number",
        )
        # The fact's own columns, this composition's calculated field, and
        # the attribute the lookup carried in from the channel dimension.
        assert [(row[0], row[1], row[2], str(row[3]), row[4], row[5]) for row in revenue] == [
            ("O-1001", 1, 2, "100.00", True, "Web store"),
            ("O-1001", 2, 1, "50.00", False, "Web store"),
            ("O-1002", 1, 3, "300.00", True, "Web store"),
            ("O-1003", 1, 5, "250.00", True, "Mobile app"),
            ("O-1004", 1, 1, "75.00", False, "Mobile app"),
        ], revenue
        assert str(_query(estate, "select sum(net_amount) from sales__order_line_revenue")[0][0]) == (
            "775.00"
        )

        report = _query(
            estate,
            "select customer_id, customer_segment from crm__customer_segment_report "
            "order by customer_id",
        )
        assert report == [("C-001", "premium"), ("C-002", "premium")], report

        # The relation a consumer built carries the types its own contract
        # declares, through the reference adapter's mapping. The contract is
        # derived from the named relation's schema, so this is the read
        # relation's typing arriving intact at the consumer's own relation.
        ctx = EstateContext.resolve(estate_root=estate)
        _plans, entries, _graph = ep.build_product_plans(ctx)
        contracts = ec.build_product_contracts(
            ctx, landing_schemas=contract_mod.fixture_relation_schemas(entries)
        )
        for published, relation_name in (
            ("sales.order_line_revenue", "sales__order_line_revenue"),
            ("crm.customer_segment_report", "crm__customer_segment_report"),
        ):
            declared = contracts[published].relations[0]
            built = _built_types(estate, relation_name)
            assert set(built) == {field.name for field in declared.fields}, (published, built)
            for field in declared.fields:
                assert built[field.name] == _physical_type(estate, field.type), (
                    published,
                    field.name,
                    built[field.name],
                )


def test_the_estate_graph_edge_names_the_relation_the_consumer_reads() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            code = cli.main(["product-graph", "--estate-root", str(estate)])
        assert code == 0, out.getvalue() + err.getvalue()

        rows = list(
            csv.DictReader(
                io.StringIO(
                    _read(estate, f"graphs/products/{product_graph.EDGES_ARTEFACT}")
                )
            )
        )
        by_target = {
            (row["dst"], row["src"], row["kind"]): row["relation"] for row in rows
        }
        assert by_target[("sales.order_line_revenue", "sales.order_star", "source")] == (
            "sales.order_star__fact_order_line"
        ), rows
        # The enrichment lookup is its own edge, at the relation it reads.
        assert by_target[("sales.order_line_revenue", "sales.order_star", "lookup")] == (
            "sales.order_star__dim_channel"
        ), rows
        assert by_target[
            ("crm.customer_segment_report", "crm.customer_interface", "source")
        ] == "crm.customer_interface__customer_segmentation", rows
        # A producer publishing one relation is still named at relation
        # level: the edge carries the relation, never the product's set.
        assert by_target[("crm.customer", "sales.order_feed", "source")] == (
            "sales.order_feed"
        ), rows


# --------------------------------------------------------------------------- red: which relation a consumer reads


def test_a_consumer_naming_a_relation_the_contract_does_not_list_fails_closed() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))
        _edit_declaration(
            estate,
            "order_line_revenue.yml",
            lambda document: document["sources"][0].update(relation="fact_absent"),
        )
        _fails(
            estate,
            "sales.order_line_revenue",
            "sales.order_star",
            "fact_absent",
            "fact_order_line",
        )


def test_a_source_repeating_the_product_prefix_is_told_the_form_expected() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))
        _edit_declaration(
            estate,
            "order_line_revenue.yml",
            lambda document: document["sources"][0].update(
                relation="sales.order_star__fact_order_line"
            ),
        )
        # The source already names the product; the key names the relation
        # as its producer names it.
        _fails(
            estate,
            "sales.order_line_revenue",
            "sales.order_star",
            "without the 'sales.order_star' prefix",
        )


def test_a_lookup_naming_no_relation_of_several_fails_closed_as_a_lookup() -> None:
    def drop_the_lookup_key(document: dict) -> None:
        step = next(s for s in document["steps"] if s["pattern"] == "data_enrichment")
        step["lookups"][0].pop("relation")

    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))
        _edit_declaration(estate, "order_line_revenue.yml", drop_the_lookup_key)
        _fails(
            estate,
            "sales.order_line_revenue",
            "lookup 'sales.order_star'",
            "key on the lookup naming one of",
            "dim_channel",
        )


def test_a_lookup_naming_a_relation_the_producer_does_not_publish_fails_closed() -> None:
    def rename_the_lookup_key(document: dict) -> None:
        step = next(s for s in document["steps"] if s["pattern"] == "data_enrichment")
        step["lookups"][0]["relation"] = "dim_absent"

    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))
        _edit_declaration(estate, "order_line_revenue.yml", rename_the_lookup_key)
        _fails(
            estate,
            "sales.order_line_revenue",
            "lookup 'sales.order_star'",
            "dim_absent",
            "dim_channel",
        )


def test_a_key_on_a_producer_publishing_only_its_own_relation_fails_closed() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))
        _edit_declaration(
            estate,
            "customer.yml",
            lambda document: document["sources"][0].update(relation="extra"),
        )
        _fails(estate, "crm.customer", "sales.order_feed", "extra")


def test_a_consumer_dropping_its_relation_key_fails_closed_rather_than_defaulting() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))
        before = _tree(estate)
        _edit_declaration(
            estate,
            "order_line_revenue.yml",
            lambda document: document["sources"][0].pop("relation"),
        )
        output = _fails(
            estate,
            "sales.order_line_revenue",
            "sales.order_star",
            "sales.order_star__dim_customer",
            "sales.order_star__fact_order_line_tax",
            "which relation it reads",
            "key on the source naming one of: dim_customer, dim_channel, "
            "fact_order_line, fact_order_line_tax",
        )
        assert "publishes 4 relations" in output, output
        # Nothing was written: the first of the four is not a default.
        assert _tree(estate) == before, "a failed emission wrote artefacts"


def test_a_consumer_expecting_a_sibling_relations_field_fails_closed() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))
        _edit_declaration(
            estate,
            "order_line_revenue.yml",
            lambda document: document["sources"][0]["expect"]["fields"].append("tax_amount"),
        )
        # tax_amount is the sibling fact's measure. The consumer sees the
        # named relation's schema only, so expecting it fails closed.
        _fails(estate, "sales.order_line_revenue", "sales.order_star", "tax_amount")


def test_a_fixture_bound_source_naming_a_relation_fails_closed() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))
        _edit_declaration(
            estate,
            "order_feed.yml",
            lambda document: document["sources"][0].update(relation="customer_orders"),
        )
        # Layer 1 names the declaration by its product name.
        _fails(estate, "order_feed", "sources[0]", "fixture_source_names_a_relation")


# --------------------------------------------------------------------------- runner


TESTS = [
    test_the_command_emits_both_shapes_and_re_emission_is_byte_identical,
    test_one_delivered_declaration_feeds_two_shapes_without_changing,
    test_the_canonical_product_renders_one_view_per_entity_at_the_declared_boundary,
    test_the_canonical_entities_publish_their_declared_columns_on_duckdb,
    test_the_dimensional_product_renders_one_relation_per_fact_and_dimension,
    test_two_facts_sharing_one_conformed_dimension_read_a_single_relation,
    test_the_type_2_dimension_carries_adjacent_half_open_ranges,
    test_the_type_1_dimension_carries_the_version_the_change_column_ranks_last,
    test_a_changed_attribute_closes_the_old_version_and_opens_a_new_one,
    test_a_second_build_of_the_same_declarations_changes_nothing,
    test_every_published_column_carries_its_declared_type_on_the_built_adapter,
    test_a_changed_declared_type_follows_into_the_built_type,
    test_both_shapes_publish_a_contract_listing_every_relation_they_render,
    test_every_generated_test_is_written_in_the_nested_arguments_form,
    test_the_semantic_document_declares_the_measures_metrics_and_time_spine,
    test_a_duplicated_dimension_version_turns_the_key_test_red,
    test_a_duplicated_fact_grain_turns_the_grain_test_red,
    test_a_broken_effective_range_turns_the_contiguity_test_red,
    test_a_canonical_product_over_an_uncurated_upstream_fails_closed,
    test_a_canonical_product_over_a_source_no_product_publishes_fails_closed,
    test_a_canonical_product_reading_two_sources_fails_closed,
    test_an_entity_naming_a_column_the_composition_does_not_carry_fails_closed,
    test_an_entity_keyed_on_a_column_it_does_not_publish_fails_closed,
    test_an_interface_boundary_the_estate_does_not_declare_fails_closed,
    test_a_fact_naming_an_undeclared_dimension_fails_closed,
    test_a_dimension_keyed_on_two_columns_fails_closed,
    test_a_change_column_that_is_not_an_instant_fails_closed,
    test_one_measure_declared_by_two_facts_fails_closed,
    test_a_metric_naming_no_declared_measure_fails_closed,
    test_a_semantic_layer_with_no_declared_time_spine_fails_closed,
    test_a_dimension_carrying_its_change_column_as_an_attribute_fails_closed,
    test_a_composition_column_clashing_with_a_working_column_fails_closed,
    test_a_shape_publishing_incrementally_fails_closed,
    test_a_consumer_of_a_product_publishing_several_relations_fails_closed,
    test_a_derivation_this_translator_has_no_arm_for_fails_closed,
    test_two_stars_in_one_estate_share_one_time_spine,
    test_a_consumer_reads_the_relation_its_source_names_and_no_other,
    test_the_consumers_carry_their_known_answers_and_declared_types_on_duckdb,
    test_the_estate_graph_edge_names_the_relation_the_consumer_reads,
    test_a_consumer_naming_a_relation_the_contract_does_not_list_fails_closed,
    test_a_source_repeating_the_product_prefix_is_told_the_form_expected,
    test_a_lookup_naming_no_relation_of_several_fails_closed_as_a_lookup,
    test_a_lookup_naming_a_relation_the_producer_does_not_publish_fails_closed,
    test_a_key_on_a_producer_publishing_only_its_own_relation_fails_closed,
    test_a_consumer_dropping_its_relation_key_fails_closed_rather_than_defaulting,
    test_a_consumer_expecting_a_sibling_relations_field_fails_closed,
    test_a_fixture_bound_source_naming_a_relation_fails_closed,
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
