"""Self-tests for the estate product graph (architecture sections 4, 8, 10
and 12): ``ergasterion/framework/graph.py``, the product route of
``ergasterion/emit_graph.py``, and the Lineage Capture capability the
publication translator carries.

Same plain assert-and-report convention as the rest of this repo (no
pytest in the .venv): each ``test_*`` raises ``AssertionError`` on failure,
``main()`` runs them all and reports PASS/FAIL.

Covers:
  - the publication translator registers Lineage Capture on every declared
    adapter, the real estate table routes it there under every label, and
    dbt registers nothing;
  - the six-product fixture estate under tests/fixtures/estates/graph_six/
    resolves to one documented topological order with every generation
    labelled structurally, through the real ``ergasterion product-graph``
    command;
  - a cycle, a source contract no product publishes, an enrichment lookup
    no product publishes, and a non-landing product with no source each
    fail closed naming the products;
  - the consolidation product's reconciliation rule references two upstream
    contracts, renders to a validation occurrence, reaches the contract as
    a consistency guarantee, and fails closed when it names a contract the
    product does not consume or carries fewer than two;
  - product-level lineage is the contract edge set and field-level lineage
    covers the four declaration mechanisms, with calculated-field lineage
    taken from the columns the parser resolved;
  - the checkpoint flag forces table materialisation intent and registers
    the relation, and is admitted only as a boolean;
  - published relation names are derived from the declaration and byte
    stable across two emissions;
  - a translator's private auxiliary relations register as auxiliary
    lineage, and an unplaceable registration fails closed;
  - the product route reads no domain-section vocabulary: no hub or link
    configuration, no ``ergasterion.graph_model`` import, no domains
    directory;
  - the command writes, then checks clean, then reports drift on a
    hand-edited artefact, on a scratch estate.

Usage:
    python tests/python/test_product_graph.py
"""

from __future__ import annotations

import contextlib
import copy
import dataclasses
import csv
import io
import json
import shutil
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
from ergasterion import emit_graph as eg
from ergasterion.estate import EstateContext, load_estate_adapters, load_translator_table
from ergasterion.framework import declaration as declaration_mod
from ergasterion.framework import graph as pg
from ergasterion.framework.adapters import ADAPTER_NAMES
from ergasterion.framework.contract import build_quality_guarantees
from ergasterion.framework.models import Capability, ExecutionPlan, Occurrence, PatternId, Role
from ergasterion.framework.routing import TranslationRouter
from ergasterion.translators.publication import (
    GRAPH_ARTEFACT_PREFIX,
    PUBLICATION_PATTERNS,
    PublicationTranslator,
)

FIXTURE_ESTATE = Path(__file__).resolve().parent.parent / "fixtures" / "estates" / "graph_six"
FIXTURE_PRODUCTS = FIXTURE_ESTATE / "declarations" / "products"
FIVE_PROFILE_PRODUCTS = Path(__file__).resolve().parent.parent / "fixtures" / "products" / "valid"

DOCUMENTED_ORDER = (
    "retail.catalogue_landing",
    "retail.orders_landing",
    "retail.order",
    "retail.product_catalogue",
    "retail.order_360",
    "retail.order_mart",
)
DOCUMENTED_GENERATIONS = {
    "retail.catalogue_landing": pg.GENERATION_LANDING,
    "retail.orders_landing": pg.GENERATION_LANDING,
    "retail.order": pg.GENERATION_FIRST,
    "retail.product_catalogue": pg.GENERATION_FIRST,
    "retail.order_360": pg.GENERATION_CONSOLIDATING,
    "retail.order_mart": pg.GENERATION_LATER,
}


# --------------------------------------------------------------------------- helpers


def _fixture_document(file_name: str) -> dict:
    return yaml.safe_load((FIXTURE_PRODUCTS / file_name).read_text(encoding="utf-8")) or {}


@contextlib.contextmanager
def scratch_estate(*, overlay: dict[str, dict] | None = None, remove: tuple[str, ...] = ()):
    """A writable copy of the fixture estate, optionally with declarations
    replaced or removed. Everything the command writes lands here, so the
    committed fixture stays an input and never carries emitted output."""

    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp) / "estate"
        shutil.copytree(FIXTURE_ESTATE, root)
        products = root / "declarations" / "products"
        for file_name in remove:
            (products / file_name).unlink()
        for file_name, document in (overlay or {}).items():
            (products / file_name).write_text(
                yaml.safe_dump(document, sort_keys=False, allow_unicode=True), encoding="utf-8"
            )
        yield root


def constant_expression_overlay() -> dict[str, dict]:
    """The fixture estate with two expressions that read no column at all:
    a constant calculated field and an aggregate over the whole relation.
    Both still declare a field, so both must still appear in the lineage."""

    order = _fixture_document("order.yml")
    for step in order["steps"]:
        if step.get("pattern") == "calculated_fields":
            step["fields"].append({"name": "is_retail", "type": "boolean", "expression": "true"})
    mart = _fixture_document("order_mart.yml")
    for step in mart["steps"]:
        if step.get("pattern") == "data_aggregation":
            step["aggregates"][0]["expression"] = "COUNT(*)"
    return {"order.yml": order, "order_mart.yml": mart}


def run_command(*args: str) -> tuple[int, str]:
    """Drive the real console entry point and return its exit code and
    everything it printed."""

    saved = list(sys.argv)
    buffer = io.StringIO()
    try:
        with contextlib.redirect_stdout(buffer):
            code = cli.main(list(args))
    finally:
        sys.argv = saved
    return code, buffer.getvalue()


def graph_for(root: Path):
    return eg.build_estate_product_graph(EstateContext.resolve(estate_root=root))


def fixture_graph():
    return graph_for(FIXTURE_ESTATE)


def csv_rows(text: str) -> list[dict[str, str]]:
    return list(csv.DictReader(io.StringIO(text)))


def expect_raises(error_type, fn, *, contains: tuple[str, ...] = ()):
    try:
        fn()
    except error_type as exc:
        message = str(exc)
        for fragment in contains:
            assert fragment in message, f"expected {fragment!r} in the failure message, got: {message}"
        return exc
    raise AssertionError(f"expected {error_type.__name__}; none raised")


def publication_plan(prefix: str, profile: str) -> ExecutionPlan:
    """A plan carrying exactly the four publication-shaped occurrences the
    estate's translator table names the publication translator for."""

    contract_occ = Occurrence(f"{prefix}.contract", PatternId.DATA_CONTRACTS, (Role.POLICY, Role.BARRIER), True)
    schema_occ = Occurrence(f"{prefix}.schema", PatternId.SCHEMA_PUBLISH, (Role.OBSERVER, Role.BARRIER), True)
    metadata_occ = Occurrence(f"{prefix}.metadata", PatternId.METADATA_CAPTURE, (Role.OBSERVER,), True)
    lineage_occ = Occurrence(f"{prefix}.lineage", PatternId.LINEAGE_CAPTURE, (Role.OBSERVER,), True)
    occurrences = tuple(
        sorted((contract_occ, schema_occ, metadata_occ, lineage_occ), key=lambda o: o.occurrence_id)
    )
    members = tuple(
        sorted(o.occurrence_id for o in occurrences if o.occurrence_id != contract_occ.occurrence_id)
    )
    return ExecutionPlan(
        profile=profile,
        occurrences=occurrences,
        edges=(),
        wrapper_id=contract_occ.occurrence_id,
        wrapper_members=members,
    )


class _FixtureAuxiliaryTranslator:
    """A translator that renders one private relation for a staged
    computation. It stands in for any translator whose rendering needs a
    relation the product's contract must not publish."""

    def auxiliary_relations(self) -> tuple[pg.AuxiliaryRelation, ...]:
        return (
            pg.AuxiliaryRelation(
                product="retail.order_mart",
                relation="retail.order_mart__stage_totals",
                translator="dbt",
                purpose="staged aggregate computation",
            ),
        )


# --------------------------------------------------------------------------- the lineage_capture capability


def test_publication_translator_registers_lineage_capture_on_every_declared_adapter() -> None:
    capabilities = PublicationTranslator().capabilities()
    assert PatternId.LINEAGE_CAPTURE in PUBLICATION_PATTERNS
    for adapter in ADAPTER_NAMES:
        assert Capability("lineage_capture", "publication", adapter) in capabilities, adapter
    assert len(capabilities) == len(PUBLICATION_PATTERNS) * len(ADAPTER_NAMES)


def test_the_real_estate_table_routes_lineage_capture_to_the_publication_translator() -> None:
    table = load_translator_table(EstateContext.default().estate_file)
    assert table, "the estate declares a translator table"
    for label, entries in table.items():
        assert entries.get("lineage_capture") == "publication", label


def test_lineage_capture_routes_to_the_publication_translator_under_every_declared_label() -> None:
    # The real verdict path: the router resolves a Lineage Capture
    # occurrence through the real estate.yml translator table, for every
    # label the estate declares and every adapter it declares, and the
    # named translator must carry the capability or the route fails closed.
    ctx = EstateContext.default()
    table = load_translator_table(ctx.estate_file)
    adapters = load_estate_adapters(ctx.estate_file)
    # The profile each label is exercised under is read from the estate's own
    # label declaration rather than restated here, so a label the estate adds
    # is covered by this test the moment it is declared.
    policy = declaration_mod.load_estate_policy(ctx.estate_file)
    for label, entries in table.items():
        plan = publication_plan(f"{label}fix", policy.labels[label][0])
        router = TranslationRouter(plan, [PublicationTranslator()])
        result = router.route(label=label, translator_table=table, adapters=adapters.names())
        lineage_rows = [a for a in result.assignments if a.occurrence_id.endswith(".lineage")]
        assert len(lineage_rows) == len(adapters.names()), (label, result.assignments)
        assert {a.adapter for a in lineage_rows} == set(adapters.names()), label
        assert all(a.translator_name == "publication" for a in lineage_rows), label
        assert entries["lineage_capture"] == "publication", label


def test_the_dbt_translator_registers_no_capability_at_all() -> None:
    from ergasterion.source_delivery import TypedDeclarations
    from ergasterion.translators.dbt import DbtTranslator

    translator = DbtTranslator(
        typed=TypedDeclarations(tables={}, estate_namespace="fixture"), bound={}
    )
    assert translator.capabilities() == frozenset()
    assert not hasattr(sys.modules["ergasterion.translators.dbt"], "DBT_LANDING_PATTERNS")


def test_the_publication_translator_renders_the_same_graph_artefacts_as_the_emitter() -> None:
    graph = fixture_graph()
    plan = publication_plan("fixture", "landing")
    rendered = PublicationTranslator(graph=graph).translate(plan).artefacts
    direct = pg.graph_artefacts(graph)
    for name, text in direct.items():
        key = f"{GRAPH_ARTEFACT_PREFIX}/{name}"
        assert key in rendered, f"the publication translator did not render {name}"
        assert rendered[key] == text, f"{name} differs between the translator and the emitter"
    assert len([k for k in rendered if k.startswith(f"{GRAPH_ARTEFACT_PREFIX}/")]) == len(direct)


def test_the_publication_translator_renders_no_graph_artefact_without_a_graph() -> None:
    plan = publication_plan("fixture", "landing")
    rendered = PublicationTranslator().translate(plan).artefacts
    assert not [key for key in rendered if key.startswith(f"{GRAPH_ARTEFACT_PREFIX}/")]


# --------------------------------------------------------------------------- order and generations


def test_the_fixture_estate_resolves_to_the_documented_order_through_the_command() -> None:
    with scratch_estate() as root:
        code, output = run_command("product-graph", "--estate-root", str(root))
        assert code == 0, output
        nodes = csv_rows((root / "graphs" / "products" / pg.NODES_ARTEFACT).read_text(encoding="utf-8"))
    assert tuple(row["id"] for row in nodes) == DOCUMENTED_ORDER, nodes
    assert {row["id"]: row["generation"] for row in nodes} == DOCUMENTED_GENERATIONS


def test_every_generation_is_labelled_structurally_and_the_description_partitions_them() -> None:
    graph = fixture_graph()
    assert {node.published_name: node.generation for node in graph.nodes} == DOCUMENTED_GENERATIONS
    description = pg.build_graph_description(graph)
    assert set(description["generations"]) == set(pg.GENERATIONS)
    members = [name for label in pg.GENERATIONS for name in description["generations"][label]]
    assert sorted(members) == sorted(DOCUMENTED_ORDER)


def test_the_five_reference_profiles_are_all_present_in_the_fixture_estate() -> None:
    graph = fixture_graph()
    assert {node.profile for node in graph.nodes} == {
        "landing",
        "integration",
        "derivation",
        "consolidation",
        "serving",
    }
    assert len(graph.nodes) == 6
    # The estate's layer labels are names the engine has never seen. It
    # resolves the whole graph without knowing what any of them mean.
    assert {node.layer for node in graph.nodes} == {"landed", "shaped", "served"}


def test_an_enrichment_lookup_is_an_edge_but_never_changes_a_generation() -> None:
    graph = fixture_graph()
    lookups = [edge for edge in graph.edges if edge.kind == pg.EDGE_LOOKUP]
    assert [(edge.source, edge.target) for edge in lookups] == [
        ("retail.catalogue_landing", "retail.order")
    ]
    # retail.order reads a second landing contract as a lookup and still
    # sits in the first generation: only the declaration's own sources
    # block classifies a generation.
    order = next(node for node in graph.nodes if node.published_name == "retail.order")
    assert order.generation == pg.GENERATION_FIRST
    # The lookup still orders the build: the looked-up product comes first.
    order_index = graph.order().index("retail.order")
    assert graph.order().index("retail.catalogue_landing") < order_index


# --------------------------------------------------------------------------- fail-closed resolution


def test_a_cycle_fails_closed_naming_every_product_in_it() -> None:
    document = _fixture_document("product_catalogue.yml")
    document["sources"] = [{"contract": "retail.order_360@1"}]
    with scratch_estate(overlay={"product_catalogue.yml": document}) as root:
        code, output = run_command("product-graph", "--estate-root", str(root))
    assert code == 1, output
    assert "not acyclic" in output, output
    for product in ("retail.product_catalogue", "retail.order_360"):
        assert product in output, output
    assert "retail.catalogue_landing" not in output.split("not acyclic", 1)[1], output


def test_a_source_contract_no_product_publishes_fails_closed_naming_both() -> None:
    document = _fixture_document("order.yml")
    document["sources"] = [{"contract": "retail.ghost_landing@1"}]
    with scratch_estate(overlay={"order.yml": document}) as root:
        code, output = run_command("product-graph", "--estate-root", str(root))
    assert code == 1, output
    assert "retail.order" in output and "retail.ghost_landing@1" in output, output
    assert "no product in this estate publishes" in output, output


def test_an_enrichment_lookup_no_product_publishes_fails_closed_naming_both() -> None:
    document = _fixture_document("order.yml")
    for step in document["steps"]:
        if step.get("pattern") == "data_enrichment":
            step["lookups"][0]["contract"] = "retail.ghost_reference@1"
    with scratch_estate(overlay={"order.yml": document}) as root:
        code, output = run_command("product-graph", "--estate-root", str(root))
    assert code == 1, output
    assert "retail.order" in output and "retail.ghost_reference@1" in output, output


def test_the_five_profile_product_set_is_not_a_closed_estate_and_fails_closed() -> None:
    # tests/fixtures/products/valid/ names two contracts no declaration in
    # that set publishes. A product graph is a closed estate by definition,
    # so resolving it names the first consumer and reference rather than
    # quietly dropping an edge.
    ctx = EstateContext.default()
    policy = declaration_mod.load_estate_policy(ctx.estate_file)
    entries = pg.load_product_entries(FIVE_PROFILE_PRODUCTS, policy=policy)
    assert len(entries) == 5
    exc = expect_raises(
        pg.MissingUpstreamProductError,
        lambda: pg.build_product_graph(entries),
        contains=("ecommerce.customer",),
    )
    assert exc.upstream not in entries


def test_two_declarations_claiming_one_published_name_fail_closed_naming_both_files() -> None:
    # The estate-level duplicate-name rule has one owner. This proves the
    # product-graph loader really delegates to it rather than resolving a
    # graph over a silently collapsed pair.
    document = _fixture_document("order_mart.yml")
    with scratch_estate(overlay={"order_mart_copy.yml": document}) as root:
        code, output = run_command("product-graph", "--estate-root", str(root))
    assert code == 1, output
    assert "duplicate_published_name" in output, output
    assert "order_mart.yml" in output and "order_mart_copy.yml" in output, output


def test_a_non_landing_product_with_no_source_has_no_generation() -> None:
    document = _fixture_document("product_catalogue.yml")
    document["sources"] = []
    with scratch_estate(overlay={"product_catalogue.yml": document}) as root:
        code, output = run_command("product-graph", "--estate-root", str(root))
    assert code == 1, output
    assert "retail.product_catalogue" in output and "derivation" in output, output
    assert "generation cannot be derived" in output, output


def test_an_entry_key_that_disagrees_with_its_declaration_fails_closed() -> None:
    ctx = EstateContext.resolve(estate_root=FIXTURE_ESTATE)
    policy = declaration_mod.load_estate_policy(ctx.estate_file)
    entries = pg.load_product_entries(FIXTURE_PRODUCTS, policy=policy)
    mislabelled = dict(entries)
    mislabelled["retail.mislabelled"] = mislabelled.pop("retail.order_mart")
    expect_raises(
        pg.ProductGraphCoverageError,
        lambda: pg.build_product_graph(mislabelled),
        contains=("retail.mislabelled", "retail.order_mart"),
    )


# --------------------------------------------------------------------------- the reconciliation rule


def test_the_reconciliation_rule_renders_to_a_validation_occurrence() -> None:
    with scratch_estate() as root:
        code, output = run_command("product-graph", "--estate-root", str(root))
        assert code == 0, output
        rows = csv_rows(
            (root / "graphs" / "products" / pg.VALIDATIONS_ARTEFACT).read_text(encoding="utf-8")
        )
    reconciliations = [row for row in rows if row["kind"] == pg.VALIDATION_RECONCILIATION]
    assert len(reconciliations) == 1, rows
    record = reconciliations[0]
    assert record["product"] == "retail.order_360"
    assert record["stage"] == "post"
    assert record["subject"] == "row_count"
    assert record["references"].split(";") == ["retail.order@1", "retail.product_catalogue@1"]
    # It sits beside the ordinary field rules of the same occurrence, not
    # instead of them.
    same_occurrence = [row for row in rows if row["occurrence"] == record["occurrence"]]
    assert {row["kind"] for row in same_occurrence} == {
        pg.VALIDATION_FIELD,
        pg.VALIDATION_RECONCILIATION,
    }


def test_the_reconciliation_rule_references_the_products_two_upstream_contracts() -> None:
    graph = fixture_graph()
    record = next(v for v in graph.validations if v.kind == pg.VALIDATION_RECONCILIATION)
    consumed = {
        edge.contract
        for edge in graph.edges
        if edge.target == record.product and edge.kind == pg.EDGE_SOURCE
    }
    assert set(record.references) == consumed
    assert len(record.references) == 2


def test_the_reconciliation_walk_covers_every_validation_occurrence_not_one_of_them() -> None:
    # The committed fixture declares the reconciliation in its second
    # validation occurrence. Moving it to the first must change nothing but
    # the stage: the walk reads the whole composition, never a chosen
    # occurrence.
    document = _fixture_document("order_360.yml")
    validations = [s for s in document["steps"] if s.get("pattern") == "data_validation"]
    moved = [rule for rule in validations[1]["rules"] if "reconcile" in rule]
    validations[1]["rules"] = [rule for rule in validations[1]["rules"] if "reconcile" not in rule]
    validations[0]["rules"] = validations[0]["rules"] + moved
    with scratch_estate(overlay={"order_360.yml": document}) as root:
        graph = graph_for(root)
    record = next(v for v in graph.validations if v.kind == pg.VALIDATION_RECONCILIATION)
    assert record.stage == "pre"
    assert record.subject == "row_count"
    assert record.references == ("retail.order@1", "retail.product_catalogue@1")


def test_a_reconciliation_rule_naming_a_contract_the_product_does_not_consume_fails_closed() -> None:
    document = _fixture_document("order_360.yml")
    for step in document["steps"]:
        for rule in step.get("rules") or []:
            if "reconcile" in rule:
                rule["reconcile"]["contracts"] = ["retail.order@1", "retail.orders_landing@1"]
    with scratch_estate(overlay={"order_360.yml": document}) as root:
        code, output = run_command("product-graph", "--estate-root", str(root))
    assert code == 1, output
    assert "retail.order_360" in output, output
    assert "retail.orders_landing@1" in output, output
    assert "does not consume" in output, output


def test_a_reconciliation_rule_with_one_contract_fails_declaration_validation() -> None:
    document = _fixture_document("order_360.yml")
    for step in document["steps"]:
        for rule in step.get("rules") or []:
            if "reconcile" in rule:
                rule["reconcile"]["contracts"] = ["retail.order@1"]
    with scratch_estate(overlay={"order_360.yml": document}) as root:
        code, output = run_command("validate", "--estate-root", str(root))
    assert code == 1, output
    assert "order_360" in output, output


def test_a_rule_carrying_both_a_field_and_a_reconciliation_fails_declaration_validation() -> None:
    document = _fixture_document("order_360.yml")
    for step in document["steps"]:
        for rule in step.get("rules") or []:
            if "reconcile" in rule:
                rule["field"] = "order_id"
    with scratch_estate(overlay={"order_360.yml": document}) as root:
        code, output = run_command("validate", "--estate-root", str(root))
    assert code == 1, output
    assert "order_360" in output, output


def test_the_reconciliation_rule_reaches_the_contract_as_a_consistency_guarantee() -> None:
    guarantees = build_quality_guarantees(_fixture_document("order_360.yml"))
    consistency = [g for g in guarantees if g.dimension == "consistency"]
    assert len(consistency) == 1, guarantees
    assert "row_count" in consistency[0].description
    assert "retail.order@1" in consistency[0].description
    assert "retail.product_catalogue@1" in consistency[0].description


# --------------------------------------------------------------------------- lineage


def test_product_level_lineage_is_the_declared_contract_edge_set() -> None:
    with scratch_estate() as root:
        code, output = run_command("product-graph", "--estate-root", str(root))
        assert code == 0, output
        rows = csv_rows((root / "graphs" / "products" / pg.EDGES_ARTEFACT).read_text(encoding="utf-8"))
    assert {(row["src"], row["dst"], row["kind"]) for row in rows} == {
        ("retail.orders_landing", "retail.order", pg.EDGE_SOURCE),
        ("retail.catalogue_landing", "retail.order", pg.EDGE_LOOKUP),
        ("retail.catalogue_landing", "retail.product_catalogue", pg.EDGE_SOURCE),
        ("retail.order", "retail.order_360", pg.EDGE_SOURCE),
        ("retail.product_catalogue", "retail.order_360", pg.EDGE_SOURCE),
        ("retail.order_360", "retail.order_mart", pg.EDGE_SOURCE),
    }
    for row in rows:
        assert row["contract"].endswith("@1"), row
        assert row["relation"] == row["src"], row


def test_field_lineage_covers_the_five_declaration_mechanisms() -> None:
    with scratch_estate() as root:
        code, output = run_command("product-graph", "--estate-root", str(root))
        assert code == 0, output
        rows = csv_rows(
            (root / "graphs" / "products" / pg.FIELD_LINEAGE_ARTEFACT).read_text(encoding="utf-8")
        )
    by_transform: dict[str, list[dict[str, str]]] = {}
    for row in rows:
        by_transform.setdefault(row["transform"], []).append(row)
    assert set(by_transform) == {
        "schema_transform",
        "calculated_fields",
        "data_enrichment",
        "data_aggregation",
        "merge",
    }

    # The fifth mechanism is the conformance a source of a combination
    # declares: the field arrives from the upstream contract under one name
    # and enters the consolidating product under another, so the record
    # names the producer, both names and the combination that renamed it.
    conformance = by_transform["merge"]
    assert len(conformance) == 1, conformance
    assert conformance[0]["product"] == "retail.order_360"
    assert conformance[0]["source_kind"] == pg.SOURCE_KIND_CONTRACT
    assert conformance[0]["source_product"] == "retail.product_catalogue"
    assert conformance[0]["source"] == "category_name"
    assert conformance[0]["target_field"] == "catalogue_category_name"
    assert conformance[0]["occurrence"] == "sources[1].conform"

    mapped = {(row["source"], row["target_field"]) for row in by_transform["schema_transform"]}
    assert mapped == {
        ("ordr_dt", "ordered_on"),
        ("cust_id", "customer_id"),
        ("ordr_ttl", "order_total"),
        ("tag_list", "tags"),
    }

    enrichment = by_transform["data_enrichment"]
    assert len(enrichment) == 1
    assert enrichment[0]["source_kind"] == pg.SOURCE_KIND_CONTRACT
    assert enrichment[0]["source_product"] == "retail.catalogue_landing"
    assert enrichment[0]["target_field"] == "category_name"

    aggregation = {(row["source"], row["target_field"]) for row in by_transform["data_aggregation"]}
    assert aggregation == {
        ("customer_id", "customer_id"),
        ("ordered_on", "ordered_on"),
        ("order_id", "order_count"),
        ("order_total", "order_value"),
    }

    for row in rows:
        assert row["source"] and row["target_field"] and row["occurrence"], row


def test_calculated_field_lineage_lists_the_columns_the_parser_resolved() -> None:
    graph = fixture_graph()
    inline = [
        edge
        for edge in graph.field_lineage
        if edge.transform == "calculated_fields" and edge.source_kind == pg.SOURCE_KIND_FIELD
    ]
    assert [(edge.target_field, edge.source) for edge in inline] == [("is_open", "status_code")]
    # The same columns the parser reports for that expression, nothing added
    # and nothing dropped.
    from ergasterion.framework.expressions import parse_expression

    parsed = parse_expression(
        "status_code IN ('N', 'P')", product="retail.order", occurrence="steps[3]:calculated_fields"
    )
    assert tuple(edge.source for edge in inline) == parsed.columns

    named = [edge for edge in graph.field_lineage if edge.source_kind == pg.SOURCE_KIND_RULE]
    assert [(edge.target_field, edge.source) for edge in named] == [("margin_band", "margin_band_v1@1")]


def test_an_expression_that_reads_no_column_still_records_its_field() -> None:
    with scratch_estate(overlay=constant_expression_overlay()) as root:
        code, output = run_command("product-graph", "--estate-root", str(root))
        assert code == 0, output
        rows = csv_rows(
            (root / "graphs" / "products" / pg.FIELD_LINEAGE_ARTEFACT).read_text(encoding="utf-8")
        )
    by_field = {(row["product"], row["target_field"]): row for row in rows}
    constant = by_field[("retail.order", "is_retail")]
    assert constant["source_kind"] == pg.SOURCE_KIND_EXPRESSION
    assert constant["source"] == "true"
    assert constant["transform"] == "calculated_fields"
    whole_relation = by_field[("retail.order_mart", "order_count")]
    assert whole_relation["source_kind"] == pg.SOURCE_KIND_EXPRESSION
    assert whole_relation["source"] == "COUNT(*)"
    assert whole_relation["transform"] == "data_aggregation"
    # The columns the parser does resolve are still recorded as columns.
    assert by_field[("retail.order_mart", "order_value")]["source_kind"] == pg.SOURCE_KIND_FIELD


def test_a_constant_calculated_field_with_no_lineage_record_fails_coverage() -> None:
    with scratch_estate(overlay=constant_expression_overlay()) as root:
        graph = graph_for(root)
    planted = _planted(
        graph,
        field_lineage=tuple(
            edge
            for edge in graph.field_lineage
            if not (edge.product == "retail.order" and edge.target_field == "is_retail")
        ),
    )
    expect_raises(
        pg.ProductGraphCoverageError,
        lambda: pg.assert_product_graph_coverage(planted),
        contains=("retail.order", "is_retail", "carries no record explaining it"),
    )


def test_a_whole_relation_aggregate_with_no_lineage_record_fails_coverage() -> None:
    with scratch_estate(overlay=constant_expression_overlay()) as root:
        graph = graph_for(root)
    planted = _planted(
        graph,
        field_lineage=tuple(
            edge
            for edge in graph.field_lineage
            if not (edge.product == "retail.order_mart" and edge.target_field == "order_count")
        ),
    )
    expect_raises(
        pg.ProductGraphCoverageError,
        lambda: pg.assert_product_graph_coverage(planted),
        contains=("retail.order_mart", "order_count", "carries no record explaining it"),
    )


def test_every_field_the_committed_fixture_estate_declares_is_explained() -> None:
    graph = fixture_graph()
    explained = {(edge.product, edge.target_field) for edge in graph.field_lineage}
    declared = {(entry.product, entry.field) for entry in graph.declared_fields}
    assert declared, "the fixture estate declares output fields"
    assert declared <= explained
    assert {entry.field for entry in graph.declared_fields} >= {
        "ordered_on",
        "customer_id",
        "order_total",
        "tags",
        "is_open",
        "category_name",
        "margin_band",
        "order_count",
        "order_value",
    }


def test_a_calculated_field_whose_expression_does_not_parse_fails_closed() -> None:
    document = _fixture_document("order.yml")
    for step in document["steps"]:
        if step.get("pattern") == "calculated_fields":
            step["fields"][0]["expression"] = "status_code IN ("
    with scratch_estate(overlay={"order.yml": document}) as root:
        code, output = run_command("product-graph", "--estate-root", str(root))
    assert code == 1, output
    assert "retail.order" in output and "calculated_fields" in output, output


# --------------------------------------------------------------------------- the checkpoint hook


def test_the_checkpoint_flag_forces_table_materialisation_and_registers_the_relation() -> None:
    with scratch_estate() as root:
        code, output = run_command("product-graph", "--estate-root", str(root))
        assert code == 0, output
        products_dir = root / "graphs" / "products"
        nodes = csv_rows((products_dir / pg.NODES_ARTEFACT).read_text(encoding="utf-8"))
        description = json.loads((products_dir / pg.DESCRIPTION_ARTEFACT).read_text(encoding="utf-8"))
    flagged = {row["id"]: row for row in nodes if row["checkpoint"] == "true"}
    assert set(flagged) == {"retail.order_360"}
    assert flagged["retail.order_360"]["materialisation"] == pg.MATERIALISATION_TABLE
    assert description["checkpoint_relations"] == ["retail.order_360"]
    assert flagged["retail.order_360"]["relations"] == "retail.order_360"


def test_a_product_without_the_flag_declares_no_materialisation_intent() -> None:
    graph = fixture_graph()
    unflagged = [node for node in graph.nodes if not node.checkpoint]
    assert len(unflagged) == 5
    assert {node.materialisation for node in unflagged} == {pg.MATERIALISATION_UNDECLARED}


def test_setting_the_flag_on_another_product_registers_that_relation_too() -> None:
    document = _fixture_document("order_mart.yml")
    document["checkpointing"]["checkpoint"] = True
    with scratch_estate(overlay={"order_mart.yml": document}) as root:
        graph = graph_for(root)
    assert set(graph.checkpoint_relations()) == {"retail.order_360", "retail.order_mart"}


def test_an_aggregate_with_no_declared_type_fails_declaration_validation() -> None:
    # An aggregate replaces the field set it groups, so its type cannot be
    # carried over from an input field: the declaration must state it, and
    # the contract derivation fails closed without it.
    document = _fixture_document("order_mart.yml")
    for step in document["steps"]:
        if step.get("pattern") == "data_aggregation":
            step["aggregates"][0].pop("type")
    with scratch_estate(overlay={"order_mart.yml": document}) as root:
        code, output = run_command("validate", "--estate-root", str(root))
    assert code == 1, output
    assert "order_mart" in output and "data_aggregation" in output, output


def test_the_committed_fixture_estate_validates_end_to_end() -> None:
    # The fixture estate is a real estate, not merely a graph-shaped one:
    # every declaration passes both validation layers, including inline
    # expression parsing, column resolution and the named-rule catalogue.
    code, output = run_command("validate", "--estate-root", str(FIXTURE_ESTATE))
    assert code == 0, output
    assert "6 product declaration(s) valid" in output, output


def test_the_checkpoint_key_is_admitted_only_as_a_boolean() -> None:
    document = _fixture_document("order_360.yml")
    document["checkpointing"]["checkpoint"] = "yes"
    with scratch_estate(overlay={"order_360.yml": document}) as root:
        code, output = run_command("validate", "--estate-root", str(root))
    assert code == 1, output
    assert "order_360" in output and "checkpointing" in output, output


# --------------------------------------------------------------------------- stable relation names


def test_published_relation_names_are_stable_across_two_emissions() -> None:
    with scratch_estate() as root:
        ctx = EstateContext.resolve(estate_root=root)
        first = eg.generate_products(ctx)
        second = eg.generate_products(ctx)
    assert first == second, "product-graph generation is not byte-stable"
    nodes = csv_rows(next(text for path, text in first.items() if path.name == pg.NODES_ARTEFACT))
    assert {row["relations"] for row in nodes} == set(DOCUMENTED_ORDER)


def test_published_relation_names_come_from_the_shape_registry() -> None:
    # What a shape renders has one owner. The graph asks the registry for
    # every product's relation set rather than assuming one relation per
    # product, so a shape that renders more extends the registry and needs
    # no change here.
    from ergasterion.framework.shapes import get_shape

    graph = fixture_graph()
    for node in graph.nodes:
        assert node.relations == get_shape(node.shape).relation_names(
            domain=node.domain, name=node.name
        ), node.published_name
    assert all(len(node.relations) == 1 for node in graph.nodes), "the declared shape renders one relation"


def test_a_product_naming_an_unregistered_shape_publishes_no_relation() -> None:
    from ergasterion.framework.shapes import UnknownShapeError

    ctx = EstateContext.resolve(estate_root=FIXTURE_ESTATE)
    policy = declaration_mod.load_estate_policy(ctx.estate_file)
    entries = pg.load_product_entries(FIXTURE_PRODUCTS, policy=policy)
    document, validated = entries["retail.order_mart"]
    entries["retail.order_mart"] = (document, dataclasses.replace(validated, shape="unregistered_shape"))
    expect_raises(
        UnknownShapeError,
        lambda: pg.build_product_graph(entries),
        contains=("unregistered_shape",),
    )


def test_a_published_relation_name_is_derived_from_the_declaration_not_fixed() -> None:
    document = _fixture_document("order_mart.yml")
    document["product"]["name"] = "order_summary"
    with scratch_estate(overlay={"order_mart.yml": document}) as root:
        graph = graph_for(root)
    relations = {node.published_name: node.relations for node in graph.nodes}
    assert "retail.order_mart" not in relations
    assert relations["retail.order_summary"] == ("retail.order_summary",)


def test_the_command_writes_then_checks_clean_on_a_scratch_estate() -> None:
    with scratch_estate() as root:
        write_code, write_output = run_command("product-graph", "--estate-root", str(root))
        assert write_code == 0, write_output
        check_code, check_output = run_command("product-graph", "--check", "--estate-root", str(root))
        assert check_code == 0, check_output
        assert "byte-match" in check_output, check_output
        rewrite_code, rewrite_output = run_command("product-graph", "--estate-root", str(root))
        assert rewrite_code == 0, rewrite_output
        assert "generated/updated 0 of 5" in rewrite_output, rewrite_output


def test_the_command_reports_drift_on_a_hand_edited_artefact() -> None:
    with scratch_estate() as root:
        assert run_command("product-graph", "--estate-root", str(root))[0] == 0
        target = root / "graphs" / "products" / pg.NODES_ARTEFACT
        target.write_text(target.read_text(encoding="utf-8") + "planted,row\n", encoding="utf-8")
        code, output = run_command("product-graph", "--check", "--estate-root", str(root))
    assert code == 1, output
    assert "DRIFT" in output and pg.NODES_ARTEFACT in output, output


def test_the_command_reports_an_orphan_artefact() -> None:
    with scratch_estate() as root:
        assert run_command("product-graph", "--estate-root", str(root))[0] == 0
        (root / "graphs" / "products" / "product-orphan.csv").write_text("id\n", encoding="utf-8")
        code, output = run_command("product-graph", "--check", "--estate-root", str(root))
    assert code == 1, output
    assert "ORPHAN" in output, output


def test_an_estate_with_no_product_declarations_emits_no_graph() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp) / "estate"
        root.mkdir(parents=True)
        shutil.copy(FIXTURE_ESTATE / "estate.yml", root / "estate.yml")
        code, output = run_command("product-graph", "--estate-root", str(root))
        assert code == 0, output
        assert "declares no product yet" in output, output
        assert not (root / "graphs").exists()


# --------------------------------------------------------------------------- auxiliary lineage


def test_a_translator_registered_auxiliary_relation_becomes_auxiliary_lineage() -> None:
    with scratch_estate() as root:
        ctx = EstateContext.resolve(estate_root=root)
        files = eg.generate_products(ctx, translators=[_FixtureAuxiliaryTranslator()])
    edges = csv_rows(next(text for path, text in files.items() if path.name == pg.EDGES_ARTEFACT))
    auxiliary = [row for row in edges if row["kind"] == pg.EDGE_AUXILIARY]
    assert len(auxiliary) == 1, edges
    assert auxiliary[0]["src"] == auxiliary[0]["dst"] == "retail.order_mart"
    assert auxiliary[0]["relation"] == "retail.order_mart__stage_totals"
    description = json.loads(
        next(text for path, text in files.items() if path.name == pg.DESCRIPTION_ARTEFACT)
    )
    assert description["auxiliary_relations"] == [
        {
            "product": "retail.order_mart",
            "relation": "retail.order_mart__stage_totals",
            "translator": "dbt",
            "purpose": "staged aggregate computation",
        }
    ]
    # An auxiliary relation is private: it is never one of the estate's
    # published relation names.
    nodes = csv_rows(next(text for path, text in files.items() if path.name == pg.NODES_ARTEFACT))
    assert "retail.order_mart__stage_totals" not in {row["relations"] for row in nodes}


def test_the_translators_this_command_constructs_register_no_auxiliary_relation() -> None:
    providers = eg.product_graph_translators()
    assert providers, "the command asks at least one translator"
    registry = pg.collect_auxiliary_relations(providers)
    assert registry.relations() == ()
    assert fixture_graph().auxiliary == ()


def test_the_emitter_fails_closed_on_a_translator_that_registers_an_unplaceable_relation() -> None:
    # The same argument the command passes: a translator whose registration
    # cannot be placed stops the emission rather than dropping the relation.
    class _GhostTranslator:
        def auxiliary_relations(self) -> tuple[pg.AuxiliaryRelation, ...]:
            return (
                pg.AuxiliaryRelation(
                    product="retail.ghost",
                    relation="retail.ghost__stage",
                    translator="dbt",
                    purpose="staging",
                ),
            )

    with scratch_estate() as root:
        ctx = EstateContext.resolve(estate_root=root)
        expect_raises(
            pg.AuxiliaryRelationError,
            lambda: eg.generate_products(ctx, translators=[_GhostTranslator()]),
            contains=("retail.ghost", "no product with that published name"),
        )
        assert not (root / "graphs").exists(), "a failed resolution writes no artefact"


def test_an_auxiliary_relation_for_a_product_outside_the_estate_fails_closed() -> None:
    ctx = EstateContext.resolve(estate_root=FIXTURE_ESTATE)
    policy = declaration_mod.load_estate_policy(ctx.estate_file)
    entries = pg.load_product_entries(FIXTURE_PRODUCTS, policy=policy)
    registry = pg.AuxiliaryRelationRegistry()
    registry.register(
        product="retail.ghost", relation="retail.ghost__stage", translator="dbt", purpose="staging"
    )
    expect_raises(
        pg.AuxiliaryRelationError,
        lambda: pg.build_product_graph(entries, auxiliary=registry),
        contains=("retail.ghost", "no product with that published name"),
    )


def test_an_auxiliary_relation_colliding_with_a_published_relation_fails_closed() -> None:
    ctx = EstateContext.resolve(estate_root=FIXTURE_ESTATE)
    policy = declaration_mod.load_estate_policy(ctx.estate_file)
    entries = pg.load_product_entries(FIXTURE_PRODUCTS, policy=policy)
    registry = pg.AuxiliaryRelationRegistry()
    registry.register(
        product="retail.order_mart", relation="retail.order_360", translator="dbt", purpose="staging"
    )
    expect_raises(
        pg.AuxiliaryRelationError,
        lambda: pg.build_product_graph(entries, auxiliary=registry),
        contains=("retail.order_360", "already a published relation"),
    )


def test_two_translators_cannot_claim_one_auxiliary_relation() -> None:
    registry = pg.AuxiliaryRelationRegistry()
    registry.register(
        product="retail.order_mart", relation="retail.order_mart__stage", translator="dbt", purpose="staging"
    )
    registry.register(
        product="retail.order_mart", relation="retail.order_mart__stage", translator="dbt", purpose="staging"
    )
    assert len(registry) == 1
    expect_raises(
        pg.AuxiliaryRelationError,
        lambda: registry.register(
            product="retail.order_mart",
            relation="retail.order_mart__stage",
            translator="spark",
            purpose="staging",
        ),
        contains=("already registered by translator 'dbt'",),
    )


def test_an_auxiliary_registration_with_an_empty_field_fails_closed() -> None:
    registry = pg.AuxiliaryRelationRegistry()
    expect_raises(
        pg.AuxiliaryRelationError,
        lambda: registry.register(product="retail.order_mart", relation="", translator="dbt", purpose="s"),
        contains=("relation must be a non-empty string",),
    )


# --------------------------------------------------------------------------- classification independence


def test_the_product_route_reads_no_domain_section_vocabulary() -> None:
    source = (Path(eg.__file__).resolve().parent / "framework" / "graph.py").read_text(encoding="utf-8")
    for token in ("hub_configs", "link_configs", "entity_configs", "bv_configs"):
        assert token not in source, f"{token} must not appear in the product-graph classifier"
    assert "graph_model" not in source, "the product route must not reach into the domain route"


def test_the_product_route_needs_no_domains_directory_and_no_manifest() -> None:
    with scratch_estate() as root:
        assert not (root / "domains").exists()
        assert not (root / "target" / "manifest.json").exists()
        assert not (root / "models").exists()
        code, output = run_command("product-graph", "--estate-root", str(root))
        assert code == 0, output
        assert (root / "graphs" / "products" / pg.NODES_ARTEFACT).is_file()


def test_the_graph_emitter_carries_one_route_and_writes_under_its_own_directory() -> None:
    """The module emits the estate product graph and nothing else: no second
    generator, no second entry point, and every artefact under graphs/products/."""
    assert not hasattr(eg, "generate"), "the domain graph route is gone, not renamed"
    assert not hasattr(eg, "product_graph_main"), "one route, one main()"
    with scratch_estate() as root:
        ctx = EstateContext.resolve(estate_root=root)
        product_files = set(eg.generate_products(ctx))
    assert product_files, "the product route emits artefacts"
    assert all(path.parent.name == eg.PRODUCT_GRAPH_DIRNAME for path in product_files)


# --------------------------------------------------------------------------- coverage gates


def _planted(graph, **changes):
    return type(graph)(**{**graph.__dict__, **changes})


def test_every_graph_coverage_branch_fails_closed_naming_what_it_found() -> None:
    """assert_product_graph_coverage is what stands between a resolved graph
    and an artefact that publishes a partial fact. Each planted defect below
    is one of its branches; each must name what it found."""
    graph = fixture_graph()
    node = graph.nodes[0]
    checkpointed = next(n for n in graph.nodes if n.checkpoint)
    edge = graph.edges[0]
    lineage = graph.field_lineage[0]
    enrichment = next(e for e in graph.field_lineage if e.source_kind == pg.SOURCE_KIND_CONTRACT)
    validation = graph.validations[0]

    cases = [
        ({"nodes": graph.nodes + (node,)}, "duplicate product node"),
        ({"nodes": (dataclasses.replace(node, relations=()),) + graph.nodes[1:]}, "publishes no relation name"),
        (
            {"nodes": (dataclasses.replace(node, relations=("",)),) + graph.nodes[1:]},
            "publishes no relation name",
        ),
        ({"nodes": (dataclasses.replace(node, generation="middling"),) + graph.nodes[1:]}, "middling"),
        (
            {"nodes": (dataclasses.replace(node, materialisation="materialised_view"),) + graph.nodes[1:]},
            "materialisation intent 'materialised_view'",
        ),
        (
            {
                "nodes": tuple(
                    dataclasses.replace(n, materialisation=pg.MATERIALISATION_UNDECLARED) if n.checkpoint else n
                    for n in graph.nodes
                )
            },
            "does not force table materialisation",
        ),
        ({"edges": (dataclasses.replace(edge, source="retail.absent"),) + graph.edges[1:]}, "retail.absent"),
        ({"edges": (dataclasses.replace(edge, relation=""),) + graph.edges[1:]}, "carries no relation"),
        (
            {"field_lineage": (dataclasses.replace(lineage, product="retail.absent"),)},
            "field lineage names product 'retail.absent'",
        ),
        (
            {"field_lineage": (dataclasses.replace(enrichment, source_product="retail.absent"),)},
            "names upstream 'retail.absent'",
        ),
        ({"field_lineage": (dataclasses.replace(lineage, source=""),)}, "is incomplete"),
        (
            {"validations": (dataclasses.replace(validation, product="retail.absent"),)},
            "validation occurrence names product 'retail.absent'",
        ),
    ]
    assert checkpointed.checkpoint, "the fixture estate declares one checkpointed product"
    for changes, fragment in cases:
        planted = _planted(graph, **changes)
        expect_raises(
            pg.ProductGraphCoverageError,
            lambda planted=planted: pg.assert_product_graph_coverage(planted),
            contains=(fragment,),
        )


def test_a_description_that_drops_a_product_fails_coverage() -> None:
    graph = fixture_graph()
    planted = copy.deepcopy(pg.build_graph_description(graph))
    planted["order"] = planted["order"][:-1]
    expect_raises(
        pg.ProductGraphCoverageError,
        lambda: pg.assert_description_coverage(planted, graph),
        contains=("does not cover every product",),
    )


def test_a_description_that_drops_a_relation_name_fails_coverage() -> None:
    graph = fixture_graph()
    planted = copy.deepcopy(pg.build_graph_description(graph))
    planted["relations"].pop("retail.order_mart")
    expect_raises(
        pg.ProductGraphCoverageError,
        lambda: pg.assert_description_coverage(planted, graph),
        contains=("description relations cover",),
    )


def test_a_description_that_blanks_a_relation_name_fails_coverage() -> None:
    graph = fixture_graph()
    planted = copy.deepcopy(pg.build_graph_description(graph))
    planted["relations"]["retail.order_mart"] = ""
    expect_raises(
        pg.ProductGraphCoverageError,
        lambda: pg.assert_description_coverage(planted, graph),
        contains=("publishes no relation name for 'retail.order_mart'",),
    )


def test_a_description_whose_counts_disagree_with_the_graph_fails_coverage() -> None:
    graph = fixture_graph()
    planted = copy.deepcopy(pg.build_graph_description(graph))
    planted["counts"]["edges"] = 0
    expect_raises(
        pg.ProductGraphCoverageError,
        lambda: pg.assert_description_coverage(planted, graph),
        contains=("disagree with the graph",),
    )


def test_a_description_that_drops_a_generation_bucket_fails_coverage() -> None:
    graph = fixture_graph()
    planted = copy.deepcopy(pg.build_graph_description(graph))
    del planted["generations"][pg.GENERATION_LATER]
    expect_raises(
        pg.ProductGraphCoverageError,
        lambda: pg.assert_description_coverage(planted, graph),
        contains=("description generations name",),
    )


def test_a_description_whose_generations_lose_a_member_fails_coverage() -> None:
    graph = fixture_graph()
    planted = copy.deepcopy(pg.build_graph_description(graph))
    planted["generations"][pg.GENERATION_LANDING] = []
    expect_raises(
        pg.ProductGraphCoverageError,
        lambda: pg.assert_description_coverage(planted, graph),
        contains=("description generations cover",),
    )


# ------------------------------------- the consolidating generation, in an estate

CONSOLIDATING_ESTATE = (
    Path(__file__).resolve().parent.parent / "fixtures" / "estates" / "consolidating_two"
)


def test_the_consolidating_generation_is_two_or_more_non_landing_sources() -> None:
    """Architecture section 8's third generation, read off a real estate.
    Both products that combine two products sit in it, each with one source
    edge per source; the lanes that read one landing contract each sit in
    the first generation, and the consumer of a consolidated product in the
    later one."""

    graph = graph_for(CONSOLIDATING_ESTATE)
    generations = {node.published_name: node.generation for node in graph.nodes}
    assert generations == {
        "crm.customer_feed_a": pg.GENERATION_LANDING,
        "crm.customer_feed_b": pg.GENERATION_LANDING,
        "crm.customer_flag_feed": pg.GENERATION_LANDING,
        "crm.customer_a": pg.GENERATION_FIRST,
        "crm.customer_b": pg.GENERATION_FIRST,
        "crm.customer_flags": pg.GENERATION_FIRST,
        "crm.customer_union": pg.GENERATION_CONSOLIDATING,
        "crm.customer_master": pg.GENERATION_CONSOLIDATING,
        "crm.customer_directory": pg.GENERATION_LATER,
    }, generations

    for consumer, sources in (
        ("crm.customer_union", {"crm.customer_a", "crm.customer_b"}),
        ("crm.customer_master", {"crm.customer_a", "crm.customer_flags"}),
    ):
        edges = [
            edge
            for edge in graph.edges
            if edge.target == consumer and edge.kind == pg.EDGE_SOURCE
        ]
        assert {edge.source for edge in edges} == sources, edges
        assert len(edges) == 2, edges
        # Each edge names the relation the read resolved to, which for a
        # product publishing one relation is the product's own.
        assert {edge.relation for edge in edges} == sources, edges


def test_a_conformance_mapping_leaves_field_lineage_naming_both_names() -> None:
    graph = graph_for(CONSOLIDATING_ESTATE)
    conformance = [
        record
        for record in graph.field_lineage
        if record.product == "crm.customer_union" and record.occurrence.startswith("sources[")
    ]
    assert {(record.source, record.target_field) for record in conformance} == {
        ("cust_ref", "customer_id"),
        ("email_address", "email"),
        ("signup_date", "signed_up_on"),
        ("ltv", "lifetime_value"),
    }, conformance
    for record in conformance:
        assert record.source_kind == pg.SOURCE_KIND_CONTRACT, record
        assert record.source_product == "crm.customer_b", record
        assert record.transform == "union", record
        assert record.occurrence == "sources[1].conform", record


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
