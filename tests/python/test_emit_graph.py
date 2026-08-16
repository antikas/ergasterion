"""Assert-script tests for ergasterion/emit_graph.py.

The graph emitter is deterministic and engine-free: tests inspect emitted text, CSV
round-trips, and description coverage only.

Usage:
    python tests/python/test_emit_graph.py
"""

from __future__ import annotations

import copy
import csv
import dataclasses
import hashlib
import io
import re
import sys
import traceback
from pathlib import Path

# Allow direct execution from a source checkout.
if __package__ in (None, ""):
    import os as _os, sys as _sys
    _sys.path.insert(0, _os.path.dirname(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))))

from ergasterion import emit
from ergasterion import emit_graph as eg
from ergasterion import graph_model

MANIFEST_PATH = emit.REPO_ROOT / "target" / "manifest.json"


def _estates() -> dict[str, graph_model.EstateGraph]:
    return graph_model.load_estates()


def _files() -> dict[Path, str]:
    return eg.generate()


def _domain_files(files: dict[Path, str], domain: str) -> dict[str, str]:
    prefix = eg.GRAPHS_DIR / domain
    return {path.name: text for path, text in files.items() if path.parent == prefix}


def _csv_rows(text: str) -> list[dict[str, str]]:
    return list(csv.DictReader(io.StringIO(text)))


def _expect_raises(label: str, fn) -> None:
    try:
        fn()
    except eg.GraphEmitError as exc:
        assert label in str(exc).lower(), f"expected {label!r}, got {exc}"
    else:
        raise AssertionError(f"expected GraphEmitError mentioning {label!r}")


def _expect_estate_raises(label: str, fn) -> None:
    try:
        fn()
    except graph_model.EstateBindingError as exc:
        assert label in str(exc).lower(), f"expected {label!r}, got {exc}"
    else:
        raise AssertionError(f"expected EstateBindingError mentioning {label!r}")


def test_both_domains_emit_all_artefacts() -> None:
    files = _files()
    expected_suffixes = {
        "nodes.csv",
        "edges.csv",
        "graph.cypher",
        "graph.pgq.sql",
        "graph.gql",
        "graph-description.json",
        "estate.pgq.sql",
    }
    for domain in graph_model.load_graph_inputs():
        names = set(_domain_files(files, domain))
        expected = {f"{domain}-{suffix}" for suffix in expected_suffixes}
        assert names == expected, f"{domain}: expected graph artefact suite"


def test_byte_determinism_from_generator() -> None:
    first = _files()
    second = _files()
    assert set(first) == set(second)
    first_hash = hashlib.sha256(
        "".join(f"{p.as_posix()}\0{first[p]}" for p in sorted(first)).encode("utf-8")
    ).hexdigest()
    second_hash = hashlib.sha256(
        "".join(f"{p.as_posix()}\0{second[p]}" for p in sorted(second)).encode("utf-8")
    ).hexdigest()
    assert first_hash == second_hash


def test_csv_round_trip_and_named_walks() -> None:
    files = _files()
    graphs = graph_model.load_graph_inputs()
    for domain, graph in graphs.items():
        domain_files = _domain_files(files, domain)
        nodes = _csv_rows(domain_files[f"{domain}-nodes.csv"])
        edges = _csv_rows(domain_files[f"{domain}-edges.csv"])
        assert list(nodes[0]) == eg.NODE_HEADERS
        assert list(edges[0]) == eg.EDGE_HEADERS
        node_ids = {row["id"] for row in nodes}
        assert node_ids == {node.entity for node in graph.nodes}
        for edge in edges:
            assert edge["src"] in node_ids
            assert edge["dst"] in node_ids
            assert edge["rel_type"] in graph.verbs
            assert edge["kind"] in {"link_backed", "column_level"}

    ecommerce_edges = _csv_rows(_domain_files(files, "ecommerce")["ecommerce-edges.csv"])
    assert any(
        row["src"] == "order"
        and row["rel_type"] == "PLACED_BY"
        and row["dst"] == "customer"
        for row in ecommerce_edges
    )
    investment_edges = _csv_rows(_domain_files(files, "investment")["investment-edges.csv"])
    assert any(
        row["src"] == "gp"
        and row["rel_type"] == "SUCCEEDED_BY"
        and row["dst"] == "gp"
        for row in investment_edges
    )


def test_description_covers_relations_and_satellites() -> None:
    graphs = graph_model.load_graph_inputs()
    for graph in graphs.values():
        description = eg.build_description(graph)
        eg.assert_description_coverage(description, graph)
        assert {r["type"] for r in description["relation_types"]} == set(graph.verbs)
        assert {(s["name"], s["anchor"]) for s in description["satellites"]} == {
            (s.name, s.anchor) for s in graph.satellites
        }
        for relation in description["relation_types"]:
            traversal = relation["example_traversal"]
            assert traversal["relation"] == relation["type"]
            assert traversal["source"] in relation["source_types"]
            assert traversal["target"] in relation["target_types"]


def test_planted_satellite_less_description_fails() -> None:
    graph = graph_model.load_graph_inputs()["investment"]
    description = eg.build_description(graph)
    planted = copy.deepcopy(description)
    planted["satellites"] = []
    _expect_raises("satellite", lambda: eg.assert_description_coverage(planted, graph))


def test_sql_pgq_and_gql_use_csv_headers() -> None:
    files = _files()
    for domain in graph_model.load_graph_inputs():
        domain_files = _domain_files(files, domain)
        pgq = domain_files[f"{domain}-graph.pgq.sql"]
        gql = domain_files[f"{domain}-graph.gql"]
        assert f"CREATE TABLE {eg.node_table(domain)}" in pgq
        assert f"CREATE TABLE {eg.edge_table(domain)}" in pgq
        assert f"REFERENCES {eg.node_table(domain)} (id)" in pgq
        assert f"COPY {eg.node_table(domain)} ({', '.join(eg.NODE_HEADERS)})" in pgq
        assert f"COPY {eg.edge_table(domain)} ({', '.join(eg.EDGE_HEADERS)})" in pgq
        for header in eg.NODE_HEADERS + eg.EDGE_HEADERS:
            assert re.search(rf"\b{re.escape(header)}\b", pgq), f"{domain}: {header} missing from PGQ"
            assert f"{header} :: STRING" in gql, f"{domain}: {header} missing from GQL"


def test_both_domains_emit_estate_artefact() -> None:
    files = _files()
    for domain in graph_model.load_graph_inputs():
        names = set(_domain_files(files, domain))
        assert f"{domain}-estate.pgq.sql" in names, f"{domain}: estate artefact missing"
        estate_sql = _domain_files(files, domain)[f"{domain}-estate.pgq.sql"]
        assert f"CREATE PROPERTY GRAPH {eg.estate_graph_name(domain)}" in estate_sql
        # The boundary is stated timelessly in the header.
        assert "LINK-BACKED relationships only" in estate_sql
        assert "no ISO GQL conformance" in estate_sql


def test_estate_binds_declared_tables_and_keys() -> None:
    """The estate binds nodes to their bv golden-record table (keyed hub_pk/hub_nk) and
    edges to their link table (keyed src_pk, FK columns = src_fk) -- straight from config."""
    inputs = graph_model.load_graph_inputs_with_config()
    for domain, (graph, config) in inputs.items():
        estate = graph_model.build_estate(domain, config, graph)
        # Every node binds; every node entity is a hub-backed node.
        assert {n.entity for n in estate.node_tables} == {n.entity for n in graph.nodes}
        for node in estate.node_tables:
            bv = config["bv_configs"].get(node.entity)
            if bv is not None:
                assert node.origin == "bv_golden_record"
                assert node.table == Path(bv["path"]).stem
                assert node.key_columns == (bv["hub_pk"], bv["hub_nk"])
        # One edge table per link_config; FK columns equal the link's declared src_fk.
        assert {e.link for e in estate.edge_tables} == set(config["link_configs"])
        for edge in estate.edge_tables:
            link = config["link_configs"][edge.link]
            assert edge.table == Path(link["path"]).stem
            assert edge.key_column == link["src_pk"]
            assert {edge.source_fk, edge.target_fk} == set(link["src_fk"])
        # Cross-check (ii) is green for the real domain.
        graph_model.assert_estate_key_correctness(estate, config)


def test_estate_role_named_self_edge_passes_via_declared_keys() -> None:
    """The load-bearing case: gp_succession's role-named FKs (predecessor_gp_hk /
    successor_gp_hk) both reference the gp node's declared keys (gp_hk, golden_gp_key).
    The key-correctness check must PASS here -- it is a declared-key cross-check, never a
    string-equality between edge FK names and node key names."""
    inputs = graph_model.load_graph_inputs_with_config()
    graph, config = inputs["investment"]
    estate = graph_model.build_estate("investment", config, graph)
    self_edges = [e for e in estate.edge_tables if e.link == "gp_succession"]
    assert len(self_edges) == 1
    se = self_edges[0]
    assert se.source_entity == se.target_entity == "gp"
    assert se.source_fk == "predecessor_gp_hk"
    assert se.target_fk == "successor_gp_hk"
    assert se.source_fk != se.target_fk
    # Neither FK name equals a node key name, yet the cross-check passes.
    node = next(n for n in estate.node_tables if n.entity == "gp")
    assert se.source_fk not in node.key_columns and se.target_fk not in node.key_columns
    graph_model.assert_estate_key_correctness(estate, config)


def test_estate_realises_link_backed_edges_only() -> None:
    """Column-level relationships (payload-column edges) have no physical link table and are
    excluded from the estate. The investment graph carries such edges; none appear here."""
    inputs = graph_model.load_graph_inputs_with_config()
    graph, config = inputs["investment"]
    estate = graph_model.build_estate("investment", config, graph)
    column_level = [e for e in graph.edges if e.kind == "column_level"]
    assert column_level, "expected the investment graph to carry column-level edges"
    estate_links = {e.link for e in estate.edge_tables}
    assert estate_links == set(config["link_configs"])
    # No column-level edge's link (they have none) leaks in; edge count == link count.
    assert len(estate.edge_tables) == len(config["link_configs"])


def test_estate_tables_exist_in_manifest() -> None:
    """Cross-check (i): every table the estate DDL references exists in dbt parse's manifest.
    The gate runs `dbt parse` before this test, so the manifest is present."""
    assert MANIFEST_PATH.exists(), (
        f"manifest absent at {MANIFEST_PATH}; run `dbt parse` first (the validator does)"
    )
    model_names = graph_model.manifest_model_names(MANIFEST_PATH)
    for domain, estate in _estates().items():
        graph_model.assert_estate_tables_exist(estate, model_names)


def test_planted_wrong_key_binding_fails() -> None:
    """A mutated edge FK column (no longer the link's declared src_fk) fails the two-sided
    key-correctness cross-check loudly."""
    inputs = graph_model.load_graph_inputs_with_config()
    graph, config = inputs["ecommerce"]
    estate = graph_model.build_estate("ecommerce", config, graph)
    edge = estate.edge_tables[0]
    broken_edge = dataclasses.replace(edge, source_fk="bogus_hk")
    broken = dataclasses.replace(
        estate, edge_tables=(broken_edge, *estate.edge_tables[1:])
    )
    _expect_estate_raises("src_fk", lambda: graph_model.assert_estate_key_correctness(broken, config))


def test_planted_wrong_node_keys_fail() -> None:
    """A node bound with keys other than its declared hub_pk/hub_nk fails the REFERENCES
    side of the key-correctness cross-check."""
    inputs = graph_model.load_graph_inputs_with_config()
    graph, config = inputs["ecommerce"]
    estate = graph_model.build_estate("ecommerce", config, graph)
    node = estate.node_tables[0]
    broken_node = dataclasses.replace(node, key_columns=("wrong_hk", "wrong_key"))
    broken = dataclasses.replace(
        estate, node_tables=(broken_node, *estate.node_tables[1:])
    )
    _expect_estate_raises("disagree", lambda: graph_model.assert_estate_key_correctness(broken, config))


def test_planted_missing_table_fails() -> None:
    """A table the estate references but the emitted model set lacks fails table-existence."""
    estate = _estates()["ecommerce"]
    reduced = {estate.edge_tables[0].table}  # a model set missing every other estate table
    _expect_estate_raises(
        "table-existence", lambda: graph_model.assert_estate_tables_exist(estate, reduced)
    )


TESTS = [
    test_both_domains_emit_all_artefacts,
    test_byte_determinism_from_generator,
    test_csv_round_trip_and_named_walks,
    test_description_covers_relations_and_satellites,
    test_planted_satellite_less_description_fails,
    test_sql_pgq_and_gql_use_csv_headers,
    test_both_domains_emit_estate_artefact,
    test_estate_binds_declared_tables_and_keys,
    test_estate_role_named_self_edge_passes_via_declared_keys,
    test_estate_realises_link_backed_edges_only,
    test_estate_tables_exist_in_manifest,
    test_planted_wrong_key_binding_fails,
    test_planted_wrong_node_keys_fail,
    test_planted_missing_table_fails,
]


def main() -> int:
    failures = 0
    for test in TESTS:
        try:
            test()
        except Exception:  # noqa: BLE001 - assert-script reports and continues
            failures += 1
            print(f"FAIL {test.__name__}")
            traceback.print_exc()
        else:
            print(f"PASS {test.__name__}")
    print(f"{len(TESTS) - failures}/{len(TESTS)} passed")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
