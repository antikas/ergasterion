"""Assert-script tests for ergasterion/graph_model.py (repo convention: no pytest).

Three proofs:
  1. A wholly invented fixture domain -- toy entities with one plain link, one link-backed
     self-edge with role-named keys, one discriminated polymorphic column-level edge, and
     one name-divergent link-entity -- loads and passes coverage through the IDENTICAL
     load_graph_inputs -> build_property_graph code path the real domains use. This is the
     engine-neutrality proof: no domain nouns in graph_model.py, so a foreign vocabulary
     builds with zero engine edits.
  2. Both real domains build green. The investment domain's three hard edges (a link-backed
     self-edge, a column-level self-edge, a discriminated polymorphic column-level edge) are
     present as typed edges, none dropped; the ecommerce domain proves the name-divergent
     structure-join (its order_line payload rides the INCLUDES edge though its link is named
     order_line_product).
  3. Seven planted defects each fail loudly with a GraphModelError.
     coverage rule: unmapped link, orphan verb, inverse collision, unresolvable endpoint,
     mis-declared self-edge, unclassifiable identifier, disagreeing src_pk/links signals.

Usage:
    python tests/python/test_graph_model.py
"""

from __future__ import annotations

import copy
import sys
import tempfile
import traceback
from pathlib import Path

import yaml

# Allow direct execution as `python tests/python/test_graph_model.py`.
if __package__ in (None, ""):
    import os as _os, sys as _sys
    _sys.path.insert(0, _os.path.dirname(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))))

from ergasterion import graph_model
from ergasterion.graph_model import GraphModelError


# --------------------------------------------------------------------------- fixture

# A wholly invented domain in the SAME shape as domains/*.yml (emit fields present so it
# loads through emit.load_domains; graph_model reads only src_pk / links / payload). Toy
# entities alpha/beta/gamma (hubs -> nodes), alpha_chain (link-backed self-edge payload),
# beta_gamma_line (name-divergent link payload, link beta_gamma_link), alpha_metric
# (measure satellite anchored to alpha).
FIXTURE_DOMAIN = {
    "entity_configs": {
        "alpha": {
            "satellite_base": "alpha",
            "src_pk": "alpha_hk",
            "hashdiff": "alpha_hashdiff",
            "payload": ["source_id", "alpha_name", "ref_id", "ref_type"],
            "hashed_columns": {"alpha_hashdiff": {"is_hashdiff": True}},
            "links": ["alpha_beta"],
        },
        "beta": {
            "satellite_base": "beta",
            "src_pk": "beta_hk",
            "hashdiff": "beta_hashdiff",
            "payload": ["source_id", "beta_name"],
            "hashed_columns": {"beta_hashdiff": {"is_hashdiff": True}},
            "links": [],
        },
        "gamma": {
            "satellite_base": "gamma",
            "src_pk": "gamma_hk",
            "hashdiff": "gamma_hashdiff",
            "payload": ["source_id", "gamma_name"],
            "hashed_columns": {"gamma_hashdiff": {"is_hashdiff": True}},
            "links": [],
        },
        "alpha_chain": {
            "satellite_base": "alpha_chain",
            "src_pk": "alpha_chain_lhk",
            "hashdiff": "alpha_chain_hashdiff",
            "payload": ["source_id", "chain_event"],
            "hashed_columns": {"alpha_chain_hashdiff": {"is_hashdiff": True}},
            "links": ["alpha_chain"],
        },
        "beta_gamma_line": {
            "satellite_base": "beta_gamma_line",
            "src_pk": "beta_gamma_lhk",
            "hashdiff": "beta_gamma_line_hashdiff",
            "payload": ["source_id", "line_qty"],
            "hashed_columns": {"beta_gamma_line_hashdiff": {"is_hashdiff": True}},
            "links": ["beta_gamma_link"],
        },
        "alpha_metric": {
            "satellite_base": "alpha_metric",
            "src_pk": "alpha_hk",
            "hashdiff": "alpha_metric_hashdiff",
            "payload": ["source_id", "metric_value"],
            "hashed_columns": {"alpha_metric_hashdiff": {"is_hashdiff": True}},
            "links": [],
        },
    },
    "hashdiff_exclude": {},
    "hub_configs": {
        "alpha": {"path": "models/raw_vault/hubs/hub_alpha.sql", "src_pk": "alpha_hk", "src_nk": "golden_alpha_key"},
        "beta": {"path": "models/raw_vault/hubs/hub_beta.sql", "src_pk": "beta_hk", "src_nk": "golden_beta_key"},
        "gamma": {"path": "models/raw_vault/hubs/hub_gamma.sql", "src_pk": "gamma_hk", "src_nk": "golden_gamma_key"},
    },
    "link_configs": {
        "alpha_beta": {"path": "models/raw_vault/links/link_alpha_beta.sql", "src_pk": "alpha_beta_lhk", "src_fk": ["alpha_hk", "beta_hk"]},
        "alpha_chain": {"path": "models/raw_vault/links/link_alpha_chain.sql", "src_pk": "alpha_chain_lhk", "src_fk": ["prev_alpha_hk", "succ_alpha_hk"]},
        "beta_gamma_link": {"path": "models/raw_vault/links/link_beta_gamma.sql", "src_pk": "beta_gamma_lhk", "src_fk": ["beta_hk", "gamma_hk"]},
    },
    "bv_configs": {},
    "res_configs": {},
}

FIXTURE_RELATIONS = {
    "verbs": {
        "LINKED_TO": {"alias": "linked-to", "direction": "directed", "kind": "association", "cardinality": "many_to_many", "inverse": "LINKED_FROM"},
        "LINKED_FROM": {"alias": "linked-from", "direction": "directed", "kind": "association", "cardinality": "many_to_many", "inverse": "LINKED_TO"},
        "SUCCEEDED_BY": {"alias": "succeeded-by", "direction": "directed", "kind": "association", "cardinality": "one_to_one", "inverse": "SUCCEEDS"},
        "SUCCEEDS": {"alias": "succeeds", "direction": "directed", "kind": "association", "cardinality": "one_to_one", "inverse": "SUCCEEDED_BY"},
        "RELATES_TO": {"alias": "relates-to", "direction": "directed", "kind": "association", "cardinality": "many_to_many", "inverse": "RELATED_FROM"},
        "RELATED_FROM": {"alias": "related-from", "direction": "directed", "kind": "association", "cardinality": "many_to_many", "inverse": "RELATES_TO"},
        "REFERS_TO": {"alias": "refers-to", "direction": "directed", "kind": "reference", "cardinality": "many_to_one", "inverse": "REFERRED_BY"},
        "REFERRED_BY": {"alias": "referred-by", "direction": "directed", "kind": "reference", "cardinality": "one_to_many", "inverse": "REFERS_TO"},
    },
    "bindings": [
        {"verb": "LINKED_TO", "source": "alpha", "target": "beta", "link": "alpha_beta", "source_key": "alpha_hk", "target_key": "beta_hk"},
        {"verb": "SUCCEEDED_BY", "source": "alpha", "target": "alpha", "link": "alpha_chain", "source_key": "prev_alpha_hk", "target_key": "succ_alpha_hk"},
        {"verb": "RELATES_TO", "source": "beta", "target": "gamma", "link": "beta_gamma_link", "source_key": "beta_hk", "target_key": "gamma_hk"},
        {"verb": "REFERS_TO", "source": "alpha", "column": "ref_id", "discriminator": {"column": "ref_type", "map": {"beta": "beta", "gamma": "gamma"}}},
    ],
}


def _base_config() -> dict:
    """The sliced config graph_model consumes (the graph sections only), deep-copied so a
    planted defect can mutate it freely."""
    return copy.deepcopy({section: FIXTURE_DOMAIN[section] for section in graph_model._GRAPH_SECTIONS})


def _base_model() -> graph_model.RelationModel:
    return graph_model.build_relation_model(copy.deepcopy(FIXTURE_RELATIONS), "fixture")


def _edges_by_verb(graph: graph_model.PropertyGraph, verb: str) -> list:
    return [e for e in graph.edges if e.verb == verb]


def _expect_raises(label: str, fn) -> None:
    try:
        fn()
    except GraphModelError as exc:
        message = str(exc).lower()
        assert label in message, f"expected {label!r} in the loud message, got: {exc}"
    else:
        raise AssertionError(f"expected a GraphModelError mentioning {label!r}; none raised")


# --------------------------------------------------------------------------- tests


def test_fixture_domain_builds_through_same_code_path() -> None:
    """The invented domain loads through load_graph_inputs (the same path the real domains
    take) and passes coverage with zero engine special-casing."""
    with tempfile.TemporaryDirectory() as tmp:
        domains_dir = Path(tmp)
        payload = {**FIXTURE_DOMAIN, "relations": FIXTURE_RELATIONS}
        (domains_dir / "fixture.yml").write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")

        graphs = graph_model.load_graph_inputs(domains_dir)
        assert set(graphs) == {"fixture"}, "one fixture domain expected"
        graph = graphs["fixture"]

        assert {n.entity for n in graph.nodes} == {"alpha", "beta", "gamma"}, "hubs are the nodes"
        # Node property set is factory-defined (entity/domain/hash_key/golden_key).
        alpha_node = next(n for n in graph.nodes if n.entity == "alpha")
        assert alpha_node.domain == "fixture" and alpha_node.hash_key == "alpha_hk"
        assert alpha_node.golden_key == "golden_alpha_key"

        # Link-backed self-edge with role-named keys.
        succ = _edges_by_verb(graph, "SUCCEEDED_BY")
        assert len(succ) == 1 and succ[0].source == succ[0].target == "alpha"
        assert succ[0].source_key == "prev_alpha_hk" and succ[0].target_key == "succ_alpha_hk"
        assert succ[0].inverse == "SUCCEEDS"

        # Discriminated polymorphic column-level edge -> two typed edges, none dropped.
        refers = _edges_by_verb(graph, "REFERS_TO")
        assert {e.target for e in refers} == {"beta", "gamma"}, "both polymorphic targets present"
        assert all(e.kind == "column_level" and e.source_column == "ref_id" for e in refers)
        assert {e.discriminator_value for e in refers} == {"beta", "gamma"}

        # Name-divergent link payload: entity beta_gamma_line rides link beta_gamma_link.
        relates = _edges_by_verb(graph, "RELATES_TO")
        assert len(relates) == 1 and "line_qty" in relates[0].properties, "payload joined by structure"

        # Measure satellite listed with its anchor, never a node.
        assert {(s.name, s.anchor) for s in graph.satellites} == {("alpha_metric", "alpha")}
        assert "alpha_metric" not in {n.entity for n in graph.nodes}


def test_real_domains_build_green_with_hard_edges() -> None:
    """Both committed domains build; the investment hard edges are present and typed, and
    the ecommerce name-divergent structure-join carries the payload as edge properties."""
    graphs = graph_model.load_graph_inputs()
    assert {"investment", "ecommerce"} <= set(graphs)

    inv = graphs["investment"]
    # Link-backed self-edge on real data (role-named keys).
    succ = _edges_by_verb(inv, "SUCCEEDED_BY")
    assert len(succ) == 1 and succ[0].source == succ[0].target == "gp"
    assert succ[0].kind == "link_backed"
    assert {succ[0].source_key, succ[0].target_key} == {"predecessor_gp_hk", "successor_gp_hk"}
    # Column-level self-edge on real data.
    sub = _edges_by_verb(inv, "SUBSIDIARY_OF")
    assert len(sub) == 1 and sub[0].source == sub[0].target == "legal_vehicle"
    assert sub[0].kind == "column_level" and sub[0].source_column == "parent_vehicle_id"
    # Discriminated polymorphic column-level edge on real data -> fund AND portfolio_company.
    conv = _edges_by_verb(inv, "CONVERTED_TO")
    assert {e.target for e in conv} == {"fund", "portfolio_company"}, "polymorphic targets, none dropped"
    assert all(e.kind == "column_level" and e.source_column == "converted_record_id" for e in conv)
    # Measure satellites listed, never nodes.
    assert {s.name for s in inv.satellites} == {"fund_cash_flow", "fund_valuation", "legal_vehicle_cash_flow"}
    assert not ({s.name for s in inv.satellites} & {n.entity for n in inv.nodes})

    ec = graphs["ecommerce"]
    # Name-divergent structure-join: the order_line payload rides the order_line_product link.
    includes = _edges_by_verb(ec, "INCLUDES")
    assert len(includes) == 1 and includes[0].source == "order" and includes[0].target == "product"
    assert includes[0].link == "order_line_product"
    assert "line_number" in includes[0].properties and "quantity" in includes[0].properties
    # Ecommerce carries no self-edge and no polymorphic edge (attribution, not faked coverage).
    assert all(e.source != e.target for e in ec.edges), "ecommerce has no self-edge"
    assert all(e.discriminator_value is None for e in ec.edges), "ecommerce has no polymorphic edge"


def test_defect_unmapped_link() -> None:
    def run():
        config = _base_config()
        config["link_configs"]["orphan_link"] = {"src_pk": "orphan_lhk", "src_fk": ["gamma_hk", "alpha_hk"]}
        graph_model.build_property_graph("fixture", config, _base_model())
    _expect_raises("unmapped link", run)


def test_defect_orphan_verb() -> None:
    def run():
        relations = copy.deepcopy(FIXTURE_RELATIONS)
        relations["verbs"]["ISOLATED_A"] = {"alias": "isolated-a", "direction": "directed", "kind": "association", "cardinality": "one_to_one", "inverse": "ISOLATED_B"}
        relations["verbs"]["ISOLATED_B"] = {"alias": "isolated-b", "direction": "directed", "kind": "association", "cardinality": "one_to_one", "inverse": "ISOLATED_A"}
        model = graph_model.build_relation_model(relations, "fixture")
        graph_model.build_property_graph("fixture", _base_config(), model)
    _expect_raises("orphan verb", run)


def test_defect_inverse_collision() -> None:
    def run():
        relations = copy.deepcopy(FIXTURE_RELATIONS)
        # RELATES_TO now also declares LINKED_FROM as its inverse -> two verbs share one inverse.
        relations["verbs"]["RELATES_TO"]["inverse"] = "LINKED_FROM"
        model = graph_model.build_relation_model(relations, "fixture")
        graph_model.build_property_graph("fixture", _base_config(), model)
    _expect_raises("inverse", run)


def test_defect_unresolvable_endpoint() -> None:
    def run():
        relations = copy.deepcopy(FIXTURE_RELATIONS)
        relations["bindings"][0]["target"] = "nonexistent_entity"
        model = graph_model.build_relation_model(relations, "fixture")
        graph_model.build_property_graph("fixture", _base_config(), model)
    _expect_raises("unresolvable", run)


def test_defect_misdeclared_self_edge() -> None:
    def run():
        config = _base_config()
        # Make the self-edge link's two FKs the same column, and bind both keys to it: a
        # self-edge whose two ends cannot be told apart.
        config["link_configs"]["alpha_chain"]["src_fk"] = ["prev_alpha_hk", "prev_alpha_hk"]
        relations = copy.deepcopy(FIXTURE_RELATIONS)
        relations["bindings"][1]["source_key"] = "prev_alpha_hk"
        relations["bindings"][1]["target_key"] = "prev_alpha_hk"
        model = graph_model.build_relation_model(relations, "fixture")
        graph_model.build_property_graph("fixture", config, model)
    _expect_raises("self-edge", run)


def test_defect_unclassifiable_identifier() -> None:
    def run():
        config = _base_config()
        config["entity_configs"]["floating"] = {"src_pk": "floating_xk", "links": [], "payload": ["source_id"]}
        graph_model.build_property_graph("fixture", config, _base_model())
    _expect_raises("unclassifiable", run)


def test_defect_disagreeing_join_signals() -> None:
    def run():
        config = _base_config()
        # beta_gamma_line's src_pk still identifies link beta_gamma_link, but its links: list
        # now names a different link -- the two structure signals disagree.
        config["entity_configs"]["beta_gamma_line"]["links"] = ["alpha_beta"]
        graph_model.build_property_graph("fixture", config, _base_model())
    _expect_raises("disagreeing", run)


TESTS = [
    test_fixture_domain_builds_through_same_code_path,
    test_real_domains_build_green_with_hard_edges,
    test_defect_unmapped_link,
    test_defect_orphan_verb,
    test_defect_inverse_collision,
    test_defect_unresolvable_endpoint,
    test_defect_misdeclared_self_edge,
    test_defect_unclassifiable_identifier,
    test_defect_disagreeing_join_signals,
]


def main() -> int:
    failures = 0
    for test in TESTS:
        name = test.__name__
        try:
            test()
        except Exception:  # noqa: BLE001 - report and continue, exit code carries the signal
            failures += 1
            print(f"FAIL {name}")
            traceback.print_exc()
        else:
            print(f"PASS {name}")
    total = len(TESTS)
    print(f"{total - failures}/{total} passed")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
