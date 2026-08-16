"""Neutral property-graph representation derived from domain declarations.

Each relation binds a source and target to a named verb with direction, kind,
cardinality, and inverse. Domain vocabulary remains data in ``domains/*.yml``; the
engine carries no domain-specific nouns.

Entity classification is mechanical and structure-based:
  * an identifier in hub_configs                -> a NODE
  * an identifier in link_configs               -> an EDGE (link-entity payload attached as
                                                   edge properties)
  * hub_configs and link_configs must be disjoint (a name collision is a loud failure)
  * every remaining entity_configs entry classifies by its DECLARED src_pk VALUE, cross-
    checked against its declared links: list:
      - equal to a link's src_pk  -> that link's payload / edge properties (the entity's
                                      links: list MUST name that link, or the two signals
                                      disagree -- a loud failure; this is where a name-
                                      divergent link-entity is joined by STRUCTURE not name)
      - equal to a hub's src_pk   -> the node's own config (name in hub_configs) or a
                                      MEASURE SATELLITE anchored to that hub (name not in
                                      hub_configs)
      - matching neither          -> a loud failure (unclassifiable identifier)

Edges come in two declared kinds:
  * link-backed  -- a physical link table (link_configs). Its binding universally declares
                    source_key/target_key columns, which MUST equal the link's declared
                    src_fk set. Self-edges (source == target) with role-named keys are
                    first-class.
  * column-level -- a relations: binding over a declared payload column (source column +
                    target entity, or a discriminator map when polymorphic). These are
                    type-graph knowledge only -- they have no physical link table. Self-
                    edges are first-class here too.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping

import yaml

# Support installed-command and direct-script execution.
if __package__ in (None, ""):
    import os as _os, sys as _sys
    _sys.path.insert(0, _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__))))

from ergasterion import emit


class GraphModelError(ValueError):
    """Raised on any classification, binding or coverage violation -- always loud,
    never a silent drop. All coverage gates route through this so the plain-script
    test can assert a non-zero, named failure per planted defect."""


REQUIRED_VERB_FIELDS = ("direction", "kind", "cardinality", "inverse")

# The domain sections the graph IR slices per domain. A subset of emit.DOMAIN_SECTIONS
# (hashdiff_exclude/res_configs carry no graph structure). Sourced from the merged,
# validated emit.load_domains() output -- never re-parsed here.
_GRAPH_SECTIONS = ("entity_configs", "hub_configs", "link_configs", "bv_configs")


# --------------------------------------------------------------------------- IR


@dataclass(frozen=True)
class RelationVerb:
    """A named relationship type. Four fields are required (direction/kind/cardinality/
    inverse); `extra` tolerates unknown keys so a domain can enrich a verb (e.g. a future
    semantic-class bullet) with no schema change -- the open, additive posture."""

    name: str
    alias: str
    direction: str
    kind: str
    cardinality: str
    inverse: str
    extra: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class Discriminator:
    """A polymorphic column-level binding: `column`'s value selects the target entity
    from `mapping` (discriminator value -> target entity name)."""

    column: str
    mapping: Mapping[str, str]


@dataclass(frozen=True)
class RelationBinding:
    """A verb bound to a source (and target) via a link or a payload column. Endpoints
    are DECLARED, never inferred from key or column names."""

    verb: str
    source: str
    target: str | None = None          # None only for a polymorphic column binding
    link: str | None = None            # set -> link-backed
    source_key: str | None = None      # link-backed only
    target_key: str | None = None      # link-backed only
    source_column: str | None = None   # column-level only
    discriminator: Discriminator | None = None

    @property
    def kind(self) -> str:
        return "link_backed" if self.link is not None else "column_level"


@dataclass(frozen=True)
class RelationModel:
    verbs: Mapping[str, RelationVerb]
    bindings: tuple[RelationBinding, ...]


@dataclass(frozen=True)
class GraphNode:
    """Factory-defined node property set: the entity id/name, its domain, and its declared
    hash / golden keys. Deliberately NOT the reference instance's field set."""

    entity: str
    domain: str
    hash_key: str
    golden_key: str


@dataclass(frozen=True)
class GraphEdge:
    source: str
    target: str
    verb: str
    inverse: str
    kind: str                              # link_backed | column_level
    link: str | None = None
    source_key: str | None = None
    target_key: str | None = None
    source_column: str | None = None
    discriminator_value: str | None = None
    properties: tuple[str, ...] = ()       # link-entity payload columns (edge properties)


@dataclass(frozen=True)
class Satellite:
    """A measure entity with no hub or golden-record table of its own, anchored to the hub
    whose src_pk it shares. Listed, never a node, never silently dropped."""

    name: str
    anchor: str
    payload: tuple[str, ...]


@dataclass(frozen=True)
class PropertyGraph:
    domain: str
    nodes: tuple[GraphNode, ...]
    edges: tuple[GraphEdge, ...]
    verbs: Mapping[str, RelationVerb]
    satellites: tuple[Satellite, ...]


# --------------------------------------------------------------- relations loader


def build_relation_model(block: Mapping[str, Any] | None, domain: str) -> RelationModel:
    """Parse a domains/<name>.yml `relations:` block into the neutral RelationModel. This
    parses the NEW relation vocabulary only -- it never re-parses the section config."""
    block = block or {}
    verbs: dict[str, RelationVerb] = {}
    for name, spec in (block.get("verbs") or {}).items():
        if not isinstance(spec, Mapping):
            raise GraphModelError(f"{domain}: verb {name!r} must be a mapping")
        missing = [f for f in REQUIRED_VERB_FIELDS if f not in spec]
        if missing:
            raise GraphModelError(
                f"{domain}: verb {name!r} missing required field(s): {', '.join(missing)}"
            )
        if "alias" not in spec:
            raise GraphModelError(f"{domain}: verb {name!r} missing required field: alias")
        extra = {
            k: v
            for k, v in spec.items()
            if k not in ("alias", *REQUIRED_VERB_FIELDS)
        }
        verbs[name] = RelationVerb(
            name=name,
            alias=spec["alias"],
            direction=spec["direction"],
            kind=spec["kind"],
            cardinality=spec["cardinality"],
            inverse=spec["inverse"],
            extra=extra,
        )

    bindings: list[RelationBinding] = []
    for raw in block.get("bindings") or []:
        if not isinstance(raw, Mapping):
            raise GraphModelError(f"{domain}: each binding must be a mapping")
        for required in ("verb", "source"):
            if required not in raw:
                raise GraphModelError(f"{domain}: binding missing required field: {required}")
        link = raw.get("link")
        column = raw.get("column")
        if link is not None and column is not None:
            raise GraphModelError(
                f"{domain}: binding for verb {raw['verb']!r} declares both link and column; "
                f"a binding is exactly one kind"
            )
        if link is None and column is None:
            raise GraphModelError(
                f"{domain}: binding for verb {raw['verb']!r} declares neither link nor column"
            )
        if link is not None:
            for required in ("source_key", "target_key", "target"):
                if required not in raw:
                    raise GraphModelError(
                        f"{domain}: link-backed binding for verb {raw['verb']!r} missing "
                        f"required field: {required}"
                    )
            bindings.append(
                RelationBinding(
                    verb=raw["verb"],
                    source=raw["source"],
                    target=raw["target"],
                    link=link,
                    source_key=raw["source_key"],
                    target_key=raw["target_key"],
                )
            )
        else:
            disc_raw = raw.get("discriminator")
            discriminator = None
            if disc_raw is not None:
                if "column" not in disc_raw or "map" not in disc_raw:
                    raise GraphModelError(
                        f"{domain}: discriminator for verb {raw['verb']!r} needs column + map"
                    )
                discriminator = Discriminator(
                    column=disc_raw["column"], mapping=dict(disc_raw["map"])
                )
            elif "target" not in raw:
                raise GraphModelError(
                    f"{domain}: column-level binding for verb {raw['verb']!r} needs a target "
                    f"entity or a discriminator map"
                )
            bindings.append(
                RelationBinding(
                    verb=raw["verb"],
                    source=raw["source"],
                    target=raw.get("target"),
                    source_column=column,
                    discriminator=discriminator,
                )
            )
    return RelationModel(verbs=verbs, bindings=tuple(bindings))


def _slice_config(merged: Mapping[str, Any], raw: Mapping[str, Any]) -> dict[str, Any]:
    """Slice the merged, validated section config down to one domain's declared keys.
    Values come from emit.load_domains() (the one reader); the raw file supplies only the
    provenance -- which keys belong to this domain."""
    sliced: dict[str, dict[str, Any]] = {}
    for section in _GRAPH_SECTIONS:
        keys = list((raw.get(section) or {}).keys())
        sliced[section] = {key: merged[section][key] for key in keys}
    return sliced


def load_graph_inputs_with_config(
    domains_dir: Path | None = None,
) -> dict[str, tuple[PropertyGraph, dict[str, Any]]]:
    """Load every domain and build its PropertyGraph, keeping the sliced section config
    beside each graph. The config carries the declared table paths and keys the estate
    binding (build_estate) needs; the PropertyGraph itself is deliberately table-agnostic.
    One reader still: emit.load_domains() supplies every value."""
    if domains_dir is None:
        domains_dir = emit.DOMAINS_DIR
    merged = emit.load_domains(domains_dir)  # the one reader: validated + hashdiff-derived
    result: dict[str, tuple[PropertyGraph, dict[str, Any]]] = {}
    for path in sorted(domains_dir.glob("*.yml")):
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            raise GraphModelError(f"{path}: expected a YAML mapping")
        domain = path.stem
        config = _slice_config(merged, raw)
        model = build_relation_model(raw.get("relations"), domain)
        result[domain] = (build_property_graph(domain, config, model), config)
    return result


def load_graph_inputs(domains_dir: Path | None = None) -> dict[str, PropertyGraph]:
    """Load every domain and build its PropertyGraph. Real domains and the fixture domain
    both reach build_property_graph() through this one path -- zero engine special-casing."""
    return {
        domain: graph
        for domain, (graph, _config) in load_graph_inputs_with_config(domains_dir).items()
    }


# ------------------------------------------------------------- entity classification


@dataclass(frozen=True)
class _Classification:
    node_entities: tuple[str, ...]
    link_payloads: Mapping[str, str]       # link name -> payload entity name
    satellites: tuple[Satellite, ...]


def _invert_src_pk(configs: Mapping[str, Mapping[str, Any]], kind: str) -> dict[str, str]:
    by_src_pk: dict[str, str] = {}
    for name, cfg in configs.items():
        src_pk = cfg.get("src_pk")
        if src_pk is None:
            raise GraphModelError(f"{kind} {name!r} declares no src_pk")
        if src_pk in by_src_pk:
            raise GraphModelError(
                f"{kind} src_pk {src_pk!r} is shared by {by_src_pk[src_pk]!r} and {name!r}"
            )
        by_src_pk[src_pk] = name
    return by_src_pk


def classify_entities(config: Mapping[str, Any]) -> _Classification:
    """Classify every entity config as a node, edge payload, or measure satellite.
    {node, link-payload, satellite} or the whole classification fails loudly."""
    entity_configs = config["entity_configs"]
    hub_configs = config["hub_configs"]
    link_configs = config["link_configs"]

    hub_names = set(hub_configs)
    link_names = set(link_configs)
    collision = hub_names & link_names
    if collision:
        raise GraphModelError(
            f"hub_configs and link_configs are not disjoint (collision): {sorted(collision)}"
        )

    hub_by_src_pk = _invert_src_pk(hub_configs, "hub")
    link_by_src_pk = _invert_src_pk(link_configs, "link")

    node_entities: list[str] = []
    link_payloads: dict[str, str] = {}
    satellites: list[Satellite] = []

    for entity_name, ec in entity_configs.items():
        src_pk = ec.get("src_pk")
        if src_pk is None:
            raise GraphModelError(f"entity {entity_name!r} declares no src_pk")
        links = list(ec.get("links", []) or [])
        if src_pk in link_by_src_pk:
            link_name = link_by_src_pk[src_pk]
            if link_name not in links:
                raise GraphModelError(
                    f"entity {entity_name!r}: its src_pk {src_pk!r} identifies link "
                    f"{link_name!r} (edge payload) but its links: list {links} does not name "
                    f"it -- disagreeing join signals (src_pk vs links:)"
                )
            if link_name in link_payloads:
                raise GraphModelError(
                    f"link {link_name!r} has two payload entities: {link_payloads[link_name]!r} "
                    f"and {entity_name!r}"
                )
            link_payloads[link_name] = entity_name
        elif src_pk in hub_by_src_pk:
            hub_name = hub_by_src_pk[src_pk]
            if entity_name in hub_names:
                node_entities.append(entity_name)
            else:
                satellites.append(
                    Satellite(
                        name=entity_name,
                        anchor=hub_name,
                        payload=tuple(ec.get("payload", [])),
                    )
                )
        else:
            raise GraphModelError(
                f"entity {entity_name!r}: its src_pk {src_pk!r} matches neither a hub nor a "
                f"link src_pk -- unclassifiable identifier"
            )

    if set(node_entities) != hub_names:
        missing = sorted(hub_names - set(node_entities))
        raise GraphModelError(
            f"every hub must have a node entity_config whose src_pk is the hub src_pk; "
            f"unmatched hub(s): {missing}"
        )

    return _Classification(
        node_entities=tuple(node_entities),
        link_payloads=dict(link_payloads),
        satellites=tuple(satellites),
    )


# ------------------------------------------------------------------- builders


def build_nodes(domain: str, config: Mapping[str, Any]) -> tuple[GraphNode, ...]:
    """One node per hub. The node property set is factory-defined."""
    nodes = []
    for name, hub in config["hub_configs"].items():
        if "src_pk" not in hub or "src_nk" not in hub:
            raise GraphModelError(f"hub {name!r} must declare src_pk and src_nk")
        nodes.append(
            GraphNode(
                entity=name,
                domain=domain,
                hash_key=hub["src_pk"],
                golden_key=hub["src_nk"],
            )
        )
    return tuple(nodes)


def build_edges(
    config: Mapping[str, Any],
    model: RelationModel,
    link_payloads: Mapping[str, str],
    node_names: set[str],
) -> tuple[GraphEdge, ...]:
    link_configs = config["link_configs"]
    entity_configs = config["entity_configs"]
    verbs = model.verbs
    edges: list[GraphEdge] = []

    for b in model.bindings:
        if b.verb not in verbs:
            raise GraphModelError(f"binding references undeclared verb {b.verb!r}")
        inverse = verbs[b.verb].inverse
        if b.source not in node_names:
            raise GraphModelError(
                f"binding for verb {b.verb!r}: source {b.source!r} does not resolve to a node "
                f"entity -- unresolvable source endpoint"
            )

        if b.kind == "link_backed":
            link = link_configs.get(b.link)
            if link is None:
                raise GraphModelError(
                    f"binding for verb {b.verb!r} names unknown link {b.link!r}"
                )
            if b.target not in node_names:
                raise GraphModelError(
                    f"binding for verb {b.verb!r}: target {b.target!r} does not resolve to a "
                    f"node entity -- unresolvable target endpoint"
                )
            # Self-edge check FIRST: a self relationship whose two ends share one key column
            # cannot be ordered -- a mis-declared self-edge.
            if b.source == b.target and b.source_key == b.target_key:
                raise GraphModelError(
                    f"binding for verb {b.verb!r} on {b.source!r}: a self-edge needs two "
                    f"distinct role keys, got {b.source_key!r} for both -- mis-declared self-edge"
                )
            declared_keys = {b.source_key, b.target_key}
            src_fk = set(link.get("src_fk", []))
            if declared_keys != src_fk:
                raise GraphModelError(
                    f"binding for verb {b.verb!r}: source_key/target_key {sorted(declared_keys)} "
                    f"must equal link {b.link!r} src_fk {sorted(src_fk)}"
                )
            payload_entity = link_payloads.get(b.link)
            properties = (
                tuple(entity_configs[payload_entity]["payload"])
                if payload_entity is not None
                else ()
            )
            edges.append(
                GraphEdge(
                    source=b.source,
                    target=b.target,
                    verb=b.verb,
                    inverse=inverse,
                    kind="link_backed",
                    link=b.link,
                    source_key=b.source_key,
                    target_key=b.target_key,
                    properties=properties,
                )
            )
        else:  # column-level
            source_payload = set(entity_configs[b.source].get("payload", []))
            if b.source_column not in source_payload:
                raise GraphModelError(
                    f"binding for verb {b.verb!r}: source column {b.source_column!r} is not in "
                    f"{b.source!r}'s payload"
                )
            if b.discriminator is not None:
                if b.discriminator.column not in source_payload:
                    raise GraphModelError(
                        f"binding for verb {b.verb!r}: discriminator column "
                        f"{b.discriminator.column!r} is not in {b.source!r}'s payload"
                    )
                for disc_value, target in b.discriminator.mapping.items():
                    if target not in node_names:
                        raise GraphModelError(
                            f"binding for verb {b.verb!r}: discriminated target {target!r} "
                            f"(value {disc_value!r}) does not resolve to a node entity -- "
                            f"unresolvable target endpoint"
                        )
                    edges.append(
                        GraphEdge(
                            source=b.source,
                            target=target,
                            verb=b.verb,
                            inverse=inverse,
                            kind="column_level",
                            source_column=b.source_column,
                            discriminator_value=disc_value,
                        )
                    )
            else:
                if b.target not in node_names:
                    raise GraphModelError(
                        f"binding for verb {b.verb!r}: target {b.target!r} does not resolve to a "
                        f"node entity -- unresolvable target endpoint"
                    )
                edges.append(
                    GraphEdge(
                        source=b.source,
                        target=b.target,
                        verb=b.verb,
                        inverse=inverse,
                        kind="column_level",
                        source_column=b.source_column,
                    )
                )
    return tuple(edges)


# --------------------------------------------------------------- coverage gates


def _assert_coverage(
    config: Mapping[str, Any],
    model: RelationModel,
    edges: tuple[GraphEdge, ...],
) -> None:
    verbs = model.verbs

    # Inverse bijectivity: the verb -> inverse map is an involution over declared verbs.
    seen_inverse: dict[str, str] = {}
    for name, verb in verbs.items():
        if verb.inverse not in verbs:
            raise GraphModelError(
                f"verb {name!r} declares inverse {verb.inverse!r} which is not a declared verb"
            )
        if verbs[verb.inverse].inverse != name:
            raise GraphModelError(
                f"verb {name!r} inverse is not bijective: {verb.inverse!r} inverts to "
                f"{verbs[verb.inverse].inverse!r}, not {name!r}"
            )
        if verb.inverse in seen_inverse and seen_inverse[verb.inverse] != name:
            raise GraphModelError(
                f"inverse collision: {name!r} and {seen_inverse[verb.inverse]!r} both declare "
                f"inverse {verb.inverse!r}"
            )
        seen_inverse[verb.inverse] = name

    # Every declared link is a typed edge.
    link_names = set(config["link_configs"])
    bound_links = {e.link for e in edges if e.kind == "link_backed"}
    unmapped = link_names - bound_links
    if unmapped:
        raise GraphModelError(
            f"link(s) declared in link_configs with no relation binding -- unmapped link: "
            f"{sorted(unmapped)}"
        )

    # Bidirectional coverage: every edge names a declared verb AND a declared inverse.
    for e in edges:
        if not e.verb or not e.inverse:
            raise GraphModelError("edge is missing its verb or inverse")
        if e.verb not in verbs or e.inverse not in verbs:
            raise GraphModelError(
                f"edge names verb {e.verb!r}/inverse {e.inverse!r} not in the declared verb set"
            )

    # Every declared verb binds at least one edge (as a forward verb or as an inverse).
    used: set[str] = set()
    for e in edges:
        used.add(e.verb)
        used.add(e.inverse)
    orphan = set(verbs) - used
    if orphan:
        raise GraphModelError(f"verb(s) bind no edge -- orphan verb: {sorted(orphan)}")


def build_property_graph(
    domain: str,
    config: Mapping[str, Any],
    model: RelationModel,
) -> PropertyGraph:
    """Build the domain-neutral graph: classify, build nodes and edges, then
    assert total coverage. Real domains and the fixture domain both run exactly this."""
    classification = classify_entities(config)
    nodes = build_nodes(domain, config)
    node_names = {n.entity for n in nodes}
    edges = build_edges(config, model, classification.link_payloads, node_names)
    _assert_coverage(config, model, edges)
    return PropertyGraph(
        domain=domain,
        nodes=nodes,
        edges=edges,
        verbs=model.verbs,
        satellites=classification.satellites,
    )


# --------------------------------------------------------- estate binding
#
# The estate binding maps the type-level graph onto the ACTUAL EMITTED TABLES, by the
# Mechanical rule determined entirely by domains/*.yml,
# never invented per-domain:
#   * NODE table  = the node entity's bv_configs golden-record table, keyed by its declared
#                   hub_pk/hub_nk; falling back to its hub table (keyed by the hub family's
#                   src_pk/src_nk) when no bv_configs entry exists.
#   * EDGE table  = the link table (keyed by its declared src_pk), with edge-side FK columns
#                   equal to its declared src_fk list.
# Nodes are a subset of hub-backed entities by construction, so every node resolves a table.
# Only LINK-BACKED edges bind: column-level relations are type-graph knowledge with no
# physical link table and never appear in the estate binding.


class EstateBindingError(GraphModelError):
    """Raised on any estate cross-check failure (table existence, key correctness)."""


@dataclass(frozen=True)
class NodeTableBinding:
    entity: str
    table: str                        # dbt model name (path stem) of the bound table
    key_columns: tuple[str, ...]      # the node's DECLARED keys (hub_pk/hub_nk or src_pk/src_nk)
    origin: str                       # "bv_golden_record" | "hub" -- provenance of the binding


@dataclass(frozen=True)
class EdgeTableBinding:
    link: str
    verb: str
    table: str                        # dbt model name of the link table
    key_column: str                   # the link's declared src_pk
    source_entity: str
    target_entity: str
    source_fk: str                    # edge-side FK column referencing the source node
    target_fk: str                    # edge-side FK column referencing the target node
    fk_columns: tuple[str, ...]       # the link's DECLARED src_fk list (edge-side key set)


@dataclass(frozen=True)
class EstateGraph:
    domain: str
    node_tables: tuple[NodeTableBinding, ...]
    edge_tables: tuple[EdgeTableBinding, ...]


def _model_name(cfg: Mapping[str, Any], what: str, name: str) -> str:
    path = cfg.get("path")
    if not path:
        raise EstateBindingError(f"{what} {name!r} declares no path -- cannot bind its table")
    return Path(path).stem


def _node_binding(
    entity: str,
    config: Mapping[str, Any],
) -> NodeTableBinding:
    """Resolve one node entity to its emitted table by the pinned mechanical rule: the
    bv_configs golden-record table keyed by hub_pk/hub_nk, else the hub table keyed by
    src_pk/src_nk. Nodes are hub-backed by construction, so a table always resolves."""
    bv = config["bv_configs"].get(entity)
    if bv is not None:
        for key in ("hub_pk", "hub_nk"):
            if not bv.get(key):
                raise EstateBindingError(
                    f"bv_configs[{entity!r}] declares no {key} -- cannot bind node keys"
                )
        return NodeTableBinding(
            entity=entity,
            table=_model_name(bv, "bv_configs", entity),
            key_columns=(bv["hub_pk"], bv["hub_nk"]),
            origin="bv_golden_record",
        )
    hub = config["hub_configs"].get(entity)
    if hub is None:
        raise EstateBindingError(
            f"node entity {entity!r} resolves to neither a bv_configs nor a hub_configs table"
        )
    for key in ("src_pk", "src_nk"):
        if not hub.get(key):
            raise EstateBindingError(
                f"hub_configs[{entity!r}] declares no {key} -- cannot bind node keys"
            )
    return NodeTableBinding(
        entity=entity,
        table=_model_name(hub, "hub_configs", entity),
        key_columns=(hub["src_pk"], hub["src_nk"]),
        origin="hub",
    )


def build_estate(
    domain: str,
    config: Mapping[str, Any],
    graph: PropertyGraph,
) -> EstateGraph:
    """Derive the estate binding from the type graph + declarations. Pure and mechanical;
    it invents nothing per-domain. Runs the two-sided key-correctness cross-check before
    returning, so an EstateGraph is always self-consistent with its declarations."""
    node_tables = tuple(
        _node_binding(node.entity, config)
        for node in sorted(graph.nodes, key=lambda n: n.entity)
    )

    link_configs = config["link_configs"]
    edge_tables: list[EdgeTableBinding] = []
    seen_links: set[str] = set()
    for edge in graph.edges:
        if edge.kind != "link_backed":
            continue  # column-level edges have no physical table -- excluded by construction
        if edge.link in seen_links:
            raise EstateBindingError(
                f"link {edge.link!r} backs more than one edge -- the estate binding needs one "
                f"physical table per link"
            )
        seen_links.add(edge.link)
        link = link_configs.get(edge.link)
        if link is None:
            raise EstateBindingError(f"link-backed edge names unknown link {edge.link!r}")
        if not link.get("src_pk"):
            raise EstateBindingError(f"link_configs[{edge.link!r}] declares no src_pk")
        edge_tables.append(
            EdgeTableBinding(
                link=edge.link,
                verb=edge.verb,
                table=_model_name(link, "link_configs", edge.link),
                key_column=link["src_pk"],
                source_entity=edge.source,
                target_entity=edge.target,
                source_fk=edge.source_key,
                target_fk=edge.target_key,
                fk_columns=tuple(link.get("src_fk", [])),
            )
        )

    estate = EstateGraph(
        domain=domain,
        node_tables=node_tables,
        edge_tables=tuple(sorted(edge_tables, key=lambda e: e.table)),
    )
    assert_estate_key_correctness(estate, config)
    return estate


def assert_estate_key_correctness(estate: EstateGraph, config: Mapping[str, Any]) -> None:
    """Cross-check (ii) -- two-sided key correctness, both sides read from domains/*.yml,
    NEVER a string-equality between edge FK column names and node key names (that would
    break the role-named self-edge predecessor_gp_hk/successor_gp_hk -> gp_hk).

      * REFERENCES side -- each node binding's key columns equal that entity's DECLARED keys
        (bv_configs hub_pk/hub_nk, or hub_configs src_pk/src_nk), re-derived independently.
      * edge side       -- each edge binding's own FK columns ({source_fk, target_fk}) equal
        the link's DECLARED src_fk list, and its endpoints reference bound node tables.
    """
    by_entity = {n.entity: n for n in estate.node_tables}

    for node in estate.node_tables:
        expected = _node_binding(node.entity, config)
        if node.key_columns != expected.key_columns or node.table != expected.table:
            raise EstateBindingError(
                f"node {node.entity!r}: bound table/keys {node.table}{list(node.key_columns)} "
                f"disagree with the declared binding {expected.table}{list(expected.key_columns)}"
            )

    link_configs = config["link_configs"]
    for edge in estate.edge_tables:
        declared_src_fk = set(link_configs[edge.link].get("src_fk", []))
        if {edge.source_fk, edge.target_fk} != declared_src_fk:
            raise EstateBindingError(
                f"edge on link {edge.link!r}: edge-side FK columns "
                f"{sorted({edge.source_fk, edge.target_fk})} must equal the link's declared "
                f"src_fk {sorted(declared_src_fk)}"
            )
        if tuple(sorted(edge.fk_columns)) != tuple(sorted(declared_src_fk)):
            raise EstateBindingError(
                f"edge on link {edge.link!r}: recorded fk_columns {sorted(edge.fk_columns)} "
                f"disagree with the link's declared src_fk {sorted(declared_src_fk)}"
            )
        for endpoint, entity in (("source", edge.source_entity), ("target", edge.target_entity)):
            if entity not in by_entity:
                raise EstateBindingError(
                    f"edge on link {edge.link!r}: {endpoint} entity {entity!r} does not bind a "
                    f"node table -- unresolvable estate endpoint"
                )


def manifest_model_names(manifest_path: Path) -> set[str]:
    """The set of dbt model names in a parsed manifest. Table-level only: the manifest
    carries no columns for raw/business-vault models (they have no schema .yml), so the
    table-existence cross-check is table-level by necessity."""
    import json

    data = json.loads(manifest_path.read_text(encoding="utf-8"))
    return {
        node["name"]
        for node in data.get("nodes", {}).values()
        if node.get("resource_type") == "model"
    }


def assert_estate_tables_exist(estate: EstateGraph, model_names: set[str]) -> None:
    """Cross-check (i) -- every table the estate DDL references exists in the emitted model
    set (from dbt parse's target/manifest.json). Table-level only."""
    missing: list[str] = []
    for node in estate.node_tables:
        if node.table not in model_names:
            missing.append(f"node table {node.table} (entity {node.entity})")
    for edge in estate.edge_tables:
        if edge.table not in model_names:
            missing.append(f"edge table {edge.table} (link {edge.link})")
    if missing:
        raise EstateBindingError(
            "estate table-existence cross-check failed -- table(s) not in the emitted model "
            "set:\n  " + "\n  ".join(sorted(missing))
        )


def load_estates(domains_dir: Path | None = None) -> dict[str, EstateGraph]:
    """Build the estate binding for every domain, from the same one-reader inputs."""
    return {
        domain: build_estate(domain, config, graph)
        for domain, (graph, config) in load_graph_inputs_with_config(domains_dir).items()
    }


def main() -> int:
    graphs = load_graph_inputs()
    for domain, graph in graphs.items():
        print(
            f"{domain}: {len(graph.nodes)} nodes, {len(graph.edges)} edges, "
            f"{len(graph.verbs)} verbs, {len(graph.satellites)} satellites"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
