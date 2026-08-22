# Ontology Map Lane

The ontology map lane turns a domain declaration into a typed graph map. A capable reader can use it to answer a basic structural question before opening the warehouse. The map names which entity types exist, which relationships connect them, what those relationships mean, which keys carry them, and where the graph binds to emitted tables.

The lane is domain-framed. Domains provide the nouns and verbs. The factory provides the loaders, validation rules, and serialisers. A domain can say that an order is `PLACED_BY` a customer or that a fund is `MANAGED_BY` a management firm, but those words live in `domains/<domain>.yml`, not in the generator.

## Declaration Schema

The declaration lives in a top-level `relations:` block in each domain YAML file.

```yaml
relations:
  verbs:
    PLACED_BY:
      alias: placed-by
      direction: directed
      kind: association
      cardinality: many_to_one
      inverse: PLACED
  bindings:
    - verb: PLACED_BY
      source: order
      target: customer
      link: order_customer
      source_key: order_hk
      target_key: customer_hk
```

Each verb requires `alias`, `direction`, `kind`, `cardinality`, and `inverse`. Extra verb fields are allowed, so a domain can add richer semantic metadata without changing the loader. Bindings attach verbs to declared structure.

A link-backed binding names a physical link from `link_configs` and gives the source and target key columns used by that link. A column-level binding names a payload column on the source entity. It then gives either a single target entity or a discriminator map when one column can point at different entity types. Endpoints are always explicit. The map lane does not derive source or target entity names from column names.

## Nodes, Edges, And Satellites

A hub is the stable identity table for an entity, such as customer, order, product, fund, or management firm. Its hash key is the technical identifier used by the warehouse. Its golden key is the business identifier chosen by the domain's survivorship rules.

The node set is the domain's `hub_configs` set. A hub entity becomes a graph node, with its entity id, domain name, hash key, and golden key.

The edge set comes from typed relation bindings. Edges backed by a link table use `link_configs` and can carry link-entity payload columns as edge properties. Edges carried by a payload column are schema knowledge over that column. Both kinds appear in the type graph.

Remaining entity configs are classified by their declared key structure. A payload entity whose primary key matches a link key becomes edge properties for that link. A measure entity whose primary key matches a hub key is listed as a satellite anchored to that hub. Satellites are part of the graph description. They are not graph nodes.

## Artefacts

Running `python ergasterion/emit_graph.py` emits the graph family under `graphs/<domain>/`.

| Artefact | Purpose |
|---|---|
| `<domain>-nodes.csv` | Node table for the type graph. |
| `<domain>-edges.csv` | Edge table for the type graph. |
| `<domain>-graph.cypher` | openCypher creation script for the type graph. |
| `<domain>-graph.pgq.sql` | SQL/PGQ creation script for the type graph tables and graph. |
| `<domain>-graph.gql` | GQL graph type declaration. It is generated syntax, not a tested engine-conformance claim. |
| `<domain>-graph-description.json` | Compact description of node types, relation types, example traversals, CSV headers, and satellites. |
| `<domain>-estate.pgq.sql` | SQL/PGQ binding over the emitted relational estate. |

The artefacts are regenerated from declarations and are not hand-edited. `python ergasterion/emit_graph.py --check` fails when the committed graph files differ from what the declarations produce.

## Type Graph

The type graph describes the domain schema as a graph. It carries every hub-backed entity as a node. It also carries every link-backed relationship and every declared column-level relationship as an edge. Every classified satellite is listed in the JSON description.

The CSV files are the common table form. The openCypher, SQL/PGQ, and GQL files serialise the same graph vocabulary for tools that prefer graph query syntax. The GQL file is not conformance-tested against a live engine. The JSON description is the easiest starting point for a reader. It names relation types, source and target entity sets, example traversals, and satellite anchors.

## Estate Graph

The estate graph binds the map to the actual emitted tables. A business-vault golden-record table is the cleaned table that holds the winning version of an entity after source matching and survivorship. Node tables use those golden-record tables when they exist, with hub tables as the fallback. Edge tables are the physical link tables from `link_configs`.

Only link-backed relationships appear in the estate binding because only they have physical link tables. Column-level relationships remain in the type graph. The estate SQL/PGQ file is a declarative binding over the relational estate. The repository validates its references offline and does not execute it against a graph engine.

## Worked Example: E-Commerce

The e-commerce domain has three node types: `customer`, `order`, and `product`. It declares four verbs:

| Verb | Meaning | Inverse |
|---|---|---|
| `PLACED_BY` | an order points to the customer that placed it | `PLACED` |
| `PLACED` | a customer points to an order they placed | `PLACED_BY` |
| `INCLUDES` | an order points to a product line | `LINE_OF` |
| `LINE_OF` | a product points back to an order line | `INCLUDES` |

Two link-backed bindings connect that vocabulary to the emitted model:

```yaml
bindings:
  - verb: PLACED_BY
    source: order
    target: customer
    link: order_customer
    source_key: order_hk
    target_key: customer_hk
  - verb: INCLUDES
    source: order
    target: product
    link: order_line_product
    source_key: order_hk
    target_key: product_hk
```

The `order_line_product` link also has a payload entity called `order_line`. The names differ, so the classification uses the declared structure: `order_line.src_pk` matches the link key, and `order_line.links` names the link. Its payload columns are stored as edge properties.

## What This Is Not

The map lane does not generate the mart layer or claim ownership of mart foreign keys. Marts and their relationship tests remain hand-authored dbt models on top of the generated source-facing pipeline.

The map lane does not turn satellites into nodes. It lists satellites with their anchor entity and payload columns in the graph description.

The map lane does not emit document nodes. It maps the structured domain declarations that exist in this repository.

The map lane does not emit OWL, RDF, or SHACL. The committed serialisations are CSV, openCypher, SQL/PGQ, GQL DDL, and JSON description files.
