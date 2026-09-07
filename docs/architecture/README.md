# The Ergasterion architecture guide

A warehouse is fed by many source systems. Each one arrives with its own keys, its own
column names, its own idea of a date, and its own delivery habits. The usual answer is a
pipeline per source. That code receives the feed, maps its fields, keeps history, applies
rules, builds tables, and publishes tests and contracts. Every source gets a separate copy.
Every source change means editing that copy.

![Several source systems feeding separate hand-written pipelines before a warehouse](problem.svg)

The repeated code is only part of the cost. The decisions that matter are spread through
it. Those decisions include what a field means, which source wins a disagreement, and what
the warehouse promises to consumers.

Ergasterion puts those decisions in version-controlled declarations that people can read
and change. Its engine generates the pipeline, tests, contracts, metadata, and lineage from
those declarations.

## What you can watch it do

The account-free demonstration makes the architecture visible in one run:

1. It regenerates all 124 declared products and reports any output that has drifted.
2. It builds the e-commerce and investment domains on DuckDB. Every generated test and all
   40 known-answer assertions run against the built relations.
3. It prints three e-commerce results: segment revenue, one resolved customer, and order
   totals reconciled to their source.

Run `bash demo/run_offline_demo.sh` from a prepared checkout. The
[demonstration guide](../../demo/README.md) explains the outputs and the separate landing
walkthrough.

## Follow one customer through the estate

CARTIVO, MERCARO, and RELATIO are three invented source systems in the e-commerce example.
All three hold a record for Ava Thompson. The records use different keys and disagree on
some contact details.

**Receive.** The engine preserves each delivered payload, parses it under a written
contract, and records every quality result. Accepted rows publish. Rejected rows retain a
locator to the raw bytes that failed. The output is a landing product.

**Align.** One declaration per source maps its native names and types onto the domain's
shared customer schema. A person can review and change each mapping. The output is a
derivation product.

**Resolve.** Another declaration says which records describe the same customer. A shared
loyalty ID is the first key, and a normalised email is the fallback. It also says that
RELATIO supplies contact values when the sources disagree. The output is an integration
product.

**Combine.** Further declarations join the agreed customer to orders and products. They
also combine order lines from two sales channels. Coverage checks catch a missing input.
The outputs are consolidation products.

**Serve.** The last declarations publish stable customer interfaces, a dimensional order
model, and an order summary. Each product publishes a contract. The next product reads
that contract as its input. The outputs are serving products.

![Ava's three source records moving through landing, alignment, resolution, and a published customer interface](engine-architecture-graph.svg)

## Architecture at a glance

One file declares each product's inputs, ordered operations, and published form. The engine
checks the file, turns it into a neutral execution plan, and assigns each operation to a
registered generator. Database-specific rules enter only through the selected adapter. The
engine then writes the output and records which checks passed.

![The engine: a declaration and estate configuration in, a neutral plan, routed to plug-in translators and adapters, artefacts, a contract, the graph and evidence out](engine-architecture-engine.svg)

An organisation can call its product groups Bronze, Silver and Gold, or use its own names.
The names live in estate configuration. The engine uses each name as a lookup key and does
not attach behaviour to it. The governing principle is **layers are configuration**.

## Vocabulary

| Term | Meaning |
|---|---|
| Estate | One governed set of products, profiles, shapes, adapters and configuration, owned by one organisation. |
| Product | The unit the engine builds. A declaration with sources, a composition and a target. It emits artefacts and exactly one contract. |
| Source | A published contract this product consumes. Never a table name. |
| Pattern | One of the fifteen building blocks in the catalogue. The engine has no other processing vocabulary. |
| Occurrence | One use of a pattern inside a product, with that pattern's configuration. A product may use a pattern more than once. |
| Composition | The ordered occurrences of a product. |
| Profile | A declared composition constraint: which patterns are mandatory, optional and forbidden, and in what order. |
| Shape | The modelling form the target takes. A registered rendering with its own declaration schema. One per product. |
| Target | What the product publishes: its shape, its relations, its contract, its materialisation intent. |
| Contract | The versioned interface a product publishes. Generated from the declaration, never hand-written. |
| Edge | A dependency between two products through a contract, or between two occurrences inside a product through a handoff schema. |
| Graph | The acyclic set of products and their contract edges. |
| Generation | A product's depth in the graph. First, later and consolidating generations are depth, not types. |
| Translator | The technology axis. It renders occurrences and shapes into artefacts for one technology. |
| Adapter | The platform axis. It owns dialect, physical types, identifier rules, layout and structural budgets. |
| Named rule | A business rule referenced by name with a neutral signature and implemented in code per translator, and per adapter where dialects differ. |
| Expression mode | Estate policy for inline SQL: `sql`, the default, or `named_only`. |
| Layer label | A name an estate gives to a group of profiles. A product declares the label it sits in. |
| Translator table | Estate configuration naming, for each label, the translator that renders each pattern and each shape. |
| Runtime binding | Physical coordinates and environment for a deployment. Outside every product declaration. |

## What you declare

### A product declaration

A product declaration is engine-neutral. It states what the product is, what it consumes,
what it does, and what it publishes. It carries no engine syntax, no platform reference,
no file path and no connection detail.

```yaml
product:
  name: customer
  domain: ecommerce
  version: "1"
  layer: silver                  # the estate's label
  profile: integration           # required when the label admits more than one profile
  owner: customer-domain

sources:
  - contract: ecommerce.cartivo_customer@1
    expect:
      fields: [customer_record_id, loyalty_id, email, customer_city]
  - contract: ecommerce.relatio_customer@1

steps:
  - pattern: batch_transfer
  - pattern: data_validation
    stage: pre
    rules:
      - {field: customer_record_id, completeness: 1.0}
  - pattern: schema_transform
    mapping:
      - {from: email, to: customer_email, type: string}
  - pattern: data_curation
    entity: customer
    resolution: {strategy: deterministic, keys: [loyalty_id, normalised_email]}
    survivorship: {customer_email: contact_authority}
  - pattern: data_contracts
  - pattern: lineage_capture
  - pattern: metadata_capture
  - pattern: schema_publish
  - pattern: data_publish

target:
  shape: declared
  contract:
    freshness: "daily by 06:00 UTC"
    access: {classification: internal}
```

The `steps` block is the composition. Each entry is one occurrence, and its keys are that
pattern's own configuration schema and nothing else. A shape may add a section to the
`target` block, and only there.

Two further keys appear on a product that reads more than one source. `combine` states
how the sources come together, by `union` of their rows under one conformed schema or by
`merge` of their columns on declared keys. Each source then carries `conform`, a mapping
that renames and casts its fields onto the shape the combination needs. A source of a
producer that publishes several relations also carries `relation`, naming the one it
reads as the producer's shape names it.

### Stored names

Every name in a declaration is a plain lower-case logical name, and the engine never
folds, quotes or rewrites one. Where an interface outside the estate requires an exact
name the estate does not own, that name is declared beside the logical one in an optional
`physical` block:

```yaml
physical:
  name: LEGACY_TARIFF_MASTER_T0   # the table this relation is stored as
  schema: LEGACY_WORK             # optional: the schema it is stored in
  fields:
    - {name: period_record_id, physical_name: PERIOD_RECORD_ID}
    - {name: tariff_type_no, physical_name: TARIFF_TYPE_NO}
  relations:                      # only for a shape that publishes several
    fact_order_line:
      name: FACT_ORDER_LINE_T0
      fields:
        - {name: order_line_id, physical_name: ORDER_LINE_ID}
```

A landing product's delivered columns state the same thing where the extract arrives
under names the estate does not own: each `fixture` field may carry `physical_name`, and
the portable Landing IDL carries the same optional name for a delivered table and each
delivered column.

Everything else follows from that one block. A consumer references producer columns by
logical name only and never spells a stored name it does not own; the renderer resolves
the producer's stored name through its contract and reads `<stored> as <logical>`. The
published relation's final projection renders `<logical> as <stored>`, and the relation
takes the declared table name and schema. Quoting is the running adapter's own: the
generated SQL stays one text for every platform and carries no quote character of its
own. The declared schema is the one place the generated project defines dbt's
`generate_schema_name` hook, and it applies to the marked relations alone; every other
node keeps dbt's own default answer.

The generated contract carries both names, the runtime manifest records the stored schema,
table and column names beside the logical ones, and the product graph resolves each
logical column to the name the built relation carries. A declaration that states no
`physical` block emits exactly what it always did.

Five rules hold the block honest, and each fails closed naming the product, the relation,
the column and the adapter: `physical_name_missing` for a blank name,
`physical_name_unportable` for one carrying an adapter's quote character, a dot, a line
break or more characters than the estate budgets, `physical_name_duplicate` for two
columns of one relation reaching one stored name under the adapter's own comparison rule,
`physical_relation_conflict` for two published relations reaching one stored schema and
table, and `physical_schema_unaddressable` for a schema override an adapter cannot
address.

### Source declarations

A product's first generation has to start somewhere. A **source declaration** at
`declarations/<source>.yml` describes one source system's tables and columns, the types
they carry, and how a delivered batch reaches the estate. It is the input a landing
product binds to, either a fixture relation for local proof or a delivered relation
under a Landing Product Contract.

Two importers seed that file so nobody types a column list twice: `ergasterion
import-odcs` reads a supplier's Open Data Contract Standard contract, and `ergasterion
import-ddl` reads the source system's own `CREATE TABLE` statements. Both transcribe
structure only, and mark every decision they cannot make with an explicit note.
[`DEMO.md`](../../DEMO.md) works both through end to end.

### Estate configuration

`estate.yml` is where an organisation's own policy lives. It declares:

- the **layer labels** and, for each, the reference profiles it admits;
- the **adapters**, each marked `reference` or `deployment`, and which one is the
  `final_target`;
- the **translator table**: for each label, the translator that renders each pattern and
  each shape;
- the **expression mode** and the structured types the estate enables;
- the estate namespace, support contact and owning team that every generated contract's
  ownership section is built from.

Per-adapter structural budgets and interface boundaries sit beside it in
`declarations/targets/<adapter>.yml`. The estate's own named-rule signatures sit in
`rules/`.

The engine enforces what the estate gives it and fails closed on what it does not. A
label with no translator-table entry for a pattern its profile carries fails at routing,
naming the label, the pattern and the adapter.

### Business rules: two routes, both neutral

A declaration states a business rule in one of two ways, and neither is an escape hatch
for the other.

**Inline SQL** is the declaration language. A calculated field, a filter predicate, an
aggregate, or a whole select body is written as SQL in the declaration. There is no
closed subset and no fixed function list. A real SQL parser checks every inline
expression. A parse error names the product, occurrence, and position. The engine resolves
each column against the schema visible at that point. An unresolved column fails closed,
and each resolved column becomes field-level lineage. The text is rendered verbatim. One
text runs on every declared adapter, and the engine never transpiles.

**Named rules** are the other route. A declaration references a rule by name. The rule
has one neutral signature -- its name, its input types, its output type, its version --
declared once in a rule catalogue. Its implementations are code, held in translator and
adapter packages. This is the route for anything an estate wants reviewed and versioned
as code, and for anything where dialects genuinely differ.

The catalogue has two sources and merges them without shadowing: the engine's reference
catalogue under `ergasterion/rules/reference/`, and the estate's own catalogue under
`rules/`, which may add rules the reference catalogue does not carry. A name declared
twice, in either source or across both, fails closed. An estate extends the reference
catalogue; it never redefines a piece of it.

Expression mode is estate policy, never a per-product or per-column choice:

| Mode | What a declaration may carry | What proves it |
|---|---|---|
| `sql`, the default | Inline SQL and named rules together. | Parse, column resolution and lineage before emission; the per-adapter parse and dialect gates and the reference-adapter build after it. |
| `named_only` | No inline SQL. Every rule is a named rule. | The completeness gate: every referenced rule resolves for every declared translator and adapter pair. |

Declarations use logical types: string, integer, decimal with precision and scale,
boolean, date, timestamp, and the structured types the estate enables. Each adapter owns
the mapping to its physical types. A type mapping never appears in a declaration.

## How the engine builds it

```text
declaration
  -> validate   schema per pattern, composition versus profile, shape constraints,
                inline expression parse and column resolution, expression-mode policy,
                reference resolution, contract compatibility, duplication
  -> plan       occurrences, edges with handoff schemas, checkpoint wrapper, digest
  -> route      each occurrence to the one translator the estate's table names
  -> emit       artefacts per translator, deterministic, a generated marker on every file
  -> gate       dialect per adapter, structural budgets per adapter, determinism, drift
  -> evidence   what was proved, per adapter
```

**Validation** is deterministic and runs before generation. It checks the profile, pattern
order, shape fields, expression policy, and visible columns. It also checks rule
implementations, declaration neutrality, source contracts, and duplicate published names.
Each failure names the declaration and rule that stopped it.

**The plan** is the resolved product: occurrences with their configuration, edges
carrying the schema handed from one occurrence to the next, and the checkpoint wrapper
enclosing the composition. It carries a digest.

**Routing is declared, never inferred.** The router looks the product's declared label up
in the estate's translator table, takes the translator it names for that occurrence's
pattern, and requires that translator to have registered the capability for every
declared adapter. Every occurrence therefore has exactly one owner. Several translators
commonly serve one product: a landing label can give its ingestion patterns to the
ingestion runtime and its contract, schema, metadata and lineage steps to the publication
translator.

**Emission is byte-deterministic.** The same declarations produce the same artefacts.
Every generated file carries the marker `Generated by Ergasterion from a product
declaration`. Check mode regenerates and reports drift against what is on disk without
writing anything. Emission prints one line per product:

```text
emitted ecommerce.order_star: label=gold profile=serving shape=dimensional owner=dbt adapters=duckdb,bigquery artefacts=21
```

**Gates** run per declared adapter: dialect rules, structural budgets, deterministic
re-emission and drift detection. A gate failure names the artefact, the rule and the
adapter.

## The fifteen patterns

The catalogue is closed. Adding a processing capability means adding a pattern to the
catalogue, not adding a special case to the engine. Each pattern is a contract the engine
holds once, with its own configuration schema under
`ergasterion/schemas/patterns/`.

| Pattern | Its configuration holds | It emits |
|---|---|---|
| Batch Ingestion | Source connection reference, scope, cadence, extract mode | Landing relations and receipt records |
| Batch Transfer | Upstream contract reference | A read of a published product |
| Schema Transform | Field mapping: rename, cast, flatten, drop, null handling | A structural mapping relation |
| Calculated Fields | Named fields with SQL expressions or named rules | Derived columns |
| Data Enrichment | Lookups against reference product contracts, join keys, no-match policy | An enriched relation |
| Data Filtering | Named predicates | A filtered relation and a filter log |
| Data Validation | Rules by type, stage, threshold, on-failure policy | Tests, a quarantine relation, an abort condition |
| Data Aggregation | Grain, groups, aggregate expressions, late-arrival policy | An aggregate relation |
| Data Curation | Entity, resolution strategy, keys, survivorship, merge rules | A surviving record and resolution evidence |
| Data Contracts | Nothing beyond the target contract block | One ODCS contract per published relation and its compliance check |
| Lineage Capture | Nothing; derived from the declaration | Product and field lineage in the estate graph |
| Metadata Capture | Descriptions, ownership, classification | An ODPS product descriptor and catalogue metadata |
| Schema Publish | Versioning policy | Version registration and change classification |
| Data Publish | Publication mode | Atomic publication, a current pointer, an SLA record |
| Checkpoint and Retries | Granularity, retries, backoff | A wrapper around the composition |

## Profiles

A profile declares which patterns are mandatory, optional and forbidden for a product,
and the ordering constraints between them. The engine ships five reference profiles under
`ergasterion/profiles/` and enforces whichever one a product's label admits. An estate may
declare its own. A product naming an unknown profile fails closed.

| Reference profile | The composition it constrains |
|---|---|
| landing | Bring external data in unchanged |
| integration | Conform and curate entities from landed products |
| derivation | Compute on top of integrated products |
| consolidation | Combine two or more products into one |
| serving | Deliver for a named consumer |

Every profile makes Data Contracts mandatory, landing included, because in this engine
the contract is the only pipe.

Layer labels are an estate mapping over profiles. One estate maps Bronze to landing,
Silver to integration, derivation and consolidation, and Gold to serving. Another uses its
own vocabulary. A product declares the label it sits in and, when that label admits more
than one profile, the profile.

## Shapes

A shape is a registered rendering of a product's target. It carries its own declaration
schema for the target block, the relations it renders, any composition constraint it
adds, and a rendering per translator. One shape per product; a second shape is a second
product downstream. A shape is never a layer.

| Shape | Renders | Needs in the target block |
|---|---|---|
| declared | The relations exactly as the composition produces them. The default. | Nothing, or an optional `select` holding one whole select body whose output columns must be exactly the relation schema the composition publishes |
| ods | Normalised relational form: keys, effectivity, cross-reference, audit tail | Entities and keys |
| dimensional | Facts and dimensions, declared grain, slowly changing dimension types, conformed dimensions, and the semantic model of measures and metrics | Grain, dimensions, measures, metrics |
| canonical | One relation per canonical entity over a curated product | An entity list, and an upstream curated product |
| data_vault | Identity, association and version stores, a surviving record and a point-in-time relation, with an evolution ledger and a declared re-baseline | Its stores and its surviving-record rules, and a Data Curation occurrence in the composition |

Registering a shape is a plug-in act under `ergasterion/shapes/<name>/`, never an engine
change. A shape that renders more than one relation says so once, and the contract, the
graph and the translator all read that one answer. A consumer of such a product declares
which relation it reads.

## Contracts: the only pipe

Every product publishes one contract, generated from its declaration and its shape. Its
elements are identity, schema, freshness, quality guarantees, versioning policy, lineage,
access and support. Two serialisations are emitted, both under
`contracts/products/<domain>/<product>/`:

- an **Open Data Contract Standard (ODCS)** document per published relation, the artefact
  of the Data Contracts pattern and the version registered by Schema Publish;
- an **Open Data Product Standard (ODPS)** descriptor per product, the artefact of
  Metadata Capture at product level.

Neither is ever hand-written. Both are schema-validated against their published schemas
and checked for drift on every run.

A consumer names the contract and major version it consumes and declares what it expects
from it. At emission the engine checks that expectation against the producer's current
published contract and fails closed on any incompatibility. Schema evolution follows one
rule: additive nullable fields are minor and non-breaking; removals, renames, type changes
and new required fields are major and breaking. A consumer pinned to a major version is
unaffected by minor changes and blocked by major ones until it re-declares.

## The product graph and generations

Nodes are products. Edges are contract dependencies. The graph is acyclic. The engine
orders it topologically and emits in that order.

Generation is depth, defined structurally. A **first generation** product takes every
source edge from a landing product. A **later generation** product takes at least one from
a non-landing product. A **consolidating** product takes two or more from non-landing
products and combines them by a declared union or merge, with each source conformed onto
the combined schema. A consolidated product proves its coverage with a Data Validation
occurrence whose rules reference its upstream contracts. A union that drops a base fails
its coverage check.

The graph itself is an emitted artefact under `graphs/products/`: nodes, edges,
field-level lineage and the validations each product declares, in both JSON and CSV.
Product-level and field-level lineage come from the declarations at build time. Run-level
lineage comes from the runtime at execution.

## Landing: receiving a delivered batch

The generated pipeline starts from rows that have already crossed a controlled delivery
boundary. The landing profile is that boundary.

Each source table has one Landing Product Contract describing its native schema, its
delivery mode (change events, append-only rows, or complete snapshots), its parsing rules,
its quality rules, its publication policy and its retention. The runtime preserves the
received bytes and manifest, then parses the payload under the declared codec. It evaluates
every rule and publishes accepted rows. Rejected rows enter quarantine with a locator to
the exact bytes that failed. The contract determines the outcome; the runtime applies it.

[`bronze-ingestion.md`](bronze-ingestion.md) is the deep dive on that mechanism.
[`landing-product-v1.md`](../specifications/landing-product-v1.md) is the field-by-field
contract reference. The [landing demonstration](../../demo/landing-ingestion/) runs the
whole sequence on the local reference platform, account-free and network-free.

A landing product does not have to arrive through that runtime. The estate's translator
table decides: one label can give its landing relations to the ingestion runtime, and
another can give them to the SQL translator, which materialises the relation from an
extract the source system delivered. Changing which one owns a pattern is an estate
configuration change with no engine change.

## Translators and adapters

Producing any kind of target and forbidding specifics in declarations are two different
guarantees, and two different mechanisms meet them.

![The split: a neutral declaration through the neutrality gate, a neutral plan, the completeness gate resolving every rule against every declared translator and adapter pair, specifics only in implementation packages](engine-architecture-split.svg)

**Any target** comes from two independent axes, both plug-ins.

| Axis | What it owns |
|---|---|
| Translator, the technology | How an occurrence and a shape are rendered for that technology, the generated tests, and that technology's deployment artefacts |
| Adapter, the platform | Dialect, physical type mapping, identifier rules, physical layout, structural budgets |

A capability is a tuple of pattern or shape, translator and adapter. Routing matches
occurrences to capabilities. Adding a technology is a translator. Adding a platform is an
adapter. Neither touches the engine or any declaration.

The reference translator set is three:

- a **SQL-model translator** that renders the transformation patterns, the shapes, the
  generated tests and the contract compliance checks into a dbt project;
- an **ingestion-runtime translator** that renders Batch Ingestion, landing validation,
  Checkpoint and Retries and Data Publish for landing products against the runtime's
  ports;
- a **publication translator** that renders Data Contracts, Lineage Capture, Metadata
  Capture and Schema Publish into ODCS contracts, ODPS descriptors and the estate graph.

A translator may render private auxiliary relations for a named rule or a staged
computation, under the product's namespace. They are excluded from the product's contract
and registered in the graph as auxiliary lineage.

This repository's estate declares two adapters. **DuckDB is the reference adapter**: it
executes the whole estate locally and is the engine's executable truth. **BigQuery is the
deployment adapter and the final target**: the estate is generated for it and gated for it
offline. Each adapter owns its conventions in `ergasterion/adapters/<adapter>/` and its
budgets and interface boundaries in `declarations/targets/<adapter>.yml`. Another platform
is another adapter package; the engine does not change for it.

**No specifics** comes from where specifics are allowed to live, and two gates that
enforce it. A declaration holds only pattern configuration, neutral types, inline SQL,
contract references and named-rule references. Specifics live only inside translator
packages, adapter packages and named-rule implementations. The **neutrality gate** rejects
any declaration carrying technology syntax, a platform reference, a file path, a URL
scheme, a connection detail or executable code; it never polices which SQL constructs an
expression uses. The **completeness gate** resolves every referenced rule against every
declared translator and adapter pair and fails closed on a gap, naming the rule and the
pair.

The ingestion runtime reaches external services through declared ports for source
connection, raw storage, scratch storage, operational state, landing, remediation,
projection, lifecycle evidence and key services. A runtime binding selects one adapter for
each port and names the relations that environment writes. The local reference binding
uses files for delivery and raw storage, SQLite for operational state, and DuckDB for
landing and projection. A packaged conformance runner checks another platform's adapters
against the runtime contract, including failure recovery and verified backup and restore.

## What keeps it honest

![The offline verification gates: byte-stable re-emission, per-adapter dialect rules and structural budgets, dbt parse for both adapters, the DuckDB build, and the publication drift checks](pipeline_gates.svg)

The engine proves the following deterministically, with no warehouse account:

1. every declaration validates, every inline expression parses with its column references
   resolved, and no declaration contains engine syntax;
2. emission is byte-stable and drift-free, and re-emission of the committed tree changes
   nothing;
3. every artefact parses for both declared adapters and passes each one's dialect and
   structural gates;
4. both worked domains execute on the reference adapter, with every generated test and all
   40 known-answer assertions passing;
5. every product's contract and descriptor is generated, schema-valid and drift-free, and
   every consumer's expectation is compatible with what its producer publishes.

`bash scripts/validate_offline.sh` runs that set. `bash
scripts/validate_engine_architecture.sh` runs the thirteen architecture acceptance checks
over it and prints one line per check.

BigQuery evidence stops at generated artefacts. The gate parses them, checks their dialect
and budgets, and regenerates them byte for byte. No query from this repository has run in
a BigQuery project. A live run needs a runtime binding plus credentials, permissions, and
cost controls supplied by the account owner.

The `ods` shape is proved on a fixture estate only. The declared root estate uses the
`declared`, `canonical`, `dimensional`, and `data_vault` shapes. Data Vault also has a
smaller fixture that isolates its own generation and DuckDB build.

The layer-neutrality gate covers Python modules under `ergasterion/`. Package data,
scripts, tests, and documentation sit outside that scan. Layer labels in `estate.yml` are
estate data by design.

Reconciling a candidate output against an expected output -- frozen inputs, a comparison
policy, first-divergence traceback -- is outside this engine. It belongs to a verification
product that consumes Ergasterion. The engine supplies hooks only: fixture-backed source
relations for local proof, a checkpoint flag that forces materialisation and registers the
relation in the emitted graph, and stable relation names.

## The two worked domains

The repository carries 124 product declarations across two worked domains and their
reference data. Each domain exercises a different set of modelling problems.

The e-commerce domain uses 29 domain products and 4 customer reference products. Its 33
products receive three overlapping customer feeds, resolve customer, product, and order
identity, combine two sales channels, and publish canonical and dimensional outputs. The
18 known-answer assertions under `tests/ecommerce/` check results a person can inspect.

The investment domain uses 75 domain products and 16 investment reference products. Its 91
products receive five source systems, curate five entities in Data Vault shapes, publish
five canonical interfaces mapped to the Open Investment Model, and build review and
decision surfaces for uncertain matches. The 22 assertions under `tests/investment/` cover
those results, including a near-duplicate deal that stays separate and enters review.

The root DuckDB build executes both domains and all 40 assertions. The account-free
demonstration runs that build, then prints three e-commerce results.

## Architectural rules

The design keeps these constraints true:

- Layer labels are configuration keys. Validation, planning, routing, and emission contain
  no branch tied to a label's name.
- Profiles hold composition rules as data. Engine code contains no composition table.
- Each shape owns its declaration fields and rendering. Products use only the shape they
  declare.
- Declarations contain business rules and neutral types. Technology syntax, platform
  references, file paths, and connection details fail validation.
- Every named rule resolves for every translator and adapter pair the estate declares.
- Products consume contracts. Physical table names stay behind the contract boundary.
- One generated project serves all declared adapters. Adapter packages own their dialect
  differences.
- People declare mappings, meaning, ownership, and tolerances. The engine does not infer
  them from names.
- Every product artefact is generated. A missing capability becomes a profile, pattern,
  translator, adapter, or shape change.
- The engine proves the artefacts it generates. A separate verification product compares a
  candidate result with an expected result.

## What this is not

- Ergasterion does not author business meaning. People set composition, mappings,
  resolution keys, survivorship rules, tolerances, and output shapes in readable files.
- The DDL and contract importers create structural starting points. A person fills every
  business decision their input did not state.
- Identity resolution applies declared keys and thresholds. Records without an approved
  identity signal stay separate.
- DuckDB executes. BigQuery is a generation target with offline evidence. Every additional
  platform needs an adapter package and evidence from that platform.
- Adapter conformance proves compatibility with runtime interfaces. The target environment
  still owns production security, resilience, access control, cost control, and operations.

## For engineers

| Location | Responsibility |
|---|---|
| `declarations/products/` | Product declarations: the label, the source contracts, the composition and the published shape |
| `declarations/` | Source schemas and landing configuration |
| `declarations/targets/` | Per-adapter structural budgets and interface boundaries |
| `estate.yml`, `rules/` | Estate policy: labels, adapters, the translator table, expression mode, named-rule signatures |
| `ergasterion/framework/` | The engine: declaration validation, plan, routing, shapes, contracts, graph, the rule catalogue |
| `ergasterion/profiles/`, `ergasterion/schemas/patterns/` | The reference profiles and the fifteen pattern schemas, as data |
| `ergasterion/shapes/` | The registered shape plug-ins |
| `ergasterion/translators/` | The SQL-model, ingestion-runtime and publication translators |
| `ergasterion/adapters/` | Per-platform conventions: dialect rules, type mapping, identifier rules |
| `ergasterion/ingestion/` | The landing runtime, its ports, its local adapters and its conformance runner |
| `models/products/`, `contracts/products/`, `graphs/products/`, `manifests/products/` | Generated output, one tree per product |
| `macros/` | Named-rule implementations and the adapter-dispatch layer |
| `tests/` | Known-answer assertions, engine tests and the acceptance run |

The root [README.md](../../README.md) is the short introduction and the command reference.
[`RUNBOOK.md`](../../RUNBOOK.md) is the operator sequence. [`DEMO.md`](../../DEMO.md) walks
source onboarding through both importers.
[`bronze-ingestion.md`](bronze-ingestion.md) is the deep dive on the landing boundary.
[`engine-architecture.md`](engine-architecture.md) is the design record this guide
describes the implementation of, including its open questions.
[`../migration/0.5-to-0.6.md`](../migration/0.5-to-0.6.md) is the declaration migration
guide for an estate moving off the 0.5 line.
