# Ergasterion architecture

Adding a source to a warehouse usually creates another set of pipeline code. The code
must receive the data, map its fields, retain its history, apply data rules, build useful
tables, and publish tests and contracts. Source changes then require corresponding code
changes throughout that path.

Ergasterion moves this repeated work into code generation. An Ergasterion project records
the target warehouse model, the sources that feed it, the mappings between them, and the
runtime configuration. Those definitions drive the production of the pipeline and its
published interfaces.

The warehouse model and its mappings can start from an existing database schema (DDL), a
reference model, or a new design. AI assistance can help design the model, complete the
mappings, and develop the served or gold layer. The resulting definitions and code remain
version controlled, reviewable, and subject to the same validation as the rest of the
estate.

![Several source systems feeding separate pipelines before a warehouse](problem.svg)

*Separate source pipelines require corresponding code changes whenever a source changes.*

## Architecture overview

![Source declarations entering the generator and flowing through staging, identity resolution, historical storage, golden records, canonical models and marts](pipeline.svg)

The diagram shows the warehouse data flow. It uses the investment example because that
estate has several sources for the same entities. The e-commerce example uses the same
components with different entity names and business rules.

A delivered batch first crosses the Bronze ingestion boundary. Bronze preserves the
received payload, applies the delivery contract, and publishes accepted records through a
stable interface. Source declarations then map those records into the warehouse model.
The generator produces the warehouse code, tests, contracts, product metadata, and domain
maps from the estate definitions.

Platform code sits behind two adapter boundaries. The warehouse generation
boundary handles SQL and dbt target differences. The Bronze runtime boundary connects the
same delivery contract to the storage, state, landing, projection, scheduling, and policy
services used by a deployment.

## Inputs, generated outputs and ownership

An Ergasterion project is called an **estate**. The estate keeps the design of the
warehouse separate from the engine that produces and runs it.

| Part | What it contains | How it is produced |
|---|---|---|
| Warehouse model | Target entities, keys, attributes, relationships, matching rules, survivorship rules, and served products | Created from DDL, a reference model, or a design for the estate |
| Source declarations | Native source fields, data types, landing details, tests, and mappings into the warehouse model | Created from DDL, an Open Data Contract Standard (ODCS) contract, or the source specification |
| Runtime and target configuration | Warehouse target, adapter bindings, physical limits, materialisation choices, and protection settings | Selected for each deployment environment |
| Generated integration layers | Typed staging, optional identity resolution, historical storage, golden records, tests, and operational interfaces | Produced deterministically from the estate definitions |
| Served or gold layer | Canonical models, dimensions, facts, calculated fields, semantic models, and measures | Produced from the warehouse design for that estate |
| Published metadata | Data contracts, product descriptors, domain maps, runtime manifests, and lineage | Generated from the same estate definitions and built models |

The engine treats entity names, field names, relationships, and rules as estate data. A
customer estate and an investment estate therefore use the same engine while carrying
different warehouse models and source mappings.

The structured definitions are the repeatable input to generation. They can be imported
from an existing model or developed for the estate. Once a design decision is recorded,
generation applies it consistently whenever a source is added or changed.

## Flow from source to warehouse

The e-commerce example shows the complete journey. CARTIVO is a storefront, MERCARO is a
marketplace, and RELATIO is a customer relationship system. Each source contains a record
for the same customer under a different source key.

### Receive and validate the delivery

Each source table has a Bronze Product Contract. The contract records the native schema,
delivery mode, parsing rules, quality rules, publication policy, and retention settings.
The team responsible for the data can read and change those decisions.

The runtime preserves the payload and its manifest exactly as received. It parses the
payload under the declared codec, checks each quality rule, and records the result. A
passing delivery is published for downstream use. A failing delivery is quarantined with
a locator back to the source bytes.

[`bronze-ingestion.md`](bronze-ingestion.md) describes this boundary in detail.
[`bronze-product-v1.md`](../specifications/bronze-product-v1.md) is the contract reference.
The [Bronze demonstration](../../demo/bronze-ingestion/) runs on the local reference
platform using local files, SQLite, and DuckDB.

### Map source fields into the warehouse model

A source declaration describes the fields supplied by one feed and maps each field into
the target warehouse model. It can rename fields, cast values into stable types, and state
which entity and attribute each value supplies.

The generator reads those mappings and produces a typed staging model for each source.
Every later layer uses the warehouse vocabulary, while the declaration retains the link
back to the source field.

![Source declarations entering the generator, which emits typed staging models](pipeline_sources.svg)

### Resolve records that describe the same entity

Identity resolution is used when several sources describe the same customer, fund, or
other entity. The estate defines the available matching signals and their order. Strong
identifiers can resolve a pair directly. Weaker evidence can feed a probabilistic score
when the estate enables that path.

Pairs below the approved threshold enter a review queue. A reviewer can accept or reject
the match, and the decision is retained for later runs. An entity supplied by one source
passes directly into the historical layer.

![Deterministic and probabilistic identity resolution leading to a review queue](pipeline_entity_resolution.svg)

### Retain source history

Every accepted source version is dated and stored in the raw vault. Hubs hold stable
entity identities, links hold relationships, and satellites hold descriptive history.
Bridge models connect source records to their resolved entity keys. Corrections and
disagreements remain visible because new versions are appended.

This history retains the source and effective date for every value. A downstream record
can therefore be traced to the source version from which it was produced.

### Apply survivorship and produce golden records

The estate defines which source should supply each attribute when several sources provide
a value. One rule may prefer the customer relationship system for contact details, while
another prefers the storefront for marketing consent. These survivorship rules are
readable and changeable estate configuration.

The business vault applies those rules to the retained history and produces a golden
record. The golden record stores the selected value, its source, and the date on which it
became effective.

### Produce the served or gold layer

The served layer reshapes integrated records for business use. It can contain canonical
models, dimensions, facts, calculated fields, semantic models, and measures. Canonical
models expose consistent domain entities. Dimensions, facts, and measures organise them
for analysis. Some platforms call this the gold layer.

Ergasterion can produce this layer as part of the estate generation process. Its design is
specific to the warehouse and the questions it must answer. Existing DDL, industry
models, direct design, and design with AI assistance can all provide that intent. Once
captured in the estate's generation inputs, the models can be produced and checked with
the rest of the pipeline.

A golden record and a gold layer serve different purposes. The golden record selects the
current value for an entity after matching and survivorship. The gold layer organises
those values into the dimensions, facts, measures, and other outputs used by consumers.

![Historical source records producing golden records, canonical models and marts](pipeline_vault.svg)

### Publish contracts, products, maps and runtime metadata

The publication boundary is generated from the same model and mappings:

- An Open Data Contract Standard (ODCS) contract describes each served table, including
  its schema, keys, tests, ownership, and field lineage.
- An Open Data Product Standard (ODPS, Bitol) descriptor groups the served tables into a
  product and records its output and management ports.
- A typed domain map records entities and relationships in relational and graph forms.
- Runtime manifests and operational evidence record how a Bronze product is bound and
  what happened to each delivery.

The verification process regenerates these artefacts and compares them with the committed
versions. A direct edit is reported as drift.

## Platform adapters

The warehouse model and source mappings describe the data. Translators and runtime
adapters contain the platform implementation. This separation allows one estate
design to run on different combinations of warehouse, storage, state, and scheduling
services.

### Warehouse generation and SQL targets

The warehouse generator emits models and tests for dbt, the tool that builds them on the
target database. dbt dispatch macros isolate SQL differences such as safe casts, regular
expressions, date arithmetic, hashing, arrays, and object construction. Target
declarations record physical limits and materialisation settings.

DuckDB, Snowflake, and BigQuery are implemented targets. The same estate definitions can
be parsed for all three. The local reference build executes the complete worked estate on
DuckDB. Snowflake and BigQuery use their corresponding dbt adapters and credentials.

A new warehouse target supplies the required SQL dispatches, target limits, and dbt
configuration. Its implementation must pass the dialect, parse, structure, and build
checks that apply to that platform.

### Bronze translators and runtime ports

The Bronze framework resolves a product contract and runtime binding into an execution
plan that is independent of any platform. Translators claim the stages they implement and
produce the target artefacts and runtime manifest. Routing checks that each stage has one
execution owner and that handoffs use compatible schemas.

The runtime reaches external services through declared ports for source connection, raw
storage, scratch storage, operational state, landing, remediation, projection, lifecycle
evidence, and key services. A runtime binding selects one adapter for each port and names
the relations used by that environment.

The local reference binding uses files for delivery and raw storage, SQLite for
operational state, and DuckDB for landing and projection. Another platform supplies
adapters for its own services. The contract and execution plan remain unchanged.

Each adapter declares its operations, supported delivery modes, codecs, limits, safety
guarantees, and protection capabilities. The conformance runner checks those declarations
against the implementation, including recovery and verified backup and restore.

## Verification

![Verification gates for regeneration, SQL dialects, dbt targets and the local estate build](pipeline_gates.svg)

Verification checks the declarations, generated code, built models, and published
metadata at several boundaries:

- Regeneration must reproduce committed generated files byte for byte.
- Dialect checks reject SQL that is incompatible with a selected target.
- dbt parses the estate for DuckDB, Snowflake, and BigQuery.
- A full DuckDB build executes the worked data and its business assertions.
- Contract, product, graph, scaffold, package, and adapter checks cover the other
  published and runtime interfaces.

[`scripts/validate_offline.sh`](../../scripts/validate_offline.sh) runs the local checks.
It needs Git, Bash, Python, dbt, and the pinned dbt packages. Snowflake and BigQuery
credentials are used only by separate authorised runs against bounded development
environments.

## Worked examples

The repository contains an e-commerce estate and an investment estate. Both use the same
engine and adapter interfaces.

The e-commerce estate models customers, products, orders, and customer segments. Its
three sources exercise source mapping, customer identity resolution, survivorship,
historical classification, order reconciliation, and revenue measures.

The investment estate models funds, management firms, portfolio companies, legal
vehicles, cash flows, valuations, and deal opportunities. Its sources exercise the same
pipeline mechanisms with a different warehouse model and different business rules.

The root [README.md](../../README.md#worked-domains) describes the results produced by
both estates. [DEMO.md](../../DEMO.md) explains their source declarations and mappings.

## Optional alignment with reference models

An estate may align its warehouse model with an industry, enterprise, or public reference
model. The reference model can supply a starting schema and vocabulary. The estate still
records the model it uses and the mappings from each source.

The investment example records attribute lineage to the
[Open Investment Model (OpenIM)](https://openinvestmentmodel.org). A validation hook can
check those references when an OpenIM checkout is supplied. BIAN, FIBO, and internal
canonical models occupy the same architectural position when a validator exists for
their format.

Alignment with a reference model is optional. The mappings from each source into the
warehouse and the generated pipeline work with an estate's own warehouse model.

The domain file also defines a relationship vocabulary. Ergasterion uses it to produce a
typed domain map and a binding to the physical warehouse tables. The
[domain map guide](ontology-map-lane.md) describes that projection.

## Boundaries

- Business meaning enters through the warehouse model, source mappings, matching rules,
  survivorship rules, and served layer design. AI assistance can help develop each of
  these inputs. The reviewed estate files record the decisions generation applies.
- DDL and ODCS importers provide structural starting points. The estate completes the
  business meaning, source mappings, ownership, matching rules, and publication details.
- Automated identity resolution follows the signals and thresholds approved for the
  estate. Uncertain pairs wait for a recorded review decision.
- DuckDB, Snowflake, and BigQuery are the current warehouse targets. Another target needs
  the required SQL implementation, target configuration, and validation evidence.
- Adapter conformance establishes compatibility with Ergasterion's interfaces.
  Production suitability also depends on the target environment's security, resilience,
  access control, operating model, and recovery evidence.

## For engineers

| Location | Responsibility |
|---|---|
| `domains/` | Warehouse models, matching rules, survivorship rules, product definitions, and relationship vocabularies |
| `declarations/` | Source schemas, landing configuration, and mappings into the warehouse model |
| `ergasterion/emit.py` and `ergasterion/templates/` | Deterministic warehouse model generation |
| `ergasterion/framework/` | Typed contracts, execution plans, routing, bindings, and translator conformance |
| `ergasterion/translators/` | Translation from execution plans into target artefacts |
| `ergasterion/ingestion/` | Bronze runtime, ports, local adapters, operational evidence, and adapter conformance |
| `models/` | Generated integration models and the estate's canonical, mart, calculated field, and semantic outputs |
| `contracts/`, `products/`, and `graphs/` | Generated publication interfaces |

The root [README.md](../../README.md#for-engineers) covers installation, commands, and the
complete repository layout. [RUNBOOK.md](../../RUNBOOK.md) covers local operation, Bronze
commands, and live warehouse validation. [`bronze-ingestion.md`](bronze-ingestion.md) and
[`ontology-map-lane.md`](ontology-map-lane.md) provide the detailed architecture for those
two areas.
