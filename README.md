# Ergasterion

*Ergasterion (ἐργαστήριον) is the ancient Greek word for a workshop, a place where
things are made. This workshop makes data pipelines.*

## Why Ergasterion exists

Data pipeline code is usually maintained by hand. Every new feed requires another set
of ingestion steps, staging models, field mappings, history logic, tests, contracts, and
operational controls. When a source adds or removes a field, changes a type, or renames a
column, developers must trace that change through the pipeline and edit the affected
code. This work repeats for every feed and every change.

Ergasterion automates the production and maintenance of that code. You describe the
target warehouse schema and each source in version-controlled metadata and configuration,
then map the source fields into the warehouse model. Ergasterion reads those definitions
and generates the pipeline code. It also generates its tests, contracts, product
metadata, domain maps, and the interfaces needed to receive source data. Onboarding a
feed or accepting a field change becomes a configuration change followed by regeneration.

The mapping matters because source systems rarely match the schema or delivery behaviour
of the warehouse they feed. A customer may arrive from a storefront, a marketplace, and
a CRM under different keys and column names. The configuration records how each source
field maps into the warehouse model. The generator applies those rules consistently to
casting, validation, history, and identity resolution.

The same definitions run on DuckDB, Snowflake, and BigQuery. Platform-specific connectors
sit behind stable interfaces. Another warehouse or database can be added without
rewriting the warehouse model or its source mappings.

The [Ergasterion architecture guide](https://github.com/antikas/ergasterion/blob/master/docs/architecture/README.md)
follows a source from its declaration through the generated warehouse layers and
verification.

## The warehouse model comes first

An Ergasterion project is called an **estate**. Its warehouse model describes the data
the warehouse needs. Source declarations describe how each feed supplies it.

The domain file, `domains/<name>.yml`, defines the warehouse model. It names the entities,
their keys and attributes, the relationships between them, and the history the warehouse
must retain. It also defines the rules for matching records from different sources,
choosing which source wins for each attribute, publishing contracts, and describing the
domain as a graph.

Each source has its own `declarations/<source>.yml`. A declaration describes the source
tables and columns, normalises their names and types, and maps them into the entities in
the domain file. The `projection` block handles the source shape. The `vault_entities`
block states where those fields belong in the warehouse model. These mappings remain
reviewable configuration and drive the generation of pipeline code.

Ergasterion generates the integrated, historical source-facing layers. The clean
canonical tables, dimensions, facts, and measure definitions remain explicit estate
design. Reference models and AI assistance can be used to develop them.

### Start from schemas you already have

A new estate does not require every definition to be typed from a blank file.

- A warehouse model can start from existing DDL or an industry or enterprise reference
  model, including the Open Investment Model (OpenIM). It can also be designed directly
  or drafted with AI assistance. When DDL is imported, primary and foreign keys provide
  the starting entity and relationship structure.
- Source-system DDL can seed a source declaration with its columns, types, keys, and
  mechanical tests.
- A supplier's Open Data Contract Standard (ODCS) contract can seed the same source
  declaration from a vendor-neutral schema.
- The generated files remain normal editable YAML. Semantic mappings, matching rules,
  survivorship decisions, and contract details can be developed directly or with AI
  assistance.

DDL and contracts describe structure. They do not fully define what a field means in
your warehouse or which source should win when values disagree. Ergasterion keeps those
decisions explicit in the configuration so they can be reviewed and changed.

## What the estate produces

One set of domain definitions and source mappings produces the working pipeline and its
public interfaces:

- **Warehouse models** under `models/`: typed staging, identity resolution, append-only
  history, golden records, and the project-defined served layer above them.
- **Data contracts** under `contracts/`: one ODCS contract per served table and one Open
  Data Product Standard (ODPS, Bitol) descriptor per domain.
- **Domain maps** under `graphs/`: entities and their relationships in relational, graph,
  and machine-readable forms.
- **Ingestion interfaces and evidence**: preserved source payloads, typed source-native
  records, publication state, quarantine records, lineage, and operational status
  (Bronze).

Identical declarations generate identical bytes. If a generated file is changed directly,
the next check reports the difference and stops.

## Following one customer through the factory

The e-commerce example contains three invented systems. CARTIVO is a storefront, MERCARO
is a marketplace, and RELATIO is a CRM. All three contain records for Ava Thompson, with
different source keys and slightly different contact details.

**Received.** A delivered file first passes through a controlled ingestion layer
(Bronze). Ergasterion preserves the payload and its manifest exactly as received, parses
the rows under a written contract, and records which rows passed or failed each quality
rule. Accepted rows become available to the generated pipeline. Rejected rows carry a
locator back to the raw bytes that failed.

**Mapped.** Each source declaration casts its native columns into stable types and names.
It then maps the customer fields into the customer entity defined by the warehouse model.
The mapping is visible in the declaration and can be reviewed before generation.

**Recognised.** The domain rules identify the three records as one customer. They use a
shared loyalty identifier first and a normalised email where no loyalty identifier is
available. A record with neither signal remains separate.

**Remembered and chosen.** Every source's version of Ava's details is retained with its
source and date. The estate defines the survivorship rules. In this example the CRM
supplies contact details, while the storefront supplies marketing preferences and consent.
The chosen record retains the source used for every field.

**Served and published.** The integrated record feeds the customer tables designed for
analysis. Ergasterion generates contracts for those tables and includes the customer
entity in the domain map. The same definitions also generate the history and tests that
support the served result.

The [demo guide](https://github.com/antikas/ergasterion/blob/master/demo/README.md) runs
the customer and investment warehouse results on invented data. The separate Bronze
demonstration covers the received-file boundary.

## DuckDB, Snowflake, and BigQuery

The estate's warehouse model and source mappings are independent of the database that
executes them. Ergasterion provides working targets for DuckDB, Snowflake, and BigQuery.
The adapter architecture separates platform work at two boundaries.

The engine resolves a platform-neutral execution plan, then produces the models and tests
that dbt runs. Adapter-dispatched macros isolate SQL differences, while target
declarations record database limits and materialisation constraints. The same estate can
be built with each supported dbt adapter.

The Bronze runtime reaches external systems through nine ports: source connection, raw
storage, scratch storage, operational state, landing, remediation, projection, lifecycle
evidence, and key services. A runtime binding selects one adapter for each port. The
local reference binding uses files, SQLite, and DuckDB.

Another platform supplies adapters for its own scheduler, state database, storage,
warehouse, and policy services. The packaged conformance runner checks those
implementations against the runtime contract, including failure recovery and
backup/restore behaviour. Adding a target is a defined adapter task; the domain schema,
source mappings, product contracts, and business rules remain unchanged.

## Receiving delivered data with Bronze

The generated warehouse pipeline starts from rows that have already crossed a controlled
delivery boundary. Bronze provides that boundary.

Each source table has one Bronze Product Contract. It describes the native schema,
delivery mode, parsing rules, quality rules, publication policy, and retention
requirements. Delivery modes cover change events, append-only rows, and complete
snapshots.

The runtime applies the contract to every delivery. It preserves the received bytes,
checks the manifest, parses the payload, evaluates each rule, and publishes or
quarantines the result. The contract decides the policy in advance. Operators inspect
evidence and handle exceptions through the command surface.

The [Bronze architecture guide](https://github.com/antikas/ergasterion/blob/master/docs/architecture/bronze-ingestion.md)
explains the runtime, its five product interfaces, and its adapter ports. The
[Bronze demonstration](https://github.com/antikas/ergasterion/tree/master/demo/bronze-ingestion)
runs locally without an external account or network call.

## Contracts, products, and domain maps

ODCS contracts describe the tables Ergasterion publishes. Each contract records the
schema, identifiers, quality checks, ownership details, and field-level source
attribution. Ergasterion generates the contracts from the same model and tests that
produce the tables.

ODPS describes the data product that a group of tables forms. Ergasterion generates one
descriptor per domain. Its output ports reference the exact ODCS contracts for served
tables, while its management ports identify the operational and documentation surfaces.

A source may also arrive with an ODCS contract. The `import-odcs` command copies its
mechanical schema facts into a new source declaration. The remaining semantic mappings
stay visible in editable configuration and can be developed directly or with AI
assistance.

### Optional reference-model alignment

An estate can record how its warehouse model aligns with an external industry or
enterprise reference model. This alignment is optional and remains separate from the
source-to-warehouse mappings used by every estate.

The included investment example records attribute lineage to the Open Investment Model
(OpenIM), and the current validation hook checks that model for spelling and schema
drift. The Banking Industry Architecture Network (BIAN), the Financial Industry Business
Ontology (FIBO), and internal canonical models occupy the same architectural role when a
validator is available for their format. A reference model can provide the starting
structure and vocabulary. The estate configuration records the warehouse model actually
used and its source mappings.

### Typed domain map

The domain file also carries a relationship vocabulary. It names each relationship,
states its direction and cardinality, and binds it to the entities and keys that realise
it. Ergasterion emits both a type-level graph and a binding to the generated relational
estate.

The [domain-map guide](https://github.com/antikas/ergasterion/blob/master/docs/architecture/ontology-map-lane.md)
describes the complete relation format and generated graph family.

## Verification

The repository checks the generated estate at several boundaries:

- Re-emission must reproduce every generated file byte for byte.
- Dialect checks reject SQL that is incompatible with a selected warehouse target.
- dbt parses the project for DuckDB, Snowflake, and BigQuery.
- The complete invented estate builds in DuckDB with known-answer business assertions.
- Structural checks enforce each target's declared limits.
- Contract, product, graph, scaffold, package, and wheel checks run from the public tree.
- Adapter conformance covers state, storage, publication, failure recovery, protection,
  and verified backup/restore behaviour.

The local validator needs no warehouse account. Live Snowflake validation is a separate
authorised run against a bounded development schema.

## Worked domains

The repository contains two complete domains built by the same engine.

### E-commerce customer view

The e-commerce estate models customers, products, orders, and changing customer
segments. Three invented feeds exercise source mapping, customer identity resolution,
attribute survivorship, dated classification, order reconciliation, and revenue
measures. It uses no external reference model.

The worked result checks that one customer resolves across three sources, order lines
reconcile to their header, and historical orders retain the customer segment that was
true when each order was placed.

### Investment data

The investment estate models funds, management firms, portfolio companies, legal
vehicles, cash flows, valuations, and deal opportunities. Its sources use different
identifiers and names for the same funds. The example retains every source version,
resolves identities, applies survivorship rules, and preserves changes in classifications
and management firms over time.

The deal path records dated stages and review decisions. Uncertain record matches wait for
review, and an accepted deal can link to the fund it became. The optional OpenIM
alignment belongs to this domain alone.

The [source-description guide](https://github.com/antikas/ergasterion/blob/master/DEMO.md)
walks through both domains and the importers.

## Boundaries

- Generation covers the source-facing integration layers. The clean canonical tables,
  dimensions, facts, and measures remain explicit estate design. Reference models and AI
  assistance can be used during their development.
- DDL and ODCS importers create editable starting files. They leave semantic mappings,
  identity rules, survivorship, and relationship vocabulary as explicit configuration
  for further development and review.
- Record matching is deliberately cautious. An uncertain pair waits for review and
  remains unmerged until the review decision is recorded.
- External reference models are optional. Each format needs validation support before
  Ergasterion can check an alignment against it.
- DuckDB, Snowflake, and BigQuery are included. Another platform needs implementations
  of the relevant translator and runtime adapter contracts.
- Adapter conformance establishes compatibility with Ergasterion's interfaces.
  Production security, resilience, access control, and operation remain responsibilities
  of the target environment.

## For engineers

### Requirements and installation

The engine requires Python 3.11 or newer. Git and Bash are needed for the repository
checks and demonstrations.

Install the engine with the complete local DuckDB path:

```bash
python -m venv .venv
source .venv/bin/activate          # Windows PowerShell: .venv\Scripts\Activate.ps1
pip install "ergasterion-factory[local-ingestion]"
```

The base package is enough for generation and import commands:

```bash
pip install ergasterion-factory
```

Install all three warehouse adapters when one environment needs every target:

```bash
pip install "ergasterion-factory[all]"
```

The `duckdb` extra remains an alias of `local-ingestion` for existing installations.
Snowflake and BigQuery can also be installed separately through the `snowflake` and
`bigquery` extras.

For an editable source checkout:

```bash
git clone https://github.com/antikas/ergasterion
cd ergasterion
pip install -e ".[all]"
```

Both command forms use the same entry point:

```bash
ergasterion --help
python -m ergasterion --help
```

### Create an estate

```bash
ergasterion init my-estate
cd my-estate
```

The new estate contains empty domain and source directories, a dbt project, target
profiles, the shared macros, a local runtime binding, and a generated
`GETTING-STARTED.md`.

### Seed the model and sources

Seed a warehouse model from DDL that carries primary and foreign keys:

```bash
ergasterion import-ddl warehouse-model.sql --mode model --domain retail
```

Seed a source declaration from its own DDL:

```bash
ergasterion import-ddl crm-source.sql --mode feed --source crm
```

Seed a source declaration from an ODCS v3 contract:

```bash
ergasterion import-odcs supplier-contract.yml --source supplier
```

Resolve the marked semantic decisions in the generated YAML before generation. AI
assistance can be used for this work. The importers refuse to overwrite an existing
destination unless `--force` is supplied.

### Generate and build

```bash
ergasterion emit --estate-root .
dbt deps --profiles-dir profiles
dbt build --profiles-dir profiles --target duckdb
```

Select `snowflake` or `bigquery` as the target to build on those platforms. Their account
settings and credentials stay in the target environment.

### Run the worked repository

The account-free demonstration builds both worked domains in DuckDB and writes its
results beneath `demo/offline-runs/`:

```bash
bash demo/run_offline_demo.sh
```

The Bronze demonstration exercises received batches, quarantine, publication, recovery,
and backup/restore through the local reference adapters:

```bash
bash demo/bronze-ingestion/run_bronze_demo.sh
```

The live Snowflake demonstration creates a fresh schema in an authorised account:

```bash
bash demo/run_clean_demo.sh
```

Review the account, role, database, schema, and warehouse before running the live path.
The [runbook](https://github.com/antikas/ergasterion/blob/master/RUNBOOK.md) contains the
complete setup and operating sequence.

### Command reference

| Command | Purpose |
|---|---|
| `ergasterion init <dir>` | Create an empty estate. |
| `ergasterion import-ddl` | Seed a domain model or source declaration from DDL. |
| `ergasterion import-odcs` | Seed a source declaration from an ODCS contract. |
| `ergasterion emit` | Generate the source-facing warehouse pipeline. |
| `ergasterion contracts` | Generate ODCS contracts for served tables. |
| `ergasterion odps` | Generate one ODPS descriptor per domain. |
| `ergasterion graph` | Generate the typed domain map. |
| `ergasterion lint` | Check SQL portability for a warehouse target. |
| `ergasterion structure` | Check the estate against target limits. |
| `ergasterion plan` | Resolve and inspect the Bronze execution plan. |
| `ergasterion contract` | Register and activate a Bronze Product Contract. |
| `ergasterion deployment` | Register and activate a runtime binding. |
| `ergasterion ingest` | Submit a delivery or process work that is due. |
| `ergasterion reconcile` | Resume or rebuild a blocked projection. |
| `ergasterion status` | Read the operational state of a Bronze product. |
| `ergasterion inspect` | Read delivery and lineage evidence. |
| `ergasterion quarantine` | List, revalidate, or release quarantined records. |
| `ergasterion local-backup` | Back up or restore the local reference runtime. |

Run `ergasterion <command> --help` for the exact options.

### Repository layout

| Path | Contents |
|---|---|
| `ergasterion/` | Engine, framework, translators, runtime ports, templates, and validators. |
| `domains/` | Warehouse domain models and relationship vocabularies. |
| `declarations/` | Source schemas, projections, mappings, and delivery contracts. |
| `models/` | Generated source-facing layers and project-defined served models. |
| `macros/` | Shared dbt and cross-database adapter logic. |
| `contracts/` | Generated ODCS contracts and ODPS descriptors. |
| `graphs/` | Generated domain-map artefacts. |
| `demo/` | Account-free and live demonstrations. |
| `streamlit/` | Review screen for uncertain matches and deal decisions. |
| `tests/` | Known-answer, structural, contract, package, and conformance checks. |

All repository examples use invented data. The e-commerce names, people, addresses, and
brands are synthetic, and its email addresses use the reserved `example.com` domain.
Ergasterion is released under the MIT licence.
