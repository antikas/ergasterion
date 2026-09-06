# Ergasterion

A warehouse that reads many source systems usually grows one pipeline per feed. Each
pipeline receives data, maps fields, keeps history, applies rules, builds tables, and
publishes tests and contracts. A source change then requires a code change in its own
pipeline.

Ergasterion moves those decisions into version-controlled YAML files. People declare what
the data means, how sources combine, which value wins a disagreement, and what each output
promises. The engine turns those declarations into the pipeline and its evidence. The same
inputs create the same files byte for byte.

The name *Ergasterion* comes from the ancient Greek word for a workshop. This workshop
makes governed data products.

The complete architecture is the
[Ergasterion architecture guide](https://github.com/antikas/ergasterion/blob/master/docs/architecture/README.md).
This page is the short introduction and command reference.

## What you can watch it do

The account-free demonstration runs the shipped system from declarations to queryable
results:

1. It regenerates all 124 product declarations and reports any drift from the committed
   output.
2. It builds both worked domains on DuckDB, including every generated test and all 40
   known-answer assertions.
3. It prints three e-commerce results that a person can check: segment revenue, customer
   resolution, and order reconciliation.

Run `bash demo/run_offline_demo.sh` from a prepared source checkout. A separate
[landing demonstration](https://github.com/antikas/ergasterion/blob/master/demo/landing-ingestion/README.md)
shows a clean publication, a rejected snapshot, and a verified backup and restore.

## Follow one customer through the workshop

The first worked domain has three invented source systems. CARTIVO is a storefront,
MERCARO is a marketplace, and RELATIO is a customer relationship system. All three hold a
record for Ava Thompson. Their keys and contact details differ.

**Receive the records.** Each delivered batch crosses a controlled boundary. The engine
preserves the payload, parses each row under a written contract, and records the result of
every quality rule. Accepted rows publish. Rejected rows keep a locator to the raw bytes
that failed. This first product is a landing product.

**Put them on one schema.** One product per source casts native columns into the names and
types the domain uses. People can read and change every mapping in the declaration before
the engine generates any code. This is a derivation product.

**Decide which records describe Ava.** The declaration says that a shared loyalty ID is
the first identity key. A normalised email is the fallback. A record with neither signal
stays separate. The same declaration says RELATIO supplies contact details when sources
disagree. This is an integration product.

**Join Ava to orders and products.** Other declarations combine order lines from both
sales channels, then join them to the agreed customer, order, and product records. Coverage
checks catch a source that disappears from a union. These are consolidation products.

**Publish for a consumer.** The final declarations create stable customer interfaces, an
order summary, and a dimensional model with measures and metrics. Each output has a data
contract. Products can read one another only through those contracts. These are serving
products.

## What you declare

An Ergasterion project is an **estate**: one governed set of inputs, products, rules,
execution targets, and outputs. People author and review three kinds of document.

**Product declarations**, one per thing the estate publishes, at
`declarations/products/<domain>/<name>.yml`. Each file states what the product reads, the
ordered operations it applies, and the form it publishes. The engine calls these inputs
contracts, the ordered operations a composition of patterns, and the published form a
shape. The file contains no platform or tool setting.

**Source declarations**, one per source system, at `declarations/<source>.yml`. These
describe the tables and columns a feed delivers and how a delivered batch reaches the
estate. Two importers seed them from what a supplier already has: an Open Data Contract
Standard contract, or the source system's own `CREATE TABLE` statements.

**Estate configuration**, in `estate.yml` and `declarations/targets/`. This is where an
organisation records its own policy. It names the labels used to group products, the
allowed operation sequences, and the components that generate each operation. It also
sets the inline SQL policy and each database target's structural limits.

The people who own the estate set these rules. They can read and change every declaration
and policy file. The engine applies what those files say and rejects incomplete or
inconsistent combinations.

## What the engine produces

One run of `ergasterion emit-products` writes, deterministically, from those declarations:

- **Models** under `models/products/`: a buildable dbt project, one tree per product, with
  schema documentation and every generated test.
- **Contracts** under `contracts/products/`: one Open Data Contract Standard document per
  published relation and one Open Data Product Standard descriptor per product. Both are
  checked against their published schemas.
- **The estate graph** under `graphs/products/`: products, their contract edges, and
  field-level lineage, in JSON and CSV.
- **Runtime manifests** under `manifests/products/`: what each product publishes and what
  it needs at run time.

Identical declarations produce identical bytes. Every generated file carries a marker
saying so, and `--check` regenerates without writing and reports any difference.

## Changing a running estate

Most source changes are one ordinary workflow. Add the field to the source declaration,
map it in every product that feeds the entity, run `ergasterion emit-products`, review the
generated change, and deploy it. Existing history keeps its identity.

Products with the `data_vault` shape need one controlled maintenance operation. A
satellite stores fingerprints built from an exact set of columns, called its hashdiff
basis. That basis is frozen after the first stored version.

Changing it takes three steps. `ergasterion vault-rebaseline --stage` records the pending
basis and stops emission while two definitions exist. `--promote` adopts the pending basis
and advances its version. The next build stores new fingerprints under that version and
keeps old fingerprints under their original version. `--abort` abandons a staged change.

The [runbook](https://github.com/antikas/ergasterion/blob/master/RUNBOOK.md) has the
operator steps.

## DuckDB and BigQuery

The estate's declarations are independent of the database that executes them. Two adapters
are declared, and the difference between them is the difference between what is executed
and what is checked.

**DuckDB executes.** It is the reference adapter: the whole estate is generated for it,
built on it, and asserted on it with known-answer business assertions, locally, with no
cloud account.

**BigQuery is a generation target with offline evidence.** Its project is generated,
parsed by dbt, dialect-checked, budget-checked and regenerated deterministically. It is
not executed here, and no query has run in a BigQuery project from this repository.
Running it needs a runtime binding, and the account owner supplies the credentials,
permissions and cost controls in the target environment.

Another platform is another adapter package: dialect rules, physical type mapping,
identifier rules and structural budgets. The declarations, the products, the contracts and
the business rules do not change for it.

## What keeps it honest

Checks decide whether the output can be trusted. The repository runs them without a
warehouse account:

- Re-emission reproduces every generated file byte for byte.
- Dialect checks reject SQL incompatible with a declared adapter.
- dbt parses the project for DuckDB and for BigQuery.
- Structural checks enforce each adapter's declared budgets and interface boundaries.
- Both worked domains build in DuckDB, with generated tests and 40 known-answer assertions
  passing.
- Contract, descriptor, graph, scaffold, package and wheel checks cover the published and
  packaged surfaces.
- Adapter conformance covers the landing runtime's state, storage, publication, failure
  recovery, protection and verified backup and restore behaviour.

`bash scripts/validate_offline.sh` runs that set. `bash
scripts/validate_engine_architecture.sh` runs the thirteen architecture acceptance checks
and prints one line per check.

## What this is not

- Ergasterion does not decide business meaning. People record mappings, ownership,
  tolerances, resolution keys, and source priority in declarations they can review and
  change.
- The importers create editable starting files from DDL or an existing contract. A person
  must fill every business decision that the input did not contain.
- Identity resolution applies declared keys and thresholds. Records without an approved
  identity signal stay separate.
- DuckDB executes. BigQuery is a generation target with offline evidence. Every additional
  platform needs an adapter package and evidence from that platform.
- Adapter conformance proves compatibility with Ergasterion's interfaces. The target
  environment still owns production security, resilience, access control, cost control,
  and operations.

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

Install both declared adapters when one environment needs each of them:

```bash
pip install "ergasterion-factory[all]"
```

The `bigquery` extra installs the deployment adapter's pinned dbt runtime on its own.

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

The new estate contains the dbt project, portable profiles, adapter macros, and an
`estate.yml` with its labels and translator table. It also contains per-adapter structural
budgets, a local runtime binding, one reference product declaration, empty declaration,
seed and test directories, and a generated `GETTING-STARTED.md`.

### Seed a source declaration

Seed one from the source system's own DDL:

```bash
ergasterion import-ddl crm-source.sql --source crm
```

Seed one from a supplier's ODCS v3 contract:

```bash
ergasterion import-odcs supplier-contract.yml --source supplier
```

Resolve the marked decisions in the generated YAML before declaring products against it.
The importers refuse to overwrite an existing destination unless `--force` is supplied,
and `--force` replaces the file without merging.

### Generate and build

```bash
ergasterion emit-products
dbt deps --profiles-dir profiles
dbt build --profiles-dir profiles --target duckdb
```

`ergasterion emit-products --check` regenerates without writing and reports any drift
between the committed project and the declarations.

### Run the worked estate

The account-free demonstration regenerates the estate, builds it in DuckDB and writes its
results beneath `demo/offline-runs/`:

```bash
bash demo/run_offline_demo.sh
```

The landing demonstration exercises received batches, quarantine, publication, recovery,
and backup and restore through the local reference adapters:

```bash
bash demo/landing-ingestion/run_landing_demo.sh
```

### Command reference

| Command | Purpose |
|---|---|
| `ergasterion init <dir>` | Create an empty estate. |
| `ergasterion import-ddl` | Seed a source declaration from DDL. |
| `ergasterion import-odcs` | Seed a source declaration from an ODCS contract. |
| `ergasterion validate` | Validate declarations without generating anything. |
| `ergasterion validate-canonical` | Check a canonical product's declared reference mappings against a reference model checkout. |
| `ergasterion emit-products` | Generate the estate from its product declarations. |
| `ergasterion contracts` | Check or regenerate the ODCS contracts. |
| `ergasterion odps` | Check or regenerate the ODPS product descriptors. |
| `ergasterion product-graph` | Check or regenerate the estate product graph. |
| `ergasterion vault-rebaseline` | Stage, promote or abandon a `data_vault` satellite re-baseline. |
| `ergasterion lint` | Check SQL portability for a declared adapter. |
| `ergasterion structure` | Check the estate against each adapter's declared budgets. |
| `ergasterion plan` | Compile and inspect a landing product's execution plan. |
| `ergasterion contract` | Register and activate a Landing Product Contract. |
| `ergasterion deployment` | Register and activate a runtime binding. |
| `ergasterion ingest` | Submit a delivery, or process work that is due. |
| `ergasterion reconcile` | Resume or rebuild a blocked projection. |
| `ergasterion status` | Read a landing product's operational state. |
| `ergasterion inspect` | Read delivery and lineage evidence. |
| `ergasterion quarantine` | List, revalidate or release quarantined records. |
| `ergasterion local-backup` | Back up or restore the local reference runtime. |

Run `ergasterion <command> --help` for the exact options.

### Repository layout

| Path | Contents |
|---|---|
| `ergasterion/` | The engine: framework, profiles, pattern schemas, shapes, translators, adapters, the landing runtime, templates and gates. |
| `declarations/products/` | Product declarations, one per published product. |
| `declarations/` | Source schemas and landing configuration. |
| `declarations/targets/` | Per-adapter structural budgets and interface boundaries. |
| `estate.yml`, `rules/` | Estate policy and the estate's named-rule signatures. |
| `models/` | Generated dbt models, one tree per product. |
| `contracts/` | Generated ODCS contracts and ODPS descriptors. |
| `graphs/`, `manifests/` | Generated estate graph and per-product runtime manifests. |
| `macros/` | Named-rule implementations and the adapter-dispatch layer. |
| `demo/` | Account-free demonstrations. |
| `tests/` | Known-answer assertions, engine tests, and the architecture acceptance run. |
| `docs/` | The architecture guide, the landing deep dive, and the specifications. |

All repository examples use invented data. The e-commerce names, people, addresses and
brands are synthetic, and its email addresses use the reserved `example.com` domain.
Ergasterion is released under the MIT licence.
