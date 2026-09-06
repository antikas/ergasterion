# Ergasterion runbook

Use this guide to install Ergasterion, run both worked domains, generate an estate of your
own, receive a source delivery, and operate a Data Vault re-baseline. The
[architecture guide](docs/architecture/README.md) explains what the engine is and why it
is built this way; this document is the sequence of commands.

Two adapters are declared. DuckDB executes: it runs locally as an embedded database held
in one file and needs no cloud account. BigQuery is a generation target with offline
evidence: its project is generated, parsed, dialect-checked, budget-checked and
regenerated deterministically, but nothing here runs a query in a BigQuery project.

## 1. Install

You need Python 3.11 or newer, Git and Bash. On Windows, Git Bash works.

### Install the released package

For the complete local path:

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install "ergasterion-factory[local-ingestion]"
```

In Windows Git Bash, activate the environment with:

```bash
source .venv/Scripts/activate
```

Install the deployment adapter's runtime, or both, when you need them:

```bash
python -m pip install "ergasterion-factory[bigquery]"
python -m pip install "ergasterion-factory[all]"
```

The base package contains the declaration engine. Each adapter extra installs the pinned
dbt runtime for that adapter.

### Work from the source repository

```bash
git clone https://github.com/antikas/ergasterion.git
cd ergasterion
python -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e ".[all]"
```

Confirm that the active commands come from the repository environment:

```bash
python -c "import sys; print(sys.executable)"
dbt --version
ergasterion --help
```

The supported release stack is dbt Core 1.11.12 with dbt-duckdb 1.11.0 and dbt-bigquery
1.11.3.

## 2. Run the worked domains

From the source repository:

```bash
bash demo/run_offline_demo.sh
```

The script regenerates all 124 product declarations and reports any drift. It resets a
local DuckDB file beneath `target/`, then builds both domains with every generated test and
all 40 known-answer assertions. It prints three e-commerce results: revenue by conformed
segment, one resolved customer, and order lines reconciled to each source total.

The transcript and three text and CSV result pairs are written beneath
`demo/offline-runs/<UTC timestamp>/`. Both that directory and `target/` are ignored by
Git.

The command resets the selected local DuckDB file before each run. Anything written only
to that database is therefore deleted. Tracked fixture rows are rebuilt; untracked
decisions are not.

For the repository's full local validation, including both adapter parses, dialect checks,
structural budgets, contract and graph validation, the complete DuckDB build and the wheel
arm:

```bash
bash scripts/validate_offline.sh
```

For the thirteen architecture acceptance checks, one line printed per check:

```bash
bash scripts/validate_engine_architecture.sh
```

Neither command reads a cloud credential or opens a warehouse connection.

## 3. Generate and gate an estate

These are the commands that operate on an estate's declarations. Run them from the estate
root, or pass `--estate-root PATH`.

```bash
ergasterion validate                # validate the declarations, generate nothing
ergasterion emit-products           # generate every artefact from the declarations
ergasterion emit-products --check   # regenerate in memory and report drift, write nothing
ergasterion contracts --check       # ODCS contracts: schema-valid and drift-free
ergasterion odps --check            # ODPS descriptors: schema-valid and drift-free
ergasterion product-graph --check   # the estate graph: drift-free
ergasterion lint --target duckdb    # per-adapter dialect rules over the generated SQL
ergasterion structure               # per-adapter structural budgets and boundaries
```

`emit-products` prints one line per product naming its label, profile, shape, owning
translator, declared adapters and artefact count, then the structural-budget result over
every declared adapter. It writes nothing outside `models/`, `contracts/`, `graphs/` and
`manifests/`.

Then build the generated project:

```bash
dbt deps --profiles-dir profiles
dbt parse --profiles-dir profiles --no-partial-parse -t duckdb
dbt parse --profiles-dir profiles --no-partial-parse -t bigquery
dbt build --profiles-dir profiles -t duckdb
```

Generated files are outputs. Do not edit one: change the declaration that produced it and
regenerate. `emit-products --check` is what reports a hand edit, and it names the file.

## 4. Landing: receive and check a source delivery

The landing profile is the boundary that receives a source's delivered batch, checks it
against a written contract, and publishes the accepted rows or quarantines the rejected
ones. [`docs/architecture/bronze-ingestion.md`](docs/architecture/bronze-ingestion.md) is
the full mechanism. This section is the operator's command sequence against the local
reference platform: SQLite for operational state, DuckDB for the projection, both stored
beneath an estate's own `runtime/data/`. No warehouse account and no network call is
needed.

Every landing command shares the same required arguments:

```bash
ergasterion <command> --project-dir PATH --source NAME --table KEY --binding PATH --environment NAME
```

`--binding` names a runtime binding YAML file, relative to `--project-dir` or absolute. It
declares which local adapter implements each of the runtime's nine ports and which target
relations the projection writes to.

Compile and check the execution plan and runtime manifest for one product. This is
read-only:

```bash
ergasterion plan --project-dir . --source SOURCE --table TABLE --binding runtime/TABLE.yml --environment local
```

Register and activate the contract, then register and activate its binding-only
deployment:

```bash
ergasterion contract register --project-dir . --source SOURCE --table TABLE --binding runtime/TABLE.yml --environment local
ergasterion contract activate --project-dir . --source SOURCE --table TABLE --binding runtime/TABLE.yml --environment local \
  --candidate-digest CONTRACT_DIGEST --migration carry
ergasterion deployment register --project-dir . --source SOURCE --table TABLE --binding runtime/TABLE.yml --environment local
ergasterion deployment activate --project-dir . --source SOURCE --table TABLE --binding runtime/TABLE.yml --environment local \
  --manifest-digest MANIFEST_DIGEST
```

A carry migration keeps a product's visibility progress across the activation. A reset
migration authorises a new baseline. Both digests are read from the JSON a prior command
already printed: `--json` on any command prints one machine-readable envelope with every
digest it produced.

Submit a delivery, its payload and sidecar manifest together:

```bash
ergasterion ingest file --project-dir . --source SOURCE --table TABLE --binding runtime/TABLE.yml --environment local \
  --manifest path/to/delivery.manifest.json --payload path/to/delivery.csv
```

Replaying the same manifest and payload is idempotent: the second call reports `noop`,
which prevents accepted rows from being duplicated.

Read a product's operational status, its evidence, and its quarantined rows:

```bash
ergasterion status --project-dir . --source SOURCE --table TABLE --binding runtime/TABLE.yml --environment local
ergasterion inspect --project-dir . --source SOURCE --table TABLE --binding runtime/TABLE.yml --environment local --delivery-id DELIVERY_ID
ergasterion quarantine --project-dir . --source SOURCE --table TABLE --binding runtime/TABLE.yml --environment local --action list
```

Release a quarantined row once its underlying cause is fixed:

```bash
ergasterion quarantine --project-dir . --source SOURCE --table TABLE --binding runtime/TABLE.yml --environment local \
  --action release --disposition-id DISPOSITION_ID
```

Resume a commit-blocked projection or rebuild a lagging target cursor:

```bash
ergasterion reconcile --project-dir . --source SOURCE --table TABLE --binding runtime/TABLE.yml --environment local
```

Create a verified backup of the complete local runtime root, and restore from it:

```bash
ergasterion local-backup --project-dir . --source SOURCE --table TABLE --binding runtime/TABLE.yml --environment local \
  --action create --destination /path/outside/the/project/and/runtime/roots
ergasterion local-backup --project-dir . --source SOURCE --table TABLE --binding runtime/TABLE.yml --environment local \
  --action restore --manifest /path/to/backup/backup-manifest.json
```

`bash demo/landing-ingestion/run_landing_demo.sh` runs this whole sequence end to end
against three worked scenarios: a normal publication, a source-complete but
acceptance-incomplete snapshot, and a backup and restore cycle. It is account-free and
network-free.

## 5. Create your own estate

After installing the package:

```bash
ergasterion init my-data-products
cd my-data-products
```

The new directory contains:

- `estate.yml` with the layer labels, the declared adapters and the translator table;
- `declarations/` for source declarations, `declarations/products/` for product
  declarations, and `declarations/targets/` for each adapter's structural budgets and
  interface boundaries;
- `profiles/` for the DuckDB and BigQuery dbt targets;
- `macros/` for the named-rule implementations and the adapter-dispatch layer;
- `runtime/local.yml`, one worked runtime binding for the local reference platform;
- `seeds/` and `tests/`, empty.

Read `GETTING-STARTED.md` in the new estate before adding the first source. Declaration
files are authored inputs. Generated models, contracts, descriptors, graphs and manifests
are outputs and are never edited by hand.

### Start from an existing schema

Create a source declaration from an ODCS v3 contract:

```bash
ergasterion import-odcs supplier-contract.yml --source supplier_name
```

Create a source declaration from the source system's own DDL:

```bash
ergasterion import-ddl source-tables.sql --source supplier_name
```

Both transcribe structure only. They leave every decision their input does not state --
ownership, scheduling, what counts as a passing row, which layer label the table sits in,
how its records resolve against other sources -- as an explicit note for a person to
answer. [`DEMO.md`](DEMO.md) works both through end to end.

## 6. Operate a data_vault re-baseline

A product whose target names the `data_vault` shape stores versions of an entity's payload
in satellites, keyed by a fingerprint of that payload. The **hashdiff basis** is the exact
column set a satellite's stored fingerprints were computed over. It is frozen once versions
are stored under it, so a declared change to it does not take effect silently: emission
fails closed and names the satellite, the columns and this operation.

Moving the basis has three declared steps. Pause scheduled builds before starting, and keep
them paused until the last one succeeds.

**Step 1: stage.** Record the currently declared basis as the satellite's pending one.
Emission then stops on the pending-basis gate, so no build can land versions while the
estate carries two answers to what change detection means.

```bash
ergasterion vault-rebaseline --product PRODUCT --satellite SATELLITE --stage
```

**Step 2: promote.** Adopt the pending basis. The recorded basis becomes the declared one
and the basis version advances.

```bash
ergasterion vault-rebaseline --product PRODUCT --satellite SATELLITE --promote
```

**Step 3: regenerate and deploy.** The next build stores exactly one version per entity
under the new basis version and leaves every version stored under the old one exactly as
it was.

```bash
ergasterion emit-products
dbt build --profiles-dir profiles -t duckdb
```

To abandon a staged re-baseline before promoting it:

```bash
ergasterion vault-rebaseline --product PRODUCT --satellite SATELLITE --abort
```

The recorded basis is unchanged, so the declared change that prompted the re-baseline fails
closed again on the next emission and the estate converges back to where it was.

Nothing in this operation rewrites a stored fingerprint. A fingerprint is a fact about the
basis it was computed under; every stored row carries its basis version beside it, and a
satellite's change detection is scoped to its own basis version. History keeps the meaning
it had under the basis that produced it.

The evolution ledger beside the product records each satellite's payload roster, its
hashdiff basis, its declared types and its basis version. It is durable state for a running
estate. Commit it and deploy it with the generated models. If it is lost, restore it from
the deployed revision. A fresh ledger cannot describe existing history.

## 7. BigQuery

Install the deployment adapter's runtime and point dbt at a project and dataset:

```bash
python -m pip install "ergasterion-factory[bigquery]"
export DPF_BQ_PROJECT="your-project"
export DPF_BQ_DATASET="ergasterion_dev"
dbt parse --profiles-dir profiles -t bigquery --no-partial-parse
```

This is what the repository's own offline lane runs, and it is exactly what it
establishes: the generated project parses under dbt-bigquery, passes the adapter's dialect
rules and its declared structural budgets, and regenerates deterministically. Building or
querying in a real project is the account owner's step, and the credentials, permissions,
cost controls and deployment settings live in that environment.

## 8. Troubleshooting

### A command resolves outside `.venv`

Reactivate the repository environment and check the paths again:

```bash
source .venv/bin/activate
python -c "import sys; print(sys.executable)"
command -v dbt
command -v ergasterion
```

Use `.venv/Scripts/activate` in Windows Git Bash.

### dbt packages are missing

From the estate root:

```bash
dbt deps --profiles-dir profiles
```

### Generated files drift

Run the check first, then regenerate from the declarations:

```bash
ergasterion emit-products --check
ergasterion emit-products
```

Do not patch a generated file directly. Change the declaration, the estate configuration
or the named-rule implementation that owns the behaviour.

### DuckDB cannot open the database

Close any process holding the file, then rerun the demonstration. The default database is
`target/ergasterion.duckdb`. If `DPF_DUCKDB_PATH` is set, keep it beneath the repository's
`target/` directory when using the demonstration's reset step.

### Emission fails closed and names a rule, a label or a pair

That is the engine refusing to guess. Three failures are common when an estate is young:

- a layer label with no translator-table entry for a pattern its profile carries. Add the
  entry to `estate.yml`;
- a named rule with no implementation for a declared translator and adapter pair. Add the
  implementation, or remove the adapter from the estate's declared set;
- a source contract whose current version is incompatible with what a consumer expects.
  Re-declare the consumer's expectation against the published contract.

## 9. Security boundary

- The local DuckDB path needs no cloud credentials.
- Deployment credentials are supplied at run time in the target environment and stay
  outside Git.
- Example people, companies, orders and products are synthetic.
- Generated run directories, database files, logs, caches and package build outputs are
  ignored and are not part of the source distribution.
- The repository's MIT licence and third-party schema notices are included in the source
  and Python package archives.
