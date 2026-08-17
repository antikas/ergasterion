# Ergasterion runbook

Ergasterion turns source and domain declarations into a tested dbt data-product
estate. The repository includes two complete examples: e-commerce and investment.

The quickest path runs locally on DuckDB, an embedded database stored in one file.
It needs no cloud account and does not contact Snowflake. A separate Snowflake path
deploys the same estate to a real account and consumes warehouse credits. BigQuery
is supported for generation, dialect linting, and static parsing; this repository
does not provide a live BigQuery deployment workflow.

## 1. Install

You need Python 3.11 or 3.12, Git, and Bash. Windows users can use Git Bash.

### Install the released package

For local DuckDB use:

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install "ergasterion-factory[duckdb]==0.3.2"
```

In Windows Git Bash, activate the environment with:

```bash
source .venv/Scripts/activate
```

Install a different adapter only when you need it:

```bash
python -m pip install "ergasterion-factory[snowflake]==0.3.2"
python -m pip install "ergasterion-factory[bigquery]==0.3.2"
python -m pip install "ergasterion-factory[all]==0.3.2"
```

The base package contains the declaration engine. Adapter extras add the pinned dbt
runtime used by this release.

### Work from the source repository

Clone the public repository, create the same environment, and install all adapters:

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

The supported release stack is dbt Core 1.11.12 with dbt-duckdb 1.11.0,
dbt-snowflake 1.11.6, and dbt-bigquery 1.11.3.

## 2. Run the complete local estate

From the source repository:

```bash
bash demo/run_offline_demo.sh
```

The command performs a complete dbt build against DuckDB, then runs the three
headline queries in this order:

1. e-commerce revenue and order metrics;
2. deterministic customer resolution and survivorship;
3. investment fund performance and hurdle metrics.

The database is written beneath `target/`. The transcript and three text/CSV result
pairs are written beneath `demo/offline-runs/<UTC timestamp>/`. Both locations are
ignored by Git.

The command resets the selected local DuckDB file before each run. Any decisions
written only to that database are therefore deleted. Tracked fixture rows are rebuilt;
untracked decisions are not.

For the repository’s full local validation, including all three adapter parses,
dialect checks, contract and graph validation, and the complete DuckDB build, run:

```bash
bash scripts/validate_offline.sh
```

Neither command reads Snowflake credentials or opens a Snowflake connection.

## 3. Create your own estate

After installing the package:

```bash
ergasterion init my-data-products
cd my-data-products
```

The new directory contains an empty dbt estate with:

- `declarations/` for source-system tables and projections;
- `domains/` for entities, vault structures, survivorship, resolution, and products;
- `seeds/` for local or test source data;
- `tests/` for business assertions;
- `profiles/` for DuckDB, Snowflake, and BigQuery targets;
- `macros/` for the adapter translation layer.

Read `GETTING-STARTED.md` in the new estate before adding the first source. The
declaration files are authored inputs. Generated models, contracts, descriptors, and
graphs are outputs and should not be edited by hand.

Useful commands inside an estate are:

```bash
ergasterion emit
ergasterion emit --check
ergasterion contracts
ergasterion odps
ergasterion graph
ergasterion lint --target duckdb
ergasterion structure
```

`emit --check` reports drift without writing. The other `--check` modes follow the
same pattern where available.

### Start from an existing schema

Create a source declaration from an ODCS v3 contract:

```bash
ergasterion import-odcs supplier-contract.yml --source supplier_name
```

Create a source or domain skeleton from SQL DDL:

```bash
ergasterion import-ddl source-tables.sql --mode feed --source supplier_name
ergasterion import-ddl model-tables.sql --mode model --domain domain_name
```

These import commands transcribe structure only. They leave business decisions such
as survivorship, identity resolution, and relationship meaning for a human to complete.

## 4. Generated architecture

The emitted dbt project follows a stable sequence:

```text
source seeds or external tables
  -> staging layer
  -> raw vault hubs, links, and satellites
  -> entity resolution
  -> business-vault survivorship
  -> canonical models and marts
  -> ODCS contracts, ODPS descriptors, and property-graph projections
```

Here, “staging layer” is a data-model term. It is the first transformation layer in
the running product, not a statement about release readiness.

The generator uses declarations as the source of truth. A source-system change is
made in its declaration and regenerated through the same path. Hand-authored business
logic remains in the domain configuration, canonical models, marts, and singular tests.

## 5. Snowflake deployment

This path uses a real Snowflake account. It creates database objects and consumes
warehouse credits. The supplied warehouse is extra-small and auto-suspends after
60 seconds.

### Prerequisites

Install the Snowflake CLI and the Snowflake package extra:

```bash
python -m pip install "ergasterion-factory[snowflake]==0.3.2"
snow --version
```

Create a Snow CLI connection with key-pair authentication. Keep the private key and
passphrase outside the repository. Test the connection before continuing:

```bash
snow connection test -c dpf
```

The dbt profile reads these environment variables:

```text
DPF_SF_ACCOUNT
DPF_SF_USER
DPF_SF_KEY_PATH
DPF_SF_KEY_PASSPHRASE
DPF_SF_ROLE       default DPF_BUILDER
DPF_SF_DB         default ERGASTERION
DPF_SF_WH         default DPF_WH
DPF_SF_SCHEMA     default DEV
```

`DPF_SF_KEY_PATH` must be an absolute path. Do not put any of these values in a
tracked file.

### One-time account setup

Review `snowflake/setup.sql`, then run it through a connection whose user can assume
`ACCOUNTADMIN`:

```bash
snow sql -c dpf -f snowflake/setup.sql
```

The script creates the `ERGASTERION` database, the auto-suspending `DPF_WH`
warehouse, and the `DPF_BUILDER` role. It does not create a user or store a secret.
Grant the role to the user used by your connection, replacing the placeholder:

```sql
GRANT ROLE DPF_BUILDER TO USER YOUR_SNOWFLAKE_USER;
```

Set the connection’s active role to `DPF_BUILDER` for normal runs.

### Build both example products

From the source repository:

```bash
bash demo/run_clean_demo.sh --connection dpf
```

The command creates a fresh timestamped schema prefix, generates the dbt estate,
downloads dbt packages locally, deploys and executes the Snowflake dbt project, prints
the same three result sets as the local demo, and suspends the warehouse. A transcript
and CSV/text result pairs are written beneath `demo/live-runs/<UTC timestamp>/`.

Use `--skip-setup-sql` after the one-time setup if the active connection no longer has
the administrator role:

```bash
bash demo/run_clean_demo.sh --connection dpf --skip-setup-sql
```

The script runs in the foreground. Stop if Snowflake reports an unexpected role,
database, schema, or warehouse.

### Deploy the management console

The Streamlit application provides the investment entity-resolution review queue,
deal approvals, and the deal pipeline browser:

```bash
snow streamlit deploy --project streamlit -c dpf --replace
```

The console runs inside Snowflake and writes approved or rejected decisions to the
append-only decision tables. A subsequent dbt build materialises downstream stage and
mart changes.

### Remove the example infrastructure

Removing the database destroys all model output and any decisions that were not
exported elsewhere. Run these statements only when that is the intended outcome:

```sql
USE ROLE ACCOUNTADMIN;
DROP DATABASE IF EXISTS ERGASTERION;
DROP WAREHOUSE IF EXISTS DPF_WH;
DROP ROLE IF EXISTS DPF_BUILDER;
```

## 6. BigQuery static validation

Install the BigQuery extra and set a project and dataset:

```bash
python -m pip install "ergasterion-factory[bigquery]==0.3.2"
export DPF_BQ_PROJECT="your-project"
export DPF_BQ_DATASET="ergasterion_dev"
dbt parse --profiles-dir profiles -t bigquery --no-partial-parse
```

This confirms that the generated project parses for dbt-bigquery. It does not validate
credentials, permissions, cost controls, or execution against a live BigQuery project.

## 7. Troubleshooting

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

Run the check first, then regenerate from declarations:

```bash
ergasterion emit --check
ergasterion emit
```

Do not patch generated SQL directly. Change the declaration, template, or the
hand-authored model that owns the behavior.

### DuckDB cannot open the database

Close any process holding the file, then rerun the local demo. The default database is
`target/ergasterion.duckdb`. If `DPF_DUCKDB_PATH` is set, keep it beneath the repository
`target/` directory when using the demo reset command.

### Snowflake authentication fails

Test the named Snow CLI connection first. Then verify the absolute key path, the active
role, and the account identifier. Do not print private-key contents or passphrases while
diagnosing the connection.

## 8. Security boundary

- The local DuckDB path needs no cloud credentials.
- Snowflake and BigQuery credentials are supplied at runtime and remain outside Git.
- Example people, companies, funds, orders, and deals are synthetic.
- Generated run directories, database files, logs, caches, and package build outputs are
  ignored and are not part of the source distribution.
- The repository’s MIT licence and third-party schema notices are included in source and
  Python package archives.
