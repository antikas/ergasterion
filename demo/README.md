# Run the worked product

Ergasterion includes two complete demonstrations over invented data. The local path
uses DuckDB, an embedded database stored in a file, and needs no warehouse account.
The Snowflake path deploys the same project into an account you control.

Both paths run the e-commerce result first, customer resolution second, and the
investment result third. They render the same SQL templates from `demo/queries/`.

## Local DuckDB demonstration

Install the local runtime from the repository root:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[duckdb]"
dbt deps --profiles-dir profiles
```

Windows PowerShell uses `.venv\Scripts\Activate.ps1` for activation. Run the
demonstration through Bash on every platform:

```bash
bash demo/run_offline_demo.sh
```

The script starts with a new DuckDB file under the ignored `target/` directory,
runs the complete dbt project, and prints the three business results. It writes one
transcript plus three `.txt` and three `.csv` result files under
`demo/offline-runs/<UTC-id>/`. Those files are local outputs and are ignored by Git.

Set `PY` and `DBT` when you need to select explicit executables. The script otherwise
uses the matching tools inside the repository `.venv`. `DPF_DUCKDB_PATH` can select
another underscore-safe `.duckdb` filename directly under `target/`.

## Live Snowflake demonstration

The live script creates a fresh schema, loads the invented source data, runs the dbt
project, and prints the same three results:

```bash
bash demo/run_clean_demo.sh
bash demo/run_clean_demo.sh --help
```

The account owner must provide a working Snow CLI connection, the Snowflake dbt
adapter, and the environment settings described in `RUNBOOK.md`. The default
connection name is `dpf`; pass `--connection <name>` to use another configured
connection. The script applies `snowflake/setup.sql` unless `--skip-setup-sql` is
given.

This path creates Snowflake objects and consumes warehouse credits. The default
warehouse is extra-small, has automatic suspension enabled, and is suspended
explicitly when the script finishes. Review the target account, role, database, and
warehouse before running it.

Each live run uses a timestamped schema by default and writes its transcript and
query results beneath `demo/live-runs/<UTC-id>/`. The directory is local runtime
output and is ignored by Git.

## Bronze ingestion walkthrough

[`demo/bronze-ingestion/`](bronze-ingestion/README.md) demonstrates Bronze, the
layer that receives a source's delivered batch and checks it against a written
contract, separately from the two dbt demonstrations above:

```bash
bash demo/bronze-ingestion/run_bronze_demo.sh
```

This needs no warehouse account, no network call, and no dbt project. Bronze reads
and writes through the local reference platform directly. See
[`demo/bronze-ingestion/README.md`](bronze-ingestion/README.md) for the three
scenarios it proves, and
[`docs/architecture/bronze-ingestion.md`](../docs/architecture/bronze-ingestion.md)
for the mechanism it exercises.
