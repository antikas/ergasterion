# Run the worked product

Ergasterion includes two complete demonstrations over invented data. The local path
uses DuckDB, an embedded database stored in a file, and needs no warehouse account.
DuckDB is the executable reference implementation. Ergasterion also generates projects for
Snowflake and BigQuery. The repository checks them through project parsing, dialect linting,
deterministic generation, structure checks, and adapter conformance tests.

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

## Estate-evolution scenario

A source eventually delivers a field the warehouse does not carry yet. Estate
evolution is how a generated warehouse takes that field while it keeps every version
of every record it already holds. The addition itself is an **extension**: a payload
change that only adds, absorbed by the running warehouse. It leaves the **hashdiff
basis** frozen, which is the exact column set the warehouse computed its stored
fingerprints over, so every fingerprint already written stays where it is.

This scenario demonstrates that on a scratch copy of the estate:

```bash
bash demo/run_offline_demo.sh --evolution
```

The run copies the estate into a scratch directory under `demo/offline-runs/`, builds
it on DuckDB, adds one payload field to the product entity, changes one record's value
in a fingerprinted column, and rebuilds. It then checks the result by machine: the new
column is present in the satellite, every version stored before the change keeps its
identity together with its load datetime and its record source, the changed record
gains exactly one new version carrying the value its projection expression authors,
each version stored before the change carries NULL for the new field, a repeat build
adds nothing, and every relation holding history is the same relation as before,
altered in place. A check that fails ends the run with a non-zero exit code.

The run needs no account and no network call, and it takes about ninety seconds. The
scratch estate is deleted at the end, so `demo/offline-runs/` is empty when the run
closes.

## Watermark-increment scenario

A source grows, and most of it stops changing. Watermark increments are how a generated
warehouse reads only the part that can still change, while it keeps the full history it
already stores. A table declares a **staging increment block**: a lookback, and an
acknowledgment that the table's effective date advances when the source redelivers a
record. Everything else is derived. The **effective column** is the one staging column
the table's bridge maps to `effective_from`. The **consumption watermark** is the point
on that column up to which every satellite fed by the table has absorbed history. The
**delta window** runs from the consumption watermark minus the lookback upwards, and a
normal run reads exactly that window.

This scenario demonstrates that on a scratch copy of the estate:

```bash
bash demo/run_offline_demo.sh --watermark
```

The run copies the estate into a scratch directory under `demo/offline-runs/`, declares
a staging increment block on the CRM customer feed, and builds it on DuckDB. That first
build absorbs everything, because a warehouse that has absorbed nothing has an unbounded
window. The run then appends four delivered records to the feed: a new customer, an
update to a customer the warehouse already stores, a record arriving late inside the
lookback, and a record sitting exactly on the window floor.

It then checks the result by machine: the second build stages exactly the records inside
the window, the stored records below the floor remain unchanged, the delete
side of the incremental write carries the resolved floor, the record on the floor lands
exactly once, the satellite gains exactly one version per changed record, a third
identical build adds nothing, a following full-refresh build leaves the stored history
byte-equal, and a table that declares no block keeps its relation digest through every
build. The run closes by adding a payload field while the block stands, and checks that
the field reaches the incremental staging relation in place and carries its delivered
value onto the new version. A check that fails ends the run with a non-zero exit code.

The run needs no account and no network call, and it takes about two minutes. The scratch
estate is deleted at the end, so `demo/offline-runs/` is empty when the run closes.

## Snowflake demonstration

The Snowflake demonstration script provisions and runs the worked estate in an account
selected by its owner. Its help can be inspected without opening a connection:

```bash
bash demo/run_clean_demo.sh --help
```

The account owner must provide a working Snow CLI connection, the Snowflake dbt
adapter, and the environment settings described in `RUNBOOK.md`. The default
connection name is `dpf`; pass `--connection <name>` to use another configured
connection. The script applies `snowflake/setup.sql` unless `--skip-setup-sql` is
given.

Running the script creates Snowflake objects and consumes warehouse credits. The default
warehouse is extra-small, has automatic suspension enabled, and is suspended explicitly
when the script finishes. Review the target account, role, database, and warehouse before
running it.

An account-owned Snowflake run uses a timestamped schema by default and writes its
transcript and query results beneath `demo/live-runs/<UTC-id>/`. The directory is local
runtime output and is ignored by Git.

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
