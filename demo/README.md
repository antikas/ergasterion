# Run the worked estate

You can inspect the generated warehouse and its source-delivery boundary without a cloud
account. Both demonstrations run locally on DuckDB, an embedded database held in one file.
They need no credentials or network call.

DuckDB executes: the whole estate is generated for the reference adapter, built on it,
and asserted on it. BigQuery is a generation target with offline evidence: its project is
parsed by dbt, dialect-checked, budget-checked and regenerated deterministically, and
nothing here runs a query in a BigQuery project.

## What you can watch it do

1. The estate demonstration regenerates all 124 product declarations and builds both
   worked domains with their generated tests and 40 known-answer assertions.
2. It prints three e-commerce results that expose resolution, survivorship, dimensional
   aggregation, and reconciliation.
3. The landing demonstration publishes a clean delivery, rejects an incomplete snapshot,
   and restores a verified local backup.

## Install the local runtime

From the repository root:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[local-ingestion]"
dbt deps --profiles-dir profiles
```

Windows PowerShell activates with `.venv\Scripts\Activate.ps1`. Run the demonstrations
through Bash on every platform.

## The estate demonstration

```bash
bash demo/run_offline_demo.sh
```

The script does four things in order:

1. regenerates every artefact from the product declarations and reports any drift
   between the committed project and what the declarations say it should be;
2. resets a fresh DuckDB file under the ignored `target/` directory;
3. builds the complete generated project, including every generated test and the
   estate's known-answer assertions;
4. queries the built relations and prints three business results.

The first result shows revenue and units by the conformed segment dimension. The second
shows one customer's three source records collapsing into one published customer, with
the contact values selected by the declared survivorship rule. The third reconciles each
order's summarised lines to the total stated by its source system.

It writes one transcript plus three `.txt` and three `.csv` result files under
`demo/offline-runs/<UTC-id>/`. Those are local outputs and Git ignores them.

Set `PY` and `DBT` to select explicit executables. Otherwise the script uses the
matching tools inside the repository `.venv`. `DPF_DUCKDB_PATH` can select another
underscore-safe `.duckdb` filename directly under `target/`.

## The landing demonstration

[`demo/landing-ingestion/`](landing-ingestion/README.md) demonstrates the landing
profile: the boundary that receives a source's delivered batch, checks it against a
written contract, and publishes or quarantines it. It runs separately from the estate
demonstration above. The landing runtime reads and writes through the local reference
platform directly. The estate demonstration builds the generated dbt project.

```bash
bash demo/landing-ingestion/run_landing_demo.sh
```

See [`demo/landing-ingestion/README.md`](landing-ingestion/README.md) for the three
scenarios it proves, and
[`docs/architecture/bronze-ingestion.md`](../docs/architecture/bronze-ingestion.md)
for the mechanism it exercises.
