#!/usr/bin/env bash
# demo/run_offline_demo.sh: the account-free demonstration of the worked estate.
#
# It regenerates the estate from its product declarations and reports any drift,
# builds the generated project on DuckDB, and prints three business results from
# the built relations. Every byte it writes lands under demo/offline-runs/<UTC-id>/,
# which Git ignores. No warehouse account, no credentials and no network call.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd -P)"
cd "${REPO_ROOT}"
. "${SCRIPT_DIR}/scenarios/common.sh"
. "${SCRIPT_DIR}/queries/render.sh"

usage() {
    cat <<'USAGE'
usage: bash demo/run_offline_demo.sh [--help]

  (no argument)  regenerate the estate, check it for drift, build it on DuckDB
                 and print the three business results, writing evidence under
                 demo/offline-runs/<UTC-id>/
  --help         print this message
USAGE
}

case "${1-}" in
    --help|-h)
        usage
        exit 0
        ;;
    "")
        ;;
    *)
        echo "unknown argument: ${1}" >&2
        usage >&2
        exit 2
        ;;
esac

PY_BIN="$(dpf_resolve_tool PY "${PY:-}" \
    "${REPO_ROOT}/.venv/bin/python" \
    "${REPO_ROOT}/.venv/Scripts/python.exe" \
    "${REPO_ROOT}/.venv/Scripts/python")"
DBT_BIN="$(dpf_resolve_tool DBT "${DBT:-}" \
    "${REPO_ROOT}/.venv/bin/dbt" \
    "${REPO_ROOT}/.venv/Scripts/dbt.exe" \
    "${REPO_ROOT}/.venv/Scripts/dbt")"

# The database is the only destructive target. Accept one .duckdb file directly
# under this repo's ignored target/ directory. Canonicalise the repo and the
# existing target (or its canonical parent before creating it), then require the
# physical target to be exactly the repo root's direct target/ child. This rejects
# external and in-repo symlink or junction redirects before reset or output write.
REPO_CANONICAL="$(dpf_canonical_path "${REPO_ROOT}")" \
    || dpf_fail "could not canonicalise repository root"
TARGET_DIR="${REPO_ROOT}/target"
if [ -e "${TARGET_DIR}" ] || [ -L "${TARGET_DIR}" ]; then
    [ -d "${TARGET_DIR}" ] || dpf_fail "target exists but is not a directory: ${TARGET_DIR}"
else
    mkdir "${TARGET_DIR}" || dpf_fail "could not create verified repo target directory"
fi
TARGET_CANONICAL="$(dpf_canonical_path "${TARGET_DIR}")" \
    || dpf_fail "could not canonicalise target directory"
dpf_assert_direct_physical_child "${TARGET_DIR}" "${REPO_ROOT}" target "target directory" \
    || dpf_fail "target directory failed physical identity"
DB_INPUT="${DPF_DUCKDB_PATH:-target/ergasterion.duckdb}"
case "${DB_INPUT}" in
    [A-Za-z]:[\\/]*)
        command -v cygpath >/dev/null 2>&1 \
            || dpf_fail "Windows DPF_DUCKDB_PATH needs cygpath for safe validation"
        DB_INPUT="$(cygpath -u "${DB_INPUT}")"
        ;;
esac
case "${DB_INPUT}" in
    /*) DB_CANDIDATE="${DB_INPUT}" ;;
    *) DB_CANDIDATE="${REPO_ROOT}/${DB_INPUT}" ;;
esac
DB_PARENT="$(dirname "${DB_CANDIDATE}")"
[ -d "${DB_PARENT}" ] || dpf_fail "DuckDB parent directory does not exist: ${DB_PARENT}"
dpf_assert_direct_physical_child "${DB_PARENT}" "${REPO_ROOT}" target "DuckDB parent" \
    || dpf_fail "DPF_DUCKDB_PATH must name a file directly under ${TARGET_CANONICAL}"
DB_FILE="$(basename "${DB_CANDIDATE}")"
[[ "${DB_FILE}" =~ ^[A-Za-z_][A-Za-z0-9_]*\.duckdb$ ]] \
    || dpf_fail "DuckDB filename must be an underscore-safe SQL identifier ending in .duckdb"
CATALOG="${DB_FILE%.duckdb}"
DB_PATH="${TARGET_DIR}/${DB_FILE}"

# The evidence path gets the same exact physical-identity treatment, through the shared
# guard in demo/scenarios/common.sh: the canonical direct demo child is validated before
# offline-runs is created, and each expected direct child before creating or opening
# transcript.log or any result file.
OFFLINE_RUNS_DIR="$(dpf_offline_runs_dir "${REPO_ROOT}")" \
    || dpf_fail "could not verify the offline-runs directory"

RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)"
RUN_DIR="${OFFLINE_RUNS_DIR}/${RUN_ID}"
mkdir "${RUN_DIR}" || dpf_fail "run directory already exists: ${RUN_DIR}"
RUN_CANONICAL="$(dpf_canonical_path "${RUN_DIR}")" \
    || dpf_fail "could not canonicalise created run directory"
dpf_assert_direct_physical_child "${RUN_DIR}" "${OFFLINE_RUNS_DIR}" "${RUN_ID}" \
    "run directory" \
    || dpf_fail "run directory failed physical identity"
LOG_FILE="${RUN_DIR}/transcript.log"
exec > >(tee "${LOG_FILE}") 2>&1

echo "== Ergasterion: account-free DuckDB demonstration =="
echo "   database      : ${DB_PATH}"
echo "   catalog       : ${CATALOG}"
echo "   run directory : demo/offline-runs/${RUN_ID}"
echo

echo "== [0/5] Pinned runtime preflight =="
DBT_VERSION_OUTPUT="$("${DBT_BIN}" --version)"
printf '%s\n' "${DBT_VERSION_OUTPUT}"
printf '%s\n' "${DBT_VERSION_OUTPUT}" | grep -Eq 'installed:[[:space:]]+1\.11\.12([[:space:]]|$)' \
    || dpf_fail "dbt-core 1.11.12 is required"
printf '%s\n' "${DBT_VERSION_OUTPUT}" | grep -Eq 'duckdb:[[:space:]]+1\.11\.0([[:space:]]|$)' \
    || dpf_fail "dbt-duckdb 1.11.0 is required"
DUCKDB_VERSION="$("${PY_BIN}" -c 'import duckdb; print(duckdb.__version__)')" \
    || dpf_fail "PY cannot import duckdb"
echo "duckdb Python module: ${DUCKDB_VERSION}"
echo

echo "== [1/5] Regenerate the estate from its product declarations, and report drift =="
"${PY_BIN}" ergasterion/emit_products.py --check \
    || dpf_fail "the committed project does not match the product declarations"
echo

echo "== [2/5] Reset verified local database =="
rm -f -- "${DB_PATH}" "${DB_PATH}.wal"
export DPF_DUCKDB_PATH="${DB_PATH}"
echo "reset: ${DB_PATH}"
echo

echo "== [3/5] Full dbt build on the reference adapter =="
"${DBT_BIN}" build --profiles-dir profiles -t duckdb
echo

SCHEMA="$("${PY_BIN}" - "${DB_PATH}" <<'PY'
import duckdb
import sys

with duckdb.connect(sys.argv[1], read_only=True) as connection:
    print(connection.execute("select current_schema()").fetchone()[0])
PY
)" || dpf_fail "could not derive DuckDB's profile schema"
[[ "${SCHEMA}" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]] \
    || dpf_fail "DuckDB returned an unsafe schema name: ${SCHEMA}"

run_query() {
    local title="$1"
    local output_stem="$2"
    local query="$3"
    local txt_path="${RUN_DIR}/${output_stem}.txt"
    local csv_path="${RUN_DIR}/${output_stem}.csv"

    echo "-- ${title} --"
    DPF_DEMO_DB="${DB_PATH}" \
    DPF_DEMO_QUERY="${query}" \
    DPF_DEMO_TXT="${txt_path}" \
    DPF_DEMO_CSV="${csv_path}" \
    "${PY_BIN}" - <<'PY'
import csv
import os

import duckdb

with duckdb.connect(os.environ["DPF_DEMO_DB"], read_only=True) as connection:
    cursor = connection.execute(os.environ["DPF_DEMO_QUERY"])
    headers = [column[0] for column in cursor.description]
    rows = cursor.fetchall()

display_rows = [["" if value is None else str(value) for value in row] for row in rows]
widths = [len(header) for header in headers]
for row in display_rows:
    widths = [max(width, len(value)) for width, value in zip(widths, row)]

separator = "+-" + "-+-".join("-" * width for width in widths) + "-+"
lines = [
    separator,
    "| " + " | ".join(header.ljust(width) for header, width in zip(headers, widths)) + " |",
    separator,
]
for row in display_rows:
    lines.append("| " + " | ".join(value.ljust(width) for value, width in zip(row, widths)) + " |")
lines.extend([separator, f"{len(rows)} row(s)"])
table_text = "\n".join(lines) + "\n"

with open(os.environ["DPF_DEMO_TXT"], "w", encoding="utf-8", newline="\n") as handle:
    handle.write(table_text)
with open(os.environ["DPF_DEMO_CSV"], "w", encoding="utf-8", newline="") as handle:
    writer = csv.writer(handle, lineterminator="\n")
    writer.writerow(headers)
    writer.writerows(rows)

print(table_text, end="")
PY
    echo "wrote: demo/offline-runs/${RUN_ID}/${output_stem}.{txt,csv}"
    echo
}

echo "== [4/5] Business results from the built relations =="
REVENUE_QUERY="$(dpf_render_query "${SCRIPT_DIR}/queries/revenue-by-segment.sql" \
    CATALOG "${CATALOG}" \
    SCHEMA "${SCHEMA}")" \
    || dpf_fail "could not render the revenue query"
run_query "revenue and units by conformed segment" revenue-by-segment "${REVENUE_QUERY}"

RESOLUTION_QUERY="$(dpf_render_query "${SCRIPT_DIR}/queries/customer-resolution.sql" \
    CATALOG "${CATALOG}" \
    SCHEMA "${SCHEMA}")" \
    || dpf_fail "could not render the customer-resolution query"
run_query "tri-source collapse and surviving contact values" \
    customer-resolution "${RESOLUTION_QUERY}"

RECONCILIATION_QUERY="$(dpf_render_query "${SCRIPT_DIR}/queries/order-reconciliation.sql" \
    CATALOG "${CATALOG}" \
    SCHEMA "${SCHEMA}")" \
    || dpf_fail "could not render the order-reconciliation query"
run_query "summarised order lines against the stated order total" \
    order-reconciliation "${RECONCILIATION_QUERY}"

echo "== [5/5] Evidence written =="
OUTPUT_COUNT="$(find "${RUN_DIR}" -maxdepth 1 -type f | wc -l | tr -d ' ')"
[ "${OUTPUT_COUNT}" = "7" ] \
    || dpf_fail "expected transcript plus three result pairs, found ${OUTPUT_COUNT} files"

echo "== Done in ${SECONDS}s =="
echo "   transcript : demo/offline-runs/${RUN_ID}/transcript.log"
echo "   results    : revenue-by-segment.{txt,csv}"
echo "                customer-resolution.{txt,csv}"
echo "                order-reconciliation.{txt,csv}"
