#!/usr/bin/env bash
# demo/run_offline_demo.sh: full, account-free DuckDB demonstration.
#
# Runs the complete dbt project locally, then presents the same three business
# queries as the live Snowflake demo. Runtime evidence is written under
# demo/offline-runs/<UTC-id>/; source data and SQL remain in their existing SSOTs.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd -P)"
cd "${REPO_ROOT}"
. "${SCRIPT_DIR}/queries/render.sh"

fail() { echo "OFFLINE DEMO ERROR: $*" >&2; exit 1; }

resolve_tool() {
    local label="$1"
    local explicit_value="$2"
    shift 2

    if [ -n "${explicit_value}" ]; then
        [ -x "${explicit_value}" ] \
            || fail "${label} interpreter from environment is not executable: ${explicit_value}"
        printf '%s' "${explicit_value}"
        return
    fi

    local candidate
    for candidate in "$@"; do
        if [ -x "${candidate}" ]; then
            printf '%s' "${candidate}"
            return
        fi
    done
    fail "${label} interpreter not found: set ${label} or create the repo .venv"
}

PY_BIN="$(resolve_tool PY "${PY:-}" \
    "${REPO_ROOT}/.venv/bin/python" \
    "${REPO_ROOT}/.venv/Scripts/python.exe" \
    "${REPO_ROOT}/.venv/Scripts/python")"
DBT_BIN="$(resolve_tool DBT "${DBT:-}" \
    "${REPO_ROOT}/.venv/bin/dbt" \
    "${REPO_ROOT}/.venv/Scripts/dbt.exe" \
    "${REPO_ROOT}/.venv/Scripts/dbt")"

canonical_path() {
    "${PY_BIN}" - "$1" <<'PY'
from pathlib import Path
import sys

try:
    print(Path(sys.argv[1]).resolve(strict=True))
except (OSError, RuntimeError) as exc:
    print(f"cannot canonicalise {sys.argv[1]}: {exc}", file=sys.stderr)
    raise SystemExit(1)
PY
}

assert_direct_physical_child() {
    local child="$1"
    local parent="$2"
    local expected_name="$3"
    local label="$4"
    "${PY_BIN}" - "${child}" "${parent}" "${expected_name}" "${label}" <<'PY'
from pathlib import Path
import sys

child = Path(sys.argv[1]).resolve(strict=True)
parent = Path(sys.argv[2]).resolve(strict=True)
expected = parent / sys.argv[3]
label = sys.argv[4]
if child != expected or child.parent != parent:
    print(
        f"{label} physical identity mismatch: {child} != {expected}",
        file=sys.stderr,
    )
    raise SystemExit(1)
PY
}

# The database is the only destructive target. Accept one .duckdb file directly
# under this repo's ignored target/ directory. Canonicalise the repo and the
# existing target (or its canonical parent before creating it), then require the
# physical target to be exactly the repo root's direct target/ child. This rejects
# external and in-repo symlink or junction redirects before reset or output write.
REPO_CANONICAL="$(canonical_path "${REPO_ROOT}")" \
    || fail "could not canonicalise repository root"
TARGET_DIR="${REPO_ROOT}/target"
if [ -e "${TARGET_DIR}" ] || [ -L "${TARGET_DIR}" ]; then
    [ -d "${TARGET_DIR}" ] || fail "target exists but is not a directory: ${TARGET_DIR}"
else
    mkdir "${TARGET_DIR}" || fail "could not create verified repo target directory"
fi
TARGET_CANONICAL="$(canonical_path "${TARGET_DIR}")" \
    || fail "could not canonicalise target directory"
assert_direct_physical_child "${TARGET_DIR}" "${REPO_ROOT}" target "target directory" \
    || fail "target directory failed physical identity"
DB_INPUT="${DPF_DUCKDB_PATH:-target/ergasterion.duckdb}"
case "${DB_INPUT}" in
    [A-Za-z]:[\\/]*)
        command -v cygpath >/dev/null 2>&1 \
            || fail "Windows DPF_DUCKDB_PATH needs cygpath for safe validation"
        DB_INPUT="$(cygpath -u "${DB_INPUT}")"
        ;;
esac
case "${DB_INPUT}" in
    /*) DB_CANDIDATE="${DB_INPUT}" ;;
    *) DB_CANDIDATE="${REPO_ROOT}/${DB_INPUT}" ;;
esac
DB_PARENT="$(dirname "${DB_CANDIDATE}")"
[ -d "${DB_PARENT}" ] || fail "DuckDB parent directory does not exist: ${DB_PARENT}"
assert_direct_physical_child "${DB_PARENT}" "${REPO_ROOT}" target "DuckDB parent" \
    || fail "DPF_DUCKDB_PATH must name a file directly under ${TARGET_CANONICAL}"
DB_FILE="$(basename "${DB_CANDIDATE}")"
[[ "${DB_FILE}" =~ ^[A-Za-z_][A-Za-z0-9_]*\.duckdb$ ]] \
    || fail "DuckDB filename must be an underscore-safe SQL identifier ending in .duckdb"
CATALOG="${DB_FILE%.duckdb}"
DB_PATH="${TARGET_DIR}/${DB_FILE}"

# The evidence path gets the same exact physical-identity treatment. Validate the
# canonical direct demo child before creating offline-runs, then each expected
# direct child before creating or opening transcript.log or any metric file.
DEMO_DIR="${REPO_ROOT}/demo"
[ -d "${DEMO_DIR}" ] || fail "demo directory is missing: ${DEMO_DIR}"
assert_direct_physical_child "${DEMO_DIR}" "${REPO_ROOT}" demo "demo directory" \
    || fail "demo directory failed physical identity"
OFFLINE_RUNS_DIR="${DEMO_DIR}/offline-runs"
if [ -e "${OFFLINE_RUNS_DIR}" ] || [ -L "${OFFLINE_RUNS_DIR}" ]; then
    [ -d "${OFFLINE_RUNS_DIR}" ] \
        || fail "offline-runs exists but is not a directory: ${OFFLINE_RUNS_DIR}"
else
    mkdir "${OFFLINE_RUNS_DIR}" || fail "could not create verified offline-runs directory"
fi
OFFLINE_RUNS_CANONICAL="$(canonical_path "${OFFLINE_RUNS_DIR}")" \
    || fail "could not canonicalise offline-runs directory"
assert_direct_physical_child "${OFFLINE_RUNS_DIR}" "${DEMO_DIR}" offline-runs \
    "offline-runs directory" \
    || fail "offline-runs directory failed physical identity"

RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)"
RUN_DIR="${OFFLINE_RUNS_DIR}/${RUN_ID}"
mkdir "${RUN_DIR}" || fail "run directory already exists: ${RUN_DIR}"
RUN_CANONICAL="$(canonical_path "${RUN_DIR}")" \
    || fail "could not canonicalise created run directory"
assert_direct_physical_child "${RUN_DIR}" "${OFFLINE_RUNS_DIR}" "${RUN_ID}" \
    "run directory" \
    || fail "run directory failed physical identity"
LOG_FILE="${RUN_DIR}/transcript.log"
exec > >(tee "${LOG_FILE}") 2>&1

echo "== Ergasterion: account-free DuckDB demo =="
echo "   database      : ${DB_PATH}"
echo "   catalog       : ${CATALOG}"
echo "   run directory : demo/offline-runs/${RUN_ID}"
echo

# The offline lane must be unable to inherit warehouse credentials accidentally.
while IFS='=' read -r env_name _; do
    case "${env_name}" in DPF_SF_*) unset "${env_name}" ;; esac
done < <(env)
if env | grep -q '^DPF_SF_'; then
    fail "Snowflake environment variables remained after offline credential scrub"
fi
echo "Snowflake environment variables: 0"

echo "== [0/5] Pinned runtime preflight =="
DBT_VERSION_OUTPUT="$("${DBT_BIN}" --version)"
printf '%s\n' "${DBT_VERSION_OUTPUT}"
printf '%s\n' "${DBT_VERSION_OUTPUT}" | grep -Eq 'installed:[[:space:]]+1\.11\.12([[:space:]]|$)' \
    || fail "dbt-core 1.11.12 is required"
printf '%s\n' "${DBT_VERSION_OUTPUT}" | grep -Eq 'duckdb:[[:space:]]+1\.11\.0([[:space:]]|$)' \
    || fail "dbt-duckdb 1.11.0 is required"
DUCKDB_VERSION="$("${PY_BIN}" -c 'import duckdb; print(duckdb.__version__)')" \
    || fail "PY cannot import duckdb"
echo "duckdb Python module: ${DUCKDB_VERSION}"
echo

echo "== [1/5] Reset verified local database =="
rm -f -- "${DB_PATH}" "${DB_PATH}.wal"
export DPF_DUCKDB_PATH="${DB_PATH}"
echo "reset: ${DB_PATH}"
echo

echo "== [2/5] Full dbt build =="
"${DBT_BIN}" build --profiles-dir profiles -t duckdb
echo

BASE_SCHEMA="$("${PY_BIN}" - "${DB_PATH}" <<'PY'
import duckdb
import sys

with duckdb.connect(sys.argv[1], read_only=True) as connection:
    print(connection.execute("select current_schema()").fetchone()[0])
PY
)" || fail "could not derive DuckDB's profile schema"
[[ "${BASE_SCHEMA}" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]] \
    || fail "DuckDB returned an unsafe base schema: ${BASE_SCHEMA}"
CALC_SCHEMA="${BASE_SCHEMA}_calculated_fields"
MARTS_SCHEMA="${BASE_SCHEMA}_marts"
RESOLUTION_SCHEMA="${BASE_SCHEMA}_resolution"
RAW_SCHEMA="${BASE_SCHEMA}_raw"
CANONICAL_SCHEMA="${BASE_SCHEMA}_canonical"

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

echo "== [3/5] E-commerce headline metrics =="
ECOMMERCE_QUERY="$(dpf_render_query "${SCRIPT_DIR}/queries/ecommerce-headline-metrics.sql" \
    CATALOG "${CATALOG}" \
    MARTS_SCHEMA "${MARTS_SCHEMA}")" \
    || fail "could not render e-commerce headline query"
run_query "revenue by segment/month + average order value" \
    ecommerce-headline-metrics "${ECOMMERCE_QUERY}"

echo "== [4/5] Customer entity-resolution proof =="
CUSTOMER_RESOLUTION_QUERY="$(dpf_render_query "${SCRIPT_DIR}/queries/ecommerce-resolution-metrics.sql" \
    CATALOG "${CATALOG}" \
    RAW_SCHEMA "${RAW_SCHEMA}" \
    RESOLUTION_SCHEMA "${RESOLUTION_SCHEMA}" \
    CANONICAL_SCHEMA "${CANONICAL_SCHEMA}")" \
    || fail "could not render customer-resolution query"
run_query "tri-source collapse + CRM-wins-contact survivorship" \
    ecommerce-resolution-metrics "${CUSTOMER_RESOLUTION_QUERY}"

echo "== [5/5] Investment headline metrics =="
METRICS_QUERY="$(dpf_render_query "${SCRIPT_DIR}/queries/headline-metrics.sql" \
    CATALOG "${CATALOG}" \
    CALC_SCHEMA "${CALC_SCHEMA}" \
    MARTS_SCHEMA "${MARTS_SCHEMA}")" \
    || fail "could not render investment headline query"
run_query "fund performance + hurdle" headline-metrics "${METRICS_QUERY}"

OUTPUT_COUNT="$(find "${RUN_DIR}" -maxdepth 1 -type f | wc -l | tr -d ' ')"
[ "${OUTPUT_COUNT}" = "7" ] \
    || fail "expected transcript plus three output pairs, found ${OUTPUT_COUNT} files"

echo "== Done in ${SECONDS}s =="
echo "   transcript : demo/offline-runs/${RUN_ID}/transcript.log"
echo "   outputs    : ecommerce-headline-metrics.{txt,csv}"
echo "                ecommerce-resolution-metrics.{txt,csv}"
echo "                headline-metrics.{txt,csv}"
