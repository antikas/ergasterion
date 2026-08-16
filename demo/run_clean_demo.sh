#!/usr/bin/env bash
# Build both example data products in a fresh Snowflake schema and export three
# result sets. This command uses a real Snowflake account and consumes warehouse
# credits. The default warehouse is extra-small and auto-suspends after 60 seconds.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${REPO_ROOT}"
. "${SCRIPT_DIR}/queries/render.sh"

CONNECTION="dpf"
DBT_VERSION="1.10.15"
SCHEMA=""
SKIP_SETUP_SQL=0

print_help() {
    cat <<'EOF'
Usage: bash demo/run_clean_demo.sh [options]

Options:
  --connection NAME   Snow CLI connection name (default: dpf)
  --schema NAME       dbt schema prefix (default: DEMO_<UTC timestamp>)
  --dbt-version VER   Snowflake native dbt version (default: 1.10.15)
  --skip-setup-sql    do not reapply the idempotent account setup
  -h, --help          show this help and exit

Prerequisites are documented in RUNBOOK.md. Each run writes a transcript and
three text/CSV result pairs beneath demo/live-runs/<UTC timestamp>/.
EOF
}

while [ $# -gt 0 ]; do
    case "$1" in
        --connection)
            CONNECTION="$2"; shift 2 ;;
        --schema)
            SCHEMA="$2"; shift 2 ;;
        --dbt-version)
            DBT_VERSION="$2"; shift 2 ;;
        --skip-setup-sql)
            SKIP_SETUP_SQL=1; shift ;;
        -h|--help)
            print_help; exit 0 ;;
        *)
            echo "Unknown option: $1" >&2; print_help; exit 2 ;;
    esac
done

if [ -z "${SCHEMA}" ]; then
    SCHEMA="DEMO_$(date -u +%Y%m%d_%H%M%S)"
fi

[[ "${SCHEMA}" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]] \
    || { echo "Schema must be a Snowflake identifier: ${SCHEMA}" >&2; exit 2; }
[[ "${DBT_VERSION}" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] \
    || { echo "dbt version must use X.Y.Z form: ${DBT_VERSION}" >&2; exit 2; }

RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)"
RUN_DIR="demo/live-runs/${RUN_ID}"
mkdir -p "${RUN_DIR}"
LOG_FILE="${RUN_DIR}/transcript.log"

# Tee standard output and errors to the run transcript.
exec > >(tee "${LOG_FILE}") 2>&1

echo "== Ergasterion Snowflake demo =="
echo "   connection    : ${CONNECTION}"
echo "   fresh schema  : ${SCHEMA}"
echo "   dbt-version   : ${DBT_VERSION}"
echo "   run directory : ${RUN_DIR}"
echo

fail() { echo "DEMO FAILED: $*" >&2; exit 1; }

echo "== [0/6] Preflight =="
command -v snow >/dev/null 2>&1 || fail "snow CLI not found on PATH (RUNBOOK.md section 1)"
if [ -x ".venv/Scripts/python.exe" ] && [ -x ".venv/Scripts/dbt.exe" ]; then
    PY=".venv/Scripts/python.exe"
    DBT=".venv/Scripts/dbt.exe"
elif [ -x ".venv/bin/python" ] && [ -x ".venv/bin/dbt" ]; then
    PY=".venv/bin/python"
    DBT=".venv/bin/dbt"
else
    fail "repo .venv is missing Python or dbt; follow RUNBOOK.md section 1"
fi
snow connection test -c "${CONNECTION}" >/dev/null || fail "snow connection test -c ${CONNECTION} did not succeed"
echo "preflight OK"
echo

if [ "${SKIP_SETUP_SQL}" -eq 0 ]; then
    echo "== [1/6] Snowflake object setup (idempotent, safe to repeat) =="
    snow sql -c "${CONNECTION}" -f snowflake/setup.sql \
        || fail "Snowflake account setup failed"
    echo
else
    echo "== [1/6] Skipped (--skip-setup-sql) =="
    echo
fi

echo "== [2/6] Declare -> emit: regenerate the dbt project from declarations/ =="
"${PY}" -m ergasterion.emit
echo

echo "== [3/6] Install dbt packages locally =="
"${DBT}" deps --profiles-dir profiles
echo

echo "== [4/6] Native deploy into fresh schema ${SCHEMA} =="
# Snowflake resolves the deployed profile remotely, where local environment variables
# are unavailable. Substitute the validated schema into a temporary copy of the tracked
# profile for deployment, then restore the original on both success and failure.
PROFILES_FILE="profiles/profiles.yml"
PROFILES_BACKUP="${RUN_DIR}/.profiles.yml.orig"
cp "${PROFILES_FILE}" "${PROFILES_BACKUP}"
restore_profiles() { cp "${PROFILES_BACKUP}" "${PROFILES_FILE}"; rm -f "${PROFILES_BACKUP}"; }
trap restore_profiles EXIT

sed -i "s#{{ env_var('DPF_SF_SCHEMA', 'DEV') }}#${SCHEMA}#" "${PROFILES_FILE}"
grep -q "schema: \"${SCHEMA}\"" "${PROFILES_FILE}" \
    || fail "profiles.yml schema substitution did not take (template line changed upstream?)"

snow dbt deploy ergasterion \
    --source . \
    --profiles-dir profiles \
    --default-target snowflake \
    --force \
    --schema PUBLIC \
    --dbt-version "${DBT_VERSION}" \
    -c "${CONNECTION}"

# Keep the deployed project owned by the least-privilege build role.
snow sql -c "${CONNECTION}" -q "GRANT OWNERSHIP ON DBT PROJECT ERGASTERION.PUBLIC.ergasterion TO ROLE DPF_BUILDER COPY CURRENT GRANTS" \
    || fail "could not grant project ownership to DPF_BUILDER"

restore_profiles
trap - EXIT
echo

echo "== [5/6] Native execute: dbt build =="
snow dbt execute -c "${CONNECTION}" ERGASTERION.PUBLIC.ergasterion build
echo

echo "== [6/6] Headline metrics -- e-commerce leads, investment second (schema ${SCHEMA}) =="
CALC_SCHEMA="${SCHEMA}_calculated_fields"
MARTS_SCHEMA="${SCHEMA}_marts"
RESOLUTION_SCHEMA="${SCHEMA}_resolution"
RAW_SCHEMA="${SCHEMA}_raw"
CANONICAL_SCHEMA="${SCHEMA}_canonical"

echo "-- e-commerce domain: revenue by segment/month + average order value --"
# Read the same measures defined by the semantic layer. The point-in-time segment
# attribution is already resolved in int_order_header.
ECOMMERCE_QUERY="$(dpf_render_query "${SCRIPT_DIR}/queries/ecommerce-headline-metrics.sql" \
    CATALOG ERGASTERION \
    MARTS_SCHEMA "${MARTS_SCHEMA}")" \
    || fail "could not render e-commerce headline query"

snow sql --format TABLE -c "${CONNECTION}" -q "${ECOMMERCE_QUERY}" | tee "${RUN_DIR}/ecommerce-headline-metrics.txt"
snow sql --format CSV -c "${CONNECTION}" -q "${ECOMMERCE_QUERY}" > "${RUN_DIR}/ecommerce-headline-metrics.csv"
echo

echo "-- e-commerce domain: customer entity resolution -- tri-source collapse + CRM-wins-contact survivorship --"
# The first row shows three source records collapsing to one golden customer. The
# second shows the CRM source winning the contact-attribute survivorship rule.
CUSTOMER_RESOLUTION_QUERY="$(dpf_render_query "${SCRIPT_DIR}/queries/ecommerce-resolution-metrics.sql" \
    CATALOG ERGASTERION \
    RAW_SCHEMA "${RAW_SCHEMA}" \
    RESOLUTION_SCHEMA "${RESOLUTION_SCHEMA}" \
    CANONICAL_SCHEMA "${CANONICAL_SCHEMA}")" \
    || fail "could not render customer-resolution query"

snow sql --format TABLE -c "${CONNECTION}" -q "${CUSTOMER_RESOLUTION_QUERY}" | tee "${RUN_DIR}/ecommerce-resolution-metrics.txt"
snow sql --format CSV -c "${CONNECTION}" -q "${CUSTOMER_RESOLUTION_QUERY}" > "${RUN_DIR}/ecommerce-resolution-metrics.csv"
echo

echo "-- investment domain: fund performance + hurdle --"
METRICS_QUERY="$(dpf_render_query "${SCRIPT_DIR}/queries/headline-metrics.sql" \
    CATALOG ERGASTERION \
    CALC_SCHEMA "${CALC_SCHEMA}" \
    MARTS_SCHEMA "${MARTS_SCHEMA}")" \
    || fail "could not render investment headline query"

snow sql --format TABLE -c "${CONNECTION}" -q "${METRICS_QUERY}" | tee "${RUN_DIR}/headline-metrics.txt"
snow sql --format CSV -c "${CONNECTION}" -q "${METRICS_QUERY}" > "${RUN_DIR}/headline-metrics.csv"
echo

echo "== Credit hygiene: suspend the build warehouse =="
snow sql -c "${CONNECTION}" -q "ALTER WAREHOUSE DPF_WH SUSPEND" || echo "(warehouse suspend skipped or failed; non-fatal)"
echo

echo "== Done in ${SECONDS}s =="
echo "   schema        : ${SCHEMA}"
echo "   run directory : ${RUN_DIR}"
echo "   transcript    : ${RUN_DIR}/transcript.log"
echo "   metrics (csv) : ${RUN_DIR}/ecommerce-headline-metrics.csv"
echo "                   ${RUN_DIR}/ecommerce-resolution-metrics.csv"
echo "                   ${RUN_DIR}/headline-metrics.csv"
