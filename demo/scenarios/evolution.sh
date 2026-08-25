#!/usr/bin/env bash
# demo/scenarios/evolution.sh: the estate-evolution lane of the account-free demo.
#
# Estate evolution is how a generated warehouse absorbs a new payload field while it
# keeps every version it already stores. This lane copies the estate into a scratch
# directory under demo/offline-runs/, builds it on DuckDB, adds one payload field
# together with a data change in a hashdiff-basis column, rebuilds without a full
# refresh, and checks the outcome by machine. Every check that fails stops the lane
# with a non-zero exit. The scratch estate is deleted at the end and
# demo/offline-runs/ is empty when the lane closes.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd -P)"
cd "${REPO_ROOT}"
. "${SCRIPT_DIR}/common.sh"

PY_BIN="$(dpf_resolve_tool PY "${PY:-}" \
    "${REPO_ROOT}/.venv/bin/python" \
    "${REPO_ROOT}/.venv/Scripts/python.exe" \
    "${REPO_ROOT}/.venv/Scripts/python")"
DBT_BIN="$(dpf_resolve_tool DBT "${DBT:-}" \
    "${REPO_ROOT}/.venv/bin/dbt" \
    "${REPO_ROOT}/.venv/Scripts/dbt.exe" \
    "${REPO_ROOT}/.venv/Scripts/dbt")"

echo "== Ergasterion: estate-evolution scenario =="
echo "   repository : ${REPO_ROOT}"
echo

# The offline lane must be unable to inherit warehouse credentials accidentally.
while IFS='=' read -r env_name _; do
    case "${env_name}" in DPF_SF_*) unset "${env_name}" ;; esac
done < <(env)
if env | grep -q '^DPF_SF_'; then
    dpf_fail "Snowflake environment variables remained after offline credential scrub"
fi
echo "Snowflake environment variables: 0"

echo "== Pinned runtime preflight =="
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

OFFLINE_RUNS_DIR="$(dpf_offline_runs_dir "${REPO_ROOT}")" \
    || dpf_fail "could not verify the offline-runs directory"

DPF_DBT_BIN="${DBT_BIN}" "${PY_BIN}" "${SCRIPT_DIR}/evolution.py" \
    --repo-root "${REPO_ROOT}" \
    --offline-runs "${OFFLINE_RUNS_DIR}"
SCENARIO_STATUS=$?
[ "${SCENARIO_STATUS}" -eq 0 ] || exit "${SCENARIO_STATUS}"

REMAINING="$(find "${OFFLINE_RUNS_DIR}" -mindepth 1 | wc -l | tr -d ' ')"
[ "${REMAINING}" = "0" ] \
    || dpf_fail "demo/offline-runs/ holds ${REMAINING} leftover entr(ies) after the scenario"
echo "demo/offline-runs/ entries at close: 0"
echo "== Estate-evolution scenario passed in ${SECONDS}s =="
