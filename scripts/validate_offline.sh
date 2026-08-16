#!/usr/bin/env bash
# Validate the complete structural lane without connecting to a warehouse.
#
# Prerequisites:
#   * Git and Bash.
#   * Python 3.11+ with the project dependencies installed.
#   * dbt Core with the Snowflake, BigQuery, and DuckDB adapters installed.
#   * Network access on the first run only, if dbt_packages/ has not been populated.
#
# Set PY and/or DBT to select specific executables. When unset, Python resolves from
# python3 (then python) on PATH and dbt resolves from PATH.
set -uo pipefail

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd) || fail "cannot resolve the script directory"
REPO_ROOT=$(cd -- "$SCRIPT_DIR/.." && pwd) || fail "cannot resolve the repository root"
cd "$REPO_ROOT" || fail "cannot enter the repository root"

command -v git >/dev/null 2>&1 || fail "git is required on PATH"
git rev-parse --is-inside-work-tree >/dev/null 2>&1 || fail "run this script from a Git checkout"

if [ -n "${PY:-}" ]; then
  command -v "$PY" >/dev/null 2>&1 || fail "PY does not name an executable: $PY"
elif command -v python3 >/dev/null 2>&1; then
  PY=$(command -v python3)
elif command -v python >/dev/null 2>&1; then
  PY=$(command -v python)
else
  fail "Python 3.11+ is required; set PY or add python3/python to PATH"
fi

if [ -n "${DBT:-}" ]; then
  command -v "$DBT" >/dev/null 2>&1 || fail "DBT does not name an executable: $DBT"
elif command -v dbt >/dev/null 2>&1; then
  DBT=$(command -v dbt)
else
  fail "dbt is required; set DBT or add dbt to PATH"
fi

"$PY" -c 'import sys; raise SystemExit(0 if sys.version_info >= (3, 11) else 1)' \
  || fail "Python 3.11 or newer is required: $PY"
[ -f dbt_project.yml ] || fail "dbt_project.yml is missing from the repository root"
[ -f profiles/profiles.yml ] || fail "profiles/profiles.yml is missing"

# DuckDB's local database and sidecars belong under target/. Create it before the
# first dbt command so a fresh checkout never relies on an adapter-created parent.
mkdir -p target || fail "cannot create target/ for DuckDB runtime artefacts"

# The checkout must use the pinned project adapter, never an older dbt-duckdb
# exposed by PATH. Both versions are load-bearing: a parse/build under another
# plugin release is not this validator's contract.
DBT_VERSION=$("$DBT" --version) || fail "cannot read dbt adapter versions: $DBT"
printf '%s\n' "$DBT_VERSION"
grep -Eq 'installed:[[:space:]]+1\.11\.12' <<< "$DBT_VERSION" \
  || fail "dbt Core 1.11.12 is required for the offline validator"
grep -Eq 'duckdb:[[:space:]]+1\.11\.0' <<< "$DBT_VERSION" \
  || fail "dbt-duckdb 1.11.0 is required for the offline validator"

echo "Using Python: $PY"
echo "Using dbt: $DBT"

echo "=== offline gate: emit (byte-stable) + emitter test scripts + three-target parse + DuckDB build ==="
echo "=== scaffold package-data gate: ergasterion/scaffold/ byte-matches its sources ==="
"$PY" ergasterion/sync_scaffold.py --check || fail "packaged scaffold drifted from its sources (run: python ergasterion/sync_scaffold.py)"

EMIT_OUTPUT=$("$PY" ergasterion/emit.py --check)
EMIT_STATUS=$?
printf '%s\n' "$EMIT_OUTPUT"

# Structural, machine-readable orphan signal: emit.py --check prints a stable
# ORPHANS=<n> marker on every run. Read that marker, never the prose message alone
# (prose can be reworded without the check drifting with it). Its absence is itself
# a failure -- an emit.py that stops printing the marker must not silently pass.
ORPHAN_MARKER=$(grep -oE '^ORPHANS=[0-9]+' <<< "$EMIT_OUTPUT" | tail -n1)
[ -n "$ORPHAN_MARKER" ] || fail "ergasterion/emit.py --check produced no ORPHANS=<n> marker line -- structural orphan signal missing"
ORPHAN_COUNT=${ORPHAN_MARKER#ORPHANS=}

# emit.py returns 1 for two independent causes -- a dialect-lint offense and a
# byte-stability diff -- named separately here so a failure message points at the
# right one instead of a blanket "not byte-stable".
if [ "$EMIT_STATUS" -ne 0 ] && grep -q '^dialect-lint FAIL' <<< "$EMIT_OUTPUT"; then
  fail "dialect-lint failed: construct(s) incompatible with a declared adapter found in model/test SQL (see emit --check output above)"
fi
if [ "$ORPHAN_COUNT" -gt 0 ]; then
  fail "orphaned generated model output would be deleted (ORPHANS=$ORPHAN_COUNT; see emit --check output above)"
fi
if [ "$EMIT_STATUS" -ne 0 ]; then
  fail "generated model output is not byte-stable"
fi
"$PY" tests/python/test_emit.py || exit 1
"$PY" tests/python/test_dialect_lint.py || exit 1
"$PY" tests/python/test_er_coverage_gate.py || exit 1
"$PY" tests/python/test_strict_undefined.py || exit 1
"$PY" ergasterion/structure_gate.py || fail "structure gate: a declared target budget is breached (see output above)"
"$PY" tests/python/test_structure_gate.py || exit 1
"$PY" streamlit/test_scoring_config.py || exit 1
[ -d dbt_packages ] || "$DBT" deps --profiles-dir profiles || exit 1
"$DBT" parse --profiles-dir profiles --no-partial-parse -t snowflake || exit 1
"$DBT" parse --profiles-dir profiles --no-partial-parse -t bigquery || exit 1
"$PY" ergasterion/dialect_lint.py --target duckdb || exit 1
"$DBT" parse --profiles-dir profiles --no-partial-parse -t duckdb || exit 1

if [ "${DPF_SIMULATE_NO_DUCKDB:-0}" = "1" ]; then
  echo "=== DuckDB build skipped: DPF_SIMULATE_NO_DUCKDB=1 ==="
else
  echo "=== DuckDB build: full local estate ==="
  "$DBT" build --profiles-dir profiles -t duckdb || exit 1
fi

echo "=== package-mode smoke: editable install + entry point + tree-identity (offline) ==="
"$PY" tests/python/test_package_mode.py || { echo "FAIL: package-mode smoke"; exit 1; }

echo "=== consumer scaffold suite: init structure + toy-domain end-to-end (offline) ==="
"$PY" tests/python/test_init.py || { echo "FAIL: consumer scaffold suite"; exit 1; }

echo "=== DDL import seeder self-tests (offline) ==="
"$PY" tests/python/test_import_ddl.py || { echo "FAIL: DDL import seeder self-tests"; exit 1; }

echo "=== ODCS import seeder self-tests (offline) ==="
"$PY" tests/python/test_import_odcs.py || { echo "FAIL: ODCS import seeder self-tests"; exit 1; }

echo "=== ODCS contract gate: schema-validate + byte-stable (never-hand-edited) ==="
"$PY" ergasterion/emit_contracts.py --check || { echo "FAIL: ODCS contracts drifted, were hand-edited, or are schema-invalid"; exit 1; }
"$PY" tests/python/test_emit_contracts.py || exit 1

echo "=== ODPS (Bitol) descriptor gate: schema-validate + byte-stable (never-hand-edited) ==="
"$PY" ergasterion/emit_odps.py --check || { echo "FAIL: ODPS (Bitol) descriptors drifted, were hand-edited, or are schema-invalid"; exit 1; }
"$PY" tests/python/test_emit_odps.py || exit 1

echo "=== property-graph projection gate: byte-stable + structural tests ==="
"$PY" ergasterion/emit_graph.py --check || { echo "FAIL: property-graph artefacts drifted or were hand-edited"; exit 1; }
"$PY" tests/python/test_emit_graph.py || exit 1
"$PY" tests/python/test_graph_model.py || { echo "FAIL: graph-model IR self-tests"; exit 1; }

echo "=== wheel-mode arm: init + emit from a non-editable install (network on first run only) ==="
PY="$PY" bash "$SCRIPT_DIR/validate_wheel.sh" || fail "wheel-mode arm (see output above)"

echo "=== offline validator green ==="
