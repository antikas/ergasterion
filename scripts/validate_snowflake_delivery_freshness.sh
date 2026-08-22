#!/usr/bin/env bash
# Isolated Snowflake DEV proof for source-delivery freshness.
#
# Creates one dedicated schema, materialises only the synthetic fixture relations,
# and runs targeted `dbt source freshness --select` through dbt Core's stock
# collect_freshness_custom_sql path. It never seeds estate/production relations.
#
# Exit 0: proof green.
# Exit 3: missing/invalid Snowflake DEV credentials (clean environment park).
# Exit 1: the custom-path proof failed.
set -uo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd) || { echo "FAIL: cannot resolve the script directory" >&2; exit 1; }
REPO_ROOT=$(git -C "$SCRIPT_DIR/.." rev-parse --show-toplevel) || { echo "FAIL: run this script from a Git checkout" >&2; exit 1; }
cd "$REPO_ROOT" || { echo "FAIL: cannot enter $REPO_ROOT" >&2; exit 1; }

fail() { echo "FAIL: $*" >&2; exit 1; }
park() { echo "ENV-PARK: $*" >&2; exit 3; }

if [ -n "${PY:-}" ]; then
  command -v "$PY" >/dev/null 2>&1 || fail "PY does not name an executable: $PY"
elif [ -x "$REPO_ROOT/.venv/Scripts/python.exe" ]; then
  PY="$REPO_ROOT/.venv/Scripts/python.exe"
elif [ -x "$REPO_ROOT/.venv/bin/python" ]; then
  PY="$REPO_ROOT/.venv/bin/python"
else
  PY=$(command -v python3 2>/dev/null || command -v python 2>/dev/null || true)
fi
[ -n "$PY" ] || fail "no Python interpreter available"

if [ -n "${DBT:-}" ]; then
  command -v "$DBT" >/dev/null 2>&1 || fail "DBT does not name an executable: $DBT"
elif [ -x "$REPO_ROOT/.venv/Scripts/dbt.exe" ]; then
  DBT="$REPO_ROOT/.venv/Scripts/dbt.exe"
elif [ -x "$REPO_ROOT/.venv/bin/dbt" ]; then
  DBT="$REPO_ROOT/.venv/bin/dbt"
else
  DBT=$(command -v dbt 2>/dev/null || true)
fi
[ -n "$DBT" ] || fail "dbt is required"

[ -n "${DPF_SF_ACCOUNT:-}" ] || park "DPF_SF_ACCOUNT is not set"
[ -n "${DPF_SF_USER:-}" ] || park "DPF_SF_USER is not set"
[ -n "${DPF_SF_KEY_PATH:-}" ] || park "DPF_SF_KEY_PATH is not set"
[ -f "${DPF_SF_KEY_PATH}" ] || park "DPF_SF_KEY_PATH does not name a file: $DPF_SF_KEY_PATH"

SCHEMA="${DPF_SF_FRESHNESS_SCHEMA:-DPF_SOURCE_DELIVERY}"
export DPF_SF_SCHEMA="$SCHEMA"
export DPF_SF_DB="${DPF_SF_DB:-ERGASTERION}"
export DPF_SF_WH="${DPF_SF_WH:-DPF_WH}"
export DPF_SF_ROLE="${DPF_SF_ROLE:-DPF_BUILDER}"

PROJECT=$(mktemp -d "${TMPDIR:-/tmp}/dpf-sf-freshness.XXXXXX") || fail "cannot create a temporary project directory"
drop_isolated_schema() {
  [ -d "$PROJECT" ] || return 0
  "$PY" - "$DBT" "$REPO_ROOT" "$PROJECT" <<'PYEOF' || true
import os, subprocess, sys, textwrap
from pathlib import Path
dbt, repo, project = sys.argv[1:4]
macro = Path(project) / "macros" / "dpf_drop_isolated_schema.sql"
if not Path(project).is_dir():
    raise SystemExit(0)
macro.write_text(
    textwrap.dedent(
        """\
        {% macro dpf_drop_isolated_schema() %}
          {% set sql %}drop schema if exists {{ target.database }}.{{ target.schema }} cascade{% endset %}
          {% do run_query(sql) %}
        {% endmacro %}
        """
    ),
    encoding="utf-8",
    newline="\n",
)
subprocess.run(
    [dbt, "run-operation", "dpf_drop_isolated_schema",
     "--profiles-dir", os.path.join(repo, "profiles"),
     "--project-dir", project, "-t", "snowflake"],
    capture_output=True,
    text=True,
)
PYEOF
}

cleanup() {
  drop_isolated_schema
  rm -rf "$PROJECT"
}
trap cleanup EXIT

echo "=== isolated Snowflake DEV freshness project: $PROJECT (schema $SCHEMA) ==="
"$PY" tests/python/test_source_delivery_dbt.py --write-project "$PROJECT" || fail "could not materialise the isolated fixture project"

echo "=== snowflake debug ($DPF_SF_DB / $SCHEMA) ==="
DEBUG_LOG=$(mktemp) || fail "cannot create a debug log"
if ! "$DBT" debug --project-dir "$PROJECT" --profiles-dir "$REPO_ROOT/profiles" -t snowflake >"$DEBUG_LOG" 2>&1; then
  cat "$DEBUG_LOG"
  park "cannot connect to Snowflake DEV"
fi
cat "$DEBUG_LOG"

echo "=== dbt run synthetic relations into $DPF_SF_DB.$SCHEMA ==="
if ! "$DBT" run --project-dir "$PROJECT" --profiles-dir "$REPO_ROOT/profiles" --no-partial-parse -t snowflake \
    --select dpf_synth_landing dpf_synth_stream_status dpf_synth_published_ledger dpf_synth_active_alias; then
  park "isolated Snowflake materialisation failed"
fi

echo "=== dbt source freshness --select source:fresh_ok.accounts (custom SQL path) ==="
FRESH_LOG=$(mktemp) || fail "cannot create a freshness log"
if ! "$DBT" --debug source freshness --project-dir "$PROJECT" --profiles-dir "$REPO_ROOT/profiles" -t snowflake \
    --select source:fresh_ok.accounts >"$FRESH_LOG" 2>&1; then
  cat "$FRESH_LOG"
  if grep -Eqi 'could not connect|250001|incorrect username|private key|authentication' "$FRESH_LOG"; then
    park "Snowflake DEV connection failed during source freshness"
  fi
  fail "targeted source freshness failed (see log above)"
fi
cat "$FRESH_LOG"
grep -q 'with source_query as' "$FRESH_LOG" \
  || fail "freshness did not wrap loaded_at_query in the stock source_query scalar subquery"
grep -q '(select \* from source_query)' "$FRESH_LOG" \
  || fail "freshness did not execute the stock collect_freshness_custom_sql scalar subquery"

echo "=== dbt source freshness --select source:sched_only.accounts must skip native freshness ==="
SKIP_LOG=$(mktemp) || fail "cannot create a skip log"
"$DBT" source freshness --project-dir "$PROJECT" --profiles-dir "$REPO_ROOT/profiles" -t snowflake \
    --select source:sched_only.accounts >"$SKIP_LOG" 2>&1 || true
cat "$SKIP_LOG"
if grep -Eq 'with source_query as|select \* from source_query' "$SKIP_LOG"; then
  fail "schedule-only source executed native freshness"
fi

drop_isolated_schema
trap - EXIT
rm -rf "$PROJECT"
echo "=== isolated Snowflake DEV freshness proof green ==="
