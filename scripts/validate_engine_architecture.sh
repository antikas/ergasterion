#!/usr/bin/env bash
# The engine architecture acceptance run: the thirteen checks of
# docs/architecture/engine-architecture.md section 14, as one deterministic offline
# script printing one line per check.
#
# Each check is stated there as a condition the design is met by, "checkable by a
# deterministic run". This script is that run. It reaches no network and no warehouse
# account: the one adapter it executes is DuckDB, the estate's declared reference
# adapter, against a scratch copy of a fixture estate.
#
# Prerequisites:
#   * Bash and Python 3.11+ with the project dependencies installed.
#   * dbt Core with the DuckDB adapter, for the one executed check (check 2).
#
# Set PY to select a specific interpreter. When unset, the repository's own .venv is
# used before anything on PATH: the driver imports the engine and its pinned runtime
# dependencies, and an interpreter that happens to be on PATH carries neither.
#
# Options are passed straight through to the driver:
#   --only N            run one check by its architecture section 14 number;
#   --estate-root PATH  the worked estate to check (default: this repository);
#   --fixtures-root PATH   the fixture estates directory;
#   --engine-root PATH  the engine package directory.
#
# The last three exist so a red proof can point one check at a scratch copy carrying
# an injected violation without copying the whole tree; tests/python/test_engine_acceptance.py
# uses exactly that to drive this script red, once per check.
#
# Output: one `check <n>: OK|FAIL <title> -- <evidence>` line per check, then the
# machine-readable marker
#   ENGINE_ACCEPTANCE_CHECKS=<n> ENGINE_ACCEPTANCE_FAILURES=<n>
# Exit status is 0 only when every selected check passed.
set -uo pipefail

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd) || fail "cannot resolve the script directory"
REPO_ROOT=$(cd -- "$SCRIPT_DIR/.." && pwd) || fail "cannot resolve the repository root"
cd "$REPO_ROOT" || fail "cannot enter the repository root"

if [ -n "${PY:-}" ]; then
  command -v "$PY" >/dev/null 2>&1 || fail "PY does not name an executable: $PY"
elif [ -x "$REPO_ROOT/.venv/Scripts/python.exe" ]; then
  PY="$REPO_ROOT/.venv/Scripts/python.exe"
elif [ -x "$REPO_ROOT/.venv/bin/python" ]; then
  PY="$REPO_ROOT/.venv/bin/python"
elif command -v python3 >/dev/null 2>&1; then
  PY=$(command -v python3)
elif command -v python >/dev/null 2>&1; then
  PY=$(command -v python)
else
  fail "Python 3.11+ is required; set PY, build the repository .venv, or add python3/python to PATH"
fi

"$PY" -c 'import sys; raise SystemExit(0 if sys.version_info >= (3, 11) else 1)' \
  || fail "Python 3.11 or newer is required: $PY"

echo "=== engine architecture acceptance: the thirteen checks of architecture section 14 ==="
"$PY" tests/python/engine_acceptance.py "$@"
STATUS=$?
if [ "$STATUS" -ne 0 ]; then
  echo "=== engine architecture acceptance: RED ==="
  exit "$STATUS"
fi
echo "=== engine architecture acceptance: green ==="
