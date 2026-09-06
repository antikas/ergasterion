#!/usr/bin/env bash
# demo/landing-ingestion/run_landing_demo.sh: account-free, network-free Landing walkthrough.
#
# Runs the operator CLI against a fresh, temporary local runtime root created and
# destroyed for this run only -- never against a checked-in database file, and never
# over the network. No warehouse account and no dbt project are needed: Landing reads
# and writes through the local reference platform (SQLite state, DuckDB projection)
# directly. See demo/landing-ingestion/landing_demo.py for the full narration and
# docs/architecture/bronze-ingestion.md for the mechanism it exercises.
#
# Usage:
#   bash demo/landing-ingestion/run_landing_demo.sh                              # all three scenarios
#   bash demo/landing-ingestion/run_landing_demo.sh normal-publication
#   bash demo/landing-ingestion/run_landing_demo.sh acceptance-incomplete-snapshot
#   bash demo/landing-ingestion/run_landing_demo.sh backup-restore
#
# Set PY to select an explicit interpreter; otherwise the repository .venv is used.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd -P)"
cd "${REPO_ROOT}"

fail() { echo "LANDING DEMO ERROR: $*" >&2; exit 1; }

resolve_python() {
    if [ -n "${PY:-}" ]; then
        case "${PY}" in
            [A-Za-z]:\\*)
                if command -v cygpath >/dev/null 2>&1; then
                    PY="$(cygpath -u "${PY}")"
                elif command -v wslpath >/dev/null 2>&1; then
                    PY="$(wslpath -u "${PY}")"
                fi
                ;;
        esac
        if [ -x "${PY}" ]; then
            printf '%s' "${PY}"
            return
        fi
        if command -v "${PY}" >/dev/null 2>&1; then
            command -v "${PY}"
            return
        fi
        fail "PY interpreter from environment is not executable: ${PY}"
    fi
    local candidate
    for candidate in "${REPO_ROOT}/.venv/bin/python" "${REPO_ROOT}/.venv/Scripts/python.exe" "${REPO_ROOT}/.venv/Scripts/python"; do
        if [ -x "${candidate}" ]; then
            printf '%s' "${candidate}"
            return
        fi
    done
    for candidate in python3 python python.exe; do
        if command -v "${candidate}" >/dev/null 2>&1; then
            command -v "${candidate}"
            return
        fi
    done
    fail "Python interpreter not found: activate the project environment or set PY"
}

PY_BIN="$(resolve_python)"

python_path() {
    local path="$1"
    case "${PY_BIN}" in
        *.exe|*.EXE)
            if command -v cygpath >/dev/null 2>&1; then
                cygpath -w "${path}"
            elif command -v wslpath >/dev/null 2>&1; then
                wslpath -w "${path}"
            else
                fail "a Windows Python interpreter needs cygpath or wslpath to resolve ${path}"
            fi
            ;;
        *) printf '%s' "${path}" ;;
    esac
}

DEMO_PY="$(python_path "${SCRIPT_DIR}/landing_demo.py")"

SCENARIO="${1:-all}"
case "${SCENARIO}" in
    all|normal-publication|acceptance-incomplete-snapshot|backup-restore) ;;
    *)
        fail "unknown scenario: ${SCENARIO} (expected all, normal-publication, acceptance-incomplete-snapshot, or backup-restore)"
        ;;
esac

exec "${PY_BIN}" "${DEMO_PY}" --scenario "${SCENARIO}"
