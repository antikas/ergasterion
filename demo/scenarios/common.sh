#!/usr/bin/env bash
# demo/scenarios/common.sh: the path and tool guards every demo lane shares.
#
# A lane sources this file, sets PY_BIN and DBT_BIN through resolve_tool, and then
# reaches the run directory through dpf_offline_runs_dir. Each function is the single
# home of the check it names, so every lane applies the same rule to the same paths.

# Print the message on standard error and stop the lane with a non-zero exit.
dpf_fail() { echo "DEMO ERROR: $*" >&2; exit 1; }

# Resolve one executable. An explicit value wins and must be executable; otherwise the
# first executable candidate wins.
dpf_resolve_tool() {
    local label="$1"
    local explicit_value="$2"
    shift 2

    if [ -n "${explicit_value}" ]; then
        [ -x "${explicit_value}" ] \
            || dpf_fail "${label} interpreter from environment is not executable: ${explicit_value}"
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
    dpf_fail "${label} interpreter not found: set ${label} or create the repo .venv"
}

# Print the canonical, symlink-free path of an existing filesystem entry.
dpf_canonical_path() {
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

# Require that a path is exactly the named direct physical child of a parent. This
# rejects symlink and junction redirects before any reset or output write.
dpf_assert_direct_physical_child() {
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

# Print the verified demo/offline-runs directory of a repository root, creating it when
# it is absent. Every runtime byte a demo lane writes lands under this directory, and
# Git ignores it.
dpf_offline_runs_dir() {
    local repo_root="$1"
    local demo_dir="${repo_root}/demo"
    [ -d "${demo_dir}" ] || dpf_fail "demo directory is missing: ${demo_dir}"
    dpf_assert_direct_physical_child "${demo_dir}" "${repo_root}" demo "demo directory" \
        || dpf_fail "demo directory failed physical identity"
    local offline_runs_dir="${demo_dir}/offline-runs"
    if [ -e "${offline_runs_dir}" ] || [ -L "${offline_runs_dir}" ]; then
        [ -d "${offline_runs_dir}" ] \
            || dpf_fail "offline-runs exists but is not a directory: ${offline_runs_dir}"
    else
        mkdir "${offline_runs_dir}" || dpf_fail "could not create verified offline-runs directory"
    fi
    dpf_assert_direct_physical_child "${offline_runs_dir}" "${demo_dir}" offline-runs \
        "offline-runs directory" \
        || dpf_fail "offline-runs directory failed physical identity"
    printf '%s' "${offline_runs_dir}"
}
