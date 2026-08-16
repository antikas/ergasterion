#!/usr/bin/env bash
# Wheel-mode arm of the offline validation chain: prove the engine works from a
# NON-EDITABLE install with no source checkout present. Builds the wheel, installs
# it into a scratch venv, and from a working directory OUTSIDE the source tree runs
# `ergasterion init`, declares the toy fixture domain, and runs `ergasterion emit`
# twice (the second in --check mode, so the emitted estate is byte-stable).
#
# Prerequisites:
#   * Python 3.11+ with the project dependencies installed (set PY, same contract
#     as validate_offline.sh).
#   * Network access on the FIRST run only: the wheel build's isolated backend and
#     the scratch venv's dependency install both resolve from the package index and
#     are served from pip's cache afterwards (same convention as the dbt_packages
#     fetch in validate_offline.sh).
set -uo pipefail

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd) || fail "cannot resolve the script directory"
REPO_ROOT=$(cd -- "$SCRIPT_DIR/.." && pwd) || fail "cannot resolve the repository root"

if [ -n "${PY:-}" ]; then
  command -v "$PY" >/dev/null 2>&1 || fail "PY does not name an executable: $PY"
elif command -v python3 >/dev/null 2>&1; then
  PY=$(command -v python3)
elif command -v python >/dev/null 2>&1; then
  PY=$(command -v python)
else
  fail "Python 3.11+ is required; set PY or add python3/python to PATH"
fi

WORK=$(mktemp -d) || fail "cannot create a scratch directory"
cleanup() { rm -rf "$WORK"; }
trap cleanup EXIT

echo "--- build the wheel"
"$PY" -m pip wheel "$REPO_ROOT" --no-deps -w "$WORK/dist" -q || fail "wheel build"
WHEEL=$(ls "$WORK"/dist/ergasterion_factory-*.whl 2>/dev/null | head -n1)
[ -n "$WHEEL" ] || fail "no ergasterion_factory wheel produced"

echo "--- scratch venv + non-editable install"
"$PY" -m venv "$WORK/venv" || fail "scratch venv creation"
if [ -x "$WORK/venv/Scripts/python.exe" ]; then
  VPY="$WORK/venv/Scripts/python.exe"
  ERG="$WORK/venv/Scripts/ergasterion.exe"
else
  VPY="$WORK/venv/bin/python"
  ERG="$WORK/venv/bin/ergasterion"
fi
"$VPY" -m pip install -q "$WHEEL" || fail "wheel install into the scratch venv"

cd "$WORK" || fail "cannot enter the scratch directory"

echo "--- init from the installed wheel, outside the source tree"
"$VPY" - "$REPO_ROOT" <<'PYEOF'
import sys
from pathlib import Path

import ergasterion

pkg = Path(ergasterion.__file__).resolve()
repo = Path(sys.argv[1]).resolve()
if repo in pkg.parents:
    raise SystemExit(f"factory imported from the source tree, {pkg} -- the wheel is not under test")

from ergasterion.init import scaffold

est = Path("estate").resolve()
scaffold(est)
for expect in (
    "dbt_project.yml",
    "packages.yml",
    "profiles/profiles.yml",
    "macros/cross_db.sql",
    "macros/survivorship.sql",
    "declarations/targets/interfaces.yml",
    "GETTING-STARTED.md",
):
    if not (est / expect).is_file():
        raise SystemExit(f"scaffold from the wheel is missing {expect}")
print("scaffold from the installed wheel complete")
PYEOF
[ $? -eq 0 ] || fail "init from the installed wheel"

echo "--- declare the toy fixture domain"
"$PY" - "$REPO_ROOT" <<'PYEOF'
import sys
from pathlib import Path

import yaml

repo = Path(sys.argv[1]).resolve()
sys.path.insert(0, str(repo / "tests" / "python"))
from test_emit import FIXTURE_DOMAIN, _fixture_declaration

est = Path("estate")
(est / "domains" / "fixture.yml").write_text(
    yaml.safe_dump(FIXTURE_DOMAIN, sort_keys=False), encoding="utf-8"
)
(est / "declarations" / "toysrc.yml").write_text(
    yaml.safe_dump(_fixture_declaration(), sort_keys=False), encoding="utf-8"
)
PYEOF
[ $? -eq 0 ] || fail "toy fixture declaration"

echo "--- emit from the installed wheel"
"$ERG" emit --estate-root estate || fail "emit from the installed wheel"
for expect in \
  estate/models/raw_vault/hubs/hub_alpha.sql \
  estate/models/raw_vault/links/link_alpha_beta.sql \
  estate/models/business_vault/bv_alpha_golden_record.sql \
  estate/models/entity_resolution/res_alpha.sql; do
  [ -f "$expect" ] || fail "expected emitted model missing: $expect"
done

echo "--- second emit in --check mode (byte-stable)"
"$ERG" emit --check --estate-root estate || fail "emitted estate is not byte-stable from the wheel"

echo "=== wheel-mode arm green: init + emit + gates run from a non-editable install ==="
