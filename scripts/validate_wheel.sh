#!/usr/bin/env bash
# Wheel-mode arm of the offline validation chain: prove the engine works from a
# NON-EDITABLE install with no source checkout present. Builds the wheel, installs
# it into a scratch venv, then installs the local-ingestion package set (duckdb,
# dbt-core, dbt-duckdb) by name at their pinned versions, asserts every pinned
# dependency version, proves DuckDB connects/queries and dbt reports the pinned
# core + adapter versions (the "ingestion and dbt proof" an empty venv owes before
# anything else), and then from a working directory OUTSIDE the source tree runs
# `ergasterion init`, declares the toy fixture domain, and runs `ergasterion emit`
# twice (the second in --check mode, so the emitted estate is byte-stable).
#
# The final section proves the SHIPPED `--binding runtime/local.yml` end to end against
# the scaffold's own reference example (source=reference, table=orders,
# ergasterion.sync_scaffold.reference_contract() -- the exact object the shipped
# binding's digest is computed from, imported from the wheel, never re-typed by hand):
# materialize the pinned dbt Hub packages from DPF_DBT_PACKAGE_CACHE (no `dbt deps`, no
# network), plan/register/activate the contract and deployment, ingest one file
# (one accepted row, one quarantined row), read back status/inspect/quarantine, create a
# local backup to a temporary destination OUTSIDE the project/runtime root, delete the
# runtime root, restore from that backup, and finally run `dbt parse`/`dbt build -t duckdb`
# against the resulting estate with the materialized packages alone.
#
# Prerequisites:
#   * Python 3.11+ with the project dependencies installed (set PY, same contract
#     as validate_offline.sh).
#   * DPF_WHEELHOUSE: a directory of wheels. Every install below -- the wheel build's own
#     backend, the wheel itself, and the scratch venv's local-ingestion packages -- resolves
#     ONLY through `pip --no-index --find-links "$DPF_WHEELHOUSE"`, offline and deterministic.
#     No step in this script contacts a package index.
#   * DPF_DBT_PACKAGE_CACHE: a directory holding a pre-fetched `dbt_utils/` and
#     `automate_dv/` (this project's two packages.yml Hub packages) plus their own
#     `package-lock.yml`. The reference-journey dbt proof materializes from this cache by
#     plain file copy -- it never runs `dbt deps` and never contacts the Hub.
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

[ -n "${DPF_WHEELHOUSE:-}" ] || fail "DPF_WHEELHOUSE is required -- the wheel-mode arm installs offline only"
[ -d "$DPF_WHEELHOUSE" ] || fail "DPF_WHEELHOUSE does not name a directory: $DPF_WHEELHOUSE"

[ -n "${DPF_DBT_PACKAGE_CACHE:-}" ] || fail "DPF_DBT_PACKAGE_CACHE is required -- the reference-journey dbt proof materializes packages offline only"
[ -d "$DPF_DBT_PACKAGE_CACHE" ] || fail "DPF_DBT_PACKAGE_CACHE does not name a directory: $DPF_DBT_PACKAGE_CACHE"
[ -d "$DPF_DBT_PACKAGE_CACHE/dbt_utils" ] || fail "DPF_DBT_PACKAGE_CACHE is missing dbt_utils/: $DPF_DBT_PACKAGE_CACHE"
[ -d "$DPF_DBT_PACKAGE_CACHE/automate_dv" ] || fail "DPF_DBT_PACKAGE_CACHE is missing automate_dv/: $DPF_DBT_PACKAGE_CACHE"

WORK=$(mktemp -d) || fail "cannot create a scratch directory"
cleanup() { rm -rf "$WORK"; }
trap cleanup EXIT

echo "--- build the wheel"
# --no-build-isolation: the backend (setuptools, pinned in [build-system].requires) is borrowed
# from $PY's own already-bootstrapped environment rather than fetched into an ephemeral isolated
# build env from the package index -- this is the ONLY way this build step stays offline.
"$PY" -m pip wheel "$REPO_ROOT" --no-deps --no-build-isolation -w "$WORK/dist" -q \
  || fail "wheel build (offline, --no-build-isolation borrowing \$PY's own setuptools)"
WHEEL=$(ls "$WORK"/dist/ergasterion_factory-*.whl 2>/dev/null | head -n1)
[ -n "$WHEEL" ] || fail "no ergasterion_factory wheel produced"

echo "--- scratch venv + non-editable install"
"$PY" -m venv "$WORK/venv" || fail "scratch venv creation"
if [ -x "$WORK/venv/Scripts/python.exe" ]; then
  VPY="$WORK/venv/Scripts/python.exe"
  ERG="$WORK/venv/Scripts/ergasterion.exe"
  VDBT="$WORK/venv/Scripts/dbt.exe"
else
  VPY="$WORK/venv/bin/python"
  ERG="$WORK/venv/bin/ergasterion"
  VDBT="$WORK/venv/bin/dbt"
fi
"$VPY" -m pip install -q --no-index --find-links "$DPF_WHEELHOUSE" "$WHEEL" \
  || fail "offline wheel install into the scratch venv via --no-index --find-links $DPF_WHEELHOUSE"
# The local-ingestion extra's packages, installed by name rather than as a
# "<wheel-path>[local-ingestion]" argument: a bracket-suffixed local-file argument does not
# survive Git Bash's native-executable path translation reliably on Windows, where $WHEEL is a
# POSIX-style mktemp path being handed to a native (non-MSYS) python.exe.
"$VPY" -m pip install -q --no-index --find-links "$DPF_WHEELHOUSE" "duckdb==1.5.5" "dbt-core==1.11.12" "dbt-duckdb==1.11.0" \
  || fail "offline local-ingestion package install into the scratch venv via --no-index --find-links $DPF_WHEELHOUSE"

echo "--- empty-venv pinned dependency versions (before ingestion and dbt proof)"
"$VPY" - <<'PYEOF' || fail "the wheel-installed scratch venv does not resolve the pinned dependency versions"
import sys
from importlib import metadata as im

want = {
    "pydantic": "2.13.4",
    "rfc8785": "0.1.4",
    "tzdata": "2026.2",
    "cryptography": "49.0.0",
    "duckdb": "1.5.5",
    "dbt-core": "1.11.12",
    "dbt-duckdb": "1.11.0",
}
bad = []
for name, expected in want.items():
    try:
        got = im.version(name)
    except im.PackageNotFoundError:
        bad.append(f"{name}: not installed (want {expected})")
        continue
    if got != expected:
        bad.append(f"{name}: installed {got}, want {expected}")
if bad:
    print("PINNED VERSION MISMATCH:")
    for line in bad:
        print(" -", line)
    sys.exit(1)
print("pinned versions OK: " + ", ".join(f"{k}=={v}" for k, v in want.items()))
PYEOF

echo "--- empty-venv local-ingestion proof: DuckDB connects and queries"
"$VPY" - <<'PYEOF' || fail "DuckDB did not connect/query from the wheel-installed scratch venv"
import duckdb

con = duckdb.connect()
row = con.execute("select 1").fetchone()
if row != (1,):
    raise SystemExit(f"unexpected DuckDB query result: {row!r}")
print("DuckDB connect + query OK")
PYEOF

echo "--- empty-venv local-ingestion proof: dbt reports the pinned core + duckdb adapter versions"
DBT_VERSION_RAW=$("$VDBT" --version) || fail "cannot read dbt versions from the wheel-installed scratch venv"
printf '%s\n' "$DBT_VERSION_RAW"
grep -Eq 'installed:[[:space:]]+1\.11\.12' <<< "$DBT_VERSION_RAW" \
  || fail "dbt Core 1.11.12 is required from the wheel-installed scratch venv"
grep -Eq 'duckdb:[[:space:]]+1\.11\.0' <<< "$DBT_VERSION_RAW" \
  || fail "dbt-duckdb 1.11.0 is required from the wheel-installed scratch venv"

cd "$WORK" || fail "cannot enter the scratch directory"

echo "--- framework, translators and ingestion import from the installed wheel"
"$VPY" - "$REPO_ROOT" <<'PYEOF'
import json
import sys
from pathlib import Path

import ergasterion.framework as fw
import ergasterion.ingestion as ing
import ergasterion.ingestion.records as ing_records
import ergasterion.translators as tr

repo = Path(sys.argv[1]).resolve()
for module, name in (
    (fw, "ergasterion.framework"),
    (tr, "ergasterion.translators"),
    (ing, "ergasterion.ingestion"),
    (ing_records, "ergasterion.ingestion.records"),
):
    pkg_file = Path(module.__file__).resolve()
    if repo in pkg_file.parents:
        raise SystemExit(f"{name} imported from the source tree, {pkg_file} -- the wheel is not under test")

from ergasterion.framework import Layer, compute_plan_digest, resolve

plan = resolve(Layer.BRONZE)
digest = compute_plan_digest(plan)
if len(digest) != 64:
    raise SystemExit(f"unexpected digest length from the wheel-installed framework: {digest!r}")

# ergasterion.ingestion.records contributes 153 of the IDL's 224 records directly, and
# re-exports the other 71 from ergasterion.framework.bronze_contract and
# ergasterion.framework.runtime_binding into ALL_RECORD_MODELS, the aggregate the assert
# below checks. This step constructs one of its closed models and confirms it round-trips,
# from the wheel install.
unit = ing_records.UnitResult(ok=True)
if unit.ok is not True:
    raise SystemExit("ergasterion.ingestion.records.UnitResult did not round-trip from the wheel")
if len(ing_records.ALL_RECORD_MODELS) != 224:
    raise SystemExit(
        f"expected 224 records in ALL_RECORD_MODELS from the wheel, got {len(ing_records.ALL_RECORD_MODELS)}"
    )

# The two generated schema/equivalence JSON files are package-data. This step confirms they
# are present and load as JSON from the installed wheel.
schemas_dir = Path(fw.__file__).resolve().parent.parent / "schemas"
for filename in ("bronze-product-v1.schema.json", "bronze-portable-idl-equivalence.json"):
    schema_path = schemas_dir / filename
    if not schema_path.is_file():
        raise SystemExit(f"{filename} is missing from the wheel-installed ergasterion/schemas/ directory")
    if repo in schema_path.resolve().parents:
        raise SystemExit(f"{filename} loaded from the source tree, {schema_path} -- the wheel is not under test")
    json.loads(schema_path.read_text(encoding="utf-8"))

print(
    "ergasterion.framework, ergasterion.translators and ergasterion.ingestion import from "
    f"the wheel (224 records, schema + equivalence JSON present); Bronze digest {digest}"
)
PYEOF
[ $? -eq 0 ] || fail "framework/translators/ingestion import from the installed wheel"

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
    "estate.yml",
    ".gitignore",
    "runtime/local.yml",
    "GETTING-STARTED.md",
):
    if not (est / expect).is_file():
        raise SystemExit(f"scaffold from the wheel is missing {expect}")

gitignore_lines = [
    line.strip() for line in (est / ".gitignore").read_text(encoding="utf-8").splitlines()
    if line.strip() and not line.strip().startswith("#")
]
if gitignore_lines != ["runtime/data/"]:
    raise SystemExit(f"scaffold .gitignore must ignore only runtime/data/, got: {gitignore_lines}")
if (est / "target").exists() or (est / "logs").exists() or (est / "dbt_packages").exists():
    raise SystemExit("a freshly scaffolded estate must carry no root target/, logs/ or dbt_packages/")
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

# The one manual step GETTING-STARTED.md documents: a raw seed for the toy source, plus
# its authored seeds: column_types block, so the later dbt build proof has real data.
(est / "seeds" / "raw_toysrc_things.csv").write_text(
    "id,alpha_name,alpha_code,beta_name\n"
    "1,Alpha One,A1,Beta One\n"
    "2,Alpha Two,A2,Beta Two\n",
    encoding="utf-8",
)
project = yaml.safe_load((est / "dbt_project.yml").read_text(encoding="utf-8"))
project["seeds"] = {
    "ergasterion": {
        "+quote_columns": False,
        "raw_toysrc_things": {
            "+column_types": {
                "id": "string", "alpha_name": "string", "alpha_code": "string", "beta_name": "string",
            },
        },
    },
}
(est / "dbt_project.yml").write_text(yaml.safe_dump(project, sort_keys=False), encoding="utf-8")
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

echo "--- materialize pinned dbt Hub packages from DPF_DBT_PACKAGE_CACHE (no dbt deps, no network)"
"$PY" - "$DPF_DBT_PACKAGE_CACHE" "$REPO_ROOT" <<'PYEOF' || fail "DPF_DBT_PACKAGE_CACHE does not match packages.yml's pins"
import sys
from pathlib import Path

import yaml

cache = Path(sys.argv[1])
repo = Path(sys.argv[2])
declared = {p["package"]: p["version"] for p in yaml.safe_load((repo / "packages.yml").read_text(encoding="utf-8"))["packages"]}
locked = {p["package"]: p["version"] for p in yaml.safe_load((cache / "package-lock.yml").read_text(encoding="utf-8"))["packages"]}
if locked != declared:
    raise SystemExit(f"DPF_DBT_PACKAGE_CACHE package-lock.yml {locked} does not match packages.yml {declared}")
print("DPF_DBT_PACKAGE_CACHE pins verified:", locked)
PYEOF
PACKAGES_DEST="estate/runtime/data/dbt/packages"
mkdir -p "$PACKAGES_DEST" || fail "cannot create $PACKAGES_DEST"
cp -r "$DPF_DBT_PACKAGE_CACHE/dbt_utils" "$PACKAGES_DEST/dbt_utils" || fail "materializing dbt_utils from DPF_DBT_PACKAGE_CACHE"
cp -r "$DPF_DBT_PACKAGE_CACHE/automate_dv" "$PACKAGES_DEST/automate_dv" || fail "materializing automate_dv from DPF_DBT_PACKAGE_CACHE"

echo "--- reference-journey: register/activate the shipped binding's own contract, ingest, inspect, backup/restore"
"$VPY" - "$REPO_ROOT" <<'PYEOF'
import io
import json
import shutil
import sys
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path

import yaml

repo = Path(sys.argv[1]).resolve()

import ergasterion
pkg = Path(ergasterion.__file__).resolve()
if repo in pkg.parents:
    raise SystemExit(f"factory imported from the source tree, {pkg} -- the wheel is not under test")

from ergasterion.cli import main as cli_main
from ergasterion.estate import EstateContext
from ergasterion.ingestion.codecs import transport_payload_fingerprint
from ergasterion.ingestion.reference_runtime import contract_digest as runtime_contract_digest
from ergasterion.source_delivery import load_typed_declarations
from ergasterion.sync_scaffold import REFERENCE_SOURCE, REFERENCE_TABLE, reference_contract


def _run(argv):
    stdout, stderr = io.StringIO(), io.StringIO()
    with redirect_stdout(stdout), redirect_stderr(stderr):
        code = cli_main(argv)
    return code, stdout.getvalue(), stderr.getvalue()


def _json_run(argv):
    code, out, err = _run([*argv, "--json"])
    return code, (json.loads(out) if out.strip() else {}), err


def _omit_nulls(value):
    if isinstance(value, dict):
        return {k: _omit_nulls(v) for k, v in value.items() if v is not None}
    if isinstance(value, list):
        return [_omit_nulls(v) for v in value]
    return value


def _check(label, code, err):
    if code != 0:
        raise SystemExit(f"{label} failed (exit {code}): {err}")
    print(f"{label}: ok")


est = Path("estate").resolve()
contract = reference_contract()

landing = _omit_nulls(contract.landing.model_dump(mode="json", by_alias=True))
delivery = _omit_nulls(contract.delivery.model_dump(mode="json", by_alias=True))
product = _omit_nulls(contract.product.model_dump(mode="json", by_alias=True))
product.pop("domain", None)
projection = [_omit_nulls(item.model_dump(mode="json", by_alias=True)) for item in contract.projection]
(est / "domains" / "reference.yml").write_text(
    yaml.safe_dump({
        "bronze": {
            "domain": {"name": "reference", "display_name": "Reference"},
            "products": [{"source": REFERENCE_SOURCE, "table": REFERENCE_TABLE}],
        },
    }, sort_keys=False),
    encoding="utf-8",
)
(est / "declarations" / f"{REFERENCE_SOURCE}.yml").write_text(
    yaml.safe_dump({
        "source": {"name": REFERENCE_SOURCE},
        "tables": {REFERENCE_TABLE: {
            "landing": landing, "delivery": delivery, "product": product, "projection": projection,
        }},
    }, sort_keys=False),
    encoding="utf-8",
)

typed = load_typed_declarations(EstateContext.resolve(estate_root=est))
loaded = typed.tables[(REFERENCE_SOURCE, REFERENCE_TABLE)].contract
digest = runtime_contract_digest(loaded)
shipped = yaml.safe_load((est / "runtime" / "local.yml").read_text(encoding="utf-8"))
if shipped["contract_digest"] != digest:
    raise SystemExit(
        f"the shipped runtime/local.yml contract_digest {shipped['contract_digest']!r} does not "
        f"match the reference declaration's compiled digest {digest!r} -- the scaffold binding "
        f"and ergasterion.sync_scaffold.reference_contract() have drifted apart"
    )

shared = [
    "--project-dir", str(est), "--source", REFERENCE_SOURCE, "--table", REFERENCE_TABLE,
    "--binding", "runtime/local.yml", "--environment", "local",
]

code, _, err = _run(["plan", *shared]); _check("plan", code, err)
code, _, err = _run(["contract", "register", *shared]); _check("contract register", code, err)
code, _, err = _run(["contract", "activate", *shared, "--candidate-digest", digest, "--migration", "carry"])
_check("contract activate", code, err)
code, dep, err = _json_run(["deployment", "register", *shared]); _check("deployment register", code, err)
manifest_digest = dep["result"]["runtime_manifest_digest"]
code, _, err = _run(["deployment", "activate", *shared, "--manifest-digest", manifest_digest])
_check("deployment activate", code, err)

deliveries = est / "deliveries"
deliveries.mkdir(exist_ok=True)
rows = [
    {"order_id": "reference-order-1", "loaded_at": "2026-01-01T01:00:00.000000Z"},
    {"order_id": None, "loaded_at": "2026-01-01T01:00:00.000000Z"},
]
payload_bytes = json.dumps(rows, separators=(",", ":")).encode("utf-8")
manifest_body = {
    "schema": "ergasterion.delivery-manifest/v1",
    "logical_identity": contract.logical_identity.model_dump(mode="json"),
    "product_version": contract.product.product_version,
    "contract_digest": digest,
    "delivery_id": "reference-delivery-1",
    "batch_id": "reference-delivery-1",
    "scheduled_boundary_at": "2026-01-01T01:00:00.000000Z",
    "effective_boundary_at": None,
    "payload": {
        "media_type": "application/x-ndjson",
        "content_encoding": "identity",
        "codec_version": 1,
        "byte_length": str(len(payload_bytes)),
        "sha256": transport_payload_fingerprint(payload_bytes),
    },
    "frame_sequence_digest": None,
    "progress_claim": {"kind": "opaque_batch"},
    "declared_row_count": str(len(rows)),
    "snapshot_attestation": None,
}
payload_path = deliveries / "reference-delivery-1.ndjson"
manifest_path = deliveries / "reference-delivery-1.manifest.json"
payload_path.write_bytes(payload_bytes)
manifest_path.write_text(json.dumps(manifest_body, separators=(",", ":")), encoding="utf-8")

code, ingested, err = _json_run([
    "ingest", "file", *shared, "--manifest", str(manifest_path), "--payload", str(payload_path),
])
_check("ingest file (one accepted row, one quarantined row)", code, err)

code, _, err = _run(["status", *shared]); _check("status", code, err)
code, inspected, err = _json_run(["inspect", *shared]); _check("inspect", code, err)
kinds = {item["kind"] for item in inspected["result"]["evidence"]["items"]}
for needed in ("contract", "schema", "quality", "lineage", "receipt"):
    if needed not in kinds:
        raise SystemExit(f"operator inspection is missing evidence kind {needed!r}: {sorted(kinds)}")

code, quarantined, err = _json_run(["quarantine", *shared, "--action", "list"])
_check("quarantine --action list", code, err)
items = quarantined["result"]["evidence"]["items"]
if not items:
    raise SystemExit("expected the quarantined row (order_id: null) to be listed")
print(f"quarantine inspection: {len(items)} item(s) diagnosed")

# Local backup to a temporary destination OUTSIDE the project/runtime root, then a
# quiescent restore after deleting the complete local runtime root.
backup_dest = est.parent.parent / f"wheel-validate-backup-{est.parent.name}"
code, created, err = _json_run(["local-backup", *shared, "--action", "create", "--destination", str(backup_dest)])
_check("local-backup create (external destination)", code, err)
before_revision = created["result"]["manifest"]["state_revision"]

runtime_root = est / "runtime" / "data"
shutil.rmtree(runtime_root)
if runtime_root.exists():
    raise SystemExit("runtime/data/ deletion did not take effect before the restore proof")

code, restored, err = _json_run([
    "local-backup", *shared, "--action", "restore",
    "--manifest", str(backup_dest / "backup-manifest.json"),
])
_check("local-backup restore (after deleting the complete runtime root)", code, err)
if restored["result"]["manifest"]["state_revision"] != before_revision:
    raise SystemExit("restored state_revision does not match the pre-deletion backup")
if "ReprocessingClaim" in json.dumps(restored):
    raise SystemExit("a verified restore must create no ReprocessingClaim")

shutil.rmtree(backup_dest)  # the external test backup is removed after verification
print("reference journey OK: plan/contract/deployment/ingest/status/inspect/quarantine/backup-restore")
PYEOF
[ $? -eq 0 ] || fail "reference-journey plan/register/activate/ingest/backup"

echo "--- dbt proof against the scaffolded estate: source checkout unavailable, packages materialized only from DPF_DBT_PACKAGE_CACHE, network disabled"
(
  cd estate || exit 1
  export DBT_LOG_PATH="$(pwd)/runtime/data/dbt/logs"
  "$VDBT" parse --profiles-dir profiles --no-partial-parse -t duckdb
) || fail "dbt parse against the scaffolded estate (packages materialized from DPF_DBT_PACKAGE_CACHE, DBT_LOG_PATH set)"
(
  cd estate || exit 1
  export DBT_LOG_PATH="$(pwd)/runtime/data/dbt/logs"
  "$VDBT" build --profiles-dir profiles --no-partial-parse -t duckdb
) || fail "dbt build -t duckdb against the scaffolded estate"
[ -d estate/target ] && fail "dbt build created a root estate/target/ -- target-path did not take effect"
[ -d estate/logs ] && fail "dbt build created a root estate/logs/ -- log-path did not take effect"
[ -d estate/dbt_packages ] && fail "dbt build created a root estate/dbt_packages/ -- packages-install-path did not take effect"
[ -f estate/runtime/data/dbt/target/manifest.json ] || fail "expected estate/runtime/data/dbt/target/manifest.json under the fixed target-path"
[ -f estate/runtime/data/ergasterion.duckdb ] || fail "expected estate/runtime/data/ergasterion.duckdb under the fixed DuckDB default path"

echo "=== wheel-mode arm green: init + emit + reference journey + dbt proof, all from a non-editable install ==="
