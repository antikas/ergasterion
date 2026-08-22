"""Independent source-to-platform acceptance: a wheel-installed consumer in a
temporary directory exercises three Bronze products end to end -- CDC JSON
Lines with an explicit tombstone, append-only CSV with one recoverable
quarantined row plus an additive migration, and a signed complete snapshot.

Builds the ``ergasterion`` wheel and a scratch venv once (mirroring
``scripts/validate_wheel.sh``'s offline mechanics), then runs
``tests/fixtures/bronze_acceptance/wheel_driver.py`` under that venv's own
interpreter against a project directory outside the source tree. The driver
prints one ``STEP: ...`` line per checkpoint; this test asserts a clean exit
and that every declared checkpoint fired, so a driver that silently exits
early (rather than raising) still fails loudly here.

Usage:
    python tests/python/test_ingestion_acceptance.py

Requires (same offline contract as scripts/validate_wheel.sh):
    DPF_WHEELHOUSE   a directory of wheels for --no-index --find-links installs.
"""

from __future__ import annotations

import os
import subprocess
import sys
import tempfile
import venv
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
FIXTURES = REPO_ROOT / "tests" / "fixtures" / "bronze_acceptance"
DRIVER = FIXTURES / "wheel_driver.py"

# Every STEP checkpoint the driver is expected to print, in order. Kept here
# (not just "driver exit code 0") so a driver that returns early -- a stray
# `raise SystemExit(0)` left behind, an exception swallowed upstream -- fails
# this test loudly instead of silently proving less than it claims to.
EXPECTED_STEPS = [
    "all three products planned, registered and activated from the installed wheel",
    "cdc delivery 1 (two upserts) ingested",
    "cdc delivery 1 replay is idempotent (retry)",
    "cdc delivery 2 (tombstone) ingested",
    "cdc row-level release rejected",
    "CDC product: raw receipts, typed/disposition evidence, publication, retry/idempotency and row-level rejection proven",
    "append-only CSV: real CSV typed parsing produced exactly one recoverable quarantined row",
    "additive migration: product version advanced, contract and schema digests changed, no historical reload required",
    "historical rows remain queryable after additive migration",
    "remediation release decision is durable and replays exactly once",
    "schedule-boundary timeliness is tracked as its own operational signal",
    "large 400-row delivery (bounded ScratchStore external-sort) ingested",
    "large delivery: parsing, bounded ScratchStore spill validation, dispositions, publication and restart cleanup proven",
    "Ed25519 verification key registered for signed snapshot attestations",
    "signed complete snapshot published under the exact synthetic-local policy",
    "source-complete but acceptance-incomplete snapshot leaves the prior snapshot current",
    "operational state after dead-letter: commit_blocked",
    "exact repair publishes evidence and flips the pointer",
    "verified local-backup restores the complete local runtime root byte-for-byte",
    "whole-file Bronze loss surfaces bronze_store_restore_required rather than silent data loss",
    "loss of applied-unconfirmed target evidence remains visibly commit_blocked",
    "local-backup create is refused while a commit-blocked attempt is in flight",
    "operator commands show commit-blocked recovery (state=committed)",
]


def _fail(message: str) -> None:
    raise AssertionError(message)


def _venv_python(venv_dir: Path) -> Path:
    candidate = venv_dir / "Scripts" / "python.exe"
    if candidate.is_file():
        return candidate
    return venv_dir / "bin" / "python"


def _clean_env() -> dict:
    """The current environment with PYTHONPATH/PYTHONHOME stripped, so every
    subprocess below (wheel build, venv installs, the driver itself) resolves
    ``ergasterion`` only from what it actually has installed -- never from an
    inherited source-tree path that would silently mask a broken wheel."""

    env = dict(os.environ)
    env.pop("PYTHONPATH", None)
    env.pop("PYTHONHOME", None)
    return env


def _build_wheel_and_venv(work: Path) -> Path:
    wheelhouse = os.environ.get("DPF_WHEELHOUSE")
    if not wheelhouse:
        _fail("DPF_WHEELHOUSE is required -- this acceptance test installs the wheel offline only")
    wheelhouse_path = Path(wheelhouse)
    if not wheelhouse_path.is_dir():
        _fail(f"DPF_WHEELHOUSE does not name a directory: {wheelhouse}")

    env = _clean_env()
    dist = work / "dist"
    dist.mkdir(parents=True, exist_ok=True)
    py = os.environ.get("PY") or sys.executable
    subprocess.run(
        [py, "-m", "pip", "wheel", str(REPO_ROOT), "--no-deps", "--no-build-isolation", "-w", str(dist), "-q"],
        check=True, env=env,
    )
    wheels = sorted(dist.glob("ergasterion_factory-*.whl"))
    if not wheels:
        _fail("no ergasterion_factory wheel produced")
    wheel = wheels[0]

    venv_dir = work / "venv"
    venv.EnvBuilder(with_pip=True).create(str(venv_dir))
    vpy = _venv_python(venv_dir)
    if not vpy.is_file():
        _fail(f"scratch venv creation produced no interpreter under {venv_dir}")

    subprocess.run(
        [str(vpy), "-m", "pip", "install", "-q", "--no-index", "--find-links", str(wheelhouse_path), str(wheel)],
        check=True, env=env,
    )
    # duckdb is the only extra runtime dependency the driver itself imports directly
    # (beyond what the wheel's own [local-ingestion] extra already pulls in via the
    # editable-install venv this repo bootstraps for every other test); install the
    # same pinned extras validate_wheel.sh installs, offline, by name.
    subprocess.run(
        [
            str(vpy), "-m", "pip", "install", "-q", "--no-index", "--find-links", str(wheelhouse_path),
            "duckdb==1.5.5", "dbt-core==1.11.12", "dbt-duckdb==1.11.0",
        ],
        check=True, env=env,
    )
    return vpy


def test_wheel_installed_three_product_acceptance() -> None:
    with tempfile.TemporaryDirectory(prefix="dpf-bronze-acceptance-") as tmp:
        work = Path(tmp)
        vpy = _build_wheel_and_venv(work)
        project_dir = work / "project"
        result = subprocess.run(
            [str(vpy), str(DRIVER), str(project_dir), str(FIXTURES)],
            cwd=str(work), capture_output=True, text=True, env=_clean_env(),
        )
        output = result.stdout + "\n" + result.stderr
        if result.returncode != 0:
            _fail(f"wheel_driver.py exited {result.returncode}:\n{output}")
        missing = [step for step in EXPECTED_STEPS if step not in output]
        if missing:
            _fail(f"driver exited 0 but did not report every expected checkpoint: {missing}\nfull output:\n{output}")
        if "SNAPSHOT_OK" not in output:
            _fail(f"driver did not reach its final SNAPSHOT_OK marker:\n{output}")


TESTS = [
    test_wheel_installed_three_product_acceptance,
]


def main() -> int:
    import traceback

    failures = 0
    for test in TESTS:
        try:
            test()
            sys.stdout.write(f"PASS {test.__name__}\n")
        except Exception:
            failures += 1
            sys.stdout.write(f"FAIL {test.__name__}\n")
            traceback.print_exc()
    if failures:
        sys.stdout.write(f"{failures} failed\n")
        return 1
    sys.stdout.write(f"{len(TESTS)} passed\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
