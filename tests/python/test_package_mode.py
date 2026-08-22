"""Package-mode smoke for the pip-installable Ergasterion engine.

The test proves source-tree imports, an editable installation, both command entry
points, estate-root resolution, and root discovery from a nested directory. It also
reads pyproject.toml directly to prove the optional-dependencies extras keys the
install and validator gates depend on by name still exist and stay consistent with
each other. It runs without contacting a package index.
"""
from __future__ import annotations

import os
import subprocess
import sys
import tempfile
import tomllib

# Run directly as ``python tests/python/test_package_mode.py``.
if __package__ in (None, ""):
    import os as _os, sys as _sys
    _sys.path.insert(0, _os.path.dirname(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))))

import ergasterion

# The tree under test is two directories above tests/python/.
TREE_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def _fail(msg: str, proc: subprocess.CompletedProcess | None = None) -> int:
    sys.stderr.write(f"FAIL (package-mode smoke): {msg}\n")
    if proc is not None:
        sys.stderr.write(f"  command : {proc.args}\n")
        sys.stderr.write(f"  exitcode: {proc.returncode}\n")
        if proc.stdout:
            sys.stderr.write("  --- stdout ---\n" + proc.stdout + "\n")
        if proc.stderr:
            sys.stderr.write("  --- stderr ---\n" + proc.stderr + "\n")
    return 1


def _run(cmd: list[str], cwd: str) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, cwd=cwd, capture_output=True, text=True)


def _venv_bin(venv_dir: str, name: str) -> str:
    # Windows lays the executables under Scripts/ with .exe; POSIX under bin/.
    scripts = os.path.join(venv_dir, "Scripts")
    if os.path.isdir(scripts):
        exe = os.path.join(scripts, name + ".exe")
        return exe if os.path.exists(exe) else os.path.join(scripts, name)
    return os.path.join(venv_dir, "bin", name)


def _venv_site_packages(venv_dir: str) -> str:
    # Windows: <venv>/Lib/site-packages. POSIX: <venv>/lib/pythonX.Y/site-packages.
    win = os.path.join(venv_dir, "Lib", "site-packages")
    if os.path.isdir(win):
        return win
    lib = os.path.join(venv_dir, "lib")
    for entry in os.listdir(lib) if os.path.isdir(lib) else []:
        candidate = os.path.join(lib, entry, "site-packages")
        if os.path.isdir(candidate):
            return candidate
    raise FileNotFoundError(f"no site-packages directory found under {venv_dir}")


def _same_path(a: str, b: str) -> bool:
    return os.path.normcase(os.path.abspath(a)) == os.path.normcase(os.path.abspath(b))


def _check_extras_by_name() -> str | None:
    """Read pyproject.toml directly and confirm the extras keys every install
    and gate script names literally -- ``local-ingestion`` and its ``duckdb``
    compatibility alias -- exist, and that ``all`` is exactly their union with
    the ``snowflake``/``bigquery`` adapters. No script installs by extra name
    today (every gate lists the pinned packages instead), so a typo or rename
    of an extras key in pyproject.toml would otherwise pass every gate
    unnoticed. Returns an error string, or ``None`` when the keys agree.
    """
    pyproject_path = os.path.join(TREE_ROOT, "pyproject.toml")
    with open(pyproject_path, "rb") as fh:
        data = tomllib.load(fh)
    extras = data.get("project", {}).get("optional-dependencies", {})

    for key in ("local-ingestion", "duckdb", "snowflake", "bigquery", "all"):
        if key not in extras:
            return f"pyproject.toml [project.optional-dependencies] is missing the '{key}' extra"

    local_ingestion = set(extras["local-ingestion"])
    duckdb_alias = set(extras["duckdb"])
    if local_ingestion != duckdb_alias:
        return (
            "the 'duckdb' extra is not an identical alias of 'local-ingestion': "
            f"local-ingestion={sorted(local_ingestion)!r}, duckdb={sorted(duckdb_alias)!r}"
        )

    expected_all = local_ingestion | set(extras["snowflake"]) | set(extras["bigquery"])
    actual_all = set(extras["all"])
    if actual_all != expected_all:
        return (
            "the 'all' extra is not the union of local-ingestion, snowflake and bigquery: "
            f"all={sorted(actual_all)!r}, expected={sorted(expected_all)!r}"
        )
    return None


def main() -> int:
    # Extras-key-by-name assertion: fast, no venv needed, runs first.
    extras_error = _check_extras_by_name()
    if extras_error:
        return _fail(extras_error)

    # Script-mode tree identity: `import ergasterion` above ran under the shim,
    # so it must have bound to THIS tree even if the carries a
    # sibling-tree editable install.
    got_root = os.path.dirname(os.path.dirname(os.path.abspath(ergasterion.__file__)))
    if not _same_path(got_root, TREE_ROOT):
        return _fail(
            "script-mode shim did not pin this tree: "
            f"ergasterion resolved to {got_root!r}, expected {TREE_ROOT!r}"
        )

    # (2) Package-mode install + entry point, in a scratch venv, from a neutral cwd.
    with tempfile.TemporaryDirectory(prefix="ergasterion-pkgsmoke-") as tmp:
        venv_dir = os.path.join(tmp, "venv")
        neutral = os.path.join(tmp, "neutral")  # a cwd with NO ergasterion/ in it
        os.makedirs(neutral, exist_ok=True)

        # Scratch venv, borrowing THIS interpreter's toolchain offline. `--system-site-packages`
        # cannot do that borrowing when sys.executable is itself a venv: Python's venv module
        # always resolves a new venv's system site-packages back to the ULTIMATE base
        # interpreter (pyvenv.cfg's `home`), skipping any intermediate venv entirely -- so a
        # scratch venv built from the source checkout's bootstrapped .venv would silently see
        # whatever (if anything) is globally installed, not this venv's pinned dependency set.
        # A .pth file injecting this interpreter's own site-packages directory borrows it for
        # real, regardless of how many venvs deep sys.executable sits.
        p = _run([sys.executable, "-m", "venv", venv_dir], cwd=tmp)
        if p.returncode != 0:
            return _fail("could not create scratch venv", p)

        vpy = _venv_bin(venv_dir, "python")
        erg_cli = _venv_bin(venv_dir, "ergasterion")

        parent_site_packages = [p for p in sys.path if p and os.path.isdir(p) and os.path.basename(p) == "site-packages"]
        if not parent_site_packages:
            return _fail(f"could not locate this interpreter's own site-packages in sys.path: {sys.path!r}")
        scratch_site = _venv_site_packages(venv_dir)
        with open(os.path.join(scratch_site, "_parent_venv.pth"), "w", encoding="utf-8") as f:
            for site_dir in parent_site_packages:
                f.write(site_dir + "\n")

        # editable install of THIS tree -- offline, no build isolation (setuptools borrowed)
        p = _run(
            [vpy, "-m", "pip", "install", "-e", TREE_ROOT,
             "--no-build-isolation", "--no-index"],
            cwd=tmp,
        )
        if p.returncode != 0:
            return _fail("editable install failed (offline)", p)

        # package-mode imports + editable-install tree identity, from the neutral cwd
        probe = (
            "import os\n"
            "import ergasterion, ergasterion.cli, ergasterion.emit, ergasterion.emit_contracts, "
            "ergasterion.emit_odps, ergasterion.emit_graph, ergasterion.graph_model, "
            "ergasterion.import_odcs, ergasterion.dialect_lint\n"
            "root = os.path.dirname(os.path.dirname(os.path.abspath(ergasterion.__file__)))\n"
            "print(root)\n"
        )
        p = _run([vpy, "-c", probe], cwd=neutral)
        if p.returncode != 0:
            return _fail("package-mode import of the engine modules failed", p)
        installed_root = p.stdout.strip().splitlines()[-1] if p.stdout.strip() else ""
        if not _same_path(installed_root, TREE_ROOT):
            return _fail(
                "editable install did not resolve to the tree under test: "
                f"ergasterion resolved to {installed_root!r}, expected {TREE_ROOT!r}",
                p,
            )

        # Namespace-package discovery: ergasterion.schemas, .scaffold, .scaffold.macros,
        # .scaffold.targets, and .templates carry no __init__.py of their own -- they are
        # admitted by [tool.setuptools.packages.find] namespaces=true, not a hard-coded package
        # list, so a future ergasterion.* subpackage is picked up the same way with no
        # pyproject.toml edit. This is the regression proof for that discovery switch.
        namespace_probe = (
            "import ergasterion.schemas, ergasterion.scaffold, ergasterion.scaffold.macros, "
            "ergasterion.scaffold.targets, ergasterion.templates\n"
            "print('namespace packages OK')\n"
        )
        np = _run([vpy, "-c", namespace_probe], cwd=neutral)
        if np.returncode != 0:
            return _fail("editable install did not admit the ergasterion.* namespace subpackages", np)

        # Base-dependency pins: pydantic, rfc8785, tzdata, and cryptography are unconditional
        # `dependencies`, not an optional extra, so an editable install of this tree must resolve
        # every one of them at the exact pin from pyproject.toml.
        pins_probe = (
            "from importlib import metadata as im\n"
            "want = {'pydantic': '2.13.4', 'rfc8785': '0.1.4', 'tzdata': '2026.2', 'cryptography': '49.0.0'}\n"
            "bad = [f'{n}: installed {im.version(n)}, want {v}' for n, v in want.items() if im.version(n) != v]\n"
            "import sys\n"
            "sys.exit('; '.join(bad)) if bad else print('pinned base dependency versions OK')\n"
        )
        pp = _run([vpy, "-c", pins_probe], cwd=neutral)
        if pp.returncode != 0:
            return _fail("editable install did not resolve the pinned base dependency versions", pp)

        # console entry point: multiplexer help + a subcommand's own help
        p = _run([erg_cli, "--help"], cwd=neutral)
        if p.returncode != 0 or "subcommands" not in (p.stdout + p.stderr):
            return _fail("`ergasterion --help` did not list its subcommands", p)
        console_help = p.stdout
        p = _run([erg_cli, "emit", "--help"], cwd=neutral)
        if p.returncode != 0:
            return _fail("`ergasterion emit --help` failed", p)

        # Launcher-free invocation: `python -m ergasterion` must reach
        # the SAME CLI as the `ergasterion` console command -- it is a thin shim onto
        # `ergasterion.cli:main`, not a second implementation. Package-mode first: the venv's own
        # interpreter, from the neutral cwd, with no launcher on PATH at all.
        p = _run([vpy, "-m", "ergasterion", "--help"], cwd=neutral)
        if p.returncode != 0:
            return _fail("`python -m ergasterion --help` (installed) failed", p)
        if p.stdout != console_help:
            return _fail(
                "`python -m ergasterion --help` (installed) output differs from "
                "`ergasterion --help` -- both must reach the same CLI",
                p,
            )
        probe = "import ergasterion\nprint(ergasterion.__file__)\n"
        p = _run([vpy, "-c", probe], cwd=neutral)
        if p.returncode != 0:
            return _fail("package-mode `import ergasterion` failed", p)
        got = p.stdout.strip().splitlines()[-1] if p.stdout.strip() else ""
        mod_root = os.path.dirname(os.path.dirname(os.path.abspath(got)))
        if not _same_path(mod_root, TREE_ROOT):
            return _fail(
                "`python -m ergasterion` (installed) did not resolve `ergasterion` to the tree "
                f"under test: got {mod_root!r}, expected {TREE_ROOT!r}",
                p,
            )

        # Script-mode: no install at all, THIS interpreter (not the scratch venv), run direct
        # from the source tree with cwd=TREE_ROOT, a different cwd than every check above and
        # below. `python -m X` prepends cwd to
        # sys.path[0], ahead of site-packages, so this must resolve `ergasterion` to TREE_ROOT.
        p = _run([sys.executable, "-m", "ergasterion", "--help"], cwd=TREE_ROOT)
        if p.returncode != 0:
            return _fail("`python -m ergasterion --help` (script-mode, source cwd) failed", p)
        if p.stdout != console_help:
            return _fail(
                "script-mode `python -m ergasterion --help` output differs from the "
                "`ergasterion --help` console form",
                p,
            )
        p = _run([sys.executable, "-c", probe], cwd=TREE_ROOT)
        if p.returncode != 0:
            return _fail("script-mode `import ergasterion` failed", p)
        got = p.stdout.strip().splitlines()[-1] if p.stdout.strip() else ""
        script_root = os.path.dirname(os.path.dirname(os.path.abspath(got)))
        if not _same_path(script_root, TREE_ROOT):
            return _fail(
                "script-mode `python -m ergasterion` did not resolve `ergasterion` to the "
                f"source tree: got {script_root!r}, expected {TREE_ROOT!r}",
                p,
            )

        # an emit from a DIFFERENT cwd (proves cwd-independence + offline). The engine is
        # __file__-anchored, so --check reads/writes-nothing against the tree under test.
        p = _run([erg_cli, "emit", "--check"], cwd=neutral)
        if p.returncode != 0:
            return _fail("`ergasterion emit --check` from a neutral cwd failed", p)

        # Explicit estate-root resolution: emit runs correctly from a
        # different cwd against an --estate-root-NAMED estate. From the neutral cwd (no estate
        # in it or above it), an explicit --estate-root must resolve TREE_ROOT and emit
        # byte-stably against it.
        p = _run([erg_cli, "emit", "--check", "--estate-root", TREE_ROOT], cwd=neutral)
        if p.returncode != 0:
            return _fail("`ergasterion emit --check --estate-root <tree>` from a neutral cwd failed", p)

        # Walk-up resolution from a nested cwd. Build a synthetic estate (dbt_project.yml +
        # domains/ markers, dbt's own project-resolution precedent) with a deeply nested
        # subdir, then resolve the estate root from that nested cwd. It must walk UP to the
        # synthetic root -- NOT fall through to the package anchor (TREE_ROOT). Asserting the
        # resolved root equals the synthetic estate (which != TREE_ROOT) is what makes this a
        # genuine walk-up proof rather than a restatement of the fallback.
        synthetic = os.path.join(tmp, "synthetic_estate")
        nested = os.path.join(synthetic, "a", "b", "c")
        os.makedirs(os.path.join(synthetic, "domains"), exist_ok=True)
        os.makedirs(nested, exist_ok=True)
        with open(os.path.join(synthetic, "dbt_project.yml"), "w", encoding="utf-8") as fh:
            fh.write("name: synthetic\n")
        probe = (
            "from ergasterion.estate import EstateContext\n"
            "print(EstateContext.resolve().root)\n"
        )
        walk_env = {k: v for k, v in os.environ.items()}
        walk_env.pop("DPF_ESTATE_ROOT", None)  # env must not pre-empt the walk-up under test
        p = subprocess.run([vpy, "-c", probe], cwd=nested, capture_output=True, text=True, env=walk_env)
        if p.returncode != 0:
            return _fail("estate-root walk-up probe failed to run", p)
        resolved = p.stdout.strip().splitlines()[-1] if p.stdout.strip() else ""
        if not _same_path(resolved, synthetic):
            return _fail(
                "walk-up from a nested cwd did not resolve the synthetic estate root: "
                f"resolved {resolved!r}, expected {synthetic!r} (a fall-through to the package "
                f"anchor {TREE_ROOT!r} means walk-up is broken)",
                p,
            )

    print("package-mode smoke OK: script-mode shim identity, editable install, entry point, "
          "cwd-independent emit, --estate-root emit, and nested-cwd walk-up all green.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
