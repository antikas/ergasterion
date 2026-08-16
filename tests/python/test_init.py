"""Standalone acceptance tests for ``ergasterion init`` and the consumer scaffold.

The suite checks the scaffold structure and package-data fidelity, then creates a toy
domain and runs init, emit, parse, a local DuckDB build, contract, descriptor, and graph
commands without a warehouse connection.
"""
from __future__ import annotations

import os
import re
import shutil
import subprocess
import sys
import tempfile
import traceback
from pathlib import Path

import duckdb
import yaml

# Allow direct execution from the repository root.
if __package__ in (None, ""):
    import os as _os, sys as _sys
    _sys.path.insert(0, _os.path.dirname(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))))

from ergasterion import init as init_mod
from ergasterion._repo_root import REPO_ROOT

TESTS_DIR = Path(__file__).resolve().parent
if str(TESTS_DIR) not in sys.path:
    sys.path.insert(0, str(TESTS_DIR))
import test_emit

TREE_ROOT = str(REPO_ROOT)


# ---------------------------------------------------------------------------
# Domain-token residue vocabulary for the generic consumer scaffold:
# the eight source-system brand tokens plus entity nouns from the two worked domains
# (domains/investment.yml, domains/ecommerce.yml). Compound/specific forms only for the
# entity-noun half -- bare "product"/"order" are excluded: both collide with the engine's
# OWN generic vocabulary ("data product", "product descriptor", SQL "order by"), which
# would make the gate noise-positive on the engine's legitimate self-description rather
# than a real residue signal.
# ---------------------------------------------------------------------------
BRAND_TOKENS = (
    "VANTORA", "MERIDEX", "CARTIVO", "MERCARO", "RELATIO", "ORIGO", "PORTIQ", "CHRONO",
)
ENTITY_NOUN_TOKENS = (
    "fund", "gp", "deal", "customer",
    "portfolio_company", "legal_vehicle", "order_line",
    "gp_succession", "fund_cash_flow", "fund_valuation", "legal_vehicle_cash_flow",
    "deal_fund_conversion", "deal_target_company", "deal_decision_log", "deal_approvals",
)
_RESIDUE_RE = re.compile(
    r"\b(" + "|".join(re.escape(t) for t in BRAND_TOKENS + ENTITY_NOUN_TOKENS) + r")\b",
    re.IGNORECASE,
)


def _residue_hits(root: Path, *, exclude: set[Path] = frozenset()) -> dict[str, list[str]]:
    """Every (relative-path -> matched lines) hit of a domain token under root, skipping
    the paths in `exclude` (given as resolved absolute Paths). LICENSE is always skipped:
    the templated MIT text's standard boilerplate ("to deal in the Software without
    restriction") false-positives the `deal` entity-noun token -- a collision with
    unmodifiable legal boilerplate, not a domain-vocabulary leak."""
    hits: dict[str, list[str]] = {}
    for path in sorted(root.rglob("*")):
        if not path.is_file() or path.name == "LICENSE" or path.resolve() in exclude:
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except (UnicodeDecodeError, OSError):
            continue
        matches = sorted({m.group(0) for m in _RESIDUE_RE.finditer(text)})
        if matches:
            hits[str(path.relative_to(root))] = matches
    return hits


# ---------------------------------------------------------------------------
# 1. Structural checks on ergasterion.init.scaffold()
# ---------------------------------------------------------------------------

def test_scaffold_structure_and_macro_fidelity() -> None:
    with tempfile.TemporaryDirectory(prefix="ergasterion-init-structure-") as tmp:
        dest = Path(tmp) / "estate"
        written = init_mod.scaffold(dest)
        assert written, "scaffold() must report at least one written path"
        assert all(p.exists() for p in written), "every reported path must exist on disk"

        # dbt_project.yml: engine-generic top, estate-only blocks stripped.
        scaffolded = yaml.safe_load((dest / "dbt_project.yml").read_text(encoding="utf-8"))
        engine = yaml.safe_load((REPO_ROOT / "dbt_project.yml").read_text(encoding="utf-8"))
        assert "seeds" not in scaffolded, "scaffold dbt_project.yml must carry no seeds: block"
        assert "on-run-start" not in scaffolded, "scaffold dbt_project.yml must carry no on-run-start: hooks"
        assert scaffolded["name"] == engine["name"] == "ergasterion", (
            "project name must stay 'ergasterion' -- macros/cross_db.sql's adapter.dispatch "
            "calls hardcode that dispatch namespace"
        )
        assert scaffolded["profile"] == engine["profile"]
        assert scaffolded["model-paths"] == engine["model-paths"]
        assert "deal_approvals" not in scaffolded["models"]["ergasterion"], (
            "the estate-only deal_approvals model-path leaf must be stripped"
        )
        for layer in ("staging", "entity_resolution", "raw_vault"):
            assert layer in scaffolded["models"]["ergasterion"], f"engine-generic layer {layer!r} must survive the strip"

        # macros/: byte-identical copy of the explicit engine-generic allow-set.
        import ergasterion.sync_scaffold as sync_scaffold
        engine_macros = [REPO_ROOT / "macros" / name for name in sync_scaffold.SCAFFOLD_MACROS]
        scaffold_macros = sorted((dest / "macros").rglob("*.sql"))
        assert sorted(p.name for p in engine_macros) == [p.name for p in scaffold_macros], "macro file set must match the generic allow-set"
        engine_macros = sorted(engine_macros)
        for src, copy in zip(engine_macros, scaffold_macros):
            assert src.read_bytes() == copy.read_bytes(), f"{copy.name}: macro copy must be byte-identical to the engine source"

        # packages.yml / profiles/profiles.yml: byte-identical copies.
        assert (dest / "packages.yml").read_bytes() == (REPO_ROOT / "packages.yml").read_bytes()
        assert (dest / "profiles" / "profiles.yml").read_bytes() == (REPO_ROOT / "profiles" / "profiles.yml").read_bytes()

        # Empty dirs, each seeded with .gitkeep. declarations/ also carries the
        # copied targets/ budget declarations (asserted below); everything else
        # ships empty.
        for name in ("domains", "declarations", "seeds", "tests"):
            d = dest / name
            assert d.is_dir() and (d / ".gitkeep").exists(), f"{name}/ must exist with .gitkeep"
            allowed = {".gitkeep"} | ({"targets"} if name == "declarations" else set())
            extras = [p for p in d.iterdir() if p.name not in allowed]
            assert not extras, f"{name}/ must ship with no other content, got: {extras}"

        # Structural budget declarations: byte-identical copies of the engine
        # estate's own committed set (the structure gate is fail-closed, so the
        # scaffold ships them).
        engine_targets = sorted((REPO_ROOT / "declarations" / "targets").glob("*.yml"))
        scaffold_targets = sorted((dest / "declarations" / "targets").glob("*.yml"))
        assert [p.name for p in engine_targets] == [p.name for p in scaffold_targets], (
            "target declaration file set must match the engine estate's own"
        )
        for src, copy in zip(engine_targets, scaffold_targets):
            assert src.read_bytes() == copy.read_bytes(), (
                f"{copy.name}: target declaration copy must be byte-identical to the engine source"
            )

        # LICENSE + GETTING-STARTED.md present, and the LICENSE carries the copyright line
        # emit_odps.py's team-attribution reader needs (Copyright (c) <year> <holder>).
        assert "Copyright (c) 2026 Your Name Here" in (dest / "LICENSE").read_text(encoding="utf-8")
        getting_started = (dest / "GETTING-STARTED.md").read_text(encoding="utf-8")
        assert "column_types" in getting_started and "authored" in getting_started, (
            "GETTING-STARTED.md must state plainly that seed column_types are authored, not generated"
        )
        assert "there is no" in getting_started.lower() and "command-line option" in getting_started.lower(), (
            "the doc must correct the record on --packages-install-path: it is a "
            "dbt_project.yml PROJECT CONFIG key, not a dbt CLI flag (`dbt deps --help` "
            "lists no such option on the installed dbt version)"
        )


def test_scaffold_package_data_is_current() -> None:
    """The packaged scaffold tree (ergasterion/scaffold/, what a wheel ships and init
    copies) byte-matches its in-tree sources. ergasterion/sync_scaffold.py --check is
    the same gate in the offline lane; this keeps it beside the scaffold proofs."""
    import ergasterion.sync_scaffold as sync_scaffold

    for rel, content in sync_scaffold.generated_set().items():
        dest = sync_scaffold.SCAFFOLD_DIR / rel
        assert dest.is_file(), f"ergasterion/scaffold/{rel.as_posix()} is missing -- run sync_scaffold"
        assert dest.read_bytes() == content, (
            f"ergasterion/scaffold/{rel.as_posix()} drifted from its source -- run sync_scaffold"
        )


def test_scaffold_model_keys_are_engine_generic_layers() -> None:
    """The scaffold dbt_project.yml's model-path keys are exactly the structural
    layers the emitters write into. A key the emitter never writes would hand a
    consumer materialisation/schema config for a layer they never declared."""
    scaffolded = yaml.safe_load(
        (REPO_ROOT / "ergasterion" / "scaffold" / "dbt_project.yml").read_text(encoding="utf-8")
    )
    keys = set(scaffolded["models"]["ergasterion"].keys()) - {"+materialized"}
    assert keys == {"staging", "entity_resolution", "raw_vault"}, (
        f"scaffold model-path keys must be the emitter's engine-generic layers, got: {sorted(keys)}"
    )


def test_scaffold_missing_engine_data_fails_whole() -> None:
    """A broken install (scaffold package data absent) fails loudly BEFORE the
    destination directory is made: nothing half-written lands at dest."""
    real_root = init_mod._SCAFFOLD_ROOT
    with tempfile.TemporaryDirectory(prefix="ergasterion-init-preflight-") as tmp:
        dest = Path(tmp) / "estate"
        init_mod._SCAFFOLD_ROOT = Path(tmp) / "no-such-package-data"
        try:
            raised = False
            try:
                init_mod.scaffold(dest)
            except SystemExit as exc:
                raised = True
                assert "engine scaffold data missing" in str(exc), (
                    f"expected the preflight message, got: {exc}"
                )
            assert raised, "expected SystemExit for missing engine scaffold data"
            assert not dest.exists(), "a failed preflight must leave no half-made estate behind"
        finally:
            init_mod._SCAFFOLD_ROOT = real_root


def test_scaffold_force_flag() -> None:
    with tempfile.TemporaryDirectory(prefix="ergasterion-init-force-") as tmp:
        dest = Path(tmp) / "estate"
        dest.mkdir()
        (dest / "keepme.txt").write_text("pre-existing content", encoding="utf-8")

        raised = False
        try:
            init_mod.scaffold(dest)
        except SystemExit:
            raised = True
        assert raised, "scaffolding into a non-empty dir without --force must refuse"

        written = init_mod.scaffold(dest, force=True)
        assert written, "--force must scaffold into the non-empty dir anyway"
        assert (dest / "keepme.txt").exists(), "--force must not touch unrelated pre-existing files"
        assert (dest / "dbt_project.yml").exists()


def test_scaffold_output_is_domain_token_clean() -> None:
    """A new estate contains no vocabulary from either worked example domain."""
    with tempfile.TemporaryDirectory(prefix="ergasterion-init-residue-") as tmp:
        dest = Path(tmp) / "estate"
        init_mod.scaffold(dest)
        hits = _residue_hits(dest)
        assert not hits, f"domain-token residue in scaffold output: {hits}"


# ---------------------------------------------------------------------------
# 2. Full CLI acceptance run against a scaffolded estate and toy domain.
# ---------------------------------------------------------------------------

# The SAME fixture-class domain tests/python/test_emit.py's own domain-agnosticism proof uses
# (two entities alpha/beta, one link) -- reused, not re-invented (one fixture, one owner).
# Extended here with the two boundary blocks emit.py itself never reads: `odcs` (the
# contract/descriptor adapter's product map -- emit_contracts serves only canonical/marts,
# so the toy needs ONE hand-authored served model claimed here) and `relations` (the graph
# adapter's typed-verb vocabulary -- every declared link needs a relation binding, so
# alpha_beta needs one).
def _toy_domain() -> dict:
    domain = dict(test_emit.FIXTURE_DOMAIN)
    domain["odcs"] = {
        "domain": "toyfixture",
        "contract_version": "1.0.0",
        "status": "active",
        "server": {
            "server": "ergasterion_snowflake",
            "type": "snowflake",
            "environment": "production",
            "account": "ergasterion",
            "database": "ERGASTERION",
            "schema": "MARTS",
        },
        "products": {
            "canonical_alpha": {"entity": "alpha"},
        },
    }
    domain["relations"] = {
        "verbs": {
            "ASSOCIATED_WITH": {
                "alias": "associated-with", "direction": "directed", "kind": "association",
                "cardinality": "many_to_one", "inverse": "HAS_ASSOCIATION",
            },
            "HAS_ASSOCIATION": {
                "alias": "has-association", "direction": "directed", "kind": "association",
                "cardinality": "one_to_many", "inverse": "ASSOCIATED_WITH",
            },
        },
        "bindings": [
            {
                "verb": "ASSOCIATED_WITH", "source": "alpha", "target": "beta",
                "link": "alpha_beta", "source_key": "alpha_hk", "target_key": "beta_hk",
            },
        ],
    }
    return domain


_CANONICAL_ALPHA_SQL = """{{ config(materialized='view', schema='canonical') }}

select
    golden_alpha_key as alpha_id,
    alpha_name,
    alpha_code
from {{ ref('bv_alpha_golden_record') }}
"""

_CANONICAL_SCHEMA_YML = """version: 2

models:
  - name: canonical_alpha
    description: >
      Toy fixture canonical view over the business-vault golden alpha record --
      Consumer scaffold acceptance model (tests/python/test_init.py).
    columns:
      - name: alpha_id
        tests:
          - not_null
          - unique
"""


def _run_cli(argv: list[str], cwd: Path) -> subprocess.CompletedProcess:
    """Invoke the checkout's CLI in a fresh subprocess.

    Placing this source tree first keeps the test independent of any Ergasterion package
    already installed in the selected interpreter.
    """
    probe = (
        "import sys\n"
        f"sys.path.insert(0, {TREE_ROOT!r})\n"
        "from ergasterion.cli import main\n"
        f"raise SystemExit(main({argv!r}))\n"
    )
    return subprocess.run(
        [sys.executable, "-c", probe], cwd=str(cwd), capture_output=True, text=True,
    )


def _fail(msg: str, proc: subprocess.CompletedProcess | None = None) -> None:
    lines = [f"consumer scaffold smoke FAIL: {msg}"]
    if proc is not None:
        lines.append(f"  command : {proc.args}")
        lines.append(f"  exitcode: {proc.returncode}")
        if proc.stdout:
            lines.append("  --- stdout ---\n" + proc.stdout)
        if proc.stderr:
            lines.append("  --- stderr ---\n" + proc.stderr)
    raise AssertionError("\n".join(lines))


def _dbt_exe() -> str:
    """A dbt executable for the offline validation chain: the DBT environment
    variable when the caller provides one (the validator scripts export the main
    tree's), else the nearest ancestor venv, else PATH. A linked source checkout carries
    no .venv of its own, so the walk-up there would pass the project venv and
    land on whatever PATH serves -- the env var is the authoritative override."""
    import os

    env_dbt = os.environ.get("DBT", "").strip()
    if env_dbt and Path(env_dbt).exists():
        return env_dbt
    main_tree = REPO_ROOT
    while not (main_tree / ".venv").exists() and main_tree.parent != main_tree:
        main_tree = main_tree.parent
    candidate = main_tree / ".venv" / "Scripts" / "dbt.exe"
    if candidate.exists():
        return str(candidate)
    # POSIX layout fallback (not exercised on this Windows workstation, kept honest).
    posix = main_tree / ".venv" / "bin" / "dbt"
    return str(posix) if posix.exists() else "dbt"


def test_consumer_scaffold_toy_domain_emit_parse_contracts_odps_graph() -> None:
    before_status = subprocess.run(
        ["git", "status", "--porcelain"], cwd=TREE_ROOT, capture_output=True, text=True,
    ).stdout

    with tempfile.TemporaryDirectory(prefix="ergasterion-consumer-smoke-") as tmp:
        estate = Path(tmp) / "estate"

        # (1) `ergasterion init <dir>` -- through the real CLI multiplexer.
        p = _run_cli(["init", str(estate)], cwd=Path(tmp))
        if p.returncode != 0:
            _fail("`ergasterion init <dir>` failed", p)
        assert (estate / "dbt_project.yml").exists()

        # (2) The toy domain + declaration (fixture-class, reused from tests/python/test_emit.py)
        # plus the one hand-authored served canonical model emit_contracts/odps need.
        domains_dir = estate / "domains"
        (domains_dir / "fixture.yml").write_text(
            yaml.safe_dump(_toy_domain(), sort_keys=False), encoding="utf-8",
        )
        test_emit._write_declaration(estate / "declarations", "toysrc.yml", test_emit._fixture_declaration())
        canonical_dir = estate / "models" / "canonical"
        canonical_dir.mkdir(parents=True, exist_ok=True)
        (canonical_dir / "canonical_alpha.sql").write_text(_CANONICAL_ALPHA_SQL, encoding="utf-8")
        (canonical_dir / "_canonical.yml").write_text(_CANONICAL_SCHEMA_YML, encoding="utf-8")

        # The one manual step GETTING-STARTED.md names: a raw seed for the toy source plus
        # its authored seeds: column_types block (the scaffold ships dbt_project.yml with
        # no seeds: block by design -- this domain adds its own, exactly as documented).
        seeds_dir = estate / "seeds"
        (seeds_dir / "raw_toysrc_things.csv").write_text(
            "id,alpha_name,alpha_code,beta_name\n"
            "1,Alpha One,A1,Beta One\n"
            "2,Alpha Two,A2,Beta Two\n",
            encoding="utf-8",
        )
        project = yaml.safe_load((estate / "dbt_project.yml").read_text(encoding="utf-8"))
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
        (estate / "dbt_project.yml").write_text(yaml.safe_dump(project, sort_keys=False), encoding="utf-8")

        # (3) dbt packages from a COPY of this repo's own dbt_packages/ -- no `dbt deps`
        # network fetch. packages-install-path defaults to `dbt_packages` (dbt_project.yml
        # carries no override), so copying straight to that default path is the direct
        # route the scaffold's own GETTING-STARTED.md documents.
        repo_dbt_packages = REPO_ROOT / "dbt_packages"
        assert repo_dbt_packages.is_dir(), "expected this repo's own dbt_packages/ to exist for the offline copy"
        shutil.copytree(repo_dbt_packages, estate / "dbt_packages")

        # (4) `ergasterion emit --estate-root <estate>` -- zero engine edits: the engine
        # tree under test is read-only from this call's perspective, only <estate> is
        # written.
        p = _run_cli(["emit", "--estate-root", str(estate)], cwd=Path(tmp))
        if p.returncode != 0:
            _fail("`ergasterion emit --estate-root <estate>` failed", p)
        for expect in ("hub_alpha.sql", "hub_beta.sql", "link_alpha_beta.sql", "bv_alpha_golden_record.sql"):
            found = list((estate / "models").rglob(expect))
            assert found, f"expected {expect} to be emitted into the scaffolded estate"

        # (5) `dbt parse` -- warehouse-free (parse never connects) AND network-free (the
        # packages are already on disk from step 3, `dbt deps` is never invoked).
        dbt = _dbt_exe()
        p = subprocess.run(
            [dbt, "parse", "--profiles-dir", "profiles", "--no-partial-parse", "-t", "bigquery"],
            cwd=str(estate), capture_output=True, text=True,
        )
        if p.returncode != 0:
            _fail("`dbt parse` against the scaffolded estate failed", p)
        assert (estate / "target" / "manifest.json").exists(), "dbt parse must produce target/manifest.json"

        # (6) Execute the generated estate locally. A parse proves the SQL is syntactically
        # available to dbt; this build also binds aliases and relations against DuckDB.
        build_env = os.environ.copy()
        for name in tuple(build_env):
            if name.startswith("DPF_SF_"):
                build_env.pop(name)
        build_env["DPF_DUCKDB_PATH"] = str(estate / "target" / "consumer_scaffold.duckdb")
        p = subprocess.run(
            [dbt, "build", "--profiles-dir", "profiles", "--no-partial-parse", "-t", "duckdb"],
            cwd=str(estate), capture_output=True, text=True, env=build_env,
        )
        if p.returncode != 0:
            _fail("`dbt build -t duckdb` against the scaffolded estate failed", p)
        assert (estate / "target" / "consumer_scaffold.duckdb").exists(), (
            "the consumer build must create its local DuckDB database"
        )
        with duckdb.connect(str(estate / "target" / "consumer_scaffold.duckdb"), read_only=True) as con:
            rows = con.execute(
                "select alpha_name, alpha_code "
                "from main_canonical.canonical_alpha order by alpha_code"
            ).fetchall()
        assert rows == [("Alpha One", "A1"), ("Alpha Two", "A2")], (
            "the consumer build must preserve each source record's canonical payload"
        )

        # (7) contracts + ODPS descriptor + graph map, all emitted inside the scaffolded
        # estate, all through the real CLI.
        for sub in ("contracts", "odps", "graph"):
            p = _run_cli([sub, "--estate-root", str(estate)], cwd=Path(tmp))
            if p.returncode != 0:
                _fail(f"`ergasterion {sub} --estate-root <estate>` failed", p)
        assert list((estate / "contracts").glob("*/canonical_alpha.odcs.yml")), "ODCS contract must be emitted"
        assert (estate / "contracts" / "odps" / "toyfixture.odps.yml").exists(), "ODPS descriptor must be emitted"
        # The graph emitter keys its output dir by the domains/*.yml FILE STEM ("fixture"),
        # a separate namespace from odcs.domain ("toyfixture", used by contracts/odps).
        assert list((estate / "graphs" / "fixture").glob("*")), "graph artefact suite must be emitted"

        # Third-party packages and dbt's compiled target are not authored scaffold output.
        skip_dirs = ("target", "dbt_packages")
        exclude = {
            p.resolve()
            for d in skip_dirs
            for p in (estate / d).rglob("*")
            if (estate / d).exists()
        }
        hits = _residue_hits(estate, exclude=exclude)
        assert not hits, f"domain-token residue in the emitted toy estate: {hits}"

    # (8) Zero engine edits: none of the above touched the engine tree under test.
    after_status = subprocess.run(
        ["git", "status", "--porcelain"], cwd=TREE_ROOT, capture_output=True, text=True,
    ).stdout
    assert before_status == after_status, (
        "the consumer smoke must make zero engine edits -- git status changed:\n"
        f"before:\n{before_status}\nafter:\n{after_status}"
    )


TESTS = [
    test_scaffold_structure_and_macro_fidelity,
    test_scaffold_package_data_is_current,
    test_scaffold_model_keys_are_engine_generic_layers,
    test_scaffold_missing_engine_data_fails_whole,
    test_scaffold_force_flag,
    test_scaffold_output_is_domain_token_clean,
    test_consumer_scaffold_toy_domain_emit_parse_contracts_odps_graph,
]


def main() -> int:
    failures = 0
    for test in TESTS:
        name = test.__name__
        try:
            test()
        except Exception:  # noqa: BLE001 - report and continue, exit code carries the signal
            failures += 1
            print(f"FAIL {name}")
            traceback.print_exc()
        else:
            print(f"PASS {name}")
    total = len(TESTS)
    print(f"{total - failures}/{total} passed")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
