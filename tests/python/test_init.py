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

TREE_ROOT = str(REPO_ROOT)


# ---------------------------------------------------------------------------
# Domain-token residue vocabulary for the generic consumer scaffold:
# the eight source-system brand tokens plus the entity nouns the worked estate declares
# under declarations/products/. Compound/specific forms only for the
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
        assert scaffolded["models"] == engine["models"], (
            "the engine-generic models block must survive the strip unchanged"
        )

        # macros/: byte-identical copy of the explicit engine-generic allow-set.
        import ergasterion.sync_scaffold as sync_scaffold
        engine_macros = [REPO_ROOT / "macros" / name for name in sync_scaffold.SCAFFOLD_MACROS]
        scaffold_macros = sorted((dest / "macros").rglob("*.sql"))
        assert sorted(p.name for p in engine_macros) == [p.name for p in scaffold_macros], "macro file set must match the generic allow-set"
        engine_macros = sorted(engine_macros)
        for src, copy in zip(engine_macros, scaffold_macros):
            assert src.read_bytes() == copy.read_bytes(), f"{copy.name}: macro copy must be byte-identical to the engine source"

        # packages.yml: byte-identical copy.
        assert (dest / "packages.yml").read_bytes() == (REPO_ROOT / "packages.yml").read_bytes()

        # profiles/profiles.yml: derived from the engine's own file -- everything except
        # the DuckDB target's default path is byte-identical, and that one path is fixed
        # under the tracked runtime/local.yml binding's data root.
        scaffolded_profiles = (dest / "profiles" / "profiles.yml").read_text(encoding="utf-8")
        engine_profiles = (REPO_ROOT / "profiles" / "profiles.yml").read_text(encoding="utf-8")
        assert scaffolded_profiles != engine_profiles
        assert "runtime/data/ergasterion.duckdb" in scaffolded_profiles
        assert "target/ergasterion.duckdb" not in scaffolded_profiles
        for other_line in engine_profiles.splitlines():
            if "target/ergasterion.duckdb" in other_line:
                continue
            assert other_line in scaffolded_profiles.splitlines(), (
                f"unexpected drift outside the DuckDB path override: {other_line!r}"
            )

        # estate.yml, .gitignore, and runtime/local.yml are required scaffold surfaces.
        estate_yml = yaml.safe_load((dest / "estate.yml").read_text(encoding="utf-8"))
        assert estate_yml["estate"]["namespace"] == "com.example.ergasterion"

        gitignore_lines = [
            line.strip() for line in (dest / ".gitignore").read_text(encoding="utf-8").splitlines()
            if line.strip() and not line.strip().startswith("#")
        ]
        assert gitignore_lines == ["runtime/data/"], (
            f"the scaffold .gitignore must ignore only runtime/data/, got: {gitignore_lines}"
        )

        binding = yaml.safe_load((dest / "runtime" / "local.yml").read_text(encoding="utf-8"))
        assert binding["schema"] == "ergasterion.runtime-binding/v1"
        assert binding["environment"] == "local"
        assert binding["runtime_resources"]["max_parallel_attempts"] == 1
        assert set(binding["ports"]) == {
            "source_connector", "raw_store", "scratch_store", "state_store", "landing_adapter",
            "remediation_repository", "projection_publisher", "lifecycle_sink", "key_resolver",
        }, "the binding must name all nine local ports"
        for digest_field in ("contract_digest", "execution_plan_digest"):
            assert len(binding[digest_field]) == 64, f"{digest_field} must be a real computed sha256 digest"

        # Empty dirs, each seeded with .gitkeep. declarations/ also carries the
        # copied targets/ budget declarations and the seeded products/ tree
        # (both asserted below); everything else ships empty.
        for name in ("declarations", "seeds", "tests"):
            d = dest / name
            assert d.is_dir() and (d / ".gitkeep").exists(), f"{name}/ must exist with .gitkeep"
            allowed = {".gitkeep"} | ({"targets", "products"} if name == "declarations" else set())
            extras = [p for p in d.iterdir() if p.name not in allowed]
            assert not extras, f"{name}/ must ship with no other content, got: {extras}"

        # The seeded product declaration: byte-identical to the packaged
        # scaffold copy, which sync_scaffold renders from
        # ergasterion/templates/product_seed.yml.j2 and its constants.
        seeded = sorted((dest / "declarations" / "products").glob("*.yml"))
        packaged = sorted((init_mod._SCAFFOLD_ROOT / "products").glob("*.yml"))
        assert [p.name for p in seeded] == [p.name for p in packaged] and seeded, (
            "the seeded product declaration set must match the packaged scaffold's"
        )
        for src, copy in zip(packaged, seeded):
            assert src.read_bytes() == copy.read_bytes(), (
                f"{copy.name}: seeded product declaration must be byte-identical to the packaged one"
            )
        seed_document = yaml.safe_load(seeded[0].read_text(encoding="utf-8"))
        estate_labels = estate_yml["estate"]["labels"]
        assert seed_document["product"]["layer"] in estate_labels, (
            "the seeded product's label must be one estate.yml declares"
        )

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
        for command in ("ergasterion emit-products", "ergasterion contracts",
                        "ergasterion odps", "ergasterion product-graph"):
            assert command in getting_started, (
                f"the doc must name {command!r}, one of the commands a scaffolded estate runs"
            )
        assert "target/manifest.json" not in getting_started, (
            "no command reads a compiled dbt manifest any more, so the doc must not send a "
            "reader to copy one: contracts, descriptors and the product graph are all built "
            "from the product declarations"
        )


def test_scaffolded_estate_emits_through_the_product_route_out_of_the_box() -> None:
    """A fresh estate is a working product estate, not only a working dbt
    project: the seeded declaration validates, emits through the real
    ``ergasterion emit-products`` command, prints its summary line, and the
    same command in check mode reports no drift against what it wrote."""

    with tempfile.TemporaryDirectory(prefix="ergasterion-init-products-") as tmp:
        dest = Path(tmp) / "estate"
        init_mod.scaffold(dest)

        proc = _run_cli(["emit-products", "--estate-root", str(dest)], cwd=dest)
        if proc.returncode != 0:
            _fail("`ergasterion emit-products` failed against a freshly scaffolded estate", proc)
        seeded = sorted((dest / "declarations" / "products").glob("*.yml"))
        document = yaml.safe_load(seeded[0].read_text(encoding="utf-8"))["product"]
        published = f"{document['domain']}.{document['name']}"
        assert f"emitted {published}: label={document['layer']} profile=" in proc.stdout, proc.stdout
        assert (dest / "models" / "products").is_dir(), "the route wrote no models tree"
        contract_dir = dest / "contracts" / "products" / document["domain"] / document["name"]
        assert (contract_dir / "contract.json").is_file(), "the route wrote no product contract"
        assert list(contract_dir.glob("*.odcs.yml")), "the route wrote no relation contract"
        assert (contract_dir / f"{document['name']}.odps.yml").is_file(), (
            "the route wrote no product descriptor"
        )
        assert (dest / "graphs" / "products" / "product-graph.json").is_file(), (
            "the route wrote no product graph"
        )
        # The estate's own structural budgets, run by the route once per
        # declared adapter over the tree it just wrote.
        assert "structural budgets: 0 offense(s) over 2 declared adapter(s)" in proc.stdout, proc.stdout

        proc = _run_cli(["emit-products", "--estate-root", str(dest), "--check"], cwd=dest)
        if proc.returncode != 0:
            _fail("`ergasterion emit-products --check` reported drift on what it had just written", proc)
        assert "0 problem(s)" in proc.stdout, proc.stdout
        assert "structural budgets: 0 offense(s) over 2 declared adapter(s)" in proc.stdout, proc.stdout
        for message in (
            "ODCS contract gate OK",
            "ODPS (Bitol) descriptor gate OK",
            "product-graph gate OK",
        ):
            assert message in proc.stdout, f"{message!r} missing from output:\n{proc.stdout}"


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
    """The scaffold dbt_project.yml carries the physical-design default and no
    per-layer key. The product route writes an explicit config into every model it
    renders, so a per-layer key here would hand a consumer materialisation config for
    a path they never declared and the route would override it anyway."""
    scaffolded = yaml.safe_load(
        (REPO_ROOT / "ergasterion" / "scaffold" / "dbt_project.yml").read_text(encoding="utf-8")
    )
    block = scaffolded["models"]["ergasterion"]
    assert block == {"+materialized": "table"}, (
        f"the scaffold models block must be the physical-design default alone, got: {block}"
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


TESTS = [
    test_scaffold_structure_and_macro_fidelity,
    test_scaffolded_estate_emits_through_the_product_route_out_of_the_box,
    test_scaffold_package_data_is_current,
    test_scaffold_model_keys_are_engine_generic_layers,
    test_scaffold_missing_engine_data_fails_whole,
    test_scaffold_force_flag,
    test_scaffold_output_is_domain_token_clean,
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
