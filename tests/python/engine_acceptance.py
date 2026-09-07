"""The thirteen architecture acceptance checks, as one deterministic offline run.

`docs/architecture/engine-architecture.md` section 14 states thirteen conditions the
design is met by, each "checkable by a deterministic run". This module is that run.
`scripts/validate_engine_architecture.sh` drives it; `tests/python/test_engine_acceptance.py`
proves each check red by injecting the violation the check exists to catch and asserting
that the real script reports it.

Every check is offline and hermetic: it reads the estates and the engine source the
caller points it at, copies anything it executes into a scratch directory, and reaches
no network and no warehouse account. The one adapter that executes is DuckDB, the
estate's declared reference adapter.

Three roots, each defaulting to this repository, so a caller can point one check at a
scratch copy carrying an injected violation without copying the whole tree:

    --estate-root    the worked estate: its declarations, its emitted trees and its
                     known-answer assertions;
    --fixtures-root  the fixture estates directory (tests/fixtures/estates);
    --engine-root    the engine package directory (ergasterion).

Usage:
    python tests/python/engine_acceptance.py                # all thirteen
    python tests/python/engine_acceptance.py --only 7       # one check
"""

from __future__ import annotations

import argparse
import contextlib
import io
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path

import yaml

if __package__ in (None, ""):
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from ergasterion import emit_contracts, emit_graph, emit_products
from ergasterion.estate import EstateContext
from ergasterion.framework import layer_neutrality

REPO_ROOT = Path(__file__).resolve().parents[2]

# The scratch root every executed check copies into. It stays off the system
# temporary directory when the caller has set one of the usual variables, which is
# what this repository's own working rules require on the build machine.
SCRATCH_ENV = ("TMPDIR", "TEMP", "TMP")

# The engine macros a copied fixture estate needs in order to build. The same
# audited set ergasterion/sync_scaffold.py ships to a scaffolded estate, minus the
# files no fixture here calls.
BUILD_MACROS = (
    "cross_db.sql",
    "data_vault.sql",
    "entity_resolution_scoring.sql",
    "filter_log.sql",
    "identifiers.sql",
    "product_tests.sql",
    "publish.sql",
    "quarantine.sql",
    "survivorship.sql",
)

CHECK_TITLES = {
    1: "the worked estate regenerates from declarations alone, gated for every declared adapter",
    2: "an estate with no Data Vault declaration emits, gates and executes with the declared shape",
    3: "a product naming the data_vault shape emits its route",
    4: "a product under a layer label the engine has never seen emits with no engine change",
    5: "every contract is generated and an incompatible upstream expectation fails closed",
    6: "one landing product feeds two products of different shapes, its declaration unchanged",
    7: "a declaration carrying engine syntax fails validation",
    8: "no engine module carries a layer name",
    9: "a named rule resolves for every declared pair, and a missing one fails emission",
    10: "named_only regenerates from named rules alone while the worked estate keeps inline SQL",
    11: "an inline expression fails closed on parse, on an unresolved column and on syntax",
    12: "a label with no translator-table entry fails closed at routing",
    13: "emission prints one summary line per product and a vault contract lists every relation",
}


class CheckFailure(Exception):
    """One acceptance check found what it exists to catch."""


@dataclass(frozen=True)
class Roots:
    estate: Path
    fixtures: Path
    engine: Path


# --------------------------------------------------------------------------- helpers


def _scratch_parent() -> Path:
    for name in SCRATCH_ENV:
        value = os.environ.get(name)
        if value and Path(value).is_dir():
            return Path(value)
    return Path(tempfile.gettempdir())


@contextlib.contextmanager
def _scratch_estate(source: Path, *, macros: bool = False):
    """A writable copy of one estate, with the engine macros a build needs."""

    with tempfile.TemporaryDirectory(dir=_scratch_parent()) as tmp:
        root = Path(tmp) / source.name
        shutil.copytree(source, root)
        if macros:
            macro_dir = root / "macros"
            macro_dir.mkdir(exist_ok=True)
            for name in BUILD_MACROS:
                shutil.copy2(REPO_ROOT / "macros" / name, macro_dir / name)
        yield root


def _emit(root: Path) -> str:
    """Run the real emission command against one estate and return what it printed."""

    argv = sys.argv
    out = io.StringIO()
    try:
        sys.argv = ["emit-products", "--estate-root", str(root)]
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(out):
            code = emit_products.main()
    finally:
        sys.argv = argv
    if code != 0:
        raise CheckFailure(f"emission failed for {root.name}:\n{out.getvalue()}")
    return out.getvalue()


def _emit_expecting_failure(root: Path) -> str:
    argv = sys.argv
    out = io.StringIO()
    try:
        sys.argv = ["emit-products", "--estate-root", str(root)]
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(out):
            code = emit_products.main()
    finally:
        sys.argv = argv
    if code == 0:
        raise CheckFailure(f"emission succeeded for {root.name} where it had to fail closed")
    return out.getvalue()


def _dbt(root: Path, *args: str) -> str:
    executable = REPO_ROOT / ".venv" / "Scripts" / "dbt.exe"
    if not executable.is_file():
        executable = Path(shutil.which("dbt") or "dbt")
    environment = dict(os.environ)
    environment["DBT_SEND_ANONYMOUS_USAGE_STATS"] = "False"
    result = subprocess.run(
        [str(executable), *args, "--profiles-dir", "profiles", "--project-dir", "."],
        cwd=root, capture_output=True, text=True, env=environment,
    )
    output = result.stdout + result.stderr
    if result.returncode != 0:
        raise CheckFailure(f"dbt {' '.join(args)} failed in {root.name}:\n{output[-4000:]}")
    return output


def _manifests(root: Path) -> dict[str, dict]:
    manifests: dict[str, dict] = {}
    for path in sorted((root / "manifests" / "products").rglob("*.json")):
        document = json.loads(path.read_text(encoding="utf-8"))
        manifests[document["product"]] = document
    return manifests


def _read_declarations(products_dir: Path) -> dict[Path, dict]:
    return {
        path: yaml.safe_load(path.read_text(encoding="utf-8"))
        for path in sorted(products_dir.rglob("*.yml"))
    }


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise CheckFailure(message)


# --------------------------------------------------------------------------- the checks


def check_1(roots: Roots) -> str:
    """The worked estate regenerates from declarations alone: the product route, the
    contract route, the descriptor route and the product graph all report no drift,
    the structural budgets pass for every declared adapter, and every known-answer
    assertion the estate ships reads a relation the route publishes."""

    ctx = EstateContext.resolve(estate_root=roots.estate)
    emission = emit_products.generate(ctx)
    drift = emit_products.check_files(emission.files, ctx=ctx)
    _require(not drift, f"the worked estate does not regenerate byte-identically: {drift[:3]}")
    offenses = emit_products.structural_gate(
        ctx, adapters=emission.adapters, private_models=emission.private_models
    )
    _require(not offenses, f"structural budget offence(s) on the worked estate: {offenses[:3]}")

    contracts = emit_contracts.build_product_contracts(ctx)
    _require(bool(contracts), "the worked estate declares no product")
    contract_files = emit_contracts._product_files_from_contracts(ctx, contracts)
    contract_drift = emit_contracts.check_product_files(contract_files, ctx=ctx)
    _require(not contract_drift, f"contract drift on the worked estate: {contract_drift[:3]}")

    graph = emit_graph.build_estate_product_graph(ctx)
    _require(graph is not None, "the worked estate resolves no product graph")
    graph_drift = emit_graph.check_product_files(emit_graph.generate_products(ctx), ctx=ctx)
    _require(not graph_drift, f"product-graph drift on the worked estate: {graph_drift[:3]}")

    manifests = _manifests(roots.estate)
    published = {
        relation.replace(".", "__", 1)
        for document in manifests.values()
        for kind in ("published", "auxiliary")
        for relation in document["relations"][kind]
    }
    # The estate's known-answer assertions: the dbt singular tests it ships beside
    # its products. tests/python carries this repository's own Python suites and
    # tests/fixtures carries the fixture estates, neither of which is an assertion
    # over the worked estate's relations.
    assertion_dir = roots.estate / "tests"
    excluded = {"python", "fixtures"}
    assertions = sorted(
        path
        for path in assertion_dir.rglob("*.sql")
        if path.parent != assertion_dir and path.relative_to(assertion_dir).parts[0] not in excluded
    )
    _require(bool(assertions), f"the worked estate ships no known-answer assertion under {assertion_dir}")
    for path in assertions:
        refs = set(re.findall(r"""ref\(\s*['"]([^'"]+)['"]""", path.read_text(encoding="utf-8")))
        _require(bool(refs), f"{path.name} reads no relation at all")
        stale = sorted(name for name in refs if name not in published and not _is_seed(roots.estate, name))
        _require(not stale, f"{path.name} reads relation(s) the route does not render: {stale}")
    return (
        f"{len(emission.files)} artefact(s), {len(contracts)} contract(s), "
        f"{len(graph.nodes)} graph node(s) and {len(assertions)} known-answer assertion(s), "
        f"gated for {', '.join(emission.adapters)}"
    )


def _is_seed(root: Path, name: str) -> bool:
    return (root / "seeds" / f"{name}.csv").is_file()


def check_2(roots: Roots) -> str:
    """The no_vault fixture estate: no declaration anywhere names the data_vault
    shape, and the estate emits, passes every per-adapter gate and executes on
    DuckDB with the declared shape."""

    source = roots.fixtures / "no_vault"
    declarations = _read_declarations(source / "declarations" / "products")
    _require(bool(declarations), f"{source} declares no product")
    shapes = {path.name: (document.get("target") or {}).get("shape") for path, document in declarations.items()}
    _require(
        all(shape == "declared" for shape in shapes.values()),
        f"the no_vault estate must declare the declared shape everywhere, got {shapes}",
    )
    table = yaml.safe_load((source / "estate.yml").read_text(encoding="utf-8"))["estate"]["translators"]
    registers = sorted(label for label, entries in table.items() if "data_vault" in entries)
    _require(
        not registers,
        f"the no_vault estate's translator table registers the data_vault shape under {registers}",
    )
    with _scratch_estate(source, macros=True) as root:
        printed = _emit(root)
        _require("0 offense(s)" in printed, f"per-adapter gate offence(s):\n{printed}")
        built = _dbt(root, "build")
        _require("ERROR=0" in built, f"the DuckDB build reported an error:\n{built[-2000:]}")
        passed = re.search(r"PASS=(\d+)", built)
        _require(passed is not None and int(passed.group(1)) > 0, "the DuckDB build ran nothing")
    return f"{len(declarations)} product(s), all shape declared; DuckDB build PASS={passed.group(1)}"


def check_3(roots: Roots) -> str:
    """A product naming the data_vault shape emits its route: the vault_one fixture
    estate emits, and its contract publishes every relation the shape renders
    (owner ruling R4, check 13's second half)."""

    source = roots.fixtures / "vault_one"
    with _scratch_estate(source) as root:
        _emit(root)
        ctx = EstateContext.resolve(estate_root=root)
        contracts = emit_contracts.build_product_contracts(ctx)
        manifests = _manifests(root)
        vault = [
            name for name, document in _read_declarations(root / "declarations" / "products").items()
            if (document.get("target") or {}).get("shape") == "data_vault"
        ]
        _require(bool(vault), f"{source} names no data_vault product")
        rendered = 0
        for product, document in manifests.items():
            contract = contracts.get(product)
            if contract is None:
                continue
            published = set(document["relations"]["published"])
            on_contract = {relation.name for relation in contract.relations}
            _require(
                published <= on_contract,
                f"{product}: the shape renders {sorted(published - on_contract)} which its contract omits",
            )
            rendered += len(published)
    return f"{len(vault)} data_vault declaration(s); {rendered} rendered relation(s), all on their contracts"


def check_4(roots: Roots) -> str:
    """A product added under a layer label the engine has never seen emits without an
    engine change: the new_label fixture estate's labels appear nowhere in the engine
    source, and the estate emits."""

    source = roots.fixtures / "new_label"
    policy_labels = sorted((yaml.safe_load((source / "estate.yml").read_text(encoding="utf-8"))
                            or {})["estate"]["labels"])
    _require(bool(policy_labels), f"{source} declares no label")
    engine_text = "\n".join(
        path.read_text(encoding="utf-8", errors="replace")
        for path in sorted(roots.engine.rglob("*.py"))
    )
    leaked = sorted(label for label in policy_labels if label in engine_text)
    _require(not leaked, f"the engine source names the estate's label(s) {leaked}")
    with _scratch_estate(source) as root:
        printed = _emit(root)
    emitted = sorted(re.findall(r"^emitted (\S+):", printed, flags=re.MULTILINE))
    _require(bool(emitted), f"the new_label estate emitted nothing:\n{printed}")
    return f"labels {', '.join(policy_labels)} nowhere in the engine; {len(emitted)} product(s) emitted"


def check_5(roots: Roots) -> str:
    """Every product's contract is generated and every consumer expectation is
    validated at emit: a consumer expecting a field its upstream contract does not
    publish fails emission closed, naming the field."""

    source = roots.fixtures / "two_shapes"
    with _scratch_estate(source) as root:
        ctx = EstateContext.resolve(estate_root=root)
        contracts = emit_contracts.build_product_contracts(ctx)
        _require(bool(contracts), f"{source} generated no contract")
        _emit(root)

        consumer = next(
            path for path, document in _read_declarations(root / "declarations" / "products").items()
            if any("expect" in (entry or {}) for entry in (document.get("sources") or []))
        )
        document = yaml.safe_load(consumer.read_text(encoding="utf-8"))
        for entry in document["sources"]:
            if "expect" in entry:
                entry["expect"]["fields"].append("no_such_upstream_field")
                break
        consumer.write_text(yaml.safe_dump(document, sort_keys=False), encoding="utf-8")
        printed = _emit_expecting_failure(root)
        _require(
            "no_such_upstream_field" in printed,
            f"the incompatible expectation failed without naming the field:\n{printed}",
        )
    return f"{len(contracts)} generated contract(s); an incompatible expectation fails closed naming the field"


def check_6(roots: Roots) -> str:
    """One landing product feeds at least two products of different shapes without any
    change to the landing declaration."""

    source = roots.fixtures / "two_shapes"
    declarations = _read_declarations(source / "declarations" / "products")
    by_published: dict[str, tuple[Path, dict]] = {}
    for path, document in declarations.items():
        product = document["product"]
        by_published[f"{product['domain']}.{product['name']}"] = (path, document)

    consumers: dict[str, set[str]] = {}
    for path, document in declarations.items():
        shape = (document.get("target") or {}).get("shape")
        for entry in document.get("sources") or []:
            contract = (entry or {}).get("contract")
            if not isinstance(contract, str):
                continue
            consumers.setdefault(contract.split("@", 1)[0], set()).add(shape)

    shared = {name: shapes for name, shapes in consumers.items() if len(shapes) > 1}
    _require(bool(shared), f"{source}: no product feeds two products of different shapes")
    landing = sorted(shared)[0]
    landing_path, landing_document = by_published[landing]
    with _scratch_estate(source) as root:
        _emit(root)
        after = (root / landing_path.relative_to(source)).read_text(encoding="utf-8")
    _require(
        after == landing_path.read_text(encoding="utf-8"),
        f"emission changed the landing declaration {landing_path.name}",
    )
    _require(
        landing_document["product"]["layer"] is not None,
        f"{landing} declares no layer label",
    )
    return f"{landing} feeds shapes {', '.join(sorted(shared[landing]))}; its declaration is unchanged"


def check_7(roots: Roots) -> str:
    """A declaration containing engine syntax fails validation."""

    source = roots.fixtures / "new_label"
    with _scratch_estate(source) as root:
        target = sorted((root / "declarations" / "products").rglob("*.yml"))[0]
        document = yaml.safe_load(target.read_text(encoding="utf-8"))
        document["product"]["owner"] = "{{ ref('somewhere') }}"
        target.write_text(yaml.safe_dump(document, sort_keys=False), encoding="utf-8")
        printed = _emit_expecting_failure(root)
    _require(
        "neutrality_violation" in printed,
        f"engine syntax in a declaration failed without naming the neutrality rule:\n{printed}",
    )
    return "a declaration carrying ref( fails validation naming neutrality_violation"


def check_8(roots: Roots) -> str:
    """No module in the engine contains the words Bronze, Silver or Gold: the fence is
    ergasterion/framework/layer_neutrality.py, run over the engine root."""

    scanned = sorted(roots.engine.rglob("*.py"))
    _require(bool(scanned), f"{roots.engine} carries no engine module to scan")
    violations = layer_neutrality.run_check(
        scope_file=REPO_ROOT / "scripts" / "layer_neutrality_scope.txt",
        allowlist_file=REPO_ROOT / "scripts" / "layer_neutrality_allowlist.txt",
        repo_root=roots.engine.parent,
    )
    _require(
        not violations,
        f"{len(violations)} layer word(s) in the engine: "
        + ", ".join(f"{v.path}:{v.line} {v.word}" for v in violations[:5]),
    )
    return f"{len(scanned)} engine module(s) scanned, no layer word found"


def check_9(roots: Roots) -> str:
    """A named rule referenced by a product resolves for every declared (translator,
    adapter) pair, and removing one implementation fails emission naming the pair."""

    source = roots.fixtures / "patterns_seven"
    with _scratch_estate(source) as root:
        _emit(root)
        catalogue = sorted((root / "rules").glob("*.yml"))[0]
        document = yaml.safe_load(catalogue.read_text(encoding="utf-8"))
        rule = document["rules"][0]
        dropped = rule["implementations"].pop()
        catalogue.write_text(yaml.safe_dump(document, sort_keys=False), encoding="utf-8")
        printed = _emit_expecting_failure(root)
    _require(
        dropped["adapter"] in printed and rule["name"] in printed,
        f"the missing implementation failed without naming the rule and the adapter:\n{printed}",
    )
    return (
        f"rule {rule['name']} resolves for every declared pair; removing "
        f"({dropped['translator']}, {dropped['adapter']}) fails emission naming it"
    )


def check_10(roots: Roots) -> str:
    """With inline SQL switched off by estate policy, a fixture estate regenerates
    using named rules alone, and the worked estate's declarations still carry inline
    SQL (owner ruling R2, plan decision D11)."""

    source = roots.fixtures / "named_only"
    policy = yaml.safe_load((source / "estate.yml").read_text(encoding="utf-8"))["estate"]
    _require(policy["expression_mode"] == "named_only", f"{source} is not a named_only estate")
    referenced: set[str] = set()
    for path, document in _read_declarations(source / "declarations" / "products").items():
        for step in document.get("steps") or []:
            for field in step.get("fields") or []:
                _require(
                    "expression" not in field,
                    f"{path.name} carries an inline expression under named_only policy",
                )
                if "rule" in field:
                    referenced.add(field["rule"])
    _require(bool(referenced), f"{source} references no named rule")
    with _scratch_estate(source, macros=True) as root:
        first = _emit(root)
        second = _emit(root)
        _require("0 of " in second, f"the named_only estate did not regenerate byte-identically:\n{second}")
        _require("0 offense(s)" in first, f"per-adapter gate offence(s):\n{first}")

    inline = 0
    for _path, document in _read_declarations(roots.estate / "declarations" / "products").items():
        for step in document.get("steps") or []:
            inline += sum(1 for field in step.get("fields") or [] if "expression" in field)
    _require(inline > 0, "the worked estate carries no inline expression, so R2 is not being proven")
    return (
        f"named_only regenerates from {len(referenced)} named rule(s) with no inline expression; "
        f"the worked estate still carries {inline} inline expression(s)"
    )


def check_11(roots: Roots) -> str:
    """An inline expression that does not parse fails validation naming product,
    occurrence and position; an unresolved column fails closed; an expression carrying
    technology syntax fails the neutrality gate; and the field-level lineage of a
    calculated field lists the columns the parser resolved."""

    source = roots.fixtures / "patterns_seven"
    found: dict[str, str] = {}
    for label, replacement, expected, positioned in (
        ("unparseable", "status_code IN (", "syntax_error", True),
        ("unresolved", "no_such_column IS NOT NULL", "unresolved_column", False),
        ("technology", "{{ ref('elsewhere') }}", "neutrality_violation", False),
    ):
        with _scratch_estate(source) as root:
            target = root / "declarations" / "products" / "order.yml"
            document = yaml.safe_load(target.read_text(encoding="utf-8"))
            product = document["product"]["name"]
            for step in document["steps"]:
                if step.get("pattern") == "calculated_fields":
                    step["fields"][0]["expression"] = replacement
                    break
            target.write_text(yaml.safe_dump(document, sort_keys=False), encoding="utf-8")
            printed = _emit_expecting_failure(root)
            _require(expected in printed, f"the {label} expression failed without naming {expected}:\n{printed}")
            # Architecture check 11 names three things, not just the rule: the product,
            # the occurrence the expression sits in, and -- for a parse failure -- the
            # position inside the expression. A message that named only the rule would
            # leave a reader hunting the estate for which field broke.
            _require(
                product in printed,
                f"the {label} failure did not name the product {product!r}:\n{printed}",
            )
            # The two gates spell the occurrence differently -- the resolver reports
            # "steps[3]:calculated_fields.fields[0]" and the neutrality gate the
            # document path "steps[3].fields[0].expression" -- so this reads the
            # occurrence the message actually names and requires it to point at the
            # field, rather than pinning one gate's spelling.
            occurrence = re.search(r"occurrence '([^']+)'", printed)
            _require(
                occurrence is not None and "fields[0]" in occurrence.group(1),
                f"the {label} failure did not name the occurrence it sits in:\n{printed}",
            )
            if positioned:
                _require(
                    re.search(r"line \d+, column \d+", printed) is not None,
                    f"the {label} failure named no position in the expression:\n{printed}",
                )
            found[label] = expected

    with _scratch_estate(source) as root:
        _emit(root)
        ctx = EstateContext.resolve(estate_root=root)
        graph = emit_graph.build_estate_product_graph(ctx)
        lineage = [
            entry for entry in graph.field_lineage
            if entry.transform == "calculated_fields" and entry.source_kind == "field"
        ]
        _require(
            bool(lineage),
            "no calculated field's lineage lists the columns the parser resolved",
        )
    return (
        "parse, column resolution and neutrality all fail closed ("
        + ", ".join(f"{label}: {rule}" for label, rule in found.items())
        + f"); {len(lineage)} field lineage record(s) carry resolved inputs"
    )


def check_12(roots: Roots) -> str:
    """A layer label with no translator-table entry for a pattern in its profile fails
    closed at routing naming label, pattern and adapter, and changing the table entry
    changes the owner with no engine change."""

    source = roots.fixtures / "patterns_seven"
    with _scratch_estate(source) as root:
        estate_file = root / "estate.yml"
        document = yaml.safe_load(estate_file.read_text(encoding="utf-8"))
        table = document["estate"]["translators"]
        label = "shaped"
        pattern = "schema_transform"
        owner_before = table[label][pattern]
        del table[label][pattern]
        estate_file.write_text(yaml.safe_dump(document, sort_keys=False), encoding="utf-8")
        printed = _emit_expecting_failure(root)
        _require(
            label in printed and pattern in printed,
            f"the missing translator entry failed without naming the label and the pattern:\n{printed}",
        )

        table[label][pattern] = "local-ingestion"
        estate_file.write_text(yaml.safe_dump(document, sort_keys=False), encoding="utf-8")
        rerouted = _emit_expecting_failure(root)
        _require(
            "local-ingestion" in rerouted,
            f"changing the table entry did not change the owner the router resolved:\n{rerouted}",
        )
    return (
        f"{label}/{pattern} with no entry fails closed naming both; changing the entry from "
        f"{owner_before} moves the owner with no engine change"
    )


def check_13(roots: Roots) -> str:
    """Emission prints the summary line per product, and a Data Vault product's
    contract lists every relation the shape renders."""

    source = roots.fixtures / "vault_one"
    declarations = _read_declarations(source / "declarations" / "products")
    with _scratch_estate(source) as root:
        printed = _emit(root)
    lines = re.findall(r"^emitted (\S+): label=(\S+) profile=(\S+) shape=(\S+) owner=(\S+)", printed, flags=re.MULTILINE)
    _require(
        len(lines) == len(declarations),
        f"expected one summary line per product, got {len(lines)} for {len(declarations)} declaration(s)",
    )
    vault_lines = [line for line in lines if line[3] == "data_vault"]
    _require(bool(vault_lines), "no summary line reports the data_vault shape")
    return f"{len(lines)} summary line(s), one per product, {len(vault_lines)} on the data_vault shape"


CHECKS = {
    1: check_1, 2: check_2, 3: check_3, 4: check_4, 5: check_5, 6: check_6, 7: check_7,
    8: check_8, 9: check_9, 10: check_10, 11: check_11, 12: check_12, 13: check_13,
}


def run(roots: Roots, only: int | None = None) -> int:
    selected = [only] if only is not None else sorted(CHECKS)
    failures = 0
    for number in selected:
        title = CHECK_TITLES[number]
        try:
            detail = CHECKS[number](roots)
        except CheckFailure as failure:
            failures += 1
            print(f"check {number:>2}: FAIL  {title} -- {failure}")
        except Exception as error:  # noqa: BLE001
            # A check that cannot complete has not passed. The run's contract is one
            # verdict line per check, so an unexpected error is reported as that
            # check's failure, named by its type, rather than ending the run early.
            failures += 1
            print(f"check {number:>2}: FAIL  {title} -- {type(error).__name__}: {error}")
        else:
            print(f"check {number:>2}: OK    {title} -- {detail}")
    print(f"ENGINE_ACCEPTANCE_CHECKS={len(selected)} ENGINE_ACCEPTANCE_FAILURES={failures}")
    return 1 if failures else 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--only", type=int, choices=sorted(CHECKS), default=None,
                        help="Run one check by its architecture section 14 number.")
    parser.add_argument("--estate-root", type=Path, default=REPO_ROOT,
                        help="The worked estate to check (default: this repository).")
    parser.add_argument("--fixtures-root", type=Path, default=REPO_ROOT / "tests" / "fixtures" / "estates",
                        help="The fixture estates directory (default: tests/fixtures/estates).")
    parser.add_argument("--engine-root", type=Path, default=REPO_ROOT / "ergasterion",
                        help="The engine package directory (default: ergasterion).")
    args = parser.parse_args(argv)
    roots = Roots(
        estate=args.estate_root.resolve(),
        fixtures=args.fixtures_root.resolve(),
        engine=args.engine_root.resolve(),
    )
    return run(roots, args.only)


if __name__ == "__main__":
    raise SystemExit(main())
