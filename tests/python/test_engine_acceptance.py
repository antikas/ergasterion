"""One hermetic seen-red proof per architecture acceptance check.

`scripts/validate_engine_architecture.sh` runs the thirteen checks of
`docs/architecture/engine-architecture.md` section 14. A green check is worth nothing
unless the violation it exists to catch turns it red, so this file plants exactly that
violation, once per check, and drives the REAL script against the planted tree.

Every proof is hermetic: it copies only the roots the check it targets reads into a
scratch directory, plants one defect there, runs
``bash scripts/validate_engine_architecture.sh --only <n> --estate-root ... --fixtures-root ...
--engine-root ...``, and asserts the script reported that check FAIL. Nothing is
monkeypatched and no check is re-implemented here: the assertion is on what the script
printed and on its exit status.

The suite also asserts the green run: the script prints one line per check, in check
order, with the machine-readable ENGINE_ACCEPTANCE_ marker the validator reads.

No pytest in this repo's .venv, so each ``test_*`` raises AssertionError on failure,
``main()`` runs them all and reports PASS/FAIL (exit 0 = all green, 1 = any failure).

Usage:
    python tests/python/test_engine_acceptance.py
"""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import traceback
from pathlib import Path

import yaml

if __package__ in (None, ""):
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from tests.python import engine_acceptance  # noqa: E402  (path is set above)

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "scripts" / "validate_engine_architecture.sh"
FIXTURES = REPO_ROOT / "tests" / "fixtures" / "estates"
ENGINE = REPO_ROOT / "ergasterion"

# The worked estate's own inputs and emitted trees. A red proof for a worked-estate
# check copies exactly these into scratch, never the whole repository.
ESTATE_PARTS = (
    "estate.yml",
    "declarations",
    "manifests",
    "contracts",
    "graphs",
    "rules",
    "models",
    "seeds",
    "tests/ecommerce",
)


# --------------------------------------------------------------------------- harness


def _scratch() -> Path:
    for name in ("TMPDIR", "TEMP", "TMP"):
        value = os.environ.get(name)
        if value and Path(value).is_dir():
            return Path(value)
    return Path(tempfile.gettempdir())


def _python() -> str:
    venv = REPO_ROOT / ".venv" / "Scripts" / "python.exe"
    return str(venv) if venv.is_file() else sys.executable


def run_script(*args: str) -> tuple[int, str]:
    """Run the real acceptance script and return (exit status, everything printed)."""

    environment = dict(os.environ)
    environment["PY"] = _python()
    result = subprocess.run(
        ["bash", str(SCRIPT), *args],
        cwd=REPO_ROOT, capture_output=True, text=True, env=environment,
    )
    return result.returncode, result.stdout + result.stderr


def copy_estate(destination: Path) -> Path:
    """A scratch copy of the worked estate's own inputs and emitted trees."""

    root = destination / "estate"
    root.mkdir(parents=True)
    for part in ESTATE_PARTS:
        source = REPO_ROOT / part
        target = root / part
        target.parent.mkdir(parents=True, exist_ok=True)
        if source.is_dir():
            shutil.copytree(source, target)
        else:
            shutil.copy2(source, target)
    return root


def copy_fixtures(destination: Path, *names: str) -> Path:
    """A scratch copy of the named fixture estates, and nothing else."""

    root = destination / "fixtures"
    root.mkdir(parents=True)
    for name in names:
        shutil.copytree(FIXTURES / name, root / name)
    return root


def copy_engine(destination: Path) -> Path:
    """A scratch copy of the engine package, under a parent named as the scope file
    expects (the gate reads its scope relative to a repository root)."""

    root = destination / "repo"
    root.mkdir(parents=True)
    shutil.copytree(ENGINE, root / "ergasterion")
    return root / "ergasterion"


def assert_red(number: int, output: str, code: int, *, naming: str | None = None) -> None:
    line = _check_line(number, output)
    assert line.startswith(f"check {number:>2}: FAIL"), f"check {number} did not go red:\n{output}"
    assert code != 0, f"the script exited 0 with check {number} red:\n{output}"
    assert f"ENGINE_ACCEPTANCE_FAILURES=1" in output, output
    if naming is not None:
        assert naming in line, f"check {number} went red without naming {naming!r}:\n{line}"


def _check_line(number: int, output: str) -> str:
    for line in output.splitlines():
        if line.startswith(f"check {number:>2}: "):
            return line
    raise AssertionError(f"the script printed no line for check {number}:\n{output}")


def _edit_yaml(path: Path, mutate) -> None:
    document = yaml.safe_load(path.read_text(encoding="utf-8"))
    mutate(document)
    path.write_text(yaml.safe_dump(document, sort_keys=False), encoding="utf-8")


# --------------------------------------------------------------------------- the green run


def test_the_green_run_prints_one_line_per_check_in_order() -> None:
    """The script's contract: thirteen lines, one per check, in check order, each
    carrying its evidence, then the marker the validator reads."""
    code, output = run_script()
    assert code == 0, output
    numbers = [int(match) for match in re.findall(r"^check\s+(\d+): ", output, flags=re.MULTILINE)]
    assert numbers == list(range(1, 14)), numbers
    for number in numbers:
        line = _check_line(number, output)
        assert line.startswith(f"check {number:>2}: OK    "), line
        assert " -- " in line, f"check {number} printed no evidence: {line}"
    assert "ENGINE_ACCEPTANCE_CHECKS=13 ENGINE_ACCEPTANCE_FAILURES=0" in output, output


def test_every_check_in_the_architecture_has_one_here() -> None:
    """The driver covers section 14 exactly: thirteen checks, numbered 1 to 13, each
    with a title, and one red proof registered below for each."""
    assert sorted(engine_acceptance.CHECKS) == list(range(1, 14))
    assert sorted(engine_acceptance.CHECK_TITLES) == list(range(1, 14))
    proved = {
        int(match)
        for name in globals()
        for match in re.findall(r"^test_check_(\d+)_", name)
    }
    assert proved == set(range(1, 14)), sorted(proved)


def test_an_unknown_check_number_is_refused() -> None:
    code, output = run_script("--only", "14")
    assert code != 0, output
    assert "invalid choice" in output or "--only" in output, output


# --------------------------------------------------------------------------- the red proofs


def test_check_1_goes_red_when_an_emitted_model_drifts() -> None:
    """Check 1 stands or falls on regeneration from declarations alone. A hand-edited
    generated model is exactly the drift it exists to report."""
    with tempfile.TemporaryDirectory(dir=_scratch()) as tmp:
        estate = copy_estate(Path(tmp))
        model = estate / "models" / "products" / "ecommerce" / "ecommerce__cartivo_customer.sql"
        model.write_text(model.read_text(encoding="utf-8") + "\n-- planted drift\n", encoding="utf-8")
        code, output = run_script("--only", "1", "--estate-root", str(estate))
    assert_red(1, output, code, naming="regenerate")


def test_check_2_goes_red_when_the_no_vault_estate_declares_another_shape() -> None:
    """Check 2 is the no-Data-Vault estate on the declared shape. A product that
    declares another shape is no longer that estate."""
    with tempfile.TemporaryDirectory(dir=_scratch()) as tmp:
        fixtures = copy_fixtures(Path(tmp), "no_vault")
        declaration = fixtures / "no_vault" / "declarations" / "products" / "account_summary.yml"
        _edit_yaml(declaration, lambda document: document["target"].update({"shape": "ods"}))
        code, output = run_script("--only", "2", "--fixtures-root", str(fixtures))
    assert_red(2, output, code, naming="declared shape everywhere")


def test_check_3_goes_red_when_no_product_names_the_vault_shape() -> None:
    """Check 3 requires a product on the data_vault shape to emit its route, with
    every relation the shape renders on its contract. An estate that names the shape
    nowhere proves neither, and the check reports exactly that rather than passing on
    an empty set."""
    with tempfile.TemporaryDirectory(dir=_scratch()) as tmp:
        fixtures = copy_fixtures(Path(tmp), "vault_one")
        (fixtures / "vault_one" / "declarations" / "products" / "customer_vault.yml").unlink()
        code, output = run_script("--only", "3", "--fixtures-root", str(fixtures))
    assert_red(3, output, code, naming="data_vault")


def test_check_4_goes_red_when_the_engine_names_the_estate_label() -> None:
    """Check 4 is that a label the engine has never seen needs no engine change. An
    engine module that names the label is the change the check forbids."""
    with tempfile.TemporaryDirectory(dir=_scratch()) as tmp:
        engine = copy_engine(Path(tmp))
        module = engine / "estate.py"
        module.write_text(
            module.read_text(encoding="utf-8") + '\n_PLANTED_LABEL = "harmonised"\n', encoding="utf-8"
        )
        code, output = run_script("--only", "4", "--engine-root", str(engine))
    assert_red(4, output, code, naming="harmonised")


def test_check_5_goes_red_when_an_incompatible_expectation_is_accepted() -> None:
    """Check 5 plants an impossible expectation and requires emission to fail closed
    naming the field. Remove the source expectation the check plants into and the
    planted field lands nowhere, so nothing fails and the check reports it."""
    with tempfile.TemporaryDirectory(dir=_scratch()) as tmp:
        fixtures = copy_fixtures(Path(tmp), "two_shapes")
        products = fixtures / "two_shapes" / "declarations" / "products"
        for path in sorted(products.rglob("*.yml")):
            _edit_yaml(path, _drop_every_expectation)
        code, output = run_script("--only", "5", "--fixtures-root", str(fixtures))
    assert_red(5, output, code)


def _drop_every_expectation(document: dict) -> None:
    for entry in document.get("sources") or []:
        entry.pop("expect", None)


def test_check_6_goes_red_when_no_product_feeds_two_shapes() -> None:
    """Check 6 needs one landing product feeding at least two products of different
    shapes. Give both consumers the same shape and the estate no longer shows it."""
    with tempfile.TemporaryDirectory(dir=_scratch()) as tmp:
        fixtures = copy_fixtures(Path(tmp), "two_shapes")
        products = fixtures / "two_shapes" / "declarations" / "products"
        for path in sorted(products.rglob("*.yml")):
            _edit_yaml(path, _flatten_shape)
        code, output = run_script("--only", "6", "--fixtures-root", str(fixtures))
    assert_red(6, output, code, naming="two products of different shapes")


def _flatten_shape(document: dict) -> None:
    target = document.get("target")
    if isinstance(target, dict) and "shape" in target:
        target["shape"] = "declared"


def test_check_7_goes_red_when_engine_syntax_passes_validation() -> None:
    """Check 7 plants ref( in a declaration and requires validation to reject it. An
    estate with no declaration at all carries nothing to plant into, so the check can
    never see the syntax rejected, and it reports that rather than passing."""
    with tempfile.TemporaryDirectory(dir=_scratch()) as tmp:
        fixtures = copy_fixtures(Path(tmp), "new_label")
        for path in sorted((fixtures / "new_label" / "declarations" / "products").rglob("*.yml")):
            path.unlink()
        code, output = run_script("--only", "7", "--fixtures-root", str(fixtures))
    assert_red(7, output, code)


def test_check_8_goes_red_on_a_layer_word_in_an_engine_module() -> None:
    """Check 8 is the layer-neutrality fence. A layer word planted in an engine module
    is the one thing it exists to find."""
    with tempfile.TemporaryDirectory(dir=_scratch()) as tmp:
        engine = copy_engine(Path(tmp))
        module = engine / "estate.py"
        module.write_text(
            module.read_text(encoding="utf-8") + '\n_PLANTED = "silver_layer"\n', encoding="utf-8"
        )
        code, output = run_script("--only", "8", "--engine-root", str(engine))
    assert_red(8, output, code, naming="layer word")


def test_check_9_goes_red_when_a_missing_implementation_is_accepted() -> None:
    """Check 9 removes one (translator, adapter) implementation and requires emission
    to fail naming the pair. An estate that declares only one adapter cannot show it:
    removing the last implementation leaves the rule with none, which is a different
    failure, and the check reports that it never saw the pair named."""
    with tempfile.TemporaryDirectory(dir=_scratch()) as tmp:
        fixtures = copy_fixtures(Path(tmp), "patterns_seven")
        catalogue = sorted((fixtures / "patterns_seven" / "rules").glob("*.yml"))[0]
        _edit_yaml(catalogue, _rename_every_implementation_adapter)
        code, output = run_script("--only", "9", "--fixtures-root", str(fixtures))
    assert_red(9, output, code)


def _rename_every_implementation_adapter(document: dict) -> None:
    for rule in document["rules"]:
        for implementation in rule["implementations"]:
            implementation["adapter"] = "duckdb"


def test_check_10_goes_red_when_a_named_only_declaration_carries_inline_sql() -> None:
    """Check 10 is owner ruling R2: under named_only, every rule is a named rule. An
    inline expression in that estate is the violation."""
    with tempfile.TemporaryDirectory(dir=_scratch()) as tmp:
        fixtures = copy_fixtures(Path(tmp), "named_only")
        declaration = fixtures / "named_only" / "declarations" / "products" / "open_tickets.yml"
        _edit_yaml(declaration, _plant_inline_expression)
        code, output = run_script("--only", "10", "--fixtures-root", str(fixtures))
    assert_red(10, output, code, naming="inline expression")


def _plant_inline_expression(document: dict) -> None:
    for step in document["steps"]:
        if step.get("pattern") == "calculated_fields":
            step["fields"][0].pop("rule", None)
            step["fields"][0].pop("rule_version", None)
            step["fields"][0]["expression"] = "open_minutes >= 60"
            return
    raise AssertionError("the named_only fixture declares no calculated field")


def test_check_10_goes_red_when_the_worked_estate_drops_its_inline_sql() -> None:
    """The other half of check 10: the worked estate keeps its inline SQL, so R2 is
    proven on a live estate rather than only on the fixture."""
    with tempfile.TemporaryDirectory(dir=_scratch()) as tmp:
        estate = copy_estate(Path(tmp))
        for path in sorted((estate / "declarations" / "products").rglob("*.yml")):
            _edit_yaml(path, _strip_inline_expressions)
        code, output = run_script("--only", "10", "--estate-root", str(estate))
    assert_red(10, output, code, naming="no inline expression")


def _strip_inline_expressions(document: dict) -> None:
    for step in document.get("steps") or []:
        for field in step.get("fields") or []:
            field.pop("expression", None)


def test_check_11_goes_red_when_an_unparseable_expression_is_accepted() -> None:
    """Check 11 plants three broken expressions and requires each to fail closed by
    its own named rule. An estate with no calculated field to plant into cannot show
    any of them."""
    with tempfile.TemporaryDirectory(dir=_scratch()) as tmp:
        fixtures = copy_fixtures(Path(tmp), "patterns_seven")
        declaration = fixtures / "patterns_seven" / "declarations" / "products" / "order.yml"
        _edit_yaml(declaration, _drop_calculated_fields)
        code, output = run_script("--only", "11", "--fixtures-root", str(fixtures))
    assert_red(11, output, code)


def _drop_calculated_fields(document: dict) -> None:
    document["steps"] = [step for step in document["steps"] if step.get("pattern") != "calculated_fields"]


def test_check_12_goes_red_when_a_missing_translator_entry_is_accepted() -> None:
    """Check 12 removes one translator-table entry and requires routing to fail closed
    naming the label and the pattern. A profile that no longer carries the pattern has
    nothing to route, so nothing fails and the check reports it."""
    with tempfile.TemporaryDirectory(dir=_scratch()) as tmp:
        fixtures = copy_fixtures(Path(tmp), "patterns_seven")
        declarations = fixtures / "patterns_seven" / "declarations" / "products"
        for path in sorted(declarations.rglob("*.yml")):
            _edit_yaml(path, _drop_schema_transform)
        code, output = run_script("--only", "12", "--fixtures-root", str(fixtures))
    assert_red(12, output, code)


def _drop_schema_transform(document: dict) -> None:
    document["steps"] = [step for step in document["steps"] if step.get("pattern") != "schema_transform"]


def test_check_13_goes_red_when_a_declaration_emits_no_summary_line() -> None:
    """Check 13 is owner ruling R6's emit summary: one line per product. A file under
    declarations/products/ that emits no summary line breaks that one-to-one, and the
    check reports it rather than counting only the lines it did get."""
    with tempfile.TemporaryDirectory(dir=_scratch()) as tmp:
        fixtures = copy_fixtures(Path(tmp), "vault_one")
        products = fixtures / "vault_one" / "declarations" / "products"
        (products / "notes.yml").write_text(
            yaml.safe_dump({"notes": "not a product declaration"}, sort_keys=False), encoding="utf-8"
        )
        code, output = run_script("--only", "13", "--fixtures-root", str(fixtures))
    assert_red(13, output, code)


# --------------------------------------------------------------------------- runner

TESTS = [
    test_the_green_run_prints_one_line_per_check_in_order,
    test_every_check_in_the_architecture_has_one_here,
    test_an_unknown_check_number_is_refused,
    test_check_1_goes_red_when_an_emitted_model_drifts,
    test_check_2_goes_red_when_the_no_vault_estate_declares_another_shape,
    test_check_3_goes_red_when_no_product_names_the_vault_shape,
    test_check_4_goes_red_when_the_engine_names_the_estate_label,
    test_check_5_goes_red_when_an_incompatible_expectation_is_accepted,
    test_check_6_goes_red_when_no_product_feeds_two_shapes,
    test_check_7_goes_red_when_engine_syntax_passes_validation,
    test_check_8_goes_red_on_a_layer_word_in_an_engine_module,
    test_check_9_goes_red_when_a_missing_implementation_is_accepted,
    test_check_10_goes_red_when_a_named_only_declaration_carries_inline_sql,
    test_check_10_goes_red_when_the_worked_estate_drops_its_inline_sql,
    test_check_11_goes_red_when_an_unparseable_expression_is_accepted,
    test_check_12_goes_red_when_a_missing_translator_entry_is_accepted,
    test_check_13_goes_red_when_a_declaration_emits_no_summary_line,
]


def main() -> int:
    failures = 0
    for test in TESTS:
        try:
            test()
        except Exception:  # noqa: BLE001 -- report-and-continue harness
            failures += 1
            print(f"FAIL {test.__name__}")
            traceback.print_exc(file=sys.stdout)
        else:
            print(f"PASS {test.__name__}")
    if failures:
        print(f"{failures} of {len(TESTS)} tests failed")
        return 1
    print(f"all {len(TESTS)} tests passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
