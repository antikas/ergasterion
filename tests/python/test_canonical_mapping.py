"""Self-tests for ``ergasterion validate-canonical``: a canonical product's
declared reference mappings checked against a reference model checkout
(architecture sections 6 and 14).

Same plain assert-and-report convention as the rest of this repo (no pytest
in the .venv): each ``test_*`` raises ``AssertionError`` on failure,
``main()`` runs them all and reports PASS/FAIL.

Covers:
  - the command validates the two_shapes estate's declared mapping against a
    reference model and reports what it proved;
  - a mapping onto an attribute the reference model does not declare fails
    closed naming the product, the entity and the attribute;
  - a reference entity no document of the model carries fails closed naming
    it;
  - a model document with no attribute-schema section fails closed rather
    than passing vacuously;
  - the checkout is resolved in one place: the flag beats the environment
    variable, the environment variable beats the sibling, and the flag is
    taken as given even when it names something that is not a checkout;
  - without a checkout, and with a directory that is not one, the command
    says which and exits 0 rather than reporting a pass it never ran, naming
    all three places it looked;
  - a mapping naming a column the entity does not publish is a shape
    constraint, refused at emission with no checkout in sight;
  - the same command against the reference model checkout on this machine,
    when there is one.

The hermetic cases build their own small reference model, so they prove the
command rather than a copy of somebody's checkout. The machine's own checkout
is found through the engine's own resolution, and the case states why it did
nothing when that resolution finds none.

Usage:
    python tests/python/test_canonical_mapping.py
"""

from __future__ import annotations

import contextlib
import io
import os
import shutil
import tempfile
import traceback
from pathlib import Path

import yaml

if __package__ in (None, ""):
    import sys as _sys

    _sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from ergasterion import cli
from ergasterion.estate import (
    OPENIM_ROOT_ENV,
    OPENIM_SIBLING_NAME,
    REFERENCE_MODEL_DIRECTORY,
    resolve_openim_root,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
FIXTURE_ESTATE = REPO_ROOT / "tests" / "fixtures" / "estates" / "two_shapes"
INTERFACE_DECLARATION = "customer_interface.yml"

# The reference entity the fixture estate's canonical product maps onto, and
# the two attributes it names.
REFERENCE_ENTITY = "E-01"
REFERENCE_DOCUMENT = f"{REFERENCE_ENTITY}-legal-entity.md"


def _model(attributes: tuple[str, ...]) -> str:
    rows = "\n".join(f"| `{name}` | varchar | The {name}. |" for name in attributes)
    return (
        f"# {REFERENCE_ENTITY} -- Legal Entity\n\n"
        "## Purpose\n\nA small stand-in for a reference model entity document.\n\n"
        "## Attribute schema\n\n"
        "| Column | Type | Definition |\n|---|---|---|\n"
        f"{rows}\n\n"
        "## Notes\n\nNothing after the table is read.\n"
    )


def _reference_model(
    root: Path,
    *,
    attributes: tuple[str, ...] = ("entity_id", "entity_name", "lei"),
    document: str = REFERENCE_DOCUMENT,
    body: str | None = None,
) -> Path:
    """A reference model checkout carrying one entity document."""

    directory = root / REFERENCE_MODEL_DIRECTORY / "entities" / "core"
    directory.mkdir(parents=True)
    (directory / document).write_text(
        _model(attributes) if body is None else body, encoding="utf-8"
    )
    return root


def _scratch_estate(root: Path) -> Path:
    estate = root / "estate"
    shutil.copytree(FIXTURE_ESTATE, estate)
    return estate


def _edit_interface(estate: Path, mutate) -> None:
    path = estate / "declarations" / "products" / INTERFACE_DECLARATION
    document = yaml.safe_load(path.read_text(encoding="utf-8"))
    mutate(document["target"]["shape_config"]["entities"][0])
    path.write_text(yaml.safe_dump(document, sort_keys=False), encoding="utf-8")


def _run(*arguments: str, environment: dict[str, str] | None = None) -> tuple[int, str]:
    """Drive the real console command. ``environment``, where supplied, states
    what the reference-model variable is for the call, and an empty one states
    that it is unset. Every case that is about anything other than the ambient
    environment supplies it, because this lane sets the variable and a case
    that inherited it would prove the wrong thing."""

    out, err = io.StringIO(), io.StringIO()
    previous = os.environ.get(OPENIM_ROOT_ENV)
    if environment is not None:
        os.environ.pop(OPENIM_ROOT_ENV, None)
        if OPENIM_ROOT_ENV in environment:
            os.environ[OPENIM_ROOT_ENV] = environment[OPENIM_ROOT_ENV]
    try:
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            code = cli.main(["validate-canonical", *arguments])
    finally:
        if environment is not None:
            os.environ.pop(OPENIM_ROOT_ENV, None)
            if previous is not None:
                os.environ[OPENIM_ROOT_ENV] = previous
    return code, out.getvalue() + err.getvalue()


def _validate(
    estate: Path, checkout: Path | None, *, environment: dict[str, str] | None = None
) -> tuple[int, str]:
    arguments = ["--estate-root", str(estate)]
    if checkout is not None:
        arguments += ["--openim-root", str(checkout)]
    return _run(*arguments, environment=environment)


# --------------------------------------------------------------------------- green


def test_the_declared_mapping_validates_against_the_reference_model() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))
        checkout = _reference_model(Path(tmp) / "reference")
        code, output = _validate(estate, checkout, environment={})
        assert code == 0, output
        assert "crm.customer_interface" in output, output
        assert "entity customer" in output, output
        assert "2 attribute(s)" in output, output
        assert f"against {REFERENCE_ENTITY}" in output, output
        assert "1 validated" in output, output


def test_an_estate_declaring_no_mapping_validates_nothing_and_says_so() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))
        _edit_interface(estate, lambda entity: entity.pop("reference"))
        checkout = _reference_model(Path(tmp) / "reference")
        code, output = _validate(estate, checkout, environment={})
        assert code == 0, output
        assert "0 validated" in output, output


# --------------------------------------------------------------------------- red


def test_a_mapping_onto_an_undeclared_attribute_fails_closed() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))
        _edit_interface(
            estate,
            lambda entity: entity["reference"]["attributes"].update(
                customer_name="legal_name"
            ),
        )
        checkout = _reference_model(Path(tmp) / "reference")
        code, output = _validate(estate, checkout, environment={})
        assert code == 1, output
        assert "crm.customer_interface" in output, output
        assert "customer" in output, output
        assert "'legal_name'" in output, output
        assert REFERENCE_ENTITY in output, output


def test_a_reference_entity_no_document_carries_fails_closed() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))
        _edit_interface(estate, lambda entity: entity["reference"].update(entity="E-99"))
        checkout = _reference_model(Path(tmp) / "reference")
        code, output = _validate(estate, checkout, environment={})
        assert code == 1, output
        assert "E-99" in output, output
        assert "0 document(s)" in output, output


def test_two_documents_carrying_one_reference_entity_fail_closed() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))
        checkout = _reference_model(Path(tmp) / "reference")
        (checkout / REFERENCE_MODEL_DIRECTORY / "entities" / "core" / f"{REFERENCE_ENTITY}-other.md").write_text(
            _model(("entity_id",)), encoding="utf-8"
        )
        code, output = _validate(estate, checkout, environment={})
        assert code == 1, output
        assert "2 document(s)" in output, output


def test_a_reference_document_with_no_attribute_schema_fails_closed() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))
        checkout = _reference_model(
            Path(tmp) / "reference",
            body=f"# {REFERENCE_ENTITY}\n\n## Purpose\n\nNo table here.\n",
        )
        code, output = _validate(estate, checkout, environment={})
        assert code == 1, output
        assert "no attribute-schema section" in output, output


def test_a_mapping_naming_a_column_the_entity_does_not_publish_fails_at_emission() -> None:
    """A shape constraint, not a mapping one: it needs no checkout, so it is
    refused where every other shape constraint is, at emission."""

    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))
        _edit_interface(
            estate,
            lambda entity: entity["reference"]["attributes"].update(
                customer_segment="entity_type"
            ),
        )
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            code = cli.main(["emit-products", "--estate-root", str(estate)])
        output = out.getvalue() + err.getvalue()
        assert code == 1, output
        assert "crm.customer_interface" in output, output
        assert "customer_segment" in output, output
        assert "columns it publishes" in output, output


# --------------------------------------------------------------------------- skipped


def test_without_a_checkout_the_command_states_why_it_validated_nothing() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))
        code, output = _validate(estate, None, environment={})
        assert code == 0, output
        assert output.startswith("skipped:"), output
        # All three places it looked, so a reader knows what to do next.
        assert "--openim-root" in output, output
        assert OPENIM_ROOT_ENV in output, output
        assert OPENIM_SIBLING_NAME in output, output


def test_the_environment_variable_is_read_when_no_flag_is_given() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))
        checkout = _reference_model(Path(tmp) / "reference")
        code, output = _validate(
            estate, None, environment={OPENIM_ROOT_ENV: str(checkout)}
        )
        assert code == 0, output
        assert f"against {REFERENCE_ENTITY}" in output, output
        assert "1 validated" in output, output


def test_the_flag_wins_over_the_environment_variable() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))
        checkout = _reference_model(Path(tmp) / "reference")
        elsewhere = Path(tmp) / "elsewhere"
        elsewhere.mkdir()
        # The flag names something that is not a checkout and the variable
        # names one: the flag is taken as given and reported, rather than
        # falling through to the variable behind it.
        code, output = _validate(
            estate, elsewhere, environment={OPENIM_ROOT_ENV: str(checkout)}
        )
        assert code == 0, output
        assert output.startswith("skipped:"), output
        assert "elsewhere" in output, output


def test_the_sibling_checkout_is_read_when_neither_flag_nor_variable_is_given() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))
        _reference_model(estate.parent / OPENIM_SIBLING_NAME)
        code, output = _validate(estate, None, environment={})
        assert code == 0, output
        assert f"against {REFERENCE_ENTITY}" in output, output
        assert "1 validated" in output, output


def test_a_directory_that_is_not_a_reference_model_is_stated_as_skipped() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))
        elsewhere = Path(tmp) / "elsewhere"
        elsewhere.mkdir()
        code, output = _validate(estate, elsewhere, environment={})
        assert code == 0, output
        assert output.startswith("skipped:"), output
        assert REFERENCE_MODEL_DIRECTORY in output, output


# --------------------------------------------------------------------------- this machine


def _machine_checkout() -> Path | None:
    """The reference model checkout on this machine, found the way the engine
    finds it (``estate.resolve_openim_root``) rather than by a second search
    of this test's own."""

    resolved = resolve_openim_root(root=REPO_ROOT)
    if resolved is not None and (resolved / REFERENCE_MODEL_DIRECTORY).is_dir():
        return resolved
    return None


def test_the_declared_mapping_validates_against_the_checkout_on_this_machine() -> None:
    checkout = _machine_checkout()
    if checkout is None:
        print(
            f"  no reference model checkout here: set {OPENIM_ROOT_ENV} or check one out as "
            f"{OPENIM_SIBLING_NAME} beside this repository; nothing was validated against one"
        )
        return
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))
        code, output = _validate(estate, checkout)
        assert code == 0, output
        assert f"against {REFERENCE_ENTITY}" in output, output

        # The same checkout, one attribute that model does not declare.
        broken = _scratch_estate(Path(tmp) / "broken")
        _edit_interface(
            broken,
            lambda entity: entity["reference"]["attributes"].update(
                customer_name="legal_name"
            ),
        )
        code, output = _validate(broken, checkout)
        assert code == 1, output
        assert "'legal_name'" in output, output


TESTS = [
    test_the_declared_mapping_validates_against_the_reference_model,
    test_an_estate_declaring_no_mapping_validates_nothing_and_says_so,
    test_a_mapping_onto_an_undeclared_attribute_fails_closed,
    test_a_reference_entity_no_document_carries_fails_closed,
    test_two_documents_carrying_one_reference_entity_fail_closed,
    test_a_reference_document_with_no_attribute_schema_fails_closed,
    test_a_mapping_naming_a_column_the_entity_does_not_publish_fails_at_emission,
    test_without_a_checkout_the_command_states_why_it_validated_nothing,
    test_the_environment_variable_is_read_when_no_flag_is_given,
    test_the_flag_wins_over_the_environment_variable,
    test_the_sibling_checkout_is_read_when_neither_flag_nor_variable_is_given,
    test_a_directory_that_is_not_a_reference_model_is_stated_as_skipped,
    test_the_declared_mapping_validates_against_the_checkout_on_this_machine,
]


def main() -> int:
    failures = 0
    for test in TESTS:
        try:
            test()
            print(f"PASS {test.__name__}")
        except Exception:  # noqa: BLE001 -- report-and-continue harness
            failures += 1
            print(f"FAIL {test.__name__}")
            traceback.print_exc()
    print(f"\n{len(TESTS) - failures}/{len(TESTS)} passed")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
