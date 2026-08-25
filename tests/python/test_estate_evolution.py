"""Unit tests for estate evolution: the evolution ledger, the grading gate and the
frozen hashdiff basis.

Vocabulary, used here exactly as the emitter and its errors use it:

  hashdiff basis  the exact column set an entity's stored hashdiffs were computed over.
  evolution ledger  the generated file per domain recording every entity's payload
                    roster and hashdiff basis, with a basis version.
  extension  an additive payload change, absorbed online.
  re-baseline  the declared operation adopting the current payload as the new hashdiff
               basis and recomputing the stored hashdiffs in place.
  estate migration requirement  the named, fail-closed error a non-additive change
                                raises, stating entity, column, change class and remedy.

The re-baseline lanes below run in two registers. The cheap lane executes the
golden-hash parity vectors in an in-process DuckDB and reads the shipped macro text,
so a change in the hash construction is caught without dbt. The isolated dbt-project
lane copies the committed estate into a scratch directory with its own DuckDB file and
walks the whole sequence there: baseline build, extension, the frozen-basis gap, every
crash boundary, promotion, and abort convergence.

No pytest dependency in this repo's .venv, so this follows the plain
assert-and-report convention already used by tests/python/test_emit.py: each test_*
function raises AssertionError on failure, main() runs them all and reports PASS/FAIL,
exit code 0 = all green, 1 = any failure.

Usage:
    python tests/python/test_estate_evolution.py
"""

from __future__ import annotations

import contextlib
import copy
import hashlib
import io
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

# Allow direct execution as `python tests/python/test_estate_evolution.py`.
if __package__ in (None, ""):
    import os as _os, sys as _sys
    _sys.path.insert(0, _os.path.dirname(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))))
    _sys.path.insert(0, _os.path.dirname(_os.path.abspath(__file__)))

from ergasterion import cli, emit
from test_emit import (
    FIXTURE_DOMAIN,
    _fixture_declaration,
    _run_main_capturing,
    _write_structure_minimum,
)


# ---------------------------------------------------------------------------
# A scratch estate built from the toy fixture domain, driven through the same public
# emit functions main() uses: load_domains -> load_declarations ->
# grade_estate_evolution -> generate_files -> write_files.
# ---------------------------------------------------------------------------


def _seed_estate(root: Path, domain_doc: dict, declaration_doc: dict) -> tuple[Path, Path]:
    domains_dir = root / "domains"
    domains_dir.mkdir(parents=True, exist_ok=True)
    (domains_dir / "fixture.yml").write_text(
        yaml.safe_dump(domain_doc, sort_keys=False), encoding="utf-8"
    )
    declarations_dir = root / "declarations"
    declarations_dir.mkdir(parents=True, exist_ok=True)
    (declarations_dir / "toysrc.yml").write_text(
        yaml.safe_dump(declaration_doc, sort_keys=False), encoding="utf-8"
    )
    return domains_dir, declarations_dir


def _emit(root: Path, domain_doc: dict, declaration_doc: dict, *, write: bool = True) -> dict:
    """One emit pass over a scratch estate, with the evolution ledger in the path."""
    domains_dir, declarations_dir = _seed_estate(root, domain_doc, declaration_doc)
    domain = emit.load_domains(domains_dir)
    ctx = emit.EstateContext.resolve(
        estate_root=root, domains_dir=domains_dir, declarations_dir=declarations_dir
    )
    declarations = emit.load_declarations(domain, ctx=ctx)
    ledger_files, notices = emit.grade_estate_evolution(domain, declarations, ctx=ctx)
    files = emit.generate_files(declarations, emit.template_env(), domain, ctx=ctx)
    files = files + ledger_files
    if write:
        emit.write_files(files, root=root)
    return {
        "domain": domain,
        "rendered": {file.path.name: file.content for file in files},
        "notices": notices,
        "ctx": ctx,
    }


def _emit_without_ledger(root: Path, domain_doc: dict, declaration_doc: dict) -> dict[str, str]:
    """The same generation with no ledger in the path: the control for byte-stability."""
    domains_dir, declarations_dir = _seed_estate(root, domain_doc, declaration_doc)
    domain = emit.load_domains(domains_dir)
    ctx = emit.EstateContext.resolve(
        estate_root=root, domains_dir=domains_dir, declarations_dir=declarations_dir
    )
    declarations = emit.load_declarations(domain, ctx=ctx)
    files = emit.generate_files(declarations, emit.template_env(), domain, ctx=ctx)
    return {file.path.name: file.content for file in files}


def _ledger(root: Path) -> dict:
    path = root / "declarations" / "evolution" / "fixture.lock.yml"
    assert path.is_file(), f"expected an evolution ledger at {path}"
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def _with_extra_payload_column(column: str) -> tuple[dict, dict]:
    """The fixture domain and declaration with one extra alpha payload column: an
    extension in every layer that carries the payload."""
    domain_doc = copy.deepcopy(FIXTURE_DOMAIN)
    domain_doc["entity_configs"]["alpha"]["payload"].append(column)
    declaration_doc = copy.deepcopy(_fixture_declaration())
    table = declaration_doc["tables"]["things"]
    table["projection"].append({"name": column, "expression": f"cast({column} as string)"})
    for vault in table["vault_entities"]:
        if vault["entity"] == "alpha":
            vault["bridge"]["select"].append(
                {"name": column, "expression": f"source.{column}"}
            )
    return domain_doc, declaration_doc


def _expect_requirement(root: Path, domain_doc: dict, declaration_doc: dict) -> str:
    try:
        _emit(root, domain_doc, declaration_doc, write=False)
    except emit.EstateMigrationRequirement as error:
        return str(error)
    raise AssertionError("expected an estate migration requirement, none raised")


def _assert_requirement_names(message: str, entity: str, column: str, change_class: str) -> None:
    for token in (entity, column, change_class, "remedy:"):
        assert token in message, f"expected {token!r} named in the requirement, got: {message}"
    assert message.startswith("estate migration requirement:"), (
        f"the error must carry its own name, got: {message}"
    )


# ---------------------------------------------------------------------------
# Bootstrap
# ---------------------------------------------------------------------------


def test_first_emit_bootstraps_the_ledger_and_leaves_every_file_byte_identical() -> None:
    """The first emit for a domain records basis equal to the derived hashdiff set, so
    every already-generated file regenerates byte-for-byte."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        control = _emit_without_ledger(root / "control", FIXTURE_DOMAIN, _fixture_declaration())
        result = _emit(root / "ledger", FIXTURE_DOMAIN, _fixture_declaration())

        for name, content in control.items():
            assert name in result["rendered"], f"{name} vanished once the ledger entered the path"
            assert result["rendered"][name] == content, (
                f"{name} changed bytes on the bootstrap emit; the recorded basis must equal "
                f"the derived hashdiff set"
            )

        ledger = _ledger(root / "ledger")
        assert ledger["version"] == emit.EVOLUTION_LEDGER_VERSION
        assert ledger["domain"] == "fixture"
        alpha = ledger["entities"]["alpha"]
        assert alpha["payload"] == FIXTURE_DOMAIN["entity_configs"]["alpha"]["payload"]
        assert alpha["hashdiff_basis"] == FIXTURE_DOMAIN["entity_configs"]["alpha"]["payload"], (
            "with no exclusion declared, the basis is every payload column"
        )
        assert alpha["basis_version"] == 1


def test_bootstrap_consumes_hashdiff_exclude_once_into_the_recorded_basis() -> None:
    """hashdiff_exclude is bootstrap input: the ledger records the basis it produced."""
    domain_doc = copy.deepcopy(FIXTURE_DOMAIN)
    domain_doc["hashdiff_exclude"] = {"alpha": ["alpha_code"]}
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        result = _emit(root, domain_doc, _fixture_declaration())
        alpha = _ledger(root)["entities"]["alpha"]
        assert "alpha_code" in alpha["payload"], "an excluded column stays in the payload roster"
        assert alpha["hashdiff_basis"] == ["source_id", "alpha_name"], (
            f"the recorded basis must be payload minus the exclusion, got {alpha['hashdiff_basis']}"
        )
        assert alpha["hashdiff_exclude"] == ["alpha_code"], (
            f"the ledger records the exclusion set the basis consumed, so a later emit "
            f"knows the payload roster the basis froze over, got {alpha['hashdiff_exclude']}"
        )
        stage = result["rendered"]["stg_dv_toysrc_alpha.sql"]
        assert "'alpha_code'" not in stage.split("hashed_columns")[1].split("automate_dv.stage")[0]


def test_ledger_bytes_are_deterministic_and_lf() -> None:
    """The same declarations produce the same ledger bytes, with LF newlines."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        _emit(root / "first", FIXTURE_DOMAIN, _fixture_declaration())
        _emit(root / "second", FIXTURE_DOMAIN, _fixture_declaration())
        first = (root / "first" / "declarations" / "evolution" / "fixture.lock.yml").read_bytes()
        second = (root / "second" / "declarations" / "evolution" / "fixture.lock.yml").read_bytes()
        assert first == second, "ledger bytes must be deterministic across runs"
        assert b"\r" not in first, "ledger bytes must be LF, with no carriage return"
        assert first.endswith(b"\n"), "the ledger must end with a newline"


# ---------------------------------------------------------------------------
# Extension
# ---------------------------------------------------------------------------


def test_extension_lands_in_every_payload_layer_with_the_basis_frozen() -> None:
    """A payload addition regenerates staging, bridge, stage and satellite with the new
    column while the hashdiff basis stays byte-identical, and the ledger records it."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        before = _emit(root, FIXTURE_DOMAIN, _fixture_declaration())
        before_ledger = _ledger(root)

        domain_doc, declaration_doc = _with_extra_payload_column("alpha_extra")
        after = _emit(root, domain_doc, declaration_doc)

        for model in ("stg_toysrc_things.sql", "br_dv_toysrc_alpha.sql", "sat_alpha_toysrc.sql"):
            assert "alpha_extra" in after["rendered"][model], (
                f"the extension must reach {model}"
            )
        assert after["rendered"]["stg_dv_toysrc_alpha.sql"] == before["rendered"]["stg_dv_toysrc_alpha.sql"], (
            "the stage model carries the hashed_columns basis; an extension must leave it "
            "byte-identical so every stored hashdiff still matches"
        )

        after_ledger = _ledger(root)
        alpha_before = before_ledger["entities"]["alpha"]
        alpha_after = after_ledger["entities"]["alpha"]
        assert alpha_after["payload"] == alpha_before["payload"] + ["alpha_extra"], (
            "the ledger records the extension in the payload roster"
        )
        assert alpha_after["hashdiff_basis"] == alpha_before["hashdiff_basis"], (
            "the hashdiff basis stays frozen through an extension"
        )
        assert alpha_after["basis_version"] == alpha_before["basis_version"]


def test_extension_prints_a_notice_naming_the_columns_outside_change_detection() -> None:
    """Emit says which columns stay outside change detection until a re-baseline."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        _emit(root, FIXTURE_DOMAIN, _fixture_declaration())
        domain_doc, declaration_doc = _with_extra_payload_column("alpha_extra")
        notices = _emit(root, domain_doc, declaration_doc)["notices"]
        extension = [line for line in notices if line.startswith("estate evolution: extension")]
        assert len(extension) == 1, f"expected exactly one extension notice, got {notices!r}"
        message = extension[0]
        for token in ("alpha", "alpha_extra", "outside change detection", "rebaseline"):
            assert token in message, f"expected {token!r} in the notice, got: {message}"


# ---------------------------------------------------------------------------
# The three failing grades, plus the hashdiff-basis conflict
# ---------------------------------------------------------------------------


def test_removal_fails_with_an_estate_migration_requirement() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        _emit(root, FIXTURE_DOMAIN, _fixture_declaration())
        domain_doc = copy.deepcopy(FIXTURE_DOMAIN)
        domain_doc["entity_configs"]["alpha"]["payload"].remove("alpha_code")
        message = _expect_requirement(root, domain_doc, _fixture_declaration())
        _assert_requirement_names(message, "alpha", "alpha_code", "removal")
        assert "reset-class rebuild" in message, (
            f"a removal's remedy is a reset-class rebuild, got: {message}"
        )


def test_rename_fails_on_the_removal_half_with_an_estate_migration_requirement() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        _emit(root, FIXTURE_DOMAIN, _fixture_declaration())
        domain_doc, declaration_doc = _with_extra_payload_column("alpha_reference")
        domain_doc["entity_configs"]["alpha"]["payload"].remove("alpha_code")
        message = _expect_requirement(root, domain_doc, declaration_doc)
        _assert_requirement_names(message, "alpha", "alpha_code", "rename")
        assert "alpha_reference" in message, (
            f"a rename names the column that arrived beside the one that left, got: {message}"
        )


def test_redefinition_of_a_recorded_type_fails_with_an_estate_migration_requirement() -> None:
    """A type delta on a payload column that carries a type fact grades as a
    redefinition, because the stored hashdiffs were computed over the old type."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        typed_declaration = copy.deepcopy(_fixture_declaration())
        for column in typed_declaration["tables"]["things"]["projection"]:
            if column["name"] == "alpha_name":
                column["expression"] = "source.alpha_name"
                column["logical_type"] = "utf8_string"
        for vault in typed_declaration["tables"]["things"]["vault_entities"]:
            if vault["entity"] == "alpha":
                for column in vault["bridge"]["select"]:
                    if column["name"] == "alpha_name":
                        column["expression"] = "source.alpha_name"
        _emit(root, FIXTURE_DOMAIN, typed_declaration)
        recorded = _ledger(root)["entities"]["alpha"]["column_types"]
        assert recorded == {"alpha_name": "utf8_string"}, (
            f"the ledger records the declared type where a type fact exists, got {recorded}"
        )

        redefined = copy.deepcopy(typed_declaration)
        for column in redefined["tables"]["things"]["projection"]:
            if column["name"] == "alpha_name":
                column["logical_type"] = "int64"
        message = _expect_requirement(root, FIXTURE_DOMAIN, redefined)
        _assert_requirement_names(message, "alpha", "alpha_name", "redefinition")
        assert "rebaseline" in message, (
            f"a redefinition's remedy names the re-baseline, got: {message}"
        )


def test_redefinition_of_a_projection_expression_fails_but_formatting_whitespace_does_not() -> None:
    """A changed resolved expression can replay stored history, while formatting alone is
    not a schema event."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        original = _fixture_declaration()
        _emit(root, FIXTURE_DOMAIN, original)
        recorded = _ledger(root)["entities"]["alpha"]["projection_fingerprints"]
        assert recorded["alpha_name"]["toysrc.things.alpha"], recorded

        formatted = copy.deepcopy(original)
        for column in formatted["tables"]["things"]["projection"]:
            if column["name"] == "alpha_name":
                column["expression"] = "cast (  alpha_name   as   string )"
        _emit(root, FIXTURE_DOMAIN, formatted, write=False)

        changed = copy.deepcopy(original)
        for column in changed["tables"]["things"]["projection"]:
            if column["name"] == "alpha_name":
                column["expression"] = "upper(cast(alpha_name as string))"
        message = _expect_requirement(root, FIXTURE_DOMAIN, changed)
        _assert_requirement_names(message, "alpha", "alpha_name", "redefinition")
        assert "toysrc.things.alpha" in message and "rebaseline" in message, message


def test_hashdiff_exclude_edit_after_bootstrap_fails_with_a_re_baseline_remedy() -> None:
    """hashdiff_exclude is consumed at bootstrap only; a later edit is graded like a
    payload delta and fails with the re-baseline remedy."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        _emit(root, FIXTURE_DOMAIN, _fixture_declaration())
        domain_doc = copy.deepcopy(FIXTURE_DOMAIN)
        domain_doc["hashdiff_exclude"] = {"alpha": ["alpha_code"]}
        message = _expect_requirement(root, domain_doc, _fixture_declaration())
        _assert_requirement_names(message, "alpha", "alpha_code", "hashdiff basis conflict")
        assert "rebaseline" in message, (
            f"the remedy for a basis conflict is a re-baseline, got: {message}"
        )


def test_post_extension_steady_state_grades_clean_on_every_later_emit() -> None:
    """The steady state after an absorbed extension: declarations that changed nothing
    since the extension grade silently on every later emit, and `emit --check` reports
    the post-extension estate byte-stable. No hashdiff_exclude is declared anywhere here,
    so the only fact under test is the frozen basis meeting a wider payload roster."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        _emit(root, FIXTURE_DOMAIN, _fixture_declaration())

        domain_doc, declaration_doc = _with_extra_payload_column("alpha_extra")
        extension = _emit(root, domain_doc, declaration_doc)
        assert any(
            notice.startswith("estate evolution: extension") for notice in extension["notices"]
        ), f"the extension emit reports the extension, got {extension['notices']!r}"
        ledger_path = root / "declarations" / "evolution" / "fixture.lock.yml"
        absorbed = ledger_path.read_bytes()

        for pass_number in (1, 2):
            steady = _emit(root, domain_doc, declaration_doc)
            assert steady["notices"] == [], (
                f"re-emit {pass_number} after the extension changed nothing and must grade "
                f"silently, got {steady['notices']!r}"
            )
            assert ledger_path.read_bytes() == absorbed, (
                f"re-emit {pass_number} rewrote the ledger; an unchanged declaration leaves "
                f"the recorded payload roster, basis and exclusion set exactly as they were"
            )

        alpha = _ledger(root)["entities"]["alpha"]
        assert alpha["hashdiff_basis"] == FIXTURE_DOMAIN["entity_configs"]["alpha"]["payload"], (
            "the basis stays frozen at the set the stored hashdiffs were computed over"
        )
        assert alpha["hashdiff_exclude"] == [], (
            "the recorded exclusion set is what the basis consumed, and an extension adds "
            "nothing to it"
        )

        # The same steady state through the emit command itself.
        _write_structure_minimum(root)
        exit_code, out, _err = _run_main_capturing(["--check", "--estate-root", str(root)])
        assert exit_code == 0, (
            f"emit --check must exit 0 on the post-extension estate, got {exit_code}: {out}"
        )
        assert "estate migration requirement" not in out, out
        summary = [line for line in out.splitlines() if line.startswith("would change ")]
        assert summary and summary[0].startswith("would change 0 of "), (
            f"the post-extension estate is byte-stable, got: {out}"
        )


def test_excluding_a_post_extension_column_leaves_the_frozen_basis_alone() -> None:
    """A post-extension column sits outside the frozen basis already, so naming it in
    hashdiff_exclude changes nothing and raises nothing."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        _emit(root, FIXTURE_DOMAIN, _fixture_declaration())
        domain_doc, declaration_doc = _with_extra_payload_column("alpha_extra")
        _emit(root, domain_doc, declaration_doc)

        # The estate already grades clean with no exclusion declared, so whatever this
        # test proves next comes from the exclusion and from nothing else.
        settled = _emit(root, domain_doc, declaration_doc)
        assert settled["notices"] == [], (
            f"the post-extension estate is silent before the exclusion arrives, got "
            f"{settled['notices']!r}"
        )

        domain_doc["hashdiff_exclude"] = {"alpha": ["alpha_extra"]}
        result = _emit(root, domain_doc, declaration_doc)
        alpha = _ledger(root)["entities"]["alpha"]
        assert "alpha_extra" not in alpha["hashdiff_basis"]
        assert alpha["hashdiff_basis"] == FIXTURE_DOMAIN["entity_configs"]["alpha"]["payload"]
        assert alpha["hashdiff_exclude"] == [], (
            "the recorded exclusion set names what the frozen basis consumed at bootstrap; "
            f"a post-extension exclusion stays out of it, got {alpha['hashdiff_exclude']!r}"
        )
        assert result["notices"] == [], f"a no-op exclusion is silent, got {result['notices']!r}"


# ---------------------------------------------------------------------------
# The physical relations and the generated declarations
# ---------------------------------------------------------------------------


def test_dbt_project_pins_the_raw_vault_satellites_for_online_evolution() -> None:
    """The two satellite lines live in dbt_project.yml, scoped to the raw-vault
    satellites, and the scaffold mirror carries the same pair."""
    for path in (
        emit.REPO_ROOT / "dbt_project.yml",
        Path(emit.__file__).resolve().parent / "scaffold" / "dbt_project.yml",
    ):
        project = yaml.safe_load(path.read_text(encoding="utf-8"))
        satellites = project["models"]["ergasterion"]["raw_vault"]["satellites"]
        assert satellites.get("+on_schema_change") == "append_new_columns", (
            f"{path}: the satellites need append_new_columns so dbt adds an extension's "
            f"column to the existing relation in place"
        )
        assert satellites.get("+full_refresh") is False, (
            f"{path}: the satellites are unrebuildable history and stay pinned against "
            f"a full refresh"
        )
        for layer in ("staging", "entity_resolution"):
            other = project["models"]["ergasterion"].get(layer, {})
            assert "+full_refresh" not in other, (
                f"{path}: the full-refresh pin is scoped to the raw-vault satellites"
            )


def test_no_vault_template_emits_model_configuration() -> None:
    """Estate-wide model configuration keeps its home in dbt_project.yml."""
    templates = Path(emit.__file__).resolve().parent / "templates"
    for name in ("satellite.sql.j2", "hub.sql.j2", "link.sql.j2", "automate_dv_stage.sql.j2",
                 "bridge.sql.j2"):
        text = (templates / name).read_text(encoding="utf-8")
        assert "config(" not in text, (
            f"{name} emits model configuration; it belongs in dbt_project.yml"
        )


def test_generated_satellite_tests_use_the_nested_arguments_mapping() -> None:
    """Every generated test declaration reads its keyword arguments from the nested
    arguments: mapping."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        rendered = _emit(root, FIXTURE_DOMAIN, _fixture_declaration())["rendered"]
        satellites = rendered["_satellites.yml"]
        document = yaml.safe_load(satellites)
        for model in document["models"]:
            for test in model["data_tests"]:
                for name, body in test.items():
                    assert "arguments" in body, (
                        f"{model['name']}: generic test {name} must nest its keyword "
                        f"arguments under arguments:"
                    )
                    assert "combination_of_columns" in body["arguments"]
        assert not re.search(r"^\s+combination_of_columns:", satellites.split("arguments:")[0], re.M)


def test_committed_estate_ledger_matches_the_committed_domains() -> None:
    """The committed ledgers grade the committed estate clean: every entity is
    recorded, and every recorded basis is the derived hashdiff set."""
    domain = emit.load_domains()
    declarations = emit.load_declarations(domain)
    files, notices = emit.grade_estate_evolution(domain, declarations)
    assert notices == [], f"the committed estate must grade silently, got {notices!r}"
    for file in files:
        assert file.path.is_file(), f"missing committed ledger {file.path}"
        assert file.path.read_bytes() == file.content.encode("utf-8"), (
            f"{file.path} drifted from what emit generates"
        )
    recorded = set()
    for file in files:
        recorded.update(yaml.safe_load(file.content)["entities"])
    assert recorded == set(domain["entity_configs"]), (
        "every entity in every domain carries a ledger record"
    )


# ---------------------------------------------------------------------------
# The pending basis: the ledger record, and the gate it puts on emit
# ---------------------------------------------------------------------------


def _record_pending(root: Path, entity: str, basis: list[str], version: int) -> None:
    """Write a pending basis into a scratch estate's ledger, the way phase one does."""
    path = root / "declarations" / "evolution" / "fixture.lock.yml"
    records = emit.load_evolution_ledger(path)
    records[entity]["pending"] = {
        "basis_version": version,
        "hashdiff_basis": list(basis),
        "hashdiff_exclude": [],
    }
    path.write_bytes(emit.render_evolution_ledger("fixture", records).encode("utf-8"))


def test_a_pending_basis_round_trips_through_the_ledger() -> None:
    """The pending record survives a write and a read with its basis, its exclusion set
    and its basis version intact, and the file stays LF."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        _emit(root, FIXTURE_DOMAIN, _fixture_declaration())
        _record_pending(root, "alpha", ["source_id", "alpha_name", "alpha_code"], 2)

        path = root / "declarations" / "evolution" / "fixture.lock.yml"
        assert b"\r" not in path.read_bytes(), "the ledger stays LF with a pending record"
        alpha = emit.load_evolution_ledger(path)["alpha"]
        assert alpha["pending"] == {
            "basis_version": 2,
            "hashdiff_basis": ["source_id", "alpha_name", "alpha_code"],
            "hashdiff_exclude": [],
        }, f"the pending record must round-trip, got {alpha.get('pending')!r}"
        assert alpha["basis_version"] == 1, "the active basis version stands beside the pending one"

        document = yaml.safe_load(path.read_text(encoding="utf-8"))["entities"]["alpha"]
        assert document["pending_basis_version"] == 2
        assert document["pending_hashdiff_basis"] == ["source_id", "alpha_name", "alpha_code"]
        assert document["pending_hashdiff_exclude"] == []


def test_a_pending_basis_stops_emit_with_the_named_error() -> None:
    """While a pending basis exists, emit fails closed and the error names the entity,
    both basis versions and the two remedies."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        _emit(root, FIXTURE_DOMAIN, _fixture_declaration())
        _record_pending(root, "alpha", ["source_id", "alpha_name", "alpha_code"], 2)

        try:
            _emit(root, FIXTURE_DOMAIN, _fixture_declaration(), write=False)
        except emit.PendingReBaselineRequirement as error:
            message = str(error)
        else:
            raise AssertionError("expected a pending re-baseline requirement, none raised")

        assert message.startswith("estate evolution: pending re-baseline"), message
        for token in ("fixture.alpha", "basis version 1 -> 2", "--complete", "--abort"):
            assert token in message, f"expected {token!r} named in the error, got: {message}"

        # The same gate through the emit command itself.
        _write_structure_minimum(root)
        exit_code, out, _err = _run_main_capturing(["--check", "--estate-root", str(root)])
        assert exit_code == 1, f"emit must fail closed on a pending basis, got {exit_code}: {out}"
        assert "pending re-baseline" in out, out


def test_promotion_and_demotion_move_the_pending_basis() -> None:
    """Promotion makes the pending basis active and drops the pending record; demotion
    drops the pending record and leaves the active basis exactly where it was."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        _emit(root, FIXTURE_DOMAIN, _fixture_declaration())
        path = root / "declarations" / "evolution" / "fixture.lock.yml"
        plan = {"entity": "alpha", "domain": "fixture", "ledger_path": path}

        _record_pending(root, "alpha", ["source_id", "alpha_name"], 2)
        assert emit.ledger_promote_pending(plan) is True
        promoted = emit.load_evolution_ledger(path)["alpha"]
        assert promoted["hashdiff_basis"] == ["source_id", "alpha_name"]
        assert promoted["basis_version"] == 2
        assert "pending" not in promoted
        assert emit.ledger_promote_pending(plan) is False, "a second promotion is a no-op"

        _record_pending(root, "alpha", ["source_id"], 3)
        assert emit.ledger_demote_pending(plan) is True
        demoted = emit.load_evolution_ledger(path)["alpha"]
        assert demoted["hashdiff_basis"] == ["source_id", "alpha_name"], (
            "an abort leaves the active basis alone"
        )
        assert demoted["basis_version"] == 2
        assert "pending" not in demoted
        assert emit.ledger_demote_pending(plan) is False, "a second demotion is a no-op"


def test_rebaseline_plan_reads_the_declared_basis_and_every_satellite() -> None:
    """The plan an operator's command works from: the declared payload as the new basis,
    the next basis version, and every enabled satellite beside the stage relation it is
    built from."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        domain_doc, declaration_doc = _with_extra_payload_column("alpha_extra")
        _emit(root, FIXTURE_DOMAIN, _fixture_declaration())
        _emit(root, domain_doc, declaration_doc)

        ctx = emit.EstateContext.resolve(
            estate_root=root,
            domains_dir=root / "domains",
            declarations_dir=root / "declarations",
        )
        plan = emit.rebaseline_plan("fixture", "alpha", ctx=ctx)
        assert plan["basis"] == ["source_id", "alpha_name", "alpha_code", "alpha_extra"], (
            f"the new basis is the declared payload, got {plan['basis']}"
        )
        assert plan["previous_basis_version"] == 1 and plan["basis_version"] == 2
        assert plan["active_basis"] == ["source_id", "alpha_name", "alpha_code"]
        assert plan["hashdiff"] == "alpha_hashdiff"
        assert plan["satellites"] == [
            {"satellite": "sat_alpha_toysrc", "stage": "stg_dv_toysrc_alpha"}
        ], f"every enabled satellite rides the plan, got {plan['satellites']}"
        assert plan["pending"] is False

        payload = emit.rebaseline_payload(plan, spot_check_rows=25)
        assert payload["basis"] == plan["basis"]
        assert payload["spot_check_rows"] == 25
        narrowed = emit.rebaseline_payload(plan, satellites=["sat_alpha_toysrc"])
        assert len(narrowed["satellites"]) == 1
        try:
            emit.rebaseline_payload(plan, satellites=["sat_nothing"])
        except ValueError as error:
            assert "sat_nothing" in str(error)
        else:
            raise AssertionError("an unknown satellite must fail the payload build")

        # Phase two adopts exactly the basis phase one recorded.
        _record_pending(root, "alpha", ["source_id", "alpha_name"], 7)
        pending_plan = emit.rebaseline_plan("fixture", "alpha", ctx=ctx)
        assert pending_plan["pending"] is True
        assert pending_plan["basis"] == ["source_id", "alpha_name"]
        assert pending_plan["basis_version"] == 7


def test_the_migration_operation_result_is_read_from_the_marker_line() -> None:
    """The CLI reads the operation's JSON outcome off the marker line, through dbt's own
    timestamped and colour-coded output."""
    output = (
        "\x1b[0m17:05:11  Running with dbt=1.11.12\n"
        "\x1b[0m17:05:14  DPF_REBASELINE_RESULT="
        '{"operation": "rewrite", "rows_rewritten": 29}\n'
    )
    result = emit.parse_rebaseline_result(output, "dpf_evolve_rebaseline_rewrite")
    assert result == {"operation": "rewrite", "rows_rewritten": 29}
    try:
        emit.parse_rebaseline_result("no marker here\n", "dpf_evolve_rebaseline_rewrite")
    except RuntimeError as error:
        assert "logged no DPF_REBASELINE_RESULT=" in str(error)
    else:
        raise AssertionError("a run with no marker line must fail loudly")


# ---------------------------------------------------------------------------
# The migration macro: one hash construction, both dispatch arms, the build gate
# ---------------------------------------------------------------------------


MIGRATION_MACRO = emit.REPO_ROOT / "macros" / "estate_evolution.sql"


def test_the_migration_carries_exactly_one_hash_construction() -> None:
    """The recompute calls automate_dv.hash with the new basis and is_hashdiff=true, so
    the stage macros and this migration share one construction and no hashing algorithm
    is spelled out a second time."""
    text = MIGRATION_MACRO.read_text(encoding="utf-8")
    calls = re.findall(r"automate_dv\.hash\((.*?)\)", text, re.S)
    assert len(calls) == 1, f"expected exactly one automate_dv.hash call, got {len(calls)}"
    assert "is_hashdiff=true" in calls[0], f"the recompute must hash as a hashdiff: {calls[0]}"
    assert "columns=columns" in calls[0], f"the recompute hashes over the given basis: {calls[0]}"
    for algorithm in ("md5(", "sha1(", "sha256("):
        assert algorithm not in text.lower(), (
            f"{algorithm} is spelled out in the migration; the hash construction belongs to "
            f"automate_dv.hash alone"
        )
    for arm in (
        "duckdb__dpf_rebaseline_migrate",
        "snowflake__dpf_rebaseline_migrate",
        "default__dpf_rebaseline_migrate",
    ):
        assert f"macro {arm}(" in text, f"the migration must carry the {arm} dispatch arm"
    fallback = text.split("macro default__dpf_rebaseline_migrate(")[1].split("endmacro")[0]
    assert "raise_compiler_error" in fallback and "no arm for target type" in fallback, (
        "a target with no arm of its own stops with a named error"
    )
    snowflake = text.split("macro snowflake__dpf_rebaseline_migrate(")[1]
    assert 'run_query("begin")' in snowflake and 'run_query("commit")' in snowflake, (
        "the Snowflake arm recomputes inside one explicit transaction"
    )
    assert snowflake.index("alter table") < snowflake.index('run_query("begin")'), (
        "Snowflake DDL autocommits, so the column additions sit outside the transaction"
    )


def test_the_build_gate_rides_every_dispatched_stage_arm() -> None:
    """Every generated stage model reaches automate_dv.stage, so the pending gate sits on
    the root project's dispatch arm for each executed target."""
    text = MIGRATION_MACRO.read_text(encoding="utf-8")
    for arm in ("duckdb__stage", "snowflake__stage"):
        assert f"macro {arm}(" in text, f"the build gate needs the {arm} arm"
        body = text.split(f"macro {arm}(")[1].split("endmacro")[0]
        assert "dpf_assert_no_pending_rebaseline()" in body, (
            f"{arm} must assert the pending gate before staging"
        )
        assert "automate_dv.default__stage(" in body, (
            f"{arm} must hand AutomateDV's own staging SQL back unchanged"
        )


def test_the_scaffold_carries_the_migration_macro_byte_for_byte() -> None:
    """A scaffolded estate gets the same migration and the same gate the engine estate
    runs, so `ergasterion init` produces an estate a re-baseline works on."""
    scaffold = Path(emit.__file__).resolve().parent / "scaffold" / "macros" / "estate_evolution.sql"
    assert scaffold.is_file(), f"missing scaffold macro {scaffold}"
    assert scaffold.read_bytes() == MIGRATION_MACRO.read_bytes(), (
        "the scaffold macro is a byte projection of macros/estate_evolution.sql"
    )


# ---------------------------------------------------------------------------
# The cheap lane: golden-hash parity, executed offline in an in-process DuckDB
# ---------------------------------------------------------------------------


PARITY_FIXTURE = emit.REPO_ROOT / "tests" / "fixtures" / "estate_evolution_hash_parity.json"


def test_golden_hash_parity_executes_offline_on_duckdb() -> None:
    """The recompute expression's output, row for row, against the golden hashes the
    stage macro's construction produces.

    The vectors pin AutomateDV's DuckDB hash construction on real values: nulls, empty
    strings, whitespace, mixed case, a decimal and a date. A parity break -- a changed
    cast, a changed null placeholder, a changed column order -- moves at least one row
    and fails here, in the cheap lane, without dbt.
    """
    import duckdb

    fixture = json.loads(PARITY_FIXTURE.read_text(encoding="utf-8"))
    expression = fixture["hashdiff_expression"]
    assert fixture["aliased_expression"] == f"{expression} AS {fixture['alias']}", (
        "the aliased rendering is the bare expression plus the alias the migration strips"
    )
    assert sorted(fixture["basis"]) != fixture["basis"], (
        "the basis is written in declaration order, so the expression proves the sort"
    )
    ordered = re.findall(r"CAST\((\w+) AS VARCHAR\)", expression)
    assert ordered == sorted(fixture["basis"]), (
        f"AutomateDV sorts the basis alphabetically before hashing, got {ordered}"
    )

    columns = fixture["column_types"]
    connection = duckdb.connect(":memory:")
    try:
        declared = ", ".join(f'"{name}" {kind}' for name, kind in columns.items())
        connection.execute(f"create table parity (row_id INTEGER, {declared})")
        placeholders = ", ".join("?" for _ in range(len(columns) + 1))
        for row in fixture["rows"]:
            connection.execute(
                f"insert into parity values ({placeholders})",
                [row["row_id"]] + [row[name] for name in columns],
            )
        computed = connection.execute(
            f"select row_id, ({expression}) from parity order by row_id"
        ).fetchall()
    finally:
        connection.close()

    expected = fixture["expected_hashdiff"]
    assert len(computed) == len(expected), (
        f"the vectors cover {len(expected)} rows, the run produced {len(computed)}"
    )
    for row_id, hashdiff in computed:
        assert hashdiff == expected[str(row_id)], (
            f"row {row_id}: the recompute produced {hashdiff}, the golden hash is "
            f"{expected[str(row_id)]}"
        )
    # The null placeholder collapses NULL, the empty string and whitespace onto one
    # fingerprint, which is what lets an added column arrive with no version replayed.
    assert expected["1"] == expected["2"] == expected["3"]
    # Casing and trimming are standardised before hashing.
    assert expected["4"] == expected["5"]
    assert len(set(expected.values())) == 5, (
        "the vectors must separate the genuinely different rows, not collapse them all"
    )


# ---------------------------------------------------------------------------
# The isolated dbt-project lane: the whole sequence, executed on DuckDB
#
# One scratch copy of the committed estate carries every phase below, in order:
# baseline build, extension, the frozen-basis gap, the crash-boundary matrix,
# promotion, and abort convergence. The copy has its own DuckDB file, its own dbt
# target directory and its own declarations, so nothing here touches the committed
# tree or the engine's own warehouse.
# ---------------------------------------------------------------------------


ESTATE_DOMAIN = "ecommerce"
ESTATE_ENTITY = "product"
ESTATE_SATELLITES = ("sat_product_cartivo", "sat_product_mercaro")
ESTATE_SELECTION = ("+sat_product_cartivo", "+sat_product_mercaro")
NEW_COLUMN = "product_note"
NOTED_RECORD = "CART-PR-001"

# The copied estate and the state captured between phases. Built once, on first use.
SCENARIO: dict = {}

ESTATE_COPY_DIRS = (
    "models",
    "seeds",
    "macros",
    "declarations",
    "domains",
    "profiles",
    "contracts",
    "dbt_packages",
)
ESTATE_COPY_FILES = ("dbt_project.yml", "packages.yml", "package-lock.yml", "estate.yml")


def _scenario_estate() -> Path:
    """The scratch estate copy, created on first use and reused by every phase."""
    if "root" in SCENARIO:
        return SCENARIO["root"]
    holder = tempfile.mkdtemp(prefix="dpf-rebaseline-")
    SCENARIO["holder"] = holder
    root = Path(holder) / "estate"
    root.mkdir(parents=True)
    for name in ESTATE_COPY_DIRS:
        shutil.copytree(emit.REPO_ROOT / name, root / name)
    for name in ESTATE_COPY_FILES:
        shutil.copy2(emit.REPO_ROOT / name, root / name)
    # The estate's singular test assertions, which the emitter's res_configs gate reads
    # by path. The python tests and their fixtures beside them stay out of the copy.
    (root / "tests").mkdir()
    for assertion in sorted((emit.REPO_ROOT / "tests").glob("*.sql")):
        shutil.copy2(assertion, root / "tests" / assertion.name)
    SCENARIO["root"] = root
    SCENARIO["duckdb"] = root / "scratch.duckdb"
    return root


def _scenario_cleanup() -> None:
    holder = SCENARIO.pop("holder", None)
    SCENARIO.clear()
    if holder:
        shutil.rmtree(holder, ignore_errors=True)


def _dbt_env() -> dict:
    environment = dict(os.environ)
    environment["DPF_DUCKDB_PATH"] = str(SCENARIO["duckdb"])
    return environment


def _invalidate_parse_cache() -> None:
    """Drop the scratch estate's cached manifest.

    dbt reparses a changed model or declaration on its own. A change to the estate's own
    project configuration -- here, a seed gaining a column -- can survive in the cache,
    and the scenario changes that configuration between invocations.
    """
    cache = _scenario_estate() / "target" / "partial_parse.msgpack"
    if cache.exists():
        cache.unlink()


def _run_dbt(*arguments: str, expect_success: bool = True) -> str:
    """One dbt invocation against the scratch estate, on its own DuckDB file."""
    root = _scenario_estate()
    command = [
        emit.resolve_dbt_executable(),
        "--no-use-colors",
        *arguments,
        "--project-dir",
        str(root),
        "--profiles-dir",
        str(root / "profiles"),
        "-t",
        "duckdb",
    ]
    completed = subprocess.run(
        command,
        cwd=str(root),
        env=_dbt_env(),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    output = f"{completed.stdout}\n{completed.stderr}"
    if expect_success and completed.returncode != 0:
        raise AssertionError(
            f"dbt {' '.join(arguments)} failed with exit code {completed.returncode}:\n"
            + "\n".join(output.splitlines()[-30:])
        )
    if not expect_success and completed.returncode == 0:
        raise AssertionError(
            f"dbt {' '.join(arguments)} was expected to fail and succeeded:\n"
            + "\n".join(output.splitlines()[-15:])
        )
    return output


def _scenario_runner() -> emit.DbtRunOperation:
    """The migration operations, run against the scratch estate's own DuckDB file."""
    root = _scenario_estate()
    return emit.DbtRunOperation(
        project_dir=root,
        profiles_dir=root / "profiles",
        target="duckdb",
        echo=False,
        env=_dbt_env(),
    )


def _scenario_ctx():
    root = _scenario_estate()
    return emit.EstateContext.resolve(estate_root=root)


def _scenario_emit(*extra: str) -> tuple[int, str]:
    root = _scenario_estate()
    return _run_main_capturing(["--estate-root", str(root), *extra])[:2]


def _satellite_state() -> dict:
    """Row counts, the identity digest and every stored hashdiff, per satellite.

    The identity digest covers the facts a re-baseline must leave alone: the business
    key, the effective time, the load datetime and the record source.
    """
    import duckdb

    connection = duckdb.connect(str(SCENARIO["duckdb"]), read_only=True)
    try:
        state: dict = {}
        for name in ESTATE_SATELLITES:
            schema = connection.execute(
                "select table_schema from information_schema.tables where table_name = ?",
                [name],
            ).fetchone()
            assert schema is not None, f"the scratch estate carries no relation {name}"
            relation = f'"{schema[0]}"."{name}"'
            columns = [
                row[0]
                for row in connection.execute(
                    "select column_name from information_schema.columns "
                    "where table_schema = ? and table_name = ? order by ordinal_position",
                    [schema[0], name],
                ).fetchall()
            ]
            rows = connection.execute(
                f"select product_hk, effective_from, load_datetime, record_source, "
                f"product_hashdiff from {relation} "
                f"order by product_hk, effective_from, load_datetime, product_hashdiff"
            ).fetchall()
            identity = hashlib.sha256(
                json.dumps(
                    [[str(value) for value in row[:4]] for row in rows], sort_keys=True
                ).encode("utf-8")
            ).hexdigest()
            state[name] = {
                "columns": columns,
                "count": len(rows),
                "identity": identity,
                "hashdiffs": {f"{row[0]}|{row[1]}": row[4] for row in rows},
            }
        return state
    finally:
        connection.close()


def _audit_rows() -> list[tuple]:
    import duckdb

    connection = duckdb.connect(str(SCENARIO["duckdb"]), read_only=True)
    try:
        return connection.execute(
            "select entity, old_basis_version, new_basis_version, rows_rewritten, migrated_at "
            "from main.dpf_estate_evolution_audit order by entity, new_basis_version"
        ).fetchall()
    finally:
        connection.close()


def _ledger_record() -> dict:
    path = _scenario_estate() / "declarations" / "evolution" / f"{ESTATE_DOMAIN}.lock.yml"
    return emit.load_evolution_ledger(path)[ESTATE_ENTITY]


def _declare_the_extension() -> None:
    """Add one payload column to the entity: a seed column on the source that carries it,
    and a null projection on the sibling source that does not."""
    root = _scenario_estate()
    domain_path = root / "domains" / f"{ESTATE_DOMAIN}.yml"
    domain_doc = yaml.safe_load(domain_path.read_text(encoding="utf-8"))
    domain_doc["entity_configs"][ESTATE_ENTITY]["payload"].append(NEW_COLUMN)
    domain_path.write_text(yaml.safe_dump(domain_doc, sort_keys=False), encoding="utf-8")

    for source, expression in (("cartivo", f"cast({NEW_COLUMN} as string)"), ("mercaro", "cast(null as string)")):
        path = root / "declarations" / f"{source}.yml"
        document = yaml.safe_load(path.read_text(encoding="utf-8"))
        table = document["tables"]["products"]
        table["projection"].append({"name": NEW_COLUMN, "expression": expression})
        for vault in table["vault_entities"]:
            if vault["entity"] == ESTATE_ENTITY:
                vault["bridge"]["select"].append(
                    {"name": NEW_COLUMN, "expression": f"source.{NEW_COLUMN}"}
                )
        path.write_text(yaml.safe_dump(document, sort_keys=False), encoding="utf-8")

    seed = root / "seeds" / "raw_cartivo_products.csv"
    lines = seed.read_text(encoding="utf-8").splitlines()
    rewritten = [f"{lines[0]},{NEW_COLUMN}"] + [f"{line}," for line in lines[1:] if line]
    seed.write_text("\n".join(rewritten) + "\n", encoding="utf-8")

    # The seed's declared column types are the estate's own dbt configuration, and a new
    # seed column joins them; dbt hands the list straight to the CSV reader.
    project_path = root / "dbt_project.yml"
    project = yaml.safe_load(project_path.read_text(encoding="utf-8"))
    project["seeds"]["ergasterion"]["raw_cartivo_products"]["+column_types"][NEW_COLUMN] = "string"
    project_path.write_text(yaml.safe_dump(project, sort_keys=False), encoding="utf-8")
    _invalidate_parse_cache()


def _set_the_note(value: str) -> None:
    """Change one source row's value in the post-extension column, and nothing else."""
    seed = _scenario_estate() / "seeds" / "raw_cartivo_products.csv"
    lines = seed.read_text(encoding="utf-8").splitlines()
    rewritten = []
    for line in lines:
        if line.startswith(f"{NOTED_RECORD},"):
            head, _, _tail = line.rpartition(",")
            line = f"{head},{value}"
        rewritten.append(line)
    seed.write_text("\n".join(rewritten) + "\n", encoding="utf-8")


def test_scenario_the_copied_estate_regenerates_and_builds_on_duckdb() -> None:
    """The scratch estate is the committed estate: it regenerates byte-identically and
    builds its product satellites on its own DuckDB file."""
    root = _scenario_estate()
    exit_code, out = _scenario_emit("--check")
    assert exit_code == 0, f"the copied estate must regenerate byte-identically: {out}"
    assert "would change 0 of " in out, out

    _run_dbt("build", "--select", *ESTATE_SELECTION)
    state = _satellite_state()
    for name in ESTATE_SATELLITES:
        assert state[name]["count"] > 0, f"{name} must carry stored history to re-baseline"
        assert NEW_COLUMN not in state[name]["columns"], (
            f"{name} carries {NEW_COLUMN} before the extension declared it"
        )
    SCENARIO["baseline"] = state


def test_scenario_the_extension_lands_online_with_history_untouched() -> None:
    """A payload addition reaches the satellite in place: the column arrives, every
    stored row keeps its identity and its fingerprint, and no version is appended."""
    _declare_the_extension()
    exit_code, out = _scenario_emit()
    assert exit_code == 0, f"an extension must emit cleanly: {out}"
    assert "estate evolution: extension" in out, out
    assert NEW_COLUMN in out, out

    # The raw landing relation takes the new column: a seed reload, which is the landing
    # layer's own act and reaches no history-bearing relation.
    _run_dbt("seed", "--select", "raw_cartivo_products", "--full-refresh")
    _run_dbt("build", "--select", *ESTATE_SELECTION)
    state = _satellite_state()
    baseline = SCENARIO["baseline"]
    for name in ESTATE_SATELLITES:
        assert NEW_COLUMN in state[name]["columns"], (
            f"{name} must gain {NEW_COLUMN} in place through append_new_columns"
        )
        assert state[name]["count"] == baseline[name]["count"], (
            f"{name} replayed {state[name]['count'] - baseline[name]['count']} version(s) on "
            f"an extension; the frozen basis must leave every stored fingerprint alone"
        )
        assert state[name]["identity"] == baseline[name]["identity"]
        assert state[name]["hashdiffs"] == baseline[name]["hashdiffs"], (
            f"{name} moved a stored hashdiff on an extension"
        )
    SCENARIO["extended"] = state


def test_scenario_a_change_only_in_the_new_column_appends_nothing() -> None:
    """While the basis is frozen, a change arriving only in a post-extension column
    produces no new version. This is the gap the re-baseline closes."""
    _set_the_note("NOTE-1")
    _run_dbt("build", "--select", *ESTATE_SELECTION)
    state = _satellite_state()
    extended = SCENARIO["extended"]
    for name in ESTATE_SATELLITES:
        assert state[name]["count"] == extended[name]["count"], (
            f"{name} appended a version for a change outside the frozen basis"
        )
        assert state[name]["hashdiffs"] == extended[name]["hashdiffs"]
    SCENARIO["before_migration"] = state


def test_scenario_crash_boundaries_block_the_next_build_or_converge() -> None:
    """The crash-boundary matrix, on DuckDB. Each arm interrupts the command at one
    boundary, and each one either blocks the next build with the named error or converges
    on a re-run, with zero replayed versions throughout."""
    runner = _scenario_runner()
    ctx = _scenario_ctx()
    before = SCENARIO["before_migration"]

    def assert_no_replay(where: str) -> dict:
        state = _satellite_state()
        for name in ESTATE_SATELLITES:
            assert state[name]["count"] == before[name]["count"], (
                f"{where}: {name} replayed "
                f"{state[name]['count'] - before[name]['count']} version(s)"
            )
            assert state[name]["identity"] == before[name]["identity"], (
                f"{where}: {name} moved a business key, effective time, load datetime or "
                f"record source"
            )
        return state

    # Arm one: interrupted before the pending commit. Nothing was recorded, so the next
    # build runs and the estate is exactly where it was.
    plan = emit.rebaseline_plan(ESTATE_DOMAIN, ESTATE_ENTITY, ctx=ctx)
    assert plan["pending"] is False
    assert NEW_COLUMN in plan["basis"], "the new basis adopts the extended payload"
    _run_dbt("run", "--select", "stg_dv_cartivo_products")
    assert_no_replay("arm one")
    assert _scenario_emit("--check")[0] == 0, "arm one leaves emit clean"

    # Arm two: interrupted between the pending commit and the rewrite. The build stops
    # with the named error, and so does emit.
    emit.rebaseline_begin(plan, runner)
    blocked = _run_dbt("run", "--select", "stg_dv_cartivo_products", expect_success=False)
    assert "estate evolution: pending re-baseline" in blocked, blocked
    for token in ("--complete", "--abort", f"{ESTATE_DOMAIN}.{ESTATE_ENTITY}"):
        assert token in blocked, f"expected {token!r} in the build's error: {blocked}"
    exit_code, out = _scenario_emit("--check")
    assert exit_code == 1 and "pending re-baseline" in out, out
    assert_no_replay("arm two")

    # Arm three: interrupted mid-rewrite. One satellite is rewritten, the other is not,
    # the build stays blocked, and the full rewrite converges the pair.
    first, second = ESTATE_SATELLITES
    partial = emit.rebaseline_rewrite(plan, runner, satellites=[first])
    assert partial["rows_rewritten"] > 0, (
        "the new basis must move the stored fingerprints, or the matrix proves nothing"
    )
    midway = assert_no_replay("arm three")
    assert midway[first]["hashdiffs"] != before[first]["hashdiffs"]
    assert midway[second]["hashdiffs"] == before[second]["hashdiffs"]
    blocked = _run_dbt("run", "--select", "stg_dv_cartivo_products", expect_success=False)
    assert "estate evolution: pending re-baseline" in blocked, blocked

    # Arm four: interrupted between the rewrite and the promotion. The build stays
    # blocked; the re-run rewrites nothing and promotes.
    full = emit.rebaseline_rewrite(plan, runner)
    assert full["rows_rewritten"] > 0
    rewritten = assert_no_replay("arm four")
    assert rewritten[second]["hashdiffs"] != before[second]["hashdiffs"]
    blocked = _run_dbt("run", "--select", "stg_dv_cartivo_products", expect_success=False)
    assert "estate evolution: pending re-baseline" in blocked, blocked

    settled = emit.rebaseline_rewrite(plan, runner)
    assert settled["rows_rewritten"] == 0, (
        f"a second execution must change zero rows, got {settled['rows_rewritten']}"
    )
    assert _satellite_state() == rewritten, "a second execution moved the estate"

    # One audit row per entity and new basis version, holding after the re-run.
    audit = _audit_rows()
    assert len(audit) == 1, f"expected exactly one audit row, got {audit}"
    entity, old_version, new_version, rows_rewritten, migrated_at = audit[0]
    assert entity == ESTATE_ENTITY
    assert old_version == plan["previous_basis_version"]
    assert new_version == plan["basis_version"]
    assert rows_rewritten == 0, "the converged re-run records the rows it rewrote: none"
    assert migrated_at is not None

    assert emit.ledger_promote_pending(plan) is True
    promoted = _ledger_record()
    assert promoted["basis_version"] == plan["basis_version"]
    assert NEW_COLUMN in promoted["hashdiff_basis"]
    assert "pending" not in promoted
    assert len(_pending_markers()) == 1, (
        "promotion does not release the warehouse gate before regenerated models deploy"
    )
    exit_code, out = _scenario_emit()
    assert exit_code == 0, f"the promoted ledger must regenerate the stage models: {out}"
    emit.rebaseline_clear(plan, runner)
    assert _pending_markers() == []
    SCENARIO["migrated"] = rewritten


def test_scenario_after_the_rebaseline_the_change_lands_as_one_version() -> None:
    """After deployment releases the gate, the new basis makes the change
    that arrived only in the newly based column appends exactly one new version."""
    _run_dbt("build", "--select", *ESTATE_SELECTION)
    state = _satellite_state()
    migrated = SCENARIO["migrated"]
    appended = {
        name: state[name]["count"] - migrated[name]["count"] for name in ESTATE_SATELLITES
    }
    assert sum(appended.values()) == 1, (
        f"expected exactly one new version across the entity's satellites, got {appended}"
    )
    assert appended["sat_product_cartivo"] == 1, (
        "the version belongs to the source that carries the changed value"
    )
    assert set(migrated["sat_product_cartivo"]["hashdiffs"]).issubset(
        set(state["sat_product_cartivo"]["hashdiffs"])
    ), "every stored version stays; the new one arrives beside them"

    _run_dbt("build", "--select", *ESTATE_SELECTION)
    assert _satellite_state() == state, "a repeat build must append zero rows"
    SCENARIO["settled"] = state


def test_scenario_an_abort_demotes_the_pending_basis_and_converges() -> None:
    """An abort drops the pending basis and rewrites under the active one, so the estate
    converges on the fingerprints it already stores and the next build runs clean."""
    root = _scenario_estate()
    runner = _scenario_runner()
    settled = SCENARIO["settled"]

    # A hashdiff_exclude edit after the basis froze is graded like a payload delta: emit
    # names the estate migration requirement and points at the re-baseline.
    domain_path = root / "domains" / f"{ESTATE_DOMAIN}.yml"
    original = domain_path.read_text(encoding="utf-8")
    document = yaml.safe_load(original)
    document.setdefault("hashdiff_exclude", {})[ESTATE_ENTITY] = [NEW_COLUMN]
    domain_path.write_text(yaml.safe_dump(document, sort_keys=False), encoding="utf-8")
    exit_code, out = _scenario_emit("--check")
    assert exit_code == 1 and "estate migration requirement" in out, out
    assert "rebaseline" in out, out

    ctx = _scenario_ctx()
    plan = emit.rebaseline_plan(ESTATE_DOMAIN, ESTATE_ENTITY, ctx=ctx)
    assert NEW_COLUMN not in plan["basis"], "the pending basis drops the excluded column"
    emit.rebaseline_begin(plan, runner)
    applied = emit.rebaseline_rewrite(plan, runner)
    assert applied["rows_rewritten"] > 0, "the abort must have something to converge"
    diverged = _satellite_state()
    assert diverged["sat_product_cartivo"]["hashdiffs"] != settled["sat_product_cartivo"]["hashdiffs"]

    # The abort: the pending basis goes, the rewrite runs under the active basis, and the
    # warehouse marker clears.
    assert emit.ledger_demote_pending(plan) is True
    converged = emit.rebaseline_rewrite(plan, runner, basis=plan["active_basis"])
    assert converged["rows_rewritten"] == applied["rows_rewritten"]
    emit.rebaseline_clear(plan, runner)

    state = _satellite_state()
    for name in ESTATE_SATELLITES:
        assert state[name]["hashdiffs"] == settled[name]["hashdiffs"], (
            f"{name} did not converge back onto the active basis"
        )
        assert state[name]["count"] == settled[name]["count"]
        assert state[name]["identity"] == settled[name]["identity"]

    record = _ledger_record()
    assert "pending" not in record
    assert record["basis_version"] == plan["previous_basis_version"]

    domain_path.write_text(original, encoding="utf-8")
    assert _scenario_emit("--check")[0] == 0, "the estate is clean once the exclusion is gone"
    _run_dbt("build", "--select", *ESTATE_SELECTION)
    assert _satellite_state() == state, "the build after an abort appends zero rows"


def _pending_markers() -> list[tuple]:
    import duckdb

    connection = duckdb.connect(str(SCENARIO["duckdb"]), read_only=True)
    try:
        return connection.execute(
            "select entity, domain_name, previous_basis_version, basis_version "
            "from main.dpf_estate_evolution_pending order by entity"
        ).fetchall()
    finally:
        connection.close()


def _run_command(*arguments: str) -> tuple[int, str, str]:
    """`ergasterion evolve ...` through the console entry point, against the scratch
    estate's own DuckDB file."""
    root = _scenario_estate()
    argv = [
        "evolve",
        "rebaseline",
        ESTATE_DOMAIN,
        ESTATE_ENTITY,
        "--estate-root",
        str(root),
        "--profiles-dir",
        str(root / "profiles"),
        "--target",
        "duckdb",
        "--quiet",
        *arguments,
    ]
    saved = {key: os.environ.get(key) for key in ("DPF_DUCKDB_PATH", "DBT_PARTIAL_PARSE")}
    os.environ.update(_dbt_env())
    old_argv = sys.argv
    out, err = io.StringIO(), io.StringIO()
    try:
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            exit_code = cli.main(argv)
    finally:
        sys.argv = old_argv
        for key, value in saved.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
    return exit_code, out.getvalue(), err.getvalue()


def test_scenario_the_command_drives_both_phases_and_converges() -> None:
    """The operator's command, end to end: `ergasterion evolve rebaseline <domain>
    <entity>` records the pending basis, runs the rewrite, verifies it, writes the audit
    row and promotes the basis while the warehouse gate remains closed. Emit and deploy
    happen before the explicit `--clear` release."""
    settled = SCENARIO["settled"]
    before = _ledger_record()

    exit_code, out, err = _run_command()
    assert exit_code == 0, f"the command must complete: {out}\n{err}"
    assert "pending basis version" in out and "warehouse rewrite" in out, out

    promoted = _ledger_record()
    assert promoted["basis_version"] == before["basis_version"] + 1
    assert promoted["hashdiff_basis"] == before["hashdiff_basis"], (
        "the declared basis has not moved, so the recomputed basis is the same column set"
    )
    assert "pending" not in promoted
    assert len(_pending_markers()) == 1, "the command must leave the warehouse gate closed"

    # One warehouse audit row for this entity and this new basis version, carrying the
    # basis versions it moved between, the rows it rewrote and when.
    matching = [
        row
        for row in _audit_rows()
        if row[0] == ESTATE_ENTITY and row[2] == promoted["basis_version"]
    ]
    assert len(matching) == 1, f"expected exactly one audit row, got {matching}"
    _entity, old_version, _new_version, rows_rewritten, migrated_at = matching[0]
    assert old_version == before["basis_version"]
    assert rows_rewritten == 0, "the basis did not move, so no fingerprint was rewritten"
    assert migrated_at is not None
    assert _satellite_state() == settled, (
        "a re-baseline onto the same column set rewrites no fingerprint and appends no row"
    )
    assert _scenario_emit("--check")[0] == 0, "the regenerated estate stays byte-stable"
    exit_code, out, err = _run_command("--clear")
    assert exit_code == 0, f"the explicit release must succeed: {out}\n{err}"
    assert "warehouse build gate released" in out, out
    assert _pending_markers() == []

    # Nothing pending anywhere: the command says so and stops.
    exit_code, out, err = _run_command("--complete")
    assert exit_code == 2, f"a --complete with nothing pending must fail: {out}\n{err}"
    assert "no pending re-baseline" in err, err

    # The last crash boundary: the promotion landed and the marker deliberately outlived
    # it. --complete explains the deployment step and does not release the gate.
    runner = _scenario_runner()
    runner(
        emit.REBASELINE_BEGIN_MACRO,
        emit.rebaseline_payload(
            emit.rebaseline_plan(ESTATE_DOMAIN, ESTATE_ENTITY, ctx=_scenario_ctx())
        ),
    )
    assert len(_pending_markers()) == 1
    blocked = _run_dbt("run", "--select", "stg_dv_cartivo_products", expect_success=False)
    assert "estate evolution: pending re-baseline" in blocked, blocked

    exit_code, out, err = _run_command("--complete")
    assert exit_code == 0, f"the command must converge the estate: {out}\n{err}"
    assert "gate remains closed" in out, out
    assert len(_pending_markers()) == 1
    exit_code, out, err = _run_command("--clear")
    assert exit_code == 0, f"the explicit release must converge the estate: {out}\n{err}"
    assert _pending_markers() == []
    _run_dbt("run", "--select", "stg_dv_cartivo_products")
    assert _satellite_state() == settled, "convergence appended no row"


def _column_type(relation: str, column: str) -> str:
    import duckdb

    connection = duckdb.connect(str(SCENARIO["duckdb"]), read_only=True)
    try:
        row = connection.execute(
            "select data_type from information_schema.columns "
            "where table_name = ? and column_name = ?",
            [relation, column],
        ).fetchone()
        assert row is not None, f"{relation} carries no column {column}"
        return row[0]
    finally:
        connection.close()


def test_scenario_the_migration_adds_a_missing_satellite_column() -> None:
    """A re-baseline run before the estate is rebuilt meets a satellite that does not
    carry the extension's column yet. The migration adds it, typed from the stage relation
    it is built from, and recomputes over the basis that now includes it, leaving every
    row identity and count where they were."""
    import duckdb

    settled = SCENARIO["settled"]
    stage_type = _column_type("stg_dv_cartivo_products", NEW_COLUMN)

    connection = duckdb.connect(str(SCENARIO["duckdb"]))
    try:
        connection.execute(
            f'alter table "main_raw_vault"."sat_product_cartivo" drop column {NEW_COLUMN}'
        )
    finally:
        connection.close()

    runner = _scenario_runner()
    plan = emit.rebaseline_plan(ESTATE_DOMAIN, ESTATE_ENTITY, ctx=_scenario_ctx())
    assert NEW_COLUMN in plan["basis"]
    outcome = emit.rebaseline_rewrite(plan, runner, satellites=["sat_product_cartivo"])
    assert outcome["columns_added"]["sat_product_cartivo"] == [NEW_COLUMN], (
        f"the migration must add the missing column, got {outcome['columns_added']}"
    )

    assert _column_type("sat_product_cartivo", NEW_COLUMN) == stage_type, (
        "the added column takes its type from the stage relation the satellite is built from"
    )
    state = _satellite_state()
    assert state["sat_product_cartivo"]["count"] == settled["sat_product_cartivo"]["count"], (
        "adding a column appends no row"
    )
    assert state["sat_product_cartivo"]["identity"] == settled["sat_product_cartivo"]["identity"], (
        "business keys, effective times, load datetimes and record sources stay as they were"
    )
    moved = [
        key
        for key, value in state["sat_product_cartivo"]["hashdiffs"].items()
        if settled["sat_product_cartivo"]["hashdiffs"][key] != value
    ]
    assert outcome["rows_rewritten"] == len(moved) == 1, (
        f"exactly the row whose value the dropped column carried is rewritten, got {moved}"
    )


TESTS = [
    test_first_emit_bootstraps_the_ledger_and_leaves_every_file_byte_identical,
    test_bootstrap_consumes_hashdiff_exclude_once_into_the_recorded_basis,
    test_ledger_bytes_are_deterministic_and_lf,
    test_extension_lands_in_every_payload_layer_with_the_basis_frozen,
    test_extension_prints_a_notice_naming_the_columns_outside_change_detection,
    test_removal_fails_with_an_estate_migration_requirement,
    test_rename_fails_on_the_removal_half_with_an_estate_migration_requirement,
    test_redefinition_of_a_recorded_type_fails_with_an_estate_migration_requirement,
    test_redefinition_of_a_projection_expression_fails_but_formatting_whitespace_does_not,
    test_hashdiff_exclude_edit_after_bootstrap_fails_with_a_re_baseline_remedy,
    test_post_extension_steady_state_grades_clean_on_every_later_emit,
    test_excluding_a_post_extension_column_leaves_the_frozen_basis_alone,
    test_dbt_project_pins_the_raw_vault_satellites_for_online_evolution,
    test_no_vault_template_emits_model_configuration,
    test_generated_satellite_tests_use_the_nested_arguments_mapping,
    test_committed_estate_ledger_matches_the_committed_domains,
    test_a_pending_basis_round_trips_through_the_ledger,
    test_a_pending_basis_stops_emit_with_the_named_error,
    test_promotion_and_demotion_move_the_pending_basis,
    test_rebaseline_plan_reads_the_declared_basis_and_every_satellite,
    test_the_migration_operation_result_is_read_from_the_marker_line,
    test_the_migration_carries_exactly_one_hash_construction,
    test_the_build_gate_rides_every_dispatched_stage_arm,
    test_the_scaffold_carries_the_migration_macro_byte_for_byte,
    test_golden_hash_parity_executes_offline_on_duckdb,
    # The isolated dbt-project lane runs in order over one scratch estate copy.
    test_scenario_the_copied_estate_regenerates_and_builds_on_duckdb,
    test_scenario_the_extension_lands_online_with_history_untouched,
    test_scenario_a_change_only_in_the_new_column_appends_nothing,
    test_scenario_crash_boundaries_block_the_next_build_or_converge,
    test_scenario_after_the_rebaseline_the_change_lands_as_one_version,
    test_scenario_an_abort_demotes_the_pending_basis_and_converges,
    test_scenario_the_command_drives_both_phases_and_converges,
    test_scenario_the_migration_adds_a_missing_satellite_column,
]


def main() -> int:
    failures = 0
    try:
        for test in TESTS:
            try:
                test()
            except Exception:
                failures += 1
                print(f"FAIL {test.__name__}")
                traceback.print_exc()
            else:
                print(f"PASS {test.__name__}")
    finally:
        _scenario_cleanup()
    if failures:
        print(f"{failures} of {len(TESTS)} estate-evolution tests failed")
        return 1
    print(f"estate evolution OK: {len(TESTS)} tests green")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
