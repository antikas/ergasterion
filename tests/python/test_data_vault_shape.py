"""Self-tests for the ``data_vault`` shape: hubs, links, satellites, a
business-vault surviving record and a point-in-time relation over a
curated product (architecture sections 6, 9, 10, 12, 13, 14 check 3).

Same plain assert-and-report convention as the rest of this repo (no
pytest in the .venv): each ``test_*`` raises ``AssertionError`` on
failure, ``main()`` runs them all and reports PASS/FAIL.

Everything runs against ``tests/fixtures/estates/vault_one``, copied into
a scratch directory so a red case can mutate one file without touching the
fixture. Every red case drives the real ``ergasterion emit-products``
command, or a real dbt build of what that command wrote, and asserts what
the failure names.

The harness below serves every lane in this file, the hashdiff-basis,
pending-gate and re-baseline lanes and the replay-suppression and watermark
lanes among them: each drives the real commands against one scratch copy of
the fixture estate.

Covers:
  - the cheap lane: the identity-key and change-fingerprint construction,
    rendered from the shipped macro and held to golden hashes executed in an
    in-process DuckDB;
  - the command emits every relation the shape renders, is byte-stable
    across two runs, and ``--check`` is clean on what it wrote;
  - the vault's stores render as insert-only relations keyed by their own
    key, the surviving record and the point-in-time relation as tables,
    and the estate's structural budgets pass on both declared adapters;
  - the executed DuckDB build: two customers survive curation, so both
    hubs, the link and both satellites carry two rows; the surviving
    record carries the value each declared strategy picks, and the two
    strategies reach different satellites for the two columns both store;
    the point-in-time relation points at the version in force at each
    snapshot; the two relations the shape derives from its stores read only
    the highest basis version each entity carries;
  - an unchanged payload delivered at a later instant separates the two
    satellite kinds, and what each store does across builds -- replay
    suppression, the watermark, the hashdiff basis and its declared
    re-baseline -- is proven here against the same fixture;
  - every published column carries its declared type on the built
    adapter, and a changed declared type follows into the built one;
  - the contract lists every relation the shape renders, and a consumer
    naming one of them by its producer-relative name resolves and builds;
  - the generated tests go red on what they name: a hub identity that does
    not depend on the business key, a snapshot spine that stops
    deduplicating, and a null in a column the contract declares required;
  - the fail-closed branches: a ``declared`` product carrying a hubs
    section, a vault whose composition curates nothing, a satellite naming
    an unknown parent, a link naming an unknown hub, a payload column the
    composition does not carry, a change column that is not an instant,
    every payload column excluded from change detection, a surviving
    attribute no declared satellite stores, a point-in-time relation
    naming another hub's satellite, and a payload column named as one the
    shape generates.

Usage:
    python tests/python/test_data_vault_shape.py
"""

from __future__ import annotations

import contextlib
import io
import json
import os
import shutil
import subprocess
import sys
import tempfile
import traceback
from pathlib import Path

import yaml

if __package__ in (None, ""):
    import os as _os
    import sys as _sys

    _sys.path.insert(0, _os.path.dirname(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))))
    _sys.path.insert(0, _os.path.dirname(_os.path.abspath(__file__)))

from ergasterion import cli
from ergasterion import emit_contracts as ec
from ergasterion import emit_products as ep
from ergasterion import structure_gate
from ergasterion.estate import EstateContext
from ergasterion.framework import contract as contract_mod
from ergasterion.framework.adapters import load_adapter_conventions
from ergasterion.shapes import data_vault as vault
from ergasterion.shapes.data_vault import evolution as evolution_mod
from ergasterion.translators.dbt_patterns import MODELS_ROOT, parse_gate
from ergasterion.translators.dbt_patterns import sql as sql_mod
from vault_hash_support import render_vault_hash

REPO_ROOT = Path(__file__).resolve().parents[2]
FIXTURE_ESTATE = REPO_ROOT / "tests" / "fixtures" / "estates" / "vault_one"
HASH_PARITY_FIXTURE = REPO_ROOT / "tests" / "fixtures" / "data_vault_hash_parity.json"
ENGINE_MACROS = (
    "cross_db.sql",
    "data_vault.sql",
    "product_tests.sql",
    "publish.sql",
    "quarantine.sql",
    "survivorship.sql",
)

VAULT_PRODUCT = "crm.customer_vault"
VAULT_DECLARATION = "customer_vault.yml"
FEED_DECLARATION = "customer_feed.yml"
ACCOUNT_DECLARATION = "account_feed.yml"
SATELLITE_DETAIL = "customer_detail"
SATELLITE_OBSERVED = "customer_observed"
LEDGER_PATH = evolution_mod.ledger_relative_path(domain="crm", name="customer_vault")

MODELS_DIR = f"{MODELS_ROOT}/crm"
VAULT_SCHEMA = f"{MODELS_DIR}/crm__customer_vault.yml"
HUB_CUSTOMER = "crm__customer_vault__hub_customer"
HUB_ACCOUNT = "crm__customer_vault__hub_account"
LINK_MODEL = "crm__customer_vault__link_customer_account"
SAT_DETAIL = "crm__customer_vault__sat_customer_detail"
SAT_OBSERVED = "crm__customer_vault__sat_customer_observed"
GOLDEN_MODEL = "crm__customer_vault__golden_customer"
PIT_MODEL = "crm__customer_vault__pit_customer"
DIRECTORY_MODEL = "crm__customer_directory"
REGISTER_MODEL = "crm__customer_account_register"
REGISTER_DECLARATION = "customer_account_register.yml"

VAULT_MODELS = (
    HUB_CUSTOMER,
    HUB_ACCOUNT,
    LINK_MODEL,
    SAT_DETAIL,
    SAT_OBSERVED,
    GOLDEN_MODEL,
    PIT_MODEL,
)
VAULT_STORES = (HUB_CUSTOMER, HUB_ACCOUNT, LINK_MODEL, SAT_DETAIL, SAT_OBSERVED)

EXPECTED_ARTEFACTS = {
    LEDGER_PATH,
    "manifests/products/crm/account_feed.json",
    "manifests/products/crm/customer_account_register.json",
    "manifests/products/crm/customer_directory.json",
    "manifests/products/crm/customer_feed.json",
    "manifests/products/crm/customer_vault.json",
    VAULT_SCHEMA,
    f"{MODELS_DIR}/crm__customer_vault__base.sql",
    *(f"{MODELS_DIR}/{model}.sql" for model in VAULT_MODELS),
    *(f"{MODELS_DIR}/{model}_current.sql" for model in VAULT_MODELS),
    *(f"{MODELS_DIR}/{model}_sla.sql" for model in VAULT_MODELS),
    f"{MODELS_DIR}/crm__customer_vault__resolution.sql",
    f"{MODELS_DIR}/crm__customer_vault__pending_keys.sql",
    f"{MODELS_DIR}/{DIRECTORY_MODEL}.sql",
    f"{MODELS_DIR}/{REGISTER_MODEL}.sql",
}

# The rows the fixture seed delivers, as the tests append to them.
CHANGED_ATTRIBUTE_ROW = "R-04,crm,C-001,Ada B. Byron,SW1A 1AA,ada@example.com,active,10,2026-03-15"
UNCHANGED_PAYLOAD_ROW = "R-05,crm,C-002,Grace Hopper,EC1A 1BB,grace@example.com,active,20,2026-04-01"
# A delivery that changes only the two columns both satellites store and
# both exclude from change detection. customer_detail's fingerprint is
# unchanged, so it stores no new version and keeps the values it holds;
# customer_observed records the observation and holds the new ones. That is
# what separates the two survivorship strategies.
EXCLUDED_COLUMNS_ROW = "R-06,crm,C-001,Ada Byron,W1A 9ZZ,ada.byron@example.com,active,10,2026-03-15"


# --------------------------------------------------------------------------- harness


def _scratch_estate(root: Path) -> Path:
    """The fixture estate copied into ``root``, with the engine's own macros
    beside the estate's. The macros are copied rather than duplicated in the
    fixture so the estate always builds against the macros the engine
    ships."""

    estate = root / "estate"
    shutil.copytree(FIXTURE_ESTATE, estate)
    (estate / "macros").mkdir(exist_ok=True)
    for name in ENGINE_MACROS:
        shutil.copy(REPO_ROOT / "macros" / name, estate / "macros" / name)
    return estate


def _run(estate: Path, *arguments: str) -> tuple[int, str]:
    out, err = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        code = cli.main(["emit-products", "--estate-root", str(estate), *arguments])
    return code, out.getvalue() + err.getvalue()


def _emit(estate: Path) -> str:
    code, output = _run(estate)
    assert code == 0, f"emit-products failed:\n{output}"
    return output


def _fails(estate: Path, *expected: str) -> str:
    code, output = _run(estate)
    assert code == 1, f"expected the command to fail closed, it exited {code}:\n{output}"
    for token in expected:
        assert token in output, f"the failure does not name {token!r}:\n{output}"
    return output


def _rebaseline(estate: Path, satellite: str, mode: str, *, expect: int = 0) -> str:
    """One phase of the declared re-baseline operation, driven through the
    real command."""

    out, err = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        code = cli.main(
            [
                "vault-rebaseline",
                "--product",
                VAULT_PRODUCT,
                "--satellite",
                satellite,
                "--estate-root",
                str(estate),
                mode,
            ]
        )
    output = out.getvalue() + err.getvalue()
    assert code == expect, f"vault-rebaseline {mode} exited {code}:\n{output}"
    return output


def _read(estate: Path, relative: str) -> str:
    return (estate / relative).read_text(encoding="utf-8")


def _ledger(estate: Path) -> dict:
    return yaml.safe_load(_read(estate, LEDGER_PATH))


def _tree(estate: Path) -> dict[str, bytes]:
    files: dict[str, bytes] = {}
    for root in (MODELS_ROOT, "manifests/products", evolution_mod.LEDGER_DIRECTORY):
        directory = estate / root
        if not directory.is_dir():
            continue
        for path in sorted(directory.rglob("*")):
            if path.is_file():
                files[path.relative_to(estate).as_posix()] = path.read_bytes()
    return files


def _edit_declaration(estate: Path, name: str, mutate) -> None:
    path = estate / "declarations" / "products" / name
    document = yaml.safe_load(path.read_text(encoding="utf-8"))
    mutate(document)
    path.write_text(yaml.safe_dump(document, sort_keys=False), encoding="utf-8")


def _shape_config(document: dict) -> dict:
    return document["target"]["shape_config"]


def _satellite(document: dict, name: str) -> dict:
    return next(
        entry for entry in _shape_config(document)["satellites"] if entry["name"] == name
    )


def _append_seed_row(estate: Path, name: str, row: str) -> None:
    path = estate / "seeds" / f"{name}.csv"
    path.write_text(path.read_text(encoding="utf-8") + row + "\n", encoding="utf-8")


def _drop_last_seed_rows(estate: Path, name: str, count: int) -> None:
    path = estate / "seeds" / f"{name}.csv"
    lines = path.read_text(encoding="utf-8").splitlines()
    path.write_text("\n".join(lines[: len(lines) - count]) + "\n", encoding="utf-8")


def _dbt_executable() -> Path:
    candidate = Path(sys.executable).resolve().parent / ("dbt.exe" if os.name == "nt" else "dbt")
    assert candidate.is_file(), (
        f"the executed DuckDB lane needs the dbt executable beside the interpreter: {candidate}"
    )
    return candidate


def _dbt(estate: Path) -> subprocess.CompletedProcess:
    return subprocess.run(
        [
            str(_dbt_executable()),
            "build",
            "--profiles-dir",
            "profiles",
            "--project-dir",
            str(estate),
        ],
        cwd=estate,
        capture_output=True,
        text=True,
    )


def _dbt_build(estate: Path) -> str:
    result = _dbt(estate)
    assert result.returncode == 0, result.stdout + result.stderr
    return result.stdout + result.stderr


def _dbt_fails(estate: Path, *expected: str) -> str:
    result = _dbt(estate)
    output = result.stdout + result.stderr
    assert result.returncode != 0, f"expected the dbt build to fail:\n{output}"
    for token in expected:
        assert token in output, f"the dbt failure does not name {token!r}:\n{output}"
    return output


def _query(estate: Path, statement: str):
    import duckdb

    connection = duckdb.connect(str(estate / "target" / "vault_one.duckdb"), read_only=True)
    try:
        return connection.execute(statement).fetchall()
    finally:
        connection.close()


def _rows(estate: Path, relation: str) -> int:
    return _query(estate, f"select count(*) from {relation}")[0][0]


def _counts(estate: Path) -> dict[str, int]:
    return {relation: _rows(estate, relation) for relation in VAULT_MODELS}


def _built_types(estate: Path, relation: str) -> dict[str, str]:
    rows = _query(
        estate,
        "select column_name, data_type from information_schema.columns "
        f"where table_name = '{relation}' order by ordinal_position",
    )
    return {name: str(kind) for name, kind in rows}


def _declared_types(estate: Path) -> dict[str, object]:
    """The neutral type the two delivered declarations state for every
    column they publish. Read from the declarations rather than restated
    here, so a type assertion is against what was declared."""

    types: dict[str, object] = {}
    for name in (FEED_DECLARATION, ACCOUNT_DECLARATION):
        document = yaml.safe_load(_read(estate, f"declarations/products/{name}"))
        step = next(entry for entry in document["steps"] if entry["pattern"] == "schema_transform")
        types.update({entry["to"]: entry["type"] for entry in step["mapping"] if "to" in entry})
    return types


def _physical_type(estate: Path, declared: object) -> str:
    """The type the reference adapter reports for one declared neutral
    type."""

    mapping = load_adapter_conventions("duckdb").type_mapping
    physical = mapping[sql_mod.SCALAR_TYPE_TOKENS[str(declared)]]
    return str(_query(estate, f"select typeof(cast(null as {physical}))")[0][0])


def _emitted_and_built(root: Path) -> Path:
    estate = _scratch_estate(root)
    _emit(estate)
    _dbt_build(estate)
    return estate


def _reemitted_and_rebuilt(estate: Path) -> None:
    _emit(estate)
    _dbt_build(estate)


# ------------------------------------------------------- cheap lane: golden-hash parity


def test_the_vault_hash_construction_matches_its_golden_vectors_on_duckdb() -> None:
    """The identity key and change fingerprint construction, pinned.

    Every hub, link and satellite row a live estate has already stored is
    keyed by ``dpf_vault_hash``. Nothing about that construction is derived
    from a declaration, and every suite in this repository builds its estate
    from scratch -- so a changed sentinel, separator, case rule or dispatch
    arm would leave every suite green while a live estate orphaned every
    stored row and re-inserted every identity under a new key.

    This lane closes that gap without dbt: it renders the shipped macro the
    way the running adapter renders it (``vault_hash_support``), holds the
    rendered SQL to the pinned text, executes it in an in-process DuckDB
    over vectors covering a null, an empty string, mixed case, an integer, a
    date and a value carrying the separator, and holds every row to its
    golden hash.
    """
    import duckdb

    fixture = json.loads(HASH_PARITY_FIXTURE.read_text(encoding="utf-8"))
    adapter = fixture["adapter"]
    expression = render_vault_hash(fixture["basis"], adapter=adapter)
    assert expression == fixture["hash_expression"], (
        "the shipped macro no longer renders the pinned construction:\n"
        f"  rendered: {expression}\n  pinned:   {fixture['hash_expression']}"
    )
    # The construction is per adapter through the shipped dispatch, not one
    # literal spelled once: the deployment adapter renders its own hex and
    # its own string type.
    deployment = render_vault_hash(fixture["basis"], adapter="bigquery")
    assert deployment != expression, "the hash construction did not dispatch per adapter"

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

    expected = fixture["expected_hash"]
    assert len(computed) == len(expected), (
        f"the vectors cover {len(expected)} rows, the run produced {len(computed)}"
    )
    for row_id, value in computed:
        assert value == expected[str(row_id)], (
            f"row {row_id}: the construction produced {value}, the golden hash is "
            f"{expected[str(row_id)]}"
        )

    # What the construction guarantees, stated as the vectors prove it: a
    # null and an empty string are different values (the sentinel is not the
    # empty string), case is carried rather than folded, and every declared
    # column reaches the fingerprint.
    assert expected["1"] != expected["2"], "a null and an empty string share a fingerprint"
    assert expected["3"] != expected["4"], "the construction folds case"
    assert expected["3"] != expected["5"], "an integer column does not reach the fingerprint"
    assert expected["3"] != expected["6"], "a date column does not reach the fingerprint"
    assert expected["3"] != expected["8"], "a value carrying the separator is not distinguished"
    assert len(set(expected.values())) == len(expected), (
        "the vectors must separate every genuinely different row"
    )


# --------------------------------------------------------------------------- green: the command


def test_the_command_emits_the_vault_and_reemission_is_byte_identical() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))
        summary = _emit(estate)
        assert "shape=data_vault" in summary, summary
        first = _tree(estate)
        assert EXPECTED_ARTEFACTS <= set(first), sorted(EXPECTED_ARTEFACTS - set(first))

        _emit(estate)
        assert _tree(estate) == first, "a second emission is not byte-identical"

        code, output = _run(estate, "--check")
        assert code == 0, output
        assert "0 problem(s)" in output, output

        parse_gate.assert_parses(
            {
                path: text.decode("utf-8")
                for path, text in first.items()
                if path.endswith(".sql")
            },
            adapters=("duckdb", "bigquery"),
        )


def test_the_vault_stores_are_insert_only_and_the_derived_relations_are_tables() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))
        _emit(estate)
        for model in VAULT_STORES:
            text = _read(estate, f"{MODELS_DIR}/{model}.sql")
            assert "materialized='incremental'" in text, model
            assert "dpf_incremental_strategy(" in text, model
        for model in (GOLDEN_MODEL, PIT_MODEL):
            assert "materialized='table'" in _read(estate, f"{MODELS_DIR}/{model}.sql"), model

        # check_structure() reports every artefact against the declared
        # view boundaries, and this estate declares none, so it also names
        # the generic checked and current-pointer views every product
        # carries -- pre-existing, shape-independent machinery, not
        # something this test judges. What this assertion checks is
        # narrower and is what this shape owns: of everything the gate
        # reports, none of it names a relation the vault publishes.
        primary = {f"{MODELS_DIR}/{model}.sql" for model in VAULT_MODELS}
        offenses = [
            offense
            for offense in structure_gate.check_structure(EstateContext.resolve(estate_root=estate))
            if offense.artefact in primary
        ]
        assert not offenses, [str(offense) for offense in offenses]


def test_the_declared_keys_of_every_store_reach_the_generated_configuration() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))
        _emit(estate)
        assert 'unique_key=["customer_hk"]' in _read(estate, f"{MODELS_DIR}/{HUB_CUSTOMER}.sql")
        assert 'unique_key=["customer_account_lhk"]' in _read(
            estate, f"{MODELS_DIR}/{LINK_MODEL}.sql"
        )
        for model in (SAT_DETAIL, SAT_OBSERVED):
            assert (
                'unique_key=["customer_hk", "effective_from", "hashdiff_basis_version"]'
                in _read(estate, f"{MODELS_DIR}/{model}.sql")
            ), model


# --------------------------------------------------------------------------- green: known answers


def test_the_hubs_and_the_link_carry_one_row_per_identity() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _emitted_and_built(Path(tmp))
        customers = _query(
            estate, f"select customer_ref, record_source from {HUB_CUSTOMER} order by customer_ref"
        )
        assert customers == [
            ("C-001", VAULT_PRODUCT),
            ("C-002", VAULT_PRODUCT),
        ], customers
        accounts = _query(estate, f"select account_ref from {HUB_ACCOUNT} order by account_ref")
        assert accounts == [("A-100",), ("A-200",)], accounts

        # The link's own hub keys are hashed from the same business keys in
        # the same order as the hubs' are, so the join below matches by
        # construction rather than by agreement.
        associations = _query(
            estate,
            f"select c.customer_ref, a.account_ref from {LINK_MODEL} as l "
            f"join {HUB_CUSTOMER} as c on c.customer_hk = l.customer_hk "
            f"join {HUB_ACCOUNT} as a on a.account_hk = l.account_hk "
            "order by c.customer_ref",
        )
        assert associations == [("C-001", "A-100"), ("C-002", "A-200")], associations


def test_the_satellites_store_one_version_per_entity_on_the_first_build() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _emitted_and_built(Path(tmp))
        detail = _query(
            estate,
            f"select h.customer_ref, s.full_name, s.postcode, s.email, s.effective_from, "
            f"s.hashdiff_basis_version from {SAT_DETAIL} as s "
            f"join {HUB_CUSTOMER} as h on h.customer_hk = s.customer_hk order by 1",
        )
        assert [(row[0], row[1], row[2], row[3], str(row[4]), row[5]) for row in detail] == [
            ("C-001", "Ada Byron", "SW1A 1AA", "ada@example.com", "2026-01-05", 1),
            ("C-002", "Grace Hopper", "EC1A 1BB", "grace@example.com", "2026-02-10", 1),
        ], detail

        observed = _query(
            estate,
            f"select h.customer_ref, s.customer_status, s.loyalty_points from {SAT_OBSERVED} "
            f"as s join {HUB_CUSTOMER} as h on h.customer_hk = s.customer_hk order by 1",
        )
        assert observed == [("C-001", "active", 10), ("C-002", "active", 20)], observed

        # Every fingerprint is derived, never empty, and two entities whose
        # basis columns differ never share one.
        fingerprints = _query(
            estate,
            f"select count(*), count(distinct hashdiff) from {SAT_DETAIL} where hashdiff is null "
            "or hashdiff = ''",
        )
        assert fingerprints == [(0, 0)], fingerprints
        distinct = _query(estate, f"select count(distinct hashdiff) from {SAT_DETAIL}")
        assert distinct == [(2,)], distinct


def test_the_surviving_record_carries_the_value_each_declared_strategy_picks() -> None:
    # C-001 is delivered by two source systems on one date, and the declared
    # source precedence puts crm first, so the curated record carries the crm
    # email. Every attribute is survived over the versions in force in the
    # declared satellites, so on the first build they are the values those
    # satellites store.
    with tempfile.TemporaryDirectory() as tmp:
        estate = _emitted_and_built(Path(tmp))
        rows = _query(
            estate,
            "select customer_ref, full_name, postcode, email, loyalty_points, record_source "
            f"from {GOLDEN_MODEL} order by customer_ref",
        )
        assert rows == [
            ("C-001", "Ada Byron", "SW1A 1AA", "ada@example.com", 10, VAULT_PRODUCT),
            ("C-002", "Grace Hopper", "EC1A 1BB", "grace@example.com", 20, VAULT_PRODUCT),
        ], rows
        empty = _query(
            estate, f"select count(*) from {GOLDEN_MODEL} where full_name is null or email is null"
        )
        assert empty == [(0,)], empty


def test_the_two_survivorship_strategies_pick_different_satellites() -> None:
    """most_recent and first_non_null, separated by an executed answer.

    ``postcode`` and ``email`` are stored by both declared satellites and
    excluded from both hashdiffs. A delivery that changes only those two
    adds no version to customer_detail -- its fingerprint is over the name
    alone -- and one to customer_observed, which records every observation.
    The two satellites then hold different values for the same two columns,
    and the surviving record shows each strategy reaching a different one:
    ``most_recent`` reads the later instant and takes the observation,
    ``first_non_null`` reads the declared satellite order and takes the
    first satellite's.
    """
    with tempfile.TemporaryDirectory() as tmp:
        estate = _emitted_and_built(Path(tmp))
        _append_seed_row(estate, "customer_records", EXCLUDED_COLUMNS_ROW)
        _dbt_build(estate)

        held = _query(
            estate,
            f"select 'detail', s.postcode, s.email from {SAT_DETAIL} as s "
            f"join {HUB_CUSTOMER} as h on h.customer_hk = s.customer_hk "
            "where h.customer_ref = 'C-001' "
            f"union all select 'observed', o.postcode, o.email from {SAT_OBSERVED} as o "
            f"join {HUB_CUSTOMER} as h on h.customer_hk = o.customer_hk "
            "where h.customer_ref = 'C-001' and o.effective_from = date '2026-03-15' "
            "order by 1",
        )
        assert held == [
            ("detail", "SW1A 1AA", "ada@example.com"),
            ("observed", "W1A 9ZZ", "ada.byron@example.com"),
        ], held

        surviving = _query(
            estate,
            f"select postcode, email from {GOLDEN_MODEL} where customer_ref = 'C-001'",
        )
        # postcode is most_recent: the observation, at the later instant.
        # email is first_non_null: the first declared satellite's value,
        # which is the one customer_detail still holds.
        assert surviving == [("W1A 9ZZ", "ada@example.com")], surviving


def test_the_point_in_time_relation_points_at_the_version_in_force() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _emitted_and_built(Path(tmp))
        rows = _query(
            estate,
            "select h.customer_ref, p.as_of, p.customer_detail_effective_from, "
            f"p.customer_observed_effective_from from {PIT_MODEL} as p "
            f"join {HUB_CUSTOMER} as h on h.customer_hk = p.customer_hk order by 1, 2",
        )
        assert [(row[0], str(row[1]), str(row[2]), str(row[3])) for row in rows] == [
            ("C-001", "2026-01-05", "2026-01-05", "2026-01-05"),
            ("C-002", "2026-02-10", "2026-02-10", "2026-02-10"),
        ], rows

        # A snapshot the vault does not store is not a snapshot this
        # relation carries: the spine is the instants the satellites
        # themselves hold.
        _append_seed_row(estate, "customer_records", UNCHANGED_PAYLOAD_ROW)
        _dbt_build(estate)
        later = _query(
            estate,
            "select h.customer_ref, p.as_of, p.customer_detail_effective_from, "
            f"p.customer_observed_effective_from from {PIT_MODEL} as p "
            f"join {HUB_CUSTOMER} as h on h.customer_hk = p.customer_hk "
            "where h.customer_ref = 'C-002' order by p.as_of",
        )
        assert [(str(row[1]), str(row[2]), str(row[3])) for row in later] == [
            ("2026-02-10", "2026-02-10", "2026-02-10"),
            ("2026-04-01", "2026-02-10", "2026-04-01"),
        ], later


def test_an_unchanged_payload_at_a_later_instant_separates_the_two_satellite_kinds() -> None:
    # The delivered payload of C-002 does not change; only the instant it
    # was delivered at does. A satellite of kind current records changes,
    # so it stores nothing; one of kind history records observations, so it
    # stores the snapshot.
    with tempfile.TemporaryDirectory() as tmp:
        estate = _emitted_and_built(Path(tmp))
        before = _counts(estate)
        _append_seed_row(estate, "customer_records", UNCHANGED_PAYLOAD_ROW)
        _dbt_build(estate)
        after = _counts(estate)
        assert after[SAT_DETAIL] == before[SAT_DETAIL], (before, after)
        assert after[SAT_OBSERVED] == before[SAT_OBSERVED] + 1, (before, after)


# --------------------------------------------------------------------- green: types, contracts


def test_the_contract_describes_the_basis_version_scoping_rule() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))
        _emit(estate)
        ctx = EstateContext.resolve(estate_root=estate)
        _plans, entries, _graph = ep.build_product_plans(ctx)
        landing = contract_mod.fixture_relation_schemas(entries)
        contracts = ec.build_product_contracts(ctx, landing_schemas=landing)

        satellites = [
            relation
            for relation in contracts[VAULT_PRODUCT].relations
            if relation.name.endswith(("sat_customer_detail", "sat_customer_observed"))
        ]
        assert len(satellites) == 2, satellites
        for relation in satellites:
            field = next(
                entry
                for entry in relation.fields
                if entry.name == vault.BASIS_VERSION_COLUMN
            )
            assert field.description == vault.BASIS_VERSION_DESCRIPTION, relation.name
            assert "highest basis version" in field.description

        files = ec.generate_products(ctx, landing_schemas=landing)
        document = next(
            yaml.safe_load(text)
            for path, text in files.items()
            if path.name.endswith("sat_customer_detail.odcs.yml")
        )
        published = {
            entry["name"]: entry for entry in document["schema"][0]["properties"]
        }
        assert published[vault.BASIS_VERSION_COLUMN]["description"] == (
            vault.BASIS_VERSION_DESCRIPTION
        ), published[vault.BASIS_VERSION_COLUMN]


def test_the_derived_relations_read_only_the_highest_basis_version() -> None:
    """A re-baseline leaves the entity's history under the old basis in
    place and stores its current state again under the new one. The two
    relations the shape derives from its stores must then read one of the
    two, and it is the newer: reading both would carry one row per basis at
    the same instant and survive a value the estate has superseded.
    """
    with tempfile.TemporaryDirectory() as tmp:
        estate = _emitted_and_built(Path(tmp))
        before_pit = _rows(estate, PIT_MODEL)

        def widen(document: dict) -> None:
            _satellite(document, SATELLITE_DETAIL).pop("hashdiff_exclude")

        _edit_declaration(estate, VAULT_DECLARATION, widen)
        _rebaseline(estate, SATELLITE_DETAIL, "--stage")
        _rebaseline(estate, SATELLITE_DETAIL, "--promote")
        _reemitted_and_rebuilt(estate)

        bases = _query(
            estate,
            f"select distinct hashdiff_basis_version from {SAT_DETAIL} order by 1",
        )
        assert bases == [(1,), (2,)], bases

        # The surviving record keeps one row per entity, and the
        # point-in-time relation keeps one row per entity and instant: both
        # scope to the basis version the store now carries.
        assert _rows(estate, GOLDEN_MODEL) == 2, _rows(estate, GOLDEN_MODEL)
        assert _rows(estate, PIT_MODEL) == before_pit, _rows(estate, PIT_MODEL)
        pointers = _query(
            estate,
            f"select count(*) from {PIT_MODEL} where customer_detail_effective_from is null",
        )
        assert pointers == [(0,)], pointers


def test_every_published_column_carries_its_declared_type_on_the_built_adapter() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _emitted_and_built(Path(tmp))
        declared = _declared_types(estate)
        generated = {
            vault.FINGERPRINT_COLUMN: vault.FINGERPRINT_TYPE,
            vault.BASIS_VERSION_COLUMN: vault.BASIS_VERSION_TYPE,
            vault.LOAD_DATETIME_COLUMN: vault.LOAD_DATETIME_TYPE,
            vault.RECORD_SOURCE_COLUMN: vault.RECORD_SOURCE_TYPE,
            "customer_hk": vault.IDENTITY_TYPE,
            "account_hk": vault.IDENTITY_TYPE,
            "customer_account_lhk": vault.IDENTITY_TYPE,
            "as_of": declared["effective_from"],
            "effective_from": declared["effective_from"],
            "customer_detail_effective_from": declared["effective_from"],
            "customer_observed_effective_from": declared["effective_from"],
        }
        for relation in VAULT_MODELS:
            for column, built in _built_types(estate, relation).items():
                expected = generated.get(column, declared.get(column))
                assert expected is not None, (relation, column)
                assert built == _physical_type(estate, expected), (relation, column, built)


def test_a_changed_declared_type_follows_into_the_built_type() -> None:
    # The relation asserted here is the surviving record, which is
    # recomputed whole on every build. A store is insert-only, so its
    # columns keep the type they were created under until the store itself
    # is rebuilt: a type restated in the declaration cannot retype history
    # already written, and the shape does not pretend otherwise.
    with tempfile.TemporaryDirectory() as tmp:
        estate = _emitted_and_built(Path(tmp))
        before = _built_types(estate, GOLDEN_MODEL)["loyalty_points"]
        assert before == _physical_type(estate, _declared_types(estate)["loyalty_points"]), before

        def restate(document: dict) -> None:
            for entry in document["steps"]:
                if entry["pattern"] != "schema_transform":
                    continue
                for mapping_entry in entry["mapping"]:
                    if mapping_entry.get("to") == "loyalty_points":
                        mapping_entry["type"] = "string"

        _edit_declaration(estate, FEED_DECLARATION, restate)
        # A stored column's declared type is part of the satellite's
        # recorded basis, so restating it is a re-baseline rather than an
        # edit that takes effect quietly.
        _fails(estate, VAULT_PRODUCT, SATELLITE_OBSERVED, "declared_type")
        _rebaseline(estate, SATELLITE_OBSERVED, "--stage")
        _rebaseline(estate, SATELLITE_OBSERVED, "--promote")
        _reemitted_and_rebuilt(estate)
        after = _built_types(estate, GOLDEN_MODEL)["loyalty_points"]
        assert after == _physical_type(estate, "string"), after
        assert after != before, after


def test_the_contract_lists_every_relation_the_shape_renders() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))
        _emit(estate)
        ctx = EstateContext.resolve(estate_root=estate)
        _plans, entries, _graph = ep.build_product_plans(ctx)
        landing = contract_mod.fixture_relation_schemas(entries)
        contracts = ec.build_product_contracts(ctx, landing_schemas=landing)

        published = [relation.name for relation in contracts[VAULT_PRODUCT].relations]
        assert published == [
            f"{VAULT_PRODUCT}__hub_customer",
            f"{VAULT_PRODUCT}__hub_account",
            f"{VAULT_PRODUCT}__link_customer_account",
            f"{VAULT_PRODUCT}__sat_customer_detail",
            f"{VAULT_PRODUCT}__sat_customer_observed",
            f"{VAULT_PRODUCT}__golden_customer",
            f"{VAULT_PRODUCT}__pit_customer",
        ], published

        files = ec.generate_products(ctx, landing_schemas=landing)
        odcs = sorted(
            path.name
            for path in files
            if path.name.endswith(".odcs.yml") and "customer_vault" in str(path)
        )
        assert len(odcs) == len(published), odcs


def test_a_consumer_declaring_one_relation_resolves_and_builds() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _emitted_and_built(Path(tmp))
        rows = _query(
            estate, f"select customer_ref, full_name, email from {DIRECTORY_MODEL} order by 1"
        )
        assert rows == [
            ("C-001", "Ada Byron", "ada@example.com"),
            ("C-002", "Grace Hopper", "grace@example.com"),
        ], rows
        # The consumer reads the surviving record's model, not the product's
        # own name: a shape rendering several relations has no single one.
        # The read sits in the consumer's own checked relation, which is
        # where its data_validation occurrence cuts the chain.
        assert f'ref("{GOLDEN_MODEL}")' in _read(
            estate, f"{MODELS_DIR}/{DIRECTORY_MODEL}__checked.sql"
        )


def test_a_consumer_naming_a_relation_the_shape_does_not_render_fails_closed() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))

        def mutate(document: dict) -> None:
            document["sources"][0]["relation"] = "golden_supplier"

        _edit_declaration(estate, "customer_directory.yml", mutate)
        _fails(estate, "golden_supplier", VAULT_PRODUCT)


def test_every_generated_test_is_written_in_the_nested_arguments_form() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))
        _emit(estate)
        document = yaml.safe_load(_read(estate, VAULT_SCHEMA))
        for model in document["models"]:
            for entry in model.get("data_tests", []):
                for name, body in entry.items():
                    assert set(body) <= {"name", "arguments", "config"}, (name, body)
                    assert "config" in body and "severity" in body["config"], name

        grain_tests = {
            model["name"]: test["dpf_unique_grain"]["arguments"]["columns"]
            for model in document["models"]
            for test in model.get("data_tests", [])
            if "dpf_unique_grain" in test
        }
        assert grain_tests[HUB_CUSTOMER] == ["customer_hk"]
        assert grain_tests[LINK_MODEL] == ["customer_account_lhk"]
        assert grain_tests[SAT_DETAIL] == [
            "customer_hk",
            "effective_from",
            "hashdiff_basis_version",
        ]
        assert grain_tests[PIT_MODEL] == ["customer_hk", "as_of"]


# --------------------------------------------------------------------------- red: generated tests


def test_an_identity_that_does_not_depend_on_the_business_key_turns_the_grain_test_red() -> None:
    # No seeded data can organically duplicate a hub key: the identity is
    # hashed from the business key and the source is distinct on it. Proving
    # the generated uniqueness test catches a duplicate therefore means
    # breaking the derivation itself, the way an identity computed from
    # nothing would.
    with tempfile.TemporaryDirectory() as tmp:
        estate = _emitted_and_built(Path(tmp))
        model = estate / f"{MODELS_DIR}/{HUB_CUSTOMER}.sql"
        text = model.read_text(encoding="utf-8")
        broken = text.replace(
            '{{ dpf_vault_hash(["customer_ref"]) }} as customer_hk',
            "'one-key-for-everything' as customer_hk",
        )
        assert broken != text, "the derivation this test breaks is not in the generated model"
        model.write_text(broken, encoding="utf-8")
        _dbt_fails(estate, f"dpf_unique_grain_{HUB_CUSTOMER}")


def test_a_snapshot_spine_that_stops_deduplicating_turns_the_grain_test_red() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _emitted_and_built(Path(tmp))
        model = estate / f"{MODELS_DIR}/{PIT_MODEL}.sql"
        text = model.read_text(encoding="utf-8")
        broken = text.replace("    union distinct\n", "    union all\n")
        assert broken != text, "the derivation this test breaks is not in the generated model"
        model.write_text(broken, encoding="utf-8")
        _dbt_fails(estate, f"dpf_unique_grain_{PIT_MODEL}")


def test_a_null_in_a_required_column_turns_the_compliance_test_red() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _emitted_and_built(Path(tmp))
        model = estate / f"{MODELS_DIR}/{GOLDEN_MODEL}.sql"
        text = model.read_text(encoding="utf-8")
        broken = text.replace(
            "cast(record_source as {{ dpf_type('string') }}) as record_source",
            "cast(null as {{ dpf_type('string') }}) as record_source",
        )
        assert broken != text, "the column this test nulls is not in the generated model"
        model.write_text(broken, encoding="utf-8")
        _dbt_fails(estate, f"dpf_contract_compliance_{GOLDEN_MODEL}")


# --------------------------------------------------------------------------- red: shape constraints


def test_a_declared_product_carrying_a_hubs_section_fails_closed() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))

        def mutate(document: dict) -> None:
            document["target"]["shape_config"] = {
                "hubs": [{"name": "customer", "business_key": ["customer_ref"]}]
            }

        _edit_declaration(estate, FEED_DECLARATION, mutate)
        _fails(estate, "hubs", "customer_feed", "wrong_shape_section")


def test_a_vault_whose_profile_mandates_curation_cannot_drop_it() -> None:
    # The integration profile mandates the occurrence, so dropping it from
    # a conformed vault never reaches the shape at all. The next test is
    # the one that reaches the shape's own constraint.
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))

        def mutate(document: dict) -> None:
            document["steps"] = [
                step for step in document["steps"] if step["pattern"] != "data_curation"
            ]

        _edit_declaration(estate, VAULT_DECLARATION, mutate)
        _fails(estate, "customer_vault", "data_curation", "integration")


def test_a_vault_whose_composition_curates_nothing_fails_closed() -> None:
    # Under a label whose profile forbids Data Curation the composition
    # cannot carry the occurrence at all, so the shape's own constraint is
    # what answers, naming the product, the shape and the composition.
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))

        def mutate(document: dict) -> None:
            document["product"]["layer"] = "computed"
            document["product"]["profile"] = "derivation"
            document["steps"] = [
                step
                for step in document["steps"]
                if step["pattern"] not in ("data_curation", "schema_transform")
            ]
            position = next(
                index
                for index, step in enumerate(document["steps"])
                if step["pattern"] == "data_enrichment"
            )
            document["steps"].insert(
                position,
                {
                    "pattern": "calculated_fields",
                    "fields": [
                        {
                            "name": "customer_reference_upper",
                            "type": "string",
                            "expression": "upper(customer_ref)",
                        }
                    ],
                },
            )

        _edit_declaration(estate, VAULT_DECLARATION, mutate)
        _fails(estate, VAULT_PRODUCT, "data_vault", "data_curation")


def test_a_satellite_naming_an_unknown_parent_fails_closed() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))

        def mutate(document: dict) -> None:
            _satellite(document, SATELLITE_DETAIL)["parent"] = "supplier"

        _edit_declaration(estate, VAULT_DECLARATION, mutate)
        _fails(estate, VAULT_PRODUCT, "data_vault", "supplier")


def test_a_link_naming_an_unknown_hub_fails_closed() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))

        def mutate(document: dict) -> None:
            _shape_config(document)["links"][0]["hubs"] = ["customer", "supplier"]

        _edit_declaration(estate, VAULT_DECLARATION, mutate)
        _fails(estate, VAULT_PRODUCT, "supplier")


def test_a_payload_column_the_composition_does_not_carry_fails_closed() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))

        def mutate(document: dict) -> None:
            _satellite(document, SATELLITE_DETAIL)["payload"].append("no_such_column")

        _edit_declaration(estate, VAULT_DECLARATION, mutate)
        _fails(estate, VAULT_PRODUCT, "data_vault", "no_such_column")


def test_a_change_column_that_is_not_an_instant_fails_closed() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))

        def mutate(document: dict) -> None:
            _satellite(document, SATELLITE_DETAIL)["change_column"] = "source_system"

        _edit_declaration(estate, VAULT_DECLARATION, mutate)
        _fails(estate, VAULT_PRODUCT, "source_system", "instant")


def test_excluding_every_payload_column_from_change_detection_fails_closed() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))

        def mutate(document: dict) -> None:
            satellite = _satellite(document, SATELLITE_DETAIL)
            satellite["hashdiff_exclude"] = list(satellite["payload"])

        _edit_declaration(estate, VAULT_DECLARATION, mutate)
        _fails(estate, VAULT_PRODUCT, "no column to fingerprint")


def test_a_surviving_attribute_no_declared_satellite_stores_fails_closed() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))

        def mutate(document: dict) -> None:
            _shape_config(document)["business_vault"][0]["attributes"].append(
                {"name": "source_system", "strategy": "most_recent"}
            )

        _edit_declaration(estate, VAULT_DECLARATION, mutate)
        _fails(estate, VAULT_PRODUCT, "source_system", "stored one")


def test_a_point_in_time_relation_naming_another_hub_s_satellite_fails_closed() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))

        def mutate(document: dict) -> None:
            config = _shape_config(document)
            config["satellites"].append(
                {
                    "name": "account_detail",
                    "kind": "current",
                    "parent": "account",
                    "payload": ["postcode"],
                    "change_column": "effective_from",
                }
            )
            config["point_in_time"][0]["satellites"].append("account_detail")

        _edit_declaration(estate, VAULT_DECLARATION, mutate)
        _fails(estate, VAULT_PRODUCT, "account_detail", "account_hk")


def test_a_payload_column_named_as_one_the_shape_generates_fails_closed() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))

        def mutate(document: dict) -> None:
            _satellite(document, SATELLITE_DETAIL)["payload"].append(
                vault.RECORD_SOURCE_COLUMN
            )

        _edit_declaration(estate, VAULT_DECLARATION, mutate)
        _fails(estate, VAULT_PRODUCT, vault.RECORD_SOURCE_COLUMN, "ambiguous")


# --------------------------------------------------------------------- across builds
#
# The three properties the shape's stores hold across builds: an unchanged build
# appends nothing, a changed attribute appends exactly one version, and a delivery
# behind the store's own watermark appends nothing. Each drives the real
# `ergasterion emit-products` command and a real DuckDB build of the vault_one
# fixture estate through the harness above.

def test_replay_suppression_appends_nothing_on_a_second_unchanged_build() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _emitted_and_built(Path(tmp))
        before = _counts(estate)
        stamps = _query(estate, f"select distinct load_datetime from {HUB_CUSTOMER}")
        _dbt_build(estate)
        assert _counts(estate) == before, (before, _counts(estate))
        assert (
            _query(estate, f"select distinct load_datetime from {HUB_CUSTOMER}") == stamps
        ), "an identity already stored was written again"


def test_a_changed_attribute_appends_exactly_one_version() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _emitted_and_built(Path(tmp))
        before = _counts(estate)
        _append_seed_row(estate, "customer_records", CHANGED_ATTRIBUTE_ROW)
        _dbt_build(estate)
        after = _counts(estate)

        assert after[SAT_DETAIL] == before[SAT_DETAIL] + 1, (before, after)
        assert after[HUB_CUSTOMER] == before[HUB_CUSTOMER], (before, after)
        assert after[LINK_MODEL] == before[LINK_MODEL], (before, after)
        versions = _query(
            estate,
            f"select s.full_name, s.effective_from from {SAT_DETAIL} as s "
            f"join {HUB_CUSTOMER} as h on h.customer_hk = s.customer_hk "
            "where h.customer_ref = 'C-001' order by s.effective_from",
        )
        assert [(row[0], str(row[1])) for row in versions] == [
            ("Ada Byron", "2026-01-05"),
            ("Ada B. Byron", "2026-03-15"),
        ], versions
        surviving = _query(
            estate, f"select full_name from {GOLDEN_MODEL} where customer_ref = 'C-001'"
        )
        assert surviving == [("Ada B. Byron",)], surviving


def test_a_delivery_behind_the_watermark_appends_nothing() -> None:
    # The store carries a version for C-001 at 2026-03-15. The source then
    # retracts that delivery, so the composition offers the earlier payload
    # again: a different fingerprint at an earlier instant. The store's own
    # watermark holds it out, and the stored history stays as it was.
    with tempfile.TemporaryDirectory() as tmp:
        estate = _emitted_and_built(Path(tmp))
        _append_seed_row(estate, "customer_records", CHANGED_ATTRIBUTE_ROW)
        _dbt_build(estate)
        watermark = _query(estate, f"select max(effective_from) from {SAT_DETAIL}")
        before = _counts(estate)

        _drop_last_seed_rows(estate, "customer_records", 1)
        _dbt_build(estate)
        assert _counts(estate) == before, (before, _counts(estate))
        assert (
            _query(estate, f"select max(effective_from) from {SAT_DETAIL}")
            == watermark
        )


def _register_compliance(estate: Path) -> dict:
    """The generated contract-compliance test of the register product, read
    off the schema document the route wrote."""

    document = yaml.safe_load(_read(estate, f"{MODELS_DIR}/{REGISTER_MODEL}.yml"))
    return next(
        entry["dpf_contract_compliance"]
        for model in document["models"]
        for entry in model.get("data_tests") or []
        if "dpf_contract_compliance" in entry
    )


def test_an_outer_merge_over_a_vault_relation_publishes_the_inherited_columns_as_optional() -> None:
    """A hub publishes its hash key, its business key, its load instant and
    its record source, and every one of them is required. An outer merge
    keeps rows the hub never saw, so the contract publishes the columns
    inherited from it as optional and the compliance check agrees with the
    contract instead of failing by construction."""

    with tempfile.TemporaryDirectory() as tmp:
        estate = _emitted_and_built(Path(tmp))

        hub = yaml.safe_load(_read(estate, VAULT_SCHEMA))
        hub_required = next(
            entry["dpf_contract_compliance"]["arguments"]["required"]
            for model in hub["models"]
            if model["name"] == HUB_CUSTOMER
            for entry in model.get("data_tests") or []
            if "dpf_contract_compliance" in entry
        )
        assert hub_required == [
            "customer_hk",
            "customer_ref",
            "load_datetime",
            "record_source",
        ], hub_required

        compliance = _register_compliance(estate)
        assert compliance["arguments"]["columns"] == [
            "customer_ref",
            "customer_hk",
            "load_datetime",
            "record_source",
            "account_ref",
        ], compliance
        assert compliance["arguments"]["required"] == ["customer_ref"], compliance

        # The unmatched row is real: an account was delivered for a customer
        # reference no delivery carries, so it reaches the register with
        # nothing from the hub.
        rows = _query(
            estate,
            f"select customer_ref, customer_hk, account_ref from {REGISTER_MODEL} "
            "order by customer_ref",
        )
        assert [row[0] for row in rows] == ["C-001", "C-002", "C-003"], rows
        assert rows[2][1] is None, rows[2]
        assert rows[2][2] == "A-300", rows[2]


def test_an_inner_merge_over_a_vault_relation_keeps_the_inherited_columns_required() -> None:
    """The same two sources one declaration apart. An inner merge keeps only
    the customer references both sides carry, so nothing it publishes can be
    empty and every column stays as required as the source it came from."""

    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))
        path = estate / "declarations" / "products" / REGISTER_DECLARATION
        path.write_text(
            path.read_text(encoding="utf-8").replace("join: outer", "join: inner"),
            encoding="utf-8",
        )
        # An inner merge drops the account whose customer the estate never
        # saw, and this product proves its coverage against both contracts,
        # so the delivery is narrowed to what both sides carry rather than
        # letting a coverage failure stand in for the join.
        _drop_last_seed_rows(estate, "account_records", 1)
        _emit(estate)
        _dbt_build(estate)

        compliance = _register_compliance(estate)
        assert compliance["arguments"]["required"] == [
            "customer_ref",
            "customer_hk",
            "load_datetime",
            "record_source",
        ], compliance
        rows = _query(
            estate, f"select customer_ref from {REGISTER_MODEL} order by customer_ref"
        )
        assert rows == [("C-001",), ("C-002",)], rows


TESTS = [
    test_an_outer_merge_over_a_vault_relation_publishes_the_inherited_columns_as_optional,
    test_an_inner_merge_over_a_vault_relation_keeps_the_inherited_columns_required,
    test_the_vault_hash_construction_matches_its_golden_vectors_on_duckdb,
    test_replay_suppression_appends_nothing_on_a_second_unchanged_build,
    test_a_changed_attribute_appends_exactly_one_version,
    test_a_delivery_behind_the_watermark_appends_nothing,
    test_the_command_emits_the_vault_and_reemission_is_byte_identical,
    test_the_vault_stores_are_insert_only_and_the_derived_relations_are_tables,
    test_the_declared_keys_of_every_store_reach_the_generated_configuration,
    test_the_hubs_and_the_link_carry_one_row_per_identity,
    test_the_satellites_store_one_version_per_entity_on_the_first_build,
    test_the_surviving_record_carries_the_value_each_declared_strategy_picks,
    test_the_two_survivorship_strategies_pick_different_satellites,
    test_the_point_in_time_relation_points_at_the_version_in_force,
    test_an_unchanged_payload_at_a_later_instant_separates_the_two_satellite_kinds,
    test_the_contract_describes_the_basis_version_scoping_rule,
    test_the_derived_relations_read_only_the_highest_basis_version,
    test_every_published_column_carries_its_declared_type_on_the_built_adapter,
    test_a_changed_declared_type_follows_into_the_built_type,
    test_the_contract_lists_every_relation_the_shape_renders,
    test_a_consumer_declaring_one_relation_resolves_and_builds,
    test_a_consumer_naming_a_relation_the_shape_does_not_render_fails_closed,
    test_every_generated_test_is_written_in_the_nested_arguments_form,
    test_an_identity_that_does_not_depend_on_the_business_key_turns_the_grain_test_red,
    test_a_snapshot_spine_that_stops_deduplicating_turns_the_grain_test_red,
    test_a_null_in_a_required_column_turns_the_compliance_test_red,
    test_a_declared_product_carrying_a_hubs_section_fails_closed,
    test_a_vault_whose_profile_mandates_curation_cannot_drop_it,
    test_a_vault_whose_composition_curates_nothing_fails_closed,
    test_a_satellite_naming_an_unknown_parent_fails_closed,
    test_a_link_naming_an_unknown_hub_fails_closed,
    test_a_payload_column_the_composition_does_not_carry_fails_closed,
    test_a_change_column_that_is_not_an_instant_fails_closed,
    test_excluding_every_payload_column_from_change_detection_fails_closed,
    test_a_surviving_attribute_no_declared_satellite_stores_fails_closed,
    test_a_point_in_time_relation_naming_another_hub_s_satellite_fails_closed,
    test_a_payload_column_named_as_one_the_shape_generates_fails_closed,
]


# --------------------------------------------------------------------------- runner

def main() -> int:
    failures = 0
    for test in TESTS:
        try:
            test()
        except Exception:
            failures += 1
            print(f"FAIL {test.__name__}")
            traceback.print_exc()
        else:
            print(f"PASS {test.__name__}")
    print(f"\n{len(TESTS) - failures}/{len(TESTS)} passed")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
