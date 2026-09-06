"""Self-tests for three of the patterns the dbt product route renders:
``data_validation`` with its quarantine and threshold, ``data_curation``
with resolution and survivorship, and ``data_aggregation`` at a declared
grain (architecture sections 4, 6, 9, 10, 12).

Same plain assert-and-report convention as the rest of this repo (no pytest
in the .venv): each ``test_*`` raises ``AssertionError`` on failure,
``main()`` runs them all and reports PASS/FAIL.

Everything runs against ``tests/fixtures/estates/entity_curation``, copied
into a scratch directory so a red case can mutate one file without touching
the fixture. Every red case drives the real ``ergasterion emit-products``
command, or a real dbt build of what that command wrote, and asserts what
the failure names.

Covers:
  - a full integration product routes every one of its profile's ten
    mandatory occurrences to exactly one owner, with none unowned;
  - the command emits both products, is byte-stable across two runs, and
    ``--check`` is clean on what it wrote;
  - the resolution relation unrolls the declared number of label-propagation
    rounds, keys each record on the declared columns, and marks what no key
    merged as pending;
  - the pending-key relation scores candidate pairs through the declared
    named rule and classifies each into the declared review band;
  - the aggregation renders at the declared grain with a uniqueness test on
    it, and its declared late-arrival policy is recorded in the runtime
    manifest;
  - the executed DuckDB build: a seeded duplicate pair resolving to one
    entity through deterministic keys (transitively, over two hops), a
    seeded near-duplicate pair scoring into the review band and appearing in
    the pending-key relation, conflicting attributes surviving per the
    declared strategy, a row quarantined under the declared threshold with
    the run continuing, a second build changing nothing, and a late-arriving
    row recomputing only its own period;
  - the deterministic strategy leaving every unmerged record visible in the
    pending-key relation;
  - the executed red cases: an error threshold below the quarantine rate
    aborting and naming the rule, and the contract compliance check failing
    on a relation whose schema disagrees with its contract, naming the
    column and leaving the product unpublished;
  - every declared rule kind renders both a violation column and its own
    generated test, and a declared value with no SQL literal fails closed;
  - the built relation carries the declared type of every aggregate, and
    restating a declared type changes the built one;
  - each generated test that is not one of dbt's own goes red on an
    injected violation: an incomplete column, a value outside the declared
    bounds, a value the declared pattern rejects, and a relation that has
    left its declared grain;
  - the fail-closed branches of the three renderings: a curation with no
    record key, no merge iterations, a non-string resolution key, a column
    with no survivorship strategy, an unknown strategy, a survivorship entry
    for a column that is not there, no precedence block, a probabilistic
    resolution with no scoring, scoring on a deterministic one, a scoring
    rule whose arity does not match the compared columns, a rule no
    catalogue carries, an inverted review band, a composition column
    clashing with a resolution column, a late-arrival policy this translator
    does not render, and one the publication cannot carry.

Usage:
    python tests/python/test_dbt_curation_patterns.py
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

from ergasterion import cli
from ergasterion import emit_products as ep
from ergasterion.dialect_lint import lint_artefacts
from ergasterion.estate import EstateContext
from ergasterion.framework.adapters import load_adapter_conventions
from ergasterion.framework.patterns import load_profile
from ergasterion.translators.dbt import DbtProductTranslator
from ergasterion.translators.dbt_patterns import parse_gate
from ergasterion.translators.dbt_patterns import sql as sql_mod

REPO_ROOT = Path(__file__).resolve().parents[2]
FIXTURE_ESTATE = REPO_ROOT / "tests" / "fixtures" / "estates" / "entity_curation"
ENGINE_MACROS = (
    "cross_db.sql",
    "entity_resolution_scoring.sql",
    "filter_log.sql",
    "product_tests.sql",
    "publish.sql",
    "quarantine.sql",
)

CUSTOMER_MODEL = "models/products/crm/crm__customer.sql"
CUSTOMER_SCHEMA = "models/products/crm/crm__customer.yml"
RESOLUTION_MODEL = "models/products/crm/crm__customer__resolution.sql"
PENDING_MODEL = "models/products/crm/crm__customer__pending_keys.sql"
CHECKED_PRE_MODEL = "models/products/crm/crm__customer__checked_pre.sql"
QUARANTINE_PRE_MODEL = "models/products/crm/crm__customer__quarantine_pre.sql"
ACTIVITY_MODEL = "models/products/crm/crm__customer_activity.sql"
ACTIVITY_SCHEMA = "models/products/crm/crm__customer_activity.yml"
ACTIVITY_MANIFEST = "manifests/products/crm/customer_activity.json"
CUSTOMER_MANIFEST = "manifests/products/crm/customer.json"

EXPECTED_ARTEFACTS = {
    CUSTOMER_MANIFEST,
    ACTIVITY_MANIFEST,
    CUSTOMER_MODEL,
    CUSTOMER_SCHEMA,
    RESOLUTION_MODEL,
    PENDING_MODEL,
    CHECKED_PRE_MODEL,
    QUARANTINE_PRE_MODEL,
    "models/products/crm/crm__customer__checked_post.sql",
    "models/products/crm/crm__customer__quarantine_post.sql",
    "models/products/crm/crm__customer__current.sql",
    "models/products/crm/crm__customer__sla.sql",
    ACTIVITY_MODEL,
    ACTIVITY_SCHEMA,
    "models/products/crm/crm__customer_activity__checked.sql",
    "models/products/crm/crm__customer_activity__quarantine.sql",
    "models/products/crm/crm__customer_activity__current.sql",
    "models/products/crm/crm__customer_activity__sla.sql",
}


# --------------------------------------------------------------------------- harness


def _scratch_estate(root: Path) -> Path:
    """The fixture estate copied into ``root``, with the engine's own macros
    beside the estate's. The macros are copied rather than duplicated in the
    fixture so the estate always builds against the macros the engine
    ships."""

    estate = root / "estate"
    shutil.copytree(FIXTURE_ESTATE, estate)
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


def _read(estate: Path, relative: str) -> str:
    return (estate / relative).read_text(encoding="utf-8")


def _tree(estate: Path) -> dict[str, bytes]:
    files: dict[str, bytes] = {}
    for root in ("models/products", "manifests/products"):
        for path in sorted((estate / root).rglob("*")):
            if path.is_file():
                files[path.relative_to(estate).as_posix()] = path.read_bytes()
    return files


def _edit_declaration(estate: Path, name: str, mutate) -> None:
    path = estate / "declarations" / "products" / name
    document = yaml.safe_load(path.read_text(encoding="utf-8"))
    mutate(document)
    path.write_text(yaml.safe_dump(document, sort_keys=False), encoding="utf-8")


def _edit_estate(estate: Path, mutate) -> None:
    path = estate / "estate.yml"
    document = yaml.safe_load(path.read_text(encoding="utf-8"))
    mutate(document["estate"])
    path.write_text(yaml.safe_dump(document, sort_keys=False), encoding="utf-8")


def _append_seed_row(estate: Path, name: str, row: str) -> None:
    path = estate / "seeds" / f"{name}.csv"
    path.write_text(path.read_text(encoding="utf-8") + row + "\n", encoding="utf-8")


def _curation_step(document: dict) -> dict:
    return next(step for step in document["steps"] if step["pattern"] == "data_curation")


def _aggregation_step(document: dict) -> dict:
    return next(step for step in document["steps"] if step["pattern"] == "data_aggregation")


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


def _query(estate: Path, statement: str):
    import duckdb

    connection = duckdb.connect(str(estate / "target" / "entity_curation.duckdb"), read_only=True)
    try:
        return connection.execute(statement).fetchall()
    finally:
        connection.close()


def _relation_exists(estate: Path, name: str) -> bool:
    rows = _query(
        estate,
        f"select count(*) from information_schema.tables where table_name = '{name}'",
    )
    return rows[0][0] > 0


# --------------------------------------------------------------------------- green: ownership


def test_every_mandatory_occurrence_of_the_integration_profile_has_exactly_one_owner() -> None:
    # Asserted through what the command did, not by resolving ownership a
    # second time here: the route already fails closed on an occurrence with
    # no table entry, an unsupplied translator or a missing capability, so a
    # command that emitted at all resolved every occurrence, and the
    # artefacts it wrote say which ones dbt took.
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))
        _emit(estate)

        profile = load_profile("integration")
        mandatory = {pattern.value for pattern in profile.mandatory}
        assert len(mandatory) == 10, sorted(mandatory)

        declaration = yaml.safe_load(_read(estate, "declarations/products/customer.yml"))
        composed = {step["pattern"] for step in declaration["steps"]} | {"checkpoint_retries"}
        assert mandatory <= composed, sorted(mandatory - composed)

        # The runtime manifest names one entry per occurrence dbt rendered,
        # by step index: no index appears twice, so no occurrence has two
        # owners on this side.
        manifest = json.loads(_read(estate, CUSTOMER_MANIFEST))
        owned = manifest["run_boundary"]["occurrences"]
        assert owned == [
            "steps[0]:batch_transfer",
            "steps[1]:data_validation",
            "steps[2]:schema_transform",
            "steps[3]:data_curation",
            "steps[4]:data_validation",
            "steps[9]:data_publish",
        ], owned
        assert len(owned) == len({entry.split(":", 1)[0] for entry in owned}), owned

        # The four the manifest does not name are the publication
        # translator's under this label, and the checkpoint wrapper and the
        # shape are dbt's -- so every mandatory occurrence has exactly one
        # owner and none is unowned.
        dbt_owned = {entry.split(":", 1)[1] for entry in owned} | {"checkpoint_retries"}
        table = yaml.safe_load(_read(estate, "estate.yml"))["estate"]["translators"]["conformed"]
        elsewhere = sorted(mandatory - dbt_owned)
        assert elsewhere == [
            "data_contracts",
            "lineage_capture",
            "metadata_capture",
            "schema_publish",
        ], elsewhere
        assert {table[pattern] for pattern in elsewhere} == {"publication"}, table
        assert table["checkpoint_retries"] == "dbt" and table["declared"] == "dbt", table


def test_a_mandatory_occurrence_with_no_table_entry_fails_the_command_closed() -> None:
    # The other half of "zero unowned": drop one mandatory occurrence's
    # entry and the command refuses to emit anything, naming the label, the
    # pattern and every declared adapter.
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))
        _edit_estate(
            estate, lambda block: block["translators"]["conformed"].pop("data_curation")
        )
        _fails(estate, "conformed", "data_curation", "duckdb", "bigquery")


# --------------------------------------------------------------------------- green: the command


def test_the_command_emits_both_products_and_re_emission_is_byte_identical() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))
        output = _emit(estate)
        first = _tree(estate)
        assert set(first) == EXPECTED_ARTEFACTS, sorted(set(first) ^ EXPECTED_ARTEFACTS)
        assert "emitted crm.customer: label=conformed profile=integration" in output, output
        assert "emitted crm.customer_activity: label=computed profile=derivation" in output, output

        second = _emit(estate)
        assert _tree(estate) == first, "re-emission is not byte-identical"
        assert f"generated 0 of {len(EXPECTED_ARTEFACTS)} file(s)" in second, second

        code, check_output = _run(estate, "--check")
        assert code == 0, check_output
        assert f"{len(EXPECTED_ARTEFACTS)} generated file(s); 0 problem(s)" in check_output


# --------------------------------------------------------------------------- green: rendering


def test_the_resolution_relation_unrolls_the_declared_iterations() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))
        _emit(estate)
        resolution = _read(estate, RESOLUTION_MODEL)
        # One edge select per declared key, each carrying its own rank and
        # a value the key's own name prefixes, so two different keys never
        # merge two records on one shared value.
        assert "'customer_ref:' || customer_ref as key_value" in resolution
        assert "'email:' || email as key_value" in resolution
        assert "1 as key_rank" in resolution and "2 as key_rank" in resolution
        # The declared three rounds, and no fourth.
        for round_index in (1, 2, 3):
            assert f"curation_values_{round_index} as (" in resolution
            assert f"curation_labels_{round_index} as (" in resolution
        assert "curation_labels_4" not in resolution
        assert "from curation_labels_3" in resolution
        # What no declared key merged stays visible, and says so.
        assert "coalesce(stats.component_row_count, 1) = 1 as resolution_pending" in resolution
        assert "else 'unresolved'" in resolution


def test_the_pending_key_relation_scores_candidate_pairs_through_the_named_rule() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))
        _emit(estate)
        pending = _read(estate, PENDING_MODEL)
        # The declared rule's macro, bound two arguments per compared
        # column, the left side then the right side.
        assert (
            '{{ customer_match_score_v1("left_side.full_name", "right_side.full_name", '
            '"left_side.postcode", "right_side.postcode") }} as match_score'
        ) in pending
        # Candidate pairs are blocked on the declared column and taken once.
        assert "on left_side.postcode = right_side.postcode" in pending
        assert (
            "and left_side.resolution_record_key < right_side.resolution_record_key" in pending
        )
        # The declared band edges, with an unscorable pair kept apart from a
        # rejected one.
        assert "when match_score is null then 'unscored'" in pending
        assert "when match_score >= 0.9 then 'accepted'" in pending
        assert "when match_score >= 0.6 then 'review'" in pending
        assert "else 'rejected'" in pending
        assert 'where resolution_pending' in pending


def test_the_survivorship_chain_renders_one_winner_per_declared_strategy() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))
        _emit(estate)
        chain = _read(estate, "models/products/crm/crm__customer__checked_post.sql")
        priority = (
            "case when source_system = 'crm' then 1 when source_system = 'billing' then 2 "
            "else 3 end"
        )
        # most_recent orders by the declared recency column first; both
        # strategies prefer a present value and break the remaining tie on
        # the record key, so a rebuild picks the same row.
        assert (
            "order by case when full_name is null then 1 else 0 end, effective_from desc, "
            f"{priority}, resolution_record_key" in chain
        )
        assert (
            "order by case when email is null then 1 else 0 end, "
            f"{priority}, resolution_record_key" in chain
        )
        assert "select distinct\n        resolution_entity_key" in chain


def test_the_aggregation_renders_at_the_declared_grain_with_its_uniqueness_test() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))
        _emit(estate)
        model = _read(estate, ACTIVITY_MODEL)
        # Each aggregate is cast to the neutral type it declares, through
        # the same adapter type dispatch every other cast goes through.
        assert "cast((count(*)) as {{ dpf_type('int') }}) as activity_count" in model
        assert "cast((sum(amount)) as {{ dpf_decimal_type(12, 2) }}) as total_amount" in model
        assert (
            "cast((sum(amount_abs)) as {{ dpf_decimal_type(12, 2) }}) as total_abs_amount"
            in model
        )
        assert "    group by\n        customer_ref,\n        activity_month" in model
        # The declared policy republishes a whole period, so the
        # publication is keyed by the grain itself.
        assert 'unique_key=["customer_ref", "activity_month"]' in model

        document = yaml.safe_load(_read(estate, ACTIVITY_SCHEMA))
        entry = next(
            model for model in document["models"] if model["name"] == "crm__customer_activity"
        )
        grain_test = next(
            body
            for test in entry["data_tests"]
            for kind, body in test.items()
            if kind == "dpf_unique_grain"
        )
        assert grain_test["arguments"] == {"columns": ["customer_ref", "activity_month"]}
        assert grain_test["config"]["severity"] == "error"


def test_the_runtime_manifest_records_the_grain_and_the_late_arrival_policy() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))
        _emit(estate)
        manifest = json.loads(_read(estate, ACTIVITY_MANIFEST))
        assert manifest["aggregation"] == [
            {
                "grain": ["customer_ref", "activity_month"],
                "late_arrival_policy": "recompute_period",
            }
        ]
        assert manifest["materialisation"]["unique_key"] == ["customer_ref", "activity_month"]
        curated = json.loads(_read(estate, CUSTOMER_MANIFEST))
        # A product that aggregates not at all carries an empty list, which
        # is a fact about its composition, not an absent field.
        assert curated["aggregation"] == []
        assert curated["relations"]["auxiliary"] == [
            "crm.customer__checked_post",
            "crm.customer__checked_pre",
            "crm.customer__current",
            "crm.customer__pending_keys",
            "crm.customer__quarantine_post",
            "crm.customer__quarantine_pre",
            "crm.customer__resolution",
            "crm.customer__sla",
        ]


# --------------------------------------------------------------------------- green: gates


def test_every_generated_model_parses_and_passes_the_deployment_dialect_gate() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))
        ctx = EstateContext.resolve(estate_root=estate)
        plans, _entries, _graph = ep.build_product_plans(ctx)
        artefacts = DbtProductTranslator(products=plans).translate().artefacts
        parse_gate.assert_parses(artefacts, adapters=("duckdb", "bigquery"))
        assert lint_artefacts(artefacts, "bigquery") == []


# --------------------------------------------------------------------------- green: executed build


def test_the_estate_builds_on_duckdb_with_the_known_answers() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))
        _emit(estate)
        _dbt_build(estate)

        # r-06 carries no customer reference. The declared quarantine policy
        # diverts it, names the rule it broke, and the run continues because
        # one row in eleven is under the declared 0.25 ceiling.
        assert _query(estate, "select rule_name, rec_id from crm__customer__quarantine_pre") == [
            ("cust_ref_not_null", "r-06")
        ]
        assert _query(estate, "select count(*) from crm__customer__checked_pre") == [(11,)]

        # The seeded duplicate pair resolves to one entity through the
        # declared keys, transitively: r-01 and r-02 share a customer
        # reference, r-11 shares only an email address with r-02.
        resolution = dict(
            _query(
                estate,
                "select resolution_record_key, resolution_entity_key "
                "from crm__customer__resolution",
            )
        )
        assert resolution["r-01"] == resolution["r-02"] == resolution["r-11"], resolution
        assert len({resolution[key] for key in ("r-03", "r-04", "r-05")}) == 3, resolution
        assert _query(
            estate,
            "select resolution_row_count, resolution_tier, resolution_pending "
            "from crm__customer__resolution where resolution_record_key = 'r-02'",
        ) == [(3, "deterministic", False)]

        # Conflicting attributes survive per the declared rule: the most
        # recent record wins the name, and the first non-null value in
        # declared source order wins the email -- which means skipping the
        # higher-priority source that carries none.
        assert _query(
            estate,
            "select record_id, source_system, customer_ref, full_name, postcode, email, "
            "effective_from from crm__customer where customer_ref = 'C-100'",
        ) == [
            (
                "r-02",
                "billing",
                "C-100",
                "Alice B Brown",
                "SW1A 1AA",
                "alice.b@example.com",
                __import__("datetime").date(2026, 3, 11),
            )
        ]
        # Eleven records, one quarantined, three merged into one entity.
        assert _query(estate, "select count(*) from crm__customer") == [(8,)]

        # The seeded near-duplicate pair scores into the declared review
        # band and appears in the pending-key evidence relation; a pair with
        # nothing comparable is unscored rather than rejected.
        pending = {
            (row[0], row[1]): (row[2], row[3])
            for row in _query(
                estate,
                "select record_key_a, record_key_b, review_band, match_score "
                "from crm__customer__pending_keys",
            )
        }
        assert pending[("r-03", "r-04")][0] == "review", pending
        assert 0.6 <= float(pending[("r-03", "r-04")][1]) < 0.9, pending
        assert pending[("r-09", "r-10")][0] == "accepted", pending
        assert pending[("r-03", "r-07")][0] == "rejected", pending
        assert pending[("r-03", "r-08")] == ("unscored", None), pending
        # Nothing that a declared key merged is offered for review.
        assert not [pair for pair in pending if "r-01" in pair or "r-11" in pair], pending

        assert _query(
            estate,
            "select customer_ref, activity_count, total_amount, total_abs_amount "
            "from crm__customer_activity where activity_month = date '2026-01-01' "
            "order by customer_ref",
        ) == [("C-100", 2, 100.00, 140.00), ("C-200", 1, 75.50, 75.50)]


def test_a_second_build_over_unchanged_input_changes_nothing() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))
        _emit(estate)
        _dbt_build(estate)
        before = {
            name: _query(estate, f"select * from {name} order by 1, 2")
            for name in (
                "crm__customer",
                "crm__customer__pending_keys",
                "crm__customer__quarantine_pre",
                "crm__customer_activity",
            )
        }
        _dbt_build(estate)
        for name, rows in before.items():
            assert _query(estate, f"select * from {name} order by 1, 2") == rows, name


def test_a_late_arriving_row_recomputes_only_its_own_period() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))
        _emit(estate)
        _dbt_build(estate)
        untouched = _query(
            estate,
            "select activity_count, total_amount, total_abs_amount from crm__customer_activity "
            "where customer_ref = 'C-100' and activity_month = date '2026-02-01'",
        )
        assert untouched == [(1, 50.00, 50.00)], untouched

        seed = estate / "seeds" / "activity_events.csv"
        seed.write_text(
            seed.read_text(encoding="utf-8") + "a-06,C-100,2026-01-01,30.00\n", encoding="utf-8"
        )
        _dbt_build(estate)

        # The period the late row belongs to is recomputed from every row in
        # it -- not incremented, not duplicated -- and republished under the
        # declared grain key.
        assert _query(
            estate,
            "select activity_count, total_amount, total_abs_amount from crm__customer_activity "
            "where customer_ref = 'C-100' and activity_month = date '2026-01-01'",
        ) == [(3, 130.00, 170.00)]
        assert _query(
            estate,
            "select activity_count, total_amount, total_abs_amount from crm__customer_activity "
            "where customer_ref = 'C-100' and activity_month = date '2026-02-01'",
        ) == untouched
        assert _query(estate, "select count(*) from crm__customer_activity") == [(4,)]


def test_a_deterministic_resolution_leaves_every_unmerged_record_visible() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))

        def deterministic(document: dict) -> None:
            resolution = _curation_step(document)["resolution"]
            resolution["strategy"] = "deterministic"
            resolution.pop("scoring")

        _edit_declaration(estate, "customer.yml", deterministic)
        _emit(estate)
        pending = _read(estate, PENDING_MODEL)
        assert "candidate_pairs" not in pending
        assert "'unresolved' as review_band" in pending
        _dbt_build(estate)
        rows = _query(
            estate,
            "select record_key_a, record_key_b, match_score, review_band "
            "from crm__customer__pending_keys order by record_key_a",
        )
        assert [row[0] for row in rows] == ["r-03", "r-04", "r-05", "r-07", "r-08", "r-09", "r-10"]
        assert all(row[1] is None and row[2] is None and row[3] == "unresolved" for row in rows)


# --------------------------------------------------------------------------- red: executed


def test_an_error_threshold_below_the_quarantine_rate_aborts_naming_the_rule() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))
        # One row in eleven is quarantined, so a ceiling of 0.05 is
        # breached where the declared 0.25 is not.
        _edit_declaration(
            estate,
            "customer.yml",
            lambda document: document["steps"][1].update(error_threshold=0.05),
        )
        _emit(estate)
        result = _dbt(estate)
        output = result.stdout + result.stderr
        assert result.returncode != 0, output
        assert "dpf_checked_pre_threshold_cust_ref_not_null" in output, output
        # The product never publishes: its own model is downstream of the
        # relation whose threshold test failed.
        assert not _relation_exists(estate, "crm__customer"), output


def test_the_contract_compliance_check_fails_when_the_emitted_schema_disagrees() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))
        _emit(estate)
        # A column in the middle of the relation, carrying no rule and no
        # validation test of its own: nothing but the compliance check
        # covers it, and the check must cover the whole document.
        model = estate / CUSTOMER_MODEL
        text = model.read_text(encoding="utf-8")
        assert "\n    postcode,\n" in text
        model.write_text(text.replace("\n    postcode,\n", "\n    postcode as post_code,\n"), encoding="utf-8")

        result = _dbt(estate)
        output = result.stdout + result.stderr
        assert result.returncode != 0, output
        assert "dpf_contract_compliance_crm__customer" in output, output
        # The stored failure names the disagreement, not only its count.
        failures = _query(
            estate,
            "select disagreement, detail from "
            "main_dbt_test__audit.dpf_contract_compliance_crm__customer order by disagreement",
        )
        assert failures == [("missing_column", "postcode"), ("unexpected_column", "post_code")]
        # Nothing published: the pointer and the record read the product's
        # relation, so they never ran.
        assert not _relation_exists(estate, "crm__customer__current")
        assert not _relation_exists(estate, "crm__customer__sla")


# --------------------------------------------------------------------------- red: curation


def test_a_curation_occurrence_with_no_record_key_fails_closed() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))
        _edit_declaration(
            estate,
            "customer.yml",
            lambda document: _curation_step(document)["resolution"].pop("record_key"),
        )
        _fails(estate, "undeclared_record_key", "data_curation")


def test_a_curation_occurrence_with_no_merge_iterations_fails_closed() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))
        _edit_declaration(
            estate,
            "customer.yml",
            lambda document: _curation_step(document)["resolution"].pop("merge_iterations"),
        )
        _fails(estate, "undeclared_merge_iterations", "data_curation")


def test_a_resolution_key_that_is_not_a_string_fails_closed() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))
        _edit_declaration(
            estate,
            "customer.yml",
            lambda document: _curation_step(document)["resolution"].update(
                keys=["customer_ref", "effective_from"]
            ),
        )
        _fails(estate, "unrenderable_curation_column", "effective_from", "resolution key")


def test_a_column_with_no_survivorship_strategy_fails_closed() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))
        _edit_declaration(
            estate,
            "customer.yml",
            lambda document: _curation_step(document)["survivorship"].pop("postcode"),
        )
        _fails(estate, "undeclared_survivorship", "postcode")


def test_an_unknown_survivorship_strategy_fails_closed() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))
        _edit_declaration(
            estate,
            "customer.yml",
            lambda document: _curation_step(document)["survivorship"].update(email="longest"),
        )
        _fails(estate, "unknown_survivorship_strategy", "longest", "email")


def test_a_survivorship_entry_for_a_column_that_is_not_there_fails_closed() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))
        _edit_declaration(
            estate,
            "customer.yml",
            lambda document: _curation_step(document)["survivorship"].update(
                telephone="most_recent"
            ),
        )
        _fails(estate, "unresolved_survivorship_column", "telephone")


def test_a_curation_occurrence_with_no_precedence_fails_closed() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))
        _edit_declaration(
            estate, "customer.yml", lambda document: _curation_step(document).pop("precedence")
        )
        _fails(estate, "undeclared_precedence", "data_curation")


def test_a_probabilistic_resolution_with_no_scoring_fails_closed() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))
        _edit_declaration(
            estate,
            "customer.yml",
            lambda document: _curation_step(document)["resolution"].pop("scoring"),
        )
        _fails(estate, "probabilistic_strategy_without_scoring", "data_curation")


def test_scoring_on_a_deterministic_resolution_fails_closed() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))
        _edit_declaration(
            estate,
            "customer.yml",
            lambda document: _curation_step(document)["resolution"].update(
                strategy="deterministic"
            ),
        )
        _fails(estate, "scoring_without_probabilistic_strategy", "data_curation")


def test_a_scoring_rule_whose_arity_does_not_match_the_compared_columns_fails_closed() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))
        _edit_declaration(
            estate,
            "customer.yml",
            lambda document: _curation_step(document)["resolution"]["scoring"].update(
                compare=["full_name"]
            ),
        )
        _fails(estate, "scoring_rule_arity", "customer_match_score_v1")


def test_a_scoring_rule_no_catalogue_carries_fails_closed() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))
        _edit_declaration(
            estate,
            "customer.yml",
            lambda document: _curation_step(document)["resolution"]["scoring"].update(
                rule="customer_match_score_v9"
            ),
        )
        _fails(estate, "customer_match_score_v9")


def test_an_inverted_review_band_fails_closed() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))
        _edit_declaration(
            estate,
            "customer.yml",
            lambda document: _curation_step(document)["resolution"]["scoring"].update(
                review_band={"lower": 0.9, "upper": 0.6}
            ),
        )
        _fails(estate, "inverted_review_band")


def test_a_composition_column_clashing_with_a_resolution_column_fails_closed() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))

        def clash(document: dict) -> None:
            transform = next(
                step for step in document["steps"] if step["pattern"] == "schema_transform"
            )
            transform["mapping"][0]["to"] = "resolution_record_key"
            curation = _curation_step(document)
            curation["resolution"]["record_key"] = "resolution_record_key"
            curation["survivorship"]["resolution_record_key"] = curation["survivorship"].pop(
                "record_id"
            )

        _edit_declaration(estate, "customer.yml", clash)
        _fails(estate, "reserved_resolution_column", "resolution_record_key")


# --------------------------------------------------------------------------- red: aggregation


def test_a_late_arrival_policy_this_translator_does_not_render_fails_closed() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))
        _edit_declaration(
            estate,
            "customer_activity.yml",
            lambda document: _aggregation_step(document).update(late_arrival_policy="ignore"),
        )
        _fails(estate, "unrenderable_late_arrival_policy", "ignore", "recompute_period")


def test_a_late_arrival_policy_the_publication_cannot_carry_fails_closed() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))

        def publish_atomically(document: dict) -> None:
            step = next(item for item in document["steps"] if item["pattern"] == "data_publish")
            step["publication_mode"] = "atomic"
            step.pop("unique_key")

        _edit_declaration(estate, "customer_activity.yml", publish_atomically)
        _fails(
            estate,
            "late_arrival_policy_needs_the_grain_key",
            "recompute_period",
            "customer_ref",
        )


def test_every_declared_rule_kind_renders_a_violation_and_a_generated_test() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))
        _emit(estate)
        checked = _read(estate, "models/products/crm/crm__customer__checked_post.sql")
        # Each rule kind renders the boolean that is true for a row breaking
        # it, and a null never passes as a pass.
        assert (
            "coalesce(customer_ref is not null and "
            "count(*) over (partition by customer_ref) > 1, true) as violation_00" in checked
        )
        assert (
            "coalesce(source_system is not null and source_system not in ('crm', 'billing'), "
            "true) as violation_01" in checked
        )
        assert (
            """coalesce(email is not null and not {{ dpf_regexp_contains("email", """
            """"'^[^@]+@[^@]+$'") }}, true) as violation_02""" in checked
        )
        activity = _read(estate, "models/products/crm/crm__customer_activity__checked.sql")
        assert (
            "coalesce(amount is not null and (amount < -1000 or amount > 100000), true) "
            "as violation_01" in activity
        )

        document = yaml.safe_load(_read(estate, CUSTOMER_SCHEMA))
        entry = next(
            model
            for model in document["models"]
            if model["name"] == "crm__customer__checked_post"
        )
        tests = {}
        for column in entry["columns"]:
            for test in column["data_tests"]:
                (kind, body), = test.items()
                tests[body["name"]] = (kind, body.get("arguments"))
        assert tests["dpf_checked_post_customer_ref_unique"] == ("unique", None)
        assert tests["dpf_checked_post_source_system_allowed_values"] == (
            "accepted_values",
            {"values": ["crm", "billing"]},
        )
        assert tests["dpf_checked_post_email_regex"] == (
            "dpf_matches_regex",
            {"rule": "email_regex", "pattern": "'^[^@]+@[^@]+$'"},
        )

        activity_document = yaml.safe_load(_read(estate, ACTIVITY_SCHEMA))
        activity_entry = next(
            model
            for model in activity_document["models"]
            if model["name"] == "crm__customer_activity__checked"
        )
        amount = next(
            column for column in activity_entry["columns"] if column["name"] == "amount"
        )
        (kind, body), = amount["data_tests"][0].items()
        assert kind == "dpf_range"
        assert body["arguments"] == {"rule": "amount_range", "min_value": "-1000", "max_value": "100000"}


def test_a_declared_value_with_no_sql_literal_fails_closed() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))

        def nested_value(document: dict) -> None:
            post = [
                step
                for step in document["steps"]
                if step["pattern"] == "data_validation" and step.get("stage") == "post"
            ][0]
            post["rules"][1]["allowed_values"] = [["crm", "billing"]]

        _edit_declaration(estate, "customer.yml", nested_value)
        _fails(estate, "unrenderable_literal", "steps[4]:data_validation")


# --------------------------------------------------------------------------- green: declared types


def _duckdb_physical_type(estate: Path, declared) -> str:
    """The physical type DuckDB spells the declared neutral type as, taken
    from the adapter conventions the engine itself casts through rather
    than from a second table written here."""

    if isinstance(declared, dict):
        physical = f"numeric({declared['precision']}, {declared['scale']})"
    else:
        token = sql_mod.SCALAR_TYPE_TOKENS[declared]
        physical = load_adapter_conventions("duckdb").type_mapping[token]
    return _query(estate, f"select typeof(cast(null as {physical}))")[0][0]


def _built_types(estate: Path, relation: str) -> dict:
    return dict(
        _query(
            estate,
            "select column_name, data_type from information_schema.columns "
            f"where table_name = '{relation}'",
        )
    )


def test_the_built_relation_carries_the_declared_type_of_every_aggregate() -> None:
    # An aggregate replaces the field set it groups, so nothing else in the
    # composition carries its type, and the width an adapter returns for a
    # count or a sum is the adapter's own. The published relation must hold
    # the type the contract states, or the contract states a type nothing
    # holds and the product is published in violation of it.
    decimal_12_2 = {"name": "decimal", "precision": 12, "scale": 2}
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))
        _emit(estate)
        _dbt_build(estate)
        built = _built_types(estate, "crm__customer_activity")
        assert built["activity_count"] == _duckdb_physical_type(estate, "integer"), built
        assert built["total_amount"] == _duckdb_physical_type(estate, decimal_12_2), built
        assert built["total_abs_amount"] == _duckdb_physical_type(estate, decimal_12_2), built
        assert built["customer_ref"] == _duckdb_physical_type(estate, "string"), built
        assert built["activity_month"] == _duckdb_physical_type(estate, "date"), built


def test_changing_a_declared_aggregate_type_changes_the_built_type() -> None:
    restated = {"name": "decimal", "precision": 18, "scale": 4}
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))

        def restate_the_type(document: dict) -> None:
            aggregate = next(
                entry
                for entry in _aggregation_step(document)["aggregates"]
                if entry["name"] == "total_amount"
            )
            aggregate["type"] = restated

        _edit_declaration(estate, "customer_activity.yml", restate_the_type)
        _emit(estate)
        _dbt_build(estate)
        built = _built_types(estate, "crm__customer_activity")
        assert built["total_amount"] == _duckdb_physical_type(estate, restated), built


# --------------------------------------------------------------------------- red: the generated tests


def _build_fails_naming(estate: Path, test_name: str) -> None:
    result = _dbt(estate)
    output = result.stdout + result.stderr
    assert result.returncode != 0, output
    assert test_name in output, output


def test_a_completeness_rule_goes_red_on_an_incomplete_column() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))

        def demand_full_completeness(document: dict) -> None:
            occurrence = document["steps"][1]
            occurrence["on_failure"] = "abort"
            occurrence.pop("error_threshold")
            rule = occurrence["rules"][0]
            rule.pop("not_null")
            rule["completeness"] = 1.0

        # r-06 carries no customer reference, so a share of 1.0 is not met.
        _edit_declaration(estate, "customer.yml", demand_full_completeness)
        _emit(estate)
        _build_fails_naming(estate, "dpf_checked_pre_cust_ref_completeness")


def test_a_range_rule_goes_red_on_a_row_outside_the_declared_bounds() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))
        _append_seed_row(estate, "activity_events", "a-99,C-100,2026-01-01,999999.00")
        _emit(estate)
        _build_fails_naming(estate, "dpf_checked_amount_range")


def test_a_regex_rule_goes_red_on_a_value_the_pattern_rejects() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))
        # A record of its own entity, so the value it contributes is the one
        # survivorship publishes and the post-stage rule reads.
        _append_seed_row(
            estate,
            "customer_records",
            "r-99,crm,C-999,Nadia Rossi,YO1 7HH,not-an-email,2026-03-20",
        )
        _emit(estate)
        _build_fails_naming(estate, "dpf_checked_post_email_regex")


def test_the_grain_uniqueness_test_goes_red_when_the_relation_leaves_the_grain() -> None:
    # The rendering groups by the declared grain, so no seeded row can put
    # two rows on one grain value: the injected violation is in the emitted
    # model itself, which is what the test exists to catch.
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))
        _emit(estate)
        model = estate / ACTIVITY_MODEL
        text = model.read_text(encoding="utf-8")
        grouped = "    group by\n        customer_ref,\n        activity_month"
        assert grouped in text
        model.write_text(text.replace(grouped, grouped + ",\n        activity_id"), encoding="utf-8")
        _build_fails_naming(estate, "dpf_unique_grain_crm__customer_activity")


def main() -> int:
    tests = [obj for name, obj in sorted(globals().items()) if name.startswith("test_") and callable(obj)]
    failures = 0
    for test in tests:
        try:
            test()
        except Exception:  # noqa: BLE001 -- report every failure, then continue
            traceback.print_exc()
            failures += 1
            print(f"FAIL {test.__name__}")
        else:
            print(f"PASS {test.__name__}")
    print(f"\n{len(tests) - failures}/{len(tests)} passed")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
