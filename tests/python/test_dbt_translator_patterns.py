"""Self-tests for the dbt product route (architecture sections 4, 6, 9, 10,
12): ``ergasterion/translators/dbt_patterns/``, the
``DbtProductTranslator`` in ``ergasterion/translators/dbt.py``, and the
``ergasterion emit-products`` command in ``ergasterion/emit_products.py``.

The curation, quarantine-threshold and aggregation renderings have their
own estate and their own suite (``test_dbt_curation_patterns.py``); this
one covers the chain every product shares.

Same plain assert-and-report convention as the rest of this repo (no pytest
in the .venv): each ``test_*`` raises ``AssertionError`` on failure,
``main()`` runs them all and reports PASS/FAIL.

Everything runs against ``tests/fixtures/estates/patterns_seven``, copied
into a scratch directory so a red case can mutate one file without touching
the fixture. Every red case drives the real command and asserts what its
message names.

Covers:
  - the command emits every product, is byte-stable across two runs, and
    ``--check`` is clean on what it wrote;
  - ``--check`` reports a hand edit made in the generated schema YAML and in
    the generated runtime manifest, not only in the model SQL;
  - every occurrence, the checkpoint wrapper and the shape route to exactly
    one owner, the set dbt owns is exactly the ten patterns plus the
    ``declared`` shape, and one estate gives the same pattern two different
    owners under two labels;
  - the patterns render: the upstream read, the rename/cast/drop, the
    verbatim inline expression, the named-rule dispatch call with its
    declared version in the emitted metadata, the as-of enrichment join, the
    filter with its filter-log relation, the validation with its checked and
    quarantine relations, the incremental publication with its current
    pointer and SLA record, and the run boundary and retry policy in the
    runtime manifest;
  - each declared on-failure policy renders its own severity: quarantine
    warns per rule and aborts on the declared threshold, abort stops on the
    rule itself, warn records and passes through;
  - every generated test that takes arguments nests them under
    ``arguments``, and the contract compliance check names every published
    column and every required one;
  - the built relation carries the declared type of every calculated field,
    and restating a declared type changes the built one;
  - a product declared as a whole select body renders as its relation;
  - every generated model resolves and parses for both declared adapters,
    with the adapter's own physical types, and passes the deployment
    adapter's dialect deny-list;
  - the estate builds on DuckDB against fixture-backed sources with the
    known answers the seeds imply, and a second build changes nothing;
  - the fail-closed branches: a pattern dbt does not own, a label with no
    table entry, publication or checkpointing owned elsewhere, a select body
    that drops a published column, names its own relation or sits beside an
    occurrence that owes evidence of its own, an unbound named-rule input, a
    rule resolving to two macros, an unknown no-match policy, a cast to a
    structured type, more than one source, a fixture binding that does not
    pair with its kind or that shadows a declared product, an incremental
    publication with no key, a publication with no declared mode, a
    validation occurrence with no on-failure policy, a quarantine policy
    with no error threshold, a validation rule declaring no check or naming
    a column the composition does not carry, a product claiming a private
    relation's name, a declared name that is not a plain SQL identifier, a
    manifest that would state a policy nobody declared, an adapter declaring
    no quote character, a template construct the parse gate cannot resolve,
    and a deployment-adapter dialect offence;
  - no Snowflake dispatch branch survives in the macros the product route
    calls.

Usage:
    python tests/python/test_dbt_translator_patterns.py
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
from datetime import date
from decimal import Decimal
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
from ergasterion.framework.adapters import ADAPTER_NAMES, load_adapter_conventions
from ergasterion.framework.contract import (
    ConformedColumn,
    OpeningComposition,
    SourceComposition,
)
from ergasterion.translators.dbt import DbtProductTranslator
from ergasterion.translators.dbt_patterns import PRODUCT_PATTERNS, PRODUCT_SHAPES, parse_gate
from ergasterion.translators.dbt_patterns import sql as sql_mod
from ergasterion.translators.dbt_patterns import validation as validation_mod
from ergasterion.translators.dbt_patterns import steps as steps_mod

REPO_ROOT = Path(__file__).resolve().parents[2]
FIXTURE_ESTATE = REPO_ROOT / "tests" / "fixtures" / "estates" / "patterns_seven"
ENGINE_MACROS = (
    "cross_db.sql",
    "filter_log.sql",
    "publish.sql",
    "quarantine.sql",
    "product_tests.sql",
)

CONSOLIDATING_ESTATE = REPO_ROOT / "tests" / "fixtures" / "estates" / "consolidating_two"

PRODUCT_MODEL = "models/products/retail/retail__order.sql"
PRODUCT_SCHEMA = "models/products/retail/retail__order.yml"
PREFILTER_MODEL = "models/products/retail/retail__order__prefilter.sql"
CHECKED_PRE_MODEL = "models/products/retail/retail__order__checked_pre.sql"
CHECKED_POST_MODEL = "models/products/retail/retail__order__checked_post.sql"
QUARANTINE_PRE_MODEL = "models/products/retail/retail__order__quarantine_pre.sql"
QUARANTINE_POST_MODEL = "models/products/retail/retail__order__quarantine_post.sql"
FILTER_LOG_MODEL = "models/products/retail/retail__order__filter_log.sql"
SLA_MODEL = "models/products/retail/retail__order__sla.sql"
POINTER_MODEL = "models/products/retail/retail__order__current.sql"
DIGEST_MODEL = "models/products/retail/retail__order_digest.sql"
ORDER_MANIFEST = "manifests/products/retail/order.json"

EXPECTED_ARTEFACTS = {
    ORDER_MANIFEST,
    "manifests/products/retail/customer_segment.json",
    "manifests/products/retail/order_digest.json",
    PRODUCT_MODEL,
    PRODUCT_SCHEMA,
    PREFILTER_MODEL,
    CHECKED_PRE_MODEL,
    CHECKED_POST_MODEL,
    QUARANTINE_PRE_MODEL,
    QUARANTINE_POST_MODEL,
    FILTER_LOG_MODEL,
    SLA_MODEL,
    POINTER_MODEL,
    DIGEST_MODEL,
    "models/products/retail/retail__order_digest.yml",
    "models/products/retail/retail__order_digest__current.sql",
    "models/products/retail/retail__order_digest__sla.sql",
    "models/products/retail/retail__customer_segment.sql",
    "models/products/retail/retail__customer_segment.yml",
    "models/products/retail/retail__customer_segment__checked.sql",
    "models/products/retail/retail__customer_segment__quarantine.sql",
    "models/products/retail/retail__customer_segment__current.sql",
    "models/products/retail/retail__customer_segment__sla.sql",
}


# --------------------------------------------------------------------------- harness


def _scratch_estate(root: Path) -> Path:
    """The fixture estate copied into ``root``, with the engine's own macros
    beside the estate's. The macros are copied rather than duplicated in the
    fixture so the estate always builds against the macros the engine ships."""

    estate = root / "estate"
    shutil.copytree(FIXTURE_ESTATE, estate)
    for name in ENGINE_MACROS:
        shutil.copy(REPO_ROOT / "macros" / name, estate / "macros" / name)
    return estate


def _scratch_consolidating_estate(root: Path) -> Path:
    """The consolidating estate copied into ``root``, with the engine's own
    macros beside it. It declares no macros of its own, so the directory is
    made here rather than committed empty."""

    estate = root / "estate"
    shutil.copytree(CONSOLIDATING_ESTATE, estate)
    macros = estate / "macros"
    macros.mkdir(exist_ok=True)
    for name in ENGINE_MACROS:
        shutil.copy(REPO_ROOT / "macros" / name, macros / name)
    return estate


def _run(estate: Path, *arguments: str) -> tuple[int, str]:
    """Run the real console command and return its exit code with everything
    it printed."""

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
        base = estate / root
        for path in sorted(base.rglob("*")):
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


# --------------------------------------------------------------------------- green: the command


def test_the_command_emits_every_product_and_re_emission_is_byte_identical() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))
        output = _emit(estate)
        first = _tree(estate)
        assert set(first) == EXPECTED_ARTEFACTS, sorted(set(first) ^ EXPECTED_ARTEFACTS)

        # One summary line per product (owner ruling R6).
        for product in ("retail.customer_segment", "retail.order", "retail.order_digest"):
            assert f"emitted {product}: label=" in output, output
        assert "adapters=duckdb,bigquery" in output

        second_output = _emit(estate)
        assert _tree(estate) == first, "re-emission is not byte-identical"
        assert f"generated 0 of {len(EXPECTED_ARTEFACTS)} file(s)" in second_output, second_output

        code, check_output = _run(estate, "--check")
        assert code == 0, check_output
        assert "0 problem(s)" in check_output, check_output


def test_check_reports_a_hand_edit_in_the_generated_schema_yaml_and_manifest() -> None:
    # Check mode must cover every artefact the route writes, not only the
    # model SQL a reader would think of first: the hand edits here are made
    # in the generated schema documentation and in the runtime manifest.
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))
        _emit(estate)

        schema = estate / PRODUCT_SCHEMA
        schema.write_text(
            _read(estate, PRODUCT_SCHEMA).replace("dpf.profile: derivation", "dpf.profile: hand"),
            encoding="utf-8",
        )
        code, output = _run(estate, "--check")
        assert code == 1, output
        assert f"DRIFT (hand-edited or stale): {PRODUCT_SCHEMA}" in output, output

        _emit(estate)
        manifest = estate / ORDER_MANIFEST
        manifest.write_text(
            _read(estate, ORDER_MANIFEST).replace('"max_retries": 3', '"max_retries": 9'),
            encoding="utf-8",
        )
        code, output = _run(estate, "--check")
        assert code == 1, output
        assert f"DRIFT (hand-edited or stale): {ORDER_MANIFEST}" in output, output


def test_a_removed_declaration_prunes_the_artefacts_it_owned() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))
        _emit(estate)
        (estate / "declarations" / "products" / "order_digest.yml").unlink()
        _emit(estate)
        remaining = set(_tree(estate))
        assert not any("order_digest" in name for name in remaining), sorted(remaining)


# --------------------------------------------------------------------------- green: routing


def test_every_occurrence_routes_to_exactly_one_owner() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))
        ctx = EstateContext.resolve(estate_root=estate)
        plans, _entries, graph = ep.build_product_plans(ctx)
        assert graph.order() == ("retail.customer_segment", "retail.order", "retail.order_digest")

        order = next(plan for plan in plans if plan.published_name == "retail.order")
        owned_patterns = {order.document["steps"][index]["pattern"] for index in order.owned_steps}
        assert owned_patterns == {
            "batch_transfer",
            "data_validation",
            "schema_transform",
            "calculated_fields",
            "data_enrichment",
            "data_filtering",
            "data_publish",
        }
        assert order.checkpoint_owned is True

        # data_validation is the estate's proof that the table, not the
        # engine, decides: the shaped label gives it to dbt and the served
        # label leaves it with the ingestion runtime translator, so the same
        # pattern has a different owner in one estate with no engine change.
        digest = next(plan for plan in plans if plan.published_name == "retail.order_digest")
        digest_patterns = {
            digest.document["steps"][index]["pattern"] for index in digest.owned_steps
        }
        assert "data_validation" not in digest_patterns, digest_patterns


def test_the_translator_registers_the_ten_patterns_and_the_shape_on_both_adapters() -> None:
    capabilities = DbtProductTranslator().capabilities()
    tokens = {capability.pattern_or_shape for capability in capabilities}
    assert tokens == set(PRODUCT_PATTERNS) | set(PRODUCT_SHAPES)
    assert len(PRODUCT_PATTERNS) == 10, PRODUCT_PATTERNS
    for token in tokens:
        adapters = {c.adapter for c in capabilities if c.pattern_or_shape == token}
        assert adapters == {"duckdb", "bigquery"}, (token, adapters)
    assert all(capability.translator == "dbt" for capability in capabilities)


# --------------------------------------------------------------------------- green: rendering


def test_every_pattern_renders_into_the_products_chain() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))
        _emit(estate)
        checked_pre = _read(estate, CHECKED_PRE_MODEL)
        prefilter = _read(estate, PREFILTER_MODEL)
        model = _read(estate, PRODUCT_MODEL)

        # batch_transfer reads the relation the upstream contract is bound
        # to. It sits in the checked relation because the pre-stage
        # validation occurrence cuts the chain there.
        assert 'from {{ ref("order_landing") }}' in checked_pre
        # data_validation computes one violation column per declared rule
        # and the quarantine relation names the rule each failing row broke.
        assert "coalesce(order_id is null, true) as violation_00" in checked_pre
        assert "'order_id_completeness' as rule_name" in _read(estate, QUARANTINE_PRE_MODEL)
        assert "'order_id_unique' as rule_name" in _read(estate, QUARANTINE_POST_MODEL)
        # the quarantine policy diverts the failing rows out of the chain.
        assert "where not violation_00" in prefilter
        assert 'from {{ ref("retail__order__checked_pre") }}' in prefilter
        # schema_transform renames, casts through the adapter type dispatch,
        # and drops.
        assert "cast(ordr_dt as {{ dpf_type('date') }}) as ordered_on" in prefilter
        assert "cast(ordr_ttl as {{ dpf_decimal_type(12, 2) }}) as order_total" in prefilter
        assert "junk_col" not in prefilter.split("step_02_schema_transform")[1]
        # calculated_fields: the inline expression verbatim, the named rule
        # through its dispatch macro.
        assert (
            "cast((status_code IN ('N', 'P')) as {{ dpf_type('boolean') }}) as is_open"
            in prefilter
        )
        assert (
            """cast(({{ order_band_v1("order_total") }}) as {{ dpf_type('string') }}) """
            "as order_band" in prefilter
        )
        # data_enrichment as an as-of read of the reference product.
        assert 'left join {{ ref("retail__customer_segment") }} as reference' in prefilter
        assert "reference.effective_from <= keys.ordered_on" in prefilter
        assert "row_number() over (" in prefilter
        # data_filtering keeps the rows every predicate holds for. It sits
        # in the post-stage checked relation, because that occurrence cuts
        # the chain after the filter.
        checked_post = _read(estate, CHECKED_POST_MODEL)
        assert "where (is_open)\n      and (order_total > 0)" in checked_post
        # data_publish renders the declared incremental publication.
        assert "materialized='incremental'" in model
        assert "incremental_strategy=dpf_incremental_strategy(" in model
        assert 'unique_key=["order_id"]' in model
        # the abort policy leaves every row in the chain: nothing is
        # diverted, the error-severity test is what stops the run.
        assert "where not violation_00" not in model
        # its two relations.
        assert 'select *\nfrom {{ ref("retail__order") }}' in _read(estate, POINTER_MODEL)
        sla = _read(estate, SLA_MODEL)
        assert "{{ dpf_publish_timestamp() }}" in sla and "count(*)" in sla


def test_the_filter_log_reports_every_predicate_by_name() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))
        _emit(estate)
        log = _read(estate, FILTER_LOG_MODEL)
        assert "'open_only' as predicate_name" in log
        assert "'positive_value' as predicate_name" in log
        assert "{{ dpf_excluded_count('predicate_00') }} as excluded_count" in log
        assert "{{ dpf_excluded_count('predicate_01') }} as excluded_count" in log
        assert 'from {{ ref("retail__order__prefilter") }}' in log


def test_a_calculated_field_carries_its_declared_rule_version_into_the_metadata() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))
        _emit(estate)
        document = yaml.safe_load(_read(estate, PRODUCT_SCHEMA))
        model = next(entry for entry in document["models"] if entry["name"] == "retail__order")
        column = next(entry for entry in model["columns"] if entry["name"] == "order_band")
        assert column["meta"] == {"dpf.rule": "order_band_v1", "dpf.rule_version": 1}
        inline = next(entry for entry in model["columns"] if entry["name"] == "is_open")
        assert "meta" not in inline, inline


def test_the_runtime_manifest_carries_the_run_boundary_and_the_retry_policy() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))
        _emit(estate)
        manifest = json.loads(_read(estate, ORDER_MANIFEST))
        assert manifest["schema"] == "ergasterion.product-runtime/v1"
        assert manifest["retry_policy"] == {"max_retries": 3, "backoff": "exponential"}
        assert manifest["run_boundary"]["granularity"] == "step"
        assert manifest["run_boundary"]["checkpoint"] is True
        assert manifest["aggregation"] == []
        assert manifest["run_boundary"]["occurrences"] == [
            "steps[0]:batch_transfer",
            "steps[1]:data_validation",
            "steps[2]:schema_transform",
            "steps[3]:calculated_fields",
            "steps[4]:data_enrichment",
            "steps[5]:data_filtering",
            "steps[6]:data_validation",
            "steps[11]:data_publish",
        ]
        assert manifest["materialisation"] == {
            "intent": "table",
            "publication_mode": "incremental",
            "strategy": {"bigquery": "merge", "duckdb": "delete+insert"},
            "unique_key": ["order_id"],
        }
        assert manifest["relations"] == {
            "published": ["retail.order"],
            "auxiliary": [
                "retail.order__checked_post",
                "retail.order__checked_pre",
                "retail.order__current",
                "retail.order__filter_log",
                "retail.order__prefilter",
                "retail.order__quarantine_post",
                "retail.order__quarantine_pre",
                "retail.order__sla",
            ],
        }


def test_a_product_declared_as_a_whole_select_body_renders_as_its_relation() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))
        _emit(estate)
        digest = _read(estate, DIGEST_MODEL)
        # A declared atomic publication is dbt's table materialisation: the
        # whole relation is built and swapped in as one step.
        assert "{{ config(materialized='table') }}" in digest
        assert "select order_id, customer_id, order_total as order_value" in digest
        assert 'from {{ ref("retail__order") }}' in digest
        assert "from source_00" in digest
        # No occurrence chain: the declared body is the whole relation.
        assert "step_" not in digest


# --------------------------------------------------------------------------- green: gates


def test_every_generated_model_parses_for_both_declared_adapters() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))
        ctx = EstateContext.resolve(estate_root=estate)
        plans, _entries, _graph = ep.build_product_plans(ctx)
        artefacts = DbtProductTranslator(products=plans).translate().artefacts
        parse_gate.assert_parses(artefacts, adapters=("duckdb", "bigquery"))

        # The resolution is the adapter's own physical type mapping, not a
        # neutral stand-in: the two adapters resolve the same cast
        # differently.
        for adapter in ("duckdb", "bigquery"):
            resolved = parse_gate.resolve_for_adapter(
                artefacts[PREFILTER_MODEL], artefact=PREFILTER_MODEL, adapter=adapter
            )
            expected = load_adapter_conventions(adapter).type_mapping["string"]
            assert f"cast(cust_id as {expected}) as customer_id" in resolved, resolved
            assert "{{" not in resolved and "{%" not in resolved


def test_every_generated_model_passes_the_deployment_adapter_dialect_gate() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))
        ctx = EstateContext.resolve(estate_root=estate)
        plans, _entries, _graph = ep.build_product_plans(ctx)
        artefacts = DbtProductTranslator(products=plans).translate().artefacts
        assert lint_artefacts(artefacts, "bigquery") == []


def test_no_snowflake_dispatch_branch_survives_in_the_macros_the_route_calls() -> None:
    for name in ENGINE_MACROS:
        text = (REPO_ROOT / "macros" / name).read_text(encoding="utf-8")
        assert "snowflake" not in text.lower(), f"macros/{name} still names Snowflake"


# --------------------------------------------------------------------------- green: executed build


def _dbt_executable() -> Path:
    candidate = Path(sys.executable).resolve().parent / ("dbt.exe" if os.name == "nt" else "dbt")
    assert candidate.is_file(), (
        f"the executed DuckDB lane needs the dbt executable beside the interpreter: {candidate}"
    )
    return candidate


def _dbt_build(estate: Path) -> None:
    result = subprocess.run(
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
    assert result.returncode == 0, result.stdout + result.stderr


def _query(estate: Path, statement: str):
    import duckdb

    project = yaml.safe_load((estate / "dbt_project.yml").read_text(encoding="utf-8"))["name"]
    connection = duckdb.connect(str(estate / "target" / f"{project}.duckdb"), read_only=True)
    try:
        return connection.execute(statement).fetchall()
    finally:
        connection.close()


def test_the_estate_builds_on_duckdb_with_the_known_answers_and_a_second_build_changes_nothing() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))
        _emit(estate)
        _dbt_build(estate)

        published = _query(
            estate,
            "select order_id, order_band, segment_label, order_total from retail__order order by order_id",
        )
        # o-3 is excluded by open_only and o-4 by positive_value. o-1's
        # order date falls after the second segment record takes effect and
        # o-2's before it, which is what the as-of join must resolve; o-5's
        # segment is not yet effective on its order date, so the no-match
        # policy leaves it empty.
        assert published == [
            ("o-1", "large", "GROWTH", 1500),
            ("o-2", "medium", "STARTER", 250),
            ("o-5", "small", None, 42),
        ], published
        assert _query(
            estate,
            "select predicate_name, excluded_count from retail__order__filter_log order by predicate_name",
        ) == [("open_only", 1), ("positive_value", 1)]
        assert _query(estate, "select row_count from retail__order__sla") == [(3,)]
        assert _query(estate, "select count(*) from retail__order__current") == [(3,)]
        assert _query(estate, "select count(*) from retail__order_digest") == [(3,)]
        # The atomically published reference product carries every row its
        # fixture relation delivers.
        assert _query(estate, "select count(*) from retail__customer_segment") == [(4,)]
        assert _query(estate, "select row_count from retail__customer_segment__sla") == [(4,)]
        # The quarantine relations are derived on every run: this estate
        # seeds no failing row, so each is empty rather than missing, and
        # the checked relations carry every row reaching their occurrence.
        assert _query(estate, "select count(*) from retail__order__quarantine_pre") == [(0,)]
        assert _query(estate, "select count(*) from retail__order__quarantine_post") == [(0,)]
        assert _query(estate, "select count(*) from retail__customer_segment__quarantine") == [(0,)]
        assert _query(estate, "select count(*) from retail__order__checked_pre") == [(5,)]
        assert _query(estate, "select count(*) from retail__order__checked_post") == [(3,)]
        sla_before = _query(estate, "select published_at is not null from retail__order__sla")
        assert sla_before == [(True,)]

        _dbt_build(estate)
        assert (
            _query(
                estate,
                "select order_id, order_band, segment_label, order_total from retail__order order by order_id",
            )
            == published
        ), "a second unchanged build changed the published relation"
        assert _query(
            estate,
            "select predicate_name, excluded_count from retail__order__filter_log order by predicate_name",
        ) == [("open_only", 1), ("positive_value", 1)]
        assert _query(estate, "select row_count from retail__order__sla") == [(3,)]


# --------------------------------------------------------------------------- red: routing


def test_the_incremental_publication_carries_every_declared_adapters_own_strategy() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))
        _emit(estate)
        model = _read(estate, PRODUCT_MODEL)
        manifest = json.loads(_read(estate, ORDER_MANIFEST))
        strategies = {
            adapter: load_adapter_conventions(adapter).incremental_strategy
            for adapter in ADAPTER_NAMES
        }
        # The two declared adapters accept different tokens, so one
        # hard-coded strategy could not compile on both.
        assert len(set(strategies.values())) == len(strategies), strategies
        for adapter, strategy in strategies.items():
            assert f'"{adapter}": "{strategy}"' in model, (adapter, model)
        assert manifest["materialisation"]["strategy"] == strategies


def test_a_select_body_beside_a_data_filtering_occurrence_fails_closed() -> None:
    # A body carries neither the declared predicates nor the filter-log
    # relation, so the rows the declaration excludes would be published
    # with nothing to audit them by.
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))

        def add_a_filter_and_a_body(document: dict) -> None:
            document["steps"].insert(
                3,
                {
                    "pattern": "data_filtering",
                    "predicates": [{"name": "named_only", "expression": "segment_name is not null"}],
                },
            )
            document["target"]["shape_config"] = {
                "select": "select segment_code, segment_name, effective_from, upper(segment_name) as segment_label"
            }

        _edit_declaration(estate, "customer_segment.yml", add_a_filter_and_a_body)
        _fails(estate, "body_cannot_carry_occurrence", "data_filtering")


def test_a_select_body_beside_a_named_rule_calculated_field_fails_closed() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))

        # The body refuses an evidence-owning occurrence first, so this
        # case hands the validation occurrence back to the ingestion runtime
        # translator: what is left for the body to collide with is the
        # named rule alone.
        _edit_estate(
            estate,
            lambda block: block["translators"]["shaped"].update(
                data_validation="local-ingestion"
            ),
        )

        def add_a_rule_and_a_body(document: dict) -> None:
            document["steps"][2]["fields"].append(
                {"name": "segment_band", "type": "string", "rule": "order_band_v1", "rule_version": 1}
            )
            document["target"]["shape_config"] = {
                "select": (
                    "select segment_code, segment_name, effective_from, "
                    "upper(segment_name) as segment_label, 'small' as segment_band"
                )
            }

        _edit_declaration(estate, "customer_segment.yml", add_a_rule_and_a_body)
        _fails(estate, "body_cannot_carry_occurrence", "order_band_v1", "segment_band")


def test_a_select_body_relation_carries_the_declared_types() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))
        _emit(estate)
        digest = _read(estate, DIGEST_MODEL)
        # The body says what a column is computed from; the declaration
        # says what type it publishes as, and the relation carries that.
        assert "cast(order_value as {{ dpf_decimal_type(12, 2) }}) as order_value" in digest
        assert "cast(order_id as {{ dpf_type('string') }}) as order_id" in digest
        _dbt_build(estate)
        types = dict(
            _query(
                estate,
                "select column_name, data_type from information_schema.columns "
                "where table_name = 'retail__order_digest'",
            )
        )
        assert types["order_value"] == "DECIMAL(12,2)", types


def test_the_parse_gate_resolves_a_declared_decimal_through_the_adapter_type_mapping() -> None:
    text = "select cast(a as {{ dpf_decimal_type(12, 2) }}) as a"
    for adapter in ADAPTER_NAMES:
        base = load_adapter_conventions(adapter).type_mapping["numeric"].split("(", 1)[0].strip()
        resolved = parse_gate.resolve_for_adapter(text, artefact="probe.sql", adapter=adapter)
        assert resolved == f"select cast(a as {base}(12, 2)) as a", (adapter, resolved)


def test_a_pattern_dbt_does_not_own_fails_closed_naming_the_pattern() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))
        _edit_estate(estate, lambda block: block["translators"]["shaped"].update(data_contracts="dbt"))
        _fails(estate, "data_contracts", "dbt", "duckdb", "no registered capability")


def test_a_label_with_no_table_entry_for_a_pattern_fails_closed() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))
        _edit_estate(estate, lambda block: block["translators"]["shaped"].pop("data_filtering"))
        _fails(estate, "shaped", "data_filtering", "no translator-table entry")


def test_publication_owned_by_another_translator_fails_closed() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))
        _edit_estate(
            estate, lambda block: block["translators"]["shaped"].update(data_publish="local-ingestion")
        )
        _fails(estate, "unowned_pattern", "data_publish")


def test_checkpointing_owned_by_another_translator_fails_closed() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))
        _edit_estate(
            estate,
            lambda block: block["translators"]["shaped"].update(checkpoint_retries="local-ingestion"),
        )
        _fails(estate, "unowned_pattern", "checkpoint_retries")


# --------------------------------------------------------------------------- red: rendering


def test_a_select_body_that_omits_a_published_column_fails_closed() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))

        def drop_a_column(document: dict) -> None:
            document["target"]["shape_config"]["select"] = "select order_id, customer_id"

        _edit_declaration(estate, "order_digest.yml", drop_a_column)
        _fails(estate, "relation_columns", "order_value")


def test_a_select_body_naming_its_own_relation_fails_closed() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))

        def name_a_relation(document: dict) -> None:
            document["target"]["shape_config"]["select"] = (
                "select order_id, customer_id, order_total as order_value from retail_order"
            )

        _edit_declaration(estate, "order_digest.yml", name_a_relation)
        _fails(estate, "select_body_names_a_relation")


def test_a_named_rule_whose_input_column_is_not_visible_fails_closed() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))
        catalogue = estate / "rules" / "order_band.yml"
        catalogue.write_text(
            catalogue.read_text(encoding="utf-8").replace("name: order_total", "name: gross_total"),
            encoding="utf-8",
        )
        _fails(estate, "unbound_rule_input", "order_band_v1", "gross_total")


def test_a_named_rule_resolving_to_two_macros_fails_closed() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))
        catalogue = estate / "rules" / "order_band.yml"
        catalogue.write_text(
            catalogue.read_text(encoding="utf-8").replace(
                "{translator: dbt, adapter: bigquery, macro: order_band_v1}",
                "{translator: dbt, adapter: bigquery, macro: order_band_bigquery}",
            ),
            encoding="utf-8",
        )
        _fails(estate, "order_band_v1", "more than one dbt macro")


def test_an_unknown_no_match_policy_fails_closed() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))

        def unknown_policy(document: dict) -> None:
            document["steps"][4]["lookups"][0]["no_match"] = "keep"

        _edit_declaration(estate, "order.yml", unknown_policy)
        _fails(estate, "unknown_no_match_policy", "keep")


def test_a_cast_to_a_structured_type_fails_closed() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))

        def structured_cast(document: dict) -> None:
            document["steps"][2]["mapping"][1]["type"] = {"name": "array"}

        _edit_declaration(estate, "order.yml", structured_cast)
        _fails(estate, "unrenderable_type", "array")


def test_a_second_source_with_no_declared_combination_fails_closed() -> None:
    # There is no default. A product reading two contracts says whether
    # their rows stack or their columns join, and one that says neither
    # fails closed naming the product and the key it has to declare, rather
    # than one of the two being picked for it.
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))

        def add_a_second_source(document: dict) -> None:
            document["sources"].append({"contract": "retail.customer_segment@1"})

        _edit_declaration(estate, "order.yml", add_a_second_source)
        _fails(estate, "undeclared_composition", "'order'", "combine", "union", "merge")


def test_a_combination_declared_by_a_product_reading_one_source_fails_closed() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))

        def combine_one_source(document: dict) -> None:
            document["combine"] = {"method": "union"}

        _edit_declaration(estate, "order.yml", combine_one_source)
        _fails(estate, "combine_without_two_sources", "'order'")


# --------------------------------------------------------------------------- red: declaration


def test_a_fixture_binding_that_does_not_pair_with_its_kind_fails_closed() -> None:
    # The pairing rule covers every source entry, not the first one: the
    # binding here is added to the SECOND entry of a product that already
    # declares one good source.
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))

        def unpaired_binding(document: dict) -> None:
            document["sources"].append(
                {
                    "contract": "retail.customer_segment@1",
                    "fixture": {
                        "relation": "segment_history",
                        "fields": [{"name": "segment_code", "type": "string"}],
                    },
                }
            )

        _edit_declaration(estate, "order.yml", unpaired_binding)
        _fails(estate, "fixture_binding_mismatch", "sources[1]")


def test_a_fixture_binding_that_shadows_a_declared_product_fails_closed() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))

        def shadow(document: dict) -> None:
            document["sources"][0]["kind"] = "fixture"
            document["sources"][0]["fixture"] = {
                "relation": "order_landing",
                "fields": [{"name": "order_id", "type": "string"}],
            }

        _edit_declaration(estate, "order_digest.yml", shadow)
        _fails(estate, "fixture_shadows_declared_product", "retail.order")


def test_an_incremental_publication_with_no_key_fails_closed() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))

        def drop_the_key(document: dict) -> None:
            document["steps"][11].pop("unique_key")

        _edit_declaration(estate, "order.yml", drop_the_key)
        _fails(estate, "publication_key_mismatch", "unique_key")


# --------------------------------------------------------------------------- red: gates


def test_a_product_claiming_a_private_relation_name_fails_closed() -> None:
    # The translator's private relations are registered in the estate graph,
    # which refuses one whose name is already a published relation. Here a
    # product is renamed onto the SLA record of another product in the same
    # domain, so the registration and the contract collide.
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))

        def claim_the_sla_name(document: dict) -> None:
            document["product"]["name"] = "order__sla"

        _edit_declaration(estate, "order_digest.yml", claim_the_sla_name)
        _fails(estate, "retail.order__sla", "already a published relation")


def test_an_unrenderable_identifier_fails_closed() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))

        def add_a_field_that_is_not_a_plain_name(document: dict) -> None:
            document["steps"][2]["fields"].append(
                {"name": "Segment Label", "type": "string", "expression": "lower(segment_name)"}
            )

        _edit_declaration(estate, "customer_segment.yml", add_a_field_that_is_not_a_plain_name)
        _fails(estate, "unrenderable_identifier", "Segment Label")


def test_a_publication_with_no_declared_mode_fails_closed() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))

        def drop_the_mode(document: dict) -> None:
            document["steps"][11].pop("publication_mode")
            document["steps"][11].pop("unique_key")

        _edit_declaration(estate, "order.yml", drop_the_mode)
        _fails(estate, "undeclared_publication_mode")


def test_the_runtime_manifest_refuses_a_policy_nobody_declared() -> None:
    from ergasterion.translators.dbt_patterns.checkpoint import runtime_manifest
    from ergasterion.translators.dbt_patterns.sql import RenderingError

    complete = {
        "product": "retail.order",
        "translator": "dbt",
        "occurrences": ["steps[0]:batch_transfer"],
        "publication": {"publication_mode": "atomic"},
        "materialisation": "table",
        "published_relations": ["retail.order"],
        "auxiliary_relations": [],
        "stored": [],
    }
    for checkpointing, occurrences, token in (
        ({"granularity": "step", "max_retries": 1}, complete["occurrences"], "backoff"),
        ({"granularity": "step", "max_retries": 1, "backoff": "none"}, [], "empty_run_boundary"),
    ):
        try:
            runtime_manifest(**{**complete, "checkpointing": checkpointing, "occurrences": occurrences})
        except RenderingError as error:
            assert token in str(error), (token, str(error))
        else:
            raise AssertionError(f"expected a partial manifest to fail closed on {token}")


def test_the_parse_gate_fails_closed_on_an_adapter_that_declares_no_quote_character() -> None:
    from ergasterion.framework import adapters as fw_adapters
    from ergasterion.framework.models import FrameworkError

    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        adapter = root / "quoteless"
        adapter.mkdir()
        (adapter / "conventions.yml").write_text(
            "adapter: quoteless\n"
            "kind: deployment\n"
            "dialect: duckdb\n"
            "incremental_strategy: delete+insert\n"
            "deny_rules:\n"
            "  - token: sample\n"
            "    pattern: 'sample'\n"
            '    message: "sample"\n'
            "type_mapping:\n"
            + "".join(f"  {token}: TEXT\n" for token in sorted(fw_adapters.NEUTRAL_TYPE_TOKENS))
            + "identifier_rules:\n  case_comparison: insensitive\n",
            encoding="utf-8",
        )
        original = fw_adapters.ADAPTERS_DIR
        fw_adapters.ADAPTERS_DIR = root
        try:
            parse_gate.resolve_for_adapter("select 1", artefact="probe.sql", adapter="quoteless")
        except FrameworkError as error:
            # The conventions loader owns what identifier rules must state,
            # so an adapter that declares no quote character never reaches
            # the gate at all.
            assert "quote_character" in str(error), str(error)
        else:
            raise AssertionError("expected an adapter with no quote character to fail closed")
        finally:
            fw_adapters.ADAPTERS_DIR = original


def test_the_parse_gate_fails_closed_on_a_construct_it_cannot_resolve() -> None:
    for text, token in (
        ("{% if true %}select 1{% endif %}", "unresolvable template statement"),
        ("select {{ 1 + 1 }} as a", "unresolvable template expression"),
        ("select cast(a as {{ dpf_type('geography') }}) as a", "names no type"),
        ("select * from {{ ref(model_name) }}", "takes plain string arguments"),
        ("select cast(a as {{ dpf_decimal_type(12) }}) as a", "takes a precision and a scale"),
    ):
        try:
            parse_gate.resolve_for_adapter(text, artefact="probe.sql", adapter="duckdb")
        except parse_gate.ArtefactParseError as error:
            assert token in str(error), (token, str(error))
        else:
            raise AssertionError(f"expected the parse gate to fail closed on {text!r}")

    try:
        parse_gate.assert_parses({"probe.sql": "select from where"}, adapters=("duckdb",))
    except Exception as error:  # noqa: BLE001 -- the parser's own error identity
        assert "probe.sql" in str(error), str(error)
    else:
        raise AssertionError("expected unparseable SQL to fail the parse gate")

    try:
        parse_gate.assert_parses(
            {"probe.sql": "{{ config(materialized='table') }}"}, adapters=("duckdb",)
        )
    except parse_gate.ArtefactParseError as error:
        assert "resolves to no SQL at all" in str(error), str(error)
    else:
        raise AssertionError("expected an artefact with no SQL left to fail the parse gate")


def test_a_deployment_adapter_dialect_offence_fails_the_command() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))

        def denied_construct(document: dict) -> None:
            document["steps"][3]["fields"][0]["expression"] = "read_csv('x') is not null"

        _edit_declaration(estate, "order.yml", denied_construct)
        _fails(estate, "dialect gate failed", "bigquery", "duckdb_file_scan")


# --------------------------------------------------------------------------- validation policies


def _model_entry(estate, relative: str, name: str) -> dict:
    document = yaml.safe_load(_read(estate, relative))
    return next(entry for entry in document["models"] if entry["name"] == name)


def _test_names(entry: dict) -> dict:
    """Every generated test on one model, keyed by its declared name, with
    the column it hangs from folded in."""

    found = {}
    for test in entry.get("data_tests") or []:
        (kind, body), = test.items()
        found[body["name"]] = {"test": kind, "column": None, **body}
    for column in entry.get("columns") or []:
        for test in column.get("data_tests") or []:
            (kind, body), = test.items()
            found[body["name"]] = {"test": kind, "column": column["name"], **body}
    return found


def test_each_on_failure_policy_renders_its_own_severity_and_threshold() -> None:
    # The three declared policies are all in this estate: the order product
    # quarantines before transformation and aborts after it, and the
    # reference product warns.
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))
        _emit(estate)

        order = _test_names(_model_entry(estate, PRODUCT_SCHEMA, "retail__order__checked_pre"))
        # quarantine: the rule itself warns, because the failing rows left
        # the chain, and the threshold is what aborts the run.
        assert order["dpf_checked_pre_order_id_completeness"]["config"]["severity"] == "warn"
        threshold = order["dpf_checked_pre_threshold_order_id_completeness"]
        assert threshold["config"]["severity"] == "error"
        assert threshold["arguments"] == {
            "violation_column": "violation_00",
            "rule": "order_id_completeness",
            "max_rate": 0.25,
        }

        post = _test_names(_model_entry(estate, PRODUCT_SCHEMA, "retail__order__checked_post"))
        # abort: no threshold at all, and the rule's own test stops the run.
        assert post["dpf_checked_post_order_id_unique"]["config"]["severity"] == "error"
        assert not [name for name in post if "threshold" in name], sorted(post)

        segment = _test_names(
            _model_entry(
                estate,
                "models/products/retail/retail__customer_segment.yml",
                "retail__customer_segment__checked",
            )
        )
        assert segment["dpf_checked_segment_code_not_null"]["config"]["severity"] == "warn"
        assert not [name for name in segment if "threshold" in name], sorted(segment)


def test_every_generated_test_that_takes_arguments_nests_them_under_arguments() -> None:
    # dbt reads a test mapping's top-level keys as its own settings, so a
    # declared value placed there is a value dbt would silently take for one
    # of its own. Every generated test therefore carries only name, config
    # and arguments.
    top_level = {"name", "config", "arguments"}
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))
        _emit(estate)
        seen = 0
        with_arguments = 0
        for relative in sorted(_tree(estate)):
            if not relative.endswith(".yml"):
                continue
            document = yaml.safe_load(_read(estate, relative))
            for entry in document["models"]:
                for name, body in _test_names(entry).items():
                    seen += 1
                    assert set(body) - {"test", "column"} <= top_level, (name, sorted(body))
                    if body.get("arguments"):
                        with_arguments += 1
        assert seen > 0 and with_arguments > 0, (seen, with_arguments)


def test_the_contract_compliance_check_names_every_published_column() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))
        _emit(estate)
        entry = _test_names(_model_entry(estate, PRODUCT_SCHEMA, "retail__order"))
        check = entry["dpf_contract_compliance_retail__order"]
        assert check["test"] == "dpf_contract_compliance"
        assert check["config"] == {"severity": "error", "store_failures": True}
        assert check["arguments"]["columns"] == [
            "order_id",
            "status_code",
            "segment_code",
            "ordered_on",
            "customer_id",
            "order_total",
            "is_open",
            "order_band",
            "segment_label",
        ]
        # The pre-stage completeness rule is what makes order_id required.
        assert check["arguments"]["required"] == ["order_id"]


# --------------------------------------------------------------------------- red: validation


def test_a_validation_occurrence_with_no_declared_policy_fails_closed() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))
        _edit_declaration(
            estate, "order.yml", lambda document: document["steps"][1].pop("on_failure")
        )
        _fails(estate, "undeclared_on_failure_policy", "steps[1]:data_validation")


def test_a_quarantine_policy_with_no_error_threshold_fails_closed() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))
        _edit_declaration(
            estate, "order.yml", lambda document: document["steps"][1].pop("error_threshold")
        )
        _fails(estate, "undeclared_error_threshold", "steps[1]:data_validation", "quarantine")


def test_a_validation_rule_that_declares_no_check_fails_closed() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))
        _edit_declaration(
            estate,
            "order.yml",
            lambda document: document["steps"][1]["rules"][0].pop("completeness"),
        )
        _fails(estate, "empty_validation_rule", "order_id")


def test_a_validation_rule_naming_a_column_the_composition_does_not_carry_fails_closed() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))
        _edit_declaration(
            estate,
            "order.yml",
            lambda document: document["steps"][1]["rules"][0].update(field="ordered_on"),
        )
        # ordered_on is the schema_transform's output, which the pre-stage
        # occurrence runs before: the rule would check nothing.
        _fails(estate, "unresolved_rule_field", "ordered_on")


def test_a_select_body_beside_a_dbt_owned_validation_occurrence_fails_closed() -> None:
    # The served label leaves data_validation with the ingestion runtime
    # translator, which is what lets its product state one select body.
    # Handing that occurrence to dbt makes the pair impossible: a body
    # renders no relation for the quarantine to divert rows into.
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))
        _edit_estate(
            estate, lambda block: block["translators"]["served"].update(data_validation="dbt")
        )
        _fails(estate, "body_cannot_carry_occurrence", "data_validation", "retail.order_digest")


def _duckdb_physical_type(estate, declared) -> str:
    """The physical type DuckDB spells the declared neutral type as, taken
    from the adapter conventions the engine itself casts through rather
    than from a second table written here."""

    if isinstance(declared, dict):
        physical = f"numeric({declared['precision']}, {declared['scale']})"
    else:
        token = sql_mod.SCALAR_TYPE_TOKENS[declared]
        physical = load_adapter_conventions("duckdb").type_mapping[token]
    return _query(estate, f"select typeof(cast(null as {physical}))")[0][0]


def test_the_built_relation_carries_the_declared_type_of_every_calculated_field() -> None:
    # A calculated field's expression has a result type of its own -- a
    # boolean comparison, a macro returning text -- and the declaration
    # states what the column publishes as. The built relation must carry
    # the declared one, or the contract states a type nothing holds.
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))
        _emit(estate)
        _dbt_build(estate)
        built = dict(
            _query(
                estate,
                "select column_name, data_type from information_schema.columns "
                "where table_name = 'retail__order'",
            )
        )
        assert built["is_open"] == _duckdb_physical_type(estate, "boolean"), built
        assert built["order_band"] == _duckdb_physical_type(estate, "string"), built
        assert built["order_total"] == _duckdb_physical_type(
            estate, {"name": "decimal", "precision": 12, "scale": 2}
        ), built


def test_changing_a_declared_calculated_field_type_changes_the_built_type() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))

        def restate_the_type(document: dict) -> None:
            field = next(
                entry
                for entry in document["steps"][3]["fields"]
                if entry["name"] == "is_open"
            )
            field["type"] = "string"

        _edit_declaration(estate, "order.yml", restate_the_type)
        _emit(estate)
        _dbt_build(estate)
        built = dict(
            _query(
                estate,
                "select column_name, data_type from information_schema.columns "
                "where table_name = 'retail__order'",
            )
        )
        assert built["is_open"] == _duckdb_physical_type(estate, "string"), built


# ------------------------------------------------- green: two or more sources

# Every artefact the consolidating estate's product route writes. Its three
# landing products are owned by the ingestion runtime translator, so no
# model, schema document or runtime manifest is written for any of them:
# that absence is what "the route hands dbt only its own products" means on
# disk.
CONSOLIDATING_PRODUCTS = (
    "customer_a",
    "customer_b",
    "customer_flags",
    "customer_union",
    "customer_master",
    "customer_directory",
)
CONSOLIDATING_ARTEFACTS = {
    f"models/products/crm/crm__{product}{suffix}"
    for product in CONSOLIDATING_PRODUCTS
    for suffix in (".sql", ".yml", "__checked.sql", "__quarantine.sql", "__current.sql", "__sla.sql")
} | {f"manifests/products/crm/{product}.json" for product in CONSOLIDATING_PRODUCTS}

UNION_COMPOSITION = "models/products/crm/crm__customer_union__checked.sql"
MERGE_COMPOSITION = "models/products/crm/crm__customer_master__checked.sql"


def _opening(estate: Path, artefact: str) -> str:
    """The common table expression a product's composition opens with."""

    return _read(estate, artefact).split("source_00 as (", 1)[1].split("),", 1)[0]


def test_the_consolidating_estate_emits_only_the_products_dbt_owns_byte_stably() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_consolidating_estate(Path(tmp))
        output = _emit(estate)
        first = _tree(estate)
        assert set(first) == CONSOLIDATING_ARTEFACTS, sorted(set(first) ^ CONSOLIDATING_ARTEFACTS)
        # Nothing is written for a landing product, and the summary still
        # names each one with the translator that owns it.
        for feed in ("customer_feed_a", "customer_feed_b", "customer_flag_feed"):
            assert not any(feed in name for name in first), sorted(first)
            assert (
                f"emitted crm.{feed}: label=landed profile=landing shape=declared "
                "owner=local-ingestion adapters=duckdb,bigquery artefacts=0"
            ) in output, output
        assert "emitted crm.customer_union: label=shaped profile=consolidation" in output
        assert "owner=dbt" in output, output

        second = _emit(estate)
        assert _tree(estate) == first, "re-emission is not byte-identical"
        assert f"generated 0 of {len(CONSOLIDATING_ARTEFACTS)} file(s)" in second, second

        code, check_output = _run(estate, "--check")
        assert code == 0, check_output
        assert "0 problem(s)" in check_output, check_output
        assert "structural budgets: 0 offense(s) over 2 declared adapter(s)" in check_output


def test_the_union_reads_every_source_and_casts_each_conformed_field() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_consolidating_estate(Path(tmp))
        _emit(estate)
        opening = _opening(estate, UNION_COMPOSITION)

        assert "union all" in opening, opening
        assert 'ref("crm__customer_a")' in opening, opening
        assert 'ref("crm__customer_b")' in opening, opening
        # Every conformed field is renamed and cast through the adapter's
        # own type mapping (the dispatch macros), never through a physical
        # type spelled in the engine.
        casts = (
            "cast(cust_ref as {{ dpf_type('string') }}) as customer_id",
            "cast(email_address as {{ dpf_type('string') }}) as email",
            "cast(signup_date as {{ dpf_type('date') }}) as signed_up_on",
            "cast(ltv as {{ dpf_decimal_type(12, 2) }}) as lifetime_value",
        )
        for cast in casts:
            assert cast in opening, (cast, opening)
        # The column both feeds already agree on is carried, not cast.
        assert "\n        source_system\n" in opening, opening
        # A union stacks rows and rows match by position, so both arms
        # project the schema's columns in the schema's order, whatever
        # order the source's own relation carries them in (the second feed
        # states them in another one).
        arms = opening.split("union all")
        assert len(arms) == 2, opening
        names = [
            [
                line.strip().rstrip(",").split(" as ")[-1]
                for line in arm.splitlines()
                if line.startswith("        ")
            ]
            for arm in arms
        ]
        assert (
            names[0]
            == names[1]
            == [
                "customer_id",
                "email",
                "signed_up_on",
                "lifetime_value",
                "loyalty_tier",
                "source_system",
            ]
        ), names
        # No feed's own column name survives into the union's own relation.
        published = _read(estate, "models/products/crm/crm__customer_union.sql")
        for private in ("cust_ref", "email_address", "signup_date", "ltv"):
            assert private not in published, published


def test_the_merge_joins_its_sources_and_coalesces_the_declared_key() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_consolidating_estate(Path(tmp))
        _emit(estate)
        opening = _opening(estate, MERGE_COMPOSITION)

        assert (
            "coalesce(merged_00.customer_id, merged_01.customer_id) as customer_id" in opening
        ), opening
        assert 'from {{ ref("crm__customer_a") }} as merged_00' in opening, opening
        assert 'full join {{ ref("crm__customer_flags") }} as merged_01' in opening, opening
        assert "on merged_00.customer_id = merged_01.customer_id" in opening, opening
        # Every non-key column is qualified by the one source that brings
        # it, so a merge never has to choose between two of a name.
        assert "merged_00.email as email" in opening, opening
        assert "merged_01.is_active as is_active" in opening, opening
        assert "union all" not in opening, opening


def test_the_consolidating_estate_parses_and_lints_for_both_declared_adapters() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_consolidating_estate(Path(tmp))
        ctx = EstateContext.resolve(estate_root=estate)
        plans, _entries, _graph = ep.build_product_plans(ctx)
        rendered = tuple(plan for plan in plans if plan.shape_owner == "dbt")
        artefacts = DbtProductTranslator(products=rendered).translate().artefacts
        parse_gate.assert_parses(artefacts, adapters=("duckdb", "bigquery"))
        for adapter in ("duckdb", "bigquery"):
            assert lint_artefacts(artefacts, adapter) == []


def test_the_consolidating_estate_builds_on_duckdb_with_the_known_answers() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_consolidating_estate(Path(tmp))
        _emit(estate)
        _dbt_build(estate)

        # The union stacks both feeds under the conformed schema: three
        # rows from the first source system and two from the second, each
        # second-system row carrying the value its whole-number column was
        # cast from and the date its own column named differently.
        # The first lane states a loyalty tier for every customer and the
        # second states none, which is what makes the coverage proof over
        # this union a real one.
        union = _query(
            estate,
            "select customer_id, email, signed_up_on, lifetime_value, loyalty_tier, "
            "source_system from crm__customer_union order by customer_id",
        )
        assert union == [
            ("c-1", "a1@example.com", date(2026, 1, 5), Decimal("100.00"), "gold", "feed_a"),
            ("c-2", "a2@example.com", date(2026, 2, 10), Decimal("250.50"), "silver", "feed_a"),
            ("c-3", "a3@example.com", date(2026, 3, 15), Decimal("75.25"), "bronze", "feed_a"),
            ("c-4", "b4@example.com", date(2026, 4, 1), Decimal("400.00"), None, "feed_b"),
            ("c-5", "b5@example.com", date(2026, 5, 20), Decimal("500.00"), None, "feed_b"),
        ], union
        assert _query(
            estate,
            "select source_system, count(*) from crm__customer_union group by source_system "
            "order by source_system",
        ) == [("feed_a", 3), ("feed_b", 2)]

        # The conformed columns carry the declared types on the built
        # adapter, not whatever the source column happened to be.
        built = dict(
            _query(
                estate,
                "select column_name, data_type from information_schema.columns "
                "where table_name = 'crm__customer_union'",
            )
        )
        assert built["lifetime_value"] == _duckdb_physical_type(
            estate, {"name": "decimal", "precision": 12, "scale": 2}
        ), built
        assert built["signed_up_on"] == _duckdb_physical_type(estate, "date"), built

        # The merge puts the two products' columns side by side and keeps
        # every row of both: the customer only the activity feed knows
        # carries the coalesced key and no column of the other side.
        master = _query(
            estate,
            "select customer_id, email, is_active, is_dormant from crm__customer_master "
            "order by customer_id",
        )
        assert master == [
            ("c-1", "a1@example.com", True, False),
            ("c-2", "a2@example.com", None, None),
            ("c-3", "a3@example.com", False, True),
            ("c-9", None, True, False),
        ], master

        # The consumer of the union sees the conformed schema and no
        # source-private column of either feed.
        directory = {
            row[0]
            for row in _query(
                estate,
                "select column_name from information_schema.columns "
                "where table_name = 'crm__customer_directory'",
            )
        }
        assert directory == {"customer_id", "email", "lifetime_value"}, directory
        assert _query(estate, "select count(*) from crm__customer_directory") == [(5,)]
        assert _query(estate, "select row_count from crm__customer_union__sla") == [(5,)]

        _dbt_build(estate)
        assert (
            _query(
                estate,
                "select customer_id, email, signed_up_on, lifetime_value, loyalty_tier, "
                "source_system from crm__customer_union order by customer_id",
            )
            == union
        ), "a second unchanged build changed the union"
        assert (
            _query(
                estate,
                "select customer_id, email, is_active, is_dormant from crm__customer_master "
                "order by customer_id",
            )
            == master
        ), "a second unchanged build changed the merge"


# --------------------------------------------------- red: two or more sources


def test_a_union_source_missing_a_field_of_the_schema_fails_closed() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_consolidating_estate(Path(tmp))

        def drop_one_conformance_entry(document: dict) -> None:
            document["sources"][1]["conform"]["mapping"] = [
                entry
                for entry in document["sources"][1]["conform"]["mapping"]
                if entry["to"] != "signed_up_on"
            ]

        _edit_declaration(estate, "customer_union.yml", drop_one_conformance_entry)
        _fails(
            estate,
            "cannot conform",
            "crm.customer_union",
            "crm.customer_b@1",
            "signed_up_on",
        )


def test_a_union_source_carrying_a_field_the_schema_does_not_fails_closed() -> None:
    # A union publishes one schema, so a source arriving with a column
    # beside it has nowhere to put it: the extra column is named, rather
    # than dropped or added to a schema the other source cannot fill.
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_consolidating_estate(Path(tmp))

        def add_a_column_of_its_own(document: dict) -> None:
            document["steps"][2]["fields"].append(
                {"name": "billing_region", "type": "string", "expression": "'north'"}
            )

        _edit_declaration(estate, "customer_b.yml", add_a_column_of_its_own)
        _fails(
            estate,
            "cannot conform",
            "crm.customer_union",
            "crm.customer_b@1",
            "billing_region",
        )


def test_a_union_source_declaring_another_type_for_a_field_fails_closed() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_consolidating_estate(Path(tmp))

        def state_another_type(document: dict) -> None:
            entry = next(
                item
                for item in document["sources"][1]["conform"]["mapping"]
                if item["to"] == "lifetime_value"
            )
            entry["type"] = "integer"

        _edit_declaration(estate, "customer_union.yml", state_another_type)
        _fails(
            estate,
            "cannot conform",
            "crm.customer_union",
            "crm.customer_b@1",
            "lifetime_value",
        )


def test_a_conformance_mapping_naming_a_field_the_source_lacks_fails_closed() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_consolidating_estate(Path(tmp))

        def rename_a_column_that_is_not_there(document: dict) -> None:
            document["sources"][1]["conform"]["mapping"][0]["from"] = "customer_ref"

        _edit_declaration(estate, "customer_union.yml", rename_a_column_that_is_not_there)
        _fails(
            estate,
            "cannot conform",
            "crm.customer_union",
            "crm.customer_b@1",
            "customer_ref",
        )


def test_a_merge_source_that_does_not_carry_a_declared_key_fails_closed() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_consolidating_estate(Path(tmp))
        _edit_declaration(
            estate,
            "customer_master.yml",
            lambda document: document["combine"].update(keys=["email"]),
        )
        _fails(
            estate,
            "cannot merge",
            "crm.customer_master",
            "crm.customer_flags@1",
            "email",
        )


def test_a_merge_whose_sources_both_bring_one_field_fails_closed() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_consolidating_estate(Path(tmp))

        def claim_the_other_sides_column(document: dict) -> None:
            document["steps"][2]["fields"].append(
                {"name": "source_system", "type": "string", "expression": "'flag_feed'"}
            )

        _edit_declaration(estate, "customer_flags.yml", claim_the_other_sides_column)
        _fails(
            estate,
            "cannot merge",
            "crm.customer_master",
            "crm.customer_flags@1",
            "source_system",
        )


def test_a_landing_product_with_no_schema_origin_fails_closed() -> None:
    # A landing product's columns are never derived from its own steps, so
    # one that neither arrives with a supplied schema nor binds the relation
    # it lands fails closed naming the product and its sources.
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_consolidating_estate(Path(tmp))
        _edit_declaration(
            estate, "customer_feed_a.yml", lambda document: document.update(sources=[])
        )
        _fails(estate, "crm.customer_feed_a", "no landing schema supplied", "sources")


def test_a_landing_product_binding_two_relations_fails_closed() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_consolidating_estate(Path(tmp))

        def bind_a_second_relation(document: dict) -> None:
            # Both bindings name the relation this landing product
            # publishes, so the pair is refused for being two statements of
            # one relation's columns, not for naming two relations.
            second = json.loads(json.dumps(document["sources"][0]))
            second["contract"] = "crm.customer_feed_a_backup@1"
            document["sources"].append(second)
            document["combine"] = {"method": "union"}

        _edit_declaration(estate, "customer_feed_a.yml", bind_a_second_relation)
        _fails(estate, "crm.customer_feed_a", "exactly one origin", "2 bound relations")


def test_a_combination_method_the_engine_does_not_know_fails_closed() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_consolidating_estate(Path(tmp))
        _edit_declaration(
            estate,
            "customer_union.yml",
            lambda document: document["combine"].update(method="join"),
        )
        _fails(estate, "unknown_combine_method", "customer_union", "'join'", "merge", "union")


def test_a_merge_declaring_no_keys_fails_closed() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_consolidating_estate(Path(tmp))
        _edit_declaration(
            estate,
            "customer_master.yml",
            lambda document: document["combine"].pop("keys"),
        )
        _fails(estate, "merge_without_keys", "customer_master", "combine.keys")


def test_a_merge_declaring_no_join_fails_closed() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_consolidating_estate(Path(tmp))
        _edit_declaration(
            estate,
            "customer_master.yml",
            lambda document: document["combine"].pop("join"),
        )
        _fails(
            estate,
            "merge_without_a_declared_join",
            "customer_master",
            "combine.join",
            "inner",
            "outer",
        )


def test_a_merge_declaring_a_join_the_engine_does_not_know_fails_closed() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_consolidating_estate(Path(tmp))
        _edit_declaration(
            estate,
            "customer_master.yml",
            lambda document: document["combine"].update(join="cross"),
        )
        _fails(
            estate,
            "merge_without_a_declared_join",
            "customer_master",
            "combine.join",
            "'cross'",
        )


def test_a_union_declaring_a_join_fails_closed() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_consolidating_estate(Path(tmp))
        _edit_declaration(
            estate,
            "customer_union.yml",
            lambda document: document["combine"].update(join="outer"),
        )
        _fails(estate, "union_with_a_join", "customer_union", "combine.join")


def test_the_declared_join_renders_and_the_contract_follows_it() -> None:
    """The one declaration decides both halves: which rows the merge keeps,
    and how required the columns it inherits can be. An outer merge keeps
    every row of every source, so a column inherited from a side that can be
    unmatched is published as optional; an inner merge keeps only the rows
    every source carries the key of, so each stays as required as the source
    it came from."""

    def compliance(estate: Path) -> dict:
        document = yaml.safe_load(_read(estate, "models/products/crm/crm__customer_master.yml"))
        return next(
            entry["dpf_contract_compliance"]
            for model in document["models"]
            for entry in model.get("data_tests") or []
            if "dpf_contract_compliance" in entry
        )

    with tempfile.TemporaryDirectory() as tmp:
        # The fixture declares an outer join. is_active and is_dormant are
        # required in the activity lane's own contract, and the merge keeps
        # customers that lane never saw, so the merged contract publishes
        # them as optional.
        outer = _scratch_consolidating_estate(Path(tmp) / "outer")
        _edit_declaration(
            outer,
            "customer_flags.yml",
            lambda document: document["steps"][1]["rules"].extend(
                [{"field": "is_active", "not_null": True}]
            ),
        )
        _emit(outer)
        assert "full join" in _opening(outer, MERGE_COMPOSITION), _opening(
            outer, MERGE_COMPOSITION
        )
        lane = yaml.safe_load(_read(outer, "models/products/crm/crm__customer_flags.yml"))
        lane_required = next(
            entry["dpf_contract_compliance"]["arguments"]["required"]
            for model in lane["models"]
            for entry in model.get("data_tests") or []
            if "dpf_contract_compliance" in entry
        )
        assert "is_active" in lane_required, lane_required
        assert compliance(outer)["arguments"]["required"] == ["customer_id"], compliance(outer)

        inner = _scratch_consolidating_estate(Path(tmp) / "inner")
        _edit_declaration(
            inner,
            "customer_flags.yml",
            lambda document: document["steps"][1]["rules"].extend(
                [{"field": "is_active", "not_null": True}]
            ),
        )
        _edit_declaration(
            inner,
            "customer_master.yml",
            lambda document: document["combine"].update(join="inner"),
        )
        _emit(inner)
        opening = _opening(inner, MERGE_COMPOSITION)
        assert "inner join" in opening, opening
        assert "full join" not in opening, opening
        assert compliance(inner)["arguments"]["required"] == [
            "customer_id",
            "is_active",
        ], compliance(inner)


def test_an_outer_merge_keeps_the_unmatched_rows_an_inner_merge_drops() -> None:
    """The same two sources, the same keys, one declaration apart. Proved on
    the built adapter, so the join keyword and the rows are the same fact."""

    with tempfile.TemporaryDirectory() as tmp:
        outer = _scratch_consolidating_estate(Path(tmp) / "outer")
        _emit(outer)
        _dbt_build(outer)
        assert _query(
            outer, "select customer_id from crm__customer_master order by customer_id"
        ) == [("c-1",), ("c-2",), ("c-3",), ("c-9",)]

        inner = _scratch_consolidating_estate(Path(tmp) / "inner")
        _edit_declaration(
            inner,
            "customer_master.yml",
            lambda document: document["combine"].update(join="inner"),
        )
        # An inner merge keeps only the customers both sides carry, and the
        # coverage proof this product declares is what says so out loud.
        _emit(inner)
        output = _dbt_build_fails(inner)
        assert "dpf_reconcile_coverage_crm__customer_master_crm__customer_a" in output, output
        assert _query(
            inner, "select customer_id from crm__customer_master order by customer_id"
        ) == [("c-1",), ("c-3",)]


def test_a_union_declaring_keys_fails_closed() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_consolidating_estate(Path(tmp))
        _edit_declaration(
            estate,
            "customer_union.yml",
            lambda document: document["combine"].update(keys=["customer_id"]),
        )
        _fails(estate, "union_with_keys", "customer_union", "combine.keys")


def test_a_conformance_mapping_on_a_product_combining_nothing_fails_closed() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_consolidating_estate(Path(tmp))

        def conform_a_lone_source(document: dict) -> None:
            document["sources"][0]["conform"] = {
                "mapping": [{"from": "email", "to": "contact_email", "type": "string"}]
            }

        _edit_declaration(estate, "customer_a.yml", conform_a_lone_source)
        _fails(estate, "conform_without_a_combination", "customer_a", "sources[0].conform")


def test_a_conformance_mapping_renaming_onto_a_name_the_source_carries_fails_closed() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_consolidating_estate(Path(tmp))

        def collide_with_a_column_already_there(document: dict) -> None:
            entry = next(
                item
                for item in document["sources"][1]["conform"]["mapping"]
                if item["to"] == "customer_id"
            )
            entry["to"] = "source_system"

        _edit_declaration(estate, "customer_union.yml", collide_with_a_column_already_there)
        _fails(
            estate,
            "cannot conform",
            "crm.customer_union",
            "crm.customer_b@1",
            "source_system",
        )


def test_a_conformance_mapping_renaming_one_column_twice_fails_closed() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_consolidating_estate(Path(tmp))

        def rename_it_twice(document: dict) -> None:
            mapping = document["sources"][1]["conform"]["mapping"]
            mapping.append({"from": "cust_ref", "to": "customer_ref", "type": "string"})

        _edit_declaration(estate, "customer_union.yml", rename_it_twice)
        _fails(estate, "cannot conform", "crm.customer_union", "crm.customer_b@1", "cust_ref")


def test_the_translator_refuses_a_combination_it_has_no_arm_for() -> None:
    # Unreachable through a declaration: layer 1 admits only the two
    # methods, and the contract pipe resolves nothing else. The branch is
    # the translator's own guard against a plan it cannot render, so it is
    # driven directly here rather than through an estate that cannot state
    # it.
    composition = OpeningComposition(
        method="cross",
        keys=(),
        fields=(),
        sources=(
            SourceComposition(key="crm.customer_a", contract="crm.customer_a@1", columns=()),
            SourceComposition(key="crm.customer_b", contract="crm.customer_b@1", columns=()),
        ),
    )
    try:
        steps_mod.render_opening(
            composition,
            name="source_00",
            models={"crm.customer_a": "crm__customer_a", "crm.customer_b": "crm__customer_b"},
            product="crm.pair",
        )
    except sql_mod.RenderingError as error:
        message = str(error)
        assert "crm.pair" in message and "unrenderable_composition" in message, message
        assert "'cross'" in message, message
    else:
        raise AssertionError("expected a RenderingError for a combination with no rendering")


def test_two_sources_reading_one_relation_fail_closed() -> None:
    # A second read of one relation is not a second source: a union would
    # stack it on itself and a merge would collide it with itself, so the
    # pair is refused before either is rendered.
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_consolidating_estate(Path(tmp))

        def read_the_same_product_twice(document: dict) -> None:
            document["sources"][1] = {"contract": "crm.customer_a@1"}
            document["sources"][1]["expect"] = {"fields": ["customer_id"]}

        _edit_declaration(estate, "customer_union.yml", read_the_same_product_twice)
        _fails(estate, "duplicate_source", "customer_union", "crm.customer_a@1", "sources[1]")


def test_a_landing_binding_naming_another_relation_fails_closed() -> None:
    # Every consumer of a landing contract reads the model of the product's
    # own relation, so a binding naming a second relation would leave them
    # reading one nothing lands.
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_consolidating_estate(Path(tmp))

        def bind_another_relation(document: dict) -> None:
            document["sources"][0]["fixture"]["relation"] = "crm__customer_feed_a_raw"

        _edit_declaration(estate, "customer_feed_a.yml", bind_another_relation)
        _fails(
            estate,
            "crm.customer_feed_a",
            "sources[0]",
            "crm__customer_feed_a_raw",
            "crm__customer_feed_a",
        )


def test_a_merge_whose_first_source_lacks_a_declared_key_fails_closed() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_consolidating_estate(Path(tmp))
        _edit_declaration(
            estate,
            "customer_master.yml",
            lambda document: document["combine"].update(keys=["is_active"]),
        )
        _fails(
            estate,
            "cannot merge",
            "crm.customer_master",
            "crm.customer_a@1",
            "is_active",
        )


def test_a_merge_whose_sources_declare_two_types_for_a_key_fails_closed() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_consolidating_estate(Path(tmp))

        def retype_the_key_on_one_side(document: dict) -> None:
            document["sources"][1]["conform"] = {
                "mapping": [{"from": "customer_id", "to": "customer_id", "type": "integer"}]
            }

        _edit_declaration(estate, "customer_master.yml", retype_the_key_on_one_side)
        _fails(
            estate,
            "cannot merge",
            "crm.customer_master",
            "crm.customer_flags@1",
            "customer_id",
            "'integer'",
        )


# ------------------------------------- the coverage a consolidated product proves


def _dbt_build_fails(estate: Path) -> str:
    """Drive the real dbt build and return everything it printed, asserting
    it failed. Used where the point of the case is a generated test going
    red on data, not the command refusing to emit."""

    result = subprocess.run(
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
    assert result.returncode != 0, "the build passed where a generated test was expected to go red"
    return result.stdout + result.stderr


def _filter_one_row_out_of_the_union(estate: Path) -> None:
    """Drop one row of the second feed from the union's own relation, so
    that source's rows are no longer all covered by what the product
    publishes."""

    _edit_estate(
        estate, lambda block: block["translators"]["shaped"].update(data_filtering="dbt")
    )

    def add_the_filter(document: dict) -> None:
        position = next(
            index
            for index, step in enumerate(document["steps"])
            if step["pattern"] == "data_contracts"
        )
        document["steps"].insert(
            position,
            {
                "pattern": "data_filtering",
                "predicates": [
                    {"name": "drop_one_feed_row", "expression": "customer_id <> " + repr("c-5")}
                ],
            },
        )

    _edit_declaration(estate, "customer_union.yml", add_the_filter)


def test_a_consolidated_product_emits_one_coverage_test_per_source() -> None:
    # Architecture section 8: a consolidated product proves its coverage
    # against the upstream contracts it consolidates. The union proves it
    # on the schema it publishes and the merge on the key its columns were
    # merged on, each source compared under its own column names.
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_consolidating_estate(Path(tmp))
        _emit(estate)
        union = yaml.safe_load(_read(estate, "models/products/crm/crm__customer_union.yml"))
        tests = {
            entry["dpf_reconcile_coverage"]["name"]: entry["dpf_reconcile_coverage"]
            for model in union["models"]
            for entry in model.get("data_tests") or []
            if "dpf_reconcile_coverage" in entry
        }
        assert set(tests) == {
            "dpf_reconcile_coverage_crm__customer_union_crm__customer_a",
            "dpf_reconcile_coverage_crm__customer_union_crm__customer_b",
        }, sorted(tests)
        second = tests["dpf_reconcile_coverage_crm__customer_union_crm__customer_b"]
        assert second["arguments"]["source"] == "ref('crm__customer_b')", second
        assert second["arguments"]["keys"] == [
            "customer_id",
            "email",
            "signed_up_on",
            "lifetime_value",
            "loyalty_tier",
            "source_system",
        ], second
        # The second feed is read under its own names, so the proof
        # compares the two namings rather than one of them twice.
        assert second["arguments"]["source_columns"] == [
            "cust_ref",
            "email_address",
            "signup_date",
            "ltv",
            "loyalty_tier",
            "source_system",
        ], second
        assert second["config"] == {"severity": "error", "store_failures": True}, second

        master = yaml.safe_load(_read(estate, "models/products/crm/crm__customer_master.yml"))
        merged = {
            entry["dpf_reconcile_coverage"]["name"]: entry["dpf_reconcile_coverage"]
            for model in master["models"]
            for entry in model.get("data_tests") or []
            if "dpf_reconcile_coverage" in entry
        }
        assert set(merged) == {
            "dpf_reconcile_coverage_crm__customer_master_crm__customer_a",
            "dpf_reconcile_coverage_crm__customer_master_crm__customer_flags",
        }, sorted(merged)
        # A merge is proved on the key it was merged on, not on every column.
        for entry in merged.values():
            assert entry["arguments"]["keys"] == ["customer_id"], entry


def test_the_coverage_test_goes_red_when_a_source_row_stops_arriving() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_consolidating_estate(Path(tmp))
        _filter_one_row_out_of_the_union(estate)
        _emit(estate)
        output = _dbt_build_fails(estate)
        assert (
            "FAIL 1 dpf_reconcile_coverage_crm__customer_union_crm__customer_b" in output
        ), output
        # Only the source that lost a row is red; the other still proves.
        assert "PASS dpf_reconcile_coverage_crm__customer_union_crm__customer_a" in output, output


def test_the_coverage_test_covers_a_lane_that_never_states_a_column() -> None:
    """A union publishes one schema, so its coverage is proved on every
    column of it, and a lane that never states one of those columns brings
    it empty on every row. Those rows are covered: the product's relation
    carries each of them, empty column and all."""

    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_consolidating_estate(Path(tmp))
        _emit(estate)
        _dbt_build(estate)
        assert _query(
            estate,
            "select count(*) from crm__customer_union where loyalty_tier is null",
        ) == [(2,)], "the second lane is expected to state no loyalty tier"


def test_an_equality_comparison_in_the_coverage_rule_turns_that_lane_red() -> None:
    """The seen-RED proof for the null-safe comparison. The same estate, the
    same data, one macro reverted to comparing with plain equality: the lane
    that states no loyalty tier then reports every row it delivered as
    missing, while every one of them is in the product's relation."""

    equality = """where not exists (
    select 1
    from published_keys
    where {% for key in keys %}source_keys.{{ key }} = published_keys.{{ key }}{% if not loop.last %}
      and {% endif %}{% endfor %}
)"""
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_consolidating_estate(Path(tmp))
        macro = estate / "macros" / "product_tests.sql"
        text = macro.read_text(encoding="utf-8")
        start = text.index("where not exists (")
        end = text.index("{%- endtest %}", start)
        macro.write_text(text[:start] + equality + chr(10) + text[end:], encoding="utf-8")

        _emit(estate)
        output = _dbt_build_fails(estate)
        assert (
            "FAIL 2 dpf_reconcile_coverage_crm__customer_union_crm__customer_b" in output
        ), output
        # The lane that does state the column is unaffected either way.
        assert "PASS dpf_reconcile_coverage_crm__customer_union_crm__customer_a" in output, output


def test_the_coverage_renderer_refuses_a_product_that_combines_nothing() -> None:
    # Unreachable through a declaration: a reconciliation names two or more
    # contracts, the estate graph requires the product to consume every one
    # of them, and a product reading two sources declares how they combine.
    # The branch is the renderer's guard against being handed a
    # composition that combines nothing, so it is driven directly.
    composition = OpeningComposition(
        method=None,
        keys=(),
        fields=(),
        sources=(
            SourceComposition(
                key="crm.customer_a", contract="crm.customer_a@1", columns=()
            ),
        ),
    )
    step = {
        "pattern": "data_validation",
        "rules": [
            {
                "reconcile": {
                    "metric": "row_count",
                    "contracts": ["crm.customer_a@1", "crm.customer_b@1"],
                }
            }
        ],
    }
    try:
        validation_mod.reconcile_tests(
            step,
            product="crm.lone",
            occurrence="steps[1]:data_validation",
            published_model="crm__lone",
            published_columns=("customer_id",),
            published_stored={},
            composition=composition,
            source_models={"crm.customer_a": "crm__customer_a"},
        )
    except sql_mod.RenderingError as error:
        message = str(error)
        assert "reconciliation_without_a_combination" in message, message
        assert "crm.lone" in message, message
    else:
        raise AssertionError("expected a RenderingError for a product that combines nothing")


def test_the_merge_renderer_refuses_a_composition_carrying_no_declared_join() -> None:
    # Unreachable through a declaration: a merge that names no join is
    # refused before anything is generated, and the contract pipe refuses it
    # again where the schemas are combined. The branch is the renderer's
    # guard against being handed a composition that never chose, so it is
    # driven directly rather than through a declaration that cannot exist.
    composition = OpeningComposition(
        method="merge",
        keys=("customer_id",),
        fields=(),
        sources=(
            SourceComposition(
                key="crm.customer_a",
                contract="crm.customer_a@1",
                columns=(ConformedColumn(source_name="customer_id", name="customer_id", cast_type=None),),
            ),
            SourceComposition(
                key="crm.customer_flags",
                contract="crm.customer_flags@1",
                columns=(ConformedColumn(source_name="customer_id", name="customer_id", cast_type=None),),
            ),
        ),
    )
    try:
        steps_mod.render_opening(
            composition,
            name="opening",
            models={
                "crm.customer_a": "crm__customer_a",
                "crm.customer_flags": "crm__customer_flags",
            },
            product="crm.customer_master",
        )
    except sql_mod.RenderingError as error:
        message = str(error)
        assert "unrenderable_merge_join" in message, message
        assert "crm.customer_master" in message, message
        assert "inner" in message and "outer" in message, message
    else:
        raise AssertionError("expected a RenderingError for a merge with no declared join")


def test_a_reconciliation_metric_this_route_does_not_prove_fails_closed() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_consolidating_estate(Path(tmp))

        def state_another_metric(document: dict) -> None:
            rule = next(rule for rule in document["steps"][1]["rules"] if "reconcile" in rule)
            rule["reconcile"]["metric"] = "row_hash"

        _edit_declaration(estate, "customer_union.yml", state_another_metric)
        _fails(
            estate,
            "unrenderable_reconciliation_metric",
            "crm.customer_union",
            "'row_hash'",
            "row_count",
        )


def test_a_reconciliation_key_the_published_relation_drops_fails_closed() -> None:
    # The proof is stated in the columns the combination was made on, so a
    # column dropped between the composition and the published relation
    # leaves the proof with nothing to compare: it names product, source
    # and key rather than emitting a test over a column that is not there.
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_consolidating_estate(Path(tmp))
        _edit_estate(
            estate, lambda block: block["translators"]["shaped"].update(schema_transform="dbt")
        )

        def drop_a_conformed_column(document: dict) -> None:
            position = next(
                index
                for index, step in enumerate(document["steps"])
                if step["pattern"] == "data_contracts"
            )
            document["steps"].insert(
                position,
                {
                    "pattern": "schema_transform",
                    "mapping": [{"from": "source_system", "drop": True}],
                },
            )

        _edit_declaration(estate, "customer_union.yml", drop_a_conformed_column)
        _fails(
            estate,
            "unreconcilable_key",
            "crm.customer_union",
            "crm.customer_a@1",
            "source_system",
        )


def test_the_coverage_renderer_refuses_a_contract_the_product_does_not_read() -> None:
    # Unreachable through a declaration: the estate graph refuses a
    # reconciliation naming a contract the product does not consume before
    # anything is rendered. The branch is the renderer's own guard, so it
    # is driven directly.
    composition = OpeningComposition(
        method="merge",
        keys=("customer_id",),
        fields=(),
        sources=(
            SourceComposition(
                key="crm.customer_a",
                contract="crm.customer_a@1",
                columns=(
                    ConformedColumn(
                        source_name="customer_id", name="customer_id", cast_type=None
                    ),
                ),
            ),
        ),
    )
    step = {
        "pattern": "data_validation",
        "rules": [{"reconcile": {"metric": "row_count", "contracts": ["crm.elsewhere@1"]}}],
    }
    try:
        validation_mod.reconcile_tests(
            step,
            product="crm.pair",
            occurrence="steps[1]:data_validation",
            published_model="crm__pair",
            published_columns=("customer_id",),
            published_stored={},
            composition=composition,
            source_models={"crm.customer_a": "crm__customer_a"},
        )
    except sql_mod.RenderingError as error:
        message = str(error)
        assert "reconciliation_source_not_read" in message, message
        assert "crm.elsewhere@1" in message and "crm.pair" in message, message
    else:
        raise AssertionError("expected a RenderingError for a contract the product does not read")


# ----------------------------------- the landing relation, rendered either way

# The relation each landing product lands, once the estate gives the
# landing label's shape to dbt: dbt then renders the product as a model
# over that relation, so the relation it lands and the model it publishes
# are two different names.
LANDED_RELATIONS = {
    "customer_feed_a": "raw_customer_feed_a",
    "customer_feed_b": "raw_customer_feed_b",
    "customer_flag_feed": "raw_customer_flag_feed",
}


def _landing_rendered_by_dbt(root: Path) -> Path:
    """The consolidating estate with its landing label given to dbt: the
    shape and the patterns that render a relation, and each landing
    product's binding naming the relation it lands rather than the model
    dbt renders for it. The seeds are renamed with it, because the seed is
    what stands in for the landed relation in this estate."""

    estate = _scratch_consolidating_estate(root)
    _edit_estate(
        estate,
        lambda block: block["translators"]["landed"].update(
            declared="dbt",
            data_validation="dbt",
            data_publish="dbt",
            checkpoint_retries="dbt",
        ),
    )
    for product, landed in LANDED_RELATIONS.items():
        (estate / "seeds" / f"crm__{product}.csv").rename(estate / "seeds" / f"{landed}.csv")
        _edit_declaration(
            estate,
            f"{product}.yml",
            lambda document, relation=landed: document["sources"][0]["fixture"].update(
                relation=relation
            ),
        )
    project = estate / "dbt_project.yml"
    document = yaml.safe_load(project.read_text(encoding="utf-8"))
    document["seeds"]["consolidating_two"] = {
        LANDED_RELATIONS[name.removeprefix("crm__")]: configuration
        for name, configuration in document["seeds"]["consolidating_two"].items()
    }
    project.write_text(yaml.safe_dump(document, sort_keys=False), encoding="utf-8")
    return estate


def test_a_landing_product_dbt_renders_reads_the_relation_it_lands() -> None:
    # Architecture section 9 and check 12: the table decides which
    # translator renders a product's relation. When it names dbt for a
    # landing product's shape, dbt renders the product as a model over the
    # relation it lands, and every consumer reads that model.
    with tempfile.TemporaryDirectory() as tmp:
        estate = _landing_rendered_by_dbt(Path(tmp))
        output = _emit(estate)
        for product, landed in LANDED_RELATIONS.items():
            assert f"emitted crm.{product}: label=landed" in output, output
            assert "owner=dbt" in output, output
            # The composition of a landing product dbt renders opens on
            # the relation it lands, and nothing it renders reads the model
            # it publishes.
            opening = _read(estate, f"models/products/crm/crm__{product}__checked.sql")
            assert f'ref("{landed}")' in opening, opening
            for artefact in (f"crm__{product}.sql", f"crm__{product}__checked.sql"):
                text = _read(estate, f"models/products/crm/{artefact}")
                assert f'ref("crm__{product}")' not in text, text
        # The consumers read the model dbt renders for the landing product,
        # which is the relation its contract publishes.
        consumer = _read(estate, "models/products/crm/crm__customer_a__checked.sql")
        assert 'ref("crm__customer_feed_a")' in consumer, consumer

        _dbt_build(estate)
        assert _query(estate, "select count(*) from crm__customer_feed_a") == [(3,)]
        assert _query(
            estate,
            "select source_system, count(*) from crm__customer_union group by source_system "
            "order by source_system",
        ) == [("feed_a", 3), ("feed_b", 2)]
        assert _query(estate, "select count(*) from crm__customer_directory") == [(5,)]


def test_a_landing_product_dbt_renders_that_binds_its_own_model_fails_closed() -> None:
    # The other side of the same rule: a binding naming the model dbt
    # renders for the product would be a model reading itself, which dbt
    # only reports as a cycle at build time.
    with tempfile.TemporaryDirectory() as tmp:
        estate = _landing_rendered_by_dbt(Path(tmp))
        _edit_declaration(
            estate,
            "customer_feed_a.yml",
            lambda document: document["sources"][0]["fixture"].update(
                relation="crm__customer_feed_a"
            ),
        )
        _fails(
            estate,
            "crm.customer_feed_a",
            "sources[0]",
            "crm__customer_feed_a",
            "landed",
            "dbt",
        )


def main() -> int:
    tests = [obj for name, obj in sorted(globals().items()) if name.startswith("test_") and callable(obj)]
    failures = 0
    for test in tests:
        try:
            test()
            print(f"PASS {test.__name__}")
        except Exception:  # noqa: BLE001 -- report-and-continue harness
            failures += 1
            print(f"FAIL {test.__name__}")
            traceback.print_exc()
    print(f"\n{len(tests) - failures}/{len(tests)} passed")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
