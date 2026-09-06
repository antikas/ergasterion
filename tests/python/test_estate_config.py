"""Self-tests for estate configuration: the labels, the profiles, the
adapters and the translator table as estate data (architecture sections 5, 9,
11 and 13; checks 4, 12 and 13; owner rulings R1, R6; plan decisions D29,
D32, D35).

The claim under test is that an estate configures the engine rather than
extending it. A label the engine has never seen, mapped to a profile the
engine ships, emits. A profile the engine does not ship, declared by the
estate and held to the same fifteen-pattern registry, emits. Both are proved
by running the real ``ergasterion emit-products`` command against
``tests/fixtures/estates/new_label``, whose two labels and own profile appear
nowhere in ``ergasterion/`` -- asserted here, not assumed.

The gate stage is here as well: the estate's structural budgets, run once per
declared adapter over the product tree, with the interface-boundary rule
governing the relations a product publishes and leaving the translator-private
relations beside them alone.

The rest is the fail-closed half. Every red case copies that fixture into a
scratch directory, changes exactly one thing in the estate configuration, and
drives the same real command, asserting what the failure names: the label and
the key at fault.

Same plain assert-and-report convention as the rest of this repo (no pytest
in the .venv): each ``test_*`` raises ``AssertionError`` on failure, and
``main()`` runs them all and reports PASS/FAIL.

Usage:
    python tests/python/test_estate_config.py
"""

from __future__ import annotations

import contextlib
import io
import shutil
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

REPO_ROOT = Path(__file__).resolve().parents[2]
ENGINE_ROOT = REPO_ROOT / "ergasterion"
FIXTURE_ESTATE = REPO_ROOT / "tests" / "fixtures" / "estates" / "new_label"

# The estate vocabulary the engine must not know: two layer labels and one
# profile, all three declared only in the fixture's estate.yml.
FIXTURE_LABELS = ("harmonised", "outbound")
FIXTURE_PROFILE = "handover"

FACTS = ("catalogue", "item_facts")


# --------------------------------------------------------------------------- harness


def _scratch_estate(root: Path) -> Path:
    estate = root / "estate"
    shutil.copytree(FIXTURE_ESTATE, estate)
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


def _edit_estate(estate: Path, mutate) -> None:
    path = estate / "estate.yml"
    document = yaml.safe_load(path.read_text(encoding="utf-8"))
    mutate(document["estate"])
    path.write_text(yaml.safe_dump(document, sort_keys=False), encoding="utf-8")


def _edit_declaration(estate: Path, name: str, mutate) -> None:
    path = estate / "declarations" / "products" / name
    document = yaml.safe_load(path.read_text(encoding="utf-8"))
    mutate(document)
    path.write_text(yaml.safe_dump(document, sort_keys=False), encoding="utf-8")


def _summary(output: str, published_name: str) -> str:
    prefix = f"emitted {published_name}: "
    for line in output.splitlines():
        if line.startswith(prefix):
            return line
    raise AssertionError(f"no summary line for {published_name!r}:\n{output}")


def _artefacts_on_disk(estate: Path, domain: str, name: str) -> int:
    """Every file the route wrote for one product: its models (the product's
    own relation, its schema documentation and every private relation, all
    named from the product's model name) and its runtime manifest."""

    models = estate / "models" / "products" / domain
    written = [path for path in models.glob(f"{domain}__{name}*") if path.is_file()]
    manifest = estate / "manifests" / "products" / domain / f"{name}.json"
    assert manifest.is_file(), f"no runtime manifest written for {domain}.{name}"
    return len(written) + 1


def _engine_files() -> list[Path]:
    return [path for path in sorted(ENGINE_ROOT.rglob("*")) if path.is_file()]


# --------------------------------------------------------------------------- green


def test_the_fixture_vocabulary_appears_nowhere_in_the_engine() -> None:
    """The proof behind check 4 is only worth anything if the labels and the
    profile really are ones the engine has never seen. Read every file the
    engine ships and assert none of the three words is in any of them."""

    unknown = (*FIXTURE_LABELS, FIXTURE_PROFILE)
    hits: dict[str, list[str]] = {}
    for path in _engine_files():
        try:
            text = path.read_text(encoding="utf-8")
        except (UnicodeDecodeError, OSError):
            continue
        found = [word for word in unknown if word in text.lower()]
        if found:
            hits[path.relative_to(REPO_ROOT).as_posix()] = found
    assert not hits, f"the engine names the fixture's own estate vocabulary: {hits}"


def test_a_label_the_engine_has_never_seen_emits_with_no_engine_change() -> None:
    """Architecture check 4. The label resolves its profile, its translator
    table names an owner for every occurrence, and the product emits."""

    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))
        output = _emit(estate)

        summary = _summary(output, "catalogue.item_facts")
        assert "label=harmonised profile=derivation shape=declared" in summary, summary
        assert "adapters=duckdb,bigquery" in summary, summary
        assert (estate / "models/products/catalogue/catalogue__item_facts.sql").is_file(), (
            "the product under the never-seen label emitted no model"
        )


def test_an_estate_declared_profile_emits_and_enforces_its_own_composition() -> None:
    """The second half of estate configuration: a profile the engine does not
    ship. It is parsed by the same profile parser the reference profiles are,
    so its composition constrains its product exactly as theirs do -- proved
    here by a step the estate's own profile forbids failing closed."""

    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))
        output = _emit(estate)
        summary = _summary(output, "catalogue.item_feed")
        assert f"label=outbound profile={FIXTURE_PROFILE} shape=declared" in summary, summary

        def forbid(document: dict) -> None:
            document["steps"].insert(
                2,
                {
                    "pattern": "schema_transform",
                    "mapping": [{"from": "item_name", "to": "item_label", "type": "string"}],
                },
            )

        _edit_declaration(estate, "item_feed.yml", forbid)
        _fails(estate, "forbidden_pattern", "schema_transform", FIXTURE_PROFILE)


def test_the_summary_line_counts_the_artefacts_the_route_actually_emitted() -> None:
    """Owner ruling R6, plan decision D32, architecture check 13. The count is
    read back off the artefacts on disk, then the fixture is changed so one
    occurrence renders two fewer of them and the count follows."""

    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))
        output = _emit(estate)
        before = _artefacts_on_disk(estate, *FACTS)
        assert f"artefacts={before}" in _summary(output, "catalogue.item_facts"), (
            f"the summary count does not match the {before} artefacts on disk:\n{output}"
        )

        def drop_filtering(document: dict) -> None:
            document["steps"] = [
                step for step in document["steps"] if step["pattern"] != "data_filtering"
            ]

        _edit_declaration(estate, "item_facts.yml", drop_filtering)
        output = _emit(estate)
        after = _artefacts_on_disk(estate, *FACTS)
        assert after < before, (
            f"dropping the filtering occurrence emitted no fewer artefacts ({before} then {after})"
        )
        assert not (estate / "models/products/catalogue/catalogue__item_facts__filter_log.sql").exists(), (
            "the filter-log relation survived the occurrence that declared it"
        )
        assert f"artefacts={after}" in _summary(output, "catalogue.item_facts"), (
            f"the summary count did not follow the {after} artefacts on disk:\n{output}"
        )


def test_write_then_check_is_idempotent_on_the_estate_it_wrote() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))
        _emit(estate)
        second = _emit(estate)
        assert "generated 0 of" in second, second

        code, output = _run(estate, "--check")
        assert code == 0, output
        assert "0 problem(s)" in output, output


# --------------------------------------------------------------------------- red: adapters


def test_a_final_target_naming_an_undeclared_adapter_fails_closed() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))

        def mutate(document: dict) -> None:
            document["final_target"] = "postgres"

        _edit_estate(estate, mutate)
        _fails(estate, "final_target", "postgres", "duckdb")


def test_an_adapter_the_engine_does_not_carry_fails_closed_naming_it() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))

        def mutate(document: dict) -> None:
            document["adapters"]["postgres"] = {"kind": "deployment"}

        _edit_estate(estate, mutate)
        _fails(estate, "adapter 'postgres'", "estate.yml")


# --------------------------------------------------------------------------- red: labels and profiles


def test_a_label_with_no_profiles_fails_closed_naming_the_label() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))

        def mutate(document: dict) -> None:
            document["labels"]["harmonised"] = {}

        _edit_estate(estate, mutate)
        _fails(estate, "label 'harmonised'", "profiles")


def test_an_estate_profile_naming_a_pattern_the_registry_does_not_know_fails_closed() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))

        def mutate(document: dict) -> None:
            document["profiles"][FIXTURE_PROFILE]["mandatory"].append("data_publication")

        _edit_estate(estate, mutate)
        _fails(estate, FIXTURE_PROFILE, "unknown pattern", "data_publication")


def test_an_estate_profile_omitting_the_mandatory_contract_pattern_fails_closed() -> None:
    """Architecture section 5's one strengthening of the catalogue: every
    profile makes Data Contracts mandatory, because the contract is the only
    pipe. The failure names the label, the profile and the pattern."""

    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))

        def mutate(document: dict) -> None:
            profile = document["profiles"][FIXTURE_PROFILE]
            profile["mandatory"] = [
                pattern for pattern in profile["mandatory"] if pattern != "data_contracts"
            ]
            profile["optional"].append("data_contracts")

        _edit_estate(estate, mutate)
        _fails(estate, "label 'outbound'", FIXTURE_PROFILE, "data_contracts")


def test_an_estate_profile_reusing_a_reference_profile_name_fails_closed() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))

        def mutate(document: dict) -> None:
            document["profiles"]["serving"] = document["profiles"].pop(FIXTURE_PROFILE)
            document["profiles"]["serving"]["name"] = "serving"
            document["labels"]["outbound"]["profiles"] = ["serving"]

        _edit_estate(estate, mutate)
        _fails(estate, "estate profile 'serving'", "reference profile name")


def test_an_estate_profile_no_label_admits_fails_closed() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))

        def mutate(document: dict) -> None:
            document["labels"]["outbound"]["profiles"] = ["serving"]

        _edit_estate(estate, mutate)
        _fails(estate, f"estate profile {FIXTURE_PROFILE!r}", "no label admits it")


# --------------------------------------------------------------------------- red: the translator table


def test_a_table_entry_naming_an_unknown_translator_fails_closed() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))

        def mutate(document: dict) -> None:
            document["translators"]["harmonised"]["calculated_fields"] = "spark"

        _edit_estate(estate, mutate)
        _fails(estate, "label 'harmonised'", "spark", "calculated_fields")


def test_a_table_entry_naming_neither_a_pattern_nor_a_shape_fails_closed() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))

        def mutate(document: dict) -> None:
            document["translators"]["harmonised"]["data_validaton"] = "dbt"

        _edit_estate(estate, mutate)
        _fails(estate, "label 'harmonised'", "data_validaton", "registered shape")


def test_a_table_entry_naming_a_pattern_every_admitted_profile_forbids_fails_closed() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))

        def mutate(document: dict) -> None:
            document["translators"]["harmonised"]["batch_ingestion"] = "local-ingestion"

        _edit_estate(estate, mutate)
        _fails(estate, "label 'harmonised'", "batch_ingestion", "derivation")


def test_a_pattern_with_no_table_entry_fails_closed_at_routing() -> None:
    """Architecture check 12. A missing entry is the routing failure, not an
    estate-configuration one: it is the occurrence that needs an owner that
    goes red, naming the label, the pattern and the declared adapters."""

    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))

        def mutate(document: dict) -> None:
            del document["translators"]["harmonised"]["calculated_fields"]

        _edit_estate(estate, mutate)
        _fails(estate, "harmonised", "calculated_fields", "duckdb", "bigquery")


def test_a_table_naming_a_label_the_estate_does_not_declare_fails_closed() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))

        def mutate(document: dict) -> None:
            document["translators"]["curated"] = dict(document["translators"]["harmonised"])

        _edit_estate(estate, mutate)
        _fails(estate, "label 'curated'", "does not declare")


def test_a_label_with_no_table_at_all_fails_closed() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))

        def mutate(document: dict) -> None:
            del document["translators"]["outbound"]

        _edit_estate(estate, mutate)
        _fails(estate, "label 'outbound'", "has no translator table")


def test_a_shape_the_label_does_not_admit_fails_closed_naming_label_and_shape() -> None:
    """A shape is routed exactly as a pattern is: through the label's own
    table. A label whose table names no owner for the shape a product
    declares cannot render it, and says so."""

    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))

        def mutate(document: dict) -> None:
            del document["translators"]["harmonised"]["declared"]

        _edit_estate(estate, mutate)
        _fails(estate, "label 'harmonised'", "'declared'")


# --------------------------------------------------------------------------- red: the estate profiles block


def test_a_profiles_block_that_is_not_a_mapping_fails_closed() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))

        def mutate(document: dict) -> None:
            document["profiles"] = [FIXTURE_PROFILE]

        _edit_estate(estate, mutate)
        _fails(estate, "'profiles' must be a mapping", "composition document")


def test_a_profile_named_by_the_empty_string_fails_closed() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))

        def mutate(document: dict) -> None:
            document["profiles"] = {"": document["profiles"][FIXTURE_PROFILE]}

        _edit_estate(estate, mutate)
        _fails(estate, "every profile name must be a non-empty string")


def test_a_profile_whose_composition_is_not_a_document_fails_closed() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))

        def mutate(document: dict) -> None:
            document["profiles"][FIXTURE_PROFILE] = "the serving profile, but ours"

        _edit_estate(estate, mutate)
        _fails(estate, f"estate profile {FIXTURE_PROFILE!r}", "must carry a composition document")


# --------------------------------------------------------------------------- the structural stage

# Every fixture estate whose products the dbt route renders, two_shapes with
# its canonical interface views and dimensional tables included. graph_six is
# not here: it declares landing products whose schemas the ingestion side
# supplies, so the product route cannot emit it and the stage never runs there.
EMITTING_FIXTURES = (
    "patterns_seven",
    "entity_curation",
    "new_label",
    "two_shapes",
    "consolidating_two",
)

CLEAN_STAGE = "structural budgets: 0 offense(s) over 2 declared adapter(s) (duckdb, bigquery)"


def _named_estate(root: Path, name: str) -> Path:
    estate = root / "estate"
    shutil.copytree(REPO_ROOT / "tests" / "fixtures" / "estates" / name, estate)
    return estate


def test_the_structural_budgets_run_once_per_declared_adapter_on_every_emitting_fixture() -> None:
    """The gate stage runs the estate's declared budgets over the product tree
    for each adapter the estate declares, in write mode over what it wrote and
    in check mode over what is on disk."""

    for name in EMITTING_FIXTURES:
        with tempfile.TemporaryDirectory() as tmp:
            estate = _named_estate(Path(tmp), name)
            assert CLEAN_STAGE in _emit(estate), f"{name}: the stage did not run clean on write"
            code, output = _run(estate, "--check")
            assert code == 0, f"{name}: {output}"
            assert CLEAN_STAGE in output, f"{name}: the stage did not run clean in check mode"


def test_a_published_relation_rendered_as_a_view_fails_the_boundary_rule() -> None:
    """The interface-boundary rule governs interfaces. A relation a product
    publishes is one, so rendering it as a view outside every declared
    boundary fails closed naming the artefact, the rule and each adapter --
    while the translator-private views beside it, which are not interfaces,
    are not reported at all."""

    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))
        _emit(estate)
        published = estate / "models/products/catalogue/catalogue__item_facts.sql"
        published.write_text(
            published.read_text(encoding="utf-8").replace(
                "materialized='table'", "materialized='view'", 1
            ),
            encoding="utf-8",
        )

        code, output = _run(estate, "--check")
        assert code == 1, output
        boundary = [line for line in output.splitlines() if "view_boundary" in line]
        for adapter in ("duckdb", "bigquery"):
            assert any(
                f"[{adapter}] view_boundary: models/products/catalogue/catalogue__item_facts.sql"
                in line
                for line in boundary
            ), f"the boundary offense does not name {adapter}:\n{output}"
        assert len(boundary) == 2, f"expected one offense per declared adapter:\n{output}"


def test_a_declared_adapter_with_no_structural_budget_declaration_fails_closed() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))
        (estate / "declarations" / "targets" / "bigquery.yml").unlink()
        _fails(estate, "bigquery", "estate.yml", "structural budget declaration")


def test_a_missing_target_declaration_directory_fails_the_structural_stage_closed() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_estate(Path(tmp))
        shutil.rmtree(estate / "declarations" / "targets")
        _fails(estate, "duckdb", "bigquery", "no structural budget declaration")





# ------------------------------------------- the route hands over what it owns

# The consolidating estate's landing label gives every occurrence of a
# landing product, and its shape, to the ingestion runtime translator. The
# product route therefore emits nothing for those three products while
# still resolving their contracts for the products that consume them.
CONSOLIDATING_ESTATE = REPO_ROOT / "tests" / "fixtures" / "estates" / "consolidating_two"
LANDING_PRODUCTS = ("customer_feed_a", "customer_feed_b", "customer_flag_feed")


def _scratch_consolidating_estate(root: Path) -> Path:
    estate = root / "estate"
    shutil.copytree(CONSOLIDATING_ESTATE, estate)
    return estate


def test_the_route_emits_nothing_for_a_product_another_translator_renders() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_consolidating_estate(Path(tmp))
        output = _emit(estate)

        for name in LANDING_PRODUCTS:
            summary = _summary(output, f"crm.{name}")
            # One summary line per product, naming the owner the estate's
            # table gives its shape to and the artefacts this route wrote
            # for it, which is none.
            assert "label=landed profile=landing shape=declared" in summary, summary
            assert "owner=local-ingestion" in summary, summary
            assert "artefacts=0" in summary, summary
            written = sorted(
                path.name
                for path in (estate / "models" / "products" / "crm").glob(f"*{name}*")
            )
            assert written == [], written
            assert not (estate / "manifests" / "products" / "crm" / f"{name}.json").is_file()

        # Their contracts still resolve: the products that consume them are
        # emitted from the schemas those landing products publish.
        assert _summary(output, "crm.customer_a").endswith("owner=dbt adapters=duckdb,bigquery artefacts=7")
        opening = (estate / "models/products/crm/crm__customer_a__checked.sql").read_text(
            encoding="utf-8"
        )
        assert 'ref("crm__customer_feed_a")' in opening, opening


def test_moving_the_shape_to_dbt_hands_the_landing_products_to_dbt() -> None:
    """The table decides which translator renders a product, and nothing
    else does. Giving the landing label's shape and its rendering patterns
    to dbt makes dbt render those three products, with no engine change:
    each becomes a model over the relation it lands, and the consumers go
    on reading the relation the product publishes.

    A landing product's binding follows the same table. Where the
    ingestion runtime lands the relation, the binding names the relation
    the product publishes; where dbt renders it, the binding names the
    relation that model reads, so the seeds standing in for the landed
    relations are renamed with the table."""

    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_consolidating_estate(Path(tmp))

        def hand_the_landing_label_to_dbt(block: dict) -> None:
            block["translators"]["landed"].update(
                declared="dbt",
                data_validation="dbt",
                data_publish="dbt",
                checkpoint_retries="dbt",
            )

        _edit_estate(estate, hand_the_landing_label_to_dbt)
        for name in LANDING_PRODUCTS:
            (estate / "seeds" / f"crm__{name}.csv").rename(estate / "seeds" / f"raw_{name}.csv")
            _edit_declaration(
                estate,
                f"{name}.yml",
                lambda document, product=name: document["sources"][0]["fixture"].update(
                    relation=f"raw_{product}"
                ),
            )

        output = _emit(estate)
        for name in LANDING_PRODUCTS:
            summary = _summary(output, f"crm.{name}")
            assert "owner=dbt" in summary, summary
            assert "artefacts=0" not in summary, summary
            assert (estate / "models/products/crm" / f"crm__{name}.sql").is_file(), summary
            assert (estate / "manifests/products/crm" / f"{name}.json").is_file(), summary
            opening = (
                estate / "models/products/crm" / f"crm__{name}__checked.sql"
            ).read_text(encoding="utf-8")
            assert f'ref("raw_{name}")' in opening, opening
        # batch_ingestion stays with the ingestion runtime, which is the
        # split architecture section 9 describes: several translators serve
        # one product when the table says so.
        assert "owner=dbt" in _summary(output, "crm.customer_union")


def test_a_landing_binding_follows_the_translator_the_table_names() -> None:
    """The same estate, the same binding, two owners and two rulings: with
    the shape left where it is, a binding naming anything but the relation
    the product publishes fails closed; with the shape given to dbt, a
    binding naming that same relation fails closed instead."""

    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_consolidating_estate(Path(tmp))
        _edit_declaration(
            estate,
            "customer_feed_a.yml",
            lambda document: document["sources"][0]["fixture"].update(relation="raw_feed_a"),
        )
        _fails(estate, "crm.customer_feed_a", "raw_feed_a", "crm__customer_feed_a", "local-ingestion")

    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_consolidating_estate(Path(tmp))
        _edit_estate(
            estate,
            lambda block: block["translators"]["landed"].update(
                declared="dbt",
                data_validation="dbt",
                data_publish="dbt",
                checkpoint_retries="dbt",
            ),
        )
        _fails(estate, "crm.customer_feed_a", "crm__customer_feed_a", "dbt", "reads")


def test_a_step_routed_to_dbt_in_a_product_dbt_does_not_render_fails_closed() -> None:
    """Two translators cannot own one relation. A label that leaves a
    product's shape with one translator and hands one of its occurrences to
    another names both, rather than silently dropping the occurrence."""

    with tempfile.TemporaryDirectory() as tmp:
        estate = _scratch_consolidating_estate(Path(tmp))
        _edit_estate(
            estate,
            lambda block: block["translators"]["landed"].update(data_validation="dbt"),
        )
        _fails(
            estate,
            "crm.customer_feed_a",
            "local-ingestion",
            "dbt",
            "renders every occurrence of it",
        )


TESTS = [
    test_the_fixture_vocabulary_appears_nowhere_in_the_engine,
    test_a_label_the_engine_has_never_seen_emits_with_no_engine_change,
    test_an_estate_declared_profile_emits_and_enforces_its_own_composition,
    test_the_summary_line_counts_the_artefacts_the_route_actually_emitted,
    test_write_then_check_is_idempotent_on_the_estate_it_wrote,
    test_a_final_target_naming_an_undeclared_adapter_fails_closed,
    test_an_adapter_the_engine_does_not_carry_fails_closed_naming_it,
    test_a_label_with_no_profiles_fails_closed_naming_the_label,
    test_an_estate_profile_naming_a_pattern_the_registry_does_not_know_fails_closed,
    test_an_estate_profile_omitting_the_mandatory_contract_pattern_fails_closed,
    test_an_estate_profile_reusing_a_reference_profile_name_fails_closed,
    test_an_estate_profile_no_label_admits_fails_closed,
    test_a_table_entry_naming_an_unknown_translator_fails_closed,
    test_a_table_entry_naming_neither_a_pattern_nor_a_shape_fails_closed,
    test_a_table_entry_naming_a_pattern_every_admitted_profile_forbids_fails_closed,
    test_a_pattern_with_no_table_entry_fails_closed_at_routing,
    test_a_table_naming_a_label_the_estate_does_not_declare_fails_closed,
    test_a_label_with_no_table_at_all_fails_closed,
    test_a_shape_the_label_does_not_admit_fails_closed_naming_label_and_shape,
    test_a_profiles_block_that_is_not_a_mapping_fails_closed,
    test_a_profile_named_by_the_empty_string_fails_closed,
    test_a_profile_whose_composition_is_not_a_document_fails_closed,
    test_the_structural_budgets_run_once_per_declared_adapter_on_every_emitting_fixture,
    test_a_published_relation_rendered_as_a_view_fails_the_boundary_rule,
    test_a_declared_adapter_with_no_structural_budget_declaration_fails_closed,
    test_a_missing_target_declaration_directory_fails_the_structural_stage_closed,
    test_the_route_emits_nothing_for_a_product_another_translator_renders,
    test_moving_the_shape_to_dbt_hands_the_landing_products_to_dbt,
    test_a_landing_binding_follows_the_translator_the_table_names,
    test_a_step_routed_to_dbt_in_a_product_dbt_does_not_render_fails_closed,
]


def main() -> int:
    failures = 0
    for test in TESTS:
        name = test.__name__
        try:
            test()
        except Exception:  # noqa: BLE001 - report and continue, the exit code carries the signal
            failures += 1
            print(f"FAIL {name}")
            traceback.print_exc()
        else:
            print(f"PASS {name}")
    total = len(TESTS)
    print(f"\n{total - failures}/{total} passed")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
