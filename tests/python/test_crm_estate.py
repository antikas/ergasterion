"""End-to-end self-test of the e-commerce estate, driven through the real
commands against the repository's own estate (architecture sections 4 to 9,
11 to 14).

Same plain assert-and-report convention as the rest of this repo (no pytest
in the .venv): each test_* raises AssertionError on failure, main() runs them
all and reports PASS/FAIL.

Nothing here is a fixture estate. Every command runs against the
declarations, rules, macros and delivered relations the repository carries,
and the executed lane builds the emitted product tree on DuckDB into a
database of its own, so a repeated run is independent of any other lane.

Covers:
  - every declaration validates and passes the neutrality gate, through
    `ergasterion validate`;
  - `ergasterion emit-products` prints one summary line per product naming
    its label, profile, shape, owner and adapters, with every occurrence
    owned; re-emits byte-identically; reports no drift under --check; and
    the structural budgets pass on both declared adapters;
  - `ergasterion product-graph` emits the estate graph, and `ergasterion
    contracts` and `ergasterion odps` emit a contract and a descriptor for
    every product, with an ODCS document per published relation;
  - the emitted tree reads no hand-authored model: every read is another
    product's relation or a delivered relation a landing product lands, and
    no product but that landing product reads a delivered relation;
  - no Data Vault relation is emitted for this estate;
  - the executed DuckDB build of the product tree passes with every
    generated test and every known-answer assertion under tests/ecommerce,
    and a second build changes nothing;
  - the served fact and a served dimension carry the types their
    declarations state, read from the adapter's own catalogue;
  - how many named rules the estate references and how many of them are
    adapter-specific, derived from the catalogue, the declarations and the
    macros rather than stated here.

Usage:
    python tests/python/test_crm_estate.py
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import subprocess
import sys
import traceback
from pathlib import Path

import yaml

if __package__ in (None, ""):
    import os as _os
    import sys as _sys

    _sys.path.insert(
        0, _os.path.dirname(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__))))
    )

REPO_ROOT = Path(__file__).resolve().parents[2]
PRODUCTS_DIR = REPO_ROOT / "declarations" / "products"
MODELS_DIR = REPO_ROOT / "models" / "products"
MANIFESTS_DIR = REPO_ROOT / "manifests" / "products"
CONTRACTS_DIR = REPO_ROOT / "contracts" / "products"
GRAPH_DIR = REPO_ROOT / "graphs" / "products"
ASSERTIONS_DIR = REPO_ROOT / "tests" / "ecommerce"
RULE_CATALOGUE = REPO_ROOT / "rules" / "ecommerce.yml"
RULE_MACROS = REPO_ROOT / "macros" / "crm_rules.sql"

# The database this test's own build writes, so it never shares a file with
# another lane's build of the same repository.
DUCKDB_PATH = "target/crm_estate.duckdb"

LANDING_LABEL = "landed"

# The domain this estate owns. Another domain may share the repository.
DOMAIN = "ecommerce"


# --------------------------------------------------------------------------- harness


def _declarations() -> dict:
    """Every product declaration, keyed by published name."""

    entries: dict = {}
    for path in sorted(PRODUCTS_DIR.rglob("*.yml")):
        document = yaml.safe_load(path.read_text(encoding="utf-8"))
        product = document["product"]
        entries[f"{product['domain']}.{product['name']}"] = document
    return entries


def _run(*arguments: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, "-m", "ergasterion", *arguments],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )


def _command(*arguments: str) -> str:
    result = _run(*arguments)
    assert result.returncode == 0, (
        f"ergasterion {' '.join(arguments)} exited {result.returncode}:\n"
        + result.stdout
        + result.stderr
    )
    return result.stdout + result.stderr


def _emitted_digest() -> str:
    digest = hashlib.sha256()
    for path in sorted(list(MODELS_DIR.rglob("*")) + list(MANIFESTS_DIR.rglob("*"))):
        if path.is_file():
            digest.update(path.relative_to(REPO_ROOT).as_posix().encode("utf-8"))
            digest.update(path.read_bytes())
    return digest.hexdigest()


def _dbt(*arguments: str) -> subprocess.CompletedProcess:
    executable = Path(sys.executable).resolve().parent / ("dbt.exe" if os.name == "nt" else "dbt")
    assert executable.is_file(), (
        f"the executed lane needs the dbt executable beside the interpreter: {executable}"
    )
    environment = dict(os.environ, DPF_DUCKDB_PATH=DUCKDB_PATH)
    return subprocess.run(
        [str(executable), *arguments, "--profiles-dir", "profiles", "-t", "duckdb"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        env=environment,
    )


def _dbt_build() -> str:
    # The product tree, the delivered relations it opens from, and the
    # known-answer assertions over it. The legacy tree is outside this
    # selection: the repository's own live gate builds that one.
    # Cautious indirect selection: a test runs only when every relation it
    # reads is inside this selection, so the legacy tree's own singular tests
    # (which also read the delivered seeds) stay with the lane that owns them.
    result = _dbt(
        "build",
        "--select",
        "+path:models/products",
        "path:tests/ecommerce",
        "--indirect-selection=cautious",
    )
    output = result.stdout + result.stderr
    assert result.returncode == 0, output[-6000:]
    return output


def _query(statement: str):
    import duckdb

    connection = duckdb.connect(str(REPO_ROOT / DUCKDB_PATH), read_only=True)
    try:
        return connection.execute(statement).fetchall()
    finally:
        connection.close()


def _relation_digest(relations) -> str:
    digest = hashlib.sha256()
    for relation in relations:
        rows = _query(f'select * from "{relation}" order by all')
        digest.update(relation.encode("utf-8"))
        digest.update(repr(rows).encode("utf-8"))
    return digest.hexdigest()


_BUILT = {"done": False}


def _ensure_built() -> None:
    if not _BUILT["done"]:
        _command("emit-products")
        _dbt_build()
        _BUILT["done"] = True


# --------------------------------------------------------------------------- validation and emission


def test_every_declaration_validates_and_passes_the_neutrality_gate() -> None:
    output = _command("validate")
    assert f"{len(_declarations())} product declaration(s) valid" in output, output


def test_the_command_emits_one_owned_summary_line_per_product() -> None:
    output = _command("emit-products")
    summaries = [line for line in output.splitlines() if line.startswith("emitted ")]
    declared = _declarations()
    assert len(summaries) == len(declared), (len(summaries), len(declared))
    pattern = re.compile(
        r"^emitted (?P<product>[^:]+): label=(?P<label>\S+) profile=(?P<profile>\S+) "
        r"shape=(?P<shape>\S+) owner=(?P<owner>\S+) adapters=(?P<adapters>\S+) "
        r"artefacts=(?P<artefacts>\d+)$"
    )
    seen = set()
    for line in summaries:
        match = pattern.match(line)
        assert match is not None, line
        product = match.group("product")
        assert product in declared, product
        declaration = declared[product]
        assert match.group("label") == declaration["product"]["layer"], line
        assert match.group("shape") == declaration["target"]["shape"], line
        assert match.group("owner") in ("dbt", "local-ingestion", "publication"), line
        assert match.group("adapters") == "duckdb,bigquery", line
        seen.add(product)
    assert seen == set(declared), sorted(set(declared) ^ seen)
    assert "structural budgets: 0 offense(s) over 2 declared adapter(s)" in output, output


def test_re_emission_is_byte_identical_and_check_reports_no_drift() -> None:
    _command("emit-products")
    first = _emitted_digest()
    output = _command("emit-products")
    assert _emitted_digest() == first, "a second emission changed the product tree"
    assert re.search(r"generated 0 of \d+ file\(s\)", output), output
    checked = _command("emit-products", "--check")
    assert re.search(r"checked \d+ generated file\(s\); 0 problem\(s\)", checked), checked
    assert "structural budgets: 0 offense(s) over 2 declared adapter(s)" in checked, checked
    assert _emitted_digest() == first, "check mode wrote to the product tree"


def test_the_graph_the_contracts_and_the_descriptors_are_emitted() -> None:
    graph = _command("product-graph")
    declared_products = _declarations()
    for published_name in sorted(declared_products):
        assert f"  {published_name}: layer=" in graph, published_name
    for name in (
        "product-graph.json",
        "product-nodes.csv",
        "product-edges.csv",
        "product-field-lineage.csv",
        "product-validations.csv",
    ):
        assert (GRAPH_DIR / name).is_file(), name
    document = json.loads((GRAPH_DIR / "product-graph.json").read_text(encoding="utf-8"))
    assert set(document["order"]) == set(declared_products), sorted(
        set(document["order"]) ^ set(declared_products)
    )
    assert document["counts"]["products"] == len(declared_products), document["counts"]
    assert document["counts"]["edges"] > 0 and document["counts"]["field_lineage"] > 0, (
        document["counts"]
    )
    lineage = (GRAPH_DIR / "product-field-lineage.csv").read_text(encoding="utf-8")
    assert "customer_business_key" in lineage, "no calculated-field lineage was emitted"

    declared = declared_products
    contracts = _command("contracts")
    assert f"{len(declared)} declared product contract(s) valid" in contracts, contracts
    descriptors = _command("odps")
    assert f"{len(declared)} declared product descriptor(s) valid" in descriptors, descriptors

    for published_name in sorted(declared):
        domain, name = published_name.split(".", 1)
        base = CONTRACTS_DIR / domain / name
        assert (base / "contract.json").is_file(), published_name
        assert (base / "contract-compliance.json").is_file(), published_name
        assert (base / f"{name}.odps.yml").is_file(), published_name
        contract = json.loads((base / "contract.json").read_text(encoding="utf-8"))
        relations = [entry["name"].rsplit(".", 1)[-1] for entry in contract["relations"]]
        assert relations, published_name
        for relation in relations:
            assert (base / f"{relation}.odcs.yml").is_file(), (published_name, relation)


# --------------------------------------------------------------------------- what the tree reads


def _model_reads() -> dict:
    """Every read each emitted model makes, keyed by model name."""

    reads: dict = {}
    for path in sorted(MODELS_DIR.rglob("*.sql")):
        text = path.read_text(encoding="utf-8")
        reads[path.stem] = set(re.findall(r'\{\{\s*ref\("([^"]+)"\)\s*\}\}', text))
    return reads


def _landed_relations() -> dict:
    """The delivered relation each landing product lands, keyed by relation
    name and valued by the model of the product that lands it."""

    owned: dict = {}
    for published_name, document in _declarations().items():
        if document["product"].get("layer") != LANDING_LABEL:
            continue
        domain, name = published_name.split(".", 1)
        for source in document.get("sources") or []:
            binding = source.get("fixture")
            if binding is not None:
                owned[str(binding["relation"])] = f"{domain}__{name}"
    return owned


def test_the_emitted_tree_reads_no_hand_authored_model() -> None:
    reads = _model_reads()
    emitted = set(reads)
    delivered = _landed_relations()
    landing_products = [
        published
        for published, document in _declarations().items()
        if document["product"].get("layer") == LANDING_LABEL
    ]
    assert len(delivered) == len(landing_products), (
        "every landing product binds exactly one delivered relation: "
        f"{len(landing_products)} landing product(s), {len(delivered)} bound relation(s)"
    )
    for model in sorted(reads):
        for target in sorted(reads[model]):
            assert target in emitted or target in delivered, (
                f"{model} reads {target!r}, which is neither a relation this estate emits "
                "nor a delivered relation a landing product lands"
            )


def test_only_a_landing_product_reads_the_relation_it_lands() -> None:
    delivered = _landed_relations()
    reads = _model_reads()
    for model in sorted(reads):
        for relation in sorted(delivered):
            if relation not in reads[model]:
                continue
            owner = delivered[relation]
            assert model.startswith(owner), (
                f"{model} reads the delivered relation {relation!r}, which only the landing "
                f"product {owner!r} may read (owner ruling R5)"
            )


def test_every_view_in_the_product_tree_is_private_or_an_interface() -> None:
    """The estate declares one interface boundary, the canonical shape's own path
    (declarations/targets/interfaces.yml). Every other view under the product tree has
    to be a relation the route itself registered as translator-private, which the
    standalone gate reads back out of the manifests the route wrote. A published
    relation rendered as a view anywhere else fails here and in that gate."""

    from ergasterion import emit_products as ep
    from ergasterion.estate import EstateContext
    from ergasterion.shapes.canonical import INTERFACE_PATH

    emission = ep.generate(EstateContext.default())
    private = emission.private_models
    assert private, "the route registered no private relation"
    interface_root = f"{ep.MODELS_ROOT}/{INTERFACE_PATH}"

    unaccounted = []
    for path in sorted(MODELS_DIR.rglob("*.sql")):
        if "materialized='view'" not in path.read_text(encoding="utf-8"):
            continue
        relative = path.relative_to(REPO_ROOT).as_posix()
        if path.stem in private:
            continue
        if relative.startswith(f"{interface_root}/"):
            continue
        unaccounted.append(relative)
    assert not unaccounted, (
        "these views are neither translator-private nor interface artefacts: " + ", ".join(unaccounted)
    )


def _estate_macros() -> set:
    """Every macro name the estate's own macros/ defines."""

    names = set()
    for path in sorted((REPO_ROOT / "macros").glob("*.sql")):
        text = path.read_text(encoding="utf-8")
        names.update(re.findall(r"\{%-?\s*macro\s+([a-z_0-9]+)", text))
    return names


def test_every_model_under_models_carries_the_generated_marker() -> None:
    """Nothing under models/ is hand-authored. Every file there is written by the
    product route and carries the marker that says so, which is also what the dialect
    gate reads to decide what it lints."""

    from ergasterion.dialect_lint import GENERATED_MARKER

    tree = REPO_ROOT / "models"
    files = sorted(path for path in tree.rglob("*") if path.is_file())
    assert files, f"{tree} carries no file at all"
    unmarked = [
        path.relative_to(REPO_ROOT).as_posix()
        for path in files
        if GENERATED_MARKER not in path.read_text(encoding="utf-8", errors="replace")
    ]
    assert not unmarked, "hand-authored file(s) under models/: " + ", ".join(unmarked)


def test_every_seed_is_read_by_a_landing_product_or_a_kept_hook() -> None:
    """A seed that nothing reads is dead fixture data. Every CSV under seeds/ is either
    the delivered relation a landing product lands, or the fixture source the
    decision-log provisioning hook merges into its append-only log (plan decision D17).
    Both arms are read off what the estate declares, never off a name."""

    seeds = {path.stem for path in sorted((REPO_ROOT / "seeds").glob("*.csv"))}
    assert seeds, "the estate carries no seed"

    landed = set(_landed_relations())

    project = yaml.safe_load((REPO_ROOT / "dbt_project.yml").read_text(encoding="utf-8"))
    hook_text = "\n".join(project.get("on-run-start") or [])
    seed_config = project.get("seeds", {}).get("ergasterion", {})
    hook_bound = {
        name
        for name, entry in seed_config.items()
        if not name.startswith("+") and isinstance(entry, dict) and "+post-hook" in entry
    }
    assert hook_bound, "no seed is bound to a provisioning post-hook, so D17's arm is untested"
    assert "dpf_ensure_deal_decision_log_table" in hook_text, hook_text

    orphans = sorted(seeds - landed - hook_bound)
    assert not orphans, (
        "seed(s) read by neither a landing product nor a kept provisioning hook: "
        + ", ".join(orphans)
    )


def test_every_assertion_the_estate_ships_reads_a_relation_it_renders() -> None:
    """Every dbt singular test under tests/ reads something this estate actually has:
    a relation the route renders, a seed it carries, or a source it declares. An
    assertion left behind pointing at a removed relation would compile against nothing
    and quietly stop asserting."""

    from ergasterion import emit_products as ep
    from ergasterion.estate import EstateContext

    emission = ep.generate(EstateContext.default())
    rendered = {path.stem for path in emission.files if path.suffix == ".sql"}
    seeds = {path.stem for path in (REPO_ROOT / "seeds").glob("*.csv")}

    sources = set()
    for path in sorted((REPO_ROOT / "seeds").glob("*.yml")):
        document = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        for group in document.get("sources") or []:
            for table in group.get("tables") or []:
                sources.add((group["name"], table["name"]))

    tests_root = REPO_ROOT / "tests"
    excluded = {"python", "fixtures"}
    assertions = [
        path
        for path in sorted(tests_root.rglob("*.sql"))
        if path.relative_to(tests_root).parts[0] not in excluded
    ]
    assert assertions, "the estate ships no assertion"

    for path in assertions:
        text = path.read_text(encoding="utf-8")
        refs = set(re.findall(r"""ref\(\s*['"]([^'"]+)['"]""", text))
        declared = set(re.findall(r"""source\(\s*['"]([^'"]+)['"]\s*,\s*['"]([^'"]+)['"]""", text))
        macros = set(re.findall(r"\{[{%]-?\s*(dpf_[a-z_0-9]+)", text))
        assert refs or declared or macros, (
            f"{path.name} reads no relation and calls no macro, so it asserts nothing"
        )
        unknown = sorted(name for name in macros if name not in _estate_macros())
        assert not unknown, f"{path.name} calls macro(s) this estate does not ship: {unknown}"
        stale = sorted(name for name in refs if name not in rendered and name not in seeds)
        assert not stale, f"{path.name} reads relation(s) this estate does not render: {stale}"
        undeclared = sorted(pair for pair in declared if pair not in sources)
        assert not undeclared, f"{path.name} reads source(s) this estate does not declare: {undeclared}"


def test_no_data_vault_relation_is_emitted_for_the_ecommerce_products() -> None:
    """The e-commerce estate declares the `declared`, `canonical` and `dimensional`
    shapes and no Data Vault (architecture check 2). Another domain in the same
    repository may declare one, so this is scoped to the products this estate
    owns and to the models emitted for them."""

    owned = {
        published
        for published, document in _declarations().items()
        if document["product"]["domain"] == DOMAIN
    }
    assert owned, "the estate declares no product"
    for published_name in sorted(owned):
        assert _declarations()[published_name]["target"]["shape"] != "data_vault", published_name
    forbidden = re.compile(r"__(hub|link|sat|pit|bridge)_")
    for path in sorted(MODELS_DIR.rglob("*.sql")):
        if not path.stem.startswith(f"{DOMAIN}__"):
            continue
        assert not forbidden.search(path.stem), path.name


# --------------------------------------------------------------------------- the executed lane


def test_the_estate_builds_on_duckdb_with_every_generated_test_and_assertion() -> None:
    _command("emit-products")
    output = _dbt_build()
    _BUILT["done"] = True
    assert "Completed successfully" in output, output[-4000:]
    assertions = sorted(ASSERTIONS_DIR.glob("*.sql"))
    assert len(assertions) >= 12, len(assertions)
    for path in assertions:
        assert f"PASS {path.stem}" in output, (
            f"the known-answer assertion {path.stem} did not run or did not pass"
        )


def test_a_second_unchanged_build_changes_nothing() -> None:
    _ensure_built()
    published = (
        "ecommerce__customer",
        "ecommerce__product",
        "ecommerce__order",
        "ecommerce__order_line",
        "ecommerce__order_star__fact_order_line",
        "ecommerce__order_star__dim_customer_segment",
        "ecommerce__order_summary",
    )
    before = _relation_digest(published)
    _dbt_build()
    assert _relation_digest(published) == before, (
        "a second unchanged build changed a published relation"
    )


def test_every_declared_type_reaches_the_built_relations() -> None:
    _ensure_built()
    from ergasterion.framework.adapters import load_adapter_conventions

    mapping = load_adapter_conventions("duckdb").type_mapping
    star = _declarations()["ecommerce.order_star"]
    computed = {
        field["name"]: field["type"]
        for step in star["steps"]
        if step["pattern"] == "calculated_fields"
        for field in step["fields"]
    }
    from ergasterion.translators.dbt_patterns import parse_gate
    from ergasterion.translators.dbt_patterns import sql as sql_mod

    tokens = sql_mod.SCALAR_TYPE_TOKENS

    def physical(neutral) -> str:
        """The type the reference adapter reports for one declared neutral
        type. The adapter is asked how it spells the type its own conventions
        map to, so an alias is compared as the adapter reports it."""

        if isinstance(neutral, dict):
            base = mapping[parse_gate.DECIMAL_TYPE_TOKEN].split("(")[0]
            spelled = f"{base}({neutral['precision']},{neutral['scale']})"
        else:
            spelled = mapping[tokens[str(neutral)]]
        return str(_query(f"select typeof(cast(null as {spelled}))")[0][0])

    def built(relation: str) -> dict:
        rows = _query(
            "select column_name, data_type from information_schema.columns "
            f"where table_name = '{relation}' order by ordinal_position"
        )
        return {str(name): str(kind) for name, kind in rows}

    decimal_12_2 = {"name": "decimal", "precision": 12, "scale": 2}
    fact = built("ecommerce__order_star__fact_order_line")
    for column, neutral in (
        ("order_id", "string"),
        ("line_number", "integer"),
        ("line_quantity", "integer"),
        ("line_revenue", decimal_12_2),
        ("line_order_date", "date"),
    ):
        assert fact[column] == physical(neutral), (column, fact[column], physical(neutral))
    assert fact["line_list_value"] == physical(computed["line_list_value"]), fact["line_list_value"]

    dimension = built("ecommerce__order_star__dim_product")
    assert dimension["product_code"] == physical("string"), dimension["product_code"]
    assert dimension["product_list_price"] == physical(decimal_12_2), dimension["product_list_price"]


# --------------------------------------------------------------------------- the named rules


def _named_rules():
    """This estate's rule catalogue, the rules its own products reference, and
    the referenced rules whose implementation dispatches per adapter. All
    three are derived from the files, never restated here."""

    catalogue = yaml.safe_load(RULE_CATALOGUE.read_text(encoding="utf-8"))
    macros = RULE_MACROS.read_text(encoding="utf-8")
    referenced = set()
    # Scoped to the products this estate owns. Another domain in the same
    # repository declares its own catalogue and its own macros, and its rules
    # are that estate's test to make.
    for document in _declarations().values():
        if document["product"]["domain"] != DOMAIN:
            continue
        for step in document.get("steps") or []:
            if step["pattern"] == "calculated_fields":
                for field in step.get("fields") or []:
                    if field.get("rule"):
                        referenced.add(str(field["rule"]))
            elif step["pattern"] == "data_curation":
                scoring = (step.get("resolution") or {}).get("scoring") or {}
                if scoring.get("rule"):
                    referenced.add(str(scoring["rule"]))
    adapter_specific = []
    for rule in catalogue["rules"]:
        if rule["name"] not in referenced:
            continue
        macro_names = {entry["macro"] for entry in rule["implementations"]}
        assert len(macro_names) == 1, rule["name"]
        macro = macro_names.pop()
        if re.search(r"adapter\.dispatch\(\s*'" + re.escape(macro) + r"'", macros):
            adapter_specific.append(rule["name"])
    return catalogue, referenced, sorted(adapter_specific)


def test_every_named_rule_the_estate_references_resolves_for_both_adapters() -> None:
    from ergasterion.estate import EstateContext
    from ergasterion.framework import rules as rules_mod

    ctx = EstateContext.default()
    catalogue, referenced, adapter_specific = _named_rules()
    declared = {rule["name"] for rule in catalogue["rules"]}
    assert referenced, "the estate references no named rule"
    assert referenced <= declared, sorted(referenced - declared)

    loaded = rules_mod.load_rule_catalogue(estate_dir=ctx.root / "rules")
    implementations = {**rules_mod.REFERENCE_IMPLEMENTATIONS, **loaded.implementations}
    macros = RULE_MACROS.read_text(encoding="utf-8")
    for name in sorted(referenced):
        macro = rules_mod.dbt_implementation_macro(
            name, implementations=implementations, adapters=("duckdb", "bigquery")
        )
        assert f"macro {macro}(" in macros, (name, macro)

    print(
        f"    named rules referenced: {len(referenced)} of {len(declared)} declared; "
        f"adapter-specific: {len(adapter_specific)} "
        f"({', '.join(adapter_specific) if adapter_specific else 'none'})"
    )


# --------------------------------------------------------------------------- runner


TESTS = (
    test_every_declaration_validates_and_passes_the_neutrality_gate,
    test_the_command_emits_one_owned_summary_line_per_product,
    test_re_emission_is_byte_identical_and_check_reports_no_drift,
    test_the_graph_the_contracts_and_the_descriptors_are_emitted,
    test_the_emitted_tree_reads_no_hand_authored_model,
    test_only_a_landing_product_reads_the_relation_it_lands,
    test_every_view_in_the_product_tree_is_private_or_an_interface,
    test_no_data_vault_relation_is_emitted_for_the_ecommerce_products,
    test_every_model_under_models_carries_the_generated_marker,
    test_every_seed_is_read_by_a_landing_product_or_a_kept_hook,
    test_every_assertion_the_estate_ships_reads_a_relation_it_renders,
    test_the_estate_builds_on_duckdb_with_every_generated_test_and_assertion,
    test_a_second_unchanged_build_changes_nothing,
    test_every_declared_type_reaches_the_built_relations,
    test_every_named_rule_the_estate_references_resolves_for_both_adapters,
)


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
    print(f"{len(TESTS) - failures}/{len(TESTS)} passed")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
