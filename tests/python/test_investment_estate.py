"""End-to-end self-test of the investment estate, driven through the real
commands against the repository's own estate (architecture sections 4 to 9,
11 to 14).

Same plain assert-and-report convention as the rest of this repo (no pytest in
the .venv): each test_* raises AssertionError on failure, main() runs them all
and reports PASS/FAIL.

Nothing here is a fixture estate. Every command runs against the declarations,
rules, macros and delivered relations the repository carries, and the executed
lane builds the emitted product tree on DuckDB into a database of its own, so a
repeated run is independent of any other lane.

Covers:
  - every declaration validates and passes the neutrality gate;
  - `ergasterion emit-products` prints one owned summary line per investment
    product, re-emits byte-identically and reports no drift under --check, with
    the structural budgets passing on both declared adapters;
  - the five Data Vault products publish every relation their shape renders, and
    every consumer of one names the relation it reads (owner ruling R4);
  - the 21 delivered source extracts each land through exactly one product, and
    no other product reads a delivered relation;
  - the canonical interfaces publish only names the five source declarations map
    to the reference model, and every entity those declarations map is published;
  - the executed DuckDB build of the product tree passes with every generated
    test and every known-answer assertion under tests/investment, and a second
    build changes nothing;
  - the resolution evidence carries the seeded near-duplicate review case;
  - the estate references its named rules and every one resolves for both
    declared adapters.

Usage:
    python tests/python/test_investment_estate.py
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
ASSERTIONS_DIR = REPO_ROOT / "tests" / "investment"
RULE_CATALOGUE = REPO_ROOT / "rules" / "investment.yml"
RULE_MACROS = REPO_ROOT / "macros" / "investment_rules.sql"

# The database this test's own build writes, so it never shares a file with
# another lane's build of the same repository.
DUCKDB_PATH = "target/investment_estate.duckdb"

DOMAIN = "investment"
LANDING_LABEL = "landed"

# The five delivered source declarations. They are landing inputs and the record
# of which delivered column carries which reference-model attribute; no route
# reads them (owner ruling R9).
SOURCE_DECLARATIONS = ("chrono", "meridex", "origo", "portiq", "vantora")

# The curated entities the estate models as vaults, and the canonical interface
# that publishes each one under the reference model's names.
VAULT_PRODUCTS = (
    "investment.fund_vault",
    "investment.gp_vault",
    "investment.portfolio_company_vault",
    "investment.legal_vehicle_vault",
    "investment.deal_vault",
)
CANONICAL_ENTITIES = {
    "fund": "investment.fund_canonical",
    "gp": "investment.gp_canonical",
    "portfolio_company": "investment.portfolio_company_canonical",
    "legal_vehicle": "investment.legal_vehicle_canonical",
    "deal": "investment.deal_canonical",
}


# --------------------------------------------------------------------------- harness


def _declarations() -> dict:
    """Every product declaration in the repository, keyed by published name."""

    entries: dict = {}
    for path in sorted(PRODUCTS_DIR.rglob("*.yml")):
        document = yaml.safe_load(path.read_text(encoding="utf-8"))
        product = document["product"]
        entries[f"{product['domain']}.{product['name']}"] = document
    return entries


def _owned() -> dict:
    """The products this estate owns: the investment domain and every reference
    product an investment product reads, directly or through another."""

    declared = _declarations()
    owned = {
        published: document
        for published, document in declared.items()
        if document["product"]["domain"] == DOMAIN
    }
    frontier = list(owned)
    while frontier:
        published = frontier.pop()
        for source in declared[published].get("sources") or []:
            if source.get("kind") == "fixture":
                continue
            upstream = str(source["contract"]).split("@", 1)[0]
            if upstream in declared and upstream not in owned:
                owned[upstream] = declared[upstream]
                frontier.append(upstream)
        for step in declared[published].get("steps") or []:
            for lookup in step.get("lookups") or []:
                upstream = str(lookup["contract"]).split("@", 1)[0]
                if upstream in declared and upstream not in owned:
                    owned[upstream] = declared[upstream]
                    frontier.append(upstream)
    return owned


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
    # The product tree, the delivered relations it opens from, and this estate's
    # own known-answer assertions. Cautious indirect selection: a test runs only
    # when every relation it reads is inside this selection.
    result = _dbt(
        "build",
        "--select",
        "+path:models/products",
        "path:tests/investment",
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


# --------------------------------------------------------------- validation and emission


def test_every_declaration_validates_and_passes_the_neutrality_gate() -> None:
    output = _command("validate")
    assert f"{len(_declarations())} product declaration(s) valid" in output, output


def test_the_command_emits_one_owned_summary_line_per_investment_product() -> None:
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
    owned = {name for name in _owned() if name.startswith(f"{DOMAIN}.")}
    assert len(owned) >= 60, len(owned)
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
    owned = _owned()
    for published_name in sorted(owned):
        assert f"  {published_name}: layer=" in graph, published_name
    document = json.loads((GRAPH_DIR / "product-graph.json").read_text(encoding="utf-8"))
    assert set(owned) <= set(document["order"]), sorted(set(owned) - set(document["order"]))
    lineage = (GRAPH_DIR / "product-field-lineage.csv").read_text(encoding="utf-8")
    assert "fund_business_key" in lineage, "no calculated-field lineage was emitted"

    contracts = _command("contracts")
    assert f"{len(_declarations())} declared product contract(s) valid" in contracts, contracts
    descriptors = _command("odps")
    assert f"{len(_declarations())} declared product descriptor(s) valid" in descriptors, descriptors

    for published_name in sorted(owned):
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


# ------------------------------------------------------------------ the Data Vault products


def _shape_relations(document) -> set:
    """Every relation a data_vault declaration says its shape renders."""

    config = document["target"]["shape_config"]
    names = set()
    for section, prefix in (
        ("hubs", "hub"),
        ("links", "link"),
        ("satellites", "sat"),
        ("business_vault", "golden"),
        ("point_in_time", "pit"),
    ):
        for entry in config.get(section) or []:
            names.add(f"{prefix}_{entry['name']}")
    return names


def test_every_vault_contract_lists_every_relation_the_shape_renders() -> None:
    """Owner ruling R4. The contract is the only pipe, so a relation the shape
    renders that the contract does not list would be unreadable by declaration."""

    declared = _declarations()
    for published_name in VAULT_PRODUCTS:
        document = declared[published_name]
        assert document["target"]["shape"] == "data_vault", published_name
        domain, name = published_name.split(".", 1)
        contract = json.loads(
            (CONTRACTS_DIR / domain / name / "contract.json").read_text(encoding="utf-8")
        )
        listed = {
            entry["name"].rsplit("__", 1)[-1]
            for entry in contract["relations"]
        }
        expected = _shape_relations(document)
        assert listed == expected, (published_name, sorted(expected ^ listed))
        for relation in sorted(expected):
            assert (CONTRACTS_DIR / domain / name / f"{name}__{relation}.odcs.yml").is_file(), (
                published_name,
                relation,
            )


def test_every_consumer_of_a_vault_names_the_relation_it_reads() -> None:
    """A vault publishes several relations, so a consumer that named none of them
    would be reading an ambiguous product (owner ruling R4, plan decision D36)."""

    declared = _declarations()
    consumers = 0
    for published_name, document in declared.items():
        for source in document.get("sources") or []:
            upstream = str(source["contract"]).split("@", 1)[0]
            if upstream not in VAULT_PRODUCTS:
                continue
            consumers += 1
            relation = source.get("relation")
            assert relation is not None, (
                f"{published_name} reads {upstream} without naming a relation"
            )
            assert relation in _shape_relations(declared[upstream]), (
                published_name,
                upstream,
                relation,
            )
        for step in document.get("steps") or []:
            for lookup in step.get("lookups") or []:
                upstream = str(lookup["contract"]).split("@", 1)[0]
                if upstream not in VAULT_PRODUCTS:
                    continue
                consumers += 1
                assert lookup.get("relation") is not None, (
                    f"{published_name} reads {upstream} in a lookup without naming a relation"
                )
    assert consumers >= len(VAULT_PRODUCTS), consumers


# ------------------------------------------------------------------ what the tree reads


def _model_reads() -> dict:
    reads: dict = {}
    for path in sorted(MODELS_DIR.rglob("*.sql")):
        text = path.read_text(encoding="utf-8")
        reads[path.stem] = set(re.findall(r'\{\{\s*ref\("([^"]+)"\)\s*\}\}', text))
    return reads


def _landed_relations() -> dict:
    owned: dict = {}
    for published_name, document in _owned().items():
        if document["product"].get("layer") != LANDING_LABEL:
            continue
        domain, name = published_name.split(".", 1)
        for source in document.get("sources") or []:
            binding = source.get("fixture")
            if binding is not None:
                owned[str(binding["relation"])] = f"{domain}__{name}"
    return owned


def test_the_twenty_one_source_extracts_each_land_through_one_product() -> None:
    """The five source declarations state 21 tables between them, and the estate
    lands each one through exactly one product."""

    tables = 0
    raw_models = set()
    for source in SOURCE_DECLARATIONS:
        document = yaml.safe_load(
            (REPO_ROOT / "declarations" / f"{source}.yml").read_text(encoding="utf-8")
        )
        for table in (document.get("tables") or {}).values():
            tables += 1
            raw_models.add(str(table["raw_model"]))
    assert tables == 21, tables

    landed = _landed_relations()
    missing = sorted(raw_models - set(landed))
    assert not missing, f"delivered relation(s) no landing product lands: {missing}"

    landed_source_extracts = [
        published
        for published, document in _owned().items()
        if document["product"].get("layer") == LANDING_LABEL
        and any(
            str((source.get("fixture") or {}).get("relation")) in raw_models
            for source in document.get("sources") or []
        )
    ]
    assert len(landed_source_extracts) == 21, sorted(landed_source_extracts)


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


def test_every_recovered_seed_is_bound_by_a_landing_product() -> None:
    """A seed nothing lands is dead fixture data. Every CSV this estate carries is
    the delivered relation one of its landing products lands."""

    landed = set(_landed_relations())
    estate_seeds = {
        str(source["fixture"]["relation"])
        for document in _owned().values()
        for source in document.get("sources") or []
        if source.get("fixture")
    }
    assert estate_seeds == landed, sorted(estate_seeds ^ landed)
    for relation in sorted(landed):
        assert (REPO_ROOT / "seeds" / f"{relation}.csv").is_file(), relation


# ------------------------------------------------------- the canonical mapping validation


def test_every_canonical_interface_declares_its_reference_mapping() -> None:
    """Each canonical interface says which entity of the reference model it is
    this estate's reading of, and which attribute each column it publishes
    carries. The mapping lives on the product that publishes the columns, so
    nothing has to be kept in step with it elsewhere."""

    declared = _declarations()
    for entity, published_name in CANONICAL_ENTITIES.items():
        document = declared[published_name]
        assert document["target"]["shape"] == "canonical", published_name
        relations = document["target"]["shape_config"]["entities"]
        names = {relation["name"] for relation in relations}
        assert entity in names, (published_name, sorted(names))
        for relation in relations:
            reference = relation.get("reference")
            assert reference, (published_name, relation["name"])
            assert reference["entity"].startswith(("PM-", "E-")), reference["entity"]
            # Every column the relation publishes carries a named attribute:
            # a column mapped onto nothing would be a claim about the
            # reference model that nothing proves.
            unmapped = sorted(set(relation["columns"]) - set(reference["attributes"]))
            assert not unmapped, (published_name, relation["name"], unmapped)
        published_columns = {
            column for relation in relations for column in relation["columns"]
        }
        assert len(published_columns) >= 5, (published_name, sorted(published_columns))


def test_the_canonical_interfaces_validate_against_the_reference_model() -> None:
    """The canonical-mapping validation this estate runs, driven through the
    real command against a checkout of the reference model. Every declared
    mapping is proved against the model's own attribute schema, so a renamed
    or misspelled attribute is a failure rather than a claim.

    With no checkout resolved the command has nothing to validate against;
    this states that and asserts the command says so rather than reporting a
    pass it never ran."""

    from ergasterion.estate import REFERENCE_MODEL_DIRECTORY, resolve_openim_root

    checkout = resolve_openim_root(root=REPO_ROOT)
    resolved = checkout is not None and (checkout / REFERENCE_MODEL_DIRECTORY).is_dir()
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "ergasterion.cli",
            "validate-canonical",
            "--estate-root",
            str(REPO_ROOT),
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )
    output = result.stdout + result.stderr
    assert result.returncode == 0, output
    if not resolved:
        assert output.startswith("skipped:"), output
        print("  no reference model checkout resolved; nothing was validated against one")
        return
    for published_name in sorted(CANONICAL_ENTITIES.values()):
        assert f"validated {published_name} entity " in output, (published_name, output)
    assert "canonical mappings: 6 validated" in output, output


# ------------------------------------------------------- the human-decision products


def test_the_human_decision_products_take_the_shape_the_estate_declares() -> None:
    """Plan decision D17. The append-only log itself stays adapter-provisioned and
    unchanged; the product route over it is a landing product whose publication is
    append only, the decision in force and the stage in force as derivations, and
    the queue and the worklist as serving products."""

    declared = _declarations()

    log = declared["investment.deal_decision_log"]
    assert log["product"]["layer"] == LANDING_LABEL, log["product"]
    publish = [step for step in log["steps"] if step["pattern"] == "data_publish"][0]
    assert publish["publication_mode"] == "incremental", publish
    assert publish["unique_key"] == ["decision_id"], publish
    binding = log["sources"][0]["fixture"]["relation"]
    assert binding == "deal_decision_log_fixtures", binding

    for published_name in ("investment.deal_decision_current", "reference.deal_stage_current"):
        document = declared[published_name]
        assert document["product"]["profile"] == "derivation", published_name
        predicates = [
            predicate["name"]
            for step in document["steps"]
            if step["pattern"] == "data_filtering"
            for predicate in step["predicates"]
        ]
        assert predicates, f"{published_name} keeps no version by a named predicate"

    for published_name in ("investment.deal_decision_queue", "investment.deal_review_worklist"):
        document = declared[published_name]
        assert document["product"]["layer"] == "gold", published_name
        assert document["product"]["profile"] == "serving", published_name

    # The provisioning D17 holds constant: the log the analysts write to is
    # created by the adapter and merged into from its fixture seed, and neither
    # is a relation this route generates.
    project = yaml.safe_load((REPO_ROOT / "dbt_project.yml").read_text(encoding="utf-8"))
    hooks = "\n".join(project.get("on-run-start") or [])
    assert "dpf_ensure_deal_decision_log_table" in hooks, hooks
    seed_config = project["seeds"]["ergasterion"]["deal_decision_log_fixtures"]
    assert "+post-hook" in seed_config, seed_config
    for model in MODELS_DIR.rglob("*.sql"):
        assert model.stem != "deal_decision_log", model


def test_each_relationship_is_a_link_inside_the_vault_that_states_it() -> None:
    """A relationship between two curated entities is a hub and a link inside the
    vault of the product that states it, as the fund vault models the manager a
    fund names."""

    declared = _declarations()
    expected = {
        "investment.fund_vault": {"fund_gp"},
        "investment.deal_vault": {"deal_portfolio_company", "deal_fund"},
        "investment.legal_vehicle_vault": {"legal_vehicle_fund"},
    }
    for published_name, links in expected.items():
        config = declared[published_name]["target"]["shape_config"]
        declared_links = {entry["name"] for entry in config.get("links") or []}
        assert declared_links == links, (published_name, sorted(declared_links ^ links))
        hubs = {entry["name"] for entry in config["hubs"]}
        for entry in config["links"]:
            assert set(entry["hubs"]) <= hubs, (published_name, entry["name"])


# --------------------------------------------------------------------- the executed lane


def _generated_coverage_tests() -> set:
    """Every reconcile-coverage test the route emitted for this estate, read
    off the schema documents it wrote."""

    names = set()
    for path in sorted(MODELS_DIR.rglob("*.yml")):
        document = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        for model in document.get("models") or []:
            for entry in model.get("data_tests") or []:
                body = entry.get("dpf_reconcile_coverage")
                if body:
                    names.add(body["name"])
    return names


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

    # Every conformed union proves its coverage back to the lanes it combines
    # (architecture section 8), and the proof has to run rather than merely be
    # emitted. The names come from the emitted schema documents, so this
    # follows the declarations rather than restating them.
    coverage = sorted(_generated_coverage_tests())
    assert len(coverage) >= 14, coverage
    for name in coverage:
        assert f"PASS {name}" in output, (
            f"the coverage proof {name} did not run or did not pass"
        )


def test_a_second_unchanged_build_changes_nothing() -> None:
    """Replay suppression: the vault stores are insert-only, so a second build of
    an unchanged input adds no version to a satellite and no row to a hub."""

    _ensure_built()
    published = (
        "investment__fund_vault__hub_fund",
        "investment__fund_vault__sat_fund_detail",
        "investment__fund_vault__sat_fund_observed",
        "investment__fund_vault__pit_fund",
        "investment__fund_vault__golden_fund",
        "investment__deal_vault__sat_deal_pipeline",
        "investment__portfolio_company_vault__sat_portfolio_company_observed",
        "investment__fund_performance_summary",
    )
    before = _relation_digest(published)
    _dbt_build()
    assert _relation_digest(published) == before, (
        "a second unchanged build changed a published relation"
    )



def test_every_link_resolves_in_the_vault_that_owns_the_entity() -> None:
    """An association is only an association if both ends hash the same identity.
    Each link below is stated by one vault and points at an entity another vault
    owns, so every identity key the link carries is looked up in the owning
    vault's own hub, by the hash and by the business key behind it."""

    _ensure_built()
    joins = (
        ("investment__fund_vault__link_fund_gp",
         "investment__fund_vault__hub_gp", "gp_hk", "fund_gp_business_key",
         "investment__gp_vault__hub_gp", "gp_business_key"),
        ("investment__deal_vault__link_deal_fund",
         "investment__deal_vault__hub_fund", "fund_hk", "deal_fund_business_key",
         "investment__fund_vault__hub_fund", "fund_business_key"),
        ("investment__deal_vault__link_deal_portfolio_company",
         "investment__deal_vault__hub_portfolio_company", "portfolio_company_hk",
         "deal_company_business_key",
         "investment__portfolio_company_vault__hub_portfolio_company",
         "company_business_key"),
        ("investment__legal_vehicle_vault__link_legal_vehicle_fund",
         "investment__legal_vehicle_vault__hub_fund", "fund_hk",
         "vehicle_fund_business_key",
         "investment__fund_vault__hub_fund", "fund_business_key"),
    )
    for link, hub, key, hub_business_key, owner, owner_business_key in joins:
        carried = _query(
            f'select count(distinct link."{key}") from "{link}" as link'
        )[0][0]
        assert carried > 0, f"{link} carries no association to check"
        dangling = _query(
            f'select count(*) from (select distinct hub."{key}" as identity_key, '
            f'hub."{hub_business_key}" as business_key from "{link}" as link '
            f'inner join "{hub}" as hub on hub."{key}" = link."{key}") as carried '
            f'left join "{owner}" as owner on owner."{key}" = carried.identity_key '
            f'and owner."{owner_business_key}" = carried.business_key '
            f'where owner."{key}" is null'
        )[0][0]
        assert dangling == 0, (
            f"{link} carries {dangling} identity key(s) the owning vault {owner!r} "
            "does not know"
        )

def test_the_resolution_evidence_carries_the_seeded_review_case() -> None:
    """The probabilistic tier, read straight off the built adapter rather than
    through the assertion that also asserts it, so the two are independent."""

    _ensure_built()
    rows = _query(
        "select record_key_a, record_key_b, review_band "
        'from "investment__deal_vault__pending_keys" '
        "where review_band = 'review' order by record_key_a"
    )
    assert rows == [("origo:D-009", "origo:D-010", "review")], rows

    bands = dict(
        _query(
            "select review_band, count(*) "
            'from "investment__fund_vault__pending_keys" group by 1'
        )
    )
    assert bands.get("accepted", 0) >= 1 and bands.get("review", 0) >= 1, bands
    assert "unscored" not in bands, bands


# ----------------------------------------------------------------------- the named rules


def test_every_named_rule_the_estate_references_resolves_for_both_adapters() -> None:
    from ergasterion.estate import EstateContext
    from ergasterion.framework import rules as rules_mod

    ctx = EstateContext.default()
    catalogue = yaml.safe_load(RULE_CATALOGUE.read_text(encoding="utf-8"))
    declared = {rule["name"] for rule in catalogue["rules"]}

    referenced = set()
    for document in _owned().values():
        for step in document.get("steps") or []:
            if step["pattern"] == "calculated_fields":
                for field in step.get("fields") or []:
                    if field.get("rule"):
                        referenced.add(str(field["rule"]))
            elif step["pattern"] == "data_curation":
                scoring = (step.get("resolution") or {}).get("scoring") or {}
                if scoring.get("rule"):
                    referenced.add(str(scoring["rule"]))
    assert declared <= referenced, sorted(declared - referenced)

    loaded = rules_mod.load_rule_catalogue(estate_dir=ctx.root / "rules")
    implementations = {**rules_mod.REFERENCE_IMPLEMENTATIONS, **loaded.implementations}
    macros = RULE_MACROS.read_text(encoding="utf-8")
    for name in sorted(declared):
        macro = rules_mod.dbt_implementation_macro(
            name, implementations=implementations, adapters=("duckdb", "bigquery")
        )
        assert f"macro {macro}(" in macros, (name, macro)

    print(
        f"    named rules declared by this estate: {len(declared)}; referenced by its "
        f"products: {len(referenced & declared)}"
    )


# --------------------------------------------------------------------------- runner


TESTS = (
    test_every_declaration_validates_and_passes_the_neutrality_gate,
    test_the_command_emits_one_owned_summary_line_per_investment_product,
    test_re_emission_is_byte_identical_and_check_reports_no_drift,
    test_the_graph_the_contracts_and_the_descriptors_are_emitted,
    test_every_vault_contract_lists_every_relation_the_shape_renders,
    test_every_consumer_of_a_vault_names_the_relation_it_reads,
    test_the_twenty_one_source_extracts_each_land_through_one_product,
    test_only_a_landing_product_reads_the_relation_it_lands,
    test_every_recovered_seed_is_bound_by_a_landing_product,
    test_every_canonical_interface_declares_its_reference_mapping,
    test_the_canonical_interfaces_validate_against_the_reference_model,
    test_the_human_decision_products_take_the_shape_the_estate_declares,
    test_each_relationship_is_a_link_inside_the_vault_that_states_it,
    test_the_estate_builds_on_duckdb_with_every_generated_test_and_assertion,
    test_a_second_unchanged_build_changes_nothing,
    test_every_link_resolves_in_the_vault_that_owns_the_entity,
    test_the_resolution_evidence_carries_the_seeded_review_case,
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
