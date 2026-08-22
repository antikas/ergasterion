# Ergasterion

*Ergasterion (ἐργαστήριον) is the ancient Greek word for a workshop, a place where things are made. This one makes data pipelines. You describe your data sources; it builds the warehouse.*

## What it is, in one minute

Turning raw source feeds into a trustworthy warehouse means writing a lot of plumbing by hand: typed staging tables, history that never overwrites, clean served tables, tests. Ergasterion is a factory that builds that plumbing instead. You write a short description of each source: what it sends, what the columns mean, and which real-world things (a customer, an order, a fund) the columns refer to. The engine reads the descriptions and generates the whole pipeline. Change a description and rebuild; the plumbing is never written by hand, so it never falls out of date.

One source per entity is a perfectly normal estate. You still get the full generated pipeline, contracts, and map. Several sources may also describe the same real-world thing. A retailer, for example, may hear about one customer from its storefront, a marketplace, and its CRM, with each source spelling the name differently. The factory resolves those records, keeps every source's version of each fact, and applies the written rule for which value wins.

The factory itself does not know or care what a "customer" or a "fund" is. Everything it does is generic. This repository proves that on two worked domains that share no vocabulary at all, one online-retail and one investment, both described in full further down.

## Before you begin

DuckDB is an embedded local database that stores the complete local run in a file inside
the checkout. The engine and the DuckDB path need local tools only, with no warehouse account.

- **Python 3.11 or newer.** Check with `python --version`.
- **Git**, to fetch the code if you install from source.
- **Bash and a terminal**, to run the repository checks and demonstrations.
- **The repository build dependencies**, when you want to validate or demonstrate this checkout. The one-time setup in the [runbook](https://github.com/antikas/ergasterion/blob/master/RUNBOOK.md#1-install) creates the repository `.venv`. It installs dbt Core 1.11.12 with dbt-snowflake 1.11.6, dbt-bigquery 1.11.3, and dbt-duckdb 1.11.0, then installs Ergasterion in editable mode. Run the validator from that activated shell or select the `.venv` Python and dbt executables explicitly as shown there.
- **A warehouse account only for the live Snowflake route.** The complete local DuckDB run needs no warehouse account.

## Install the engine and local database support

The `local-ingestion` extra installs the engine, dbt Core, DuckDB, and the tested
DuckDB adapter.

```bash
python -m venv .venv
source .venv/bin/activate          # Windows PowerShell: .venv\Scripts\Activate.ps1
pip install "ergasterion-factory[local-ingestion]"
```

The virtual environment keeps the installation isolated from the rest of your system. The base package, `pip install ergasterion-factory`, is sufficient when you only need generation and import commands. Existing installs that use the `duckdb` extra remain supported; it is an alias of `local-ingestion`. Snowflake and BigQuery users can install the `snowflake` or `bigquery` extra.

**Or install from the source**, if you want to read the engine or keep an editable checkout that tracks your own edits. Clone the repository and install it in place. The `-e` means *editable*, so pulling updates never means reinstalling.

```bash
git clone https://github.com/antikas/ergasterion
cd ergasterion
pip install -e ".[local-ingestion]"
```

## Check it works, and two ways to run it

If the help text prints, you are installed.

```bash
ergasterion --help
# ...or, identically, with no separate program involved:
python -m ergasterion --help
```

Both do exactly the same thing. `ergasterion` is a short launcher the installer sets up for convenience. `python -m ergasterion` runs the engine straight through Python, so nothing extra is installed on your system and it behaves the same way on macOS, Linux, and Windows. Use whichever you prefer. Every example below works with either. If the `ergasterion` command is ever not found, reach for the `python -m ergasterion` form.

## Create your first workshop

One command lays down an empty, ready-to-fill estate.

```bash
ergasterion init my-estate
cd my-estate
```

An **estate** is your own project, kept separate from the engine. It contains empty `domains/`, `declarations/`, `seeds/`, and `tests/` folders, plus a `dbt_project.yml` and connection template. The engine's shared helper macros are copied into the estate because dbt loads macros from the project's own tree. A generated `GETTING-STARTED.md` walks through the estate in full. This README gets you to the first build; that file is the reference after it.

## Describe what you have

Three small files. This is the only part you author. Everything downstream is generated from it.

- **`domains/<name>.yml`** is the **domain**: the real-world things you care about (say, customer, product, order) and how they relate. When several sources describe the same thing, this is also where your rules live for resolving duplicates and choosing the winning value.
- **`declarations/<source>.yml`** is one **source**: a single incoming feed, its columns, and which thing in the domain each column refers to. One file per feed.
- **`seeds/<table>.csv`** is the **data**: your real rows, or fixture rows to try it with.

You do not have to invent the shapes from a blank page. This repository ships two complete, worked estates, an e-commerce customer view and an investment domain, and `GETTING-STARTED.md` walks the exact YAML field by field. Copy from those and adapt.

## Build it

One command turns your descriptions into the whole pipeline.

```bash
ergasterion emit --estate-root .
```

This regenerates everything from your declarations: typed staging models, the append-only history layer (a warehouse pattern called a Data Vault), golden records, and the models that resolve identity when sources overlap. With one source per entity that layer has nothing to argue about and simply passes your records through. Run it again any time you change a description. It always produces identical output from identical input, and it refuses to build if anyone hand-edits a generated file instead of the description it came from.

## See what it made

The generated pipeline, plus three things it publishes at the boundary.

- **`models/`** is the dbt pipeline: staging, the history vault, golden records, entity resolution. Point dbt at your warehouse to run it.
- **`contracts/`** is an open **data contract** for every served table (the industry-standard ODCS format), so catalogue tools understand your outputs out of the box.
- **`graphs/`** is a typed **map** of your domain, its entities, and how they relate, in every standard graph form for tools and agents to navigate.

The [architecture guide](https://github.com/antikas/ergasterion/blob/master/docs/architecture/README.md) shows how the generated pipeline fits together.

## Two shortcuts, once you are comfortable

You do not always start a source description from scratch.

```bash
ergasterion import-ddl schema.sql               # from CREATE TABLE statements
ergasterion import-odcs supplier-contract.yml   # from a supplier's data contract
```

Both read a machine-readable definition you already have and write the mechanical first draft of a source description: the column names, the types, and which ones must never be blank or duplicated. What no definition can tell you, which real-world thing each table describes, is left as a clearly marked `TODO` for you to fill. They speed up the typing. They never guess the judgement. See the worked [ODCS](https://github.com/antikas/ergasterion/blob/master/DEMO.md#worked-example-hand-me-your-data-contract) and [DDL](https://github.com/antikas/ergasterion/blob/master/DEMO.md#worked-example-seeding-from-raw-ddl) examples.

## Generation and estate commands

Run any of these as `ergasterion <command>` or `python -m ergasterion <command>`. Add `--help` to any for its options.

| Command | What it does |
|---|---|
| `ergasterion init <dir>` | Scaffold a new empty estate to fill. |
| `ergasterion emit` | Generate the whole pipeline from your descriptions. |
| `ergasterion contracts` | Generate an ODCS data contract per served table. |
| `ergasterion odps` | Generate the data-product descriptor for a domain. |
| `ergasterion graph` | Emit the typed relationship map of the domain. |
| `ergasterion import-ddl` | Draft a source from CREATE TABLE statements. |
| `ergasterion import-odcs` | Draft a source from a supplier's data contract. |
| `ergasterion lint` | Check generated SQL for cross-warehouse portability. |
| `ergasterion structure` | Check the estate against each target's declared structural budgets. |

Everything the repository ships runs on made-up data that imitates the real shapes these sources take. There is no real or client information anywhere in it, and the e-commerce domain's people, addresses, and brands are entirely invented (synthetic emails sit on the reserved `example.com` domain). The public repository is released under the MIT licence; see the LICENSE file in the repository you are reading.

## Bronze: how a source's delivered batch gets in

Everything above starts from a domain description that already trusts its seed data. Bronze earns that trust. A team writes one Bronze Product Contract per source table, covering its native schema, delivery mode, quality rules, and publication policy. The runtime applies that contract to every delivery the same way. A delivery's raw bytes are preserved exactly as received, parsed, checked against every declared rule, and either published or quarantined with a locator back to the exact bytes that failed. Nobody decides a single delivery's outcome by hand at run time; the contract decided it in advance.

Bronze runs through its own closed operator command surface (`ergasterion plan`, `contract register`/`activate`, `deployment register`/`activate`, `ingest file`/`due`, `reconcile`, `local-backup`, `status`, `inspect`, `quarantine`). The local reference platform needs no external account. [`bronze-ingestion.md`](https://github.com/antikas/ergasterion/blob/master/docs/architecture/bronze-ingestion.md) explains the full mechanism. [`demo/bronze-ingestion/`](https://github.com/antikas/ergasterion/blob/master/demo/bronze-ingestion/) runs it end to end with no warehouse account or network call.

The runtime binds to its environment through declared adapter ports. Delivery transport, operational state, raw storage, the Bronze store, projections, and the policy authority each sit behind a port, and a runtime binding names the adapter that fills each one. The local reference platform uses SQLite for operational state, DuckDB for Bronze and projections, and local files for delivery and raw storage. The packaged conformance runner checks another implementation against the same interface vectors. Those checks establish compatibility with the runtime contract; production access, resilience, and operation remain the responsibility of that environment.

---

## The two worked domains

The sections below cover the two worked domains, how the output is verified, the standards at the boundary, the graph map, the live warehouse path, and the engine layout. None of this is needed for the first local run. The complete system shape is in the [architecture guide](https://github.com/antikas/ergasterion/blob/master/docs/architecture/README.md).

The factory core carries no domain vocabulary. To show that, this repository proves the identical mechanism on two domains that share not a single noun: an online retailer's view of its own customers, and an investment dataset built from fund-administration and market-data feeds. The engine that builds both (`ergasterion/emit.py` and its templates) contains zero e-commerce or investment words either way. Both domains are *data*, read by the same generic code.

The two domains are peers with equal standing. Investment exercises some structural patterns that have no retail equivalent, including a legal-vehicle grain and manager succession, so it appears in more examples. The extra coverage reflects the domain, while the factory remains domain-independent. E-commerce runs the same generator unmodified and builds identically without an external model.

### Worked domain: e-commerce customer-360

An online retailer's view of its own customers, built from three invented feeds: **CARTIVO** (a web storefront), **MERCARO** (a marketplace), and **RELATIO** (a CRM system that only covers some customers). The entities are customer, product, and order. The [onboarding recipe](https://github.com/antikas/ergasterion/blob/master/DEMO.md#worked-example-adding-a-whole-new-domain) works through it step by step.

Follow one customer through it. Ava Thompson buys from all three feeds under three different-looking records: the storefront and marketplace both carry her loyalty programme number (`LOY-1001`), and the CRM record carries a third, slightly different email address with no loyalty number at all.

**Recognition.** The system resolves all three records to one customer because two of them share the loyalty id, the strongest signal available, checked first. Where no loyalty id is shared, it tries a normalised email match. A customer seen under a case-different or punctuation-different email in two feeds still resolves to one identity. Records with neither signal remain separate. The e-commerce domain does not send customer pairs into the probabilistic review queue.

**Memory and choosing.** Every feed's version of a customer's contact details, preferences, and order history is kept, dated. Survivorship rules are written by the person responsible and can be changed at any time. The CRM supplies contact attributes, while marketing preferences and consent come from the storefront. The marketplace remains the authority for its order attributes. A seeded conflict (Ava's city, recorded differently across feeds) proves the rule picks the intended winner, not an arbitrary one.

**Placed, over time.** A customer's loyalty tier (bronze, silver, gold) changes without the customer's own identity changing. Ava was silver from the start of 2025 and became gold from May. An order placed in March attributes to silver, one placed in June attributes to gold, and a query about March always gets March's answer no matter when it is asked, from the same dated history (`models/marts`, `dim_customer_segment`).

**Conserved.** An order's line items must sum to its own header total. Revenue cannot appear or vanish between the two grains, and the pipeline checks that it does not (`tests/assert_order_line_revenue_conservation.sql`).

**Measuring.** Revenue by segment per month, average order value, and the repeat-purchase rate are each defined once and computed from the clean tables (`models/marts/fact_order.sql` and the metric definitions).

The e-commerce domain is proven the same way this repository proves everything: named, seeded tests that check specific known answers. A specific customer must resolve to one identity across three sources, an order's line items must sum to its header total, and a segment change must attribute correctly by date. The account-free demonstration runs this domain first, covering revenue by segment and customer resolution, then runs the investment result in the same session. The [demo guide](https://github.com/antikas/ergasterion/blob/master/demo/README.md) gives the commands and output locations.

### Worked domain: investment, following one fund through

An investment dataset built from four described sources: fund-administration and market-data feeds, each with its own spellings, identifiers, and layouts. The entities are funds, management firms, portfolio companies, and a deal pipeline. Imagine a fund called Apex Growth II. Three of your sources know about it, and each calls it something different: one says "Apex Growth Fund II", another says "Apex Growth II, LP", a third says "Apex Growth II Feeder". Here is what happens to it, stage by stage.

**Arrival.** Each source's file is loaded exactly as it came, into its own tables, nothing changed. Keeping the raw arrival untouched means any later question ("where did this number come from?") can always be answered by walking back to what actually arrived. The tidied, typed copies of these tables are the first thing the generator builds (in the repository these live under `models/staging`).

**Recognition.** The system works out that those three differently spelled records are the same fund. It does this with rules, in order of confidence: shared official identifiers first, then shared reference codes, and only then similarity of names and other attributes. Each match it makes is recorded with the rule that made it. When the rules are not confident enough, the candidate pair is not merged. It is sent to a review screen where a person decides, and the person's decision is remembered for next time (`models/entity_resolution`).

**Memory.** Every source's version of every fact is kept, with the date it arrived and the source it came from. Nothing is overwritten. If a source corrects itself next month, both the old and new values are there, each with its time (`models/raw_vault`, a warehouse pattern the industry calls a Data Vault).

**Choosing.** For each fact about the fund, one value must win. The responsible person sets the rules in one readable file and can change them at any time. Capital figures prefer the fund administrator, while the most recent business date breaks a source tie. The system applies those rules; it never invents its own. The chosen record keeps, for every field, a note of which source won and when (`models/business_vault`, "golden records").

**Serving.** The winning values are arranged into a small set of clean tables shaped for people and tools to use: funds, management firms, companies, cash movements, valuations (`models/canonical` and `models/marts`).

**Measuring.** The performance measures investors ask about (how much was paid in, how much came back, the rates of return) are each defined once, in one place, and computed from the clean tables. If a measure needs to change, it changes in one file and everywhere that shows it follows (`models/calculated_fields` and the metric definitions).

**Placed.** Some facts about a fund can change without the fund itself changing: which department holds it, what sector it plays in. Apex Growth II sat in the Private Equity department from 2021, then moved into the newer Institutional Growth department from the middle of 2025; its sector, Growth Equity, never changed. Every placement is kept with the date it started, so a question about an earlier year gets that year's answer and a question about today gets today's, from the same history (`models/marts`, the time-varying classification).

Two more things can happen to a fund that Apex Growth II does not show, so here they are on funds that do.

**Wrapped.** Some funds are held through a legal vehicle between the fund and its cash flows, sometimes with one vehicle nested inside another. Orion Credit Opportunities I is held this way through several vehicles. Whichever vehicle a payment arrives through, its totals must reconcile to what the fund as a whole received and paid out. The system checks that reconciliation every time (`models/canonical` and `models/marts`, the vehicle-to-fund rollup).

**Renamed.** A fund's manager can change through a merger, rebrand, or acquisition without changing the fund's identity. Harbor Infrastructure III's manager was Harbor Infrastructure Partners until the start of 2025, when it became Meridian Infrastructure Advisors. The two manager names are kept as separate records joined by a dated event. A return earned in 2024 stays attributed to Harbor Infrastructure Partners; a return earned after the rebrand belongs to Meridian (`models/raw_vault`, the manager succession link).

### Investment, second arc: the deal pipeline

A firm also tracks opportunities that may become investments. Ergasterion carries that stream through the same source description, generated pipeline, dated history, and human decision controls used elsewhere in the investment domain.

**Declared.** One new source, ORIGO, describes a deal-origination system: the log a deal team keeps of opportunities it is sourcing, screening, and deciding on. Ten deal records arrive, several deliberately awkward: two pairs are the same deal typed in twice under a shared reference the source itself gave it, and a third pair share no reference at all, only very similar names.

**Recognised.** The two pairs sharing a reference merge automatically. The pair with no reference and only similar names waits on the review screen. Deal matching is source-local: ORIGO deals are compared with other ORIGO deals. Fund matching, by contrast, compares records across every source that describes a fund.

**Tracked.** Each recognised deal moves through a small set of stages over time: sourced, screened, examined in detail, considered for a decision, then committed or declined. Every stage it has occupied is kept and dated in an append-only log. A buyout aimed at Lumina Health, code-named Project Atlas inside the deal team, reached the decision point and stayed there. The investment committee first met in March and chose to wait for more information.

**Decided.** A decision on a deal at that point (approve it, approve it with conditions attached, decline it, or wait longer) is written to a permanent record. A deal that reaches committed can also carry a link to the fund it became. One of the ten, the direct-lending opportunity, converts into Orion Credit Opportunities I. The decision log keeps every decision; the dated stage model derives its current result from the latest decision.

**Reviewed.** The same review screen that shows an uncertain fund match has a tab for these deals too: a queue of the ones sitting at a decision point, the history behind each one, and the four choices above.

This second arc reuses the source-description shape, confident-first matching, never-overwritten history, and review screen from the fund journey. It extends that machinery to deals. The example is deliberately narrower: a deal's identity is never checked against a second source, and nothing learned while reviewing a deal feeds back into fund matching. That is a stated limit, not an oversight.

### What is different between the two domains, stated plainly

The e-commerce domain is a complete worked domain in its own right. It runs the same generator and answers to no external model, which proves the factory is domain-independent. The practical differences are:

- **No canonical model repo.** The investment domain's declarations carry an optional `canonical_mappings` block that cross-checks attribute lineage against the Open Investment Model (see "What this is not," below); the e-commerce declarations carry none. There is no external model this domain validates against, and the pipeline builds and runs identically without one.
- **A shared scoring slot.** The scoring configuration calls its categorical comparison weight `weight_sector`. Investment entities use it for sector; other entities can use it for another categorical attribute. The e-commerce declarations carry customer weights, but customer records do not enter the probabilistic scoring branch.
- **Deterministic customer resolution.** The e-commerce customer path uses loyalty id and normalised email. Its unmatched records stay separate. The review console serves the investment domain's probabilistic matches and deal decisions.
- **The clean, final tables are hand-authored in both domains.** A person designs `canonical_customer`, `canonical_product`, the customer, product, and segment dimensions, and `fact_order` on top of the generated pipeline. The investment domain follows the same boundary for `canonical_fund`, `dim_fund`, and `fact_fund_performance`. See "What this is not," below, for why that boundary exists.

## How the output is verified

The generator always produces identical output from identical descriptions. The build fails if anyone edits a generated file by hand, and a bad description is rejected before generation with a message naming what is missing. A test suite asserts known answers on the invented data.

In e-commerce, a seeded customer duplicate must resolve to one identity, order lines must sum to their header, and segment changes must attribute correctly by date. The investment checks pin a specific fund return and reject a return when its cash flows contradict one another. They also verify the planted record overlaps, vehicle-to-fund totals, manager history across a rebrand, and historical classifications. The deal pipeline checks its dated stages and requires every conversion into a fund to name a fund that exists.

The account-free validator re-emits the estate, parses all three supported targets, and builds the complete seeded project in DuckDB. It also runs the package, contract, graph, scaffold, and wheel checks. Live Snowflake validation is a separate authorised run against a bounded development schema.

The architecture guide follows these checks through the generated pipeline and names the boundary between generated source-facing models and human-designed served models.

## Speaking the standard at the boundary

When a team finishes a clean, consumable table, its users need to know what that table promises. They need its columns, identifier, quality rules, owner, and meaning. Writing those facts by hand leaves a document that can drift away from the real table.

The **Open Data Contract Standard (ODCS)** is a published industry format maintained under the Linux Foundation. One small machine-readable contract per table records its schema, keys, quality checks, optional external definitions, and producer notes. Data catalogue and governance tools, including Collibra and OpenMetadata, read this format. A table that ships its contract can enter those tools without a second handwritten description.

The factory generates one contract for every served view, dimension, and fact in both worked domains. Columns and keys come from the built model. Existing build tests become quality entries, while survivorship rules record which source wins each field. A contract also links to an authoritative external model where one exists.

Investment contracts link to the public Open Investment Model. E-commerce contracts carry no external-model link because that domain answers to none. The same generator produces valid contracts in both cases. Each contract is checked against the standard's official schema held in this repository, so validation needs no network. The build also fails if a contract was edited by hand or has drifted from its source descriptions.

The line to hold in your head is this: the factory **speaks the shared standard at its boundary, and generates everything between the boundaries itself**. The contract is the interface a served table presents to the outside world; the machinery that builds the table (the identity resolution, the survivorship, the vault discipline) is the factory's own, and no standard describes it because none needs to. The regeneration command and validation steps are in the [runbook](https://github.com/antikas/ergasterion/blob/master/RUNBOOK.md).

That boundary has two more pieces, both from the same standards family (Bitol, under the Linux Foundation) and both generated the same never-by-hand way.

**Contracts in.** When a supplier already hands you an ODCS contract for what they send, there is no reason to retype its schema by hand. `ergasterion import-odcs` writes the first draft of a `declarations/<source>.yml`: column names, types, and constraints straight from the contract. What no contract can tell you, such as which real-world thing each table describes, is left as an explicit TODO for a person to fill in. See the [worked example](https://github.com/antikas/ergasterion/blob/master/DEMO.md).

**Products out.** The **Open Data Product Standard (ODPS)** describes the product that a group of tables belongs to. It is maintained by Bitol and is distinct from the similarly named specification at opendataproducts.org. Ergasterion generates one descriptor per domain. Output ports reference the exact ODCS contracts emitted for served tables. Input ports appear when a source declaration came from an imported contract. Management ports identify the available review console and dbt documentation surface; a domain can expose a common operational surface even when that domain produces no rows for one of its views. Team and support details come from repository metadata. Each descriptor is regenerated under `contracts/odps/` and checked against the vendored ODPS v1.0.0 schema.

Put together: **contracts describe a table, ODPS (Bitol) describes the domain those tables belong to, and this factory generates both, at both ends of the boundary, from the same descriptions that build everything else.**

## The map lane

The same domain descriptions also emit a typed map of the domain. A map names the business entities as graph nodes and names the relationships between them as graph edges. The verbs live in `domains/<domain>.yml`. The factory keeps the domain language in YAML and serialises the map without carrying words such as customer, order, product, or fund in the generator code.

Each domain emits a graph artefact family under `graphs/<domain>/`:

- `<domain>-nodes.csv` lists the hub-backed entity types.
- `<domain>-edges.csv` lists the typed relationships, their endpoints, their cardinality, their inverse verb, and the key or column that carries the relationship.
- `<domain>-graph.cypher`, `<domain>-graph.pgq.sql`, and `<domain>-graph.gql` describe the type-level graph in common graph query forms. The GQL file is a generated DDL form, not a tested claim of engine conformance.
- `<domain>-graph-description.json` gives tools and engineers a compact description of the node types, relation types, examples, CSV headers, and satellites.
- `<domain>-estate.pgq.sql` binds the graph to the emitted relational estate: golden-record or hub node tables, plus the physical link tables that realise link-backed edges.

There are two graph views. The **type graph** is the schema-level map: entity types, relation types, examples, and satellites. The **estate graph** is the SQL/PGQ binding over the actual emitted tables. Link-backed relationships appear in both views because they have physical link tables.

Column-level relationships appear in the type graph because they are relationship knowledge carried by a payload column, with no separate physical link table. Satellites are listed with their anchor entity in the graph description; they are not node types. Mart-layer foreign keys stay in dbt model YAML and tests, outside the declarative map system. The map lane does not emit document nodes, OWL, RDF, or SHACL.

### Adding a domain vocabulary

The e-commerce example shows the shape. Its hubs are `customer`, `order`, and `product`. Its physical links are `order_customer` and `order_line_product`. The relationship vocabulary lives in `domains/ecommerce.yml`:

```yaml
relations:
  verbs:
    PLACED_BY:
      alias: placed-by
      direction: directed
      kind: association
      cardinality: many_to_one
      inverse: PLACED
    PLACED:
      alias: placed
      direction: directed
      kind: association
      cardinality: one_to_many
      inverse: PLACED_BY
    INCLUDES:
      alias: includes
      direction: directed
      kind: composition
      cardinality: many_to_many
      inverse: LINE_OF
    LINE_OF:
      alias: line-of
      direction: directed
      kind: composition
      cardinality: many_to_many
      inverse: INCLUDES
  bindings:
    - verb: PLACED_BY
      source: order
      target: customer
      link: order_customer
      source_key: order_hk
      target_key: customer_hk
    - verb: INCLUDES
      source: order
      target: product
      link: order_line_product
      source_key: order_hk
      target_key: product_hk
```

The verbs define the vocabulary a reader sees: an order is `PLACED_BY` a customer, and an order `INCLUDES` a product. The bindings connect that vocabulary to the declared structure. Endpoints are named directly; the factory does not infer them from column names. That matters when a domain has role-named keys, self-relationships, or polymorphic references.

For a new domain, add a `relations:` block beside the existing entity, hub, link, and survivorship config. Define every verb with `alias`, `direction`, `kind`, `cardinality`, and `inverse`. Then bind each declared link, or each declared column-level relationship, to a verb and explicit source and target entity. Regenerate with `ergasterion graph` and check byte stability with `ergasterion graph --check`. The [vocabulary reference](https://github.com/antikas/ergasterion/blob/master/docs/architecture/ontology-map-lane.md) gives the complete shape.

## Choose how to run the worked repository

The repository supports Snowflake, BigQuery, and DuckDB. Choose the path that matches what you need to prove.

| Need | Command | Account required | Outcome |
|------|---------|------------------|---------|
| Generate the estate | `ergasterion emit` | No | Rebuilds the generated project from the declarations. |
| Verify the repository locally | `bash scripts/validate_offline.sh` | No | Re-emits the estate, parses all three targets, and completes the seeded DuckDB build before the downstream gates. |
| Run the worked demonstration locally | `bash demo/run_offline_demo.sh` | No | Builds both domains in DuckDB and prints the e-commerce, resolution, and investment results. |
| Run the live Snowflake demonstration | `bash demo/run_clean_demo.sh` | Yes | Uses an authorised Snowflake account and a fresh schema. |

The live Snowflake path needs credentials supplied by the account owner or an authorised operator. BigQuery support covers generation, dialect linting, and static parsing. The repository provides complete execution paths for DuckDB and Snowflake. See the [runbook](https://github.com/antikas/ergasterion/blob/master/RUNBOOK.md), [demo guide](https://github.com/antikas/ergasterion/blob/master/demo/README.md), and [source-description guide](https://github.com/antikas/ergasterion/blob/master/DEMO.md).

## What this is not

The generator builds the source-facing plumbing. The clean consumable layer (the final tables and the measure definitions) is designed once by people, deliberately, because that layer is the product and deserves human judgment. This is true for both worked domains: `canonical_customer` / `dim_customer` / `fact_order` in the e-commerce domain and `canonical_fund` / `dim_fund` / `fact_fund_performance` in the investment domain are all hand-authored on top of what the generator produces, never generated themselves. The supported targets are Snowflake, BigQuery, and DuckDB. The record matching is deliberately cautious: when it is not sure, it asks a person instead of guessing, because a wrong merge is far more expensive than a short review queue.

Ergasterion is a data-product factory. Its templates, identity-resolution mechanism, vault pattern, and survivorship engine contain no investment-domain knowledge. Investment declarations may add an optional `canonical_mappings` block as an onboarding aid. When `--openim-root <path>` points to a local [Open Investment Model](https://openinvestmentmodel.org) checkout, `ergasterion emit` checks the declared attribute lineage against that reference model.

The default mode reports a warning when no OpenIM checkout is available. `--strict-openim` turns the same condition into a non-zero exit. E-commerce declarations carry no `canonical_mappings` block, need no OpenIM checkout, and run through the same generator unchanged.

## Pointing at an external model (investment domain only)

The install steps at the top of this README are the whole of what most estates need. This one further route exists only for a domain that has an external reference model to validate against, and the investment domain is the one worked example that does.

A declaration's optional `canonical_mappings` block can be checked against a real reference model. Clone the public [Open Investment Model](https://openinvestmentmodel.org) repository anywhere on disk and pass its path to `ergasterion emit --openim-root <path>`. The generator checks the declared attribute lineage for spelling and schema drift. It does not generate or decide a mapping.

With no valid path, the check records a warning in the run summary and continues. Pass `--strict-openim` when a CI run must treat that missing validation as a failure. Whether an attribute maps as declared remains a human judgment. A domain with no external reference model simply omits the block, as the e-commerce example does.

## For engineers

The factory core and per-domain content share the same layer folders: `staging`, `raw_vault`, `business_vault`, `entity_resolution`, `canonical`, and `marts`. The two domains share no entity names, so separate folder trees would add structure without clarifying ownership. The engine boundary is defined by which files carry domain vocabulary:

```text
pyproject.toml       # the engine's OWN packaging manifest: distribution name
                     #   ergasterion-factory, console entry point `ergasterion`
                     #   (ergasterion/cli.py, multiplexing every generator below as
                     #   a subcommand), package data (ergasterion/templates/,
                     #   ergasterion/schemas/). Installing this file's package
                     #   gets you the generator, its scaffold, and its schemas.
ergasterion/             # FACTORY CORE: the generators and their checks, the
                     #   thing the pip package actually ships. Carries no
                     #   domain vocabulary (no customer/fund/deal literals);
                     #   reads whichever ESTATE's domains/*.yml config it is
                     #   pointed at (ergasterion/estate.py's EstateContext resolves
                     #   the estate root and every estate-side path; the
                     #   engine's own templates and vendored JSON Schemas
                     #   resolve package-relative, under ergasterion/templates/
                     #   and ergasterion/schemas/, and are never estate-specific).
                     #   emit.py generates the warehouse models;
                     #   emit_contracts.py generates the ODCS data contracts
                     #   for the served layer; emit_odps.py wraps each
                     #   domain's contracts into one ODPS (Bitol) product
                     #   descriptor; emit_graph.py emits the graph map
                     #   artefacts under graphs/; dialect_lint.py is the
                     #   per-adapter SQL gate emit.py runs automatically and
                     #   `ergasterion lint` also runs standalone; import_odcs.py
                     #   seeds a new source's declaration from a supplier's
                     #   ODCS contract; import_ddl.py seeds a source
                     #   declaration or a domain model config straight from a
                     #   CREATE TABLE statement set; init.py scaffolds a
                     #   brand new, empty estate (`ergasterion init`).
domains/             # PER-DOMAIN model config: one YAML file per domain
                     #   (entities, hubs, links, survivorship, ER scoring,
                     #   relation vocabulary),
                     #   read by ergasterion/emit.py at generation time:
                     #   ecommerce.yml, investment.yml
declarations/        # PER-DOMAIN source descriptions, one file per source:
                     #   cartivo/mercaro/relatio (e-commerce),
                     #   vantora/meridex/portiq/chrono/origo (investment)
seeds/               # PER-DOMAIN made-up source data, one raw_<source>_* file
                     #   set per described source, plus each domain's own
                     #   entity-resolution overlap manifest
models/              # generated + hand-authored warehouse layers, BOTH domains
                     #   side by side in the same layer folders (staging/
                     #   raw_vault/business_vault/entity_resolution/canonical/
                     #   marts). See "What this is not," above, for which
                     #   layers are generated and which are not
macros/              # FACTORY CORE: shared warehouse logic, including the
                     #   three-engine dialect layer. `ergasterion init` copies
                     #   this whole directory into a fresh scaffold, because
                     #   dbt only ever loads macros from inside its own
                     #   project's tree, never from an installed package.
contracts/           # GENERATED ODCS v3.1.0 data contracts, one per served
                     #   table, split by domain (ecommerce/, investment/);
                     #   regenerated by ergasterion/emit_contracts.py, never
                     #   hand-edited. contracts/odps/ holds the sibling ODPS
                     #   (Bitol) v1.0.0 product descriptors, one per domain,
                     #   regenerated by ergasterion/emit_odps.py
graphs/              # GENERATED graph map artefacts, one directory per domain:
                     #   node and edge CSVs, openCypher, SQL/PGQ, GQL DDL,
                     #   JSON graph description, and SQL/PGQ estate binding;
                     #   regenerated by ergasterion/emit_graph.py, never hand-edited
snowflake/           # one-time Snowflake account setup
demo/                # account-free and live demonstrations for both domains
streamlit/           # the human review screen for uncertain record matches
                     #   (investment probabilistic matches and deal decisions)
```

Build and test without an account: `bash scripts/validate_offline.sh`. Run the worked local demonstration with `bash demo/run_offline_demo.sh`. The public [source-description guide](https://github.com/antikas/ergasterion/blob/master/DEMO.md#worked-example-adding-a-whole-new-domain) covers onboarding a source. The [graph vocabulary reference](https://github.com/antikas/ergasterion/blob/master/docs/architecture/ontology-map-lane.md) and [runbook](https://github.com/antikas/ergasterion/blob/master/RUNBOOK.md) cover the remaining operational detail.
