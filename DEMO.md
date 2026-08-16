# Source Declaration Demo

To onboard a source:

1. Add synthetic or approved raw input files under `seeds/`.
2. Write a source declaration at `declarations/<source>.yml` (by hand, or seeded from a
   supplier's ODCS contract -- see "Worked example: hand me your data contract," below).
3. Run `python ergasterion/emit.py`.
4. Validate metadata generation with `dbt parse --profiles-dir profiles --no-partial-parse`.
5. Run `dbt build` only in an environment with the target warehouse available.

The declaration is the onboarding contract for everything the emitter generates: the
raw tables, typed staging projections, deterministic ER branches, bridge joins, and
the raw-vault entities the source feeds. The Python emitter is deterministic:
templates plus declarations produce dbt SQL, with no LLM in the generation path.

A single source per entity is the normal, complete case. You describe your sources and
the factory builds the warehouse from them: typed staging, the append-only history
vault, golden records, the served tables, contracts, and the graph map, all from one
source if that is all you have. The entity-resolution and survivorship machinery earns
its keep when several sources describe the same real-world thing. The `entity_resolution`
branch discussed below is what a source opts into when it overlaps existing records,
never a precondition for onboarding.

One distinction matters here: the pipeline regenerates from the declaration alone,
with no hand edits to any generated file. The entity resolution ground truth does
not. If the source declares an `entity_resolution` branch, the pipeline can resolve
its records against existing funds, but scoring how well it resolved them needs a
human-labelled answer key: `seeds/entity_resolution_overlap_manifest.csv`. That
manifest is a labelled-data input the pipeline reads, not pipeline code the emitter
produces, and it is hand-authored, one row per source record whose true fund
identity is already known. Adding an ER-participating source without adding its
rows to the manifest is still a valid emit, but the build will fail: the precision
test that scores entity resolution treats an unlabelled resolved record as ground
truth gone missing, on purpose, and stops the pipeline rather than pass silently.

A declaration in the **investment domain** can also carry a `canonical_mappings`
block per entity -- an **optional, investment-domain-specific** onboarding aid, not a
factory-wide requirement. That block is descriptive-only: it documents the intended
attribute lineage to the OpenIM canonical model for onboarding review, and
`ergasterion/emit.py` validates it for spelling and schema drift against the canonical
model *when one is present on disk* (`--openim-root`), skipping with a warning rather
than failing when it is not. It does not generate or drive anything: the canonical
layer itself (`models/canonical/*.sql`) is hand-authored, not templated from the
declaration. A domain with no external model to validate against simply omits the
block; the e-commerce domain's declarations do exactly that (see "Worked example:
adding a whole new domain," below).

The fourth-source proof is `CHRONO`, declared in `declarations/chrono.yml` with
raw files named `seeds/raw_chrono_*.csv`. It uses Chronograph-shaped column names,
overlaps existing funds through LEI and shared external IDs, and adds a new
synthetic fund, `Helio Climate Fund I`.

## Worked example: adding a whole new domain

The e-commerce customer view shows how to define a domain whose vocabulary is completely separate from the investment example. The engine (`ergasterion/emit.py` and `ergasterion/templates/`) carries no entity or source vocabulary. A domain lives in one model configuration plus its declarations, seeds, and labelled identity manifest. Each step below points to a working file in this repository.

The repository contains two worked domains, so the example directories are populated. A new estate created with `ergasterion init <directory>` contains the same directory structure, the shared macros, and an empty project ready for its first domain. Use `domains/ecommerce.yml` as the concrete example for your own `domains/<your-domain>.yml`.

1. **Domain model config** (`domains/<domain>.yml`, e.g. `domains/ecommerce.yml`): define the payload and hashed keys for `customer`, `product`, `order`, and `order_line`; then define the hubs, links, golden records, and deterministic customer match keys. Customers match first on a shared loyalty id and then on a normalised email. All domain files are merged at generation time, and the loader rejects duplicate section keys.
2. **Declarations**, one per source (`declarations/cartivo.yml` the web storefront, `declarations/mercaro.yml` the marketplace, `declarations/relatio.yml` the CRM with only partial customer coverage): the same `vault_entities` shape every investment-domain declaration uses, pointing at the new domain's entities instead. A domain needs no `canonical_mappings` block at all if it has no external model to validate against -- e-commerce has none, which is itself part of the proof that the OpenIM-facing validation is optional and investment-specific (see the README's "What this is not").
3. **Seeds**: invented source data per feed (customers/products/orders), with deliberate cross-source overlaps and disagreements seeded on purpose, the same convention every investment-domain source follows -- a shared loyalty id in two feeds, a case-different email in two others, a customer visible in only one feed (the partial-coverage CRM case).
4. **The entity-resolution manifest, staged BEFORE the first build**: `seeds/<domain>_er_overlap_manifest.csv` (e-commerce: `seeds/customer_er_overlap_manifest.csv`), one row per source record whose true customer identity is already known by hand. Exactly the same rule DEMO's opening section states for the investment domain applies here: an ER-participating entity with no manifest rows is a valid emit that fails the build, on purpose, because the precision test that scores resolution treats an unlabelled resolved record as ground truth gone missing.
5. Run `python ergasterion/emit.py`, then `dbt parse --profiles-dir profiles --no-partial-parse` for the structural check, then `dbt build` against a configured warehouse target.

**What stays hand-authored:** the clean consumable layer, including `canonical_customer`, `canonical_product`, the dimensions, and `fact_order`, is designed by a person on top of the generated source-facing pipeline. The investment domain follows the same boundary.

Customer resolution is deterministic in this domain. Records that share neither a loyalty id nor a normalised email remain separate. The customer path does not enter the probabilistic review queue. The review console displays probabilistic investment matches and deal decisions.

## Worked example: hand me your data contract

Every source onboarded above started from a blank `declarations/<source>.yml`, written
column by column, by hand. If a supplier can instead hand you an **ODCS contract** --
a YAML document following the Bitol Open Data Contract Standard (ODCS), a
vendor-neutral way of describing a dataset's schema that a growing set of
data-catalogue and governance tools already speak (Databricks, Collibra,
OpenMetadata, `datacontract-cli`) -- `ergasterion/import_odcs.py` turns it into a
declaration skeleton mechanically, in seconds, instead of by hand.

What it does: reads the contract's schema section (each column's name, type, and
whether it is required, unique, or a primary key) and writes out a
`declarations/<source>.yml` with the projection stubs and `seed_tests`/`model_tests`
already filled in from that. What it deliberately does **not** do: guess how this
source's records map onto this factory's vault entities, how they resolve against
other sources, or how they should be prioritised in survivorship if two sources
disagree about the same fact. No ODCS contract carries that information -- it is
domain knowledge that lives with the person onboarding the source, not with the
supplier who wrote the contract. The seeder marks every one of those gaps with an
explicit `# TODO` comment and a worked example lifted from a real declaration, so
nothing is silently assumed on your behalf.

Try it against one of this repository's own contracts (`ergasterion/emit_contracts.py`'s
output, `contracts/ecommerce/dim_customer_segment.odcs.yml`) -- a stand-in for "a
supplier just sent me their ODCS contract":

```bash
python ergasterion/import_odcs.py contracts/ecommerce/dim_customer_segment.odcs.yml --source acme_supplier
```

This writes `declarations/acme_supplier.yml`. Open it: the `projection` list already
has one entry per column, cast to the right type (`cast(... as string)`, or
`{{ dpf_safe_cast('...', 'date') }}` for anything that can fail to parse);
`seed_tests`/`model_tests` already carry `not_null`/`unique` wherever the contract
declared a column required or unique. The `vault_entities: []` at the bottom of each
table is empty on purpose, with a commented-out worked example directly above it
showing the shape to fill in -- see `declarations/cartivo.yml` for a complete one.
Fill that in (and the two `# TODO` blocks at the end of the file, `entity_resolution`
and `canonical_mappings`, if they apply to this source), then run
`python ergasterion/emit.py` as usual -- the file the seeder wrote is a normal,
hand-editable declaration from that point on, not a generated artefact.

The seeder refuses a contract it cannot safely read, naming the exact problem instead
of guessing:

```bash
python ergasterion/import_odcs.py some_old_contract.yml --source acme_supplier
# FAIL: some_old_contract.yml: ODCS v2.2.2 is not supported -- this is a pre-v3 contract.
# ODCS v3.0.0 was a breaking rewrite over v2 (uuid->id, quantumName->dataProduct, ...).
# Upgrade the contract to ODCS v3.x before importing -- see
# https://bitol-io.github.io/open-data-contract-standard/latest/ .
```

A contract missing its schema section, missing a column name, or declaring a `kind`
other than `DataContract` fails the same way: one line naming what is wrong, before
anything is written to disk.

## Worked example: seeding from raw DDL

Not every source hands you an ODCS contract. Often what you have instead is the raw
`CREATE TABLE` statements a source system already uses -- or a model you want to onboard
that is only described that way. `ergasterion/import_ddl.py` (`ergasterion import-ddl`) reads
that DDL directly and writes the same kind of TODO-stubbed starting point `import_odcs.py`
writes from a contract, the same refusal to guess anything the input does not state.

It has two modes, because a DDL statement set can describe two different things.

**`--mode feed`** reads one source system's own `CREATE TABLE` set and writes a
`declarations/<source>.yml` stub: one projection entry per column, cast to the right
type, with `not_null` / `unique` / primary-key tests filled in from the DDL's own
`NOT NULL` / `PRIMARY KEY` / `UNIQUE` constraints. Save this to a file, `customers.sql`:
```sql
CREATE TABLE customers (
    id INTEGER PRIMARY KEY,
    email VARCHAR(255) NOT NULL,
    full_name VARCHAR(200),
    signup_date DATE NOT NULL,
    is_active BOOLEAN
);
```
then run:
```bash
ergasterion import-ddl customers.sql --mode feed --source acme_crm
```
This writes `declarations/acme_crm.yml`. Which real-world thing each table's records
describe, and how they resolve against other sources, is left as an explicit `# TODO`
block -- no DDL states that either, so nothing here guesses on your behalf.

**`--mode model`** reads a whole domain's `CREATE TABLE` set, one that declares its own
`PRIMARY KEY` and `FOREIGN KEY` constraints, and writes a `domains/<name>.yml` stub: one
entity per table, with hub and link config derived from the primary/foreign-key
structure (a table whose primary key is entirely foreign keys becomes a link; a plain
foreign key elsewhere becomes a relationship between two entities). Save this to
`model.sql`:
```sql
CREATE TABLE customer (
    customer_id INTEGER PRIMARY KEY,
    email VARCHAR(255) NOT NULL
);

CREATE TABLE order_header (
    order_id INTEGER PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    order_date DATE NOT NULL,
    FOREIGN KEY (customer_id) REFERENCES customer(customer_id)
);
```
then run:
```bash
ergasterion import-ddl model.sql --mode model --domain acme
```
This writes `domains/acme.yml` with a `customer` entity, an `order_header` entity, and a
link (`order_header_customer`) connecting them, the same shape `domains/ecommerce.yml`
uses for its own order-to-customer link. Survivorship rules, entity-resolution match
keys, and the map-lane relation vocabulary are never inferable from a key structure
alone, so those stay `# TODO` blocks.

Both modes refuse to overwrite a destination that already exists unless you pass
`--force` -- and `--force` overwrites, it never merges with hand edits you have already
made, the same one-way seeding rule `import_odcs.py` follows. Fill in the `# TODO` blocks,
then run `python ergasterion/emit.py` (or `ergasterion emit`) as usual: the seeded file is a
normal, hand-editable declaration or domain config from that point on, not a generated
artefact.

## Worked example: adding a new entity type (legal vehicle / SPV)

Onboarding a whole new **entity type**, not just a new source of an existing one,
is a code-plus-declaration change, and the SPV/legal-vehicle layer is the worked
example. The steps:

1. **Emitter configs** (`ergasterion/emit.py`): add the entity to `ENTITY_CONFIGS`
   (payload + hashed columns), and, for a first-class entity, `HUB_CONFIGS`,
   `LINK_CONFIGS`, and `BV_CONFIGS`. `legal_vehicle` adds `hub_legal_vehicle`,
   `link_investment_vehicle`, and `bv_legal_vehicle_golden_record`, plus a
   satellite-only `legal_vehicle_cash_flow` rider (the same shape `fund_cash_flow`
   uses against fund).
2. **Declarations**: add `vault_entities` blocks in at least two sources so
   survivorship has something to arbitrate, plus the vehicle and vehicle-grain
   cash-flow seeds. The seeds follow the subtle-discrepancy convention (a re-spelt
   name, a missing date) and one vehicle nests under another via `parent_vehicle_id`.
3. **Keying**: `golden_legal_vehicle_key` is a bridge-select hash of the declared
   natural vehicle id: `stable_golden_key('legal_vehicle', vehicle_natural_id)`.
   There is **no entity resolution for vehicles** and no `res_legal_vehicle` model
   ; the bridge joins `res_fund` only
   to resolve the vehicle's parent fund. Because vehicles do not enter entity
   resolution, they need **no** rows in `entity_resolution_overlap_manifest.csv`.
4. **Hand-authored on top**: `canonical_legal_vehicle`, `dim_legal_vehicle`, and the
   vehicle→fund aggregation bridge are hand-authored, like every canonical/mart model.
5. **Named test**: `assert_vehicle_to_fund_cash_flow_conservation` checks the
   vehicle-grain flows reconcile to the fund's own total within epsilon.

Run `python ergasterion/emit.py`, then `dbt parse` / `dbt build` as with any source.

If the source declares an `entity_resolution` branch, add its labelled manifest rows
to `seeds/entity_resolution_overlap_manifest.csv` before running the build. The
precision test rejects resolved source records that have no labelled answer.

## Worked example: operating the deal pipeline

The deal pipeline uses the same declaration, generation, resolution, and golden-record
path as other entities. It adds one source, `ORIGO`. Deal identity matching is
source-local within ORIGO; fund identity matching spans all declared fund sources.

The demo arc, run end to end:

1. **Declare.** `declarations/origo.yml` and `seeds/raw_origo_deals.csv` describe
   the source. `python ergasterion/emit.py`
   regenerates `hub_deal`, `res_deal`, `bv_deal_golden_record`, and the stage-history
   and approvals models on top.
2. **Build**, then open the management console (`RUNBOOK.md` §9) and select the
   **Deal Approvals** tab.
3. **The deferred deal.** `ORIGO-EXT-001`, "Project Atlas", sits awaiting a decision. Its
   investment committee deferred it in March (the seeded decision,
   `DEC-ORIGO-EXT-001-01`), so it is still at the DECISION stage with no terminal
   call made. This is the live-approval moment: approve it with conditions.
4. **Approve with conditions**, then **rebuild**. A targeted `dbt build
   --profiles-dir profiles --target snowflake --select int_deal_latest_decision+
   int_entity_resolution_latest_decision+` takes well under a minute. The next build
   derives Project Atlas's next stage row (COMMITTED) from that decision
   (`int_deal_stage_from_decision.sql`); the deal drops out of the awaiting-decision queue
   because `deal_approval_queue` only ever lists deals still at DECISION with no
   terminal decision recorded.
5. **Pipeline Browser tab** shows the funnel with Project Atlas at COMMITTED;
   the awaiting-decision queue count the analyst saw a moment ago has emptied by one.

**What the example proves:**

- **The three fixture decisions set the starting state.**
  Project Atlas deferred (`DEC-ORIGO-EXT-001-01`), and the merged Cedar Renewables
  pair (`ORIGO-EXT-DUP-B`) first deferred then approved with conditions
  (`DEC-ORIGO-EXT-DUP-B-01`/`-02`), which is why Cedar Renewables already sits at
  COMMITTED before anyone touches the console. Cedar Renewables carries no fund
  conversion (`converted_record_type` is blank for both its source rows) -- only
  the direct-lending deal `ORIGO-EXT-002` ("Orion Credit Facility") converts, into
  the existing `Orion Credit Opportunities I` fund golden key.
- **Approval uses a parameterised insert.** The write-back goes to
  `deal_decision_log`. The reset procedure is in the Snowflake demo section of
  `RUNBOOK.md`.
- **The ER Review Queue tab's Riverstone pair is a different kind of moment.**
  `D-009`/`D-010` ("Riverstone Logistics" / "Riverstone Logistix") share no external
  id, only similar names, so they sit in the SAME tier-2 middle-band review queue an
  uncertain fund pair would, with the same composite-score decomposition and a
  golden-key **preview**. Reviewing or even approving that pair in the ER tab does
  does not mint a row in the deal pipeline immediately. A merge decision changes
  `res_deal` and the pipeline mart on the next `dbt build`.
