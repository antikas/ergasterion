# Onboarding a source, and declaring a product from it

When a new feed arrives, you need to record what the source sends before you can declare
what the warehouse should publish from it. Ergasterion keeps those as two separate,
reviewable decisions.

A **source** is an incoming feed. It is declared once, at `declarations/<source>.yml`,
describing the tables and columns a delivered batch carries, how each one is typed, and
how the batch reaches the estate. Two importers below seed that file from what a supplier
already has, so nobody types a column list twice.

A **product** is what the engine builds. It is declared at
`declarations/products/<domain>/<name>.yml`: the layer label it sits in, the contracts it
consumes, the ordered patterns it composes, and the shape of what it publishes.
`ergasterion emit-products` turns every declaration there into dbt models, their schema
documentation, their generated tests, the product's runtime manifest, its published
contract and descriptor, and its node in the estate graph.

To onboard a source and publish something from it:

1. Make the delivered data reachable. Add approved or synthetic input files under `seeds/`,
   or bind a delivered relation through a Landing Product Contract
   ([`docs/specifications/landing-product-v1.md`](docs/specifications/landing-product-v1.md)
   is its field-by-field reference; [`RUNBOOK.md`](RUNBOOK.md) section 4 is the operator
   command sequence).
2. Write the source declaration at `declarations/<source>.yml`, by hand or seeded from a
   supplier's ODCS contract or the source system's raw DDL. Both are worked below.
3. Declare one product per thing you want published, under `declarations/products/`.
4. Run `ergasterion emit-products`.
5. Check the generated project with `dbt parse --profiles-dir profiles --no-partial-parse`.
6. Run `dbt build` against the reference adapter, or prepare it for a deployment adapter.

The engine is deterministic. Declarations plus the estate's own configuration produce the
whole project, with no model in the generation path. Nothing under `models/`, `contracts/`,
`manifests/` or `graphs/` is hand-authored: every file there carries the generated marker
and is rewritten from its declaration on the next run.

A product declaration names no technology and no platform. `estate.yml` maps each layer
label onto the profile a composition must satisfy. It also maps each pattern to its
translator. Changing that ownership is an estate configuration change. The engine and
product declarations stay unchanged.

## Worked example: hand me your data contract

A supplier may already have an **ODCS contract**, a YAML document that follows the Bitol
Open Data Contract Standard. ODCS describes a dataset's schema in a vendor-neutral format
understood by a range of tools. `ergasterion import-odcs` turns that contract into a
source-declaration skeleton.

It reads each column's name, type, and required, unique or primary-key status. It writes a
`declarations/<source>.yml` with projection stubs and the matching `seed_tests` and
`model_tests`. The input contract cannot say which products should read the source, how
records match across sources, or which source wins a disagreement. The person onboarding
the source owns those decisions. The seeder marks each gap with an explicit `# TODO` and a
worked example from a real declaration.

Try it against one of this repository's own generated contracts, a stand-in for a supplier
sending you theirs:

```bash
ergasterion import-odcs contracts/products/reference/customer_segment/customer_segment.odcs.yml --source acme_supplier
```

This writes `declarations/acme_supplier.yml`. Open it: the `projection` list already has
one entry per column, cast to the right type; `seed_tests` and `model_tests` already carry
`not_null` and `unique` wherever the contract declared a column required or unique. Fill in
the `# TODO` blocks, then declare the products that read the source. The imported file is a
normal, hand-editable declaration from that point on, not a generated artefact.

The seeder refuses a contract it cannot safely read, naming the exact problem instead of
guessing:

```bash
ergasterion import-odcs some_old_contract.yml --source acme_supplier
# FAIL: some_old_contract.yml: ODCS v2.2.2 is not supported -- this is a pre-v3 contract.
# ODCS v3.0.0 was a breaking rewrite over v2 (uuid->id, quantumName->dataProduct, ...).
# Upgrade the contract to ODCS v3.x before importing -- see
# https://bitol-io.github.io/open-data-contract-standard/latest/ .
```

A contract missing its schema section, missing a column name, or declaring a `kind` other
than `DataContract` fails the same way: one line naming what is wrong, before anything is
written to disk.

## Worked example: seeding from raw DDL

Not every source hands you an ODCS contract. Often what you have instead is the raw
`CREATE TABLE` statements the source system already uses. `ergasterion import-ddl` reads
that DDL directly and writes the same kind of TODO-stubbed starting point, with the same
refusal to guess anything the input does not state.

It reads one source system's own `CREATE TABLE` set and writes a
`declarations/<source>.yml` stub. Each column becomes one projection entry, cast to the
right type. The DDL's own `NOT NULL`, `PRIMARY KEY` and `UNIQUE` constraints supply the
`not_null`, `unique` and primary-key tests. Save this to a file, `customers.sql`:

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
ergasterion import-ddl customers.sql --source acme_crm
```

This writes `declarations/acme_crm.yml`. Which real-world thing each table's records
describe, and how they resolve against other sources, is left as an explicit `# TODO`
block: no DDL states that either, so nothing here guesses on your behalf.

`--landing source` emits a landing and delivery draft carrying the physical schema alone.
Use it when batches arrive through the ingestion runtime. The default creates a seed-backed
declaration for local fixtures.

The importer refuses to overwrite a destination that already exists unless you pass
`--force`, and `--force` overwrites: it never merges with hand edits you have already made,
the same one-way seeding rule `import-odcs` follows.

## What keeps the example honest

The e-commerce example contains 33 products across five profiles, including four reference
products. Three overlapping customer feeds pass through the complete route. Its evidence
includes:

- **emission**, byte-stable across two runs, with every declaration validated against the
  profile its label admits and every occurrence routed through the estate's translator
  table;
- **the per-adapter gates**, run for every adapter the estate declares: parse, dialect and
  the structural budgets under `declarations/targets/`;
- **an executed DuckDB build** of the repository's two worked domains, with every generated
  test and all 40 known-answer assertions passing;
- **the contracts, the descriptors and the graph**, generated from the same declarations,
  schema-validated and checked for drift on every run.

`bash scripts/validate_engine_architecture.sh` runs the thirteen architecture acceptance
checks over that evidence and prints one line per check.

The importers make no business decision for the user. Their output is an editable starting
file, and a person must resolve every marked gap before publishing a product from it.
