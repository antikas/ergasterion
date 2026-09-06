"""Create an empty Ergasterion data-product estate.

The scaffold contains the dbt project, portable profiles, adapter macros, structural
budgets, and empty declaration, domain, seed, and test directories. All copied assets
ship inside the installed package, so ``ergasterion init`` works without a source
checkout.

Keep the scaffold's project and profile name as ``ergasterion`` unless you also update
the adapter dispatch namespace in ``macros/cross_db.sql``.

Usage:
    ergasterion init <dir>            # scaffold a new, empty estate at <dir>
    ergasterion init <dir> --force    # scaffold into a non-empty <dir> anyway
"""

from __future__ import annotations

import argparse
import shutil
from pathlib import Path

# Support installed-command and direct-script execution.
if __package__ in (None, ""):
    import os as _os, sys as _sys
    _sys.path.insert(0, _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__))))

# Package-relative assets copied into a new estate.
_SCAFFOLD_ROOT = Path(__file__).resolve().parent / "scaffold"

# Every empty directory the scaffold ships is seeded with a `.gitkeep` so a
# consumer's own fresh git init has something to track before they add real content.
_EMPTY_DIRS = ("declarations", "seeds", "tests")

_LICENSE_TEMPLATE = """\
MIT License

Copyright (c) 2026 Your Name Here

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""

GETTING_STARTED_TEMPLATE = """\
# Getting started with this data-product estate

A source system delivers data as files or events: a batch of records plus a manifest
describing that batch, dropped somewhere Ergasterion can read it. Before that data reaches
anyone downstream, three questions need an answer every time. What shape does each
delivered record have? What happens to a record that fails validation? What happens if a
delivery is only partly usable? This estate answers all three the same way for every
source: a declared contract, an immutable record of what arrived, and a queryable,
quality-checked result.

This directory is an empty Ergasterion **estate**. The `ergasterion` command turns
authored declarations into two things: a generated dbt project, one tree per declared
product, with its tests, its published contracts and its lineage; and a local Landing
runtime that ingests, validates, and publishes source-delivered data into a queryable
DuckDB layer.

## The shortest working local journey

1. Describe one source table's physical shape. Point the importer at a `CREATE TABLE`
   statement or an ODCS contract and ask for a draft:

   ```bash
   ergasterion import-ddl your_table.sql --source yoursource --landing source
   ```

   This writes `declarations/yoursource.yml` with the columns, types, and nullability read
   straight from what you gave it, and nothing else: no owner, no schedule, no guessed
   business meaning. The file's `delivery: {kind: draft, reason: delivery_contract_required}`
   states plainly that it is not ready to receive data yet.

2. Fill in the TODOs the draft leaves for you: who owns this data, how it is scheduled,
   what counts as a passing row, and which estate layer it sits in (`layer:`, one of
   `estate.yml`'s own `labels:` keys -- the runtime route looks that key up in the
   translator table rather than inferring it). Flip `delivery.kind` to `production` once
   every TODO is answered. This is the one manual step; nothing here is guessed on your
   behalf.

3. Register a deployment and ingest a file. `runtime/local.yml` ships already computed
   for the walkthrough identity below (`--source reference --table orders`), so these
   commands run as written, with no editing:

   ```bash
   ergasterion plan --project-dir . --source reference --table orders \\
     --binding runtime/local.yml --environment local
   ergasterion contract register --project-dir . --source reference --table orders \\
     --binding runtime/local.yml --environment local
   ergasterion ingest file --project-dir . --source reference --table orders \\
     --binding runtime/local.yml --environment local \\
     --manifest path/to/delivery.manifest.json --payload path/to/delivery.ndjson
   ```

   `ergasterion status`, `ergasterion inspect`, and `ergasterion quarantine --action list`
   read back what happened, without touching anything.

`runtime/local.yml` is the one binding this estate ships with (see below): it names every
local port, fixes one delivery attempt at a time, and points every runtime file at
`runtime/data/`, the one directory this estate's `.gitignore` ignores. Once you register
your own source and table (step 1-2 above, with your own names), the binding's own header
comment says exactly which two fields to update -- `logical_identity` and
`contract_digest` -- to point it at your contract instead of the walkthrough one; the
nine port bindings and resource envelope stay as they are. Run `ergasterion <subcommand>
--help` for a command's full option list.

## What happens to a rejected row, and to an incomplete delivery

A row that fails a declared quality rule (a missing required field, a value outside an
accepted range) is quarantined, not dropped: it keeps a stable pointer back to the exact
raw bytes and position it came from, and `ergasterion quarantine` lists and inspects it.
A delivery whose disposition policy demands every row pass publishes nothing until you
release or fix the failing rows; a delivery that tolerates some failures publishes the
rows that passed and quarantines the rest. Either way the previous good result stays
visible and queryable until a new delivery replaces it -- an incomplete delivery never
overwrites a complete one.

## ODCS import versus runtime delivery

`ergasterion import-ddl` and `ergasterion import-odcs` are design-time tools: they read a
`CREATE TABLE` statement or an existing ODCS contract and seed a starting-point
declaration file for you to review and complete. They never touch the runtime. Once a
declaration's `delivery.kind` is `production`, `ergasterion ingest file` (or `ingest due`
against a due schedule) is the runtime-delivery path: it reads an actual batch of records
plus its manifest, validates and publishes them, and updates the operational state this
estate tracks in `runtime/data/`.

## The current file boundary, and the later connector seam

Today, Ergasterion receives a structured payload file plus its manifest -- `ingest file`
takes both as local paths. It does not yet reach out to a source system and pull data
itself. That is a deliberate boundary, not a missing feature: the validation, contract,
quarantine, and publication logic ahead of that boundary is complete and does not change
when a direct connector is added later. A future connector is a different adapter behind
the same ports this estate's binding already names.

## What is already here

- `dbt_project.yml` defines paths and structural materialisation defaults. Every dbt
  working path (`target-path`, `log-path`, `packages-install-path`) is fixed under
  `runtime/data/dbt/`.
- `packages.yml` declares the dbt Hub packages generated models may call.
- `profiles/profiles.yml` contains environment-driven DuckDB and BigQuery targets. It
  contains no credentials. The DuckDB target defaults to
  `runtime/data/ergasterion.duckdb`.
- `macros/` contains the adapter-dispatch, publication, quarantine, generated-test,
  curation and survivorship helpers the generated models call.
- `estate.yml` names this estate's namespace -- the qualifier every Landing product's
  globally unique identity is built from -- and its declared ownership (`support`
  and `team`), which every published product contract carries. Replace the three
  placeholders before authoring a production contract; the engine supplies no default.
  It also declares this estate's adapters and its translator table: for each layer
  label, which translator renders each pattern. The shipped `reference` label already
  covers the landing composition the reference example below uses, so `plan`,
  `contract`, `ingest`, `reconcile`, and `status` work against it without further setup.
- `runtime/local.yml` is this estate's tracked local `RuntimeBinding`: the nine local
  ports, one parallel delivery attempt at a time, and the resource envelope that makes
  admission deterministic. `runtime/data/` (SQLite, DuckDB, raw objects and receipts,
  scratch space) is the one directory this estate's `.gitignore` ignores -- the binding
  beside it stays tracked, so losing `runtime/data/` never erases the binding you would
  need to restore into it. Local backups belong outside this directory entirely; a backup
  written back inside it would be destroyed by whatever destroyed the original.
- `declarations/products/` holds one seeded product declaration. It is authored input,
  not generated output: edit it, rename it, or delete it once you declare your own.
  `ergasterion emit-products --estate-root .` turns every declaration there into a dbt
  model, its schema documentation and its runtime manifest.
- `seeds/` and `tests/` ship empty, and `declarations/` ships with only the
  seeded product and the target budgets described above. All three are authored-input
  areas: files you write by hand. Everything under `models/`, `contracts/`, `graphs/`, and
  `runtime/data/` is generated or runtime state: files a command writes for you and a
  later run of that same command safely overwrites.
- `declarations/targets/` contains structural budgets, one per deployment
  target keyed by dbt adapter name, plus `interfaces.yml` naming the model paths that
  may materialise as views. `ergasterion emit-products` validates every model it
  writes against these budgets, and `ergasterion structure` validates the whole tree.
- `LICENSE` is an MIT template. Replace the holder before publishing your estate.

## Declaring a product

A **product declaration** is one `declarations/products/<name>.yml` file. It names the
label the product sits in, the source contracts it reads, the ordered patterns it
composes, and the shape of what it publishes. `estate.yml` maps that label onto the
profile the composition must satisfy and onto the translator that renders each pattern,
so a declaration names no technology and no platform anywhere.

The seeded declaration composes the reference `derivation` profile against a fixture-bound
source, so it emits before anything upstream of it exists:

```bash
ergasterion emit-products --estate-root .
ergasterion emit-products --estate-root . --check
```

The first command writes `models/products/` and `manifests/products/` and prints one
summary line per product: its label, profile, shape, the adapters it was gated for, and
how many artefacts it emitted. The second reports drift against what is on disk without
writing. Changing a label's entry in `estate.yml`'s translator table changes which
translator owns a pattern, with no change to any declaration.

## Generating contracts, a product descriptor and a graph map

Every one of these reads the product declarations, never a compiled dbt manifest:

```bash
ergasterion contracts --estate-root .       # one ODCS v3.1.0 contract per declared product
ergasterion odps --estate-root .            # one ODPS (Bitol) v1.0.0 descriptor per product
ergasterion product-graph --estate-root .   # the estate product graph
```

Each command regenerates its output by default. Pass `--check` to report on-disk drift
without writing.

### Seed column types are authored, not generated

dbt seeds need a `+column_types:` pin per column so a header-only or all-blank CSV column
never gets type-inferred as something wrong (a common failure mode: an all-numeric-looking
id column silently becomes an integer and loses its leading zeroes). This is a genuinely
manual step: add a `seeds:` block to `dbt_project.yml` yourself, one entry per seed
table, e.g.:

```yaml
seeds:
  ergasterion:
    +quote_columns: false
    raw_yoursource_things:
      +column_types:
        id: string
        name: string
```

There is no generator for this block. The engine does not guess column types from a CSV.

## Running dbt without network access

dbt reads installed packages from `packages-install-path` in `dbt_project.yml`, fixed here
at `runtime/data/dbt/packages`. If that directory already contains the packages
`packages.yml` declares, parse and build commands do not need a network fetch. There is no
`dbt deps --packages-install-path` command-line option; `packages-install-path` is a
`dbt_project.yml` project-config key, set once, not a flag passed per command.

## Where the exact rules live

The generated `ergasterion/schemas/landing-product-v1.schema.json` inside this package
(present in every install, source checkout or not) is the exact machine-checkable shape
every Landing declaration, contract, and runtime record follows. This project's own README
links the fuller architecture write-up for the reasoning behind those rules.
"""


def scaffold(dest: Path, *, force: bool = False) -> list[Path]:
    """Create the empty estate at ``dest``. Returns every path written, sorted.

    ``dest`` must not already exist as a non-empty, non-scaffold directory unless
    ``force`` is set (an existing scaffold is always safe to re-run over -- every write
    here is either a fresh file or an overwrite of the same generated/templated content).
    """
    # Validate all package data before creating the destination.
    for required in (
        "dbt_project.yml", "packages.yml", "profiles.yml", "macros", "targets",
        "estate.yml", "runtime", "products", ".gitignore",
    ):
        if not (_SCAFFOLD_ROOT / required).exists():
            raise SystemExit(
                f"ergasterion init: engine scaffold data missing ({_SCAFFOLD_ROOT / required}) "
                f"-- the installed package is incomplete; reinstall the engine"
            )

    dest = Path(dest).resolve()
    if dest.exists() and dest.is_dir() and any(dest.iterdir()) and not force:
        raise SystemExit(
            f"ergasterion init: {dest} already exists and is not empty "
            f"(pass --force to scaffold into it anyway)"
        )
    if dest.exists() and not dest.is_dir():
        raise SystemExit(f"ergasterion init: {dest} exists and is not a directory")
    dest.mkdir(parents=True, exist_ok=True)

    written: list[Path] = []

    shutil.copy2(_SCAFFOLD_ROOT / "dbt_project.yml", dest / "dbt_project.yml")
    written.append(dest / "dbt_project.yml")

    shutil.copy2(_SCAFFOLD_ROOT / "packages.yml", dest / "packages.yml")
    written.append(dest / "packages.yml")

    (dest / "profiles").mkdir(parents=True, exist_ok=True)
    shutil.copy2(_SCAFFOLD_ROOT / "profiles.yml", dest / "profiles" / "profiles.yml")
    written.append(dest / "profiles" / "profiles.yml")

    shutil.copy2(_SCAFFOLD_ROOT / "estate.yml", dest / "estate.yml")
    written.append(dest / "estate.yml")

    shutil.copy2(_SCAFFOLD_ROOT / ".gitignore", dest / ".gitignore")
    written.append(dest / ".gitignore")

    # The tracked runtime binding. Its relative data root, runtime/data/, is the ONLY
    # thing the estate's own .gitignore (just copied above) ignores -- runtime/local.yml
    # itself stays tracked. The directory is created empty here (not by a later command)
    # so a fresh estate's runtime/ layout is visible immediately.
    runtime_dest = dest / "runtime"
    runtime_dest.mkdir(parents=True, exist_ok=True)
    shutil.copy2(_SCAFFOLD_ROOT / "runtime" / "local.yml", runtime_dest / "local.yml")
    written.append(runtime_dest / "local.yml")

    macros_dest = dest / "macros"
    if macros_dest.exists():
        shutil.rmtree(macros_dest)
    shutil.copytree(_SCAFFOLD_ROOT / "macros", macros_dest)
    written.extend(sorted(macros_dest.rglob("*")))

    for name in _EMPTY_DIRS:
        directory = dest / name
        directory.mkdir(parents=True, exist_ok=True)
        keep = directory / ".gitkeep"
        keep.write_text("", encoding="utf-8")
        written.append(keep)

    # Per-target structural budgets and interface boundaries.
    targets_dest = dest / "declarations" / "targets"
    targets_dest.mkdir(parents=True, exist_ok=True)
    for target_file in sorted((_SCAFFOLD_ROOT / "targets").glob("*.yml")):
        shutil.copy2(target_file, targets_dest / target_file.name)
        written.append(targets_dest / target_file.name)

    # The seeded product declaration. declarations/products/ is the tree the
    # product route reads, so a fresh estate validates and emits through
    # `ergasterion emit-products` before its owner authors anything.
    products_dest = dest / "declarations" / "products"
    products_dest.mkdir(parents=True, exist_ok=True)
    for seed_file in sorted((_SCAFFOLD_ROOT / "products").glob("*.yml")):
        shutil.copy2(seed_file, products_dest / seed_file.name)
        written.append(products_dest / seed_file.name)

    license_path = dest / "LICENSE"
    license_path.write_text(_LICENSE_TEMPLATE, encoding="utf-8")
    written.append(license_path)

    getting_started = dest / "GETTING-STARTED.md"
    getting_started.write_text(GETTING_STARTED_TEMPLATE, encoding="utf-8")
    written.append(getting_started)

    return sorted(set(written))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("dir", type=Path, help="Directory to scaffold the new, empty estate into.")
    parser.add_argument(
        "--force", action="store_true",
        help="Scaffold into a non-empty directory anyway (existing scaffold content is overwritten).",
    )
    args = parser.parse_args()

    written = scaffold(args.dir, force=args.force)
    dest = Path(args.dir).resolve()
    print(f"scaffolded a new estate at {dest} ({len(written)} file(s))")
    print("next: read GETTING-STARTED.md")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
