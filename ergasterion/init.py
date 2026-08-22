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
_EMPTY_DIRS = ("domains", "declarations", "seeds", "tests")

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
authored declarations into two things: a generated dbt pipeline (staging through
entity-resolution and business-vault survivorship), and a local Bronze runtime that
ingests, validates, and publishes source-delivered data into a queryable DuckDB layer.

## The shortest working local journey

1. Describe one source table's physical shape. Point the importer at a `CREATE TABLE`
   statement or an ODCS contract and ask for a draft:

   ```bash
   ergasterion import-ddl your_table.sql --mode feed --source yoursource --landing source
   ```

   This writes `declarations/yoursource.yml` with the columns, types, and nullability read
   straight from what you gave it, and nothing else: no owner, no schedule, no guessed
   business meaning. The file's `delivery: {kind: draft, reason: delivery_contract_required}`
   states plainly that it is not ready to receive data yet.

2. Fill in the TODOs the draft leaves for you: who owns this data, how it is scheduled,
   what counts as a passing row. Flip `delivery.kind` to `production` once every TODO is
   answered. This is the one manual step; nothing here is guessed on your behalf.

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
- `packages.yml` declares `dbt_utils` and `automate_dv`.
- `profiles/profiles.yml` contains environment-driven DuckDB, Snowflake, and BigQuery
  targets. It contains no credentials. The DuckDB target defaults to
  `runtime/data/ergasterion.duckdb`.
- `macros/` contains the adapter, normalisation, entity-resolution, and survivorship
  helpers called by generated models.
- `estate.yml` names this estate's namespace -- the qualifier every Bronze product's
  globally unique identity is built from. Replace the placeholder before authoring a
  production contract.
- `runtime/local.yml` is this estate's tracked local `RuntimeBinding`: the nine local
  ports, one parallel delivery attempt at a time, and the resource envelope that makes
  admission deterministic. `runtime/data/` (SQLite, DuckDB, raw objects and receipts,
  scratch space) is the one directory this estate's `.gitignore` ignores -- the binding
  beside it stays tracked, so losing `runtime/data/` never erases the binding you would
  need to restore into it. Local backups belong outside this directory entirely; a backup
  written back inside it would be destroyed by whatever destroyed the original.
- `domains/`, `declarations/`, `seeds/`, and `tests/` are empty authored-input areas: files
  you write by hand. Everything under `models/`, `contracts/`, `graphs/`, and
  `runtime/data/` is generated or runtime state: files a command writes for you and a
  later run of that same command safely overwrites.
- `declarations/targets/` contains structural budgets, one per deployment
  target keyed by dbt adapter name, plus `interfaces.yml` naming the model paths that
  may materialise as views. `ergasterion emit` validates every generated and
  hand-authored model against these budgets.
- `LICENSE` is an MIT template. Replace the holder before publishing your estate.

## Building your first domain (the vault pipeline)

A **domain** is one `domains/<name>.yml` file declaring the entities, hubs, links, and
survivorship rules the engine turns into a dbt pipeline. A **source** is one
`declarations/<source>.yml` file declaring one incoming feed (a table's columns, and how
each column maps onto the entities the domain declares). This is a separate, unchanged
path from the Bronze journey above: a `landing: {kind: seed}` table (the default, and
every table an importer seeds without `--landing source`) still loads from a dbt seed CSV
and flows straight through staging into the vault, exactly as before.

Add a domain, at least one source declaration, and either a seed or external-source
definition. Then run:

```bash
ergasterion emit --estate-root .
dbt deps --profiles-dir profiles
dbt parse --profiles-dir profiles -t duckdb --no-partial-parse
```

This regenerates the full pipeline: staging models, an Automate-DV raw-vault layer
(hubs/links/satellites), business-vault survivorship (golden records), and
entity-resolution models, under `models/`.

### Seed column types are authored, not generated

dbt seeds need a `+column_types:` pin per column so a header-only or all-blank CSV column
never gets type-inferred as something wrong (a common failure mode: an all-numeric-looking
id column silently becomes an integer and loses its leading zeroes). This is a genuinely
manual step: add a `seeds:` block to `dbt_project.yml` yourself, one entry per raw seed
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

### The one manual layer

Everything from staging through business-vault survivorship is generated. The served
tables under `models/canonical/` and `models/marts/` are hand-authored dbt SQL containing
your business logic. Add each served table to the domain's `odcs.products` map so the
contract and product descriptor commands can publish its interface.

## Generating contracts, a product descriptor, and a graph map

`dbt parse` writes its manifest to `runtime/data/dbt/target/manifest.json` (the fixed
`target-path` set above). `contracts`, `odps`, and `graph` read the manifest from the
conventional `target/manifest.json` location, so once `ergasterion emit` has run and
`dbt parse` succeeds, copy the manifest there first:

```bash
mkdir -p target
cp runtime/data/dbt/target/manifest.json target/manifest.json
ergasterion contracts --estate-root .   # one ODCS v3.1.0 contract per served table
ergasterion odps --estate-root .        # one ODPS (Bitol) v1.0.0 product descriptor per domain
ergasterion graph --estate-root .       # one property-graph artefact suite per domain
```

Each command regenerates its output by default. Pass `--check` to report on-disk drift
without writing.

## Running dbt without network access

dbt reads installed packages from `packages-install-path` in `dbt_project.yml`, fixed here
at `runtime/data/dbt/packages`. If that directory already contains `dbt_utils` and
`automate_dv`, parse and build commands do not need a network fetch. There is no
`dbt deps --packages-install-path` command-line option; `packages-install-path` is a
`dbt_project.yml` project-config key, set once, not a flag passed per command.

## Where the exact rules live

The generated `ergasterion/schemas/bronze-product-v1.schema.json` inside this package
(present in every install, source checkout or not) is the exact machine-checkable shape
every Bronze declaration, contract, and runtime record follows. This project's own README
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
        "estate.yml", "runtime", ".gitignore",
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
