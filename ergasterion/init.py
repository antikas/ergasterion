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

This directory is an empty Ergasterion **estate**. The `ergasterion` command reads
source and domain declarations here, then generates the repeatable parts of a dbt data
product: staging, vault, entity-resolution, survivorship, contract, descriptor, and graph
artefacts.

## What is already here

- `dbt_project.yml` defines paths and structural materialisation defaults.
- `packages.yml` declares `dbt_utils` and `automate_dv`.
- `profiles/profiles.yml` contains environment-driven DuckDB, Snowflake, and BigQuery
  targets. It contains no credentials.
- `macros/` contains the adapter, normalisation, entity-resolution, and survivorship
  helpers called by generated models.
- `domains/`, `declarations/`, `seeds/`, and `tests/` are empty authored-input areas.
- `declarations/targets/` contains structural budgets, one per deployment
  target keyed by dbt adapter name, plus `interfaces.yml` naming the model paths that
  may materialise as views. `ergasterion emit` validates every generated and
  hand-authored model against these budgets.
- `LICENSE` is an MIT template. Replace the holder before publishing your estate.

## Building your first domain

A **domain** is one `domains/<name>.yml` file declaring the entities, hubs, links, and
survivorship rules the engine turns into a dbt pipeline. A **source** is one
`declarations/<source>.yml` file declaring one incoming feed (a table's columns, and how
each column maps onto the entities the domain declares).

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

Once `ergasterion emit` has run and `dbt parse` succeeds against the result:

```
ergasterion contracts --estate-root .   # one ODCS v3.1.0 contract per served table
ergasterion odps --estate-root .        # one ODPS (Bitol) v1.0.0 product descriptor per domain
ergasterion graph --estate-root .       # one property-graph artefact suite per domain
```

Each command regenerates its output by default. Pass `--check` to report on-disk drift
without writing.

## Running dbt without network access

dbt reads installed packages from `packages-install-path` in `dbt_project.yml`, which
defaults to `dbt_packages/`. If that directory already contains `dbt_utils` and
`automate_dv`, parse and build commands do not need a network fetch. There is no
`dbt deps --packages-install-path` command-line option.
"""


def scaffold(dest: Path, *, force: bool = False) -> list[Path]:
    """Create the empty estate at ``dest``. Returns every path written, sorted.

    ``dest`` must not already exist as a non-empty, non-scaffold directory unless
    ``force`` is set (an existing scaffold is always safe to re-run over -- every write
    here is either a fresh file or an overwrite of the same generated/templated content).
    """
    # Validate all package data before creating the destination.
    for required in ("dbt_project.yml", "packages.yml", "profiles.yml", "macros", "targets"):
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
