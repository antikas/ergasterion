"""The estate-evolution scenario: a payload field added to a live warehouse.

A generated Ergasterion warehouse keeps every version of every record. Estate
evolution is how that warehouse absorbs a new payload field without losing or
rewriting the versions it already holds. An **extension** is an additive payload
change, absorbed online. The **hashdiff basis** is the exact column set an entity's
stored fingerprints were computed over; an extension leaves it frozen, so no stored
fingerprint moves and no version replays.

The scenario copies the estate into a scratch directory under ``demo/offline-runs/``,
builds it on DuckDB, then in one step adds a payload field and changes a value in a
hashdiff-basis column for one record. It rebuilds without a full refresh and checks
the outcome by machine:

* the new column exists in the satellite;
* the identity of every prior version, over (business key, hashdiff, effective time),
  survives with the row count intact;
* every prior version keeps its load datetime and its record source;
* at least one genuine new version is appended, so the value check below is not
  vacuous;
* an affected business key gains at most one new version;
* each appended version carries the value the projection expression authors for the
  new field, and every prior version carries NULL for it;
* a repeat rebuild appends nothing;
* the satellite relation's own catalog object survives the field-add, so the relation
  is altered in place and never recreated;
* no history-bearing relation is dropped.

A failed check stops the scenario with a non-zero exit. The scratch estate is deleted
before the scenario returns.
"""

from __future__ import annotations

import sys
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent))

from estate import (  # noqa: E402
    Runner,
    check,
    history_bearing_relations,
    identity_digest,
    locate,
    object_identity_tokens,
    read_yaml,
    relation_columns,
    relation_sql,
    run_lane,
    stamp_object_identity,
    step,
    surviving_relations,
    tail,
    write_yaml,
)

# The entity the scenario evolves, and the two source-aligned satellites that store its
# history. CARTIVO carries the new field; MERCARO is the sibling source whose feed lacks
# it and projects null for it.
DOMAIN = "ecommerce"
ENTITY = "product"
CARRYING_SATELLITE = "sat_product_cartivo"
SIBLING_SATELLITE = "sat_product_mercaro"
SATELLITES = (CARRYING_SATELLITE, SIBLING_SATELLITE)
# The built slice reaches all three history-bearing families: the two product
# satellites, the product hub, and a link the product takes part in. Every relation in
# it has to survive the field-add.
BUILD_SELECTION = (
    f"+{CARRYING_SATELLITE}",
    f"+{SIBLING_SATELLITE}",
    "+hub_product",
    "+link_order_line_product",
)
CARRYING_SOURCE = "cartivo"
SIBLING_SOURCE = "mercaro"
SOURCE_TABLE = "products"
CARRYING_SEED = "raw_cartivo_products"

# The payload field the extension adds, and the projection expression that authors its
# value. The expression carries a declared default: a delivered value wins, and a record
# arriving without one gets the default. The default reaches new versions only; versions
# already stored keep NULL, because dbt fills a newly added column with NULL in place.
NEW_COLUMN = "product_note"
NEW_COLUMN_DEFAULT = "unlabelled"
CARRYING_EXPRESSION = (
    f"coalesce(nullif(cast({NEW_COLUMN} as string), ''), '{NEW_COLUMN_DEFAULT}')"
)
SIBLING_EXPRESSION = "cast(null as string)"

# The record the scenario changes, the value it authors for the new field on that
# record, and the hashdiff-basis column it changes at the same time. The basis change is
# what makes the record produce a genuine new version, so the value check below has a
# real appended row to read.
CHANGED_RECORD = "CART-PR-001"
CHANGED_RECORD_NOTE = "restocked-2025-02"
BASIS_COLUMN = "list_price"
BASIS_COLUMN_NEW_VALUE = "64.99"


def satellite_state(database: Path, columns_expected: bool) -> dict:
    """Row identities, values and catalog facts for each product satellite.

    A row identity is (business key, hashdiff, effective time), the same key the
    satellite's replay suppression uses. The scenario reads the load datetime, the
    record source, the source record id and the new field beside it.
    """
    connection = duckdb.connect(str(database), read_only=True)
    try:
        state: dict = {}
        for name in SATELLITES:
            schema = locate(connection, name)
            check(schema is not None, f"the scratch estate carries no relation {name}")
            relation = relation_sql(schema, name)
            columns = relation_columns(connection, schema, name)
            note_select = NEW_COLUMN if columns_expected else "cast(null as varchar)"
            rows = connection.execute(
                f"select product_hk, product_hashdiff, effective_from, load_datetime, "
                f"record_source, source_record_id, {note_select}, rowid "
                f"from {relation} order by rowid"
            ).fetchall()
            versions = {}
            for row in rows:
                identity = (str(row[0]), str(row[1]), str(row[2]))
                check(
                    identity not in versions,
                    f"{name} stores the same version twice: {identity}",
                )
                versions[identity] = {
                    "load_datetime": str(row[3]),
                    "record_source": str(row[4]),
                    "source_record_id": str(row[5]),
                    NEW_COLUMN: row[6],
                    "rowid": row[7],
                }
            comment = connection.execute(
                "select comment from duckdb_tables() "
                "where schema_name = ? and table_name = ?",
                [schema, name],
            ).fetchone()
            state[name] = {
                "columns": columns,
                "count": len(rows),
                "versions": versions,
                "identity_digest": identity_digest(versions),
                "comment": None if comment is None else comment[0],
                "max_rowid": max((row[7] for row in rows), default=-1),
            }
        return state
    finally:
        connection.close()


# ---------------------------------------------------------------------------
# Declaring the extension
# ---------------------------------------------------------------------------


def authored_value(satellite: str, source_record_id: str) -> str | None:
    """The value the projection expression authors for the new field.

    The sibling source's feed lacks the field, so its expression projects null. The
    carrying source projects the delivered value, and the declared default stands in
    where the delivery carries none.
    """
    if satellite == SIBLING_SATELLITE:
        return None
    if source_record_id == CHANGED_RECORD:
        return CHANGED_RECORD_NOTE
    return NEW_COLUMN_DEFAULT



def declare_the_extension(runner: Runner) -> None:
    """Add one payload field to the entity, and change one basis-column value.

    The payload list is entity-scoped, so every source feeding the entity maps the new
    column: the carrying source authors the value through its projection expression, and
    the sibling source projects null for it.
    """
    root = runner.root

    domain_path = root / "domains" / f"{DOMAIN}.yml"
    domain_document = read_yaml(domain_path)
    payload = domain_document["entity_configs"][ENTITY]["payload"]
    check(NEW_COLUMN not in payload, f"{NEW_COLUMN} is already a declared payload column")
    payload.append(NEW_COLUMN)
    write_yaml(domain_path, domain_document)

    for source, expression in (
        (CARRYING_SOURCE, CARRYING_EXPRESSION),
        (SIBLING_SOURCE, SIBLING_EXPRESSION),
    ):
        path = root / "declarations" / f"{source}.yml"
        document = read_yaml(path)
        table = document["tables"][SOURCE_TABLE]
        table["projection"].append({"name": NEW_COLUMN, "expression": expression})
        for vault_entity in table["vault_entities"]:
            if vault_entity["entity"] == ENTITY:
                vault_entity["bridge"]["select"].append(
                    {"name": NEW_COLUMN, "expression": f"source.{NEW_COLUMN}"}
                )
        write_yaml(path, document)

    rewrite_the_seed(root)

    # The seed's declared column types are the estate's own dbt configuration, and a new
    # seed column joins them; dbt hands the list straight to the CSV reader.
    project_path = root / "dbt_project.yml"
    project = read_yaml(project_path)
    project["seeds"]["ergasterion"][CARRYING_SEED]["+column_types"][NEW_COLUMN] = "string"
    write_yaml(project_path, project)
    runner.invalidate_parse_cache()


def rewrite_the_seed(root: Path) -> None:
    """Give the carrying source's delivered data the new field, and change one record's
    value in a hashdiff-basis column."""
    seed = root / "seeds" / f"{CARRYING_SEED}.csv"
    lines = [line for line in seed.read_text(encoding="utf-8").splitlines() if line]
    header = lines[0].split(",")
    check(NEW_COLUMN not in header, f"the seed already carries {NEW_COLUMN}")
    basis_index = header.index(BASIS_COLUMN)
    rewritten = [",".join(header + [NEW_COLUMN])]
    changed = 0
    for line in lines[1:]:
        fields = line.split(",")
        if fields[0] == CHANGED_RECORD:
            fields[basis_index] = BASIS_COLUMN_NEW_VALUE
            fields.append(CHANGED_RECORD_NOTE)
            changed += 1
        else:
            fields.append("")
        rewritten.append(",".join(fields))
    check(changed == 1, f"the seed carries {changed} row(s) for {CHANGED_RECORD}, expected 1")
    seed.write_text("\n".join(rewritten) + "\n", encoding="utf-8")


# ---------------------------------------------------------------------------
# The scenario
# ---------------------------------------------------------------------------


def run_scenario(repo_root: Path, root: Path, dbt_bin: str) -> None:
    database = root / "scratch.duckdb"
    runner = Runner(repo_root, root, database, dbt_bin)

    step("The copied estate regenerates byte-identically")
    output = runner.emit("--check")
    check(
        "would change 0 of " in output,
        "the copied estate does not regenerate byte-identically:\n" + tail(output),
    )
    print("   the scratch estate is the committed estate")

    step("Baseline build on DuckDB")
    runner.dbt("build", "--select", *BUILD_SELECTION)
    baseline = satellite_state(database, columns_expected=False)
    for name in SATELLITES:
        check(
            baseline[name]["count"] > 0,
            f"{name} stores no history to evolve",
        )
        check(
            NEW_COLUMN not in baseline[name]["columns"],
            f"{name} already carries {NEW_COLUMN} before the extension declares it",
        )
        print(f"   {name}: {baseline[name]['count']} stored version(s)")

    connection = duckdb.connect(str(database), read_only=True)
    try:
        relations = history_bearing_relations(root, connection)
    finally:
        connection.close()
    print(f"   history-bearing relations: {', '.join(sorted(relations))}")

    identity_token = "dpf-evolution-object-identity"
    stamp_object_identity(database, relations, identity_token)

    step("Declare the extension and change one basis-column value")
    declare_the_extension(runner)
    output = runner.emit()
    check(
        "estate evolution: extension" in output and NEW_COLUMN in output,
        "emit did not grade the payload addition as an extension:\n" + tail(output),
    )
    print(f"   the factory graded the addition of {NEW_COLUMN} as an extension")

    step("Rebuild with no full refresh")
    # The landing relation takes the new delivered field. A seed is the landing layer's
    # own recomputed table and stores no history.
    runner.dbt("seed", "--select", CARRYING_SEED, "--full-refresh")
    runner.dbt("build", "--select", *BUILD_SELECTION)
    evolved = satellite_state(database, columns_expected=True)

    step("Check the outcome")
    appended: dict = {}
    for name in SATELLITES:
        state = evolved[name]
        prior = baseline[name]

        check(
            NEW_COLUMN in state["columns"],
            f"{name} did not gain {NEW_COLUMN}; the relation must take the column in place",
        )

        kept = {
            identity: values
            for identity, values in state["versions"].items()
            if identity in prior["versions"]
        }
        check(
            len(kept) == prior["count"],
            f"{name} lost {prior['count'] - len(kept)} prior version(s); the identity of "
            "every stored version must survive the field-add",
        )
        check(
            identity_digest(kept) == prior["identity_digest"],
            f"{name} moved a prior row identity over (business key, hashdiff, effective time)",
        )

        for identity, values in kept.items():
            before = prior["versions"][identity]
            check(
                values["load_datetime"] == before["load_datetime"],
                f"{name} changed the load datetime of a prior version: {identity}",
            )
            check(
                values["record_source"] == before["record_source"],
                f"{name} changed the record source of a prior version: {identity}",
            )
            check(
                values[NEW_COLUMN] is None,
                f"{name} wrote a value into {NEW_COLUMN} on the prior version {identity}; "
                "a version stored before the extension carries NULL",
            )
            check(
                values["rowid"] == before["rowid"],
                f"{name} moved the stored position of a prior version: {identity}",
            )

        new_versions = {
            identity: values
            for identity, values in state["versions"].items()
            if identity not in prior["versions"]
        }
        appended[name] = new_versions

        per_key: dict = {}
        for identity in new_versions:
            per_key.setdefault(identity[0], []).append(identity)
        for business_key, identities in per_key.items():
            check(
                len(identities) == 1,
                f"{name} appended {len(identities)} versions for business key "
                f"{business_key}; an affected key gains at most one",
            )

        for identity, values in new_versions.items():
            check(
                values["rowid"] > prior["max_rowid"],
                f"{name} did not append version {identity} after the stored rows",
            )
            expected = authored_value(name, values["source_record_id"])
            check(
                values[NEW_COLUMN] == expected,
                f"{name} appended version {identity} carrying "
                f"{values[NEW_COLUMN]!r} in {NEW_COLUMN}; the projection expression "
                f"authors {expected!r}",
            )

        print(
            f"   {name}: {len(kept)} prior version(s) intact, "
            f"{len(new_versions)} appended"
        )

    carrying = CARRYING_SATELLITE
    check(
        len(appended[carrying]) >= 1,
        f"{carrying} appended no new version; the basis-column change must produce one, "
        "or the value check reads nothing",
    )
    changed_identities = [
        identity
        for identity, values in appended[carrying].items()
        if values["source_record_id"] == CHANGED_RECORD
    ]
    check(
        len(changed_identities) == 1,
        f"{carrying} appended {len(changed_identities)} version(s) for {CHANGED_RECORD}, "
        "expected exactly one",
    )
    print(
        f"   {carrying} appended one version for {CHANGED_RECORD} carrying "
        f"{CHANGED_RECORD_NOTE!r} in {NEW_COLUMN}"
    )

    step("Check the relations themselves")
    survivors = surviving_relations(database, relations)
    check(
        sorted(survivors) == sorted(relations),
        "the field-add dropped history-bearing relation(s): "
        + ", ".join(sorted(set(relations) - set(survivors))),
    )
    tokens = object_identity_tokens(database, relations)
    for name in sorted(relations):
        check(
            tokens[name] == identity_token,
            f"{name} lost its catalog object identity; the relation was recreated rather "
            "than altered in place",
        )
    print(f"   {len(relations)} history-bearing relation(s) altered in place, none dropped")

    for arguments in runner.dbt_commands:
        if "--full-refresh" not in arguments:
            continue
        check(
            arguments[0] == "seed",
            "the scenario ran a full refresh outside the landing layer: dbt "
            + " ".join(arguments),
        )

    step("Repeat the rebuild")
    runner.dbt("build", "--select", *BUILD_SELECTION)
    repeated = satellite_state(database, columns_expected=True)
    for name in SATELLITES:
        check(
            repeated[name]["count"] == evolved[name]["count"],
            f"{name} appended "
            f"{repeated[name]['count'] - evolved[name]['count']} row(s) on a repeat build",
        )
        check(
            repeated[name]["identity_digest"] == evolved[name]["identity_digest"],
            f"{name} moved a row identity on a repeat build",
        )
        print(f"   {name}: {repeated[name]['count']} version(s), zero appended")

    tokens = object_identity_tokens(database, relations)
    for name in sorted(relations):
        check(
            tokens[name] == identity_token,
            f"{name} lost its catalog object identity on the repeat build",
        )

    print("\nEvery estate-evolution check held.")


def main(argv: list[str] | None = None) -> int:
    return run_lane(
        "The estate-evolution scenario.", "evolution", run_scenario, argv
    )


if __name__ == "__main__":
    raise SystemExit(main())
