"""The watermark scenario: a warehouse that reads only what is new.

A generated Ergasterion warehouse loads a source by reading it. Watermark increments
are how it reads only the part that can still change, while it keeps the full history
it has already stored. The words the factory uses for the mechanism, used here the same
way:

* **effective column** -- the one staging output column a table's bridge maps to
  ``effective_from``. It is the single home of the effective-time fact.
* **staging increment block** -- the declared per-table configuration for watermark
  increments: the lookback, and the acknowledgment that the effective column advances
  when the source redelivers a record.
* **consumption watermark** -- the point on the effective column up to which every
  satellite fed by the table has absorbed history.
* **delta window** -- the interval [consumption watermark minus lookback, infinity),
  entered with a ``>=`` comparison at the floor.
* **replay suppression** -- the satellite guard discarding a candidate row whose
  (business key, hashdiff, effective time) already exists in the target.
* **extension** -- an additive payload change, absorbed online with the **hashdiff
  basis** frozen, where the basis is the exact column set the stored fingerprints were
  computed over.

The scenario copies the estate into a scratch directory under ``demo/offline-runs/``,
declares a staging increment block on one table, and checks the outcome by machine:

* the initial load builds all history, with the row count and the identity digest over
  (business key, hashdiff, effective time) pinned;
* a later run stages exactly the delta-window rows, and the stored rows below the floor
  remain byte-equal;
* the within-batch uniqueness assertion holds on the staging unique key, and the same
  assertion covers the Bronze-backed visibility-join path;
* the delete side of the staging write carries the resolved floor;
* a row sitting exactly at the lookback floor lands exactly once;
* the satellites append exactly the expected versions, covering an insert, an update and
  a late arrival inside the lookback;
* a third identical run appends nothing;
* a following full-refresh build leaves the stored history byte-equal on the identity
  digest and appends nothing;
* a table that declares no block keeps its relation digest through every run;
* with the block standing, one payload field is added and its value reaches the
  incremental staging relation in place and the new satellite version.

A failed check stops the scenario with a non-zero exit. The scratch estate is deleted
before the scenario returns.
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent))

from estate import (  # noqa: E402
    Runner,
    check,
    identity_digest,
    locate,
    object_identity_tokens,
    read_yaml,
    relation_columns,
    relation_digest,
    relation_sql,
    run_lane,
    stamp_object_identity,
    step,
    tail,
    write_yaml,
)

# The source the scenario windows. RELATIO declares exactly one table, so it owns exactly
# one satellite and its consumption watermark is a single satellite maximum. Its bridge
# maps as_of_date to effective_from, and an as-of snapshot date advances when the CRM
# redelivers a record, which is the precondition a staging increment block carries.
DOMAIN = "ecommerce"
ENTITY = "customer"
SOURCE = "relatio"
SOURCE_TABLE = "customers"
SEED = "raw_relatio_customers"
STAGING_MODEL = "stg_relatio_customers"
SATELLITE = "sat_customer_relatio"
BUSINESS_KEY = "customer_hk"
HASHDIFF = "customer_hashdiff"

# The declared block, and the key the factory derives from it.
EFFECTIVE_COLUMN = "as_of_date"
NATURAL_KEY = ["source_record_id"]
LOOKBACK_MINUTES = 1440
UNIQUE_KEY = NATURAL_KEY + [EFFECTIVE_COLUMN]

# A table that declares no block. Its staging relation is a full recompute and its
# satellite is append-only, and neither may move while the windowed table changes.
UNDECLARED_STAGING = "stg_cartivo_customers"
UNDECLARED_SATELLITE = "sat_customer_cartivo"

# The built slice: the windowed table's own chain, the hub it feeds, and the undeclared
# table's satellite beside it. Cautious indirect selection keeps the slice self-contained:
# a test runs when every relation it reads is inside the slice, so an estate assertion
# reaching a relation this slice does not build stays out of the run.
BUILD_SELECTION = (
    "--select",
    f"+{SATELLITE}",
    "+hub_customer",
    f"+{UNDECLARED_SATELLITE}",
    "--indirect-selection",
    "cautious",
)

# The committed seed stores two records, the later one on 2025-06-15. The initial load
# absorbs both, so the consumption watermark lands on 2025-06-15 and a one-day lookback
# puts the next run's window floor on 2025-06-14.
EXPECTED_WATERMARK = "2025-06-15"
EXPECTED_FLOOR = "2025-06-14"
# The floor a source with no absorbed history reports, so the first run's window is
# unbounded below and the load builds all history.
INITIAL_LOAD_SENTINEL = "1900-01-01"

# The delta, appended to the delivered feed before the second run. Every row sits inside
# the window by construction: the scenario excludes no out-of-lookback data at any point
# before the recovery build, which is the precondition the zero-append claim there rests
# on, and the scenario checks it.
INSERTED_RECORD = "RELA-CR-003"
UPDATED_RECORD = "RELA-CR-004"
LATE_RECORD = "RELA-CR-005"
FLOOR_RECORD = "RELA-CR-006"
# The update is a new CRM record for a customer the warehouse already stores, so the same
# business key gains one new version. It carries its own record id because the customer
# resolution map pins uniqueness on (source system, source id, source record id) and,
# under the block, staging holds one row per (natural key, effective time).
UPDATED_CUSTOMER = "RELA-CUST-001"
UPDATED_CITY = "Salford"

DELTA_ROWS = (
    {
        "relatio_record_id": INSERTED_RECORD,
        "relatio_customer_id": "RELA-CUST-003",
        "loyalty_id": "LOY-1010",
        "email": "noah.bright@example.com",
        "full_name": "Noah Bright",
        "phone": "+44-11-1000-0003",
        "address_line": "12 Park Row",
        "city": "Leeds",
        "postal_code": "LS1 5HD",
        "country": "GB",
        "customer_status": "active",
        "as_of_date": "2025-06-20",
    },
    {
        "relatio_record_id": UPDATED_RECORD,
        "relatio_customer_id": UPDATED_CUSTOMER,
        "loyalty_id": "LOY-1001",
        "email": "ava.thompson@example.com",
        "full_name": "Ava Thompson",
        "phone": "+44-16-1000-0001",
        "address_line": "88 Deansgate",
        "city": UPDATED_CITY,
        "postal_code": "M3 2ER",
        "country": "GB",
        "customer_status": "active",
        "as_of_date": "2025-06-18",
    },
    {
        "relatio_record_id": LATE_RECORD,
        "relatio_customer_id": "RELA-CUST-005",
        "loyalty_id": "LOY-1011",
        "email": "priya.raman@example.com",
        "full_name": "Priya Raman",
        "phone": "+44-12-1000-0005",
        "address_line": "7 Kingsway",
        "city": "Cardiff",
        "postal_code": "CF10 3AT",
        "country": "GB",
        "customer_status": "active",
        "as_of_date": "2025-06-15",
    },
    {
        "relatio_record_id": FLOOR_RECORD,
        "relatio_customer_id": "RELA-CUST-006",
        "loyalty_id": "LOY-1012",
        "email": "tomas.lind@example.com",
        "full_name": "Tomas Lind",
        "phone": "+44-14-1000-0006",
        "address_line": "3 Castle Street",
        "city": "Bristol",
        "postal_code": "BS1 4TP",
        "country": "GB",
        "customer_status": "active",
        "as_of_date": EXPECTED_FLOOR,
    },
)

# The payload field the intersection step adds, and the record that delivers a value for
# it. The field arrives while the block stands, so the extension has to reach the
# incremental staging relation in place and its value has to reach the new version.
NEW_COLUMN = "customer_note"
NEW_COLUMN_DEFAULT = "unlabelled"
CARRYING_EXPRESSION = (
    f"coalesce(nullif(cast({NEW_COLUMN} as string), ''), '{NEW_COLUMN_DEFAULT}')"
)
SIBLING_EXPRESSION = "cast(null as string)"
SIBLING_SOURCES = ("cartivo", "mercaro")
EXTENSION_RECORD = "RELA-CR-007"
EXTENSION_NOTE = "vip-2025-06"
EXTENSION_ROW = {
    "relatio_record_id": EXTENSION_RECORD,
    "relatio_customer_id": UPDATED_CUSTOMER,
    "loyalty_id": "LOY-1001",
    "email": "ava.thompson@example.com",
    "full_name": "Ava Thompson",
    "phone": "+44-16-1000-0001",
    "address_line": "88 Deansgate",
    "city": UPDATED_CITY,
    "postal_code": "M3 2ER",
    "country": "GB",
    "customer_status": "active",
    "as_of_date": "2025-06-24",
}

FLOOR_LOG = re.compile(r"DPF_WINDOW_FLOOR=(\{.*\})")
ROWS_LOG = re.compile(r"DPF_WINDOW_ROWS=(\{.*\})")
FLOOR_LITERAL = re.compile(r"cast\('([^']+)' as ([^)]+)\)", re.IGNORECASE)


# ---------------------------------------------------------------------------
# Declaring the block
# ---------------------------------------------------------------------------


def declare_the_block(runner: Runner) -> None:
    """Give the table a natural key and a staging increment block.

    The block authors the lookback and the acknowledgment. The unique key is derived,
    the natural key plus the effective column, so the key is declared once.
    """
    path = runner.root / "declarations" / f"{SOURCE}.yml"
    document = read_yaml(path)
    table = document["tables"][SOURCE_TABLE]
    check(
        "staging_increment" not in table,
        f"{SOURCE}.{SOURCE_TABLE} already declares a staging increment block",
    )
    table["natural_key"] = list(NATURAL_KEY)
    table["staging_increment"] = {
        "lookback_minutes": LOOKBACK_MINUTES,
        "effective_advances_on_redelivery": True,
    }
    write_yaml(path, document)
    runner.invalidate_parse_cache()


def append_delivered_rows(root: Path, rows: tuple[dict, ...] | list[dict]) -> None:
    """Append delivered records to the source feed."""
    seed = root / "seeds" / f"{SEED}.csv"
    lines = [line for line in seed.read_text(encoding="utf-8").splitlines() if line]
    header = lines[0].split(",")
    for row in rows:
        check(
            set(row) == set(header),
            f"the delta row {row.get('relatio_record_id')!r} does not match the feed "
            f"columns {header}",
        )
        lines.append(",".join(row[column] for column in header))
    seed.write_text("\n".join(lines) + "\n", encoding="utf-8")


def delivered_rows(root: Path) -> list[dict]:
    """Every record the source feed currently delivers."""
    seed = root / "seeds" / f"{SEED}.csv"
    lines = [line for line in seed.read_text(encoding="utf-8").splitlines() if line]
    header = lines[0].split(",")
    return [dict(zip(header, line.split(","))) for line in lines[1:]]


# ---------------------------------------------------------------------------
# Reading the warehouse
# ---------------------------------------------------------------------------


def staging_state(database: Path, note_expected: bool) -> dict:
    """Every staged row, keyed on the staging unique key, with its physical position."""
    connection = duckdb.connect(str(database), read_only=True)
    try:
        schema = locate(connection, STAGING_MODEL)
        check(schema is not None, f"the scratch estate carries no relation {STAGING_MODEL}")
        columns = relation_columns(connection, schema, STAGING_MODEL)
        note_select = NEW_COLUMN if note_expected else "cast(null as varchar)"
        rows = connection.execute(
            f"select source_record_id, cast({EFFECTIVE_COLUMN} as varchar), city, "
            f"{note_select}, rowid from {relation_sql(schema, STAGING_MODEL)} "
            f"order by rowid"
        ).fetchall()
        staged: dict = {}
        for row in rows:
            key = (str(row[0]), str(row[1]))
            check(
                key not in staged,
                f"{STAGING_MODEL} stages the same unique key twice: {key}",
            )
            staged[key] = {"city": row[2], NEW_COLUMN: row[3], "rowid": row[4]}
        return {"columns": columns, "count": len(rows), "rows": staged}
    finally:
        connection.close()


def satellite_state(database: Path, note_expected: bool) -> dict:
    """Row identities and values for the windowed table's satellite.

    A row identity is (business key, hashdiff, effective time), the same key replay
    suppression reads.
    """
    connection = duckdb.connect(str(database), read_only=True)
    try:
        schema = locate(connection, SATELLITE)
        check(schema is not None, f"the scratch estate carries no relation {SATELLITE}")
        columns = relation_columns(connection, schema, SATELLITE)
        note_select = NEW_COLUMN if note_expected else "cast(null as varchar)"
        rows = connection.execute(
            f"select {BUSINESS_KEY}, {HASHDIFF}, cast(effective_from as varchar), "
            f"load_datetime, record_source, source_record_id, city, {note_select} "
            f"from {relation_sql(schema, SATELLITE)} order by rowid"
        ).fetchall()
        versions: dict = {}
        for row in rows:
            identity = (str(row[0]), str(row[1]), str(row[2]))
            check(
                identity not in versions,
                f"{SATELLITE} stores the same version twice: {identity}",
            )
            versions[identity] = {
                "load_datetime": str(row[3]),
                "record_source": str(row[4]),
                "source_record_id": str(row[5]),
                "city": row[6],
                NEW_COLUMN: row[7],
            }
        return {
            "columns": columns,
            "count": len(rows),
            "versions": versions,
            "identity_digest": identity_digest(versions),
        }
    finally:
        connection.close()


def consumption_watermark(database: Path) -> str:
    """The point on the effective column the satellite has absorbed history up to."""
    connection = duckdb.connect(str(database), read_only=True)
    try:
        schema = locate(connection, SATELLITE)
        check(schema is not None, f"the scratch estate carries no relation {SATELLITE}")
        row = connection.execute(
            f"select cast(max(effective_from) as varchar) "
            f"from {relation_sql(schema, SATELLITE)}"
        ).fetchone()
        check(row is not None and row[0] is not None, f"{SATELLITE} has absorbed nothing")
        return str(row[0])
    finally:
        connection.close()


def relation_schema(database: Path, table_name: str) -> str:
    connection = duckdb.connect(str(database), read_only=True)
    try:
        schema = locate(connection, table_name)
        check(schema is not None, f"the scratch estate carries no relation {table_name}")
        return str(schema)
    finally:
        connection.close()


# ---------------------------------------------------------------------------
# Reading the run's own report
# ---------------------------------------------------------------------------


def logged_records(pattern: re.Pattern, output: str) -> list[dict]:
    return [json.loads(match) for match in pattern.findall(output)]


def applied_window_floor(output: str) -> str:
    """The one window floor the run applied to the declared table.

    Every windowed layer of a source resolves the same floor, so a run reporting more
    than one distinct floor for one table is itself a failure.
    """
    records = [
        record
        for record in logged_records(FLOOR_LOG, output)
        if record["source"] == SOURCE and record["table"] == SOURCE_TABLE
    ]
    check(records, f"the run reported no window floor for {SOURCE}.{SOURCE_TABLE}")
    floors = sorted({record["floor"] for record in records})
    check(
        len(floors) == 1,
        f"the run applied {len(floors)} different window floors to "
        f"{SOURCE}.{SOURCE_TABLE}: {floors}",
    )
    return floors[0]


def floor_value_and_type(floor: str) -> tuple[str, str]:
    match = FLOOR_LITERAL.match(floor.strip())
    check(match is not None, f"the window floor is not a normalised literal: {floor!r}")
    assert match is not None
    return match.group(1), match.group(2).strip()


def window_row_report(output: str) -> dict:
    records = [
        record
        for record in logged_records(ROWS_LOG, output)
        if record["source"] == SOURCE and record["table"] == SOURCE_TABLE
    ]
    check(
        len(records) == 1,
        f"the run reported {len(records)} row reports for {SOURCE}.{SOURCE_TABLE}, "
        "expected exactly one",
    )
    return records[0]


def assert_delete_side_is_bounded(runner: Runner, floor: str) -> None:
    """The delete side of the staging write carries the resolved floor.

    dbt writes every executed statement to its debug log, so the check reads the delete
    statement the run actually ran against DuckDB.
    """
    log = runner.dbt_log_text()
    deletes = [
        log[index : index + 4000]
        for index in (match.start() for match in re.finditer(r"delete from", log, re.I))
        if STAGING_MODEL in log[index : index + 400]
    ]
    check(
        deletes,
        f"the run's debug log holds no delete statement against {STAGING_MODEL}; the "
        "incremental write is not the delete-and-insert the block declares",
    )
    predicate = f"{EFFECTIVE_COLUMN} >= {floor}"
    bounded = [statement for statement in deletes if predicate.lower() in statement.lower()]
    check(
        bounded,
        f"no delete statement against {STAGING_MODEL} carries the window bound "
        f"{predicate!r}; the delete side would scan cumulative history. Statements "
        f"found:\n" + "\n---\n".join(statement[:600] for statement in deletes),
    )
    print(f"   the delete side is bounded by {predicate}")


# ---------------------------------------------------------------------------
# The within-batch uniqueness assertion
# ---------------------------------------------------------------------------


def assert_staging_unique_key_holds(database: Path, where: str) -> None:
    """No two staged rows share the staging unique key.

    Under a delete-and-insert write keyed on that unique key, two rows sharing it strand
    or duplicate each other, so the assertion is the guard the batch has to pass.
    """
    connection = duckdb.connect(str(database), read_only=True)
    try:
        schema = locate(connection, STAGING_MODEL)
        check(schema is not None, f"the scratch estate carries no relation {STAGING_MODEL}")
        duplicates = connection.execute(
            f"select {', '.join(UNIQUE_KEY)}, count(*) as staged "
            f"from {relation_sql(schema, STAGING_MODEL)} "
            f"group by {', '.join(UNIQUE_KEY)} having count(*) > 1"
        ).fetchall()
        check(
            not duplicates,
            f"{where}: {STAGING_MODEL} stages {len(duplicates)} duplicate(s) on the "
            f"staging unique key {UNIQUE_KEY}: {duplicates}",
        )
    finally:
        connection.close()
    print(f"   {where}: the staging unique key {UNIQUE_KEY} holds within the batch")


def assert_visibility_join_sits_inside_the_covered_batch(repo_root: Path) -> None:
    """The Bronze-backed visibility join lands inside the batch the unique key covers.

    A Bronze-backed table reaches its delivered rows through a visibility join onto the
    published delivery ledger. The check renders the staging template through the
    factory's own template environment with a marked join, and reads where the mark
    lands: inside the projected batch, which is the batch the staging unique key and the
    delta window both apply to.
    """
    sys.path.insert(0, str(repo_root))
    from ergasterion import emit  # noqa: PLC0415

    mark = "-- MARKED VISIBILITY JOIN"
    table = {
        "landing": {"kind": "model"},
        "raw_model": SEED,
        "projection": [
            {"name": column, "expression": column} for column in NATURAL_KEY + [EFFECTIVE_COLUMN]
        ],
        "visibility_join": f"\n{mark}",
        "staging_increment_config": "{{ config(materialized='incremental') }}",
        "staging_increment_window": "{% if is_incremental() %}\nwhere windowed\n{% endif %}",
    }
    rendered = emit.render(
        emit.template_env(),
        "staging.sql.j2",
        table=table,
        generated_header="-- generated",
    )
    check(mark in rendered, "the staging template dropped the visibility join")
    opening = rendered.index("projected as (")
    closing = rendered.index("select * from projected")
    check(
        opening < rendered.index(mark) < closing,
        "the Bronze-backed visibility join lands outside the projected batch, so the "
        "staging unique key would not cover the rows the join produces:\n" + rendered,
    )
    print("   the visibility join lands inside the batch the staging unique key covers")


def assert_the_uniqueness_assertion_catches_a_visibility_fanout(database: Path) -> None:
    """The same assertion detects a duplicate the visibility join can produce.

    A published delivery ledger holding the same visibility triple twice fans every
    matching delivered row out, and the fan-out lands on the staging unique key. The
    check runs the assertion over a join in that shape, once on a ledger with one row
    per triple and once on a ledger holding a duplicate.
    """
    connection = duckdb.connect(str(database))
    try:
        connection.execute("create schema if not exists visibility_control")
        connection.execute("drop table if exists visibility_control.delivered")
        connection.execute("drop table if exists visibility_control.published_ledger")
        connection.execute(
            "create table visibility_control.delivered as select * from (values "
            "('REC-1', date '2025-06-14', 1, 'batch', 'D-1'), "
            "('REC-2', date '2025-06-15', 1, 'batch', 'D-2')) "
            "as delivered(source_record_id, as_of_date, visibility_epoch, "
            "visibility_kind, visibility_id)"
        )

        def duplicates_under(ledger_rows: str) -> list:
            connection.execute("drop table if exists visibility_control.published_ledger")
            connection.execute(
                "create table visibility_control.published_ledger as select * from (values "
                f"{ledger_rows}) as published(visibility_epoch, visibility_kind, "
                "visibility_id, identity_key, projection_target)"
            )
            return connection.execute(
                "select source_record_id, as_of_date, count(*) from ("
                "  select source.source_record_id, source.as_of_date"
                "  from visibility_control.delivered as source"
                "  inner join visibility_control.published_ledger as published"
                "      on source.visibility_epoch = published.visibility_epoch"
                "     and source.visibility_kind = published.visibility_kind"
                "     and source.visibility_id = published.visibility_id"
                "     and published.identity_key = 'relatio.customers'"
                "     and published.projection_target = 'staging'"
                ") group by 1, 2 having count(*) > 1"
            ).fetchall()

        clean = duplicates_under(
            "(1, 'batch', 'D-1', 'relatio.customers', 'staging'), "
            "(1, 'batch', 'D-2', 'relatio.customers', 'staging')"
        )
        check(
            not clean,
            "the uniqueness assertion reported a duplicate on a ledger holding one row "
            f"per visibility triple: {clean}",
        )
        fanned = duplicates_under(
            "(1, 'batch', 'D-1', 'relatio.customers', 'staging'), "
            "(1, 'batch', 'D-1', 'relatio.customers', 'staging'), "
            "(1, 'batch', 'D-2', 'relatio.customers', 'staging')"
        )
        check(
            len(fanned) == 1 and fanned[0][2] == 2,
            "the uniqueness assertion missed the fan-out a duplicated visibility row "
            f"produces: {fanned}",
        )
    finally:
        connection.execute("drop table if exists visibility_control.delivered")
        connection.execute("drop table if exists visibility_control.published_ledger")
        connection.execute("drop schema if exists visibility_control")
        connection.close()
    print("   the assertion catches the fan-out a duplicated visibility row produces")


# ---------------------------------------------------------------------------
# Declaring the extension
# ---------------------------------------------------------------------------


def declare_the_extension(runner: Runner) -> None:
    """Add one payload field to the entity while the block stands.

    The payload list is entity-scoped, so every source feeding the entity maps the new
    column: the delivering source authors the value through its projection expression,
    and each sibling source projects null for it.
    """
    root = runner.root

    domain_path = root / "domains" / f"{DOMAIN}.yml"
    domain_document = read_yaml(domain_path)
    payload = domain_document["entity_configs"][ENTITY]["payload"]
    check(NEW_COLUMN not in payload, f"{NEW_COLUMN} is already a declared payload column")
    payload.append(NEW_COLUMN)
    write_yaml(domain_path, domain_document)

    for source, expression in (
        (SOURCE, CARRYING_EXPRESSION),
        *((sibling, SIBLING_EXPRESSION) for sibling in SIBLING_SOURCES),
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

    add_new_field_to_the_feed(root)

    # The seed's declared column types are the estate's own dbt configuration, and a new
    # seed column joins them; dbt hands the list straight to the CSV reader.
    project_path = root / "dbt_project.yml"
    project = read_yaml(project_path)
    project["seeds"]["ergasterion"][SEED]["+column_types"][NEW_COLUMN] = "string"
    write_yaml(project_path, project)
    runner.invalidate_parse_cache()


def add_new_field_to_the_feed(root: Path) -> None:
    """Give the delivered feed the new field, and deliver one record carrying a value."""
    seed = root / "seeds" / f"{SEED}.csv"
    lines = [line for line in seed.read_text(encoding="utf-8").splitlines() if line]
    header = lines[0].split(",")
    check(NEW_COLUMN not in header, f"the feed already carries {NEW_COLUMN}")
    rewritten = [",".join(header + [NEW_COLUMN])]
    for line in lines[1:]:
        rewritten.append(line + ",")
    rewritten.append(
        ",".join([EXTENSION_ROW[column] for column in header] + [EXTENSION_NOTE])
    )
    seed.write_text("\n".join(rewritten) + "\n", encoding="utf-8")


# ---------------------------------------------------------------------------
# The scenario
# ---------------------------------------------------------------------------


def run_scenario(repo_root: Path, root: Path, dbt_bin: str) -> None:
    database = root / "scratch.duckdb"
    runner = Runner(repo_root, root, database, dbt_bin)
    recovery_build_index: int | None = None

    step("The copied estate regenerates byte-identically")
    output = runner.emit("--check")
    check(
        "would change 0 of " in output,
        "the copied estate does not regenerate byte-identically:\n" + tail(output),
    )
    print("   the scratch estate is the committed estate")

    step("Declare the staging increment block")
    undeclared_sql = (root / "models" / "staging" / f"{UNDECLARED_STAGING}.sql").read_bytes()
    declare_the_block(runner)
    output = runner.emit()
    generated = (root / "models" / "staging" / f"{STAGING_MODEL}.sql").read_text(
        encoding="utf-8"
    )
    check(
        "materialized='incremental'" in generated
        and "incremental_strategy='delete+insert'" in generated,
        f"{STAGING_MODEL} is not the incremental delete-and-insert shape the block "
        "declares:\n" + generated[:800],
    )
    check(
        f"unique_key={UNIQUE_KEY!r}".replace('"', "'") in generated.replace('"', "'"),
        f"{STAGING_MODEL} does not carry the derived unique key {UNIQUE_KEY}:\n"
        + generated[:800],
    )
    check(
        (root / "models" / "staging" / f"{UNDECLARED_STAGING}.sql").read_bytes()
        == undeclared_sql,
        f"declaring the block changed the generated SQL of {UNDECLARED_STAGING}, a table "
        "that declares no block",
    )
    print(f"   {STAGING_MODEL} carries the window and the derived key {UNIQUE_KEY}")
    print(f"   {UNDECLARED_STAGING} regenerated byte-identically")

    step("Initial load")
    seeded = delivered_rows(root)
    runner.reset_dbt_log()
    output = runner.dbt("build", *BUILD_SELECTION)
    floor_value, _ = floor_value_and_type(applied_window_floor(output))
    check(
        floor_value == INITIAL_LOAD_SENTINEL,
        f"the initial load applied the floor {floor_value}, not the initial-load "
        f"sentinel {INITIAL_LOAD_SENTINEL}; its window has to be unbounded below",
    )
    initial_staging = staging_state(database, note_expected=False)
    initial_satellite = satellite_state(database, note_expected=False)
    check(
        initial_staging["count"] == len(seeded),
        f"the initial load staged {initial_staging['count']} row(s) from a feed "
        f"delivering {len(seeded)}; it has to build all history",
    )
    check(
        initial_satellite["count"] == len(seeded),
        f"{SATELLITE} stores {initial_satellite['count']} version(s) after the initial "
        f"load of {len(seeded)} delivered record(s)",
    )
    assert_staging_unique_key_holds(database, "initial load")
    print(
        f"   all history built: {initial_staging['count']} staged row(s), "
        f"{initial_satellite['count']} stored version(s)"
    )
    print(f"   identity digest: {initial_satellite['identity_digest'][:16]}")

    watermark = consumption_watermark(database)
    check(
        watermark == EXPECTED_WATERMARK,
        f"the consumption watermark stands at {watermark}, expected {EXPECTED_WATERMARK}",
    )
    print(f"   consumption watermark: {watermark}")

    undeclared_digests = {
        name: relation_digest(database, name)
        for name in (UNDECLARED_STAGING, UNDECLARED_SATELLITE)
    }
    print(f"   undeclared relations pinned: {', '.join(sorted(undeclared_digests))}")

    step("The uniqueness assertion covers the Bronze-backed visibility-join path")
    assert_visibility_join_sits_inside_the_covered_batch(repo_root)
    assert_the_uniqueness_assertion_catches_a_visibility_fanout(database)

    step("Append the delta: an insert, an update, and late arrivals")
    append_delivered_rows(root, DELTA_ROWS)
    delivered = delivered_rows(root)
    in_window = {
        (row["relatio_record_id"], row["as_of_date"])
        for row in delivered
        if row["as_of_date"] >= EXPECTED_FLOOR
    }
    outside_window = {
        (row["relatio_record_id"], row["as_of_date"])
        for row in delivered
        if row["as_of_date"] < EXPECTED_FLOOR
    }
    # The precondition the recovery build's zero-append claim rests on: no delivered
    # record sits below the floor except the ones the initial load already absorbed.
    seeded_keys = {(row["relatio_record_id"], row["as_of_date"]) for row in seeded}
    check(
        outside_window <= seeded_keys,
        "the scenario authored a delta row below the window floor, so the recovery "
        f"build would re-include excluded data: {sorted(outside_window - seeded_keys)}",
    )
    print(
        f"   delivered rows: {len(delivered)} ({len(in_window)} inside the window, "
        f"{len(outside_window)} below the floor, all of them absorbed by the initial load)"
    )

    step("The delta run")
    runner.reset_dbt_log()
    output = runner.dbt("seed", "--select", SEED)
    output += runner.dbt("build", *BUILD_SELECTION)
    floor = applied_window_floor(output)
    floor_value, floor_type = floor_value_and_type(floor)
    check(
        floor_value == EXPECTED_FLOOR,
        f"the delta run applied the floor {floor_value}; the consumption watermark "
        f"{watermark} minus a {LOOKBACK_MINUTES}-minute lookback is {EXPECTED_FLOOR}",
    )
    check(
        floor_type.lower() == "date",
        f"the window floor is normalised to {floor_type}, not the effective column's "
        "own date type, so the boundary would evaluate at another granularity",
    )
    print(f"   applied window floor: {floor_value} ({floor_type})")

    delta_staging = staging_state(database, note_expected=False)
    staged_in_window = {key for key in delta_staging["rows"] if key[1] >= EXPECTED_FLOOR}
    check(
        staged_in_window == in_window,
        "the delta run did not stage exactly the delta-window rows. Missing: "
        f"{sorted(in_window - staged_in_window)}; unexpected: "
        f"{sorted(staged_in_window - in_window)}",
    )
    report = window_row_report(output)
    check(
        report["relation_rows_in_window"] == len(in_window),
        f"the staging relation reports {report['relation_rows_in_window']} row(s) "
        f"at or above the floor against {len(in_window)} expected",
    )
    check(
        report["relation_rows_total"] > report["relation_rows_in_window"],
        f"all {report['relation_rows_total']} stored row(s) sit at or above the floor, "
        "so the unchanged-history check below has no below-floor row to prove",
    )
    for key in outside_window:
        check(
            key in delta_staging["rows"],
            f"the delta run dropped the stored row {key}, which sits below the floor",
        )
        check(
            delta_staging["rows"][key]["rowid"] == initial_staging["rows"][key]["rowid"],
            f"the delta run moved the stored row {key}, which sits below the floor",
        )
    print(
        f"   staged exactly the {len(in_window)} window row(s); "
        f"{len(outside_window)} stored row(s) below the floor untouched"
    )
    assert_staging_unique_key_holds(database, "delta run")
    assert_delete_side_is_bounded(runner, floor)

    delta_satellite = satellite_state(database, note_expected=False)
    appended = {
        identity: values
        for identity, values in delta_satellite["versions"].items()
        if identity not in initial_satellite["versions"]
    }
    kept = {
        identity: values
        for identity, values in delta_satellite["versions"].items()
        if identity in initial_satellite["versions"]
    }
    check(
        len(kept) == initial_satellite["count"],
        f"{SATELLITE} lost {initial_satellite['count'] - len(kept)} stored version(s)",
    )
    appended_records = {values["source_record_id"] for values in appended.values()}
    expected_records = {INSERTED_RECORD, UPDATED_RECORD, LATE_RECORD, FLOOR_RECORD}
    check(
        appended_records == expected_records and len(appended) == len(expected_records),
        f"{SATELLITE} appended {len(appended)} version(s) for {sorted(appended_records)}, "
        f"expected exactly one each for {sorted(expected_records)}",
    )
    update_identities = [
        identity
        for identity, values in appended.items()
        if values["source_record_id"] == UPDATED_RECORD
    ]
    check(
        len(update_identities) == 1,
        f"{SATELLITE} appended {len(update_identities)} version(s) for the update "
        f"{UPDATED_RECORD}, expected exactly one",
    )
    stored_keys = {
        identity[0]
        for identity, values in initial_satellite["versions"].items()
        if values["source_record_id"] == "RELA-CR-001"
    }
    check(
        update_identities[0][0] in stored_keys,
        f"the update landed on a new business key; {UPDATED_RECORD} redelivers customer "
        f"{UPDATED_CUSTOMER}, which the warehouse already stores",
    )
    check(
        appended[update_identities[0]]["city"] == UPDATED_CITY,
        f"the appended version for {UPDATED_RECORD} carries "
        f"{appended[update_identities[0]]['city']!r}, not the delivered {UPDATED_CITY!r}",
    )
    floor_versions = [
        identity
        for identity, values in delta_satellite["versions"].items()
        if values["source_record_id"] == FLOOR_RECORD
    ]
    check(
        len(floor_versions) == 1,
        f"the record delivered exactly at the floor {EXPECTED_FLOOR} landed "
        f"{len(floor_versions)} time(s), expected exactly once",
    )
    print(
        f"   {SATELLITE}: {len(kept)} stored version(s) intact, {len(appended)} appended "
        f"for {sorted(appended_records)}"
    )
    print(f"   the record delivered exactly at the floor {EXPECTED_FLOOR} landed once")

    for name, digest in undeclared_digests.items():
        check(
            relation_digest(database, name) == digest,
            f"the delta run changed {name}, a relation of a table that declares no block",
        )
    print(f"   undeclared relations unchanged: {', '.join(sorted(undeclared_digests))}")

    step("Replay the same input")
    output = runner.dbt("build", *BUILD_SELECTION)
    replayed_satellite = satellite_state(database, note_expected=False)
    check(
        replayed_satellite["count"] == delta_satellite["count"],
        f"{SATELLITE} appended "
        f"{replayed_satellite['count'] - delta_satellite['count']} row(s) on a replay",
    )
    check(
        replayed_satellite["identity_digest"] == delta_satellite["identity_digest"],
        f"{SATELLITE} moved a row identity on a replay",
    )
    replay_report = window_row_report(output)
    check(
        replay_report["relation_rows_total"] == delta_staging["count"],
        f"the replay changed the staged row count from {delta_staging['count']} to "
        f"{replay_report['relation_rows_total']}",
    )
    assert_staging_unique_key_holds(database, "replay")
    for name, digest in undeclared_digests.items():
        check(
            relation_digest(database, name) == digest,
            f"the replay changed {name}, a relation of a table that declares no block",
        )
    print(
        f"   {replayed_satellite['count']} stored version(s), zero appended; "
        f"{replay_report['relation_rows_in_window']} staging row(s) at or above the floor"
    )

    step("The recovery build: a full refresh over the same estate")
    # A full refresh regenerates the full candidate set and replay suppression converges
    # it against the satellites, plus any out-of-lookback late data the incremental path
    # excluded by declaration, which is the recovery this build is for. The scenario has
    # excluded none: every delivered record either entered through the initial load or
    # sits at or above the window floor, checked above. On that stated precondition the
    # recovery build has nothing to re-include, so it appends nothing.
    print(
        "   precondition: every delivered record entered through the initial load or "
        "sits at or above the window floor, so this build re-includes nothing"
    )
    recovery_build_index = len(runner.dbt_commands)
    runner.dbt("build", *BUILD_SELECTION, "--full-refresh")
    refreshed_satellite = satellite_state(database, note_expected=False)
    check(
        refreshed_satellite["identity_digest"] == delta_satellite["identity_digest"],
        f"{SATELLITE} history moved under a full refresh; the identity digest went from "
        f"{delta_satellite['identity_digest']} to {refreshed_satellite['identity_digest']}",
    )
    check(
        refreshed_satellite["count"] == delta_satellite["count"],
        f"{SATELLITE} appended "
        f"{refreshed_satellite['count'] - delta_satellite['count']} version(s) under a "
        "full refresh; the scenario excluded no out-of-lookback data, so the recovery "
        "build has nothing to re-include",
    )
    for name, digest in undeclared_digests.items():
        check(
            relation_digest(database, name) == digest,
            f"the recovery build changed {name}, a relation of a table that declares no "
            "block",
        )
    assert_staging_unique_key_holds(database, "recovery build")
    print(
        f"   history byte-equal on the identity digest, zero appended; undeclared "
        f"relations unchanged"
    )

    step("Add a payload field while the block stands")
    staging_schema = relation_schema(database, STAGING_MODEL)
    identity_token = "dpf-watermark-object-identity"
    stamp_object_identity(database, {STAGING_MODEL: staging_schema}, identity_token)
    before_extension = satellite_state(database, note_expected=False)
    declare_the_extension(runner)
    output = runner.emit()
    check(
        "estate evolution: extension" in output and NEW_COLUMN in output,
        "emit did not grade the payload addition as an extension:\n" + tail(output),
    )
    runner.dbt("seed", "--select", SEED, "--full-refresh")
    runner.dbt("build", *BUILD_SELECTION)

    extended_staging = staging_state(database, note_expected=True)
    check(
        NEW_COLUMN in extended_staging["columns"],
        f"{STAGING_MODEL} did not take {NEW_COLUMN}; the incremental staging relation "
        "has to take the column in place",
    )
    tokens = object_identity_tokens(database, {STAGING_MODEL: staging_schema})
    check(
        tokens[STAGING_MODEL] == identity_token,
        f"{STAGING_MODEL} lost its catalog object identity; the incremental relation was "
        "recreated rather than altered in place",
    )
    extension_key = (EXTENSION_RECORD, EXTENSION_ROW["as_of_date"])
    check(
        extension_key in extended_staging["rows"],
        f"the incremental staging relation did not take the delivered record "
        f"{EXTENSION_RECORD}",
    )
    check(
        extended_staging["rows"][extension_key][NEW_COLUMN] == EXTENSION_NOTE,
        f"the incremental staging relation carries "
        f"{extended_staging['rows'][extension_key][NEW_COLUMN]!r} in {NEW_COLUMN} for "
        f"{EXTENSION_RECORD}, not the delivered {EXTENSION_NOTE!r}",
    )
    assert_staging_unique_key_holds(database, "extension run")

    extended_satellite = satellite_state(database, note_expected=True)
    check(
        NEW_COLUMN in extended_satellite["columns"],
        f"{SATELLITE} did not take {NEW_COLUMN}",
    )
    new_versions = {
        identity: values
        for identity, values in extended_satellite["versions"].items()
        if identity not in before_extension["versions"]
    }
    check(
        len(new_versions) == 1,
        f"{SATELLITE} appended {len(new_versions)} version(s) for one delivered record, "
        "expected exactly one",
    )
    identity, values = next(iter(new_versions.items()))
    check(
        values["source_record_id"] == EXTENSION_RECORD,
        f"the appended version carries record {values['source_record_id']}, expected "
        f"{EXTENSION_RECORD}",
    )
    check(
        values[NEW_COLUMN] == EXTENSION_NOTE,
        f"the appended version carries {values[NEW_COLUMN]!r} in {NEW_COLUMN}, not the "
        f"delivered {EXTENSION_NOTE!r}",
    )
    for stored_identity in before_extension["versions"]:
        check(
            extended_satellite["versions"][stored_identity][NEW_COLUMN] is None,
            f"{SATELLITE} wrote a value into {NEW_COLUMN} on the version stored before "
            f"the extension: {stored_identity}",
        )
    print(
        f"   {NEW_COLUMN} reached the incremental staging relation in place and the one "
        f"new version, carrying {EXTENSION_NOTE!r}"
    )

    step("Every full refresh on the path is declared")
    for index, arguments in enumerate(runner.dbt_commands):
        if "--full-refresh" not in arguments:
            continue
        check(
            arguments[0] == "seed" or index == recovery_build_index,
            "the scenario ran a full refresh outside the landing layer and the one "
            "declared recovery build: dbt " + " ".join(arguments),
        )
    print("   the incremental path carries no full refresh")

    print("\nEvery watermark check held.")


def main(argv: list[str] | None = None) -> int:
    return run_lane("The watermark scenario.", "watermark", run_scenario, argv)


if __name__ == "__main__":
    raise SystemExit(main())
