"""Plain-script tests for logical-to-physical identifiers (architecture
sections 3, 7, 9, 10, 11, 13 and 15, P7).

A declaration keeps plain lower-case logical names and states, beside them,
the exact names an external interface requires. Nothing is inferred, nothing
is folded, and the renderer writes no quote character of its own: a stored
name reaches generated SQL only through the dispatch macro that calls the
running adapter's own quoting.

The fixture estate ``tests/fixtures/estates/physical_names`` is the whole
case in one place: a landing product whose delivered columns arrive under
the stored names, and a product that reads them by logical name and
publishes LEGACY_WORK.LEGACY_TARIFF_MASTER_T0 with the three required
column names.

Every command below is the real one, run as a subprocess against a scratch
copy of that estate. The five named rules are each driven red the same way,
on a scratch copy carrying exactly one injected violation.

Usage:
    python tests/python/test_physical_names.py
"""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import traceback
from pathlib import Path

import yaml

if __package__ in (None, ""):
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from ergasterion.translators.dbt_patterns.parse_gate import resolve_for_adapter

REPO_ROOT = Path(__file__).resolve().parents[2]
FIXTURE = REPO_ROOT / "tests" / "fixtures" / "estates" / "physical_names"
PYTHON = Path(sys.executable)

# The engine macros a copied fixture estate needs in order to build. The same
# set tests/python/engine_acceptance.py copies.
BUILD_MACROS = (
    "cross_db.sql",
    "filter_log.sql",
    "identifiers.sql",
    "product_tests.sql",
    "publish.sql",
    "quarantine.sql",
)

# The external interface this estate has to reproduce, stated here as the
# schema a downstream reader holds the estate to. Every check below compares
# what the engine produced against this, with no rename step in between.
REQUIRED_SCHEMA = "LEGACY_WORK"
REQUIRED_TABLE = "LEGACY_TARIFF_MASTER_T0"
REQUIRED_COLUMNS = ("PERIOD_RECORD_ID", "TARIFF_TYPE_NO", "Account_In_Scope_Flag")
LOGICAL_COLUMNS = ("period_record_id", "tariff_type_no", "account_in_scope_flag")

PRODUCT = "energy.tariff_master"
LANDING_PRODUCT = "energy.tariff_extract"
PRODUCT_MODEL = "energy__tariff_master"

# The two quote characters the shipped adapters wrap an identifier in. The
# renderer writes neither; this is the test's own copy, so a renderer that
# started writing one would be caught by a check that does not read the same
# constant the renderer would have used.
DOUBLE_QUOTE = chr(34)
BACKTICK = chr(96)

_JINJA = re.compile(r"\{\{.*?\}\}", re.DOTALL)


def _scratch_parent() -> Path:
    for name in ("TMPDIR", "TEMP", "TMP"):
        value = os.environ.get(name)
        if value and Path(value).is_dir():
            return Path(value)
    return Path(tempfile.gettempdir())


def _copy_estate(destination: Path) -> Path:
    """A writable copy of the fixture estate, with the engine macros a build
    needs beside it."""

    shutil.copytree(FIXTURE, destination)
    macro_dir = destination / "macros"
    macro_dir.mkdir(exist_ok=True)
    for name in BUILD_MACROS:
        shutil.copy2(REPO_ROOT / "macros" / name, macro_dir / name)
    return destination


def _ergasterion(root: Path, *arguments: str) -> tuple[int, str]:
    """One real ``ergasterion`` command against ``root``."""

    result = subprocess.run(
        [str(PYTHON), "-m", "ergasterion.cli", *arguments, "--estate-root", str(root)],
        cwd=str(REPO_ROOT), capture_output=True, text=True,
    )
    return result.returncode, result.stdout + result.stderr


def _dbt(root: Path, *arguments: str) -> tuple[int, str]:
    """One real ``dbt`` command against a copied estate."""

    executable = Path(sys.executable).resolve().parent / ("dbt.exe" if os.name == "nt" else "dbt")
    if not executable.is_file():
        executable = Path(shutil.which("dbt") or "dbt")
    environment = dict(os.environ)
    environment["DBT_SEND_ANONYMOUS_USAGE_STATS"] = "False"
    result = subprocess.run(
        [
            str(executable), *arguments,
            "--project-dir", ".", "--profiles-dir", "profiles", "--no-partial-parse",
        ],
        cwd=str(root), capture_output=True, text=True, env=environment,
    )
    return result.returncode, result.stdout + result.stderr


# The nine commands the requesting side has to be able to run, in order.
SEQUENCE: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("ergasterion validate", ("validate",)),
    ("ergasterion emit-products --check", ("emit-products", "--check")),
    ("ergasterion emit-products", ("emit-products",)),
    ("ergasterion lint --target duckdb", ("lint", "--target", "duckdb")),
    ("ergasterion lint --target bigquery", ("lint", "--target", "bigquery")),
    ("ergasterion structure", ("structure",)),
)
DBT_SEQUENCE: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("dbt parse --target duckdb", ("parse", "--target", "duckdb")),
    ("dbt parse --target bigquery", ("parse", "--target", "bigquery")),
)


_EMITTED: dict[str, object] = {}


def _last_line(output: str) -> str:
    return next((line for line in reversed(output.strip().splitlines()) if line.strip()), "").strip()


def _run_sequence(root: Path, *, allow_drift: bool) -> list[str]:
    """The requesting side's sequence, in order, against ``root``. Returns one
    line per command. ``allow_drift`` is true only on the first pass over an
    estate that has emitted nothing yet, where the drift check has a whole
    tree to report as missing and is doing its job by refusing."""

    lines: list[str] = []
    for label, arguments in SEQUENCE:
        code, output = _ergasterion(root, *arguments)
        lines.append(f"{label}: exit={code} {_last_line(output)}")
        if allow_drift and label == "ergasterion emit-products --check":
            assert code != 0, f"the drift check accepted an estate with nothing emitted:\n{output}"
            continue
        assert code == 0, f"{label} failed:\n{output}"
    for label, arguments in DBT_SEQUENCE:
        code, output = _dbt(root, *arguments)
        lines.append(f"{label}: exit={code} {_last_line(output)}")
        assert code == 0, f"{label} failed:\n{output[-3000:]}"
    return lines


def _emitted() -> dict[str, object]:
    """The estate run through the sequence twice and built once, and every
    artefact the checks read.

    The sequence runs twice because its second command is a drift check. On a
    scratch copy nothing is emitted yet, so that first check refuses, which is
    the check doing its job rather than the sequence failing. The second pass
    is the sequence as the requesting side runs it, against an estate whose
    tree is already on disk, and every one of its nine commands is green."""

    if _EMITTED:
        return _EMITTED
    root = _scratch_parent() / "dpf_physical_names"
    if root.exists():
        shutil.rmtree(root, ignore_errors=True)
    _copy_estate(root)

    first_pass = _run_sequence(root, allow_drift=True)
    lines = _run_sequence(root, allow_drift=False)

    code, build = _dbt(root, "build", "--target", "duckdb")
    assert code == 0, f"dbt build failed:\n{build[-3000:]}"
    lines.append(
        "dbt build --target duckdb: exit=0 "
        + next(line.split("Done.")[-1].strip() for line in build.splitlines() if "Done." in line)
    )

    _EMITTED["root"] = root
    _EMITTED["first_pass"] = first_pass
    _EMITTED["sequence"] = lines
    _EMITTED["build"] = build
    return _EMITTED


def _root() -> Path:
    return _emitted()["root"]  # type: ignore[return-value]


def _read_yaml(path: Path) -> dict:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


# --------------------------------------------------------------------------- tests


def test_the_requesting_sides_sequence_runs_green() -> None:
    """Every command of the unblock sequence, in order, against the fixture
    estate, plus the DuckDB build. All nine commands report success, and the
    drift check refuses the estate before anything is emitted."""

    emitted = _emitted()
    lines = emitted["sequence"]
    assert isinstance(lines, list) and len(lines) == 9, lines
    for line in lines:
        print(f"    {line}")
    failed = [line for line in lines if "exit=0" not in line]
    assert not failed, failed

    first_pass = emitted["first_pass"]
    assert isinstance(first_pass, list)
    refused = [line for line in first_pass if line.startswith("ergasterion emit-products --check")]
    assert refused and "exit=0" not in refused[0], first_pass


def test_emit_products_check_is_byte_stable_after_emission() -> None:
    """Re-running the drift check over what emission wrote reports no drift,
    and a second emission writes the same bytes."""

    root = _root()
    before = {
        path: path.read_bytes()
        for path in sorted((root / "models").rglob("*"))
        if path.is_file()
    }
    code, output = _ergasterion(root, "emit-products", "--check")
    assert code == 0, output
    code, output = _ergasterion(root, "emit-products")
    assert code == 0, output
    after = {
        path: path.read_bytes()
        for path in sorted((root / "models").rglob("*"))
        if path.is_file()
    }
    assert before == after, sorted(
        str(path) for path in set(before) | set(after) if before.get(path) != after.get(path)
    )


def test_the_built_relation_carries_the_required_schema_table_and_column_case() -> None:
    """After the DuckDB build, ``information_schema`` reports the relation
    and its three columns under exactly the names the interface requires."""

    import duckdb

    database = _root() / "target" / "physical_names.duckdb"
    connection = duckdb.connect(str(database), read_only=True)
    try:
        tables = connection.execute(
            "select table_schema, table_name from information_schema.tables "
            "where table_name = ?",
            [REQUIRED_TABLE],
        ).fetchall()
        assert tables == [(REQUIRED_SCHEMA, REQUIRED_TABLE)], tables
        columns = connection.execute(
            "select column_name from information_schema.columns "
            "where table_schema = ? and table_name = ? order by ordinal_position",
            [REQUIRED_SCHEMA, REQUIRED_TABLE],
        ).fetchall()
    finally:
        connection.close()
    assert tuple(name for (name,) in columns) == REQUIRED_COLUMNS, columns


def test_the_built_relation_carries_every_delivered_row() -> None:
    """The rename is a rename and nothing else: every seeded row reaches the
    published relation, read back through the required column names."""

    import duckdb

    database = _root() / "target" / "physical_names.duckdb"
    connection = duckdb.connect(str(database), read_only=True)
    try:
        rows = connection.execute(
            'select "PERIOD_RECORD_ID", "TARIFF_TYPE_NO", "Account_In_Scope_Flag" '
            f'from "{REQUIRED_SCHEMA}"."{REQUIRED_TABLE}" order by 1'
        ).fetchall()
    finally:
        connection.close()
    seed = (FIXTURE / "seeds" / "energy__tariff_extract.csv").read_text(encoding="utf-8")
    expected = [tuple(line.split(",")) for line in seed.strip().splitlines()[1:]]
    assert rows == sorted(expected), (rows, expected)


def test_the_renderer_writes_no_literal_quote_character() -> None:
    """No emitted model carries a quote character outside a template
    expression: every stored name is written through the dispatch macro, and
    the only quotes in the file are the Jinja string literals inside it."""

    offenders: list[str] = []
    for path in sorted((_root() / "models").rglob("*.sql")):
        outside = _JINJA.sub("", path.read_text(encoding="utf-8"))
        if DOUBLE_QUOTE in outside or BACKTICK in outside:
            offenders.append(path.name)
    assert not offenders, offenders

    model = (_root() / "models" / "products" / "energy" / f"{PRODUCT_MODEL}.sql").read_text(encoding="utf-8")
    for name in REQUIRED_COLUMNS:
        call = "{{ dpf_quote(" + DOUBLE_QUOTE + name + DOUBLE_QUOTE + ") }}"
        assert call in model, (name, model)


def test_the_parse_gate_resolves_the_quoting_per_adapter() -> None:
    """The same generated text resolves to the double-quoted form on DuckDB
    and the backticked form on BigQuery, which is the compiled SQL each
    adapter would carry."""

    model = (_root() / "models" / "products" / "energy" / f"{PRODUCT_MODEL}.sql").read_text(encoding="utf-8")
    duckdb_sql = resolve_for_adapter(model, artefact=f"{PRODUCT_MODEL}.sql", adapter="duckdb")
    bigquery_sql = resolve_for_adapter(model, artefact=f"{PRODUCT_MODEL}.sql", adapter="bigquery")
    for name in REQUIRED_COLUMNS:
        assert DOUBLE_QUOTE + name + DOUBLE_QUOTE in duckdb_sql, (name, duckdb_sql)
        assert BACKTICK + name + BACKTICK in bigquery_sql, (name, bigquery_sql)
    assert BACKTICK not in duckdb_sql, duckdb_sql
    assert DOUBLE_QUOTE not in bigquery_sql, bigquery_sql


def test_the_adapter_owned_quoting_is_dbts_own() -> None:
    """The macro's body is ``adapter.quote``, and the two installed dbt
    adapters implement it as the double-quoted and backticked forms the
    parse gate resolves to. No BigQuery account is reached: this is the
    adapter class the compiled SQL would come from."""

    from dbt.adapters.bigquery.impl import BigQueryAdapter
    from dbt.adapters.duckdb.impl import DuckDBAdapter

    macro = (REPO_ROOT / "macros" / "identifiers.sql").read_text(encoding="utf-8")
    assert "adapter.quote(name)" in macro, macro
    for name in REQUIRED_COLUMNS:
        assert BigQueryAdapter.quote(name) == BACKTICK + name + BACKTICK
        assert DuckDBAdapter.quote(name) == DOUBLE_QUOTE + name + DOUBLE_QUOTE


def test_the_compiled_duckdb_sql_carries_the_stored_names() -> None:
    """dbt's own compilation of the model, from the build above: the stored
    names, quoted by the adapter, with no macro call left in the text."""

    compiled = sorted((_root() / "target" / "compiled").rglob(f"{PRODUCT_MODEL}.sql"))
    assert compiled, "the DuckDB build compiled no model"
    text = compiled[0].read_text(encoding="utf-8")
    assert "dpf_quote" not in text, text
    for name in REQUIRED_COLUMNS:
        assert f"as {DOUBLE_QUOTE}{name}{DOUBLE_QUOTE}" in text, (name, text)


def test_the_consumer_reads_the_producers_stored_names() -> None:
    """The product reads the extract by the names the extract is stored
    under and carries the logical names forward. Its own declaration names
    none of them."""

    checked = (
        _root() / "models" / "products" / "energy" / f"{PRODUCT_MODEL}__checked.sql"
    ).read_text(encoding="utf-8")
    for stored, logical in zip(REQUIRED_COLUMNS, LOGICAL_COLUMNS):
        expected = "{{ dpf_quote(" + DOUBLE_QUOTE + stored + DOUBLE_QUOTE + ") }} as " + logical
        assert expected in checked, (expected, checked)
    declaration = (FIXTURE / "declarations" / "products" / "tariff_master.yml").read_text(
        encoding="utf-8"
    )
    sources = declaration.split("physical:")[0]
    for stored in REQUIRED_COLUMNS:
        assert stored not in sources, (stored, sources)


def test_the_contract_carries_the_required_schema_table_and_columns() -> None:
    """The generated ODCS contract states the stored table, the stored
    schema and the stored name of every column, so a downstream reader
    compares it against the required schema with no rename step."""

    document = _read_yaml(
        _root() / "contracts" / "products" / "energy" / "tariff_master"
        / "tariff_master.odcs.yml"
    )
    schema_object = document["schema"][0]
    assert schema_object["physicalName"] == REQUIRED_TABLE, schema_object
    custom = {entry["property"]: entry["value"] for entry in schema_object.get("customProperties", [])}
    assert custom.get("dpf.physicalSchema") == REQUIRED_SCHEMA, custom
    properties = schema_object["properties"]
    assert tuple(entry["name"] for entry in properties) == LOGICAL_COLUMNS, properties
    assert tuple(entry["physicalName"] for entry in properties) == REQUIRED_COLUMNS, properties


def test_the_landing_contract_carries_the_delivered_stored_names() -> None:
    """The extract's own contract carries the stored name of every delivered
    column, which is where the consumer's read resolves them from."""

    document = _read_yaml(
        _root() / "contracts" / "products" / "energy" / "tariff_extract"
        / "tariff_extract.odcs.yml"
    )
    properties = document["schema"][0]["properties"]
    assert tuple(entry["name"] for entry in properties) == LOGICAL_COLUMNS, properties
    assert tuple(entry["physicalName"] for entry in properties) == REQUIRED_COLUMNS, properties


def test_the_runtime_manifest_carries_both_names() -> None:
    """The manifest records the stored schema and table of the relation and
    the stored name of every column beside its logical name."""

    manifest = json.loads(
        (_root() / "manifests" / "products" / "energy" / "tariff_master.json").read_text(
            encoding="utf-8"
        )
    )
    stored = manifest["stored"]
    assert len(stored) == 1, stored
    entry = stored[0]
    assert entry["relation"] == PRODUCT, entry
    assert entry["physical_relation"] == {"schema": REQUIRED_SCHEMA, "name": REQUIRED_TABLE}, entry
    assert tuple(column["name"] for column in entry["columns"]) == LOGICAL_COLUMNS, entry
    assert tuple(column["physical_name"] for column in entry["columns"]) == REQUIRED_COLUMNS, entry


def test_the_product_graph_carries_both_names() -> None:
    """The graph description resolves every logical column the field-lineage
    rows carry to the name the built relation stores it under."""

    description = json.loads(
        (_root() / "graphs" / "products" / "product-graph.json").read_text(encoding="utf-8")
    )
    relations = {
        (entry["product"], entry["relation"]): (entry["physical_schema"], entry["physical_name"])
        for entry in description["stored_relations"]
    }
    assert relations[(PRODUCT, PRODUCT)] == [REQUIRED_SCHEMA, REQUIRED_TABLE] or relations[
        (PRODUCT, PRODUCT)
    ] == (REQUIRED_SCHEMA, REQUIRED_TABLE), relations

    fields = [entry for entry in description["stored_fields"] if entry["product"] == PRODUCT]
    assert tuple(entry["field"] for entry in fields) == LOGICAL_COLUMNS, fields
    assert tuple(entry["physical_name"] for entry in fields) == REQUIRED_COLUMNS, fields

    # The field-lineage table keeps the shape it always had: the stored
    # names sit beside it rather than inside it, so the graph of an estate
    # that renames nothing is byte-identical to the graph it was.
    headers = description["csv"]["field_lineage_headers"]
    assert headers == [
        "edge_id",
        "product",
        "target_field",
        "source_kind",
        "source_product",
        "source",
        "occurrence",
        "transform",
    ], headers


def test_the_compliance_check_names_the_stored_columns() -> None:
    """The generated contract-compliance check reads the built relation's own
    columns, so it compares the stored names rather than the logical ones."""

    document = _read_yaml(_root() / "models" / "products" / "energy" / f"{PRODUCT_MODEL}.yml")
    model = next(entry for entry in document["models"] if entry["name"] == PRODUCT_MODEL)
    assert tuple(column["name"] for column in model["columns"]) == REQUIRED_COLUMNS, model
    compliance = next(
        test["dpf_contract_compliance"]
        for test in model["data_tests"]
        if "dpf_contract_compliance" in test
    )
    assert tuple(compliance["arguments"]["columns"]) == REQUIRED_COLUMNS, compliance


# --------------------------------------------------------------------------- red proofs

# One injected violation per named rule. Each is applied to a scratch copy of
# the fixture estate and driven through the real ``ergasterion validate``.
RED_CASES: tuple[tuple[str, str, str, str, str], ...] = (
    (
        "physical_name_missing",
        "duckdb",
        "tariff_master.yml",
        "    - {name: tariff_type_no, physical_name: TARIFF_TYPE_NO}",
        '    - {name: tariff_type_no, physical_name: "   "}',
    ),
    (
        "physical_name_unportable",
        "duckdb",
        "tariff_master.yml",
        "    - {name: tariff_type_no, physical_name: TARIFF_TYPE_NO}",
        "    - {name: tariff_type_no, physical_name: LEGACY_WORK.TARIFF_TYPE_NO}",
    ),
    (
        "physical_name_duplicate",
        "duckdb",
        "tariff_master.yml",
        "    - {name: tariff_type_no, physical_name: TARIFF_TYPE_NO}",
        "    - {name: tariff_type_no, physical_name: period_record_id}",
    ),
    (
        "physical_schema_unaddressable",
        "duckdb",
        "tariff_master.yml",
        "  schema: LEGACY_WORK",
        "  schema: LEGACY_WORK.STAGE",
    ),
    (
        "physical_relation_conflict",
        "duckdb",
        "tariff_extract.yml",
        "steps:",
        "physical:\n  name: legacy_tariff_master_t0\n  schema: legacy_work\n\nsteps:",
    ),
)


def _red(rule: str, declaration: str, old: str, new: str, *, adapter_first: str | None = None) -> str:
    """One scratch copy of the fixture estate carrying exactly one injected
    violation, driven through the real command. Returns what the command
    printed."""

    root = _scratch_parent() / f"dpf_physical_names_red_{rule}_{adapter_first or 'declared'}"
    if root.exists():
        shutil.rmtree(root, ignore_errors=True)
    _copy_estate(root)
    path = root / "declarations" / "products" / declaration
    text = path.read_text(encoding="utf-8")
    assert old in text, (rule, old)
    path.write_text(text.replace(old, new, 1), encoding="utf-8")
    if adapter_first is not None:
        estate_file = root / "estate.yml"
        estate = estate_file.read_text(encoding="utf-8")
        reordered = estate.replace(
            "  adapters:\n    duckdb:\n      kind: reference\n    bigquery:\n      kind: deployment\n",
            "  adapters:\n    bigquery:\n      kind: deployment\n    duckdb:\n      kind: reference\n",
            1,
        )
        assert reordered != estate, "the adapters block was not reordered"
        estate_file.write_text(reordered, encoding="utf-8")
    code, output = _ergasterion(root, "validate")
    assert code != 0, f"{rule}: validate accepted the injected violation:\n{output}"
    shutil.rmtree(root, ignore_errors=True)
    return output


def test_every_named_rule_is_refused_naming_product_relation_column_and_adapter() -> None:
    """Each of the five rules, driven red on its own scratch copy through the
    real command. Every message names the product, the relation, the column
    (``n/a`` where the rule is about the relation itself) and the adapter."""

    for rule, adapter, declaration, old, new in RED_CASES:
        output = _red(rule, declaration, old, new)
        print(f"    {rule}: {output.strip().splitlines()[-1][:150]}")
        assert rule in output, (rule, output)
        assert "product 'energy." in output, (rule, output)
        assert f"adapter '{adapter}'" in output, (rule, output)
        assert "relation 'energy." in output, (rule, output)
        assert "column:" in output or "columns '" in output, (rule, output)


def test_a_case_only_duplicate_is_refused_on_both_adapters() -> None:
    """Two columns whose stored names differ only in case are one name on
    each declared adapter, because each declares an insensitive comparison.
    The rule names whichever adapter the estate declares first, so the same
    violation is driven twice, once per declaration order."""

    injected = (
        "tariff_master.yml",
        "    - {name: tariff_type_no, physical_name: TARIFF_TYPE_NO}",
        "    - {name: tariff_type_no, physical_name: period_record_ID}",
    )
    for adapter, order in (("duckdb", None), ("bigquery", "bigquery")):
        output = _red("physical_name_duplicate", *injected, adapter_first=order)
        assert "physical_name_duplicate" in output, (adapter, output)
        assert f"adapter '{adapter}'" in output, (adapter, output)
        assert "'insensitive'" in output, (adapter, output)


def test_a_declaration_addressing_an_unpublished_relation_is_refused() -> None:
    """A ``physical.relations`` entry naming a relation the product's shape
    does not publish is a statement about nothing, and is refused rather than
    ignored."""

    output = _red(
        "physical_name_missing",
        "tariff_master.yml",
        "physical:\n  name: LEGACY_TARIFF_MASTER_T0",
        "physical:\n  relations:\n    fact_tariff:\n      name: FACT_TARIFF\n  name: LEGACY_TARIFF_MASTER_T0",
    )
    assert "fact_tariff" in output, output
    assert "shape publishes" in output, output


def test_a_declaration_naming_an_unpublished_column_is_refused_by_validate() -> None:
    """A ``fields`` entry naming a column the relation does not publish is a
    stored name for nothing, and ``ergasterion validate`` refuses it rather
    than leaving it to emission."""

    output = _red(
        "unknown_physical_column",
        "tariff_master.yml",
        "    - {name: tariff_type_no, physical_name: TARIFF_TYPE_NO}",
        "    - {name: nothing_carries_this, physical_name: TARIFF_TYPE_NO}",
    )
    assert "does not publish column 'nothing_carries_this'" in output, output
    assert "FAIL (layer 1)" in output, output
    assert f"product '{PRODUCT}'" in output, output


def test_an_incremental_publication_is_keyed_by_the_stored_column_names() -> None:
    """A product that republishes by key states the key in logical names, and
    the generated configuration carries the names the built relation actually
    has, beside the stored coordinate of the relation itself. A key on a
    column with no stored name of its own stays the logical name."""

    from ergasterion.framework.models import RelationField, RelationSchema
    from ergasterion.translators.dbt_patterns.publish import incremental_config

    relation = RelationSchema(
        name=PRODUCT,
        fields=(
            RelationField(name="period_record_id", type="string", physical_name=REQUIRED_COLUMNS[0]),
            RelationField(name="tariff_type_no", type="string", physical_name=REQUIRED_COLUMNS[1]),
            RelationField(name="load_batch_id", type="string"),
        ),
        physical_name=REQUIRED_TABLE,
        physical_schema=REQUIRED_SCHEMA,
    )
    rendered = incremental_config(
        ["period_record_id", "load_batch_id"],
        product=PRODUCT,
        strategies={"duckdb": "delete+insert", "bigquery": "merge"},
        occurrence="data_publish",
        relation=relation,
    )
    assert f'unique_key=[{DOUBLE_QUOTE}{REQUIRED_COLUMNS[0]}{DOUBLE_QUOTE}, ' in rendered, rendered
    assert f'{DOUBLE_QUOTE}load_batch_id{DOUBLE_QUOTE}]' in rendered, rendered
    assert f'alias={DOUBLE_QUOTE}{REQUIRED_TABLE}{DOUBLE_QUOTE}' in rendered, rendered
    assert f'schema={DOUBLE_QUOTE}{REQUIRED_SCHEMA}{DOUBLE_QUOTE}' in rendered, rendered
    assert "materialized='incremental'" in rendered, rendered
    assert "period_record_id" not in rendered.split("unique_key=")[1], rendered

    plain = incremental_config(
        ["period_record_id"],
        product=PRODUCT,
        strategies={"duckdb": "delete+insert"},
        occurrence="data_publish",
        relation=None,
    )
    assert f'unique_key=[{DOUBLE_QUOTE}period_record_id{DOUBLE_QUOTE}]' in plain, plain
    assert "alias=" not in plain and "schema=" not in plain, plain


def test_the_schema_override_is_marked_for_the_one_dbt_hook_that_reads_it() -> None:
    """dbt resolves a model's schema through one project-wide hook, so the
    renderer marks the relations that declared one and the hook changes the
    answer for those alone. Every other node keeps dbt's own default."""

    from ergasterion.translators.dbt_patterns.publish import PHYSICAL_SCHEMA_FLAG

    model = (_root() / "models" / "products" / "energy" / f"{PRODUCT_MODEL}.sql").read_text(
        encoding="utf-8"
    )
    assert f'meta={{{DOUBLE_QUOTE}{PHYSICAL_SCHEMA_FLAG}{DOUBLE_QUOTE}: true}}' in model, model
    macro = (REPO_ROOT / "macros" / "identifiers.sql").read_text(encoding="utf-8")
    assert "dbt.default__generate_schema_name(custom_schema_name, node)" in macro, macro
    assert "dpf.physical_schema" in macro, macro

    # Every other model this estate publishes declares no schema, so none of
    # them carries the marker and none of them changes schema.
    for path in sorted((_root() / "models").rglob("*.sql")):
        if path.name == f"{PRODUCT_MODEL}.sql":
            continue
        assert PHYSICAL_SCHEMA_FLAG not in path.read_text(encoding="utf-8"), path.name


def test_a_declaration_with_no_physical_block_is_unchanged() -> None:
    """The estate with its stored names removed emits a model that renames
    nothing: the whole surface is opt-in."""

    root = _scratch_parent() / "dpf_physical_names_plain"
    if root.exists():
        shutil.rmtree(root, ignore_errors=True)
    _copy_estate(root)
    for name in ("tariff_master.yml", "tariff_extract.yml"):
        path = root / "declarations" / "products" / name
        text = path.read_text(encoding="utf-8")
        text = re.sub(r"\n *physical:\n(?:.*\n)*?(?=\n[a-z])", "\n", text)
        text = re.sub(r", physical_name: [^}]+\}", "}", text)
        path.write_text(text, encoding="utf-8")
    try:
        code, output = _ergasterion(root, "emit-products")
        assert code == 0, output
        model = (root / "models" / "products" / "energy" / f"{PRODUCT_MODEL}.sql").read_text(
            encoding="utf-8"
        )
        assert "dpf_quote" not in model, model
        assert "alias=" not in model and "schema=" not in model, model
        for logical in LOGICAL_COLUMNS:
            assert f"    {logical}" in model, (logical, model)
    finally:
        shutil.rmtree(root, ignore_errors=True)


def test_a_shape_relation_takes_its_own_stored_coordinate_and_columns() -> None:
    """A shape that publishes several relations addresses each one by the
    name the shape gives it. The relation's own coordinate becomes the dbt
    alias and schema, and its declared column names become one projection
    over whatever the shape's arm rendered, so every derivation is renamed
    the same way."""

    from ergasterion.framework.models import RelationField, RelationSchema
    from ergasterion.framework.shapes import DERIVATION_COMPOSITION, ShapeRelation
    from ergasterion.translators.dbt_patterns import _stored_projection
    from ergasterion.translators.dbt_patterns.publish import stored_coordinate

    schema = RelationSchema(
        name="sales.order_star__fact_order_line",
        fields=(
            RelationField(name="order_line_id", type="string", physical_name="ORDER_LINE_ID"),
            RelationField(name="net_amount", type="string"),
        ),
        physical_name="FACT_ORDER_LINE_T0",
        physical_schema=REQUIRED_SCHEMA,
    )
    relation = ShapeRelation(
        suffix="fact_order_line", schema=schema, derivation=DERIVATION_COMPOSITION
    )

    from ergasterion.translators.dbt_patterns.publish import PHYSICAL_SCHEMA_FLAG

    coordinate = stored_coordinate(schema, product="sales.order_star")
    assert coordinate == (
        ', alias="FACT_ORDER_LINE_T0", schema="'
        + REQUIRED_SCHEMA
        + '", meta={"'
        + PHYSICAL_SCHEMA_FLAG
        + '": true}'
    ), coordinate

    ctes, columns, final = _stored_projection(
        relation,
        ctes=(),
        columns=["order_line_id", "cast(amount as numeric) as net_amount"],
        final_cte="base",
        product="sales.order_star",
    )
    assert final == "stored_names_input", final
    assert len(ctes) == 1 and ctes[0].name == "stored_names_input", ctes
    assert "cast(amount as numeric) as net_amount" in ctes[0].body, ctes[0].body
    assert columns == [
        "order_line_id as {{ dpf_quote(" + DOUBLE_QUOTE + "ORDER_LINE_ID" + DOUBLE_QUOTE + ") }}",
        "net_amount",
    ], columns

    plain = ShapeRelation(
        suffix="fact_order_line",
        schema=RelationSchema(name=schema.name, fields=(RelationField(name="a", type="string"),)),
        derivation=DERIVATION_COMPOSITION,
    )
    assert stored_coordinate(plain.schema, product="sales.order_star") == ""
    assert _stored_projection(
        plain, ctes=(), columns=["a"], final_cte="base", product="sales.order_star"
    ) == ([], ["a"], "base")


TESTS = [
    test_the_requesting_sides_sequence_runs_green,
    test_emit_products_check_is_byte_stable_after_emission,
    test_the_built_relation_carries_the_required_schema_table_and_column_case,
    test_the_built_relation_carries_every_delivered_row,
    test_the_renderer_writes_no_literal_quote_character,
    test_the_parse_gate_resolves_the_quoting_per_adapter,
    test_the_adapter_owned_quoting_is_dbts_own,
    test_the_compiled_duckdb_sql_carries_the_stored_names,
    test_the_consumer_reads_the_producers_stored_names,
    test_the_contract_carries_the_required_schema_table_and_columns,
    test_the_landing_contract_carries_the_delivered_stored_names,
    test_the_runtime_manifest_carries_both_names,
    test_the_product_graph_carries_both_names,
    test_the_compliance_check_names_the_stored_columns,
    test_every_named_rule_is_refused_naming_product_relation_column_and_adapter,
    test_a_case_only_duplicate_is_refused_on_both_adapters,
    test_a_declaration_addressing_an_unpublished_relation_is_refused,
    test_a_declaration_naming_an_unpublished_column_is_refused_by_validate,
    test_an_incremental_publication_is_keyed_by_the_stored_column_names,
    test_the_schema_override_is_marked_for_the_one_dbt_hook_that_reads_it,
    test_a_declaration_with_no_physical_block_is_unchanged,
    test_a_shape_relation_takes_its_own_stored_coordinate_and_columns,
]


def main() -> int:
    failures = 0
    for test in TESTS:
        try:
            test()
            print(f"PASS {test.__name__}")
        except Exception:
            failures += 1
            print(f"FAIL {test.__name__}")
            traceback.print_exc()
    root = _EMITTED.get("root")
    if isinstance(root, Path):
        shutil.rmtree(root, ignore_errors=True)
    print(f"{len(TESTS) - failures}/{len(TESTS)} physical-name tests passed")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
