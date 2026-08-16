"""Create a declaration or domain skeleton from ``CREATE TABLE`` statements.

The command has two modes and reads only the structure present in the DDL:

  --mode feed   feed DDL (one source system's raw CREATE TABLE set) -> a
                declarations/<source>.yml stub: schema projection + seed_tests/model_tests
                skeleton, with column types and declared constraints transcribed into
                projections and data tests.

  --mode model  model DDL (a domain's CREATE TABLE set, PRIMARY KEY + FOREIGN KEY
                declared) -> a domains/<name>.yml stub: entity_configs / hub_configs /
                link_configs derived from the PK/FK structure by this repo's own
                golden_<entity>_key / <entity>_hk / <entity>_hashdiff naming convention
                (a structural convention, not a business-semantics guess). What no DDL carries -- survivorship rules
                (bv_configs), entity-resolution match-key strategy (res_configs), the
                map-lane relation vocabulary (relations), the ODCS contract-adapter
                boundary (odcs) -- is NEVER guessed: left as an explicit TODO comment
                block for a human to fill in by hand.

The generated file is an editable starting point. Ergasterion never guesses survivorship,
entity-resolution, relation vocabulary, or contract-boundary semantics. Review and complete
those fields before emitting a pipeline. Re-running with ``--force`` overwrites the file.

The parser covers a well-formed, unadorned ANSI-ish CREATE TABLE surface: column defs
(name, type incl. one level of parenthesised type args, NOT NULL, PRIMARY KEY, UNIQUE,
inline REFERENCES), plus table-level PRIMARY KEY(...)/FOREIGN KEY(...) REFERENCES
...(...)/UNIQUE(...) constraints, with -- line comments and /* */ block comments
stripped first. DEFAULT expressions and CHECK constraints are read past, never modeled
onto the seed (they carry no shape this format needs). Multiple CREATE TABLE statements
in one input are all seeded together (one declarations/domains file per --mode run).

Usage:
    python ergasterion/import_ddl.py <path-to.ddl.sql> --mode feed --source <name>
    python ergasterion/import_ddl.py <path-to.ddl.sql> --mode model --domain <name>
"""

from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

# Support installed-command and direct-script execution.
if __package__ in (None, ""):
    import os as _os, sys as _sys
    _sys.path.insert(0, _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__))))

from ergasterion import emit
from ergasterion.estate import EstateContext
# SSOT reuse (never duplicated): the logical-type -> dpf_safe_cast expression map and the
# source/domain-name slugifier already live in import_odcs.py -- this sibling module reads
# them rather than re-declaring the same mapping under a second name.
from ergasterion.import_odcs import _cast_expression, _slugify

# Ambient estate context; main() resolves its own (honouring --estate-root).
_DEFAULT_CTX = EstateContext.default()
REPO_ROOT = _DEFAULT_CTX.root
DECLARATION_TEMPLATE = "declaration_seed.yml.j2"
DOMAIN_TEMPLATE = "domain_seed.yml.j2"


class DdlImportError(ValueError):
    """A supplied DDL failed to parse into at least one well-formed CREATE TABLE --
    the message always names the specific problem, never a silent best-effort read."""


# --- SQL type -> ODCS-style logical-type bucket --------------------------------------
# The same seven-bucket vocabulary import_odcs.py already casts from (string / integer /
# number / date / boolean / object) -- a DDL type keyword is mapped onto whichever bucket
# it is closest to; DATE and every TIMESTAMP-family type share the "date" bucket because
# that is the only temporal cast this factory's macros/cross_db.sql dpf_safe_cast dispatch
# supports (dpf_type: int|float|numeric|date|string|boolean) -- a documented, mechanical
# choice, not a business-semantics guess. The supplier's raw DDL type always rides the
# projection entry as an informational trailing comment (physical_type), same as
# import_odcs.py's physicalType comment, so nothing about the source type is lost.
_SQL_TYPE_TO_LOGICAL: dict[str, str] = {
    "int": "integer", "integer": "integer", "bigint": "integer", "smallint": "integer",
    "tinyint": "integer", "serial": "integer", "bigserial": "integer",
    "int2": "integer", "int4": "integer", "int8": "integer",
    "numeric": "number", "decimal": "number", "number": "number",
    "float": "number", "float4": "number", "float8": "number",
    "double": "number", "real": "number", "money": "number",
    "varchar": "string", "char": "string", "character": "string", "text": "string",
    "string": "string", "nvarchar": "string", "nchar": "string", "clob": "string", "uuid": "string",
    "date": "date",
    "timestamp": "date", "timestamptz": "date", "datetime": "date", "time": "date",
    "boolean": "boolean", "bool": "boolean",
    "json": "object", "jsonb": "object", "variant": "object", "struct": "object", "array": "object",
}


def _sql_base_type(sql_type: str) -> str:
    match = re.match(r"[A-Za-z_]+", sql_type)
    return match.group(0).lower() if match else ""


def _sql_type_to_logical(sql_type: str) -> str:
    return _SQL_TYPE_TO_LOGICAL.get(_sql_base_type(sql_type), "string")


# --- DDL structure ---------------------------------------------------------------------

@dataclass
class Column:
    name: str
    sql_type: str
    nullable: bool = True
    primary_key: bool = False
    unique: bool = False
    references: tuple[str, str] | None = None  # (ref_table, ref_column)


@dataclass
class ForeignKey:
    columns: list[str]
    ref_table: str
    ref_columns: list[str]


@dataclass
class ParsedTable:
    name: str
    columns: list[Column] = field(default_factory=list)
    primary_key: list[str] = field(default_factory=list)
    foreign_keys: list[ForeignKey] = field(default_factory=list)
    table_unique: set[str] = field(default_factory=set)  # single-column table-level UNIQUE(...)


# --- tokenizing helpers (paren/quote aware) ---------------------------------------------

_QUOTE_CHARS = ("'", '"', "`")
_COMMENT_LINE_RE = re.compile(r"--[^\n]*")
_COMMENT_BLOCK_RE = re.compile(r"/\*.*?\*/", re.DOTALL)


def _strip_comments(text: str) -> str:
    text = _COMMENT_BLOCK_RE.sub(" ", text)
    text = _COMMENT_LINE_RE.sub("", text)
    return text


def _strip_ident(token: str) -> str:
    token = token.strip()
    if len(token) >= 2 and token[0] == token[-1] and token[0] in _QUOTE_CHARS:
        return token[1:-1]
    if len(token) >= 2 and token[0] == "[" and token[-1] == "]":
        return token[1:-1]
    return token


def _split_top_level(text: str, sep: str = ",") -> list[str]:
    """Split on ``sep`` at paren-depth 0, outside any quoted string. Used both for
    splitting a CREATE TABLE body into column/constraint defs and for splitting a
    parenthesised column list."""
    parts: list[str] = []
    depth = 0
    quote: str | None = None
    start = 0
    for i, ch in enumerate(text):
        if quote:
            if ch == quote:
                quote = None
            continue
        if ch in _QUOTE_CHARS:
            quote = ch
        elif ch in "([":
            depth += 1
        elif ch in ")]":
            depth -= 1
        elif ch == sep and depth == 0:
            parts.append(text[start:i])
            start = i + 1
    parts.append(text[start:])
    return [p.strip() for p in parts if p.strip()]


def _find_matching_paren(text: str, open_idx: int, table_name: str) -> int:
    depth = 0
    quote: str | None = None
    for i in range(open_idx, len(text)):
        ch = text[i]
        if quote:
            if ch == quote:
                quote = None
            continue
        if ch in _QUOTE_CHARS:
            quote = ch
        elif ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth == 0:
                return i
    raise DdlImportError(f"CREATE TABLE {table_name}: unbalanced parentheses")


def _cols(raw: str) -> list[str]:
    return [_strip_ident(c) for c in raw.split(",") if c.strip()]


_CREATE_TABLE_RE = re.compile(
    r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?"
    r"([\"`\[]?[\w.]+[\"`\]]?)\s*\(",
    re.IGNORECASE,
)
_PK_RE = re.compile(r"^(?:CONSTRAINT\s+\S+\s+)?PRIMARY\s+KEY\s*\(([^)]*)\)", re.IGNORECASE)
_FK_RE = re.compile(
    r"^(?:CONSTRAINT\s+\S+\s+)?FOREIGN\s+KEY\s*\(([^)]*)\)\s*REFERENCES\s+"
    r"([\"`\[]?[\w.]+[\"`\]]?)\s*\(([^)]*)\)",
    re.IGNORECASE,
)
_UNIQUE_RE = re.compile(r"^(?:CONSTRAINT\s+\S+\s+)?UNIQUE\s*\(([^)]*)\)", re.IGNORECASE)
_CHECK_RE = re.compile(r"^(?:CONSTRAINT\s+\S+\s+)?CHECK\s*\(", re.IGNORECASE)
_IDENT_THEN_REST_RE = re.compile(r'^([\"`\[]?[\w]+[\"`\]]?)\s+(.*)$', re.DOTALL)
_TYPE_RE = re.compile(r"^(\w+)\s*(\([^)]*\))?")
_REFERENCES_INLINE_RE = re.compile(
    r"REFERENCES\s+([\"`\[]?[\w.]+[\"`\]]?)\s*\(([^)]*)\)", re.IGNORECASE
)


def _parse_column(raw: str, table: ParsedTable) -> None:
    ident_match = _IDENT_THEN_REST_RE.match(raw.strip())
    if not ident_match:
        raise DdlImportError(f"CREATE TABLE {table.name}: cannot parse column definition {raw!r}")
    name = _strip_ident(ident_match.group(1))
    rest = ident_match.group(2).strip()
    type_match = _TYPE_RE.match(rest)
    if not type_match:
        raise DdlImportError(f"CREATE TABLE {table.name}: column {name!r} has no type in {raw!r}")
    sql_type = type_match.group(1).upper()
    if type_match.group(2):
        sql_type += type_match.group(2).replace(" ", "")
    remainder = rest[type_match.end():].strip()
    upper_remainder = remainder.upper()

    primary_key = bool(re.search(r"\bPRIMARY\s+KEY\b", upper_remainder))
    nullable = not (primary_key or re.search(r"\bNOT\s+NULL\b", upper_remainder))
    unique = bool(re.search(r"\bUNIQUE\b", upper_remainder))

    references: tuple[str, str] | None = None
    ref_match = _REFERENCES_INLINE_RE.search(remainder)
    if ref_match:
        ref_table = _strip_ident(ref_match.group(1).split(".")[-1])
        ref_cols = _cols(ref_match.group(2))
        references = (ref_table, ref_cols[0] if ref_cols else name)

    column = Column(
        name=name, sql_type=sql_type, nullable=nullable,
        primary_key=primary_key, unique=unique, references=references,
    )
    table.columns.append(column)
    if primary_key:
        table.primary_key.append(name)
    if references:
        table.foreign_keys.append(
            ForeignKey(columns=[name], ref_table=references[0], ref_columns=[references[1]])
        )


def _parse_def(raw_def: str, table: ParsedTable) -> None:
    stripped = raw_def.strip()
    pk_match = _PK_RE.match(stripped)
    if pk_match:
        table.primary_key.extend(_cols(pk_match.group(1)))
        return
    fk_match = _FK_RE.match(stripped)
    if fk_match:
        table.foreign_keys.append(ForeignKey(
            columns=_cols(fk_match.group(1)),
            ref_table=_strip_ident(fk_match.group(2).split(".")[-1]),
            ref_columns=_cols(fk_match.group(3)),
        ))
        return
    unique_match = _UNIQUE_RE.match(stripped)
    if unique_match:
        cols = _cols(unique_match.group(1))
        if len(cols) == 1:
            table.table_unique.add(cols[0])
        return
    if _CHECK_RE.match(stripped):
        return  # CHECK constraints are read past -- not modeled onto the seed
    _parse_column(stripped, table)


def parse_ddl(text: str) -> list[ParsedTable]:
    """Parse every ``CREATE TABLE`` statement in ``text`` into a ParsedTable. Raises
    DdlImportError naming the specific problem for malformed input; never a silent
    best-effort parse (import_odcs.py's posture, verbatim)."""
    cleaned = _strip_comments(text)
    tables: list[ParsedTable] = []
    for match in _CREATE_TABLE_RE.finditer(cleaned):
        table_name = _strip_ident(match.group(1).split(".")[-1])
        open_idx = match.end() - 1
        close_idx = _find_matching_paren(cleaned, open_idx, table_name)
        body = cleaned[open_idx + 1:close_idx]
        table = ParsedTable(name=table_name)
        for raw_def in _split_top_level(body):
            _parse_def(raw_def, table)
        if not table.columns:
            raise DdlImportError(f"CREATE TABLE {table_name}: no column definitions found")
        tables.append(table)
    if not tables:
        raise DdlImportError("no CREATE TABLE statement found in the supplied DDL")
    return tables


# --- (a) feed DDL -> declarations/<source>.yml -----------------------------------------

def _relative_path(path: Path) -> str:
    try:
        return path.relative_to(REPO_ROOT).as_posix()
    except ValueError:
        return path.as_posix()


def _declaration_header(ddl_path: Path, tables: list[ParsedTable]) -> str:
    table_names = ", ".join(t.name for t in tables)
    return "\n".join([
        f"# Seeded by ergasterion/import_ddl.py --mode feed from DDL: {_relative_path(ddl_path)}",
        f"#   tables={table_names!r}",
        "#",
        "# This is a STARTING POINT, not regenerated output -- unlike ergasterion/emit.py's and",
        "# ergasterion/emit_contracts.py's outputs, this file is meant to be hand-edited. It",
        "# mechanically transcribes the DDL's CREATE TABLE column list (name, type -> cast",
        "# expression, PRIMARY KEY/UNIQUE/NOT NULL -> seed_tests/model_tests) into",
        "# projection stubs plus a seed_tests/model_tests skeleton. What no DDL carries --",
        "# vault_entities mapping, entity_resolution config, survivorship stance -- is left",
        "# as an explicit TODO below and never guessed. Run ergasterion/emit.py once those",
        "# TODOs are filled in.",
    ])


def build_declaration_tables(tables: list[ParsedTable]) -> list[dict[str, Any]]:
    """Mechanically transcribe parsed DDL tables into declaration table dicts: one per
    CREATE TABLE, straight passthrough of column names, types -> cast expressions,
    NOT NULL/PRIMARY KEY/UNIQUE -> seed_tests + model_tests. No vault/entity-resolution/
    survivorship content -- see module docstring."""
    out: list[dict[str, Any]] = []
    for table in tables:
        pk_cols = set(table.primary_key)
        single_col_pk = pk_cols if len(pk_cols) == 1 else set()
        projection: list[dict[str, Any]] = []
        column_tests: list[dict[str, Any]] = []
        for col in table.columns:
            logical_type = _sql_type_to_logical(col.sql_type)
            projection.append({
                "name": col.name,
                "expression": _cast_expression(col.name, logical_type),
                "physical_type": col.sql_type,
            })
            data_tests: list[str] = []
            if (not col.nullable) or col.name in pk_cols:
                data_tests.append("not_null")
            if col.unique or col.name in single_col_pk or col.name in table.table_unique:
                data_tests.append("unique")
            if data_tests:
                column_tests.append({"name": col.name, "data_tests": data_tests})
        out.append({
            "name": table.name,
            "raw_model": None,
            "description": f"TODO: describe {table.name}",
            "seed_tests": list(column_tests),
            "model_tests": list(column_tests),
            "projection": projection,
        })
    return out


def build_declaration_context(
    tables: list[ParsedTable],
    ddl_path: Path,
    source_name: str,
    display_name: str | None,
    priority: int,
) -> dict[str, Any]:
    display_name = display_name or source_name.upper()
    decl_tables = build_declaration_tables(tables)
    for decl_table in decl_tables:
        decl_table["raw_model"] = f"raw_{source_name}_{decl_table['name']}"
    return {
        "header": _declaration_header(ddl_path, tables),
        "source": {
            "name": source_name,
            "display_name": display_name,
            "system": f"TODO: describe the {display_name} source system",
            "priority": priority,
        },
        "tables": decl_tables,
    }


def seed_declaration_from_ddl(
    ddl_path: Path,
    source_name: str | None = None,
    display_name: str | None = None,
    priority: int = 100,
) -> tuple[str, str]:
    """Pure function: parse the DDL, build the seeded declaration YAML text. Returns
    (source_name, yaml_text). Does not touch disk -- callers (main(), tests) decide
    where the text lands."""
    tables = parse_ddl(ddl_path.read_text(encoding="utf-8"))
    resolved_source = source_name or _slugify(ddl_path.stem)
    context = build_declaration_context(tables, ddl_path, resolved_source, display_name, priority)
    env = emit.template_env()
    return resolved_source, env.get_template(DECLARATION_TEMPLATE).render(**context)


# --- (b) model DDL -> domains/<name>.yml -------------------------------------------------

def _domain_header(ddl_path: Path, tables: list[ParsedTable]) -> str:
    table_names = ", ".join(t.name for t in tables)
    return "\n".join([
        f"# Seeded by ergasterion/import_ddl.py --mode model from DDL: {_relative_path(ddl_path)}",
        f"#   tables={table_names!r}",
        "#",
        "# This is a STARTING POINT, not regenerated output -- unlike ergasterion/emit.py's",
        "# outputs, this file is meant to be hand-edited. It mechanically derives",
        "# entity_configs / hub_configs / link_configs from the DDL's PRIMARY KEY / FOREIGN",
        "# KEY structure, following this repo's own golden_<entity>_key / <entity>_hk /",
        "# <entity>_hashdiff naming convention (uniform across every domains/*.yml here --",
        "# structural, not a business-semantics guess; confirm each against the domain's",
        "# actual natural-key columns before relying on it). What no DDL carries --",
        "# survivorship rules (bv_configs), entity-resolution match-key strategy",
        "# (res_configs), the map-lane relation vocabulary (relations), the ODCS",
        "# contract-adapter boundary (odcs) -- is left as an explicit TODO below and",
        "# never guessed. Run ergasterion/emit.py once those TODOs are filled in.",
    ])


def _is_pure_junction(table: ParsedTable) -> bool:
    """A table with >=2 foreign keys whose PRIMARY KEY is exactly the union of its local
    FK columns -- i.e. it carries no identity of its own beyond the relationships it
    joins. Mechanical PK/FK-structure test, never a name guess."""
    if len(table.foreign_keys) < 2:
        return False
    fk_local_cols: set[str] = set()
    for fk in table.foreign_keys:
        fk_local_cols.update(fk.columns)
    return bool(table.primary_key) and set(table.primary_key) == fk_local_cols


def build_domain_context(tables: list[ParsedTable], ddl_path: Path, domain_name: str) -> dict[str, Any]:
    table_by_name = {t.name: t for t in tables}
    entities: list[dict[str, Any]] = []
    links: list[dict[str, Any]] = []

    for table in tables:
        if _is_pure_junction(table):
            key_cols: set[str] = set(table.primary_key)
            for fk in table.foreign_keys:
                key_cols.update(fk.columns)
            extra_cols = [c for c in table.columns if c.name not in key_cols]
            link_name = table.name
            fk_hk_pairs = [(fk.ref_table, f"{fk.ref_table}_hk") for fk in table.foreign_keys]
            links.append({
                "name": link_name,
                "path": f"models/raw_vault/links/link_{link_name}.sql",
                "src_pk": f"{link_name}_lhk",
                "src_fk": [hk for _, hk in fk_hk_pairs],
            })
            if extra_cols:
                # Link-with-payload (satellite-on-link): register the junction table as
                # its OWN entity too, exactly domains/ecommerce.yml's order_line pattern.
                hashed_columns = [
                    {
                        "key": hk, "value": f"golden_{ref_table}_key", "is_composite": False,
                        "external": ref_table not in table_by_name,
                    }
                    for ref_table, hk in fk_hk_pairs
                ]
                hashed_columns.append({
                    "key": f"{link_name}_lhk",
                    "value_list": [f"golden_{ref_table}_key" for ref_table, _ in fk_hk_pairs],
                    "is_composite": True, "external": False,
                })
                entities.append({
                    "name": link_name,
                    "src_pk": f"{link_name}_lhk",
                    "hashdiff": f"{link_name}_hashdiff",
                    "payload": [c.name for c in table.columns],
                    "hashed_columns": hashed_columns,
                    "links": [link_name],
                    "is_link_entity": True,
                })
            continue

        # Plain entity (hub-worthy): every FOREIGN KEY becomes a link to the referenced
        # entity, mirroring domains/ecommerce.yml's order -> customer (order_customer) shape.
        src_pk = f"{table.name}_hk"
        hashed_columns = [
            {"key": src_pk, "value": f"golden_{table.name}_key", "is_composite": False, "external": False},
        ]
        entity_links: list[str] = []
        for fk in table.foreign_keys:
            ref_table = fk.ref_table
            ref_hk = f"{ref_table}_hk"
            hashed_columns.append({
                "key": ref_hk, "value": f"golden_{ref_table}_key", "is_composite": False,
                "external": ref_table not in table_by_name,
            })
            link_name = f"{table.name}_{ref_table}"
            lhk_name = f"{link_name}_lhk"
            hashed_columns.append({
                "key": lhk_name,
                "value_list": [f"golden_{table.name}_key", f"golden_{ref_table}_key"],
                "is_composite": True, "external": False,
            })
            entity_links.append(link_name)
            links.append({
                "name": link_name,
                "path": f"models/raw_vault/links/link_{link_name}.sql",
                "src_pk": lhk_name,
                "src_fk": [src_pk, ref_hk],
            })
        entities.append({
            "name": table.name,
            "src_pk": src_pk,
            "hashdiff": f"{table.name}_hashdiff",
            "payload": [c.name for c in table.columns],
            "hashed_columns": hashed_columns,
            "links": entity_links,
            "is_link_entity": False,
        })

    hub_entities = [e for e in entities if not e["is_link_entity"]]
    return {
        "header": _domain_header(ddl_path, tables),
        "entities": entities,
        "hub_entities": hub_entities,
        "links": links,
    }


def seed_domain_from_ddl(ddl_path: Path, domain_name: str | None = None) -> tuple[str, str]:
    """Pure function: parse the DDL, build the seeded domain YAML text. Returns
    (domain_name, yaml_text). Does not touch disk -- callers (main(), tests) decide
    where the text lands."""
    tables = parse_ddl(ddl_path.read_text(encoding="utf-8"))
    resolved_domain = domain_name or _slugify(ddl_path.stem)
    context = build_domain_context(tables, ddl_path, resolved_domain)
    env = emit.template_env()
    return resolved_domain, env.get_template(DOMAIN_TEMPLATE).render(**context)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("ddl", type=Path, help="Path to the CREATE TABLE DDL statement set.")
    parser.add_argument(
        "--mode", choices=("feed", "model"), required=True,
        help="feed: seed declarations/<source>.yml from source-system DDL. "
             "model: seed domains/<name>.yml from PK/FK-carrying model DDL.",
    )
    parser.add_argument("--source", default=None, help="[feed mode] Declaration source name. Defaults to a slug of the DDL filename.")
    parser.add_argument("--display-name", default=None, help="[feed mode] Source display_name. Defaults to SOURCE.upper().")
    parser.add_argument(
        "--priority", type=int, default=100,
        help="[feed mode] Initial source.priority (default 100). No DDL carries survivorship "
             "intent -- confirm this against the domain's other sources before relying on it.",
    )
    parser.add_argument("--domain", default=None, help="[model mode] Domain name. Defaults to a slug of the DDL filename.")
    parser.add_argument("--out", type=Path, default=None, help="Output path. Defaults to declarations/<source>.yml or domains/<domain>.yml.")
    parser.add_argument("--force", action="store_true", help="Overwrite an existing destination file.")
    parser.add_argument("--estate-root", type=Path, default=None, help="Estate root whose declarations/domains receives the seed (resolved from the environment or working directory when omitted).")
    args = parser.parse_args()

    ctx = EstateContext.resolve(estate_root=args.estate_root)

    try:
        if args.mode == "feed":
            name, text = seed_declaration_from_ddl(args.ddl, args.source, args.display_name, args.priority)
            out_path = args.out or (ctx.declarations_dir / f"{name}.yml")
            next_hint = "vault_entities / entity_resolution"
        else:
            name, text = seed_domain_from_ddl(args.ddl, args.domain)
            out_path = args.out or (ctx.domains_dir / f"{name}.yml")
            next_hint = "bv_configs / res_configs / relations / odcs"
    except DdlImportError as exc:
        print(f"FAIL: {exc}", file=sys.stderr)
        return 1

    if out_path.exists() and not args.force:
        print(f"FAIL: {out_path} already exists -- pass --force to overwrite (never merges with hand edits).", file=sys.stderr)
        return 1

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(text, encoding="utf-8")
    rel = out_path.relative_to(ctx.root).as_posix() if out_path.is_relative_to(ctx.root) else out_path
    print(f"seeded {rel}")
    print(f"Next: fill in the {next_hint} TODOs, then run ergasterion/emit.py.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
