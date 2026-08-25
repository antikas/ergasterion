"""The scratch estate every machine-checked demo scenario runs against.

A scenario copies the committed estate into a fresh directory under
``demo/offline-runs/``, regenerates it with the factory, builds it on DuckDB, changes
one declared fact, rebuilds, and checks the outcome by machine. This module is the
single home of the parts every scenario shares: the copy, the factory and dbt runner,
the readers that answer questions about the built warehouse, and the entry point that
removes the scratch estate before the scenario returns.

Terms used here as the factory and its errors use them:

  identity digest    one digest over every stored row identity of a satellite, where a
                     row identity is (business key, hashdiff, effective time).
  object identity    the catalog entry of a relation. A comment belongs to the object,
                     so a comment written before a change and read back after it says
                     whether the same object survived.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

import duckdb
import yaml

# What the copied estate needs to regenerate and build on its own.
ESTATE_COPY_DIRS = (
    "models",
    "seeds",
    "macros",
    "declarations",
    "domains",
    "profiles",
    "contracts",
    "dbt_packages",
)
ESTATE_COPY_FILES = ("dbt_project.yml", "packages.yml", "package-lock.yml", "estate.yml")


class ScenarioFailure(Exception):
    """A machine check that did not hold."""


def check(condition: object, message: str) -> None:
    if not condition:
        raise ScenarioFailure(message)


def step(title: str) -> None:
    print(f"\n-- {title} --", flush=True)


def tail(output: str, lines: int = 30) -> str:
    return "\n".join(output.splitlines()[-lines:])


# ---------------------------------------------------------------------------
# The scratch estate
# ---------------------------------------------------------------------------


def materialise_estate(
    repo_root: Path, offline_runs: Path, prefix: str
) -> tuple[Path, Path]:
    """Copy the estate into a fresh scratch directory under demo/offline-runs/."""
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    holder = Path(tempfile.mkdtemp(prefix=f"{prefix}-{run_id}-", dir=str(offline_runs)))
    root = holder / "estate"
    root.mkdir()
    for name in ESTATE_COPY_DIRS:
        shutil.copytree(repo_root / name, root / name)
    for name in ESTATE_COPY_FILES:
        shutil.copy2(repo_root / name, root / name)
    # The estate's singular test assertions, which the emitter reads by path. The Python
    # tests and their fixtures beside them stay out of the copy.
    (root / "tests").mkdir()
    for assertion in sorted((repo_root / "tests").glob("*.sql")):
        shutil.copy2(assertion, root / "tests" / assertion.name)
    print(f"scratch estate  : {root}")
    return holder, root


# ---------------------------------------------------------------------------
# Running the factory and dbt against the scratch estate
# ---------------------------------------------------------------------------


class Runner:
    def __init__(self, repo_root: Path, root: Path, database: Path, dbt_bin: str) -> None:
        self.repo_root = repo_root
        self.root = root
        self.database = database
        self.dbt_bin = dbt_bin
        self.log_dir = root / "logs"
        self.dbt_commands: list[list[str]] = []

    def _env(self) -> dict:
        environment = dict(os.environ)
        environment["DPF_DUCKDB_PATH"] = str(self.database)
        return environment

    def emit(self, *extra: str) -> str:
        command = [
            sys.executable,
            "-m",
            "ergasterion",
            "emit",
            "--estate-root",
            str(self.root),
            *extra,
        ]
        completed = subprocess.run(
            command,
            cwd=str(self.repo_root),
            env=self._env(),
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        output = f"{completed.stdout}\n{completed.stderr}"
        check(
            completed.returncode == 0,
            "ergasterion emit exited "
            f"{completed.returncode} on the scratch estate:\n" + tail(output),
        )
        return output

    def dbt(self, *arguments: str) -> str:
        command = [
            self.dbt_bin,
            "--no-use-colors",
            "--log-path",
            str(self.log_dir),
            *arguments,
            "--project-dir",
            str(self.root),
            "--profiles-dir",
            str(self.root / "profiles"),
            "-t",
            "duckdb",
        ]
        self.dbt_commands.append(list(arguments))
        print(f"   dbt {' '.join(arguments)}", flush=True)
        completed = subprocess.run(
            command,
            cwd=str(self.root),
            env=self._env(),
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        output = f"{completed.stdout}\n{completed.stderr}"
        check(
            completed.returncode == 0,
            f"dbt {' '.join(arguments)} exited {completed.returncode}:\n" + tail(output),
        )
        return output

    def reset_dbt_log(self) -> None:
        """Start a fresh dbt debug log, so the next invocation owns every line in it."""
        if self.log_dir.exists():
            shutil.rmtree(self.log_dir, ignore_errors=True)

    def dbt_log_text(self) -> str:
        log_file = self.log_dir / "dbt.log"
        check(log_file.exists(), f"dbt wrote no debug log at {log_file}")
        return log_file.read_text(encoding="utf-8", errors="replace")

    def invalidate_parse_cache(self) -> None:
        """Drop the cached manifest so dbt rereads a changed project configuration."""
        cache = self.root / "target" / "partial_parse.msgpack"
        if cache.exists():
            cache.unlink()


# ---------------------------------------------------------------------------
# Reading the warehouse
# ---------------------------------------------------------------------------


def locate(connection: duckdb.DuckDBPyConnection, table_name: str) -> str | None:
    """The schema holding a table, or None when the table does not exist."""
    row = connection.execute(
        "select table_schema from information_schema.tables where table_name = ?",
        [table_name],
    ).fetchone()
    return None if row is None else row[0]


def relation_sql(schema: str, table_name: str) -> str:
    return f'"{schema}"."{table_name}"'


def relation_columns(
    connection: duckdb.DuckDBPyConnection, schema: str, table_name: str
) -> list[str]:
    return [
        row[0]
        for row in connection.execute(
            "select column_name from information_schema.columns "
            "where table_schema = ? and table_name = ? order by ordinal_position",
            [schema, table_name],
        ).fetchall()
    ]


def identity_digest(identities: object) -> str:
    """One digest over a collection of row identities, in a stable order."""
    payload = json.dumps(sorted([str(part) for part in identity] for identity in identities))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def relation_digest(database: Path, table_name: str) -> str:
    """One digest over every stored byte of a relation, read in a stable order.

    A relation that does not exist digests to a named constant, so a scenario comparing
    two digests never reads a missing relation as an unchanged one.
    """
    connection = duckdb.connect(str(database), read_only=True)
    try:
        schema = locate(connection, table_name)
        if schema is None:
            return "absent"
        columns = relation_columns(connection, schema, table_name)
        projection = ", ".join(f'cast("{column}" as varchar)' for column in columns)
        rows = connection.execute(
            f"select {projection} from {relation_sql(schema, table_name)}"
        ).fetchall()
        payload = json.dumps(
            {
                "columns": columns,
                "rows": sorted(
                    ["" if value is None else str(value) for value in row] for row in rows
                ),
            },
            sort_keys=True,
        )
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()
    finally:
        connection.close()


def stamp_object_identity(database: Path, relations: dict, token: str) -> None:
    """Attach a token to each named relation's catalog entry.

    A comment belongs to the relation object. Altering the relation keeps it; recreating
    the relation destroys it. Reading it back after a change is a direct check that the
    same object survived.
    """
    connection = duckdb.connect(str(database))
    try:
        for name, schema in relations.items():
            connection.execute(
                f"comment on table {relation_sql(schema, name)} is '{token}'"
            )
    finally:
        connection.close()


def object_identity_tokens(database: Path, relations: dict) -> dict:
    connection = duckdb.connect(str(database), read_only=True)
    try:
        tokens: dict = {}
        for name, schema in relations.items():
            row = connection.execute(
                "select comment from duckdb_tables() "
                "where schema_name = ? and table_name = ?",
                [schema, name],
            ).fetchone()
            tokens[name] = None if row is None else row[0]
        return tokens
    finally:
        connection.close()


def history_bearing_relations(root: Path, connection: duckdb.DuckDBPyConnection) -> dict:
    """The relations that store history, read from dbt's own manifest.

    A relation stores history when its materialisation is incremental: the hubs, the
    links, the satellites, and any incremental staging relation. Stage and bridge
    relations are recomputed tables by materialisation contract and stay outside this
    set. Only relations that the build actually created are returned.
    """
    manifest_path = root / "target" / "manifest.json"
    check(manifest_path.exists(), f"dbt wrote no manifest at {manifest_path}")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    found: dict = {}
    for node in manifest["nodes"].values():
        if node.get("resource_type") != "model":
            continue
        if node.get("config", {}).get("materialized") != "incremental":
            continue
        name = node.get("alias") or node["name"]
        located = locate(connection, name)
        if located is not None:
            found[name] = located
    check(found, "the build created no history-bearing relation")
    return found


def surviving_relations(database: Path, relations: dict) -> list[str]:
    connection = duckdb.connect(str(database), read_only=True)
    try:
        return [name for name in relations if locate(connection, name) is not None]
    finally:
        connection.close()


# ---------------------------------------------------------------------------
# Declaration files
# ---------------------------------------------------------------------------


def read_yaml(path: Path) -> dict:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def write_yaml(path: Path, document: dict) -> None:
    path.write_text(yaml.safe_dump(document, sort_keys=False), encoding="utf-8")


# ---------------------------------------------------------------------------
# The lane entry point
# ---------------------------------------------------------------------------


def run_lane(
    description: str,
    prefix: str,
    scenario: Callable[[Path, Path, str], None],
    argv: list[str] | None = None,
) -> int:
    """Parse the lane arguments, materialise the scratch estate, run the scenario, and
    remove the scratch estate whatever the outcome."""
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--repo-root", type=Path, required=True)
    parser.add_argument("--offline-runs", type=Path, required=True)
    arguments = parser.parse_args(argv)

    try:
        repo_root = arguments.repo_root.resolve(strict=True)
        offline_runs = arguments.offline_runs.resolve(strict=True)
        check(
            offline_runs == (repo_root / "demo" / "offline-runs").resolve(),
            f"the scratch estate must live under demo/offline-runs/, not {offline_runs}",
        )
        dbt_bin = os.environ.get("DPF_DBT_BIN")
        check(bool(dbt_bin), "DPF_DBT_BIN names the dbt executable and is unset")
    except ScenarioFailure as failure:
        print(f"\nSCENARIO CHECK FAILED: {failure}", file=sys.stderr)
        return 1

    holder, root = materialise_estate(repo_root, offline_runs, prefix)
    try:
        scenario(repo_root, root, str(dbt_bin))
    except ScenarioFailure as failure:
        print(f"\nSCENARIO CHECK FAILED: {failure}", file=sys.stderr)
        return 1
    finally:
        shutil.rmtree(holder, ignore_errors=True)
        print(f"removed scratch estate: {holder}")
    return 0
