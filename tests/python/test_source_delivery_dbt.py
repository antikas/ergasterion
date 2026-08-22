"""dbt integrity, schedule-timeliness and optional maximum-age freshness.

Exercises macros/source_delivery.sql against the stock dbt Core 1.11.12
collect_freshness_custom_sql call path. DuckDB executes pass/stale freshness,
signal separation and fail-closed projection diagnostics. Snowflake, BigQuery
and DuckDB parse. --write-project DIR materialises the isolated fixture used by
scripts/validate_snowflake_delivery_freshness.sh.

Usage:
    python tests/python/test_source_delivery_dbt.py
    python tests/python/test_source_delivery_dbt.py --write-project DIR
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import traceback
from datetime import datetime, timedelta, timezone
from pathlib import Path

import duckdb
import yaml

if __package__ in (None, ""):
    import os as _os

    sys.path.insert(0, _os.path.dirname(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))))

from ergasterion.sync_scaffold import SCAFFOLD_MACROS, generated_set

REPO_ROOT = Path(__file__).resolve().parents[2]
FIXTURE = REPO_ROOT / "tests" / "fixtures" / "snowflake_delivery_freshness"
CASES_PATH = FIXTURE / "cases.json"
MACRO_SRC = REPO_ROOT / "macros" / "source_delivery.sql"
STOCK_FRESHNESS = (
    Path(sys.prefix)
    / "Lib"
    / "site-packages"
    / "dbt"
    / "include"
    / "global_project"
    / "macros"
    / "adapters"
    / "freshness.sql"
)
if not STOCK_FRESHNESS.is_file():
    STOCK_FRESHNESS = (
        REPO_ROOT
        / ".venv"
        / "Lib"
        / "site-packages"
        / "dbt"
        / "include"
        / "global_project"
        / "macros"
        / "adapters"
        / "freshness.sql"
    )


def _fail(msg: str, proc: subprocess.CompletedProcess | None = None) -> int:
    sys.stderr.write(f"FAIL (source-delivery dbt): {msg}\n")
    if proc is not None:
        sys.stderr.write(f"  command : {proc.args}\n")
        sys.stderr.write(f"  exitcode: {proc.returncode}\n")
        if proc.stdout:
            sys.stderr.write("  --- stdout ---\n" + proc.stdout + "\n")
        if proc.stderr:
            sys.stderr.write("  --- stderr ---\n" + proc.stderr + "\n")
    return 1


def _dbt_exe() -> str:
    scripts = REPO_ROOT / ".venv" / "Scripts" / "dbt.exe"
    if scripts.is_file():
        return str(scripts)
    posix = REPO_ROOT / ".venv" / "bin" / "dbt"
    if posix.is_file():
        return str(posix)
    found = shutil.which("dbt")
    if not found:
        raise FileNotFoundError("dbt executable not found")
    return found


def _load_cases() -> dict:
    return json.loads(CASES_PATH.read_text(encoding="utf-8"))


def identity_key(source: str, table: str, namespace: str) -> str:
    return json.dumps(
        {"estate_namespace": namespace, "source": source, "table": table},
        sort_keys=True,
        separators=(",", ":"),
    )


def digest_for(case_id: str, *, wrong: bool = False) -> str:
    payload = f"{'wrong' if wrong else 'digest'}:{case_id}".encode()
    return hashlib.sha256(payload).hexdigest()


def utc_instant(now: datetime, *, minutes: int = 0, seconds: int = 0) -> str:
    instant = now + timedelta(minutes=minutes, seconds=seconds)
    return instant.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _sql_quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _stream_row_sql(*, key: str, digest: str, projection_target: str, revision: int, payload: str) -> str:
    return (
        "select {key} as identity_key, {digest} as contract_digest, "
        "{target} as projection_target, {rev} as projection_revision, "
        "{payload} as payload".format(
            key=_sql_quote(key),
            digest=_sql_quote(digest),
            target=_sql_quote(projection_target),
            rev=int(revision),
            payload=_sql_quote(payload),
        )
    )


def write_project(dest: Path, *, now: datetime | None = None) -> dict:
    """Materialise an isolated dbt project that only contains synthetic relations."""
    spec = _load_cases()
    now = now or datetime.now(timezone.utc)
    dest = dest.resolve()
    if dest.exists():
        for child in dest.iterdir():
            if child.name in {".git", ".venv"}:
                continue
            if child.is_dir():
                shutil.rmtree(child)
            else:
                child.unlink()
    (dest / "macros").mkdir(parents=True, exist_ok=True)
    (dest / "models").mkdir(parents=True, exist_ok=True)
    shutil.copyfile(FIXTURE / "dbt_project.yml", dest / "dbt_project.yml")
    shutil.copyfile(MACRO_SRC, dest / "macros" / "source_delivery.sql")

    namespace = spec["estate_namespace"]
    table = spec["table"]
    target = spec["projection_target"]
    age = spec["maximum_age"]
    sources: list[dict] = []
    projection_groups: list[dict] = []
    stream_rows: list[str] = []

    landing_sql = (
        "{{ config(materialized='table') }}\n"
        "select cast(null as {{ dbt.type_string() }}) as dummy\n"
        "where 1 = 0\n"
    )
    empty_ledger = (
        "{{ config(materialized='table') }}\n"
        "select\n"
        "    cast(null as {{ dbt.type_string() }}) as identity_key,\n"
        "    cast(null as {{ dbt.type_string() }}) as visibility_epoch,\n"
        "    cast(null as {{ dbt.type_string() }}) as visibility_kind,\n"
        "    cast(null as {{ dbt.type_string() }}) as visibility_id,\n"
        "    cast(null as {{ dbt.type_string() }}) as projection_target,\n"
        "    cast(null as {{ dbt.type_string() }}) as payload_digest,\n"
        "    cast(null as {{ dbt.type_string() }}) as claim_digest,\n"
        "    cast(null as {{ dbt.type_string() }}) as json\n"
        "where 1 = 0\n"
    )
    empty_alias = (
        "{{ config(materialized='table') }}\n"
        "select\n"
        "    cast(null as {{ dbt.type_string() }}) as identity_key,\n"
        "    cast(null as {{ dbt.type_string() }}) as projection_target,\n"
        "    cast(null as {{ dbt.type_string() }}) as relation_ref,\n"
        "    cast(null as {{ dbt.type_string() }}) as product_version,\n"
        "    cast(null as {{ dbt.type_string() }}) as contract_digest\n"
        "where 1 = 0\n"
    )

    for case in spec["cases"]:
        case_id = case["id"]
        active_digest = digest_for(case_id)
        row_digest = digest_for(case_id, wrong=case.get("row_digest") == "wrong")
        key = identity_key(case_id, table, namespace)
        identity_meta = {
            "contract_digest": active_digest,
            "domain": "operations",
            "estate_namespace": namespace,
            "execution_plan_digest": digest_for("plan"),
            "product_version": "1.0.0",
            "published_schema_digest": digest_for("published"),
            "source": case_id,
            "source_schema_digest": digest_for("source"),
            "table": table,
        }
        tests = [
            {
                "dpf_projection_integrity": {
                    "identity_key": key,
                    "projection_target": target,
                }
            },
            {
                "dpf_schedule_timeliness": {
                    "identity_key": key,
                    "projection_target": target,
                }
            },
        ]
        table_cfg: dict = {
            "name": table,
            "identifier": "dpf_synth_landing",
            "meta": {"dpf.identity": identity_meta},
            "data_tests": tests,
        }
        if case.get("native_freshness"):
            table_cfg["config"] = {
                "freshness": {
                    "warn_after": {"count": age["warn_after_minutes"], "period": "minute"},
                    "error_after": {"count": age["error_after_minutes"], "period": "minute"},
                },
                "loaded_at_query": "{{ dpf_source_freshness_query() }}",
            }
        sources.append({"name": case_id, "schema": "{{ target.schema }}", "tables": [table_cfg]})
        projection_groups.append(
            {
                "name": f"bronze_{case_id}_{table}",
                "schema": "{{ target.schema }}",
                "tables": [
                    {"name": "published_ledger", "identifier": "dpf_synth_published_ledger"},
                    {"name": "stream_status", "identifier": "dpf_synth_stream_status"},
                    {"name": "active_alias", "identifier": "dpf_synth_active_alias"},
                ],
            }
        )
        def append_stream_rows(count: int, digest: str, row_target: str) -> None:
            for index in range(int(count)):
                status = {
                    "accepted_progress": {},
                    "committed_at": (
                        None
                        if case.get("committed_at", "present") is None
                        else utc_instant(now, minutes=int(case.get("committed_offset_minutes", -20)))
                    ),
                    "contract_digest": digest,
                    "heartbeat_at": utc_instant(now, seconds=int(case.get("heartbeat_offset_seconds", 3600))),
                    "logical_identity": {
                        "estate_namespace": namespace,
                        "source": case_id,
                        "table": table,
                    },
                    "processing": "committed",
                    "projected_at": utc_instant(now, seconds=-5),
                    "projection_revision": str(int(case.get("projection_revision", 1)) + index),
                    "projection_target": row_target,
                    "snapshot_reconciliation": "not_applicable",
                    "timeliness": case.get("timeliness", "on_time"),
                    "evaluated_through_at": utc_instant(now, seconds=-5),
                }
                stream_rows.append(
                    _stream_row_sql(
                        key=key,
                        digest=digest,
                        projection_target=row_target,
                        revision=int(status["projection_revision"]),
                        payload=json.dumps(status, sort_keys=True, separators=(",", ":")),
                    )
                )

        append_stream_rows(int(case.get("copies", 0)), row_digest, case.get("row_projection_target", target))
        append_stream_rows(int(case.get("prior_digest_copies", 0)), digest_for(case_id, wrong=True), target)
        append_stream_rows(
            int(case.get("extra_target_copies", 0)),
            digest_for(case_id, wrong=case.get("extra_row_digest") == "wrong"),
            case.get("extra_projection_target", "other_target"),
        )

    if not stream_rows:
        stream_rows.append(
            "select cast(null as {{ dbt.type_string() }}) as identity_key, "
            "cast(null as {{ dbt.type_string() }}) as contract_digest, "
            "cast(null as {{ dbt.type_string() }}) as projection_target, "
            "cast(null as bigint) as projection_revision, "
            "cast(null as {{ dbt.type_string() }}) as payload where 1 = 0"
        )

    stream_sql = (
        "{{ config(materialized='table') }}\n"
        "select identity_key, contract_digest, projection_target, projection_revision, "
        "payload as {{ adapter.quote('json') }}\n"
        "from (\n"
        + "\nunion all\n".join(stream_rows)
        + "\n) as synthetic_rows\n"
    )
    (dest / "models" / "dpf_synth_landing.sql").write_text(landing_sql, encoding="utf-8", newline="\n")
    (dest / "models" / "dpf_synth_stream_status.sql").write_text(stream_sql, encoding="utf-8", newline="\n")
    (dest / "models" / "dpf_synth_published_ledger.sql").write_text(empty_ledger, encoding="utf-8", newline="\n")
    (dest / "models" / "dpf_synth_active_alias.sql").write_text(empty_alias, encoding="utf-8", newline="\n")

    doc = {
        "version": 2,
        "sources": sources + projection_groups,
    }
    (dest / "models" / "sources.yml").write_text(
        yaml.safe_dump(doc, sort_keys=False, width=120),
        encoding="utf-8",
        newline="\n",
    )
    spec["_written_at"] = utc_instant(now)
    spec["_dest"] = str(dest)
    return spec


_ANSI = re.compile(r"\x1b\[[0-9;]*m")


def _compiled_sql_path(output: str, dest: Path) -> Path | None:
    clean = _ANSI.sub("", output)
    for line in clean.splitlines():
        marker = "compiled code at"
        idx = line.lower().find(marker)
        if idx == -1:
            continue
        raw = line[idx + len(marker) :].strip()
        path = Path(raw)
        if not path.is_absolute():
            path = dest / path
        return path
    return None


def _query_test_reasons(dest: Path, duckdb_path: Path, output: str) -> list[str]:
    compiled = _compiled_sql_path(output, dest)
    if compiled is None or not compiled.is_file():
        raise AssertionError(f"could not locate compiled test SQL\n{output}")
    sql = compiled.read_text(encoding="utf-8")

    def _run(path: Path) -> list[str]:
        con = duckdb.connect(str(path), read_only=True)
        try:
            return [str(row[0]) for row in con.execute(sql).fetchall()]
        finally:
            con.close()

    try:
        return _run(duckdb_path)
    except duckdb.IOException:
        copied = duckdb_path.with_suffix(".readonly.copy.duckdb")
        shutil.copy2(duckdb_path, copied)
        wal = Path(str(duckdb_path) + ".wal")
        if wal.is_file():
            shutil.copy2(wal, Path(str(copied) + ".wal"))
        return _run(copied)


def _run_dbt(args: list[str], cwd: Path, env: dict, timeout: int = 180) -> subprocess.CompletedProcess:
    return subprocess.run(
        [_dbt_exe(), *args],
        cwd=str(cwd),
        env=env,
        capture_output=True,
        text=True,
        timeout=timeout,
    )


def _dbt_env(duckdb_path: Path) -> dict:
    env = os.environ.copy()
    env["DPF_DUCKDB_PATH"] = str(duckdb_path)
    return env


def test_stock_call_path_is_collect_freshness_custom_sql() -> None:
    stock = STOCK_FRESHNESS.read_text(encoding="utf-8")
    assert "{% macro collect_freshness_custom_sql" in stock, stock
    assert "{% macro default__collect_freshness_custom_sql" in stock, stock
    assert "with source_query as (" in stock, stock
    project = MACRO_SRC.read_text(encoding="utf-8")
    assert "{% macro collect_freshness" not in project
    assert "{% macro default__collect_freshness" not in project
    assert "{% macro collect_freshness_custom_sql" not in project
    assert "adapter.dispatch('collect_freshness'" not in project


def test_fault_sql_scopes_active_digest_and_target() -> None:
    sql = MACRO_SRC.read_text(encoding="utf-8")
    assert "when match_count > 1 then 'duplicate_projection'" not in sql
    assert "when expected_count > 1 then 'duplicate_projection'" in sql
    assert "when expected_count = 0 and match_count = 0 then 'missing_projection'" in sql
    assert "dpf_configured_projection_target" in sql
    assert "{%- if projection_target is not none and projection_target | length > 0 %}" not in sql
    assert "and stream_row.projection_target = '{{ target }}'" in sql
    assert "{% macro collect_freshness" not in sql


def test_scaffold_sync_includes_source_delivery() -> None:
    assert "source_delivery.sql" in SCAFFOLD_MACROS
    generated = generated_set()
    rel = Path("macros") / "source_delivery.sql"
    assert rel in generated, sorted(str(path) for path in generated)
    assert generated[rel] == MACRO_SRC.read_bytes()
    scaffold = REPO_ROOT / "ergasterion" / "scaffold" / "macros" / "source_delivery.sql"
    assert scaffold.is_file(), "scaffold copy must exist"
    assert scaffold.read_bytes() == MACRO_SRC.read_bytes()
    proc = subprocess.run(
        [sys.executable, str(REPO_ROOT / "ergasterion" / "sync_scaffold.py"), "--check"],
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 0, proc.stdout + proc.stderr


def test_duckdb_freshness_integrity_and_parse() -> None:
    spec = _load_cases()
    with tempfile.TemporaryDirectory(prefix="dpf-source-delivery-dbt-") as tmp:
        dest = Path(tmp) / "project"
        write_project(dest)
        duckdb_path = dest / "target" / "source_delivery.duckdb"
        duckdb_path.parent.mkdir(parents=True, exist_ok=True)
        env = _dbt_env(duckdb_path)
        profiles = str(REPO_ROOT / "profiles")

        sources_yml = yaml.safe_load((dest / "models" / "sources.yml").read_text(encoding="utf-8"))
        by_name = {group["name"]: group for group in sources_yml["sources"]}
        assert "config" not in by_name["sched_only"]["tables"][0], by_name["sched_only"]
        assert "freshness" not in (by_name["sched_only"]["tables"][0].get("config") or {})
        assert (by_name["fresh_ok"]["tables"][0].get("config") or {}).get("loaded_at_query") == (
            "{{ dpf_source_freshness_query() }}"
        )
        assert "loaded_at_field" not in json.dumps(by_name["fresh_ok"])
        assert "filter" not in json.dumps((by_name["fresh_ok"]["tables"][0].get("config") or {}).get("freshness") or {})

        parsed = _run_dbt(
            ["parse", "--profiles-dir", profiles, "--no-partial-parse", "-t", "duckdb"],
            dest,
            env,
        )
        if parsed.returncode != 0:
            raise AssertionError(_fail("dbt parse -t duckdb failed", parsed) or parsed.stderr)

        built = _run_dbt(
            [
                "run",
                "--profiles-dir",
                profiles,
                "--no-partial-parse",
                "-t",
                "duckdb",
                "--select",
                "dpf_synth_landing",
                "dpf_synth_stream_status",
                "dpf_synth_published_ledger",
                "dpf_synth_active_alias",
            ],
            dest,
            env,
        )
        if built.returncode != 0:
            raise AssertionError(_fail("dbt run of synthetic relations failed", built) or built.stderr)

        for case in spec["cases"]:
            source_sel = f"source:{case['id']}.{spec['table']}"
            integrity = _run_dbt(
                [
                    "test",
                    "--profiles-dir",
                    profiles,
                    "-t",
                    "duckdb",
                    "--select",
                    f"{source_sel},test_name:dpf_projection_integrity",
                ],
                dest,
                env,
            )
            integrity_out = integrity.stdout + integrity.stderr
            if case["expect_integrity"] == "pass":
                if integrity.returncode != 0:
                    raise AssertionError(_fail(f"{case['id']} integrity should pass", integrity) or integrity_out)
            else:
                if integrity.returncode == 0:
                    raise AssertionError(_fail(f"{case['id']} integrity should fail closed", integrity) or integrity_out)
                reasons = _query_test_reasons(dest, duckdb_path, integrity_out)
                assert reasons == [case["expect_integrity_reason"]], reasons

            schedule = _run_dbt(
                [
                    "test",
                    "--profiles-dir",
                    profiles,
                    "-t",
                    "duckdb",
                    "--select",
                    f"{source_sel},test_name:dpf_schedule_timeliness",
                ],
                dest,
                env,
            )
            schedule_out = schedule.stdout + schedule.stderr
            if case["expect_schedule"] == "pass":
                if schedule.returncode != 0:
                    raise AssertionError(_fail(f"{case['id']} schedule should pass", schedule) or schedule_out)
            else:
                if schedule.returncode == 0:
                    raise AssertionError(_fail(f"{case['id']} schedule should fail", schedule) or schedule_out)
                reasons = _query_test_reasons(dest, duckdb_path, schedule_out)
                assert reasons == [case.get("expect_schedule_reason", "late")], reasons

            if not case.get("native_freshness"):
                continue
            freshness = _run_dbt(
                [
                    "--debug",
                    "source",
                    "freshness",
                    "--profiles-dir",
                    profiles,
                    "-t",
                    "duckdb",
                    "--select",
                    source_sel,
                ],
                dest,
                env,
            )
            fresh_out = freshness.stdout + freshness.stderr
            if case["id"] == "fresh_ok":
                assert "with source_query as" in fresh_out, fresh_out
                assert "(select * from source_query)" in fresh_out, fresh_out
                assert f"projection_target = '{spec['projection_target']}'" in fresh_out, fresh_out
            expect = case["expect_freshness"]
            if expect == "pass":
                assert freshness.returncode == 0, fresh_out
                assert "ERROR" not in fresh_out.split("Freshness of")[-1] if "Freshness of" in fresh_out else True
            elif expect == "error":
                assert freshness.returncode != 0, fresh_out
            elif expect == "runtime_error":
                assert freshness.returncode != 0, fresh_out

        # Signal separation: schedule-late / native-fresh is a pass on freshness and a
        # fail on schedule-timeliness in the same projection row.
        late = next(case for case in spec["cases"] if case["id"] == "sched_late")
        assert late["expect_freshness"] == "pass"
        assert late["expect_schedule"] == "fail"
        assert late["expect_integrity"] == "pass"

        prior = next(case for case in spec["cases"] if case["id"] == "prior_digest_ok")
        assert prior["prior_digest_copies"] == 1
        assert prior["expect_integrity"] == "pass"
        extra_target = next(case for case in spec["cases"] if case["id"] == "fresh_extra_target_ok")
        assert extra_target["extra_target_copies"] == 1
        assert extra_target["expect_integrity"] == "pass"
        assert extra_target["expect_freshness"] == "pass"
        wrong_target = next(case for case in spec["cases"] if case["id"] == "fault_wrong_target")
        assert wrong_target["row_projection_target"] == "other_target"
        assert wrong_target["expect_integrity_reason"] == "missing_projection"
        assert wrong_target["expect_freshness"] == "runtime_error"

        for target in ("snowflake", "bigquery"):
            parsed = _run_dbt(
                ["parse", "--profiles-dir", profiles, "--no-partial-parse", "-t", target],
                dest,
                env,
            )
            if parsed.returncode != 0:
                raise AssertionError(_fail(f"dbt parse -t {target} failed", parsed) or parsed.stderr)


TESTS = [
    test_stock_call_path_is_collect_freshness_custom_sql,
    test_scaffold_sync_includes_source_delivery,
    test_fault_sql_scopes_active_digest_and_target,
    test_duckdb_freshness_integrity_and_parse,
]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--write-project", metavar="DIR", help="Materialise the isolated fixture project and exit.")
    args = parser.parse_args(argv)
    if args.write_project:
        dest = Path(args.write_project)
        dest.mkdir(parents=True, exist_ok=True)
        write_project(dest)
        print(f"wrote isolated source-delivery dbt project to {dest}")
        return 0

    failures = 0
    for test in TESTS:
        name = test.__name__
        try:
            test()
        except Exception:  # noqa: BLE001
            failures += 1
            print(f"FAIL {name}")
            traceback.print_exc()
        else:
            print(f"PASS {name}")
    total = len(TESTS)
    print(f"{total - failures}/{total} passed")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
