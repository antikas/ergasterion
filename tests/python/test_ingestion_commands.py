"""Assert-script tests for the local-ingestion translator and operator commands.

Usage:
    python tests/python/test_ingestion_commands.py
"""

from __future__ import annotations

import io
import json
import shutil
import sys
import tempfile
import traceback
from contextlib import redirect_stderr, redirect_stdout
from datetime import datetime, timedelta, timezone
from pathlib import Path

if __package__ in (None, ""):
    import os as _os
    import sys as _sys
    _sys.path.insert(0, _os.path.dirname(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))))

import yaml

from ergasterion.cli import main as cli_main
from ergasterion.estate import EstateContext
from ergasterion.framework.bronze_contract import BronzeProductContract
from ergasterion.framework.models import Layer, compute_plan_digest
from ergasterion.framework.resolver import resolve
from ergasterion.framework.runtime_binding import RuntimeBinding
from ergasterion.framework.translator_conformance import check_translator_conformance
from ergasterion.ingestion.codecs import transport_payload_fingerprint
from ergasterion.ingestion.reference_runtime import (
    LIFECYCLE_ORDINAL_FILENAME,
    set_clock,
    set_projection_faults,
)
from ergasterion.ingestion.runtime import Clock, canonical_digest
from ergasterion.ingestion.settings import SettingsError, reject_store_relocation
from ergasterion.source_delivery import TypedDeclarations, load_typed_declarations
from ergasterion.translators.dbt import DbtTranslator
from ergasterion.translators.local_ingestion import (
    LocalIngestionTranslator,
    build_local_binding,
    compile_runtime_manifest,
)

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
VECTORS = REPO_ROOT / "tests" / "fixtures" / "source_delivery_vectors.json"


def _fail(message: str) -> None:
    raise AssertionError(message)


def _contract() -> BronzeProductContract:
    document = json.loads(VECTORS.read_text(encoding="utf-8"))
    for entry in document["positive"]:
        if entry["case"] == "append_only_managed_opaque_batch":
            return BronzeProductContract.model_validate(entry["payload"])
    raise AssertionError("append_only_managed_opaque_batch missing")


def _run(argv: list[str]) -> tuple[int, str, str]:
    stdout = io.StringIO()
    stderr = io.StringIO()
    with redirect_stdout(stdout), redirect_stderr(stderr):
        code = cli_main(argv)
    return code, stdout.getvalue(), stderr.getvalue()


def _json_run(argv: list[str]) -> tuple[int, dict, str]:
    code, out, err = _run([*argv, "--json"])
    if not out.strip():
        return code, {}, err
    return code, json.loads(out), err


def _write_binding(path: Path, binding: RuntimeBinding) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        yaml.safe_dump(_omit_nulls(binding.model_dump(mode="json", by_alias=True)), sort_keys=False),
        encoding="utf-8",
    )


def _omit_nulls(value):
    if isinstance(value, dict):
        return {key: _omit_nulls(item) for key, item in value.items() if item is not None}
    if isinstance(value, list):
        return [_omit_nulls(item) for item in value]
    return value


def _declaration_yaml(contract: BronzeProductContract) -> str:
    landing = _omit_nulls(contract.landing.model_dump(mode="json", by_alias=True))
    delivery = _omit_nulls(contract.delivery.model_dump(mode="json", by_alias=True))
    product = _omit_nulls(contract.product.model_dump(mode="json", by_alias=True))
    product.pop("domain", None)
    projection = [_omit_nulls(item.model_dump(mode="json", by_alias=True)) for item in contract.projection]
    document = {
        "source": {"name": contract.logical_identity.source},
        "tables": {
            contract.logical_identity.table: {
                "landing": landing,
                "delivery": delivery,
                "product": product,
                "projection": projection,
            }
        },
    }
    return yaml.safe_dump(document, sort_keys=False)


def _make_project(root: Path, contract: BronzeProductContract, binding: RuntimeBinding) -> Path:
    (root / "domains").mkdir()
    (root / "declarations").mkdir()
    (root / "runtime").mkdir()
    (root / "dbt_project.yml").write_text("name: bronze_tmp\nprofile: bronze_tmp\n", encoding="utf-8")
    (root / "estate.yml").write_text(
        "estate:\n  namespace: " + contract.logical_identity.estate_namespace + "\n",
        encoding="utf-8",
    )
    (root / "domains" / "operations.yml").write_text(
        yaml.safe_dump(
            {
                "bronze": {
                    "domain": {"name": "operations", "display_name": "Operations"},
                    "products": [{"source": contract.logical_identity.source, "table": contract.logical_identity.table}],
                }
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    (root / "declarations" / "orders.yml").write_text(_declaration_yaml(contract), encoding="utf-8")
    typed = load_typed_declarations(EstateContext.resolve(estate_root=root))
    key = (contract.logical_identity.source, contract.logical_identity.table)
    loaded = typed.tables[key].contract
    digest = canonical_digest(loaded.model_dump(mode="json", by_alias=True))
    if digest != binding.contract_digest:
        binding = binding.model_copy(update={"contract_digest": digest})
    _write_binding(root / "runtime" / "local.yml", binding)
    return root / "runtime" / "local.yml"


def _shared(project: Path, contract: BronzeProductContract, binding_rel: str = "runtime/local.yml") -> list[str]:
    return [
        "--project-dir", str(project),
        "--source", contract.logical_identity.source,
        "--table", contract.logical_identity.table,
        "--binding", binding_rel,
        "--environment", "local",
    ]


def _sidecar(contract: BronzeProductContract, payload: bytes, delivery_id: str, directory: Path, contract_digest: str | None = None) -> tuple[Path, Path]:
    digest = contract_digest or canonical_digest(contract.model_dump(mode="json", by_alias=True))
    body = {
        "schema": "ergasterion.delivery-manifest/v1",
        "logical_identity": contract.logical_identity.model_dump(mode="json"),
        "product_version": contract.product.product_version,
        "contract_digest": digest,
        "delivery_id": delivery_id,
        "batch_id": delivery_id,
        "scheduled_boundary_at": "2026-01-01T01:00:00.000000Z",
        "effective_boundary_at": None,
        "payload": {
            "media_type": "application/x-ndjson",
            "content_encoding": "identity",
            "codec_version": 1,
            "byte_length": str(len(payload)),
            "sha256": transport_payload_fingerprint(payload),
        },
        "frame_sequence_digest": None,
        "progress_claim": {"kind": "opaque_batch"},
        "declared_row_count": str(len(json.loads(payload)) if payload[:1] == b"[" else (payload.count(b"\n") or 1)),
        "snapshot_attestation": None,
    }
    payload_path = directory / f"{delivery_id}.ndjson"
    manifest_path = directory / f"{delivery_id}.manifest.json"
    payload_path.write_bytes(payload)
    manifest_path.write_text(json.dumps(body, separators=(",", ":")), encoding="utf-8")
    return manifest_path, payload_path


def _rows_payload(rows: list[dict]) -> bytes:
    return json.dumps(rows, separators=(",", ":")).encode("utf-8")


def test_help_introduces_bronze_terms() -> None:
    code, out, err = _run(["--help"])
    assert code == 0, err
    text = out.lower()
    for token in (
        "subcommands", "bronze", "received-batch", "direct connector", "read-only",
        "mutating", "--project-dir", "--binding", "--environment", "inspect", "ingest",
    ):
        assert token in text, f"top-level help missing {token!r}"
    code, out, err = _run(["plan", "--help"])
    assert code == 0, err
    text = (out + err).lower()
    for token in ("bronze", "read-only", "--project-dir", "--source", "--table", "--binding", "--environment", "contract register"):
        assert token in text, f"plan help missing {token!r}: {out}{err}"
    code, out, err = _run(["ingest", "--help"])
    assert code == 0, err
    text = (out + err).lower()
    assert "sidecar" in text or "received-batch" in text or "manifest" in text
    code, out, err = _run(["status", "--help"])
    assert code == 0, err
    assert "read-only" in (out + err).lower()
    code, out, err = _run(["contract", "activate", "--help"])
    assert code == 0, err
    assert "carry" in (out + err).lower() and "reset" in (out + err).lower()


def test_local_and_dbt_conformance_and_deterministic_manifest() -> None:
    contract = _contract()
    plan = resolve(Layer.BRONZE)
    digest = compute_plan_digest(plan)
    binding = build_local_binding(
        contract, execution_plan_digest=digest,
        contract_digest=canonical_digest(contract.model_dump(mode="json", by_alias=True)),
    )
    assert binding.ports.scratch_store.adapter_id == "local_scratch_store"
    assert int(binding.runtime_resources.max_parallel_attempts) == 1
    assert binding.ports.source_connector.endpoint_ref == "local-file"
    assert binding.ports.key_resolver.secret_resolver_refs == ()
    with tempfile.TemporaryDirectory() as tmp:
        project = Path(tmp)
        _make_project(project, contract, binding)
        typed = load_typed_declarations(EstateContext.resolve(estate_root=project))
        local = LocalIngestionTranslator(binding=binding, plan_digest=digest)
        dbt = DbtTranslator(
            typed=typed,
            bound={(contract.logical_identity.source, contract.logical_identity.table): binding},
            plan_digest=digest,
        )
        routed = check_translator_conformance(plan, [local, dbt])
    assert routed.translations["local-ingestion"].metadata["runtime_manifest_digest"]
    first = compile_runtime_manifest(plan, binding)
    second = compile_runtime_manifest(plan, binding)
    assert first.runtime_manifest_digest == second.runtime_manifest_digest
    assert first.model_dump(mode="json") == second.model_dump(mode="json")
    assert any(route.role.value == "observer" and route.translator_id == "dbt" for route in first.routes)
    assert {route.occurrence_id for route in first.routes if route.role.value == "execution_owner"} == {
        "bronze.checkpoint", "bronze.ingest", "bronze.validate", "bronze.contract",
        "bronze.schema", "bronze.publish", "bronze.lineage", "bronze.metadata",
    }


def test_environment_mismatch_exits_2() -> None:
    contract = _contract()
    plan = resolve(Layer.BRONZE)
    binding = build_local_binding(
        contract, execution_plan_digest=compute_plan_digest(plan),
        contract_digest=canonical_digest(contract.model_dump(mode="json", by_alias=True)),
    )
    with tempfile.TemporaryDirectory() as tmp:
        project = Path(tmp)
        _make_project(project, contract, binding)
        code, payload, err = _json_run([
            "plan", "--project-dir", str(project), "--source", contract.logical_identity.source,
            "--table", contract.logical_identity.table, "--binding", "runtime/local.yml",
            "--environment", "prod",
        ])
        assert code == 2, (payload, err)
        assert payload.get("status") == "failed"
        assert any(error["code"] == "invalid_config" for error in payload.get("errors", []))


def test_temporary_project_operator_journey() -> None:
    contract = _contract()
    plan = resolve(Layer.BRONZE)
    plan_digest = compute_plan_digest(plan)
    contract_digest = canonical_digest(contract.model_dump(mode="json", by_alias=True))
    binding = build_local_binding(contract, execution_plan_digest=plan_digest, contract_digest=contract_digest)
    holder = {"dt": datetime(2026, 1, 1, 1, 0, tzinfo=timezone.utc)}
    set_clock(Clock(lambda: holder["dt"]))
    set_projection_faults(0)
    try:
        with tempfile.TemporaryDirectory() as tmp:
            project = Path(tmp)
            binding_path = _make_project(project, contract, binding)
            deliveries = project / "deliveries"
            deliveries.mkdir()
            shared = _shared(project, contract)

            code, planned, err = _json_run(["plan", *shared])
            assert code == 0, err
            assert planned["result"]["kind"] == "plan"
            assert planned["runtime_manifest_digest"] == planned["result"]["runtime_manifest_digest"]
            contract_digest = planned["contract_digest"]

            code, registered, err = _json_run(["contract", "register", *shared])
            assert code == 0, err
            assert registered["result"]["kind"] == "contract_registered"

            code, carry, err = _json_run([
                "contract", "activate", *shared,
                "--candidate-digest", contract_digest, "--migration", "carry",
            ])
            assert code == 0, err
            assert carry["result"]["activation_state"] == "active"

            code, again, err = _json_run(["contract", "register", *shared])
            assert code == 0, err
            code, reset, err = _json_run([
                "contract", "activate", *shared,
                "--candidate-digest", contract_digest, "--migration", "reset",
            ])
            assert code == 0, err
            assert reset["result"]["migration"]["kind"] == "reset"

            code, dep, err = _json_run(["deployment", "register", *shared])
            assert code == 0, err
            digest = dep["result"]["runtime_manifest_digest"]
            code, activated, err = _json_run(["deployment", "activate", *shared, "--manifest-digest", digest])
            assert code == 0, err

            good = {"order_id": "o-1", "loaded_at": "2026-01-01T01:00:00.000000Z", "key": "o-1", "accept": True}
            bad = {"order_id": None, "loaded_at": "2026-01-01T01:00:00.000000Z", "key": "o-bad", "accept": False}
            first_payload = _rows_payload([good, bad])
            manifest, payload = _sidecar(contract, first_payload, "delivery-1", deliveries, contract_digest)
            code, ingested, err = _json_run(["ingest", "file", *shared, "--manifest", str(manifest), "--payload", str(payload)])
            assert code == 0, (ingested, err)
            claim = ingested["result"]["attempt"]["claim_digest"]
            first_attempt = ingested["result"]["attempt"]["attempt_id"]

            relocated = build_local_binding(
                contract, execution_plan_digest=plan_digest, contract_digest=contract_digest,
                endpoints={"source_connector": "local-file-relocated"}, schema_ref="bronze_relocated",
                binding_id="local-synthetic-relocated",
            )
            relocated_path = project / "runtime" / "relocated.yml"
            _write_binding(relocated_path, relocated)
            relocated_shared = _shared(project, contract, "runtime/relocated.yml")
            code, dep2, err = _json_run(["deployment", "register", *relocated_shared])
            assert code == 0, err
            code, act2, err = _json_run([
                "deployment", "activate", *relocated_shared,
                "--manifest-digest", dep2["result"]["runtime_manifest_digest"],
            ])
            assert code == 0, (act2, err)

            other = _rows_payload([{"order_id": "o-2", "loaded_at": "2026-01-01T01:00:00.000000Z", "key": "o-old", "accept": True}])
            old_manifest, old_payload = _sidecar(contract, other, "delivery-old", deliveries, contract_digest)
            code, rejected, err = _json_run([
                "ingest", "file", *shared, "--manifest", str(old_manifest), "--payload", str(old_payload),
            ])
            assert code == 4, (rejected, err)
            assert any(error["code"] == "superseded_deployment" for error in rejected.get("errors", []))

            code, replay, err = _json_run([
                "ingest", "file", *shared, "--manifest", str(manifest), "--payload", str(payload),
            ])
            assert code == 0, (replay, err)
            assert replay["status"] == "noop"
            assert replay["result"]["attempt"]["attempt_id"] == first_attempt
            assert replay["result"]["attempt"]["claim_digest"] == claim

            second = _rows_payload([{"order_id": "o-2", "loaded_at": "2026-01-01T02:00:00.000000Z", "key": "o-2", "accept": True}])
            m2, p2 = _sidecar(contract, second, "delivery-2", deliveries, contract_digest)
            code, ingest2, err = _json_run([
                "ingest", "file", *relocated_shared, "--manifest", str(m2), "--payload", str(p2),
            ])
            assert code == 0, (ingest2, err)

            holder["dt"] = holder["dt"] + timedelta(hours=3)
            code, due, err = _json_run(["ingest", "due", *relocated_shared])
            assert code == 0, (due, err)
            assert due["result"]["kind"] == "due_evaluation"
            assert int(due["result"]["transitions_applied"]) >= 1

            code, status, err = _json_run(["status", *relocated_shared])
            assert code == 0, err
            assert status["result"]["kind"] == "stream_status"

            code, replay2, err = _json_run([
                "ingest", "file", *relocated_shared, "--manifest", str(m2), "--payload", str(p2),
            ])
            assert code == 0, err
            assert replay2["status"] == "noop"

            blocked = _rows_payload([{"order_id": "o-3", "loaded_at": "2026-01-01T03:00:00.000000Z", "key": "o-3", "accept": True}])
            m3, p3 = _sidecar(contract, blocked, "delivery-3", deliveries, contract_digest)
            set_projection_faults(4)
            code, blocked_result, err = _json_run([
                "ingest", "file", *relocated_shared, "--manifest", str(m3), "--payload", str(p3),
            ])
            assert blocked_result["result"]["attempt"]["state"] in {"commit_blocked", "committing"} or code in {0, 5}, (
                blocked_result, err,
            )
            for _ in range(4):
                _json_run(["ingest", "due", *relocated_shared])
            code, refused, err = _json_run([
                "local-backup", *relocated_shared, "--action", "create",
                "--destination", str(project.parent / "backup-live"),
            ])
            assert code != 0, refused
            set_projection_faults(0)
            code, recon, err = _json_run(["reconcile", *relocated_shared])
            assert code == 0, (recon, err)
            assert recon["result"]["kind"] == "reconciliation"

            code, inspected, err = _json_run(["inspect", *relocated_shared])
            assert code == 0, err
            kinds = {item["kind"] for item in inspected["result"]["evidence"]["items"]}
            for needed in ("contract", "schema", "quality", "lineage", "receipt"):
                assert needed in kinds, kinds

            code, quarantined, err = _json_run(["quarantine", *relocated_shared, "--action", "list"])
            assert code == 0, err
            items = quarantined["result"]["evidence"]["items"]
            assert items, quarantined
            rejected = [
                item for item in items
                if item["disposition"]["status"] == "rejected"
                or (item.get("decision_page") or {}).get("items")
            ]
            assert rejected, items
            disposition_id = rejected[0]["disposition"]["disposition_id"]
            code, released, err = _json_run([
                "quarantine", *relocated_shared, "--action", "release", "--disposition-id", disposition_id,
            ])
            assert code == 0, (released, err)
            assert released["result"]["status"] == "released"

            code, row_level, err = _json_run([
                "quarantine", *relocated_shared, "--action", "list", "--row-level",
            ])
            assert code == 2, (row_level, err)

            backup_dir = project / "backup-out"
            code, created, err = _json_run([
                "local-backup", *relocated_shared, "--action", "create", "--destination", str(backup_dir),
            ])
            assert code == 0, (created, err)
            runtime_root = project / "runtime" / "data"
            before_revision = created["result"]["manifest"]["state_revision"]
            before_projection = created["result"]["manifest"]["projection_revision"]
            shutil.rmtree(runtime_root)
            assert not runtime_root.exists()
            code, restored, err = _json_run([
                "local-backup", *relocated_shared, "--action", "restore",
                "--manifest", str(backup_dir / "backup-manifest.json"),
            ])
            assert code == 0, (restored, err)
            assert restored["result"]["manifest"]["state_revision"] == before_revision
            assert restored["result"]["manifest"]["projection_revision"] == before_projection
            code, status_after, err = _json_run(["status", *relocated_shared])
            assert code == 0, err
            assert status_after["result"]["operational_status"]["state"]["state_revision"] == before_revision
            assert "ReprocessingClaim" not in json.dumps(restored)
    finally:
        set_clock(None)
        set_projection_faults(0)


def test_translator_has_no_backend_imports() -> None:
    path = REPO_ROOT / "ergasterion" / "translators" / "local_ingestion.py"
    text = path.read_text(encoding="utf-8")
    for banned in ("import duckdb", "from duckdb", "import sqlite3", "from sqlite3", "import airflow", "from airflow"):
        for line in text.splitlines():
            stripped = line.strip()
            assert not stripped.startswith(banned), stripped


def _activate_existing(project: Path, contract: BronzeProductContract, binding_rel: str) -> tuple[list[str], str]:
    shared = _shared(project, contract, binding_rel)
    code, planned, err = _json_run(["plan", *shared])
    assert code == 0, err
    digest = planned["contract_digest"]
    code, _, err = _json_run(["contract", "register", *shared])
    assert code == 0, err
    code, _, err = _json_run([
        "contract", "activate", *shared, "--candidate-digest", digest, "--migration", "carry",
    ])
    assert code == 0, err
    code, dep, err = _json_run(["deployment", "register", *shared])
    assert code == 0, err
    code, _, err = _json_run([
        "deployment", "activate", *shared, "--manifest-digest", dep["result"]["runtime_manifest_digest"],
    ])
    assert code == 0, err
    return shared, digest


def _activate(project: Path, contract: BronzeProductContract, binding: RuntimeBinding) -> tuple[list[str], str]:
    _make_project(project, contract, binding)
    return _activate_existing(project, contract, "runtime/local.yml")


def _make_two_identity_project(
    root: Path,
    first: BronzeProductContract,
    first_binding: RuntimeBinding,
    second: BronzeProductContract,
    second_binding: RuntimeBinding,
) -> None:
    (root / "domains").mkdir()
    (root / "declarations").mkdir()
    (root / "runtime").mkdir()
    (root / "dbt_project.yml").write_text("name: bronze_tmp\nprofile: bronze_tmp\n", encoding="utf-8")
    (root / "estate.yml").write_text(
        "estate:\n  namespace: " + first.logical_identity.estate_namespace + "\n",
        encoding="utf-8",
    )
    (root / "domains" / "operations.yml").write_text(
        yaml.safe_dump(
            {
                "bronze": {
                    "domain": {"name": "operations", "display_name": "Operations"},
                    "products": [
                        {"source": first.logical_identity.source, "table": first.logical_identity.table},
                        {"source": second.logical_identity.source, "table": second.logical_identity.table},
                    ],
                }
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    (root / "declarations" / "orders.yml").write_text(_declaration_yaml(first), encoding="utf-8")
    (root / "declarations" / "shipments.yml").write_text(_declaration_yaml(second), encoding="utf-8")
    typed = load_typed_declarations(EstateContext.resolve(estate_root=root))
    for contract, binding, rel in (
        (first, first_binding, "runtime/orders.yml"),
        (second, second_binding, "runtime/shipments.yml"),
    ):
        key = (contract.logical_identity.source, contract.logical_identity.table)
        loaded = typed.tables[key].contract
        digest = canonical_digest(loaded.model_dump(mode="json", by_alias=True))
        if digest != binding.contract_digest:
            binding = binding.model_copy(update={"contract_digest": digest})
        _write_binding(root / rel, binding)


def test_two_identities_one_runtime_root_do_not_gap_ordinals() -> None:
    first = _contract()
    second = first.model_copy(
        update={
            "logical_identity": first.logical_identity.model_copy(update={"table": "shipments"}),
            "product": first.product.model_copy(update={"display_name": "Shipments"}),
        }
    )
    plan_digest = compute_plan_digest(resolve(Layer.BRONZE))
    first_digest = canonical_digest(first.model_dump(mode="json", by_alias=True))
    second_digest = canonical_digest(second.model_dump(mode="json", by_alias=True))
    first_binding = build_local_binding(first, execution_plan_digest=plan_digest, contract_digest=first_digest)
    second_binding = build_local_binding(
        second, execution_plan_digest=plan_digest, contract_digest=second_digest, binding_id="local-synthetic-shipments",
    )
    set_clock(Clock(lambda: datetime(2026, 1, 1, 1, 0, tzinfo=timezone.utc)))
    set_projection_faults(0)
    try:
        with tempfile.TemporaryDirectory() as tmp:
            project = Path(tmp)
            _make_two_identity_project(project, first, first_binding, second, second_binding)
            first_shared, first_loaded = _activate_existing(project, first, "runtime/orders.yml")
            second_shared, second_loaded = _activate_existing(project, second, "runtime/shipments.yml")
            deliveries = project / "deliveries"
            deliveries.mkdir()

            first_payload = _rows_payload([
                {"order_id": "o-1", "loaded_at": "2026-01-01T01:00:00.000000Z", "key": "o-1", "accept": True},
            ])
            m1, p1 = _sidecar(first, first_payload, "orders-1", deliveries, first_loaded)
            code, ingested, err = _json_run([
                "ingest", "file", *first_shared, "--manifest", str(m1), "--payload", str(p1),
            ])
            assert code == 0, (ingested, err)

            second_payload = _rows_payload([
                {"order_id": "s-1", "loaded_at": "2026-01-01T01:00:00.000000Z", "key": "s-1", "accept": True},
            ])
            m2, p2 = _sidecar(second, second_payload, "shipments-1", deliveries, second_loaded)
            code, ingested_b, err = _json_run([
                "ingest", "file", *second_shared, "--manifest", str(m2), "--payload", str(p2),
            ])
            assert code == 0, (ingested_b, err)

            again = _rows_payload([
                {"order_id": "o-2", "loaded_at": "2026-01-01T02:00:00.000000Z", "key": "o-2", "accept": True},
            ])
            m3, p3 = _sidecar(first, again, "orders-2", deliveries, first_loaded)
            code, ingested_a2, err = _json_run([
                "ingest", "file", *first_shared, "--manifest", str(m3), "--payload", str(p3),
            ])
            assert code == 0, (ingested_a2, err)
            assert not any(error.get("code") == "event_conflict" for error in ingested_a2.get("errors", [])), ingested_a2

            sidecar = project / "runtime" / "data" / LIFECYCLE_ORDINAL_FILENAME
            sidecar.write_text("{not-json\n", encoding="utf-8")
            third = _rows_payload([
                {"order_id": "o-3", "loaded_at": "2026-01-01T03:00:00.000000Z", "key": "o-3", "accept": True},
            ])
            m4, p4 = _sidecar(first, third, "orders-3", deliveries, first_loaded)
            code, ingested_a3, err = _json_run([
                "ingest", "file", *first_shared, "--manifest", str(m4), "--payload", str(p4),
            ])
            assert code == 0, (ingested_a3, err)
            assert not any(error.get("code") == "event_conflict" for error in ingested_a3.get("errors", [])), ingested_a3

            sidecar.unlink(missing_ok=True)
            fourth = _rows_payload([
                {"order_id": "o-4", "loaded_at": "2026-01-01T04:00:00.000000Z", "key": "o-4", "accept": True},
            ])
            m5, p5 = _sidecar(first, fourth, "orders-4", deliveries, first_loaded)
            code, ingested_a4, err = _json_run([
                "ingest", "file", *first_shared, "--manifest", str(m5), "--payload", str(p5),
            ])
            assert code == 0, (ingested_a4, err)
            assert not any(error.get("code") == "event_conflict" for error in ingested_a4.get("errors", [])), ingested_a4
    finally:
        set_clock(None)
        set_projection_faults(0)


def test_durable_store_retarget_rejected_before_lifecycle() -> None:
    contract = _contract()
    plan_digest = compute_plan_digest(resolve(Layer.BRONZE))
    contract_digest = canonical_digest(contract.model_dump(mode="json", by_alias=True))
    prior = build_local_binding(contract, execution_plan_digest=plan_digest, contract_digest=contract_digest)
    moved = build_local_binding(
        contract, execution_plan_digest=plan_digest, contract_digest=contract_digest,
        endpoints={"raw_store": "local-scratch"},
    )
    try:
        reject_store_relocation(prior, moved)
        _fail("raw_store local-raw -> local-scratch must raise unsupported_store_migration")
    except SettingsError as exc:
        assert exc.code == "unsupported_store_migration", exc.code
    reject_store_relocation(prior, prior)
    retargeted_target = prior.model_copy(update={"projection_target": "elsewhere"})
    try:
        reject_store_relocation(prior, retargeted_target)
        _fail("projection_target change must raise unsupported_secondary_target")
    except SettingsError as exc:
        assert exc.code == "unsupported_secondary_target", exc.code

    set_clock(Clock(lambda: datetime(2026, 1, 1, 1, 0, tzinfo=timezone.utc)))
    set_projection_faults(0)
    try:
        with tempfile.TemporaryDirectory() as tmp:
            project = Path(tmp)
            shared, digest = _activate(project, contract, prior)
            moved = moved.model_copy(update={"contract_digest": digest, "execution_plan_digest": prior.execution_plan_digest})
            _write_binding(project / "runtime" / "moved.yml", moved)
            moved_shared = _shared(project, contract, "runtime/moved.yml")
            code, payload, err = _json_run(["deployment", "register", *moved_shared])
            assert code != 0, (payload, err)
            assert any(error["code"] == "unsupported_store_migration" for error in payload.get("errors", [])), payload
    finally:
        set_clock(None)
        set_projection_faults(0)


def test_inspect_delivery_id_and_quarantine_revalidate() -> None:
    contract = _contract()
    plan_digest = compute_plan_digest(resolve(Layer.BRONZE))
    contract_digest = canonical_digest(contract.model_dump(mode="json", by_alias=True))
    binding = build_local_binding(contract, execution_plan_digest=plan_digest, contract_digest=contract_digest)
    set_clock(Clock(lambda: datetime(2026, 1, 1, 1, 0, tzinfo=timezone.utc)))
    set_projection_faults(0)
    try:
        with tempfile.TemporaryDirectory() as tmp:
            project = Path(tmp)
            shared, digest = _activate(project, contract, binding)
            deliveries = project / "deliveries"
            deliveries.mkdir()
            first = _rows_payload([
                {"order_id": "o-1", "loaded_at": "2026-01-01T01:00:00.000000Z", "key": "o-1", "accept": True},
                {"order_id": None, "loaded_at": "2026-01-01T01:00:00.000000Z", "key": "o-bad", "accept": False},
            ])
            m1, p1 = _sidecar(contract, first, "delivery-1", deliveries, digest)
            code, ingested, err = _json_run(["ingest", "file", *shared, "--manifest", str(m1), "--payload", str(p1)])
            assert code == 0, (ingested, err)
            second = _rows_payload([
                {"order_id": "o-2", "loaded_at": "2026-01-01T02:00:00.000000Z", "key": "o-2", "accept": True},
            ])
            m2, p2 = _sidecar(contract, second, "delivery-2", deliveries, digest)
            code, ingest2, err = _json_run(["ingest", "file", *shared, "--manifest", str(m2), "--payload", str(p2)])
            assert code == 0, (ingest2, err)

            code, inspected, err = _json_run(["inspect", *shared])
            assert code == 0, err
            all_items = inspected["result"]["evidence"]["items"]
            kinds = {item["kind"] for item in all_items}
            for needed in ("contract", "schema", "quality", "lineage", "receipt"):
                assert needed in kinds, kinds
            assert sum(1 for item in all_items if item["kind"] == "contract") == 1, all_items
            assert sum(1 for item in all_items if item["kind"] == "receipt") == 2, all_items

            code, filtered, err = _json_run(["inspect", *shared, "--delivery-id", "delivery-1"])
            assert code == 0, err
            filtered_items = filtered["result"]["evidence"]["items"]
            assert sum(1 for item in filtered_items if item["kind"] == "contract") == 1
            assert sum(1 for item in filtered_items if item["kind"] == "receipt") == 1, filtered_items
            assert sum(1 for item in filtered_items if item["kind"] == "quality") == 1, filtered_items
            lineage = [item for item in filtered_items if item["kind"] == "lineage"]
            assert len(lineage) == 1, filtered_items
            assert lineage[0]["run_lineage"]["delivery_id"] == "delivery-1"

            code, quarantined, err = _json_run(["quarantine", *shared, "--action", "list"])
            assert code == 0, err
            rejected = [
                item for item in quarantined["result"]["evidence"]["items"]
                if item["disposition"]["status"] == "rejected"
            ]
            assert rejected, quarantined
            disposition_id = rejected[0]["disposition"]["disposition_id"]
            ruleset = rejected[0]["disposition"]["ruleset_digest"]
            code, revalidated, err = _json_run([
                "quarantine", *shared, "--action", "revalidate",
                "--disposition-id", disposition_id, "--ruleset-digest", ruleset,
            ])
            assert code == 0, (revalidated, err)
            assert revalidated["result"]["status"] == "unchanged_finding", revalidated
            bogus = "0" * 64
            code, ignored, err = _json_run([
                "quarantine", *shared, "--action", "revalidate",
                "--disposition-id", disposition_id, "--ruleset-digest", bogus,
            ])
            assert code == 2, (ignored, err)
            assert any(error["code"] == "invalid_usage" for error in ignored.get("errors", [])), ignored
    finally:
        set_clock(None)
        set_projection_faults(0)


TESTS = [
    test_help_introduces_bronze_terms,
    test_local_and_dbt_conformance_and_deterministic_manifest,
    test_environment_mismatch_exits_2,
    test_temporary_project_operator_journey,
    test_translator_has_no_backend_imports,
    test_durable_store_retarget_rejected_before_lifecycle,
    test_inspect_delivery_id_and_quarantine_revalidate,
    test_two_identities_one_runtime_root_do_not_gap_ordinals,
]


def main() -> int:
    failures = 0
    for test in TESTS:
        try:
            test()
            sys.stdout.write(f"PASS {test.__name__}\n")
        except Exception:
            failures += 1
            sys.stdout.write(f"FAIL {test.__name__}\n")
            traceback.print_exc()
    if failures:
        sys.stdout.write(f"{failures} failed\n")
        return 1
    sys.stdout.write(f"{len(TESTS)} passed\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
