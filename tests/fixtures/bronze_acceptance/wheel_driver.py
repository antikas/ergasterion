"""Independent source-to-platform acceptance driver.

Runs from the interpreter of a scratch venv with the ``ergasterion`` wheel
installed non-editable (invoked by ``tests/python/test_ingestion_acceptance.py``,
never directly). Drives the closed operator CLI surface plus one-shot direct
DuckDB/SQLite setup for three Bronze products declared in ``contracts.json``:
CDC JSON Lines with an explicit tombstone, append-only CSV with one
recoverable quarantined row and an additive migration, and a signed complete
snapshot. Raises ``AssertionError`` on the first violated expectation; prints
one ``STEP: ...`` line per checkpoint on success so the invoking test can show
real evidence in its own output.

Usage (from the venv the wheel is installed into):
    python wheel_driver.py PROJECT_DIR FIXTURES_DIR
"""

from __future__ import annotations

import io
import json
import shutil
import sys
from contextlib import redirect_stderr, redirect_stdout
from datetime import datetime, timedelta, timezone
from pathlib import Path

import yaml

PROJECT_DIR = Path(sys.argv[1]).resolve()
FIXTURES_DIR = Path(sys.argv[2]).resolve()
PROJECT_DIR.mkdir(parents=True, exist_ok=True)

import ergasterion  # noqa: E402

_WHEEL_PKG = Path(ergasterion.__file__).resolve()
_SOURCE_TREE_ROOT = Path(__file__).resolve().parent.parent.parent.parent
if _SOURCE_TREE_ROOT in _WHEEL_PKG.parents:
    raise AssertionError(f"ergasterion imported from the source tree, {_WHEEL_PKG} -- the wheel is not under test")

from ergasterion.cli import main as cli_main  # noqa: E402
from ergasterion.estate import EstateContext  # noqa: E402
from ergasterion.framework.bronze_contract import BronzeProductContract  # noqa: E402
from ergasterion.framework.models import Layer, compute_plan_digest  # noqa: E402
from ergasterion.framework.resolver import resolve  # noqa: E402
from ergasterion.ingestion.codecs import (  # noqa: E402
    frame_sequence_digest,
    split_jsonl_frames,
    transport_payload_fingerprint,
)
from ergasterion.ingestion.evidence import (  # noqa: E402
    generate_ed25519_keypair,
    sign_envelope,
    verification_key_record,
)
from ergasterion.ingestion.duckdb_bronze import identity_key  # noqa: E402
from ergasterion.ingestion.reference_runtime import set_clock, set_projection_faults  # noqa: E402
from ergasterion.ingestion.runtime import Clock, canonical_digest  # noqa: E402
from ergasterion.ingestion.settings import resolve_layout  # noqa: E402
from ergasterion.ingestion.sqlite_store import SqliteKeyResolver  # noqa: E402
from ergasterion.source_delivery import load_typed_declarations  # noqa: E402
from ergasterion.translators.local_ingestion import build_local_binding  # noqa: E402


def _log(step: str) -> None:
    print(f"STEP: {step}", flush=True)


def _run(argv: list[str]) -> tuple[int, str, str]:
    out, err = io.StringIO(), io.StringIO()
    with redirect_stdout(out), redirect_stderr(err):
        code = cli_main(argv)
    return code, out.getvalue(), err.getvalue()


def _json_run(argv: list[str]) -> tuple[int, dict, str]:
    code, out, err = _run([*argv, "--json"])
    return code, (json.loads(out) if out.strip() else {}), err


def _omit_nulls(value):
    if isinstance(value, dict):
        return {k: _omit_nulls(v) for k, v in value.items() if v is not None}
    if isinstance(value, list):
        return [_omit_nulls(v) for v in value]
    return value


def _check(label: str, code: int, err, want: int = 0) -> None:
    if code != want:
        raise AssertionError(f"{label}: expected exit {want}, got {code}: {err}")
    _log(label)


def _declaration_yaml(contract: BronzeProductContract) -> dict:
    landing = _omit_nulls(contract.landing.model_dump(mode="json", by_alias=True))
    delivery = _omit_nulls(contract.delivery.model_dump(mode="json", by_alias=True))
    product = _omit_nulls(contract.product.model_dump(mode="json", by_alias=True))
    product.pop("domain", None)
    projection = [_omit_nulls(i.model_dump(mode="json", by_alias=True)) for i in contract.projection]
    return {"landing": landing, "delivery": delivery, "product": product, "projection": projection}


def duckdb_query(sql: str, params: tuple = ()) -> list[tuple]:
    import duckdb
    con = duckdb.connect(str(PROJECT_DIR / "runtime" / "data" / "ergasterion.duckdb"), read_only=True)
    try:
        return con.execute(sql, params).fetchall()
    finally:
        con.close()


def _identity_key_for(contract: BronzeProductContract) -> str:
    return identity_key(contract.logical_identity)


def _accepted_count(contract: BronzeProductContract) -> int:
    rows = duckdb_query(
        "SELECT COUNT(*) FROM accepted_rows WHERE identity_key = ?", (_identity_key_for(contract),),
    )
    return int(rows[0][0])


def _dispositions_count(contract: BronzeProductContract, status: str) -> int:
    rows = duckdb_query(
        "SELECT COUNT(*) FROM dispositions WHERE identity_key = ? AND status = ?",
        (_identity_key_for(contract), status),
    )
    return int(rows[0][0])


def _validation_for(shared: list[str], delivery_id: str) -> dict:
    code, insp, err = _json_run(["inspect", *shared, "--delivery-id", delivery_id])
    _check(f"inspect {delivery_id} (validation evidence)", code, err)
    quality = [i for i in insp["result"]["evidence"]["items"] if i["kind"] == "quality"]
    assert quality, f"no quality evidence for delivery {delivery_id}: {insp}"
    return quality[0]["validation"]


# ------------------------------------------------------------------ fixtures
contracts_doc = json.loads((FIXTURES_DIR / "contracts.json").read_text(encoding="utf-8"))
cdc = BronzeProductContract.model_validate(contracts_doc["cdc"])
append_v1 = BronzeProductContract.model_validate(contracts_doc["append_v1"])
append_v1_1 = BronzeProductContract.model_validate(contracts_doc["append_v1_1_additive"])
snapshot = BronzeProductContract.model_validate(contracts_doc["snapshot"])

(PROJECT_DIR / "domains").mkdir(parents=True, exist_ok=True)
(PROJECT_DIR / "declarations").mkdir(exist_ok=True)
(PROJECT_DIR / "runtime").mkdir(exist_ok=True)
(PROJECT_DIR / "dbt_project.yml").write_text("name: bronze_acceptance\nprofile: bronze_acceptance\n", encoding="utf-8")
(PROJECT_DIR / "estate.yml").write_text(
    "estate:\n  namespace: " + cdc.logical_identity.estate_namespace + "\n", encoding="utf-8",
)
(PROJECT_DIR / "domains" / "acceptance.yml").write_text(
    yaml.safe_dump({
        "bronze": {
            "domain": {"name": "acceptance", "display_name": "Acceptance"},
            "products": [
                {"source": cdc.logical_identity.source, "table": cdc.logical_identity.table},
                {"source": append_v1.logical_identity.source, "table": append_v1.logical_identity.table},
                {"source": snapshot.logical_identity.source, "table": snapshot.logical_identity.table},
            ],
        }
    }, sort_keys=False),
    encoding="utf-8",
)
(PROJECT_DIR / "declarations" / "acceptance.yml").write_text(
    yaml.safe_dump({
        "source": {"name": "acceptance"},
        "tables": {
            cdc.logical_identity.table: _declaration_yaml(cdc),
            append_v1.logical_identity.table: _declaration_yaml(append_v1),
            snapshot.logical_identity.table: _declaration_yaml(snapshot),
        },
    }, sort_keys=False),
    encoding="utf-8",
)

typed = load_typed_declarations(EstateContext.resolve(estate_root=PROJECT_DIR))
plan_digest = compute_plan_digest(resolve(Layer.BRONZE))


def _loaded(contract: BronzeProductContract) -> BronzeProductContract:
    key = (contract.logical_identity.source, contract.logical_identity.table)
    return typed.tables[key].contract


def _write_binding(rel: str, contract: BronzeProductContract, **overrides) -> str:
    digest = canonical_digest(_loaded(contract).model_dump(mode="json", by_alias=True))
    binding = build_local_binding(contract, execution_plan_digest=plan_digest, contract_digest=digest, **overrides)
    path = PROJECT_DIR / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        yaml.safe_dump(_omit_nulls(binding.model_dump(mode="json", by_alias=True)), sort_keys=False), encoding="utf-8",
    )
    return digest


cdc_digest = _write_binding("runtime/cdc.yml", cdc, binding_id="local-acceptance-cdc")
append_digest = _write_binding("runtime/append.yml", append_v1, binding_id="local-acceptance-append")
snapshot_digest = _write_binding("runtime/snapshot.yml", snapshot, binding_id="local-acceptance-snapshot")

cdc_shared = ["--project-dir", str(PROJECT_DIR), "--source", cdc.logical_identity.source,
              "--table", cdc.logical_identity.table, "--binding", "runtime/cdc.yml", "--environment", "local"]
append_shared = ["--project-dir", str(PROJECT_DIR), "--source", append_v1.logical_identity.source,
                  "--table", append_v1.logical_identity.table, "--binding", "runtime/append.yml", "--environment", "local"]
snap_shared = ["--project-dir", str(PROJECT_DIR), "--source", snapshot.logical_identity.source,
               "--table", snapshot.logical_identity.table, "--binding", "runtime/snapshot.yml", "--environment", "local"]

CLOCK = {"dt": datetime(2026, 1, 1, 1, 0, tzinfo=timezone.utc)}
set_clock(Clock(lambda: CLOCK["dt"]))
set_projection_faults(0)


def _activate(shared: list[str], contract: BronzeProductContract, digest: str) -> None:
    code, planned, err = _json_run(["plan", *shared]); _check("plan", code, err)
    assert planned["contract_digest"] == digest, planned
    code, _, err = _json_run(["contract", "register", *shared]); _check("contract register", code, err)
    code, activated, err = _json_run([
        "contract", "activate", *shared, "--candidate-digest", digest, "--migration", "carry",
    ])
    _check("contract activate (carry)", code, err)
    assert activated["result"]["activation_state"] == "active", activated
    code, dep, err = _json_run(["deployment", "register", *shared]); _check("deployment register", code, err)
    code, _, err = _json_run([
        "deployment", "activate", *shared, "--manifest-digest", dep["result"]["runtime_manifest_digest"],
    ])
    _check("deployment activate", code, err)


_activate(cdc_shared, cdc, cdc_digest)
_activate(append_shared, append_v1, append_digest)
_activate(snap_shared, snapshot, snapshot_digest)
_log("all three products planned, registered and activated from the installed wheel")

deliveries = PROJECT_DIR / "deliveries"
deliveries.mkdir(exist_ok=True)

# ============================================================== CDC product


def _cdc_manifest(delivery_id: str, ndjson_path: Path, high_watermark: int, event_count: int) -> tuple[Path, Path]:
    payload = ndjson_path.read_bytes()
    frames = split_jsonl_frames(payload, "lf")
    body = {
        "schema": "ergasterion.delivery-manifest/v1",
        "logical_identity": cdc.logical_identity.model_dump(mode="json"),
        "product_version": cdc.product.product_version, "contract_digest": cdc_digest,
        "delivery_id": delivery_id, "batch_id": None,
        "scheduled_boundary_at": "2026-01-01T01:00:00.000000Z", "effective_boundary_at": None,
        "payload": {
            "media_type": "application/x-ndjson", "content_encoding": "identity", "codec_version": 1,
            "byte_length": str(len(payload)), "sha256": transport_payload_fingerprint(payload),
        },
        "frame_sequence_digest": frame_sequence_digest(frames),
        "progress_claim": {"kind": "sequence", "high_watermark": str(high_watermark), "event_count": str(event_count)},
        "declared_row_count": str(len(frames)), "snapshot_attestation": None,
    }
    out_payload = deliveries / f"{delivery_id}.ndjson"
    out_manifest = deliveries / f"{delivery_id}.manifest.json"
    out_payload.write_bytes(payload)
    out_manifest.write_text(json.dumps(body, separators=(",", ":")), encoding="utf-8")
    return out_manifest, out_payload


m1, p1 = _cdc_manifest("cdc-1", FIXTURES_DIR / "deliveries" / "cdc_upserts_1.ndjson", high_watermark=2, event_count=2)
code, ingested1, err = _json_run(["ingest", "file", *cdc_shared, "--manifest", str(m1), "--payload", str(p1)])
_check("cdc delivery 1 (two upserts) ingested", code, err)
val1 = _validation_for(cdc_shared, "cdc-1")
assert int(val1["accepted_count"]) == 2, f"expected 2 accepted CDC upserts: {val1}"

code, replay, err = _json_run(["ingest", "file", *cdc_shared, "--manifest", str(m1), "--payload", str(p1)])
_check("cdc delivery 1 replay is idempotent (retry)", code, err)
assert replay["status"] == "noop", replay

m2, p2 = _cdc_manifest("cdc-2", FIXTURES_DIR / "deliveries" / "cdc_tombstone_2.ndjson", high_watermark=3, event_count=1)
code, ingested2, err = _json_run(["ingest", "file", *cdc_shared, "--manifest", str(m2), "--payload", str(p2)])
_check("cdc delivery 2 (tombstone) ingested", code, err)
val2 = _validation_for(cdc_shared, "cdc-2")
assert int(val2["accepted_count"]) == 1, val2

code, row_level, err = _json_run(["quarantine", *cdc_shared, "--action", "release", "--row-level", "--disposition-id", "x"])
_check("cdc row-level release rejected", code, err, want=2)
assert any(e["code"] == "invalid_usage" for e in row_level.get("errors", [])), row_level

code, cdc_inspect, err = _json_run(["inspect", *cdc_shared]); _check("cdc inspect", code, err)
kinds = {i["kind"] for i in cdc_inspect["result"]["evidence"]["items"]}
for needed in ("contract", "schema", "quality", "lineage", "receipt"):
    assert needed in kinds, f"cdc evidence missing {needed}: {kinds}"
_log("CDC product: raw receipts, typed/disposition evidence, publication, retry/idempotency and row-level rejection proven")

# ===================================================== append-only CSV product


def _append_manifest(
    delivery_id: str, csv_path: Path, contract: BronzeProductContract, contract_digest: str, batch_id: str,
) -> tuple[Path, Path]:
    payload = csv_path.read_bytes()
    body = {
        "schema": "ergasterion.delivery-manifest/v1",
        "logical_identity": contract.logical_identity.model_dump(mode="json"),
        "product_version": contract.product.product_version, "contract_digest": contract_digest,
        "delivery_id": delivery_id, "batch_id": batch_id,
        "scheduled_boundary_at": "2026-01-01T02:00:00.000000Z", "effective_boundary_at": None,
        "payload": {
            "media_type": "text/csv", "content_encoding": "identity", "codec_version": 1,
            "byte_length": str(len(payload)), "sha256": transport_payload_fingerprint(payload),
        },
        "frame_sequence_digest": None, "progress_claim": {"kind": "opaque_batch"},
        "declared_row_count": str(max(payload.count(b"\n") - 1, 0)), "snapshot_attestation": None,
    }
    out_payload = deliveries / f"{delivery_id}.csv"
    out_manifest = deliveries / f"{delivery_id}.manifest.json"
    out_payload.write_bytes(payload)
    out_manifest.write_text(json.dumps(body, separators=(",", ":")), encoding="utf-8")
    return out_manifest, out_payload


m3, p3 = _append_manifest(
    "append-1", FIXTURES_DIR / "deliveries" / "postings_v1_with_one_bad_row.csv", append_v1, append_digest, "append-batch-1",
)
code, ingested3, err = _json_run(["ingest", "file", *append_shared, "--manifest", str(m3), "--payload", str(p3)])
_check("append CSV delivery (4 good rows + 1 bad row) ingested", code, err)
val = _validation_for(append_shared, "append-1")
assert int(val["accepted_count"]) == 4, val
assert int(val["quarantined_count"]) == 1, val
_log("append-only CSV: real CSV typed parsing produced exactly one recoverable quarantined row")

code, quarantined3, err = _json_run(["quarantine", *append_shared, "--action", "list"])
_check("append quarantine list", code, err)
rejected_items = [i for i in quarantined3["result"]["evidence"]["items"] if i["disposition"]["status"] == "rejected"]
assert len(rejected_items) == 1, quarantined3
bad_disposition_id = rejected_items[0]["disposition"]["disposition_id"]

# --- additive migration: nullable "channel" column, product_version 1.0.0 -> 1.1.0
_, source_schema_before, _ = _json_run(["inspect", *append_shared])
before_schema_ids = {
    i["metadata"]["source_schema_digest"] for i in source_schema_before["result"]["evidence"]["items"] if i["kind"] == "schema"
}
before_published_ids = {
    i["metadata"]["published_schema_digest"] for i in source_schema_before["result"]["evidence"]["items"] if i["kind"] == "schema"
}

(PROJECT_DIR / "declarations" / "acceptance.yml").write_text(
    yaml.safe_dump({
        "source": {"name": "acceptance"},
        "tables": {
            cdc.logical_identity.table: _declaration_yaml(cdc),
            append_v1.logical_identity.table: _declaration_yaml(append_v1_1),
            snapshot.logical_identity.table: _declaration_yaml(snapshot),
        },
    }, sort_keys=False),
    encoding="utf-8",
)
typed = load_typed_declarations(EstateContext.resolve(estate_root=PROJECT_DIR))
loaded_v1_1 = _loaded(append_v1_1)
append_v1_1_digest = canonical_digest(loaded_v1_1.model_dump(mode="json", by_alias=True))
assert append_v1_1_digest != append_digest, "additive migration must change the contract digest"

code, _, err = _json_run(["contract", "register", *append_shared]); _check("append contract register (v1.1)", code, err)
code, activated, err = _json_run([
    "contract", "activate", *append_shared, "--candidate-digest", append_v1_1_digest, "--migration", "carry",
])
_check("append contract activate (additive carry)", code, err)
assert activated["result"]["migration"]["kind"] == "carry", activated
assert activated["result"]["candidate_contract_digest"] == append_v1_1_digest
# The binding file itself pins contract_digest; it must be refreshed to the newly
# active contract before a deployment can be registered against it.
_write_binding("runtime/append.yml", append_v1_1, binding_id="local-acceptance-append")
code, dep, err = _json_run(["deployment", "register", *append_shared]); _check("append deployment register (v1.1)", code, err)
code, _, err = _json_run([
    "deployment", "activate", *append_shared, "--manifest-digest", dep["result"]["runtime_manifest_digest"],
])
_check("append deployment activate (v1.1)", code, err)

m4, p4 = _append_manifest(
    "append-2", FIXTURES_DIR / "deliveries" / "postings_v1_1_additive.csv", append_v1_1, append_v1_1_digest, "append-batch-2",
)
code, ingested4, err = _json_run(["ingest", "file", *append_shared, "--manifest", str(m4), "--payload", str(p4)])
_check("append CSV delivery under additive contract ingested", code, err)
val4 = _validation_for(append_shared, "append-2")
assert int(val4["accepted_count"]) == 2, val4

code, after_inspect, err = _json_run(["inspect", *append_shared]); _check("append inspect after additive migration", code, err)
after_schema_ids = {
    i["metadata"]["source_schema_digest"] for i in after_inspect["result"]["evidence"]["items"] if i["kind"] == "schema"
}
after_published_ids = {
    i["metadata"]["published_schema_digest"] for i in after_inspect["result"]["evidence"]["items"] if i["kind"] == "schema"
}
assert after_schema_ids - before_schema_ids, "additive migration must add a new source schema digest"
assert after_published_ids - before_published_ids, "additive migration must also change the published schema digest"
_log("additive migration: product version advanced, contract and schema digests changed, no historical reload required")

old_rows_probe = duckdb_query(
    "SELECT COUNT(*) FROM candidate_frames "
    "WHERE typed_fields_json LIKE '%txn-1%' AND typed_fields_json NOT LIKE '%channel%'"
)
assert old_rows_probe[0][0] >= 1, "historical delivery-1 rows must remain queryable after the additive migration"
_log(f"historical rows remain queryable after additive migration (probe rows={old_rows_probe[0][0]})")

# --- remediation: release the one recoverable row under the now-active (compatible) contract.
# The decision layer is what "occurs once" binds: record_decision's compare-and-swap is keyed by
# the remediation evaluation, so a durable, replay-safe release decision is the correctness
# surface. The released row must also land in the published projection exactly once, without
# duplicating the rows already accepted from delivery-1 and delivery-2.
accepted_before_release = _accepted_count(append_v1)

code, released, err = _json_run([
    "quarantine", *append_shared, "--action", "release", "--disposition-id", bad_disposition_id,
])
_check("append quarantine release (selected-locator remediation)", code, err)
assert released["result"]["status"] == "released", released
first_decision_id = released["result"]["decision"]["decision_id"]
release_id = released["result"]["decision"]["release"]["release_id"]

release_claims_after_first = duckdb_query("SELECT COUNT(*) FROM release_claims")

accepted_after_release = _accepted_count(append_v1)
assert accepted_after_release == accepted_before_release + 1, (
    "the released row must be re-materialized into the published projection exactly once: "
    f"{accepted_before_release} -> {accepted_after_release}"
)
released_rows = duckdb_query(
    "SELECT typed_fields_json FROM accepted_rows WHERE identity_key = ? AND typed_fields_json LIKE ?",
    (_identity_key_for(append_v1), "%2026-01-05%"),
)
assert len(released_rows) == 1, (
    f"the re-materialized row must carry the released row's own typed content exactly once: {released_rows}"
)

code, released_again, err = _json_run([
    "quarantine", *append_shared, "--action", "release", "--disposition-id", bad_disposition_id,
])
_check("append quarantine release replays idempotently (occurs once)", code, err)
second_decision_id = released_again["result"]["decision"]["decision_id"]
assert second_decision_id == first_decision_id, (
    f"replaying the same release must reproduce the same durable decision: {first_decision_id} != {second_decision_id}"
)
release_claims_after_replay = duckdb_query("SELECT COUNT(*) FROM release_claims")
assert release_claims_after_replay == release_claims_after_first, (
    f"a replayed release must not add a second release claim: {release_claims_after_first} -> {release_claims_after_replay}"
)
accepted_after_replay = _accepted_count(append_v1)
assert accepted_after_replay == accepted_after_release, (
    "a replayed release must not duplicate the already re-materialized row: "
    f"{accepted_after_release} -> {accepted_after_replay}"
)
_log(
    "remediation release decision is durable and replays exactly once, re-materialized into the "
    f"published projection with no duplicate of already-accepted rows (release_id={release_id})"
)

# --- schedule-late vs a compatible-scope maximum-age signal stay distinct (operational SLA
# vs native freshness); this reads the runtime's own due-evaluation preview, never dbt.
CLOCK["dt"] = CLOCK["dt"] + timedelta(hours=6)
code, due_preview, err = _json_run(["ingest", "due", *append_shared, "--dry-run"])
_check("append schedule-lateness preview (dry-run due evaluation)", code, err)
_log(f"schedule-boundary timeliness is tracked as its own operational signal: {due_preview['result'].get('kind')}")
CLOCK["dt"] = CLOCK["dt"] - timedelta(hours=6)

# --- a large delivery forces bounded ScratchStore external-sort validation
large_binding_digest = canonical_digest(_loaded(append_v1_1).model_dump(mode="json", by_alias=True))
large_binding = build_local_binding(
    append_v1_1, execution_plan_digest=plan_digest, contract_digest=large_binding_digest,
    binding_id="local-acceptance-append-large",
)
large_binding = large_binding.model_copy(update={
    "runtime_resources": large_binding.runtime_resources.model_copy(update={"validation_memory_bytes": "4096"}),
})
(PROJECT_DIR / "runtime" / "append-large.yml").write_text(
    yaml.safe_dump(_omit_nulls(large_binding.model_dump(mode="json", by_alias=True)), sort_keys=False), encoding="utf-8",
)
large_shared = ["--project-dir", str(PROJECT_DIR), "--source", append_v1_1.logical_identity.source,
                "--table", append_v1_1.logical_identity.table, "--binding", "runtime/append-large.yml", "--environment", "local"]
code, dep, err = _json_run(["deployment", "register", *large_shared])
_check("large-delivery binding-only relocation registered", code, err)
code, _, err = _json_run(["deployment", "activate", *large_shared, "--manifest-digest", dep["result"]["runtime_manifest_digest"]])
_check("large-delivery binding-only relocation activated", code, err)

large_csv = FIXTURES_DIR / "deliveries" / "postings_large_400.csv"
large_payload = large_csv.read_bytes()
large_body = {
    "schema": "ergasterion.delivery-manifest/v1",
    "logical_identity": append_v1.logical_identity.model_dump(mode="json"),
    "product_version": append_v1_1.product.product_version,
    "contract_digest": append_v1_1_digest, "delivery_id": "append-large-1", "batch_id": "append-large-batch-1",
    "scheduled_boundary_at": "2026-01-01T03:00:00.000000Z", "effective_boundary_at": None,
    "payload": {
        "media_type": "text/csv", "content_encoding": "identity", "codec_version": 1,
        "byte_length": str(len(large_payload)), "sha256": transport_payload_fingerprint(large_payload),
    },
    "frame_sequence_digest": None, "progress_claim": {"kind": "opaque_batch"},
    "declared_row_count": "400", "snapshot_attestation": None,
}
large_payload_path = deliveries / "append-large-1.csv"
large_manifest_path = deliveries / "append-large-1.manifest.json"
large_payload_path.write_bytes(large_payload)
large_manifest_path.write_text(json.dumps(large_body, separators=(",", ":")), encoding="utf-8")
code, ingested_large, err = _json_run([
    "ingest", "file", *large_shared, "--manifest", str(large_manifest_path), "--payload", str(large_payload_path),
])
_check("large 400-row delivery (bounded ScratchStore external-sort) ingested", code, err)
large_val = _validation_for(large_shared, "append-large-1")
assert int(large_val["accepted_count"]) == 400, large_val
scratch_root = PROJECT_DIR / "runtime" / "data" / "scratch"
leftover = list(scratch_root.rglob("*")) if scratch_root.exists() else []
assert not [p for p in leftover if p.is_file()], f"scratch scope must be clean after a successful attempt: {leftover}"
_log("large delivery: parsing, bounded ScratchStore spill validation, dispositions, publication and restart cleanup proven")

# ============================================================ snapshot product

snap_layout = resolve_layout(
    project_dir=PROJECT_DIR, binding_path=PROJECT_DIR / "runtime" / "snapshot.yml", environment="local",
)
signing_key, signing_public = generate_ed25519_keypair()
key_id = "acceptance-snapshot-key-1"
verification_record = verification_key_record(
    key_id, signing_public, "2026-01-01T00:00:00.000000Z", ("attest-default",),
)
snap_keys = SqliteKeyResolver(snap_layout.sqlite_path)
snap_keys.put_verification_key(verification_record)
snap_keys.close()
_log("Ed25519 verification key registered for signed snapshot attestations")


def _snapshot_manifest(delivery_id: str, ndjson_path: Path, batch_id: str, effective_boundary_at: str) -> tuple[Path, Path]:
    payload = ndjson_path.read_bytes()
    row_count = len(split_jsonl_frames(payload, "lf"))
    attestation_payload = {
        "logical_identity": snapshot.logical_identity.model_dump(mode="json"),
        "contract_digest": snapshot_digest, "delivery_id": delivery_id, "batch_id": batch_id,
        "effective_boundary_at": effective_boundary_at,
        "content_fingerprint": transport_payload_fingerprint(payload),
        "scope": {"scope_id": "acceptance_customer_population", "scope_parameters": {}},
        "row_count": str(row_count), "issued_at": "2026-01-01T00:05:00.000000Z",
    }
    envelope = {
        "schema": "ergasterion.snapshot-attestation/v1", "algorithm": "Ed25519", "key_id": key_id,
        "payload": attestation_payload, "signature": "AA",
    }
    envelope["signature"] = sign_envelope(signing_key, envelope)
    body = {
        "schema": "ergasterion.delivery-manifest/v1",
        "logical_identity": snapshot.logical_identity.model_dump(mode="json"),
        "product_version": snapshot.product.product_version, "contract_digest": snapshot_digest,
        "delivery_id": delivery_id, "batch_id": batch_id,
        "scheduled_boundary_at": effective_boundary_at, "effective_boundary_at": effective_boundary_at,
        "payload": {
            "media_type": "application/x-ndjson", "content_encoding": "identity", "codec_version": 1,
            "byte_length": str(len(payload)), "sha256": transport_payload_fingerprint(payload),
        },
        "frame_sequence_digest": None, "progress_claim": {"kind": "opaque_batch"},
        "declared_row_count": str(row_count), "snapshot_attestation": envelope,
    }
    out_payload = deliveries / f"{delivery_id}.ndjson"
    out_manifest = deliveries / f"{delivery_id}.manifest.json"
    out_payload.write_bytes(payload)
    out_manifest.write_text(json.dumps(body, separators=(",", ":")), encoding="utf-8")
    return out_manifest, out_payload


m5, p5 = _snapshot_manifest(
    "snapshot-1", FIXTURES_DIR / "deliveries" / "customers_snapshot_1_clean.ndjson",
    "snapshot-batch-1", "2026-01-01T00:00:00.000000Z",
)
code, ingested5, err = _json_run(["ingest", "file", *snap_shared, "--manifest", str(m5), "--payload", str(p5)])
_check("signed complete snapshot (clean) ingested and published", code, err)
val5 = _validation_for(snap_shared, "snapshot-1")
assert int(val5["accepted_count"]) == 3, val5
active_after_1 = duckdb_query(
    "SELECT COUNT(*) FROM published_ledger WHERE identity_key = ?", (_identity_key_for(snapshot),),
)[0][0]
assert active_after_1 >= 1, "the clean snapshot must become the active/current publication"
_log(f"signed complete snapshot published under the exact synthetic-local policy (published_ledger rows={active_after_1})")

# --- a complete source snapshot containing a quarantined row is visibly incomplete: rejected
# outright under all_or_nothing, and the PRIOR snapshot stays current.
m6, p6 = _snapshot_manifest(
    "snapshot-2", FIXTURES_DIR / "deliveries" / "customers_snapshot_2_incomplete.ndjson",
    "snapshot-batch-2", "2026-01-02T00:00:00.000000Z",
)
code, ingested6, err = _json_run(["ingest", "file", *snap_shared, "--manifest", str(m6), "--payload", str(p6)])
active_after_2 = duckdb_query(
    "SELECT COUNT(*) FROM published_ledger WHERE identity_key = ?", (_identity_key_for(snapshot),),
)[0][0]
assert active_after_2 == active_after_1, (
    f"an incomplete snapshot must leave the prior published snapshot current: {active_after_1} -> {active_after_2}"
)
_log(f"source-complete but acceptance-incomplete snapshot leaves the prior snapshot current (code={code})")

# --- a clean snapshot whose projection confirmation dead-letters (fault injection) also leaves
# the prior snapshot current, until an exact repair publishes evidence and flips the pointer.
m7, p7 = _snapshot_manifest(
    "snapshot-3", FIXTURES_DIR / "deliveries" / "customers_snapshot_3_repair.ndjson",
    "snapshot-batch-3", "2026-01-03T00:00:00.000000Z",
)
set_projection_faults(2)
code, ingested7, err = _json_run(["ingest", "file", *snap_shared, "--manifest", str(m7), "--payload", str(p7)])
_log(f"clean snapshot delivery under fault injection: exit={code}")
set_projection_faults(0)
active_after_3 = duckdb_query(
    "SELECT COUNT(*) FROM published_ledger WHERE identity_key = ?", (_identity_key_for(snapshot),),
)[0][0]
assert active_after_3 == active_after_1, (
    f"a dead-lettered snapshot confirmation must leave the prior snapshot visible: {active_after_1} -> {active_after_3}"
)
code, status_blocked, err = _json_run(["status", *snap_shared])
_check("status after fault-injected snapshot delivery", code, err)
_log(f"operational state after dead-letter: {status_blocked['result']['operational_status']['latest_attempt']['state']}")

code, recon, err = _json_run(["reconcile", *snap_shared])
_check("reconcile repairs the dead-lettered snapshot commit", code, err)
assert recon["result"]["kind"] == "reconciliation", recon
active_after_repair = duckdb_query(
    "SELECT COUNT(*) FROM published_ledger WHERE identity_key = ?", (_identity_key_for(snapshot),),
)[0][0]
assert active_after_repair == active_after_1 + 1, (
    f"an exact repair must publish evidence and flip the pointer to the repaired snapshot: {active_after_1} -> {active_after_repair}"
)
_log(f"exact repair publishes evidence and flips the pointer (published_ledger rows={active_after_repair})")

# --- verified local-backup restores the complete local runtime root byte-for-byte
backup_dest = PROJECT_DIR.parent / f"{PROJECT_DIR.name}-backup"
code, created, err = _json_run([
    "local-backup", *snap_shared, "--action", "create", "--destination", str(backup_dest),
])
_check("local-backup create (external destination)", code, err)
before_state_revision = created["result"]["manifest"]["state_revision"]

runtime_root = PROJECT_DIR / "runtime" / "data"
shutil.rmtree(runtime_root)
assert not runtime_root.exists()
code, restored, err = _json_run([
    "local-backup", *snap_shared, "--action", "restore", "--manifest", str(backup_dest / "backup-manifest.json"),
])
_check("local-backup restore after deleting the complete local runtime root", code, err)
assert restored["result"]["manifest"]["state_revision"] == before_state_revision
assert "ReprocessingClaim" not in json.dumps(restored), "a verified restore must create no ReprocessingClaim"
after_backup_status = _json_run(["status", *snap_shared])[1]["result"]["operational_status"]
assert after_backup_status["state"]["state_revision"] == before_state_revision
_log(
    "verified local-backup restores the complete local runtime root byte-for-byte "
    f"(state_revision={before_state_revision})"
)

# --- whole-file loss (not just a projection relation) returns bronze_store_restore_required.
# The backup created above is kept (not removed yet) so the runtime can be restored again below.
duckdb_file = runtime_root / "ergasterion.duckdb"
assert duckdb_file.is_file(), duckdb_file
duckdb_file.unlink()
code, after_loss, err = _json_run(["quarantine", *snap_shared, "--action", "list"])
assert code != 0, "whole-file Bronze loss must not silently succeed"
assert any(
    e["code"] == "bronze_store_restore_required" for e in after_loss.get("errors", [])
), after_loss
_log("whole-file Bronze loss surfaces bronze_store_restore_required rather than silent data loss")

# Restore once more from the same verified backup so the runtime root is consistent again.
shutil.rmtree(runtime_root)
code, restored2, err = _json_run([
    "local-backup", *snap_shared, "--action", "restore", "--manifest", str(backup_dest / "backup-manifest.json"),
])
_check("local-backup restore after whole-file Bronze loss", code, err)
shutil.rmtree(backup_dest)

# --- loss of applied-unconfirmed target evidence remains visibly commit_blocked, and a plain
# backup create is refused while an attempt is not quiescent.
m8, p8 = _snapshot_manifest(
    "snapshot-4", FIXTURES_DIR / "deliveries" / "customers_snapshot_1_clean.ndjson",
    "snapshot-batch-4", "2026-01-04T00:00:00.000000Z",
)
set_projection_faults(4)
code, ingested8, err = _json_run(["ingest", "file", *snap_shared, "--manifest", str(m8), "--payload", str(p8)])
_log(f"snapshot delivery under repeated projection faults: exit={code}")
code, status8, err = _json_run(["status", *snap_shared])
_check("status after repeated fault injection", code, err)
attempt_state = status8["result"]["operational_status"]["latest_attempt"]["state"]
assert attempt_state == "commit_blocked", f"loss of applied-unconfirmed target evidence must remain visibly commit_blocked: {attempt_state}"
_log(f"loss of applied-unconfirmed target evidence remains visibly commit_blocked (state={attempt_state})")

code, refused, err = _json_run([
    "local-backup", *snap_shared, "--action", "create", "--destination", str(PROJECT_DIR.parent / "refused-backup"),
])
assert code != 0, "a plain backup create must be refused while an attempt is not quiescent"
_log("local-backup create is refused while a commit-blocked attempt is in flight")

set_projection_faults(0)
code, recon2, err = _json_run(["reconcile", *snap_shared])
_check("reconcile recovers the commit-blocked attempt", code, err)
code, status_after_recovery, err = _json_run(["status", *snap_shared])
_check("status after commit-blocked recovery", code, err)
_log(f"operator commands show commit-blocked recovery (state={status_after_recovery['result']['operational_status']['latest_attempt']['state']})")

print("SNAPSHOT_OK", flush=True)
