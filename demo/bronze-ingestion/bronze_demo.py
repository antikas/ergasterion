"""Account-free, network-free Bronze walkthrough over the local reference platform.

Run it with ``bash demo/bronze-ingestion/run_bronze_demo.sh``. It needs no warehouse
account and makes no network call: every command runs against a fresh, temporary
local runtime root created and destroyed by this script, never against a checked-in
database file.

Bronze is the layer that receives a source's delivered batch before anything else in
Ergasterion reads it. A source declares a **Bronze Product Contract**: its native
schema, how it delivers (change events, append-only, or a full snapshot), and the
quality rules a delivery must clear. The runtime applies that contract every time a
batch arrives: it parses the batch under the declared codec, checks every quality
rule, and either publishes the accepted rows or quarantines the rejected ones with a
locator back to the exact bytes that failed. Nothing about a single delivery's outcome
is decided by a person at run time; the contract decided it in advance, and the runtime
only applies it. `docs/architecture/bronze-ingestion.md` explains the mechanism this
script exercises; `docs/specifications/bronze-product-v1.md` is the exact field-by-field
contract reference.

Three scenarios, selected with ``--scenario``:

``normal-publication``
    A clean append-only CSV delivery. Every row passes the declared quality rules and
    publishes.

``acceptance-incomplete-snapshot``
    Two signed complete-snapshot deliveries. The first is clean and becomes the current
    snapshot. The second is source-complete (every row the source meant to send is
    present) but acceptance-incomplete (one row fails a mandatory quality rule). Under
    the contract's ``all_or_nothing`` publication mode the whole delivery is rejected,
    and the first snapshot stays the one consumers see.

``backup-restore``
    One delivery publishes, then the runtime's local backup command copies the
    complete local runtime root to a verified, restorable location outside both the
    project and the runtime root. The runtime root is deleted and restored from that
    backup, and the delivery's claim identity, visibility, progress and semantic
    times are proven unchanged across the deletion and restore.

Every contract, delivery and payload below is invented for this script. No production
schema, connector configuration or credential is read or required.
"""

from __future__ import annotations

import argparse
import io
import json
import shutil
import sys
import tempfile
from contextlib import redirect_stderr, redirect_stdout
from datetime import datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import yaml

from ergasterion.cli import main as cli_main
from ergasterion.estate import EstateContext
from ergasterion.framework.bronze_contract import BronzeProductContract
from ergasterion.framework.models import Layer, compute_plan_digest
from ergasterion.framework.resolver import resolve
from ergasterion.ingestion.codecs import transport_payload_fingerprint
from ergasterion.ingestion.duckdb_bronze import identity_key
from ergasterion.ingestion.evidence import generate_ed25519_keypair, sign_envelope, verification_key_record
from ergasterion.ingestion.reference_runtime import set_clock
from ergasterion.ingestion.runtime import Clock, canonical_digest
from ergasterion.ingestion.settings import resolve_layout
from ergasterion.ingestion.sqlite_store import SqliteKeyResolver
from ergasterion.source_delivery import load_typed_declarations
from ergasterion.translators.local_ingestion import build_local_binding

FROZEN_NOW = "2026-01-01T02:00:00.000000Z"


def _instant(text: str) -> datetime:
    return datetime.fromisoformat(text.replace("Z", "+00:00"))


class _MutableClock:
    """A clock a scenario can advance between deliveries, so a demo run stays
    deterministic (fixed instants, reproducible digests) while still moving
    time forward the way a real operator sequence does."""

    def __init__(self, start: str) -> None:
        self.now = _instant(start)

    def __call__(self) -> datetime:
        return self.now


# --------------------------------------------------------------------------- narration


def _log(message: str) -> None:
    print(f"  -- {message}")


def _heading(title: str) -> None:
    print(f"\n=== {title} ===")


# ------------------------------------------------------------------------- CLI helpers


def _run(argv: list[str]) -> tuple[int, str, str]:
    out, err = io.StringIO(), io.StringIO()
    with redirect_stdout(out), redirect_stderr(err):
        code = cli_main(argv)
    return code, out.getvalue(), err.getvalue()


def _json_run(argv: list[str]) -> tuple[int, dict, str]:
    code, out, err = _run([*argv, "--json"])
    return code, (json.loads(out) if out.strip() else {}), err


def _check(label: str, code: int, err: str, want: int = 0) -> None:
    if code != want:
        raise SystemExit(f"FAILED: {label}: expected exit {want}, got {code}\n{err}")
    _log(f"OK  {label}")


def _omit_nulls(value: object) -> object:
    if isinstance(value, dict):
        return {k: _omit_nulls(v) for k, v in value.items() if v is not None}
    if isinstance(value, list):
        return [_omit_nulls(v) for v in value]
    return value


# ---------------------------------------------------------------------- estate wiring


def _new_estate_root(label: str) -> Path:
    root = Path(tempfile.mkdtemp(prefix=f"ergasterion-bronze-demo-{label}-"))
    (root / "domains").mkdir()
    (root / "declarations").mkdir()
    (root / "runtime").mkdir()
    (root / "dbt_project.yml").write_text("name: bronze_demo\nprofile: bronze_demo\n", encoding="utf-8")
    return root


def _declaration_yaml(contract: BronzeProductContract) -> dict:
    landing = _omit_nulls(contract.landing.model_dump(mode="json", by_alias=True))
    delivery = _omit_nulls(contract.delivery.model_dump(mode="json", by_alias=True))
    product = _omit_nulls(contract.product.model_dump(mode="json", by_alias=True))
    product.pop("domain", None)
    projection = [_omit_nulls(i.model_dump(mode="json", by_alias=True)) for i in contract.projection]
    return {"landing": landing, "delivery": delivery, "product": product, "projection": projection}


def _activate(root: Path, contract: BronzeProductContract, binding_id: str) -> tuple[list[str], str]:
    """Write the estate files, then register and activate the contract and its
    binding-only deployment through the same operator commands a real estate uses."""

    source = contract.logical_identity.source
    table = contract.logical_identity.table

    (root / "estate.yml").write_text(
        f"estate:\n  namespace: {contract.logical_identity.estate_namespace}\n", encoding="utf-8",
    )
    (root / "domains" / f"{source}.yml").write_text(
        yaml.safe_dump(
            {"bronze": {"domain": {"name": source, "display_name": source.title()},
                        "products": [{"source": source, "table": table}]}},
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    (root / "declarations" / f"{source}.yml").write_text(
        yaml.safe_dump(
            {"source": {"name": source}, "tables": {table: _declaration_yaml(contract)}}, sort_keys=False,
        ),
        encoding="utf-8",
    )

    typed = load_typed_declarations(EstateContext.resolve(estate_root=root))
    loaded = typed.tables[(source, table)].contract
    contract_digest = canonical_digest(loaded.model_dump(mode="json", by_alias=True))
    plan_digest = compute_plan_digest(resolve(Layer.BRONZE))
    binding = build_local_binding(
        contract, execution_plan_digest=plan_digest, contract_digest=contract_digest, binding_id=binding_id,
    )
    binding_rel = f"runtime/{table}.yml"
    (root / binding_rel).write_text(
        yaml.safe_dump(_omit_nulls(binding.model_dump(mode="json", by_alias=True)), sort_keys=False),
        encoding="utf-8",
    )

    shared = [
        "--project-dir", str(root), "--source", source, "--table", table,
        "--binding", binding_rel, "--environment", "local",
    ]

    code, planned, err = _json_run(["plan", *shared])
    _check("plan (compiled Bronze graph and runtime manifest)", code, err)
    assert planned["execution_plan_digest"] == plan_digest

    code, _, err = _json_run(["contract", "register", *shared])
    _check("contract register (candidate, no active alias change yet)", code, err)

    code, activated, err = _json_run(
        ["contract", "activate", *shared, "--candidate-digest", contract_digest, "--migration", "carry"],
    )
    _check("contract activate (carry: keeps visibility progress)", code, err)
    assert activated["result"]["activation_state"] == "active"

    code, deployed, err = _json_run(["deployment", "register", *shared])
    _check("deployment register (candidate runtime manifest)", code, err)

    code, _, err = _json_run(
        ["deployment", "activate", *shared, "--manifest-digest", deployed["result"]["runtime_manifest_digest"]],
    )
    _check("deployment activate", code, err)

    return shared, contract_digest


def _csv_delivery(root: Path, delivery_id: str, csv_bytes: bytes, contract: BronzeProductContract,
                   contract_digest: str, boundary_at: str, batch_id: str) -> tuple[Path, Path]:
    body = {
        "schema": "ergasterion.delivery-manifest/v1",
        "logical_identity": contract.logical_identity.model_dump(mode="json"),
        "product_version": contract.product.product_version,
        "contract_digest": contract_digest,
        "delivery_id": delivery_id,
        "batch_id": batch_id,
        "scheduled_boundary_at": boundary_at,
        "effective_boundary_at": None,
        "payload": {
            "media_type": "text/csv", "content_encoding": "identity", "codec_version": 1,
            "byte_length": str(len(csv_bytes)), "sha256": transport_payload_fingerprint(csv_bytes),
        },
        "frame_sequence_digest": None,
        "progress_claim": {"kind": "opaque_batch"},
        "declared_row_count": str(max(csv_bytes.count(b"\n") - 1, 0)),
        "snapshot_attestation": None,
    }
    deliveries = root / "deliveries"
    deliveries.mkdir(exist_ok=True)
    payload_path = deliveries / f"{delivery_id}.csv"
    manifest_path = deliveries / f"{delivery_id}.manifest.json"
    payload_path.write_bytes(csv_bytes)
    manifest_path.write_text(json.dumps(body, separators=(",", ":")), encoding="utf-8")
    return manifest_path, payload_path


def _signed_snapshot_delivery(
    root: Path, delivery_id: str, ndjson_bytes: bytes, contract: BronzeProductContract, contract_digest: str,
    boundary_at: str, batch_id: str, scope_id: str, signing_key, key_id: str,
) -> tuple[Path, Path]:
    row_count = sum(1 for line in ndjson_bytes.splitlines() if line.strip())
    attestation_payload = {
        "logical_identity": contract.logical_identity.model_dump(mode="json"),
        "contract_digest": contract_digest, "delivery_id": delivery_id, "batch_id": batch_id,
        "effective_boundary_at": boundary_at,
        "content_fingerprint": transport_payload_fingerprint(ndjson_bytes),
        "scope": {"scope_id": scope_id, "scope_parameters": {}},
        "row_count": str(row_count), "issued_at": boundary_at,
    }
    envelope = {
        "schema": "ergasterion.snapshot-attestation/v1", "algorithm": "Ed25519", "key_id": key_id,
        "payload": attestation_payload, "signature": "AA",
    }
    envelope["signature"] = sign_envelope(signing_key, envelope)
    body = {
        "schema": "ergasterion.delivery-manifest/v1",
        "logical_identity": contract.logical_identity.model_dump(mode="json"),
        "product_version": contract.product.product_version, "contract_digest": contract_digest,
        "delivery_id": delivery_id, "batch_id": batch_id,
        "scheduled_boundary_at": boundary_at, "effective_boundary_at": boundary_at,
        "payload": {
            "media_type": "application/x-ndjson", "content_encoding": "identity", "codec_version": 1,
            "byte_length": str(len(ndjson_bytes)), "sha256": transport_payload_fingerprint(ndjson_bytes),
        },
        "frame_sequence_digest": None,
        "progress_claim": {"kind": "opaque_batch"},
        "declared_row_count": str(row_count),
        "snapshot_attestation": envelope,
    }
    deliveries = root / "deliveries"
    deliveries.mkdir(exist_ok=True)
    payload_path = deliveries / f"{delivery_id}.ndjson"
    manifest_path = deliveries / f"{delivery_id}.manifest.json"
    payload_path.write_bytes(ndjson_bytes)
    manifest_path.write_text(json.dumps(body, separators=(",", ":")), encoding="utf-8")
    return manifest_path, payload_path


# --------------------------------------------------------------------------- contracts


def _postings_contract() -> BronzeProductContract:
    return BronzeProductContract.model_validate({
        "schema": "ergasterion.bronze-product/v1",
        "logical_identity": {
            "estate_namespace": "com.example.ergasterion.demo", "source": "ledger", "table": "postings",
        },
        "product": {
            "product_version": "1.0.0", "display_name": "Ledger postings (append-only CSV)",
            "description": "Synthetic append-only ledger postings delivered as CSV.",
            "owner": "demo-reader", "domain": "demo", "support": "demo-reader",
            "classification": "synthetic", "access_policy_ref": "local-process-user", "retention_policy_ref": "local-ephemeral",
        },
        "landing": {
            "kind": "source", "source_name": "ledger_postings", "identifier": "postings",
            "integration": {"kind": "managed"}, "content_encodings": ["identity"],
            "codec": {
                "kind": "csv", "version": 1, "charset": "utf-8", "delimiter": ",", "header": True,
                "quote": "\"", "escape": "\\", "newline": "lf", "null_tokens": [""], "trim_whitespace": False,
            },
            "physical_columns": [
                {"name": "txn_id", "logical_type": "utf8_string", "nullable": False},
                {"name": "status", "logical_type": "utf8_string", "nullable": False},
                {"name": "amount", "logical_type": {"kind": "decimal", "precision": 18, "scale": 2}, "nullable": False},
                {"name": "booked_on", "logical_type": "date", "nullable": False},
                {"name": "loaded_at", "logical_type": "utc_instant", "nullable": False},
            ],
        },
        "delivery": {
            "kind": "production", "mode": "append_only", "progress": {"kind": "opaque_batch"},
            "delete_strategy": "none",
            "schedule": {"kind": "interval", "every_minutes": 60, "anchor_at": "2026-01-01T00:00:00.000000Z"},
            "schedule_lateness": {"warn_after_minutes": 15, "error_after_minutes": 60},
            "timestamps": {"load_field": "loaded_at"},
            "record_key": {"fields": ["txn_id"]},
            "quality": {
                "publication_mode": "publish_valid_rows", "max_error_fraction": "0.5",
                "rules": [
                    {"kind": "not_null", "field": "txn_id", "severity": "error"},
                    {"kind": "unique_key", "fields": ["txn_id"], "severity": "error"},
                ],
            },
            "retry": {"max_attempts": 3, "backoff": "exponential", "base_seconds": 5, "cap_seconds": 60},
        },
        "projection": [
            {"source": "txn_id", "name": "txn_id", "logical_type": "utf8_string", "nullable": False},
            {"source": "status", "name": "status", "logical_type": "utf8_string", "nullable": False},
            {"source": "amount", "name": "amount", "logical_type": {"kind": "decimal", "precision": 18, "scale": 2}, "nullable": False},
            {"source": "booked_on", "name": "booked_on", "logical_type": "date", "nullable": False},
            {"source": "loaded_at", "name": "loaded_at", "logical_type": "utc_instant", "nullable": False},
        ],
        "interfaces": {
            "raw": "bronze-demo-postings-raw", "source_native": "bronze-demo-postings-source-native",
            "published": "bronze-demo-postings-published", "quarantine": "bronze-demo-postings-quarantine",
            "deletion_evidence": "bronze-demo-postings-deletion-evidence",
        },
    })


def _customers_snapshot_contract(key_id: str) -> BronzeProductContract:
    return BronzeProductContract.model_validate({
        "schema": "ergasterion.bronze-product/v1",
        "logical_identity": {
            "estate_namespace": "com.example.ergasterion.demo", "source": "crm", "table": "customers",
        },
        "product": {
            "product_version": "1.0.0", "display_name": "Customer population (complete snapshot)",
            "description": "Synthetic signed customer population snapshot.",
            "owner": "demo-reader", "domain": "demo", "support": "demo-reader",
            "classification": "synthetic", "access_policy_ref": "local-process-user", "retention_policy_ref": "local-ephemeral",
        },
        "landing": {
            "kind": "source", "source_name": "crm_customers", "identifier": "customers",
            "integration": {"kind": "managed"}, "content_encodings": ["identity"],
            "codec": {
                "kind": "jsonl", "version": 1, "charset": "utf-8", "newline": "lf", "top_level": "object",
                "duplicate_keys": "reject", "number_mode": "exact_decimal", "allow_blank_lines": False,
            },
            "physical_columns": [
                {"name": "customer_id", "logical_type": "utf8_string", "nullable": False},
                {"name": "effective_at", "logical_type": "utc_instant", "nullable": False},
                {"name": "loaded_at", "logical_type": "utc_instant", "nullable": False},
            ],
        },
        "delivery": {
            "kind": "production", "mode": "complete_snapshot", "progress": {"kind": "opaque_batch"},
            "delete_strategy": "snapshot_diff",
            "schedule": {"kind": "interval", "every_minutes": 1440, "anchor_at": "2026-01-01T00:00:00.000000Z"},
            "schedule_lateness": {"warn_after_minutes": 60, "error_after_minutes": 240},
            "timestamps": {"load_field": "loaded_at", "effective_field": "effective_at"},
            "record_key": {
                "fields": ["customer_id"],
                "fingerprint_scope": {"scope_id": "demo_customer_population", "scope_parameters": {}},
                "hmac_key_id": "synthetic-local-hmac",
            },
            "snapshot": {
                "scope_id": "demo_customer_population", "scope_parameters": {}, "attestation_policy_ref": "attest-default",
                "allowed_key_ids": [key_id], "future_clock_skew_seconds": 30,
            },
            "quality": {
                "publication_mode": "all_or_nothing", "max_error_fraction": "0",
                "rules": [{"kind": "not_null", "field": "customer_id", "severity": "error"}],
            },
            "retry": {"max_attempts": 3, "backoff": "exponential", "base_seconds": 5, "cap_seconds": 60},
        },
        "projection": [
            {"source": "customer_id", "name": "customer_id", "logical_type": "utf8_string", "nullable": False},
            {"source": "loaded_at", "name": "loaded_at", "logical_type": "utc_instant", "nullable": False},
        ],
        "interfaces": {
            "raw": "bronze-demo-customers-raw", "source_native": "bronze-demo-customers-source-native",
            "published": "bronze-demo-customers-published", "quarantine": "bronze-demo-customers-quarantine",
            "deletion_evidence": "bronze-demo-customers-deletion-evidence",
        },
    })


# ---------------------------------------------------------------------------- scenarios


def scenario_normal_publication() -> None:
    _heading("normal publication")
    print(
        "A clean append-only CSV delivery lands. Every row clears the declared quality\n"
        "rules, so every row publishes and nothing is quarantined."
    )
    root = _new_estate_root("normal")
    try:
        clock = _MutableClock(FROZEN_NOW)
        set_clock(Clock(clock))
        contract = _postings_contract()
        shared, digest = _activate(root, contract, "local-demo-postings")

        csv_bytes = (
            b"txn_id,status,amount,booked_on,loaded_at\n"
            b"txn-1,settled,100.00,2026-01-01,2026-01-01T01:00:00.000000Z\n"
            b"txn-2,pending,50.25,2026-01-02,2026-01-01T01:00:00.000000Z\n"
            b"txn-3,failed,10.00,2026-01-03,2026-01-01T01:00:00.000000Z\n"
        )
        manifest, payload = _csv_delivery(
            root, "delivery-1", csv_bytes, contract, digest, "2026-01-01T01:00:00.000000Z", "batch-1",
        )
        code, ingested, err = _json_run(
            ["ingest", "file", *shared, "--manifest", str(manifest), "--payload", str(payload)],
        )
        _check("ingest file (received-batch sidecar preserved, landed, validated, published)", code, err)

        code, insp, err = _json_run(["inspect", *shared, "--delivery-id", "delivery-1"])
        _check("inspect delivery-1 (quality evidence)", code, err)
        quality = [i for i in insp["result"]["evidence"]["items"] if i["kind"] == "quality"][0]["validation"]
        accepted, quarantined = int(quality["accepted_count"]), int(quality["quarantined_count"])
        print(f"  accepted rows     : {accepted}")
        print(f"  quarantined rows  : {quarantined}")
        if accepted != 3 or quarantined != 0:
            raise SystemExit(f"FAILED: expected 3 accepted / 0 quarantined, got {accepted} / {quarantined}")

        code, status, err = _json_run(["status", *shared])
        _check("status (operator-visible freshness and progress)", code, err)
        latest = status["result"]["operational_status"]["latest_attempt"]
        print(f"  operational state : {latest['state']}")
        print("  every delivered row published; the quarantine surface stays empty.")
    finally:
        set_clock(None)
        shutil.rmtree(root, ignore_errors=True)


def scenario_acceptance_incomplete_snapshot() -> None:
    _heading("acceptance-incomplete snapshot")
    print(
        "Two signed complete-snapshot deliveries land for the same product. The first is\n"
        "clean and becomes the current snapshot. The second carries every row the source\n"
        "meant to send (it is source-complete) but one row fails the mandatory not-null\n"
        "rule (it is acceptance-incomplete). The contract's all_or_nothing publication mode\n"
        "rejects the whole second delivery, and the first snapshot stays the one a consumer\n"
        "reads -- a rejected delivery never silently blends with the accepted one before it."
    )
    root = _new_estate_root("snapshot")
    try:
        clock = _MutableClock(FROZEN_NOW)
        set_clock(Clock(clock))
        key_id = "demo-snapshot-key-1"
        signing_key, signing_public = generate_ed25519_keypair()
        contract = _customers_snapshot_contract(key_id)
        shared, digest = _activate(root, contract, "local-demo-customers")

        layout = resolve_layout(
            project_dir=root, binding_path=root / "runtime" / "customers.yml", environment="local",
        )
        keys = SqliteKeyResolver(layout.sqlite_path)
        keys.put_verification_key(
            verification_key_record(key_id, signing_public, "2026-01-01T00:00:00.000000Z", ("attest-default",)),
        )
        keys.close()
        _log("Ed25519 verification key registered for signed snapshot attestations")

        clean = b'{"customer_id":"cust-1","effective_at":"2026-01-01T00:00:00.000000Z","loaded_at":"2026-01-01T00:00:00.000000Z"}\n' \
                b'{"customer_id":"cust-2","effective_at":"2026-01-01T00:00:00.000000Z","loaded_at":"2026-01-01T00:00:00.000000Z"}\n' \
                b'{"customer_id":"cust-3","effective_at":"2026-01-01T00:00:00.000000Z","loaded_at":"2026-01-01T00:00:00.000000Z"}\n'
        m1, p1 = _signed_snapshot_delivery(
            root, "snapshot-1", clean, contract, digest, "2026-01-01T00:00:00.000000Z", "batch-1",
            "demo_customer_population", signing_key, key_id,
        )
        code, ingested1, err = _json_run(["ingest", "file", *shared, "--manifest", str(m1), "--payload", str(p1)])
        _check("signed complete snapshot (clean) ingested and published", code, err)

        pointer_after_1 = _current_snapshot_pointer_count(root, contract)
        print(f"  current-snapshot pointer entries after the clean snapshot: {pointer_after_1}")

        incomplete = b'{"customer_id":"cust-1","effective_at":"2026-01-02T00:00:00.000000Z","loaded_at":"2026-01-02T00:00:00.000000Z"}\n' \
                     b'{"customer_id":null,"effective_at":"2026-01-02T00:00:00.000000Z","loaded_at":"2026-01-02T00:00:00.000000Z"}\n' \
                     b'{"customer_id":"cust-3","effective_at":"2026-01-02T00:00:00.000000Z","loaded_at":"2026-01-02T00:00:00.000000Z"}\n'
        clock.now = _instant("2026-01-02T00:00:05.000000Z")
        m2, p2 = _signed_snapshot_delivery(
            root, "snapshot-2", incomplete, contract, digest, "2026-01-02T00:00:00.000000Z", "batch-2",
            "demo_customer_population", signing_key, key_id,
        )
        code, ingested2, err = _json_run(["ingest", "file", *shared, "--manifest", str(m2), "--payload", str(p2)])
        attempt = ingested2["result"]["attempt"]
        print(
            f"  second snapshot delivery: attempt state {attempt['state']!r}, "
            f"reason {attempt['reason_code']!r} -- one row failed the mandatory not-null rule"
        )

        pointer_after_2 = _current_snapshot_pointer_count(root, contract)
        print(f"  current-snapshot pointer entries after the incomplete snapshot: {pointer_after_2}")
        if pointer_after_2 != pointer_after_1:
            raise SystemExit(
                f"FAILED: an acceptance-incomplete snapshot must leave the prior snapshot current: "
                f"{pointer_after_1} -> {pointer_after_2}"
            )

        code, quarantine, err = _json_run(["quarantine", *shared, "--action", "list"])
        _check("quarantine list (the rejected row is locatable)", code, err)
        rejected_second = [
            item for item in quarantine["result"]["evidence"]["items"]
            if item["disposition"]["delivery_id"] == "snapshot-2" and item["disposition"]["status"] == "rejected"
        ]
        print(f"  rows rejected out of the second delivery: {len(rejected_second)}")
        print("  the first snapshot's three rows remain what a reader sees; the second")
        print("  delivery's own rows, accepted and rejected alike, never entered the")
        print("  published surface because the delivery as a whole did not clear the")
        print("  contract's all_or_nothing publication mode.")
    finally:
        set_clock(None)
        shutil.rmtree(root, ignore_errors=True)


def _current_snapshot_pointer_count(root: Path, contract: BronzeProductContract) -> int:
    import duckdb

    db_path = root / "runtime" / "data" / "ergasterion.duckdb"
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        rows = con.execute(
            "SELECT COUNT(*) FROM published_ledger WHERE identity_key = ?",
            (identity_key(contract.logical_identity),),
        ).fetchall()
        return int(rows[0][0])
    finally:
        con.close()


def scenario_backup_restore() -> None:
    _heading("backup and restore")
    print(
        "One clean delivery publishes. A verified local backup then copies the complete\n"
        "local runtime root -- state, raw evidence and the published surface together --\n"
        "to a location outside both the project root and the runtime root it copies. The\n"
        "runtime root is deleted outright and restored from that backup, and the delivery's\n"
        "claim identity, visibility, progress and semantic times are checked unchanged\n"
        "across the deletion and restore."
    )
    root = _new_estate_root("backup")
    backup_dest = None
    try:
        set_clock(Clock(_MutableClock(FROZEN_NOW)))
        contract = _postings_contract()
        shared, digest = _activate(root, contract, "local-demo-postings-backup")

        csv_bytes = (
            b"txn_id,status,amount,booked_on,loaded_at\n"
            b"txn-1,settled,100.00,2026-01-01,2026-01-01T01:00:00.000000Z\n"
        )
        manifest, payload = _csv_delivery(
            root, "delivery-1", csv_bytes, contract, digest, "2026-01-01T01:00:00.000000Z", "batch-1",
        )
        code, ingested, err = _json_run(
            ["ingest", "file", *shared, "--manifest", str(manifest), "--payload", str(payload)],
        )
        _check("ingest file", code, err)

        code, before, err = _json_run(["status", *shared])
        _check("status before backup", code, err)
        before_stream = before["result"]["stream_status"]
        before_claim = {
            "identity": before_stream["logical_identity"],
            "visibility": before_stream["latest_snapshot_visibility"],
            "progress": before_stream["accepted_progress"],
            "committed_at": before_stream["committed_at"],
            "scheduled_boundary_at": before_stream["scheduled_boundary_at"],
        }
        before_revision = before["result"]["operational_status"]["state"]["state_revision"]

        backup_dest = root.parent / f"{root.name}-backup"
        code, created, err = _json_run(
            ["local-backup", *shared, "--action", "create", "--destination", str(backup_dest)],
        )
        _check("local-backup create (verified copy, outside the project and runtime roots)", code, err)
        print(f"  backup written to a temporary location outside the project: {backup_dest.name}")

        runtime_root = root / "runtime" / "data"
        shutil.rmtree(runtime_root)
        print("  the complete local runtime root (state, raw evidence, published surface) was deleted.")

        code, restored, err = _json_run(
            ["local-backup", *shared, "--action", "restore", "--manifest", str(backup_dest / "backup-manifest.json")],
        )
        _check("local-backup restore", code, err)
        restored_revision = restored["result"]["manifest"]["state_revision"]
        if restored_revision != before_revision:
            raise SystemExit(f"FAILED: state_revision changed across restore: {before_revision} -> {restored_revision}")

        code, after, err = _json_run(["status", *shared])
        _check("status after restore", code, err)
        after_stream = after["result"]["stream_status"]
        after_claim = {
            "identity": after_stream["logical_identity"],
            "visibility": after_stream["latest_snapshot_visibility"],
            "progress": after_stream["accepted_progress"],
            "committed_at": after_stream["committed_at"],
            "scheduled_boundary_at": after_stream["scheduled_boundary_at"],
        }
        if after_claim != before_claim:
            raise SystemExit(f"FAILED: claim fields changed across restore:\nbefore={before_claim}\nafter={after_claim}")

        print(f"  state revision unchanged        : {before_revision}")
        print(f"  claim identity unchanged         : {after_claim['identity']['source']}.{after_claim['identity']['table']}")
        print(f"  visibility unchanged              : {after_claim['visibility']}")
        print(f"  accepted progress unchanged       : {after_claim['progress']}")
        print(f"  committed / scheduled times unchanged")
    finally:
        set_clock(None)
        if backup_dest is not None:
            shutil.rmtree(backup_dest, ignore_errors=True)
        shutil.rmtree(root, ignore_errors=True)


SCENARIOS = {
    "normal-publication": scenario_normal_publication,
    "acceptance-incomplete-snapshot": scenario_acceptance_incomplete_snapshot,
    "backup-restore": scenario_backup_restore,
}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--scenario", choices=[*SCENARIOS, "all"], default="all",
        help="Which scenario to run. Defaults to all three, in order.",
    )
    args = parser.parse_args(argv)

    to_run = list(SCENARIOS) if args.scenario == "all" else [args.scenario]
    print("Ergasterion Bronze: account-free, network-free local walkthrough")
    for name in to_run:
        SCENARIOS[name]()
    print("\nAll requested scenarios completed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
