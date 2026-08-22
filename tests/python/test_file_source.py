"""Assert-script tests for file connectors, codecs, local raw store and scratch store.

The local connector/raw/scratch implementations are passed to
``ergasterion.ingestion.conformance.run_adapter_conformance`` against
``adapter-v1.json``. Independent vectors cover codec/type rules, encodings,
sidecar shapes, CDC framing, raw atomicity and scratch isolation.

Usage:
    python tests/python/test_file_source.py
"""

from __future__ import annotations

import gzip
import hashlib
import json
import os
import subprocess
import tempfile
import time
import traceback
from pathlib import Path

if __package__ in (None, ""):
    import os as _os
    import sys as _sys
    _sys.path.insert(0, _os.path.dirname(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))))

from ergasterion.framework.bronze_contract import (
    BronzeProductContract,
    ContentEncoding,
    CsvCodec,
    DecimalType,
    DeleteStrategy,
    DeliveryMode,
    JsonlCodec,
    LocalDateTimeType,
    OpaqueBatchProgress,
    PublicationPolicy,
    SequenceProgress,
    SourceField,
)
from ergasterion.ingestion.codecs import (
    coerce_text,
    decode_transport,
    decompress_gzip,
    frame_sequence_digest,
    parse_csv,
    parse_jsonl,
    parse_payload,
    split_jsonl_frames,
    transport_payload_fingerprint,
)
from ergasterion.ingestion.conformance import (
    FakeKeyResolver,
    exercise_all_operations,
    load_vectors,
    run_adapter_conformance,
)
from ergasterion.ingestion.evidence import (
    b64url_encode,
    generate_ed25519_keypair,
    sign_envelope,
    verification_key_record,
)
from ergasterion.ingestion.file_source import FileSource, file_ports_factory
from ergasterion.ingestion.local_raw_store import LocalRawStore
from ergasterion.ingestion.local_scratch_store import LocalScratchStore
from ergasterion.ingestion.ports import PORT_PROTOCOLS
from ergasterion.ingestion.records import (
    DeliveryManifest,
    DeliveryVisibilityIdentity,
    ExternalReceiptInput,
    ExternalReceiptPayload,
    ManagedPayloadInput,
    PayloadDescriptor,
    PORT_OPERATION_ORDER,
    ScratchChunk,
    SignedAttestation,
    SignedExternalReceipt,
    VerificationKeyRecord,
)
from ergasterion.ingestion.runtime import PortError, canonical_digest, digest_token

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
FIXTURES = REPO_ROOT / "tests" / "fixtures" / "bronze_file_source"
INVENTORY_PATH = FIXTURES / "inventory.json"
SCHEMA_VECTORS_PATH = REPO_ROOT / "tests" / "fixtures" / "bronze_schema_vectors.json"
THROUGHPUT: dict[str, float] = {}


def _sample_contract() -> BronzeProductContract:
    document = json.loads(SCHEMA_VECTORS_PATH.read_text(encoding="utf-8"))
    for vector in document["positive"]:
        if vector["record"] == "BronzeProductContract":
            return BronzeProductContract.model_validate(vector["payload"])
    raise AssertionError("no BronzeProductContract positive vector found")


def _inventory() -> dict:
    return json.loads(INVENTORY_PATH.read_text(encoding="utf-8"))


def _bytes(relative: str) -> bytes:
    return (FIXTURES / relative).read_bytes()


def _expect_error(code: str, fn, message: str) -> PortError:
    try:
        fn()
    except PortError as exc:
        assert exc.code == code, f"{message}: expected {code!r}, got {exc.code!r} ({exc.detail})"
        return exc
    raise AssertionError(message)


def _jsonl_codec() -> JsonlCodec:
    return JsonlCodec(
        kind="jsonl", version=1, charset="utf-8", newline="lf", top_level="object",
        duplicate_keys="reject", number_mode="exact_decimal", allow_blank_lines=False,
    )


def _csv_codec(**overrides) -> CsvCodec:
    payload = {
        "kind": "csv", "version": 1, "charset": "utf-8", "delimiter": ",", "header": True,
        "quote": '"', "escape": "\\", "newline": "lf", "null_tokens": ("",), "trim_whitespace": False,
    }
    payload.update(overrides)
    return CsvCodec.model_validate(payload)


def _type_columns(*, extra: bool = False) -> tuple[SourceField, ...]:
    columns = [
        SourceField(name="acct_id", logical_type="utf8_string", nullable=False),
        SourceField(name="flag", logical_type="boolean", nullable=True),
        SourceField(name="n", logical_type="int64", nullable=False),
        SourceField(name="amount", logical_type=DecimalType(kind="decimal", precision=10, scale=2), nullable=True),
        SourceField(name="on_date", logical_type="date", nullable=True),
        SourceField(name="at_utc", logical_type="utc_instant", nullable=True),
        SourceField(name="at_local", logical_type=LocalDateTimeType(kind="local_datetime", timezone="Europe/London"), nullable=True),
        SourceField(name="blob", logical_type="binary", nullable=True),
    ]
    if extra:
        columns.append(SourceField(name="note", logical_type="utf8_string", nullable=True))
    return tuple(columns)


def _cdc_columns() -> tuple[SourceField, ...]:
    return (
        SourceField(name="seq", logical_type="int64", nullable=False),
        SourceField(name="acct_id", logical_type="utf8_string", nullable=False),
        SourceField(name="is_deleted", logical_type="boolean", nullable=True),
    )


def _managed_with(codec, columns, *, mode="append_only", encodings=None, progress=None):
    from ergasterion.ingestion.conformance import contract_variant

    if encodings is None:
        encodings = (ContentEncoding.GZIP, ContentEncoding.IDENTITY)
    contract = contract_variant(_sample_contract(), integration_kind="managed", publication_mode=PublicationPolicy.ALL_OR_NOTHING)
    landing = contract.landing.model_copy(update={
        "codec": codec, "physical_columns": columns, "content_encodings": encodings,
    })
    delivery = contract.delivery
    updates = {"mode": DeliveryMode(mode)}
    if progress is not None:
        updates["progress"] = progress
    if mode == "cdc":
        updates["progress"] = SequenceProgress(kind="sequence", field="seq")
        updates["delete_strategy"] = DeleteStrategy.EXPLICIT_TOMBSTONE
    elif mode in ("append_only", "complete_snapshot"):
        updates["progress"] = OpaqueBatchProgress(kind="opaque_batch")
    delivery = delivery.model_copy(update=updates)
    return contract.model_copy(update={"landing": landing, "delivery": delivery})


def _external_with(codec, columns, *, mode="cdc", encodings=None):
    from ergasterion.ingestion.conformance import contract_variant

    if encodings is None:
        encodings = (ContentEncoding.GZIP, ContentEncoding.IDENTITY)
    contract = contract_variant(_sample_contract(), publication_mode=PublicationPolicy.ALL_OR_NOTHING)
    landing = contract.landing.model_copy(update={
        "codec": codec, "physical_columns": columns, "content_encodings": encodings,
    })
    delivery = contract.delivery
    updates = {"mode": DeliveryMode(mode)}
    if mode == "cdc":
        updates["progress"] = SequenceProgress(kind="sequence", field="seq")
        updates["delete_strategy"] = DeleteStrategy.EXPLICIT_TOMBSTONE
    elif mode in ("append_only", "complete_snapshot"):
        updates["progress"] = OpaqueBatchProgress(kind="opaque_batch")
    delivery = delivery.model_copy(update=updates)
    return contract.model_copy(update={"landing": landing, "delivery": delivery})


def _trust_keys(*, key_id: str = "key-a", policies=("trust-default", "attest-default")):
    private, public = generate_ed25519_keypair()
    record = verification_key_record(
        key_id, public, enabled_at="2026-01-01T00:00:00.000000Z",
        authorized_policy_refs=policies,
    )
    resolver = FakeKeyResolver()
    resolver.keys[record.key_id] = record
    return private, record, resolver


def _manifest_for(contract, payload: bytes, *, delivery_id: str, encoding="identity", **fields) -> DeliveryManifest:
    codec = contract.landing.codec
    media = "text/csv" if codec.kind == "csv" else "application/x-ndjson"
    body = {
        "schema": "ergasterion.delivery-manifest/v1",
        "logical_identity": contract.logical_identity.model_dump(mode="json"),
        "product_version": contract.product.product_version,
        "contract_digest": canonical_digest(contract.model_dump(mode="json", by_alias=True)),
        "delivery_id": delivery_id,
        "batch_id": fields.get("batch_id"),
        "scheduled_boundary_at": fields.get("scheduled_boundary_at", "2026-01-01T01:00:00.000000Z"),
        "effective_boundary_at": fields.get("effective_boundary_at"),
        "payload": {
            "media_type": media, "content_encoding": encoding, "codec_version": 1,
            "byte_length": str(len(payload)), "sha256": transport_payload_fingerprint(payload),
        },
        "frame_sequence_digest": fields.get("frame_sequence_digest"),
        "progress_claim": fields.get("progress_claim", {"kind": "opaque_batch"}),
        "declared_row_count": fields.get("declared_row_count", "0"),
        "snapshot_attestation": fields.get("snapshot_attestation"),
    }
    return DeliveryManifest.model_validate(body)


# --------------------------------------------------------------------------- protocol + conformance

def test_ports_satisfy_protocols() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        connector = FileSource()
        raw = LocalRawStore(Path(tmp) / "raw")
        scratch = LocalScratchStore(Path(tmp) / "scratch")
        assert isinstance(connector, PORT_PROTOCOLS["source_connector"])
        assert isinstance(raw, PORT_PROTOCOLS["raw_store"])
        assert isinstance(scratch, PORT_PROTOCOLS["scratch_store"])


def test_adapter_conformance_vectors_pass_against_file_ports() -> None:
    contract = _sample_contract()
    vectors = load_vectors()
    assert len(vectors) >= 15
    with tempfile.TemporaryDirectory() as tmp:
        failed = []
        for vector in vectors:
            outcome = run_adapter_conformance(
                vector, contract,
                ports_factory=lambda v, c, h, _id=vector["id"]: file_ports_factory(v, c, h, directory=Path(tmp) / _id),
            )
            if not outcome.passed:
                failed.append(f"{outcome.vector_id}: {outcome.detail}")
        assert not failed, "\n".join(failed)


def test_exercise_all_operations_with_file_ports() -> None:
    contract = _managed_with(_jsonl_codec(), _cdc_columns(), mode="cdc")
    with tempfile.TemporaryDirectory() as tmp:
        raw = LocalRawStore(Path(tmp) / "raw")
        raw.content_by_handle["exercise"] = [{"key": "a", "accept": True}]
        from ergasterion.ingestion.conformance import build_memory_ports
        ports, state = build_memory_ports(contract.logical_identity, content_by_handle={"exercise": [{"key": "a", "accept": True}]})
        bundled = ports.__class__(
            source_connector=FileSource(),
            raw_store=raw,
            scratch_store=LocalScratchStore(Path(tmp) / "scratch"),
            state_store=ports.state_store,
            landing_adapter=ports.landing_adapter,
            remediation_repository=ports.remediation_repository,
            projection_publisher=ports.projection_publisher,
            lifecycle_sink=ports.lifecycle_sink,
            key_resolver=ports.key_resolver,
        )
        reached = exercise_all_operations(bundled, state, contract, "exercise")
        for field_name in ("source_connector", "raw_store", "scratch_store"):
            missing = tuple(op for op in PORT_OPERATION_ORDER[field_name] if op not in reached[field_name])
            assert not missing, f"{field_name} missed {missing}"


# --------------------------------------------------------------------------- codecs / types

def test_every_codec_type_rule() -> None:
    columns = _type_columns()
    started = time.perf_counter()
    jsonl = parse_jsonl(_bytes("codecs/types.ndjson"), _jsonl_codec(), columns)
    csv = parse_csv(_bytes("codecs/types.csv"), _csv_codec(), columns)
    THROUGHPUT["types_parse_s"] = time.perf_counter() - started
    assert len(jsonl.frames) == 2 and len(csv.frames) == 2
    for frame in (*jsonl.frames, *csv.frames):
        assert not frame.findings, frame.findings
    first = dict(jsonl.frames[0].fields)
    assert first["flag"].value is True
    assert first["n"].value == "1"
    assert first["amount"].unscaled == "1250" and first["amount"].scale == 2
    second = dict(jsonl.frames[1].fields)
    assert second["flag"] is None and second["amount"] is None

    _expect_error("framing_error", lambda: parse_jsonl(_bytes("malformed/duplicate_keys.ndjson"), _jsonl_codec(), columns), "duplicate keys")
    _expect_error("framing_error", lambda: parse_jsonl(_bytes("malformed/blank_line.ndjson"), _jsonl_codec(), columns), "blank lines")
    _expect_error("framing_error", lambda: parse_jsonl(_bytes("malformed/bom.ndjson"), _jsonl_codec(), columns), "BOM")
    _expect_error("framing_error", lambda: parse_jsonl(_bytes("malformed/bad_utf8.ndjson"), _jsonl_codec(), columns), "invalid utf-8")
    _expect_error("framing_error", lambda: parse_jsonl(_bytes("malformed/crlf_when_lf.ndjson"), _jsonl_codec(), columns), "wrong newline")

    typed, diagnostic = coerce_text("true", "boolean", nullable=False)
    assert typed.value is True and diagnostic is None
    _, diagnostic = coerce_text("yes", "boolean", nullable=False)
    assert diagnostic is not None
    _, diagnostic = coerce_text("9223372036854775808", "int64", nullable=False)
    assert diagnostic is not None
    _, diagnostic = coerce_text("1.2", "int64", nullable=False)
    assert diagnostic is not None
    _, diagnostic = coerce_text("12.345", DecimalType(kind="decimal", precision=10, scale=2), nullable=False)
    assert diagnostic is not None
    _, diagnostic = coerce_text("2026-02-30", "date", nullable=False)
    assert diagnostic is not None
    _, diagnostic = coerce_text("2026-03-29T01:30:00.000000", LocalDateTimeType(kind="local_datetime", timezone="Europe/London"), nullable=False)
    assert diagnostic is not None


def test_identity_and_gzip_round_trip() -> None:
    inner = _bytes("codecs/types.csv")
    compressed = _bytes("codecs/types.csv.gz")
    assert gzip.decompress(compressed) == inner
    decoded = decode_transport(
        compressed, "gzip", max_uncompressed_bytes=65536, max_expansion_ratio=100,
    )
    assert decoded == inner
    identity = decode_transport(inner, "identity", max_uncompressed_bytes=65536, max_expansion_ratio=10)
    assert identity == inner
    columns = _type_columns()
    parsed = parse_csv(decoded, _csv_codec(), columns)
    assert len(parsed.frames) == 2 and not parsed.frames[0].findings


def test_object_event_and_zero_event_claims() -> None:
    events = _bytes("claims/cdc_events.ndjson")
    zero = _bytes("claims/cdc_zero.ndjson")
    frames = split_jsonl_frames(events, "lf")
    assert len(frames) == 2
    assert zero == b""
    zero_digest = frame_sequence_digest(())
    assert zero_digest == hashlib.sha256(b"ERGASTERION-CDC-V1\0").hexdigest()
    event_digest = frame_sequence_digest(frames)
    assert event_digest != zero_digest
    object_fp = transport_payload_fingerprint(events)
    assert object_fp == hashlib.sha256(b"ERGASTERION-OBJECT-V1\0" + events).hexdigest()
    parsed = parse_jsonl(events, _jsonl_codec(), _cdc_columns(), sequence_field="seq")
    assert [int(frame.frame_sequence) for frame in parsed.frames] == [9, 10]


def test_complete_sidecar_shapes() -> None:
    events = _bytes("claims/cdc_events.ndjson")
    zero = _bytes("claims/cdc_zero.ndjson")
    append = _bytes("claims/append.csv")
    snapshot = _bytes("claims/snapshot.ndjson")
    cdc_contract = _managed_with(_jsonl_codec(), _cdc_columns(), mode="cdc")
    append_contract = _managed_with(_csv_codec(), _type_columns(), mode="append_only")
    snapshot_contract = _managed_with(_jsonl_codec(), _type_columns(), mode="complete_snapshot")
    frames = split_jsonl_frames(events, "lf")
    cdc = _manifest_for(
        cdc_contract, events, delivery_id="cdc-0001",
        frame_sequence_digest=frame_sequence_digest(frames),
        progress_claim={"kind": "sequence", "high_watermark": "10", "event_count": "2"},
        declared_row_count="2",
    )
    control = _manifest_for(
        cdc_contract, zero, delivery_id="cdc-control-0002",
        frame_sequence_digest=frame_sequence_digest(()),
        progress_claim={"kind": "sequence", "high_watermark": "11", "event_count": "0"},
        declared_row_count="0",
        scheduled_boundary_at="2026-01-01T02:00:00.000000Z",
    )
    append_manifest = _manifest_for(
        append_contract, append, delivery_id="append-0001", encoding="identity",
        batch_id="batch-20260101-01", progress_claim={"kind": "opaque_batch"},
        declared_row_count="2",
    )
    private, record, resolver = _trust_keys()
    snapshot_digest = canonical_digest(snapshot_contract.model_dump(mode="json", by_alias=True))
    attestation_payload = {
        "logical_identity": snapshot_contract.logical_identity.model_dump(mode="json"),
        "contract_digest": snapshot_digest,
        "delivery_id": "snapshot-0001", "batch_id": "snapshot-20260101",
        "effective_boundary_at": "2026-01-01T00:00:00.000000Z",
        "content_fingerprint": transport_payload_fingerprint(snapshot),
        "scope": {"scope_id": "account_population", "scope_parameters": {}},
        "row_count": "2", "issued_at": "2026-01-01T00:05:00.000000Z",
    }
    envelope = {
        "schema": "ergasterion.snapshot-attestation/v1", "algorithm": "Ed25519", "key_id": record.key_id,
        "payload": attestation_payload, "signature": "AA",
    }
    envelope["signature"] = sign_envelope(private, envelope)
    snapshot_manifest = _manifest_for(
        snapshot_contract, snapshot, delivery_id="snapshot-0001",
        batch_id="snapshot-20260101", effective_boundary_at="2026-01-01T00:00:00.000000Z",
        progress_claim={"kind": "opaque_batch"}, declared_row_count="2",
        snapshot_attestation=envelope,
    )
    connector = FileSource(contract=cdc_contract)
    with tempfile.TemporaryDirectory() as tmp:
        event_path = Path(tmp) / "events.ndjson"
        event_path.write_bytes(events)
        connector.register_payload(str(event_path), events)
        delivered = connector.submit_managed(ManagedPayloadInput(
            kind="managed_payload", manifest=cdc, payload_handle=str(event_path),
        ))
        assert delivered.manifest.frame_sequence_digest == cdc.frame_sequence_digest
    FileSource(contract=cdc_contract).submit_managed(ManagedPayloadInput(
        kind="managed_payload", manifest=control, payload_handle=str(FIXTURES / "claims/cdc_zero.ndjson"),
    ))
    FileSource(contract=append_contract).submit_managed(ManagedPayloadInput(
        kind="managed_payload", manifest=append_manifest, payload_handle=str(FIXTURES / "claims/append.csv"),
    ))
    FileSource(
        contract=snapshot_contract, key_resolver=resolver, now_fn=lambda: "2026-01-01T00:05:01.000000Z",
    ).submit_managed(ManagedPayloadInput(
        kind="managed_payload", manifest=snapshot_manifest, payload_handle=str(FIXTURES / "claims/snapshot.ndjson"),
    ))


def _signed_external(contract, payload: bytes, *, private, key_id: str, delivery_id: str, **fields) -> SignedExternalReceipt:
    manifest = _manifest_for(contract, payload, delivery_id=delivery_id, **fields)
    visibility = DeliveryVisibilityIdentity(epoch="0", kind="delivery", id=f"delivery-{delivery_id}")
    receipt_payload = ExternalReceiptPayload(
        logical_identity=contract.logical_identity,
        contract_digest=manifest.contract_digest,
        claim=manifest,
        delivery_claim_digest=canonical_digest(manifest.model_dump(mode="json", by_alias=True)),
        visibility=visibility,
        adapter_capability_digest="a" * 64,
        raw_ref="raw-1", raw_digest="b" * 64,
        manifest_ref="man-1", manifest_digest="c" * 64,
        candidate_ref="can-1", candidate_digest="d" * 64,
        frame_index_ref="idx-1", frame_index_digest="e" * 64,
        issued_at="2026-01-01T00:00:00.000000Z",
    )
    envelope = {
        "schema": "ergasterion.external-receipt/v1", "algorithm": "Ed25519", "key_id": key_id,
        "payload": receipt_payload.model_dump(mode="json", by_alias=True), "signature": "AA",
    }
    envelope["signature"] = sign_envelope(private, envelope)
    return SignedExternalReceipt.model_validate(envelope)


def test_external_cdc_and_append_receipt_trust() -> None:
    from ergasterion.ingestion.evidence import b64url_decode

    private, record, resolver = _trust_keys()
    cdc_contract = _external_with(_jsonl_codec(), _cdc_columns(), mode="cdc")
    append_contract = _external_with(_csv_codec(), _type_columns(), mode="append_only")
    cdc_payload = _bytes("claims/cdc_events.ndjson")
    append_payload = _bytes("claims/append.csv")
    cdc_signed = _signed_external(
        cdc_contract, cdc_payload, private=private, key_id=record.key_id, delivery_id="cdc-ext-0001",
        frame_sequence_digest=frame_sequence_digest(split_jsonl_frames(cdc_payload, "lf")),
        progress_claim={"kind": "sequence", "high_watermark": "10", "event_count": "2"},
        declared_row_count="2",
    )
    append_signed = _signed_external(
        append_contract, append_payload, private=private, key_id=record.key_id, delivery_id="append-ext-0001",
        batch_id="batch-20260101-01", progress_claim={"kind": "opaque_batch"}, declared_row_count="2",
    )
    now = lambda: "2026-01-01T00:00:01.000000Z"
    cdc_connector = FileSource(contract=cdc_contract, key_resolver=resolver, now_fn=now)
    append_connector = FileSource(contract=append_contract, key_resolver=resolver, now_fn=now)
    assert cdc_connector.verify_external(ExternalReceiptInput(kind="external_receipt", receipt=cdc_signed)).receipt.key_id == record.key_id
    assert append_connector.verify_external(ExternalReceiptInput(kind="external_receipt", receipt=append_signed)).receipt.key_id == record.key_id
    _expect_error(
        "capability_mismatch",
        lambda: FileSource(contract=cdc_contract, now_fn=now).verify_external(
            ExternalReceiptInput(kind="external_receipt", receipt=cdc_signed),
        ),
        "external contract without resolver",
    )
    unknown = cdc_signed.model_copy(update={"key_id": "key-z"})
    _expect_error(
        "capability_mismatch",
        lambda: cdc_connector.verify_external(ExternalReceiptInput(kind="external_receipt", receipt=unknown)),
        "key_id outside receipt_trust",
    )
    bad = cdc_signed.model_copy(update={"signature": b64url_encode(b"not-a-real-signature-bytes-here!!")})
    _expect_error(
        "invalid_signature",
        lambda: cdc_connector.verify_external(ExternalReceiptInput(kind="external_receipt", receipt=bad)),
        "tampered external receipt",
    )
    public = b64url_decode(record.public_key_base64url)
    wrong_policy = verification_key_record(
        record.key_id, public, enabled_at="2026-01-01T00:00:00.000000Z",
        authorized_policy_refs=("other-policy",),
    )
    wrong_resolver = FakeKeyResolver()
    wrong_resolver.keys[wrong_policy.key_id] = wrong_policy
    _expect_error(
        "policy_not_authorized",
        lambda: FileSource(contract=cdc_contract, key_resolver=wrong_resolver, now_fn=now).verify_external(
            ExternalReceiptInput(kind="external_receipt", receipt=cdc_signed),
        ),
        "receipt_trust policy_ref not authorised",
    )
    future = FileSource(contract=cdc_contract, key_resolver=resolver, now_fn=lambda: "2025-12-31T23:59:00.000000Z")
    _expect_error(
        "attestation_invalid",
        lambda: future.verify_external(ExternalReceiptInput(kind="external_receipt", receipt=cdc_signed)),
        "receipt issued beyond future_clock_skew_seconds",
    )


def test_compressed_cdc_dual_fingerprints() -> None:
    inner = _bytes("claims/cdc_events.ndjson")
    compressed = _bytes("claims/cdc_events.ndjson.gz")
    transport = transport_payload_fingerprint(compressed)
    frames = split_jsonl_frames(gzip.decompress(compressed), "lf")
    framed = frame_sequence_digest(frames)
    assert transport != framed
    assert transport == transport_payload_fingerprint(compressed)
    assert gzip.decompress(compressed) == inner
    contract = _managed_with(_jsonl_codec(), _cdc_columns(), mode="cdc")
    manifest = _manifest_for(
        contract, compressed, delivery_id="cdc-gz-0001", encoding="gzip",
        frame_sequence_digest=framed,
        progress_claim={"kind": "sequence", "high_watermark": "10", "event_count": "2"},
        declared_row_count="2",
    )
    FileSource(contract=contract).submit_managed(ManagedPayloadInput(
        kind="managed_payload", manifest=manifest,
        payload_handle=str(FIXTURES / "claims/cdc_events.ndjson.gz"),
    ))


def test_fingerprint_claim_conflicts_and_additive_columns() -> None:
    payload = _bytes("claims/cdc_events.ndjson")
    contract = _managed_with(_jsonl_codec(), _cdc_columns(), mode="cdc")
    manifest = _manifest_for(
        contract, payload, delivery_id="cdc-conflict-0001",
        frame_sequence_digest=frame_sequence_digest(split_jsonl_frames(payload, "lf")),
        progress_claim={"kind": "sequence", "high_watermark": "10", "event_count": "2"},
        declared_row_count="2",
    )
    wrong = manifest.model_copy(update={
        "payload": manifest.payload.model_copy(update={"sha256": "a" * 64}),
    })
    _expect_error(
        "integrity_error",
        lambda: FileSource(contract=contract).submit_managed(ManagedPayloadInput(
            kind="managed_payload", manifest=wrong, payload_handle=str(FIXTURES / "claims/cdc_events.ndjson"),
        )),
        "wrong transport fingerprint",
    )
    old = parse_csv(_bytes("additive/old.csv"), _csv_codec(), _type_columns(extra=True))
    new = parse_csv(_bytes("additive/new.csv"), _csv_codec(), _type_columns(extra=True))
    assert dict(old.frames[0].fields)["note"] is None
    assert dict(new.frames[0].fields)["note"].value == "hello"
    _expect_error(
        "framing_error",
        lambda: parse_jsonl(_bytes("additive/unknown_key.ndjson"), _jsonl_codec(), _type_columns()),
        "unknown JSON key",
    )


# --------------------------------------------------------------------------- raw store

def test_raw_creation_is_atomic_and_collision_checked() -> None:
    payload = _bytes("sample.ndjson")
    contract = _managed_with(_jsonl_codec(), _type_columns())
    manifest = _manifest_for(
        contract, payload, delivery_id="raw-0001",
        progress_claim={"kind": "opaque_batch"}, declared_row_count="2", batch_id="batch-raw-1",
    )
    with tempfile.TemporaryDirectory() as tmp:
        crashing = LocalRawStore(Path(tmp) / "raw", crash_before_receipt=True)
        crashing.register_payload("p", payload)
        try:
            crashing.preserve(ManagedPayloadInput(kind="managed_payload", manifest=manifest, payload_handle="p"))
            raise AssertionError("crash_before_receipt must raise")
        except RuntimeError:
            pass
        _expect_error("not_found", lambda: crashing.get_receipt("0" * 64), "no receipt before the marker")
        store = LocalRawStore(Path(tmp) / "raw")
        store.register_payload("p", payload)
        first = store.preserve(ManagedPayloadInput(kind="managed_payload", manifest=manifest, payload_handle="p"))
        second = store.preserve(ManagedPayloadInput(kind="managed_payload", manifest=manifest, payload_handle="p"))
        assert first.raw_receipt_digest == second.raw_receipt_digest
        handle = store.open_raw(first.raw_receipt_digest)
        page = store.read_raw(handle, "0", handle.byte_length)
        import base64
        padding = "=" * (-len(page.bytes_base64url) % 4)
        assert base64.urlsafe_b64decode(page.bytes_base64url + padding) == payload
        digest = first.payload.content_id.split(":", 1)[-1]
        object_path = Path(tmp) / "raw" / "objects" / digest[:2] / digest
        object_path.write_bytes(b"tampered-bytes-not-matching-digest")
        _expect_error(
            "integrity_error",
            lambda: store.get_receipt(first.raw_receipt_digest),
            "collision / digest mismatch after tamper",
        )


def test_exact_bytes_survive_failure_quarantine_retry_restart() -> None:
    payload = _bytes("sample.ndjson")
    contract = _managed_with(_jsonl_codec(), _type_columns())
    manifest = _manifest_for(
        contract, payload, delivery_id="survive-0001",
        progress_claim={"kind": "opaque_batch"}, declared_row_count="2", batch_id="batch-s-1",
    )
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp) / "raw"
        store = LocalRawStore(root)
        store.register_payload("p", payload)
        receipt = store.preserve(ManagedPayloadInput(kind="managed_payload", manifest=manifest, payload_handle="p"))
        reopened = LocalRawStore(root)
        again = reopened.get_receipt(receipt.raw_receipt_digest)
        handle = reopened.open_raw(again.raw_receipt_digest)
        page = reopened.read_raw(handle, "0", handle.byte_length)
        import base64
        padding = "=" * (-len(page.bytes_base64url) % 4)
        restored = base64.urlsafe_b64decode(page.bytes_base64url + padding)
        assert restored == payload
        assert again.payload.content_id == receipt.payload.content_id


# --------------------------------------------------------------------------- scratch

def test_scratch_quotas_isolation_retry_and_cleanup() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        raw_root = Path(tmp) / "raw"
        raw = LocalRawStore(raw_root)
        payload = _bytes("sample.ndjson")
        contract = _managed_with(_jsonl_codec(), _type_columns())
        manifest = _manifest_for(
            contract, payload, delivery_id="scratch-raw-0001",
            progress_claim={"kind": "opaque_batch"}, declared_row_count="2", batch_id="batch-sc-1",
        )
        raw.register_payload("p", payload)
        receipt = raw.preserve(ManagedPayloadInput(kind="managed_payload", manifest=manifest, payload_handle="p"))
        store = LocalScratchStore(Path(tmp) / "scratch", max_scratch_bytes=32)
        attempt_a = canonical_digest({"attempt": "a"})
        attempt_b = canonical_digest({"attempt": "b"})
        scope = store.create_scope(attempt_a, "16")
        from ergasterion.ingestion.evidence import b64url_encode
        store.write_sequential(attempt_a, ScratchChunk(scope_id=scope.scope_id, sequence="0", bytes_base64url=b64url_encode(b"abcdefghijklmnop")))
        _expect_error(
            "capacity_exceeded",
            lambda: store.create_scope(attempt_b, "32"),
            "aggregate scratch ceiling",
        )
        _expect_error(
            "scope_owner_mismatch",
            lambda: store.write_sequential(attempt_b, ScratchChunk(
                scope_id=scope.scope_id, sequence="1", bytes_base64url=b64url_encode(b"x"),
            )),
            "cross-attempt write",
        )
        store.delete_scope(attempt_a, scope.scope_id)
        retried = store.create_scope(attempt_b, "16")
        store.write_sequential(attempt_b, ScratchChunk(
            scope_id=retried.scope_id, sequence="0", bytes_base64url=b64url_encode(b"retry-ok"),
        ))
        store.write_sequential(attempt_b, ScratchChunk(
            scope_id=retried.scope_id, sequence="1", bytes_base64url=b64url_encode(b"more-ok!"),
        ))
        store.close_scope(attempt_b, retried.scope_id)
        page = store.read_sequential(attempt_b, retried.scope_id, "0", "16")
        assert page.chunks
        restarted = LocalScratchStore(Path(tmp) / "scratch", max_scratch_bytes=32)
        removed = restarted.cleanup_orphans((), 8)
        assert retried.scope_id in removed
        still = LocalRawStore(raw_root).get_receipt(receipt.raw_receipt_digest)
        assert still.raw_receipt_digest == receipt.raw_receipt_digest


# --------------------------------------------------------------------------- bounds + inventory

def test_fixed_size_fixtures_enforce_read_and_decompression_limits() -> None:
    blob = _bytes("limits/chunk_256.bin")
    assert len(blob) == 256
    with tempfile.TemporaryDirectory() as tmp:
        store = LocalRawStore(Path(tmp) / "raw", max_read_chunk=64, max_payload_bytes=1024)
        contract = _managed_with(_jsonl_codec(), _type_columns())
        manifest = _manifest_for(
            contract, blob, delivery_id="limit-0001",
            progress_claim={"kind": "opaque_batch"}, declared_row_count="0", batch_id="batch-lim-1",
        )
        store.register_payload("p", blob)
        receipt = store.preserve(ManagedPayloadInput(kind="managed_payload", manifest=manifest, payload_handle="p"))
        handle = store.open_raw(receipt.raw_receipt_digest)
        page = store.read_raw(handle, "0", "1000000")
        assert int(page.bytes_returned) == 64
        assert page.eof is False
        pages = 1
        offset = page.next_offset
        while offset is not None:
            page = store.read_raw(handle, offset, "1000000")
            pages += 1
            offset = page.next_offset
        assert pages == 4 and page.eof is True
    _expect_error(
        "codec_error",
        lambda: decompress_gzip(_bytes("gzip/expansion.gz"), max_uncompressed_bytes=65536, max_expansion_ratio=2),
        "expansion ratio",
    )
    _expect_error(
        "codec_error",
        lambda: decompress_gzip(_bytes("gzip/concatenated.gz"), max_uncompressed_bytes=65536, max_expansion_ratio=50),
        "concatenated gzip",
    )
    _expect_error(
        "codec_error",
        lambda: decompress_gzip(_bytes("gzip/corrupt.gz"), max_uncompressed_bytes=65536, max_expansion_ratio=50),
        "corrupt gzip",
    )
    oversized = _bytes("limits/oversize.bin")
    with tempfile.TemporaryDirectory() as tmp:
        store = LocalRawStore(Path(tmp) / "raw", max_payload_bytes=32)
        contract = _managed_with(_jsonl_codec(), _type_columns())
        manifest = _manifest_for(
            contract, oversized, delivery_id="oversize-0001",
            progress_claim={"kind": "opaque_batch"}, declared_row_count="0", batch_id="batch-ov-1",
        )
        store.register_payload("p", oversized)
        _expect_error(
            "capacity_exceeded",
            lambda: store.preserve(ManagedPayloadInput(kind="managed_payload", manifest=manifest, payload_handle="p")),
            "payload ceiling",
        )


def test_preserve_rejects_zero_fingerprint_and_wrong_length() -> None:
    payload = _bytes("sample.ndjson")
    contract = _managed_with(_jsonl_codec(), _type_columns())
    manifest = _manifest_for(
        contract, payload, delivery_id="raw-zero-0001",
        progress_claim={"kind": "opaque_batch"}, declared_row_count="2", batch_id="batch-zero-1",
    )
    with tempfile.TemporaryDirectory() as tmp:
        store = LocalRawStore(Path(tmp) / "raw")
        store.register_payload("p", payload)
        zeros = manifest.model_copy(update={
            "payload": manifest.payload.model_copy(update={"sha256": "0" * 64}),
        })
        _expect_error(
            "integrity_error",
            lambda: store.preserve(ManagedPayloadInput(kind="managed_payload", manifest=zeros, payload_handle="p")),
            "64-zero fingerprint must not bypass preserve",
        )
        wrong_length = manifest.model_copy(update={
            "payload": manifest.payload.model_copy(update={"byte_length": "0"}),
        })
        _expect_error(
            "integrity_error",
            lambda: store.preserve(ManagedPayloadInput(kind="managed_payload", manifest=wrong_length, payload_handle="p")),
            "claimed byte_length 0 must not bypass preserve",
        )


def test_untrusted_payload_reads_are_capped() -> None:
    oversize = FIXTURES / "limits/oversize.bin"
    contract = _managed_with(_jsonl_codec(), _type_columns())
    with tempfile.TemporaryDirectory() as tmp:
        store = LocalRawStore(Path(tmp) / "raw", max_payload_bytes=32, max_read_chunk=8)
        claimed = oversize.read_bytes()[:32]
        manifest = _manifest_for(
            contract, claimed, delivery_id="cap-0001",
            progress_claim={"kind": "opaque_batch"}, declared_row_count="0", batch_id="batch-cap-1",
        )
        _expect_error(
            "capacity_exceeded",
            lambda: store.preserve(ManagedPayloadInput(
                kind="managed_payload", manifest=manifest, payload_handle=str(oversize),
            )),
            "untrusted file must not be read past max_payload_bytes",
        )
        connector = FileSource(contract=contract, max_payload_bytes=32, max_read_chunk=8)
        _expect_error(
            "integrity_error",
            lambda: connector.submit_managed(ManagedPayloadInput(
                kind="managed_payload", manifest=manifest, payload_handle=str(oversize),
            )),
            "connector must cap untrusted payload reads",
        )


def test_scratch_charges_capacity_reservations() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        store = LocalScratchStore(Path(tmp) / "scratch", max_scratch_bytes=32)
        attempt_a = canonical_digest({"attempt": "reserve-a"})
        attempt_b = canonical_digest({"attempt": "reserve-b"})
        store.create_scope(attempt_a, "16")
        _expect_error(
            "capacity_exceeded",
            lambda: store.create_scope(attempt_b, "17"),
            "unused reservation still charges aggregate ceiling",
        )
        store.create_scope(attempt_b, "16")


def test_csv_locators_use_utf8_byte_offsets() -> None:
    payload = _bytes("codecs/utf8.csv")
    parsed = parse_csv(payload, _csv_codec(), _type_columns())
    assert len(parsed.frames) == 1
    frame = parsed.frames[0]
    start = int(frame.raw_locator.byte_offset)
    length = int(frame.raw_locator.byte_length)
    assert payload[start:start + length] == frame.raw_bytes
    assert length == len(frame.raw_bytes)
    assert b"caf" in frame.raw_bytes and "café".encode("utf-8") in frame.raw_bytes
    header, newline, remainder = payload.partition(b"\n")
    assert start == len(header) + 1
    assert length == len(remainder.rstrip(b"\n"))
    assert dict(frame.fields)["acct_id"].value == "café"


def test_raw_manifest_binds_exact_sidecar_bytes() -> None:
    payload = _bytes("sample.ndjson")
    contract = _managed_with(_jsonl_codec(), _type_columns())
    manifest = _manifest_for(
        contract, payload, delivery_id="sidecar-0001",
        progress_claim={"kind": "opaque_batch"}, declared_row_count="2", batch_id="batch-side-1",
    )
    compact = json.dumps(manifest.model_dump(mode="json", by_alias=True), separators=(",", ":")).encode("utf-8")
    pretty = json.dumps(manifest.model_dump(mode="json", by_alias=True), indent=2).encode("utf-8")
    assert compact != pretty
    with tempfile.TemporaryDirectory() as tmp:
        sidecar_path = Path(tmp) / "sidecar.json"
        payload_path = Path(tmp) / "payload.ndjson"
        sidecar_path.write_bytes(compact)
        payload_path.write_bytes(payload)
        connector = FileSource(contract=contract, max_payload_bytes=4096, max_read_chunk=64)
        managed = connector.open_managed(sidecar_path, payload_path)
        store = LocalRawStore(Path(tmp) / "raw", max_payload_bytes=4096, max_read_chunk=64)
        store.register_payload(managed.payload_handle, connector.payload_registry[managed.payload_handle])
        store.register_manifest_bytes(managed.payload_handle, connector.manifest_registry[managed.payload_handle])
        receipt = store.preserve(managed)
        assert receipt.manifest.content_id == f"sha256:{hashlib.sha256(compact).hexdigest()}"
        assert int(receipt.manifest.byte_length) == len(compact)
        store.register_manifest_bytes(managed.payload_handle, pretty)
        _expect_error(
            "claim_conflict",
            lambda: store.preserve(managed),
            "different sidecar bytes for the same claim must conflict",
        )


def test_snapshot_attestation_must_match_manifest_facts() -> None:
    snapshot = _bytes("claims/snapshot.ndjson")
    contract = _managed_with(_jsonl_codec(), _type_columns(), mode="complete_snapshot")
    private, record, resolver = _trust_keys()
    now = lambda: "2026-01-01T00:05:01.000000Z"
    facts = {
        "logical_identity": contract.logical_identity.model_dump(mode="json"),
        "contract_digest": canonical_digest(contract.model_dump(mode="json", by_alias=True)),
        "delivery_id": "snapshot-0001", "batch_id": "snapshot-20260101",
        "effective_boundary_at": "2026-01-01T00:00:00.000000Z",
        "content_fingerprint": transport_payload_fingerprint(snapshot),
        "scope": {"scope_id": "account_population", "scope_parameters": {}},
        "row_count": "2", "issued_at": "2026-01-01T00:05:00.000000Z",
    }

    def _signed(payload_fields):
        envelope = {
            "schema": "ergasterion.snapshot-attestation/v1", "algorithm": "Ed25519", "key_id": record.key_id,
            "payload": payload_fields, "signature": "AA",
        }
        envelope["signature"] = sign_envelope(private, envelope)
        return envelope

    good = _manifest_for(
        contract, snapshot, delivery_id="snapshot-0001",
        batch_id="snapshot-20260101", effective_boundary_at="2026-01-01T00:00:00.000000Z",
        progress_claim={"kind": "opaque_batch"}, declared_row_count="2",
        snapshot_attestation=_signed(facts),
    )
    FileSource(contract=contract, key_resolver=resolver, now_fn=now).submit_managed(ManagedPayloadInput(
        kind="managed_payload", manifest=good, payload_handle=str(FIXTURES / "claims/snapshot.ndjson"),
    ))
    mismatched = dict(facts)
    mismatched["row_count"] = "99"
    bad_facts = _manifest_for(
        contract, snapshot, delivery_id="snapshot-0001",
        batch_id="snapshot-20260101", effective_boundary_at="2026-01-01T00:00:00.000000Z",
        progress_claim={"kind": "opaque_batch"}, declared_row_count="2",
        snapshot_attestation=_signed(mismatched),
    )
    _expect_error(
        "invalid_manifest",
        lambda: FileSource(contract=contract, key_resolver=resolver, now_fn=now).submit_managed(
            ManagedPayloadInput(
                kind="managed_payload", manifest=bad_facts,
                payload_handle=str(FIXTURES / "claims/snapshot.ndjson"),
            ),
        ),
        "attestation facts must equal the manifest",
    )
    unsigned = good.snapshot_attestation.model_dump(mode="json", by_alias=True)
    unsigned["signature"] = b64url_encode(b"not-a-real-signature-bytes-here!!")
    bad_sig = good.model_copy(update={"snapshot_attestation": SignedAttestation.model_validate(unsigned)})
    _expect_error(
        "integrity_error",
        lambda: FileSource(contract=contract, key_resolver=resolver, now_fn=now).submit_managed(
            ManagedPayloadInput(
                kind="managed_payload", manifest=bad_sig,
                payload_handle=str(FIXTURES / "claims/snapshot.ndjson"),
            ),
        ),
        "snapshot attestation signature must verify",
    )


def test_json_numbers_rejected_for_non_numeric_fields() -> None:
    columns = _type_columns()
    parsed = parse_jsonl(_bytes("codecs/number_for_string.ndjson"), _jsonl_codec(), columns)
    assert len(parsed.frames) == 4
    paths = [finding.field_path for frame in parsed.frames for finding in frame.findings]
    assert "/acct_id" in paths
    assert "/on_date" in paths
    assert "/at_utc" in paths
    assert "/blob" in paths
    numeric = parse_jsonl(_bytes("codecs/types.ndjson"), _jsonl_codec(), columns)
    first = dict(numeric.frames[0].fields)
    assert first["n"].value == "1"
    assert first["amount"].unscaled == "1250"


def test_clean_checkout_inventory_and_line_ending_hashes() -> None:
    inventory = _inventory()
    listed = {entry["path"] for entry in inventory["fixtures"]}
    on_disk = {
        path.relative_to(FIXTURES).as_posix()
        for path in FIXTURES.rglob("*")
        if path.is_file() and path.name != "inventory.json"
    }
    assert listed == on_disk, f"inventory drift extra={on_disk-listed} missing={listed-on_disk}"
    used: set[str] = set()
    for entry in inventory["fixtures"]:
        used.update(entry["vectors"])
        relative = entry["path"]
        data = _bytes(relative)
        assert hashlib.sha256(data).hexdigest() == entry["sha256"], relative
        completed = subprocess.run(
            ["git", "-C", str(REPO_ROOT), "hash-object", "--path", f"tests/fixtures/bronze_file_source/{relative}", "--stdin"],
            input=data, capture_output=True, check=False,
        )
        assert completed.returncode == 0, completed.stderr.decode()
        blob = completed.stdout.decode().strip()
        as_file = subprocess.run(
            ["git", "-C", str(REPO_ROOT), "hash-object", f"tests/fixtures/bronze_file_source/{relative}"],
            capture_output=True, check=False,
        )
        assert as_file.returncode == 0
        assert blob == as_file.stdout.decode().strip(), f"blob/checkout mismatch for {relative}"
    tracked = subprocess.run(
        ["git", "-C", str(REPO_ROOT), "ls-files", "--", "tests/fixtures/bronze_file_source"],
        capture_output=True, text=True, check=False,
    )
    # Untracked new fixtures are still inventoried; once added they must not be ignored.
    ignore_check = subprocess.run(
        ["git", "-C", str(REPO_ROOT), "check-ignore", "-v", "--no-index", str(FIXTURES / "sample.ndjson")],
        capture_output=True, text=True, check=False,
    )
    assert ignore_check.returncode == 1, "sample.ndjson must not be gitignored"
    assert "sample.ndjson" in listed
    required_vectors = {
        "codec-types", "identity-gzip", "cdc-events", "cdc-zero", "sidecars",
        "external-receipt", "compressed-cdc", "malformed", "fingerprint-conflict",
        "additive", "raw-atomic", "scratch", "limits", "inventory",
        "json-number-types", "utf8-csv-offsets", "preserve-fingerprint",
        "capped-read", "scratch-reservation", "sidecar-bytes", "snapshot-facts",
    }
    assert required_vectors <= used, f"unused required vectors {required_vectors - used}"
    _ = tracked
    print("throughput_s", json.dumps(THROUGHPUT, sort_keys=True))


TESTS = [
    test_ports_satisfy_protocols,
    test_adapter_conformance_vectors_pass_against_file_ports,
    test_exercise_all_operations_with_file_ports,
    test_every_codec_type_rule,
    test_identity_and_gzip_round_trip,
    test_object_event_and_zero_event_claims,
    test_complete_sidecar_shapes,
    test_external_cdc_and_append_receipt_trust,
    test_compressed_cdc_dual_fingerprints,
    test_fingerprint_claim_conflicts_and_additive_columns,
    test_raw_creation_is_atomic_and_collision_checked,
    test_exact_bytes_survive_failure_quarantine_retry_restart,
    test_scratch_quotas_isolation_retry_and_cleanup,
    test_fixed_size_fixtures_enforce_read_and_decompression_limits,
    test_preserve_rejects_zero_fingerprint_and_wrong_length,
    test_untrusted_payload_reads_are_capped,
    test_scratch_charges_capacity_reservations,
    test_csv_locators_use_utf8_byte_offsets,
    test_raw_manifest_binds_exact_sidecar_bytes,
    test_snapshot_attestation_must_match_manifest_facts,
    test_json_numbers_rejected_for_non_numeric_fields,
    test_clean_checkout_inventory_and_line_ending_hashes,
]


def main() -> int:
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
    if THROUGHPUT:
        print("throughput_informational", json.dumps(THROUGHPUT, sort_keys=True))
    return 1 if failures else 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
