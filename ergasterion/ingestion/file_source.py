"""Local CSV / JSON Lines source connector: sidecar manifests in, DeliveryInput out.

The connector receives files; it does not poll or extract from a network source.
It computes transport and CDC fingerprints from exact received bytes before any
typed parse, and never writes delivery state or Bronze rows. ``file_ports_factory``
hands this connector plus the local raw and scratch stores to the packaged
conformance runner.
"""

from __future__ import annotations

import tempfile
from pathlib import Path

from ergasterion.framework.bronze_contract import BronzeProductContract, ContentEncoding, MediaType
from ergasterion.ingestion.codecs import (
    decode_transport,
    frame_sequence_digest,
    parse_payload,
    split_jsonl_frames,
    transport_payload_fingerprint,
)
from ergasterion.ingestion.conformance import memory_ports_factory
from ergasterion.ingestion.evidence import verify_signed_attestation, verify_signed_external_receipt
from ergasterion.ingestion.local_raw_store import LocalRawStore
from ergasterion.ingestion.local_scratch_store import LocalScratchStore
from ergasterion.ingestion.ports import PortSet
from ergasterion.ingestion.records import (
    DeliveryInput,
    DeliveryManifest,
    ExternalReceiptInput,
    ManagedPayloadInput,
    UtcInstant,
)

from ergasterion.ingestion.runtime import PortError, utc_now_string

DEFAULT_MAX_PAYLOAD_BYTES = 16 * 1024 * 1024
DEFAULT_MAX_UNCOMPRESSED_BYTES = 64 * 1024 * 1024
DEFAULT_MAX_EXPANSION_RATIO = 10
DEFAULT_MAX_READ_CHUNK = 1024 * 1024


def _wire(value: object) -> str:
    return value.value if hasattr(value, "value") else str(value)


def _media_for_codec(kind: str) -> str:
    return MediaType.CSV.value if kind == "csv" else MediaType.NDJSON.value


def _remap_connector_error(exc: PortError) -> PortError:
    if exc.code in {"capability_mismatch", "invalid_manifest", "integrity_error"}:
        return exc
    if exc.code in {"codec_error", "framing_error"}:
        return PortError("invalid_manifest", exc.detail)
    return PortError("integrity_error", exc.detail)


class FileSource:
    """``SourceConnectorPort`` for local managed files and signed external receipts."""

    def __init__(
        self,
        *,
        contract: BronzeProductContract | None = None,
        key_resolver=None,
        max_payload_bytes: int = DEFAULT_MAX_PAYLOAD_BYTES,
        max_uncompressed_bytes: int = DEFAULT_MAX_UNCOMPRESSED_BYTES,
        max_expansion_ratio: int | float = DEFAULT_MAX_EXPANSION_RATIO,
        max_read_chunk: int = DEFAULT_MAX_READ_CHUNK,
        now_fn=None,
        payload_registry: dict[str, bytes] | None = None,
    ) -> None:
        self.contract = contract
        self.key_resolver = key_resolver
        self.max_payload_bytes = max_payload_bytes
        self.max_uncompressed_bytes = max_uncompressed_bytes
        self.max_expansion_ratio = max_expansion_ratio
        self.max_read_chunk = max_read_chunk
        self.now_fn = now_fn or (lambda: utc_now_string())
        self.payload_registry = payload_registry if payload_registry is not None else {}
        self.manifest_registry: dict[str, bytes] = {}

    def register_payload(self, handle: str, payload: bytes) -> None:
        self.payload_registry[handle] = payload

    def _read_capped(self, path: Path) -> bytes:
        chunks: list[bytes] = []
        remaining = self.max_payload_bytes
        chunk_size = max(1, self.max_read_chunk)
        with path.open("rb") as stream:
            while remaining > 0:
                piece = stream.read(min(chunk_size, remaining))
                if not piece:
                    break
                chunks.append(piece)
                remaining -= len(piece)
            extra = stream.read(1)
            if extra:
                raise PortError("integrity_error", "payload exceeds maximum payload bytes")
        return b"".join(chunks)

    def _read_handle(self, handle: str) -> bytes | None:
        if handle in self.payload_registry:
            payload = self.payload_registry[handle]
            if len(payload) > self.max_payload_bytes:
                raise PortError("integrity_error", "payload exceeds maximum payload bytes")
            return payload
        path = Path(handle)
        if path.is_file():
            return self._read_capped(path)
        return None

    def open_managed(self, manifest_path: str | Path, payload_path: str | Path) -> ManagedPayloadInput:
        manifest_bytes = self._read_capped(Path(manifest_path))
        if manifest_bytes.startswith(b"\xef\xbb\xbf"):
            raise PortError("invalid_manifest", "sidecar manifest must not carry a UTF-8 BOM")
        try:
            manifest = DeliveryManifest.model_validate_json(manifest_bytes)
        except Exception as exc:
            raise PortError("invalid_manifest", f"sidecar is not a closed delivery manifest: {exc}") from exc
        payload_path = Path(payload_path)
        handle = str(payload_path)
        payload = self._read_handle(handle)
        if payload is None:
            raise PortError("integrity_error", f"payload file {payload_path} is not readable")
        self.register_payload(handle, payload)
        self.manifest_registry[handle] = manifest_bytes
        return ManagedPayloadInput(kind="managed_payload", manifest=manifest, payload_handle=handle)

    def submit_managed(self, input: ManagedPayloadInput) -> DeliveryInput:
        payload = self._read_handle(input.payload_handle)
        if payload is None:
            # Opaque conformance handle: identity pass-through, same seam FakeSourceConnector uses.
            return input
        try:
            return self._submit_bytes(input, payload)
        except PortError as exc:
            raise _remap_connector_error(exc) from exc

    def _submit_bytes(self, input: ManagedPayloadInput, payload: bytes) -> DeliveryInput:
        manifest = input.manifest
        if len(payload) > self.max_payload_bytes:
            raise PortError("integrity_error", "payload exceeds maximum payload bytes")
        if int(manifest.payload.byte_length) != len(payload):
            raise PortError("integrity_error", "manifest byte_length does not match the received payload")
        fingerprint = transport_payload_fingerprint(payload)
        if manifest.payload.sha256 != fingerprint:
            raise PortError("integrity_error", "transport payload fingerprint does not match the received bytes")
        encoding = _wire(manifest.payload.content_encoding)
        if encoding not in {ContentEncoding.IDENTITY.value, ContentEncoding.GZIP.value}:
            raise PortError("capability_mismatch", f"unsupported content encoding {encoding!r}")
        if self.contract is not None:
            allowed = {_wire(item) for item in self.contract.landing.content_encodings}
            if encoding not in allowed:
                raise PortError("capability_mismatch", f"content encoding {encoding!r} is not in the contract")
            codec_kind = self.contract.landing.codec.kind
            expected_media = _media_for_codec(codec_kind)
            if _wire(manifest.payload.media_type) != expected_media:
                raise PortError("capability_mismatch", "payload media type does not match the contract codec")
            if manifest.payload.codec_version != self.contract.landing.codec.version:
                raise PortError("capability_mismatch", "codec_version does not match the contract")
            self._check_sidecar_shape(manifest, self.contract)
        inner = decode_transport(
            payload,
            encoding if isinstance(encoding, str) else encoding.value,
            max_uncompressed_bytes=self.max_uncompressed_bytes,
            max_expansion_ratio=self.max_expansion_ratio,
        )
        if manifest.frame_sequence_digest is not None:
            if _wire(manifest.payload.media_type) != MediaType.NDJSON.value:
                raise PortError("invalid_manifest", "CDC frame digest requires JSON Lines")
            newline = self.contract.landing.codec.newline if self.contract is not None else "lf"
            frames = split_jsonl_frames(inner, newline)
            digest = frame_sequence_digest(frames)
            if digest != manifest.frame_sequence_digest:
                raise PortError("integrity_error", "CDC frame_sequence_digest does not match receiver-order frames")
            if self.contract is not None and self.contract.delivery.progress.kind == "sequence":
                field = self.contract.delivery.progress.field
                parsed = parse_payload(inner, self.contract.landing.codec, self.contract.landing.physical_columns, sequence_field=field)
                claimed = int(manifest.progress_claim.high_watermark)
                if parsed.frames:
                    maximum = max(int(frame.frame_sequence) for frame in parsed.frames)
                    if maximum != claimed:
                        raise PortError("invalid_manifest", "CDC high watermark is not the maximum declared sequence")
                event_count = int(manifest.progress_claim.event_count)
                if event_count != len(frames) or int(manifest.declared_row_count) != len(frames):
                    raise PortError("invalid_manifest", "CDC event_count / declared_row_count disagree with framed events")
        return input

    def _check_sidecar_shape(self, manifest: DeliveryManifest, contract: BronzeProductContract) -> None:
        mode = _wire(contract.delivery.mode)
        progress = manifest.progress_claim
        if mode == "cdc":
            if manifest.batch_id is not None or manifest.effective_boundary_at is not None:
                raise PortError("invalid_manifest", "CDC sidecar must omit batch_id and effective_boundary_at")
            if manifest.scheduled_boundary_at is None or manifest.frame_sequence_digest is None:
                raise PortError("invalid_manifest", "CDC sidecar requires scheduled_boundary_at and frame_sequence_digest")
            if progress.kind != "sequence":
                raise PortError("invalid_manifest", "CDC sidecar requires sequence progress")
            if manifest.snapshot_attestation is not None:
                raise PortError("invalid_manifest", "CDC sidecar must omit snapshot_attestation")
        elif mode == "append_only":
            if manifest.scheduled_boundary_at is None:
                raise PortError("invalid_manifest", "append sidecar requires scheduled_boundary_at")
            if manifest.frame_sequence_digest is not None:
                raise PortError("invalid_manifest", "append sidecar must omit frame_sequence_digest")
            if manifest.snapshot_attestation is not None:
                raise PortError("invalid_manifest", "append sidecar must omit snapshot_attestation")
            if progress.kind == "opaque_batch" and manifest.batch_id is None:
                raise PortError("invalid_manifest", "opaque append sidecar requires batch_id")
        elif mode == "complete_snapshot":
            if manifest.batch_id is None or manifest.scheduled_boundary_at is None or manifest.effective_boundary_at is None:
                raise PortError("invalid_manifest", "snapshot sidecar requires batch_id and both boundaries")
            if manifest.frame_sequence_digest is not None:
                raise PortError("invalid_manifest", "snapshot sidecar must omit frame_sequence_digest")
            if progress.kind != "opaque_batch":
                raise PortError("invalid_manifest", "snapshot sidecar requires opaque_batch progress")
            if manifest.snapshot_attestation is None:
                raise PortError("invalid_manifest", "snapshot sidecar requires snapshot_attestation")
            self._require_snapshot_attestation(manifest, contract)
        if self.contract is not None and manifest.logical_identity != self.contract.logical_identity:
            raise PortError("invalid_manifest", "sidecar logical_identity does not match the contract")

    def _require_snapshot_attestation(self, manifest: DeliveryManifest, contract: BronzeProductContract) -> None:
        attestation = manifest.snapshot_attestation
        if attestation is None:
            raise PortError("invalid_manifest", "snapshot sidecar requires snapshot_attestation")
        payload = attestation.payload
        if payload.logical_identity != manifest.logical_identity:
            raise PortError("invalid_manifest", "snapshot attestation logical_identity does not match the manifest")
        if payload.contract_digest != manifest.contract_digest:
            raise PortError("invalid_manifest", "snapshot attestation contract_digest does not match the manifest")
        if payload.delivery_id != manifest.delivery_id:
            raise PortError("invalid_manifest", "snapshot attestation delivery_id does not match the manifest")
        if payload.batch_id != manifest.batch_id:
            raise PortError("invalid_manifest", "snapshot attestation batch_id does not match the manifest")
        if payload.effective_boundary_at != manifest.effective_boundary_at:
            raise PortError("invalid_manifest", "snapshot attestation effective_boundary_at does not match the manifest")
        if payload.content_fingerprint != manifest.payload.sha256:
            raise PortError("invalid_manifest", "snapshot attestation content_fingerprint does not match the manifest")
        if payload.row_count != manifest.declared_row_count:
            raise PortError("invalid_manifest", "snapshot attestation row_count does not match the manifest")
        snapshot = contract.delivery.snapshot
        if snapshot is None:
            raise PortError("invalid_manifest", "complete_snapshot contract requires snapshot trust policy")
        if (
            payload.scope.scope_id != snapshot.scope_id
            or payload.scope.scope_parameters != snapshot.scope_parameters
        ):
            raise PortError("invalid_manifest", "snapshot attestation scope does not match the contract")
        if attestation.key_id not in snapshot.allowed_key_ids:
            raise PortError("capability_mismatch", "snapshot key_id is not in the contract trust set")
        if self.key_resolver is None:
            raise PortError("capability_mismatch", "snapshot attestation requires a key resolver")
        record = self.key_resolver.resolve_verification_key(attestation.key_id)
        now: UtcInstant = self.now_fn()
        verify_signed_attestation(
            attestation,
            record,
            now,
            int(snapshot.future_clock_skew_seconds),
            policy_ref=snapshot.attestation_policy_ref,
        )

    def verify_external(self, input: ExternalReceiptInput) -> DeliveryInput:
        receipt = input.receipt
        payload = receipt.payload
        if payload.logical_identity != payload.claim.logical_identity:
            raise PortError("integrity_error", "external receipt identity does not match the embedded claim")
        if payload.contract_digest != payload.claim.contract_digest:
            raise PortError("integrity_error", "external receipt contract_digest does not match the embedded claim")
        external = self.contract is not None and _wire(self.contract.landing.integration.kind) == "external"
        if external:
            if self.key_resolver is None:
                raise PortError("capability_mismatch", "external receipt verification requires a key resolver")
            trust = self.contract.landing.integration.receipt_trust
            if receipt.key_id not in trust.allowed_key_ids:
                raise PortError("capability_mismatch", "receipt key_id is not in the contract trust set")
            record = self.key_resolver.resolve_verification_key(receipt.key_id)
            now: UtcInstant = self.now_fn()
            verify_signed_external_receipt(
                receipt, record, now, int(trust.future_clock_skew_seconds), policy_ref=trust.policy_ref,
            )
            return input
        if self.key_resolver is not None:
            record = self.key_resolver.resolve_verification_key(receipt.key_id)
            now: UtcInstant = self.now_fn()
            verify_signed_external_receipt(receipt, record, now, 30, policy_ref=None)
        return input


def file_ports_factory(
    vector: dict,
    contract: BronzeProductContract,
    payload_handle: str,
    *,
    directory: str | Path | None = None,
):
    """``run_adapter_conformance`` factory: local connector/raw/scratch, in-memory remainder."""

    directory = Path(directory) if directory is not None else Path(tempfile.mkdtemp(prefix="ergasterion-file-"))
    directory.mkdir(parents=True, exist_ok=True)
    raw = LocalRawStore(directory / "raw")
    raw.content_by_handle[payload_handle] = vector["rows"]
    scratch = LocalScratchStore(directory / "scratch")
    connector = FileSource(contract=None)
    ports, _ignored = memory_ports_factory(vector, contract, payload_handle)
    return PortSet(
        source_connector=connector,
        raw_store=raw,
        scratch_store=scratch,
        state_store=ports.state_store,
        landing_adapter=ports.landing_adapter,
        remediation_repository=ports.remediation_repository,
        projection_publisher=ports.projection_publisher,
        lifecycle_sink=ports.lifecycle_sink,
        key_resolver=ports.key_resolver,
    ), ports.state_store.stream_state


__all__ = [
    "DEFAULT_MAX_EXPANSION_RATIO",
    "DEFAULT_MAX_PAYLOAD_BYTES",
    "DEFAULT_MAX_READ_CHUNK",
    "DEFAULT_MAX_UNCOMPRESSED_BYTES",
    "FileSource",
    "file_ports_factory",
]
