"""Content-addressed local raw store: exact payload and manifest bytes, then a receipt.

Temporary objects are never listed. Final creation is create-if-absent with length
and digest collision checks. The receipt marker is what ``get_receipt`` / ``open_raw``
mean by received; a crash before that marker leaves unclaimed content. This store
never writes delivery state or typed Bronze rows.
"""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from pathlib import Path

from ergasterion.ingestion.codecs import transport_payload_fingerprint
from ergasterion.ingestion.records import (
    Digest,
    ExternalReceiptInput,
    ManagedPayloadInput,
    NonNegativeIntegerString,
    PositiveIntegerString,
    RawManifestObject,
    RawPayloadObject,
    RawReadHandle,
    RawReadPage,
    RawReceipt,
)
from ergasterion.ingestion.runtime import PortError, canonical_digest

DEFAULT_MAX_PAYLOAD_BYTES = 16 * 1024 * 1024
DEFAULT_MAX_READ_CHUNK = 1024 * 1024


def _b64url(raw: bytes) -> str:
    import base64

    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _payload_from_registered(value: object) -> bytes | Path | str:
    if isinstance(value, (bytes, bytearray)):
        return bytes(value)
    if isinstance(value, Path):
        return value
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        return json.dumps(value).encode("utf-8")
    raise PortError("integrity_error", f"unsupported payload handle type {type(value)!r}")


def _atomic_write(dest: Path, data: bytes) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.exists():
        existing = dest.read_bytes()
        if existing != data:
            raise PortError(
                "integrity_error",
                f"content-addressed object {dest.name} already exists with different bytes",
            )
        if len(existing) != len(data):
            raise PortError("integrity_error", f"content-addressed object {dest.name} length collided")
        return
    fd, tmp_name = tempfile.mkstemp(prefix=f".{dest.name}.", suffix=".tmp", dir=str(dest.parent))
    tmp_path = Path(tmp_name)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, dest)
    except Exception:
        try:
            tmp_path.unlink(missing_ok=True)
        except OSError:
            pass
        raise


def _object_path(root: Path, digest: str) -> Path:
    return root / "objects" / digest[:2] / digest


def _receipt_path(root: Path, digest: str) -> Path:
    return root / "receipts" / digest[:2] / f"{digest}.json"


def _receipt_digest(claim_digest: str, payload: RawPayloadObject, manifest: RawManifestObject, frame_sequence_digest: str | None) -> str:
    body: dict = {
        "schema": "ergasterion.raw-receipt/v1",
        "claim_digest": claim_digest,
        "payload": payload.model_dump(mode="json"),
        "manifest": manifest.model_dump(mode="json"),
    }
    if frame_sequence_digest is not None:
        body["frame_sequence_digest"] = frame_sequence_digest
    return canonical_digest(body)


class LocalRawStore:
    """Filesystem raw store. ``content_by_handle`` lets tests and the packaged
    conformance runner register payload bytes (or row lists) under an opaque handle
    the same way the in-memory fake does."""

    def __init__(
        self,
        root: str | Path,
        *,
        max_payload_bytes: int = DEFAULT_MAX_PAYLOAD_BYTES,
        max_read_chunk: int = DEFAULT_MAX_READ_CHUNK,
        crash_before_receipt: bool = False,
    ) -> None:
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        (self.root / "objects").mkdir(exist_ok=True)
        (self.root / "receipts").mkdir(exist_ok=True)
        self.max_payload_bytes = max_payload_bytes
        self.max_read_chunk = max_read_chunk
        self.crash_before_receipt = crash_before_receipt
        self.content_by_handle: dict[str, object] = {}
        self._manifest_bytes: dict[str, bytes] = {}

    def register_payload(self, handle: str, payload: bytes | list | Path) -> None:
        self.content_by_handle[handle] = payload

    def register_manifest_bytes(self, handle: str, manifest_bytes: bytes) -> None:
        self._manifest_bytes[handle] = manifest_bytes

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
                raise PortError(
                    "capacity_exceeded",
                    f"payload exceeds {self.max_payload_bytes}",
                )
        return b"".join(chunks)

    def _hash_object(self, path: Path, expected_length: int) -> str:
        hasher = hashlib.sha256()
        total = 0
        chunk_size = max(1, self.max_read_chunk)
        with path.open("rb") as stream:
            while True:
                piece = stream.read(chunk_size)
                if not piece:
                    break
                total += len(piece)
                if total > expected_length or total > self.max_payload_bytes:
                    raise PortError("integrity_error", "stored payload exceeds declared length")
                hasher.update(piece)
        if total != expected_length:
            raise PortError("integrity_error", "stored payload length does not match the receipt")
        return hasher.hexdigest()

    def _resolve_registered(self, value: object) -> bytes:
        resolved = _payload_from_registered(value)
        if isinstance(resolved, bytes):
            return resolved
        if isinstance(resolved, Path):
            return self._read_capped(resolved)
        path = Path(resolved)
        if path.is_file():
            return self._read_capped(path)
        raise PortError("not_found", f"payload handle {resolved!r} is not a readable file")

    def _resolve(self, handle: str) -> bytes:
        if handle in self.content_by_handle:
            return self._resolve_registered(self.content_by_handle[handle])
        path = Path(handle)
        if path.is_file():
            return self._read_capped(path)
        raise PortError("not_found", f"payload handle {handle!r} is not registered")

    def _receipts_for_claim_payload(self, claim_digest: str, payload_content_id: str) -> list[RawReceipt]:
        found: list[RawReceipt] = []
        receipts_root = self.root / "receipts"
        if not receipts_root.is_dir():
            return found
        for path in receipts_root.glob("*/*.json"):
            try:
                receipt = RawReceipt.model_validate_json(path.read_bytes())
            except (OSError, ValueError):
                continue
            if receipt.claim_digest == claim_digest and receipt.payload.content_id == payload_content_id:
                found.append(receipt)
        return found

    def preserve(self, input: ManagedPayloadInput) -> RawReceipt:
        registered = self.content_by_handle.get(input.payload_handle)
        synthetic_rows = isinstance(registered, list)
        payload = self._resolve(input.payload_handle)
        if len(payload) > self.max_payload_bytes:
            raise PortError("capacity_exceeded", f"payload {len(payload)} bytes exceeds {self.max_payload_bytes}")
        if not synthetic_rows:
            claimed = input.manifest.payload.sha256
            actual = transport_payload_fingerprint(payload)
            if claimed != actual:
                raise PortError("integrity_error", "transport payload fingerprint does not match the received bytes")
            if int(input.manifest.payload.byte_length) != len(payload):
                raise PortError("integrity_error", "payload byte_length does not match the received bytes")
        payload_digest = hashlib.sha256(payload).hexdigest()
        manifest_bytes = self._manifest_bytes.get(input.payload_handle)
        if manifest_bytes is None:
            manifest_bytes = json.dumps(
                input.manifest.model_dump(mode="json", by_alias=True), separators=(",", ":"),
            ).encode("utf-8")
        manifest_digest = hashlib.sha256(manifest_bytes).hexdigest()
        claim_digest = canonical_digest(input.manifest.model_dump(mode="json", by_alias=True))
        payload_object = RawPayloadObject(
            content_id=f"sha256:{payload_digest}",
            algorithm="sha256",
            byte_length=str(len(payload)),
            media_type=input.manifest.payload.media_type,
            content_encoding=input.manifest.payload.content_encoding,
        )
        manifest_object = RawManifestObject(
            content_id=f"sha256:{manifest_digest}",
            algorithm="sha256",
            byte_length=str(len(manifest_bytes)),
        )
        frame_digest = input.manifest.frame_sequence_digest
        receipt_digest = _receipt_digest(claim_digest, payload_object, manifest_object, frame_digest)
        existing = self._load_receipt(receipt_digest)
        if existing is not None:
            if (
                existing.payload.content_id != payload_object.content_id
                or existing.claim_digest != claim_digest
                or existing.manifest.content_id != manifest_object.content_id
            ):
                raise PortError("claim_conflict", "raw receipt digest already binds different evidence")
            return existing
        for prior in self._receipts_for_claim_payload(claim_digest, payload_object.content_id):
            if prior.manifest.content_id != manifest_object.content_id:
                raise PortError("claim_conflict", "raw receipt already binds different sidecar bytes")
            return prior
        _atomic_write(_object_path(self.root, payload_digest), payload)
        _atomic_write(_object_path(self.root, manifest_digest), manifest_bytes)
        if self.crash_before_receipt:
            raise RuntimeError("simulated crash after content write, before receipt marker")
        receipt_body: dict = {
            "schema": "ergasterion.raw-receipt/v1",
            "claim_digest": claim_digest,
            "payload": payload_object,
            "manifest": manifest_object,
            "raw_receipt_digest": receipt_digest,
        }
        if frame_digest is not None:
            receipt_body["frame_sequence_digest"] = frame_digest
        receipt = RawReceipt.model_validate(receipt_body)
        dumped = receipt.model_dump(mode="json", by_alias=True)
        if dumped.get("frame_sequence_digest") is None:
            dumped.pop("frame_sequence_digest", None)
        _atomic_write(_receipt_path(self.root, receipt_digest), json.dumps(
            dumped, separators=(",", ":"),
        ).encode("utf-8"))
        return receipt

    def _load_receipt(self, digest: str) -> RawReceipt | None:
        path = _receipt_path(self.root, digest)
        if not path.is_file():
            return None
        return RawReceipt.model_validate_json(path.read_bytes())

    def get_receipt(self, raw_receipt_digest: Digest) -> RawReceipt:
        receipt = self._load_receipt(raw_receipt_digest)
        if receipt is None:
            raise PortError("not_found", raw_receipt_digest)
        payload_path = _object_path(self.root, receipt.payload.content_id.split(":", 1)[-1])
        if not payload_path.is_file():
            raise PortError("integrity_error", "receipt exists but payload object is missing")
        actual = self._hash_object(payload_path, int(receipt.payload.byte_length))
        if f"sha256:{actual}" != receipt.payload.content_id:
            raise PortError("integrity_error", "stored payload digest does not match the receipt")
        return receipt

    def open_raw(self, raw_receipt_digest: Digest) -> RawReadHandle:
        receipt = self.get_receipt(raw_receipt_digest)
        return RawReadHandle(
            raw_receipt_digest=raw_receipt_digest,
            content_id=receipt.payload.content_id,
            byte_length=receipt.payload.byte_length,
            handle_ref=raw_receipt_digest,
        )

    def read_raw(
        self, handle: RawReadHandle, offset: NonNegativeIntegerString, max_bytes: PositiveIntegerString,
    ) -> RawReadPage:
        receipt = self.get_receipt(handle.raw_receipt_digest)
        payload_path = _object_path(self.root, receipt.payload.content_id.split(":", 1)[-1])
        start = int(offset)
        requested = int(max_bytes)
        cap = min(requested, self.max_read_chunk)
        if cap < 1:
            raise PortError("integrity_error", "maximum read chunk is zero")
        size = int(receipt.payload.byte_length)
        if start > size:
            raise PortError("integrity_error", "raw read offset is past the payload end")
        if size == 0:
            return RawReadPage(
                handle_ref=handle.handle_ref, offset=offset, bytes_base64url="",
                bytes_returned="0", next_offset=None, eof=True,
            )
        with payload_path.open("rb") as stream:
            stream.seek(start)
            chunk = stream.read(cap)
        returned = len(chunk)
        next_offset = start + returned
        eof = next_offset >= size
        return RawReadPage(
            handle_ref=handle.handle_ref,
            offset=offset,
            bytes_base64url=_b64url(chunk),
            bytes_returned=str(returned),
            next_offset=None if eof else str(next_offset),
            eof=eof,
        )

    def verify_open(self, input: ExternalReceiptInput) -> RawReceipt:
        payload = input.receipt.payload
        digest = payload.raw_digest
        stored = self._load_receipt(digest)
        if stored is not None:
            return stored
        raise PortError("not_found", digest)


__all__ = ["DEFAULT_MAX_PAYLOAD_BYTES", "DEFAULT_MAX_READ_CHUNK", "LocalRawStore"]
