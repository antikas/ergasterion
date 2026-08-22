"""Attestation verification and keyed record-key fingerprints.

This module is the non-secret cryptographic surface the SQLite operational
store (and a later production key resolver) uses: Ed25519 envelope signatures,
HMAC-SHA-256 record-key tags under the frozen IDL ``mac_framing``, immutable
key commitments, and the issued-at / revocation / clock-skew / policy rules
for snapshot attestations and external receipts.

HMAC secret bytes never appear on a return value, a wire record, or a digest
input that a caller might persist. A record-key fingerprint is an opaque tag;
the plaintext components used to produce it are the caller's to forget.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
from datetime import datetime, timedelta, timezone
from typing import Mapping

import rfc8785
from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey, Ed25519PublicKey
from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat

from ergasterion.ingestion.records import (
    Base64Url,
    Digest,
    FingerprintScope,
    KeyCommitmentRecord,
    LogicalIdentity,
    MacResult,
    SignedAttestation,
    SignedExternalReceipt,
    Token,
    UtcInstant,
    VerificationKeyRecord,
)
from ergasterion.ingestion.runtime import PortError, canonical_digest, parse_utc_instant, utc_now_string

RECORD_KEY_DOMAIN = "ergasterion.record-key/v1"
"""UTF-8 domain the frozen IDL pins for record-key HMAC framing."""

SNAPSHOT_ATTESTATION_SCHEMA = "ergasterion.snapshot-attestation/v1"
EXTERNAL_RECEIPT_SCHEMA = "ergasterion.external-receipt/v1"
SNAPSHOT_KEYSET_SCHEMA = "ergasterion.snapshot-keyset/v1"
TOMBSTONE_KEYSET_SCHEMA = "ergasterion.tombstone-keyset/v1"
DELETION_EVIDENCE_INTENT_SCHEMA = "ergasterion.deletion-evidence-intent/v1"


def b64url_encode(raw: bytes) -> Base64Url:
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def b64url_decode(text: str) -> bytes:
    padding = "=" * (-len(text) % 4)
    return base64.urlsafe_b64decode(text + padding)


def dump_json(value: object) -> dict:
    """RFC-8785-ready JSON projection of a closed model or already-plain mapping."""

    if hasattr(value, "model_dump"):
        return value.model_dump(mode="json", by_alias=True)
    if isinstance(value, Mapping):
        return dict(value)
    raise TypeError(f"cannot project {type(value)!r} to canonical JSON")


def canonical_bytes(value: object) -> bytes:
    return rfc8785.dumps(dump_json(value) if not isinstance(value, (bytes, bytearray)) else value)


def frame_mac(domain: bytes, message: bytes) -> bytes:
    """IDL ``mac_framing``: ``uint32be(domain_length) || domain || uint64be(message_length) || message``."""

    return len(domain).to_bytes(4, "big") + domain + len(message).to_bytes(8, "big") + message


def hmac_sha256_tag(secret: bytes, domain: str, message: bytes) -> Digest:
    """HMAC-SHA-256 over the framed input, lowercase hex. ``secret`` is not returned."""

    framed = frame_mac(domain.encode("utf-8"), message)
    return hmac.new(secret, framed, hashlib.sha256).hexdigest()


def record_key_envelope(
    logical_identity: LogicalIdentity | Mapping,
    scope: FingerprintScope | Mapping,
    components: tuple[object, ...] | list[object],
) -> dict:
    """JCS envelope the golden vectors pin: schema, identity, scope, ordered typed components."""

    return {
        "schema": RECORD_KEY_DOMAIN,
        "logical_identity": dump_json(logical_identity),
        "scope": dump_json(scope),
        "components": [dump_json(item) for item in components],
    }


def record_key_message(
    logical_identity: LogicalIdentity | Mapping,
    scope: FingerprintScope | Mapping,
    components: tuple[object, ...] | list[object],
) -> bytes:
    return rfc8785.dumps(record_key_envelope(logical_identity, scope, components))


def record_key_fingerprint(
    secret: bytes,
    logical_identity: LogicalIdentity | Mapping,
    scope: FingerprintScope | Mapping,
    components: tuple[object, ...] | list[object],
) -> Digest:
    """Opaque HMAC tag for one record key. Plaintext components never appear in the tag."""

    message = record_key_message(logical_identity, scope, components)
    return hmac_sha256_tag(secret, RECORD_KEY_DOMAIN, message)


def hmac_key_commitment(key_id: Token, secret: bytes) -> Digest:
    """Immutable non-secret commitment for one HMAC key id.

    Binds the identifier to a digest of the material without returning or
    embedding the secret. Reusing ``key_id`` with different material yields a
    different commitment, which the store treats as ``key_commitment_conflict``.
    """

    material_digest = hashlib.sha256(secret).hexdigest()
    return canonical_digest({
        "algorithm": "HMAC-SHA-256",
        "key_id": key_id,
        "material_sha256": material_digest,
    })


def hmac_commitment_record(key_id: Token, secret: bytes) -> KeyCommitmentRecord:
    return KeyCommitmentRecord(key_id=key_id, algorithm="HMAC-SHA-256", commitment=hmac_key_commitment(key_id, secret))


def public_key_fingerprint(public_key_raw: bytes) -> Digest:
    return hashlib.sha256(public_key_raw).hexdigest()


def trust_record_digest(
    key_id: Token,
    public_key_fingerprint_hex: Digest,
    enabled_at: UtcInstant,
    authorized_policy_refs: tuple[Token, ...],
    expires_at: UtcInstant | None = None,
    revoked_at: UtcInstant | None = None,
) -> Digest:
    """Digest of the non-secret verification record: fingerprint in, public-key bytes and this digest out."""

    payload: dict = {
        "algorithm": "Ed25519",
        "authorized_policy_refs": list(authorized_policy_refs),
        "enabled_at": enabled_at,
        "key_id": key_id,
        "public_key_fingerprint": public_key_fingerprint_hex,
    }
    if expires_at is not None:
        payload["expires_at"] = expires_at
    if revoked_at is not None:
        payload["revoked_at"] = revoked_at
    return canonical_digest(payload)


def verification_key_record(
    key_id: Token,
    public_key_raw: bytes,
    enabled_at: UtcInstant,
    authorized_policy_refs: tuple[Token, ...],
    expires_at: UtcInstant | None = None,
    revoked_at: UtcInstant | None = None,
) -> VerificationKeyRecord:
    fingerprint = public_key_fingerprint(public_key_raw)
    policies = tuple(sorted(set(authorized_policy_refs)))
    payload: dict = {
        "key_id": key_id,
        "algorithm": "Ed25519",
        "public_key_base64url": b64url_encode(public_key_raw),
        "public_key_fingerprint": fingerprint,
        "enabled_at": enabled_at,
        "authorized_policy_refs": policies,
        "trust_record_digest": trust_record_digest(
            key_id, fingerprint, enabled_at, policies, expires_at=expires_at, revoked_at=revoked_at,
        ),
    }
    if expires_at is not None:
        payload["expires_at"] = expires_at
    if revoked_at is not None:
        payload["revoked_at"] = revoked_at
    return VerificationKeyRecord.model_validate(payload)


def generate_ed25519_keypair() -> tuple[Ed25519PrivateKey, bytes]:
    private = Ed25519PrivateKey.generate()
    public_raw = private.public_key().public_bytes(Encoding.Raw, PublicFormat.Raw)
    return private, public_raw


def signing_basis(envelope: Mapping) -> bytes:
    """JCS of a signed envelope with the ``signature`` field omitted."""

    body = {key: value for key, value in dump_json(envelope).items() if key != "signature"}
    return rfc8785.dumps(body)


def sign_envelope(private_key: Ed25519PrivateKey, envelope: Mapping) -> Base64Url:
    return b64url_encode(private_key.sign(signing_basis(envelope)))


def verify_envelope_signature(public_key_raw: bytes, envelope: Mapping, signature: str) -> None:
    try:
        Ed25519PublicKey.from_public_bytes(public_key_raw).verify(b64url_decode(signature), signing_basis(envelope))
    except (InvalidSignature, ValueError) as exc:
        raise PortError("invalid_signature", "Ed25519 signature does not match the envelope") from exc


def _require_key_id(record: VerificationKeyRecord, key_id: Token) -> None:
    if record.key_id != key_id:
        raise PortError("integrity_error", f"verification record key_id {record.key_id!r} does not equal {key_id!r}")


def check_verification_window(
    record: VerificationKeyRecord,
    issued_at: UtcInstant,
    now: UtcInstant,
    future_clock_skew_seconds: int,
    policy_ref: Token | None = None,
) -> None:
    """Enable / expiry / revocation / clock-skew / policy rules shared by attestations and receipts.

    Revocation at or before ``issued_at`` refuses the artefact. Revocation after a
    valid ``issued_at`` does not invalidate that artefact; ``now`` is used only
    for the future-clock-skew ceiling on newly presented artefacts.
    """

    issued = parse_utc_instant(issued_at)
    observed = parse_utc_instant(now)
    enabled = parse_utc_instant(record.enabled_at)
    if issued < enabled:
        raise PortError("attestation_invalid", f"key {record.key_id!r} was not enabled at {issued_at}")
    if record.expires_at is not None and issued > parse_utc_instant(record.expires_at):
        raise PortError("attestation_invalid", f"key {record.key_id!r} had expired at {issued_at}")
    if record.revoked_at is not None and parse_utc_instant(record.revoked_at) <= issued:
        raise PortError("key_revoked", f"key {record.key_id!r} was revoked at {record.revoked_at}")
    skew = timedelta(seconds=future_clock_skew_seconds)
    if issued > observed + skew:
        raise PortError(
            "attestation_invalid",
            f"issued_at {issued_at} is more than {future_clock_skew_seconds}s ahead of {now}",
        )
    if policy_ref is not None and policy_ref not in record.authorized_policy_refs:
        raise PortError("policy_not_authorized", f"key {record.key_id!r} is not authorised for {policy_ref!r}")


def verify_signed_attestation(
    attestation: SignedAttestation,
    record: VerificationKeyRecord,
    now: UtcInstant,
    future_clock_skew_seconds: int,
    policy_ref: Token | None = None,
) -> None:
    _require_key_id(record, attestation.key_id)
    if attestation.algorithm != "Ed25519":
        raise PortError("attestation_invalid", f"unsupported attestation algorithm {attestation.algorithm!r}")
    if attestation.schema_ != SNAPSHOT_ATTESTATION_SCHEMA:
        raise PortError("attestation_invalid", f"unexpected attestation schema {attestation.schema_!r}")
    verify_envelope_signature(b64url_decode(record.public_key_base64url), attestation, attestation.signature)
    check_verification_window(
        record, attestation.payload.issued_at, now, future_clock_skew_seconds, policy_ref=policy_ref,
    )
    fingerprint = public_key_fingerprint(b64url_decode(record.public_key_base64url))
    if fingerprint != record.public_key_fingerprint:
        raise PortError("integrity_error", "public-key bytes do not match the stored fingerprint")


def verify_signed_external_receipt(
    receipt: SignedExternalReceipt,
    record: VerificationKeyRecord,
    now: UtcInstant,
    future_clock_skew_seconds: int,
    policy_ref: Token | None = None,
) -> None:
    _require_key_id(record, receipt.key_id)
    if receipt.algorithm != "Ed25519":
        raise PortError("invalid_signature", f"unsupported receipt algorithm {receipt.algorithm!r}")
    if receipt.schema_ != EXTERNAL_RECEIPT_SCHEMA:
        raise PortError("invalid_signature", f"unexpected receipt schema {receipt.schema_!r}")
    verify_envelope_signature(b64url_decode(record.public_key_base64url), receipt, receipt.signature)
    check_verification_window(
        record, receipt.payload.issued_at, now, future_clock_skew_seconds, policy_ref=policy_ref,
    )


def snapshot_keyset_digest(
    logical_identity: LogicalIdentity | Mapping,
    visibility: object,
    scope: FingerprintScope | Mapping,
    hmac_key_id: Token,
    key_commitment: Digest,
    tags: tuple[Digest, ...] | list[Digest],
) -> Digest:
    """SHA-256 of the closed snapshot-keyset envelope with sorted unique tags."""

    unique_sorted = sorted(set(tags))
    return canonical_digest({
        "schema": SNAPSHOT_KEYSET_SCHEMA,
        "logical_identity": dump_json(logical_identity),
        "visibility": dump_json(visibility),
        "scope": dump_json(scope),
        "hmac_key_id": hmac_key_id,
        "key_commitment": key_commitment,
        "tags": unique_sorted,
    })


def tombstone_keyset_digest(
    logical_identity: LogicalIdentity | Mapping,
    visibility: object,
    scope: FingerprintScope | Mapping,
    hmac_key_id: Token,
    key_commitment: Digest,
    items: tuple[object, ...] | list[object],
) -> Digest:
    """SHA-256 of the closed tombstone-keyset envelope over ordered ``(event_sequence, tag)`` pairs."""

    encoded = []
    for item in items:
        payload = dump_json(item)
        encoded.append({"event_sequence": payload["event_sequence"], "tag": payload["tag"]})
    return canonical_digest({
        "schema": TOMBSTONE_KEYSET_SCHEMA,
        "logical_identity": dump_json(logical_identity),
        "visibility": dump_json(visibility),
        "scope": dump_json(scope),
        "hmac_key_id": hmac_key_id,
        "key_commitment": key_commitment,
        "items": encoded,
    })


def deletion_evidence_intent_digest(intent_payload: Mapping) -> Digest:
    """Digest of a deletion-evidence intent with the digest field itself omitted."""

    body = {key: value for key, value in dump_json(intent_payload).items() if key != "deletion_evidence_intent_digest"}
    body["schema"] = DELETION_EVIDENCE_INTENT_SCHEMA
    return canonical_digest(body)


def mac_result(key_id: Token, tag_hex: Digest) -> MacResult:
    return MacResult(algorithm="HMAC-SHA-256", key_id=key_id, tag_hex=tag_hex)


def add_utc(instant: UtcInstant, seconds: int) -> UtcInstant:
    return utc_now_string(parse_utc_instant(instant) + timedelta(seconds=seconds))


def aware_now(dt: datetime | None = None) -> datetime:
    value = dt or datetime.now(timezone.utc)
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


__all__ = [
    "RECORD_KEY_DOMAIN",
    "add_utc",
    "b64url_decode",
    "b64url_encode",
    "canonical_bytes",
    "check_verification_window",
    "deletion_evidence_intent_digest",
    "dump_json",
    "frame_mac",
    "generate_ed25519_keypair",
    "hmac_commitment_record",
    "hmac_key_commitment",
    "hmac_sha256_tag",
    "mac_result",
    "public_key_fingerprint",
    "record_key_envelope",
    "record_key_fingerprint",
    "record_key_message",
    "sign_envelope",
    "signing_basis",
    "snapshot_keyset_digest",
    "tombstone_keyset_digest",
    "trust_record_digest",
    "verification_key_record",
    "verify_envelope_signature",
    "verify_signed_attestation",
    "verify_signed_external_receipt",
]
