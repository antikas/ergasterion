"""Closed CSV and JSON Lines codecs plus typed scalar coercion.

Landing (a later adapter) and this package's file connector share the same
parse/type rules: delimiter/quote/escape/newline/null-token handling for CSV,
object-per-line JSON Lines with duplicate-key rejection and exact-decimal
numbers, and the eight v1 logical types. Nothing here writes a receipt, a
state row or a typed Bronze partition -- it only turns bounded bytes into
frames, findings and fingerprints.
"""

from __future__ import annotations

import hashlib
import json
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Iterator
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from ergasterion.framework.bronze_contract import (
    Codec,
    CsvCodec,
    DiagnosticCode,
    Finding,
    FindingKind,
    FindingMetadata,
    JsonlCodec,
    LogicalType,
    LogicalTypeKind,
    NewlineKind,
    RawLocator,
    Severity,
    SimpleLogicalType,
    SourceField,
    TypedBinary,
    TypedBoolean,
    TypedDate,
    TypedDecimal,
    TypedInt64,
    TypedLocalDateTime,
    TypedScalar,
    TypedString,
    TypedUtcInstant,
)
from ergasterion.ingestion.runtime import PortError

OBJECT_FINGERPRINT_PREFIX = b"ERGASTERION-OBJECT-V1\0"
CDC_FINGERPRINT_PREFIX = b"ERGASTERION-CDC-V1\0"

INT64_MIN = -9223372036854775808
INT64_MAX = 9223372036854775807
UTC_INSTANT_WIDTH = 27  # YYYY-MM-DDTHH:MM:SS.ffffffZ
DATE_WIDTH = 10
LOCAL_DATETIME_WIDTH = 26  # YYYY-MM-DDTHH:MM:SS.ffffff
UTF8 = "utf-8"
BOM = b"\xef\xbb\xbf"

_BOOLEAN_TOKENS = {"true": True, "false": False}


class DuplicateKeyError(ValueError):
    """JSON object carried the same key twice -- JSON Lines ``duplicate_keys: reject``."""


@dataclass(frozen=True)
class ParsedFrame:
    """One codec-framed unit: exact frame bytes, ordered coerced fields, structural findings."""

    frame_sequence: str
    raw_locator: RawLocator
    fields: tuple[tuple[str, TypedScalar | None], ...]
    findings: tuple[Finding, ...]
    raw_bytes: bytes


@dataclass(frozen=True)
class ParseResult:
    """Bounded parse of one payload: receiver-order frames plus a batch-level abort finding."""

    frames: tuple[ParsedFrame, ...]
    batch_findings: tuple[Finding, ...]


def transport_payload_fingerprint(payload: bytes) -> str:
    """SHA-256 of ``ERGASTERION-OBJECT-V1\\0`` plus the exact received bytes (including gzip)."""

    return hashlib.sha256(OBJECT_FINGERPRINT_PREFIX + payload).hexdigest()


def frame_sequence_digest(frames: tuple[bytes, ...] | list[bytes]) -> str:
    """SHA-256 of ``ERGASTERION-CDC-V1\\0`` plus length-prefixed receiver-order frame bytes."""

    hasher = hashlib.sha256()
    hasher.update(CDC_FINGERPRINT_PREFIX)
    for frame in frames:
        hasher.update(len(frame).to_bytes(8, "big"))
        hasher.update(frame)
    return hasher.hexdigest()


def newline_bytes(kind: NewlineKind | str) -> bytes:
    value = kind.value if isinstance(kind, NewlineKind) else kind
    if value == "crlf":
        return b"\r\n"
    return b"\n"


def logical_type_kind(logical_type: LogicalType) -> LogicalTypeKind:
    if isinstance(logical_type, str):
        return LogicalTypeKind(logical_type)
    if isinstance(logical_type, SimpleLogicalType):
        return LogicalTypeKind(logical_type.value)
    if getattr(logical_type, "kind", None) == "decimal":
        return LogicalTypeKind.DECIMAL
    return LogicalTypeKind.LOCAL_DATETIME


def decompress_gzip(
    payload: bytes,
    *,
    max_uncompressed_bytes: int,
    max_expansion_ratio: int | float,
) -> bytes:
    """Single-member gzip only. Corrupt, trailing, concatenated or over-limit streams fail closed."""

    if not payload:
        raise PortError("codec_error", "gzip payload is empty")
    compressed_len = len(payload)
    ratio_cap = int(compressed_len * Decimal(str(max_expansion_ratio))) if compressed_len else 0
    ceiling = min(max_uncompressed_bytes, ratio_cap if ratio_cap > 0 else max_uncompressed_bytes)
    if ceiling < 1:
        raise PortError("codec_error", "gzip expansion ceiling is zero")
    import zlib

    decoder = zlib.decompressobj(16 + zlib.MAX_WBITS)
    try:
        data = decoder.decompress(payload, max_length=ceiling)
    except zlib.error as exc:
        raise PortError("codec_error", f"gzip stream is corrupt: {exc}") from exc
    if not decoder.eof:
        raise PortError("codec_error", "gzip uncompressed size or expansion ratio exceeded")
    if decoder.unused_data:
        raise PortError("codec_error", "gzip stream has trailing or concatenated members")
    return data


def decode_text(payload: bytes, newline: NewlineKind | str) -> str:
    if payload.startswith(BOM):
        raise PortError("framing_error", "UTF-8 BOM is not permitted")
    try:
        text = payload.decode(UTF8)
    except UnicodeDecodeError as exc:
        raise PortError("framing_error", f"payload is not valid UTF-8: {exc}") from exc
    marker = newline_bytes(newline)
    if marker == b"\n":
        if "\r" in text:
            raise PortError("framing_error", "payload newline is not lf")
    else:
        stripped = text.replace("\r\n", "")
        if "\n" in stripped or "\r" in stripped:
            raise PortError("framing_error", "payload newline is not crlf")
    return text


def split_jsonl_frames(payload: bytes, newline: NewlineKind | str) -> tuple[bytes, ...]:
    """Exact configured newline splits; blank frames are illegal; missing final newline is illegal unless empty."""

    decode_text(payload, newline)
    marker = newline_bytes(newline)
    if not payload:
        return ()
    if not payload.endswith(marker):
        raise PortError("framing_error", "JSON Lines payload is missing a terminating newline")
    parts = payload.split(marker)
    frames = tuple(part for part in parts[:-1])
    if any(frame == b"" for frame in frames):
        raise PortError("framing_error", "JSON Lines blank lines are not permitted")
    return frames


def _json_pairs(items: list[tuple[str, object]]) -> dict[str, object]:
    seen: set[str] = set()
    out: dict[str, object] = {}
    for key, value in items:
        if key in seen:
            raise DuplicateKeyError(key)
        seen.add(key)
        out[key] = value
    return out


def parse_json_object(frame: bytes) -> dict[str, object]:
    try:
        text = frame.decode(UTF8)
    except UnicodeDecodeError as exc:
        raise PortError("framing_error", "JSON Lines frame is not valid UTF-8") from exc
    try:
        parsed = json.loads(
            text,
            object_pairs_hook=_json_pairs,
            parse_float=lambda token: Decimal(token),
        )
    except DuplicateKeyError as exc:
        raise PortError("framing_error", f"JSON object has duplicate key {exc.args[0]!r}") from exc
    except json.JSONDecodeError as exc:
        raise PortError("framing_error", f"JSON Lines frame is not valid JSON: {exc}") from exc
    if not isinstance(parsed, dict):
        raise PortError("framing_error", "JSON Lines top_level must be an object")
    return parsed


def _is_null_token(raw: str, null_tokens: tuple[str, ...]) -> bool:
    return raw in null_tokens


def _canonical_int_string(raw: str) -> str | None:
    if not raw or raw == "-0" or raw == "+0" or raw.startswith("+"):
        return None
    if raw[0] == "-":
        body = raw[1:]
        if not body.isdigit() or (len(body) > 1 and body[0] == "0"):
            return None
    else:
        if not raw.isdigit() or (len(raw) > 1 and raw[0] == "0"):
            return None
    try:
        value = int(raw, 10)
    except ValueError:
        return None
    if value < INT64_MIN or value > INT64_MAX:
        return None
    return str(value)


def _canonical_decimal(raw: str, precision: int, scale: int) -> tuple[str, int] | None:
    if raw in {"", "+", "-", ".", "+.", "-."} or raw.startswith("+"):
        return None
    try:
        number = Decimal(raw)
    except InvalidOperation:
        return None
    if not number.is_finite() or number != number:  # noqa: PLR0124
        return None
    sign = "-" if number < 0 else ""
    absolute = -number if number < 0 else number
    text = format(absolute, "f")
    if "e" in text or "E" in text:
        return None
    if "." in text:
        whole, frac = text.split(".", 1)
    else:
        whole, frac = text, ""
    if len(frac) > scale:
        return None
    frac = frac.ljust(scale, "0")
    digits = (whole.lstrip("0") or "0") + frac
    digits = digits.lstrip("0") or "0"
    if len(digits) > precision:
        return None
    unscaled = sign + digits if digits != "0" else "0"
    if unscaled == "-0":
        unscaled = "0"
    return unscaled, scale


def _parse_date(raw: str) -> str | None:
    if len(raw) != DATE_WIDTH:
        return None
    try:
        datetime.strptime(raw, "%Y-%m-%d")
    except ValueError:
        return None
    return raw


def _parse_utc_instant(raw: str) -> str | None:
    if len(raw) != UTC_INSTANT_WIDTH or not raw.endswith("Z"):
        return None
    try:
        datetime.strptime(raw, "%Y-%m-%dT%H:%M:%S.%fZ")
    except ValueError:
        return None
    return raw


def _parse_local_datetime(raw: str, iana_zone: str) -> tuple[str, str] | None:
    if len(raw) != LOCAL_DATETIME_WIDTH or raw.endswith("Z") or "+" in raw:
        return None
    try:
        naive = datetime.strptime(raw, "%Y-%m-%dT%H:%M:%S.%f")
        zone = ZoneInfo(iana_zone)
    except (ValueError, ZoneInfoNotFoundError):
        return None
    dt0 = naive.replace(tzinfo=zone, fold=0)
    dt1 = naive.replace(tzinfo=zone, fold=1)
    if dt0.utcoffset() != dt1.utcoffset():
        return None
    back = dt0.astimezone(timezone.utc).astimezone(zone)
    if (
        back.year, back.month, back.day, back.hour, back.minute, back.second, back.microsecond,
    ) != (naive.year, naive.month, naive.day, naive.hour, naive.minute, naive.second, naive.microsecond):
        return None
    return raw, iana_zone


def coerce_text(
    raw: str | None,
    logical_type: LogicalType,
    *,
    nullable: bool,
    null_tokens: tuple[str, ...] = (),
) -> tuple[TypedScalar | None, DiagnosticCode | None]:
    """Coerce one untrimmed decoded field. ``raw is None`` is JSON null."""

    if raw is None or (isinstance(raw, str) and _is_null_token(raw, null_tokens)):
        if nullable:
            return None, None
        return None, DiagnosticCode.NULL_NOT_ALLOWED

    kind = logical_type_kind(logical_type)
    if kind is LogicalTypeKind.UTF8_STRING:
        return TypedString(logical_type="utf8_string", value=raw), None
    if kind is LogicalTypeKind.BOOLEAN:
        if raw not in _BOOLEAN_TOKENS:
            return None, DiagnosticCode.INVALID_LOGICAL_TYPE
        return TypedBoolean(logical_type="boolean", value=_BOOLEAN_TOKENS[raw]), None
    if kind is LogicalTypeKind.INT64:
        canonical = _canonical_int_string(raw)
        if canonical is None:
            return None, DiagnosticCode.INVALID_LOGICAL_TYPE
        return TypedInt64(logical_type="int64", value=canonical), None
    if kind is LogicalTypeKind.DECIMAL:
        parsed = _canonical_decimal(raw, logical_type.precision, logical_type.scale)
        if parsed is None:
            return None, DiagnosticCode.INVALID_LOGICAL_TYPE
        unscaled, scale = parsed
        return TypedDecimal(logical_type="decimal", unscaled=unscaled, scale=scale), None
    if kind is LogicalTypeKind.DATE:
        value = _parse_date(raw)
        if value is None:
            return None, DiagnosticCode.INVALID_LOGICAL_TYPE
        return TypedDate(logical_type="date", value=value), None
    if kind is LogicalTypeKind.UTC_INSTANT:
        value = _parse_utc_instant(raw)
        if value is None:
            return None, DiagnosticCode.INVALID_LOGICAL_TYPE
        return TypedUtcInstant(logical_type="utc_instant", value=value), None
    if kind is LogicalTypeKind.LOCAL_DATETIME:
        parsed = _parse_local_datetime(raw, logical_type.timezone)
        if parsed is None:
            return None, DiagnosticCode.INVALID_LOGICAL_TYPE
        value, zone = parsed
        return TypedLocalDateTime(logical_type="local_datetime", value=value, timezone=zone), None
    if kind is LogicalTypeKind.BINARY:
        if not _is_unpadded_base64url(raw):
            return None, DiagnosticCode.INVALID_LOGICAL_TYPE
        return TypedBinary(logical_type="binary", value=raw), None
    return None, DiagnosticCode.INVALID_LOGICAL_TYPE


def _is_unpadded_base64url(raw: str) -> bool:
    if raw == "":
        return True
    alphabet = set("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_")
    if any(char not in alphabet for char in raw):
        return False
    padding = "=" * (-len(raw) % 4)
    import base64

    try:
        base64.urlsafe_b64decode(raw + padding)
    except (ValueError, OSError):
        return False
    return True


def _finding(
    *,
    kind: FindingKind,
    code: str,
    diagnostic: DiagnosticCode,
    locator: RawLocator,
    field_path: str | None = None,
    expected: LogicalTypeKind | None = None,
    observed: LogicalTypeKind | None = None,
) -> Finding:
    return Finding(
        kind=kind,
        field_path=field_path,
        code=code,
        severity=Severity.ERROR,
        metadata=FindingMetadata(
            diagnostic_code=diagnostic,
            raw_locator=locator,
            expected_logical_type=expected,
            observed_logical_type=observed,
            observed_count=None,
            expected_min_count=None,
            expected_max_count=None,
            duplicate_group_size=None,
        ),
    )


def _locator(sequence: int, offset: int, length: int, line: int) -> RawLocator:
    return RawLocator(
        frame_sequence=str(sequence),
        byte_offset=str(offset),
        byte_length=str(length),
        line_number=str(line),
    )


def _coerce_json_value(
    value: object,
    logical_type: LogicalType,
    nullable: bool,
) -> tuple[TypedScalar | None, DiagnosticCode | None]:
    kind = logical_type_kind(logical_type)
    if value is None:
        return coerce_text(None, logical_type, nullable=nullable)
    if isinstance(value, bool):
        if kind is not LogicalTypeKind.BOOLEAN:
            return None, DiagnosticCode.INVALID_LOGICAL_TYPE
        return TypedBoolean(logical_type="boolean", value=value), None
    if isinstance(value, str):
        if kind is LogicalTypeKind.BOOLEAN:
            return None, DiagnosticCode.INVALID_LOGICAL_TYPE
        return coerce_text(value, logical_type, nullable=nullable)
    if isinstance(value, int) or isinstance(value, Decimal):
        if kind not in {LogicalTypeKind.INT64, LogicalTypeKind.DECIMAL}:
            return None, DiagnosticCode.INVALID_LOGICAL_TYPE
        raw = str(value) if isinstance(value, int) else format(value, "f")
        return coerce_text(raw, logical_type, nullable=nullable)
    return None, DiagnosticCode.INVALID_LOGICAL_TYPE


def _row_fields(
    values: dict[str, object] | dict[str, str | None],
    columns: tuple[SourceField, ...],
    locator: RawLocator,
    *,
    json_mode: bool,
    null_tokens: tuple[str, ...] = (),
) -> tuple[tuple[tuple[str, TypedScalar | None], ...], tuple[Finding, ...]]:
    findings: list[Finding] = []
    fields: list[tuple[str, TypedScalar | None]] = []
    declared = {column.name: column for column in columns}
    extra = [name for name in values if name not in declared]
    if extra:
        findings.append(_finding(
            kind=FindingKind.PARSE, code="codec_error", diagnostic=DiagnosticCode.UNEXPECTED_FIELD,
            locator=locator, field_path=f"/{extra[0]}",
        ))
        return (), tuple(findings)
    for column in columns:
        if column.name not in values:
            if column.nullable:
                fields.append((column.name, None))
                continue
            findings.append(_finding(
                kind=FindingKind.PARSE, code="codec_error",
                diagnostic=DiagnosticCode.MISSING_REQUIRED_FIELD,
                locator=locator, field_path=f"/{column.name}",
                expected=logical_type_kind(column.logical_type),
            ))
            continue
        raw = values[column.name]
        if json_mode:
            typed, diagnostic = _coerce_json_value(raw, column.logical_type, column.nullable)
        else:
            typed, diagnostic = coerce_text(
                raw if isinstance(raw, str) or raw is None else str(raw),
                column.logical_type,
                nullable=column.nullable,
                null_tokens=null_tokens,
            )
        if diagnostic is not None:
            findings.append(_finding(
                kind=FindingKind.TYPE if diagnostic is DiagnosticCode.INVALID_LOGICAL_TYPE else FindingKind.PARSE,
                code="codec_error",
                diagnostic=diagnostic,
                locator=locator,
                field_path=f"/{column.name}",
                expected=logical_type_kind(column.logical_type),
            ))
            fields.append((column.name, None))
        else:
            fields.append((column.name, typed))
    return tuple(fields), tuple(findings)


def parse_jsonl(
    payload: bytes,
    codec: JsonlCodec,
    columns: tuple[SourceField, ...],
    *,
    sequence_field: str | None = None,
) -> ParseResult:
    frames_bytes = split_jsonl_frames(payload, codec.newline)
    frames: list[ParsedFrame] = []
    offset = 0
    marker_len = len(newline_bytes(codec.newline))
    seen_sequences: set[int] = set()
    previous: int | None = None
    for index, raw in enumerate(frames_bytes):
        line_number = index + 1
        locator = _locator(index, offset, len(raw), line_number)
        offset += len(raw) + marker_len
        parsed = parse_json_object(raw)
        sequence = index
        if sequence_field is not None:
            claimed = parsed.get(sequence_field)
            if isinstance(claimed, bool):
                raise PortError("framing_error", "CDC sequence is missing or not a canonical int64")
            if isinstance(claimed, int):
                if claimed < INT64_MIN or claimed > INT64_MAX:
                    raise PortError("framing_error", "CDC sequence is missing or not a canonical int64")
                sequence = claimed
            elif isinstance(claimed, str) and _canonical_int_string(claimed) is not None:
                sequence = int(claimed, 10)
            else:
                raise PortError("framing_error", "CDC sequence is missing or not a canonical int64")
            if sequence in seen_sequences or (previous is not None and sequence <= previous):
                raise PortError("framing_error", "CDC sequences must be unique and strictly increasing")
            seen_sequences.add(sequence)
            previous = sequence
        extra = [name for name in parsed if name not in {column.name for column in columns}]
        if extra:
            raise PortError("framing_error", f"JSON object has unexpected field {extra[0]!r}")
        fields, findings = _row_fields(parsed, columns, locator, json_mode=True)
        frames.append(ParsedFrame(
            frame_sequence=str(sequence),
            raw_locator=locator,
            fields=fields,
            findings=findings,
            raw_bytes=raw,
        ))
    return ParseResult(frames=tuple(frames), batch_findings=())


def _csv_records(text: str, codec: CsvCodec) -> Iterator[tuple[list[str], int, int]]:
    delimiter = codec.delimiter
    quote = codec.quote
    escape = codec.escape
    newline = "\r\n" if codec.newline.value == "crlf" else "\n"
    if len(delimiter) != 1 or len(quote) != 1 or len(escape) != 1:
        raise PortError("codec_error", "CSV delimiter, quote and escape must be single characters")
    if len({delimiter, quote, escape}) != 3:
        raise PortError("codec_error", "CSV delimiter, quote and escape must be distinct")
    fields: list[str] = []
    current: list[str] = []
    in_quotes = False
    escaped = False
    record_start = 0
    byte_pos = 0
    index = 0
    length = len(text)
    newline_byte_len = len(newline.encode(UTF8))
    while index < length:
        char = text[index]
        char_bytes = len(char.encode(UTF8))
        if escaped:
            current.append(char)
            escaped = False
            index += 1
            byte_pos += char_bytes
            continue
        if char == escape:
            escaped = True
            index += 1
            byte_pos += char_bytes
            continue
        if in_quotes:
            if char == quote:
                in_quotes = False
                index += 1
                byte_pos += char_bytes
                continue
            current.append(char)
            index += 1
            byte_pos += char_bytes
            continue
        if char == quote:
            in_quotes = True
            index += 1
            byte_pos += char_bytes
            continue
        if char == delimiter:
            fields.append("".join(current))
            current = []
            index += 1
            byte_pos += char_bytes
            continue
        if text.startswith(newline, index):
            fields.append("".join(current))
            yield fields, record_start, byte_pos - record_start
            fields = []
            current = []
            index += len(newline)
            byte_pos += newline_byte_len
            record_start = byte_pos
            continue
        if char in "\r\n":
            raise PortError("framing_error", "CSV record uses the wrong newline")
        current.append(char)
        index += 1
        byte_pos += char_bytes
    if escaped or in_quotes:
        raise PortError("framing_error", "CSV record is unterminated")
    if current or fields:
        fields.append("".join(current))
        yield fields, record_start, byte_pos - record_start


def parse_csv(
    payload: bytes,
    codec: CsvCodec,
    columns: tuple[SourceField, ...],
) -> ParseResult:
    text = decode_text(payload, codec.newline)
    for token in codec.null_tokens:
        if unicodedata.normalize("NFC", token) != token or len(token.encode(UTF8)) > 128:
            raise PortError("codec_error", "CSV null token is not NFC or exceeds 128 UTF-8 bytes")
    if len(set(codec.null_tokens)) != len(codec.null_tokens):
        raise PortError("codec_error", "CSV null tokens must be duplicate-free")
    records = list(_csv_records(text, codec))
    header: list[str] | None = None
    data_records = records
    if codec.header:
        if not records:
            raise PortError("framing_error", "CSV header is missing")
        header, _, _ = records[0]
        expected = [column.name for column in columns]
        if len(header) > len(expected) or header != expected[: len(header)]:
            raise PortError("framing_error", "CSV header does not match the declared physical columns")
        missing = expected[len(header):]
        by_name = {column.name: column for column in columns}
        if any(not by_name[name].nullable for name in missing):
            raise PortError("framing_error", "CSV header is missing a required physical column")
        data_records = records[1:]
    frames: list[ParsedFrame] = []
    for index, (values, start, length) in enumerate(data_records):
        line_number = index + (2 if codec.header else 1)
        locator = _locator(index, start, length, line_number)
        if len(values) > len(columns):
            raise PortError("framing_error", "CSV row has more columns than the physical schema")
        mapped: dict[str, str | None] = {}
        for position, column in enumerate(columns):
            if position < len(values):
                mapped[column.name] = values[position]
        fields, findings = _row_fields(
            mapped, columns, locator, json_mode=False, null_tokens=codec.null_tokens,
        )
        raw_slice = payload[start:start + length]
        frames.append(ParsedFrame(
            frame_sequence=str(index),
            raw_locator=locator,
            fields=fields,
            findings=findings,
            raw_bytes=raw_slice,
        ))
    return ParseResult(frames=tuple(frames), batch_findings=())


def parse_payload(
    payload: bytes,
    codec: Codec,
    columns: tuple[SourceField, ...],
    *,
    sequence_field: str | None = None,
) -> ParseResult:
    if codec.kind == "jsonl":
        return parse_jsonl(payload, codec, columns, sequence_field=sequence_field)
    return parse_csv(payload, codec, columns)


def decode_transport(
    payload: bytes,
    content_encoding: str,
    *,
    max_uncompressed_bytes: int,
    max_expansion_ratio: int | float,
) -> bytes:
    if content_encoding == "identity":
        return payload
    if content_encoding == "gzip":
        return decompress_gzip(
            payload,
            max_uncompressed_bytes=max_uncompressed_bytes,
            max_expansion_ratio=max_expansion_ratio,
        )
    raise PortError("capability_mismatch", f"unsupported content encoding {content_encoding!r}")


__all__ = [
    "CDC_FINGERPRINT_PREFIX",
    "OBJECT_FINGERPRINT_PREFIX",
    "ParseResult",
    "ParsedFrame",
    "coerce_text",
    "decode_text",
    "decode_transport",
    "decompress_gzip",
    "frame_sequence_digest",
    "logical_type_kind",
    "newline_bytes",
    "parse_csv",
    "parse_json_object",
    "parse_jsonl",
    "parse_payload",
    "split_jsonl_frames",
    "transport_payload_fingerprint",
]
