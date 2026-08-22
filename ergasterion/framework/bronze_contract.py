"""Closed Pydantic projections of the frozen Bronze portable IDL, vocabulary and
contract-declaration half.

The single structural authority is ``docs/specifications/bronze-portable-idl-v1.json``
(schema ``ergasterion.portable-idl/v1``, pinned at ``EXPECTED_IDL_SHA256`` below). This
module never authors or edits that file; it projects it into three things a Python
caller needs: typed scalar constraints, the closed enum vocabulary, and the
contract-declaration record family (``BronzeProductContract`` and everything it is
built from). ``ergasterion.framework.runtime_binding`` and ``ergasterion.ingestion.records``
build on top of this module for the runtime-binding and delivery/state record families;
the three modules together cover every record the IDL declares.

Every class here is a *structural* (wire-shape) projection: type, requiredness,
nullability, closedness (``extra="forbid"``) and the IDL's own ``const``/pattern/range
scalar constraints. The IDL's list ``ordering`` hints (``declared``, ``set``, ``ordered``,
canonical digest rules) belong to contract compilation. This module does not sort,
deduplicate, or digest anything.

This module performs no file I/O at import time -- it is safe to import from a wheel
install with no ``docs/`` tree present. ``load_idl()`` and the schema/equivalence
generators below read the real IDL file from an explicit path supplied by the caller
(``tests/python/test_bronze_schema.py`` calls them from a repository checkout); nothing
here reads that path implicitly.
"""

from __future__ import annotations

import re
from enum import Enum
from typing import Annotated, ClassVar, Literal, Union

from pydantic import AfterValidator, BaseModel, ConfigDict, Field, StringConstraints, model_validator

from ergasterion.framework.models import HandoffSchemaId, PatternId

# --------------------------------------------------------------------------- IDL pin

EXPECTED_IDL_SHA256 = "fe933cd4b51cf8a17cd166295eb99f739ef8111684243ed8f9af87160fb588e4"
"""Git-blob-byte SHA-256 of ``docs/specifications/bronze-portable-idl-v1.json``. Every
generator/equivalence entry point in this file family asserts the real file hashes to
this value before trusting anything it parses from it."""


# --------------------------------------------------------------------------- base

class ClosedModel(BaseModel):
    """Base for every Bronze IDL record projection: closed (unknown fields rejected),
    immutable, and able to reject an explicit JSON ``null`` on a field the IDL marks
    ``required: false, nullable: false`` -- a field that may be *omitted* but, if
    present, must carry a real value. Subclasses declare that field-name set on
    ``_omittable_not_nullable``; the two other optional/nullable combinations
    (``required: true`` and/or ``nullable: true``) are already exact under ordinary
    Python typing and need no extra check."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    _omittable_not_nullable: ClassVar[frozenset[str]] = frozenset()

    @model_validator(mode="before")
    @classmethod
    def _reject_null_on_omittable_fields(cls, data: object) -> object:
        if isinstance(data, dict):
            for name in cls._omittable_not_nullable:
                if name in data and data[name] is None:
                    raise ValueError(
                        f"{cls.__name__}.{name} is optional but not nullable "
                        "(required: false, nullable: false in the IDL): omit the field "
                        "rather than sending JSON null"
                    )
        return data


# --------------------------------------------------------------------------- scalars
#
# One Python type alias per IDL scalar (``docs/specifications/bronze-portable-idl-v1.json``
# ``scalars``), named identically for direct traceability from a field's annotation back
# to its IDL scalar. ``DateScalar`` is spelled out (not ``Date``) only to avoid shadowing
# the stdlib name in this module's namespace; the wire scalar name is still ``Date``.

StringScalar = str
BooleanScalar = bool
SafeInteger = Annotated[int, Field(ge=-9007199254740991, le=9007199254740991)]
PositiveInteger = Annotated[int, Field(ge=1, le=9007199254740991)]
NonNegativeInteger = Annotated[int, Field(ge=0, le=9007199254740991)]
IntegerString = Annotated[str, StringConstraints(pattern=r"^(0|[1-9][0-9]*|-[1-9][0-9]*)$")]
NonNegativeIntegerString = Annotated[str, StringConstraints(pattern=r"^(0|[1-9][0-9]*)$")]
PositiveIntegerString = Annotated[str, StringConstraints(pattern=r"^[1-9][0-9]*$")]
DecimalString = Annotated[str, StringConstraints(pattern=r"^-?(0|[1-9][0-9]*)(\.[0-9]+)?$")]
Digest = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]
ContentId = Annotated[str, StringConstraints(pattern=r"^sha256:[0-9a-f]{64}$")]
Base64Url = Annotated[str, StringConstraints(pattern=r"^[A-Za-z0-9_-]+$")]
ByteStringBase64Url = Annotated[str, StringConstraints(pattern=r"^[A-Za-z0-9_-]*$")]
Token = Annotated[str, StringConstraints(pattern=r"^[a-z][a-z0-9._:-]{0,126}$")]
Identifier = Annotated[str, StringConstraints(pattern=r"^[a-z_][a-z0-9_]*$")]
_ESTATE_NAMESPACE_RE = re.compile(
    r"^(?=.{3,253}$)[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)+$"
)


def _validate_estate_namespace(value: str) -> str:
    # pydantic-core's built-in ``pattern=`` constraint runs on the Rust `regex` crate,
    # which rejects the IDL's length look-ahead (``(?=.{3,253}$)``); this scalar alone
    # is checked with a plain Python ``re`` validator instead of ``StringConstraints``.
    if not _ESTATE_NAMESPACE_RE.match(value):
        raise ValueError(f"{value!r} does not match the EstateNamespace pattern")
    return value


EstateNamespace = Annotated[str, AfterValidator(_validate_estate_namespace)]
SemVer = Annotated[
    str,
    StringConstraints(
        pattern=r"^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(?:-[0-9A-Za-z.-]+)?(?:\+[0-9A-Za-z.-]+)?$"
    ),
]
UtcInstant = Annotated[str, StringConstraints(pattern=r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{6}Z$")]
DateScalar = Annotated[str, StringConstraints(pattern=r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$")]
FileMode = Annotated[str, StringConstraints(pattern=r"^[0-7]{6}$")]
JsonPointer = Annotated[str, StringConstraints(pattern=r"^(|/(?:[^~/]|~[01])*)$")]
OpaqueRef = Annotated[str, StringConstraints(min_length=1, max_length=512)]

ERROR_CODES: tuple[str, ...] = (
    "access_denied", "ancestry_mismatch", "attestation_invalid", "authorization_denied",
    "bronze_store_restore_required", "capability_mismatch", "capacity_exceeded",
    "claim_conflict", "codec_error", "concurrency_conflict", "contract_conflict",
    "contract_invalid", "decision_conflict", "event_conflict", "evidence_conflict",
    "framing_error", "inflight_attempt", "integrity_error", "intent_conflict",
    "invalid_config", "invalid_manifest", "invalid_signature", "invalid_usage",
    "item_too_large", "key_commitment_conflict", "key_not_found", "key_revoked",
    "migration_conflict", "missing_extra", "not_found", "policy_not_authorized",
    "production_policy_adapter_required", "projection_conflict", "projection_gap",
    "release_conflict", "row_attribution_error", "schema_invalid", "scope_closed",
    "scope_conflict", "scope_open", "scope_owner_mismatch", "sequence_conflict",
    "stale_revision", "superseded_contract", "superseded_deployment", "target_unavailable",
    "throttled", "transient_io", "unconfirmed_projection_evidence_lost",
    "unconfirmed_revision", "unsupported_layer", "unsupported_optional_pattern",
    "unsupported_secondary_target", "unsupported_store_migration",
)
ErrorCode = Literal[ERROR_CODES]  # type: ignore[valid-type]


# --------------------------------------------------------------------------- enums
#
# Every IDL enum except ``PatternId`` and ``HandoffSchemaId``. Those two already exist as
# canonical vocabulary in ``ergasterion.framework.models`` and are re-exported here under
# their IDL names for a caller that only knows this module.

PatternIdEnum = PatternId
HandoffSchemaIdEnum = HandoffSchemaId


class DeliveryMode(str, Enum):
    CDC = "cdc"
    APPEND_ONLY = "append_only"
    COMPLETE_SNAPSHOT = "complete_snapshot"


class DeliveryInputKind(str, Enum):
    MANAGED_PAYLOAD = "managed_payload"
    EXTERNAL_RECEIPT = "external_receipt"


class IntegrationKind(str, Enum):
    EXTERNAL = "external"
    MANAGED = "managed"


class CodecKind(str, Enum):
    CSV = "csv"
    JSONL = "jsonl"


class CapabilityCodecKind(str, Enum):
    CSV_V1 = "csv_v1"
    JSONL_V1 = "jsonl_v1"


class ContentEncoding(str, Enum):
    IDENTITY = "identity"
    GZIP = "gzip"


class NewlineKind(str, Enum):
    CRLF = "crlf"
    LF = "lf"


class BackoffKind(str, Enum):
    EXPONENTIAL = "exponential"
    FIXED = "fixed"


class MediaType(str, Enum):
    NDJSON = "application/x-ndjson"
    CSV = "text/csv"


class LogicalTypeKind(str, Enum):
    BINARY = "binary"
    BOOLEAN = "boolean"
    DATE = "date"
    DECIMAL = "decimal"
    INT64 = "int64"
    LOCAL_DATETIME = "local_datetime"
    UTC_INSTANT = "utc_instant"
    UTF8_STRING = "utf8_string"


class SimpleLogicalType(str, Enum):
    BINARY = "binary"
    BOOLEAN = "boolean"
    DATE = "date"
    INT64 = "int64"
    UTC_INSTANT = "utc_instant"
    UTF8_STRING = "utf8_string"


class ProgressKind(str, Enum):
    OPAQUE_BATCH = "opaque_batch"
    SEQUENCE = "sequence"


class DeleteStrategy(str, Enum):
    EXPLICIT_TOMBSTONE = "explicit_tombstone"
    NONE = "none"
    SNAPSHOT_DIFF = "snapshot_diff"


class PublicationPolicy(str, Enum):
    ALL_OR_NOTHING = "all_or_nothing"
    PUBLISH_VALID_ROWS = "publish_valid_rows"


class PublicationDecision(str, Enum):
    PUBLISH_ALL = "publish_all"
    PUBLISH_VALID_ROWS = "publish_valid_rows"
    REJECT_DELIVERY = "reject_delivery"


class Severity(str, Enum):
    ERROR = "error"
    WARN = "warn"


class RuleKind(str, Enum):
    ACCEPTED_VALUES = "accepted_values"
    NOT_NULL = "not_null"
    RANGE = "range"
    ROW_COUNT = "row_count"
    UNIQUE_KEY = "unique_key"


class PortKind(str, Enum):
    KEY_RESOLVER = "key_resolver"
    LANDING_ADAPTER = "landing_adapter"
    LIFECYCLE_SINK = "lifecycle_sink"
    PROJECTION_PUBLISHER = "projection_publisher"
    RAW_STORE = "raw_store"
    REMEDIATION_REPOSITORY = "remediation_repository"
    SCRATCH_STORE = "scratch_store"
    SOURCE_CONNECTOR = "source_connector"
    STATE_STORE = "state_store"


class ProfileClass(str, Enum):
    PRODUCTION_CANDIDATE = "production_candidate"
    SYNTHETIC_LOCAL_ONLY = "synthetic_local_only"


class BackupRestoreCapability(str, Enum):
    NONE = "none"
    OPERATOR_MANAGED = "operator_managed"
    VERIFIED = "verified"


class SecretBoundary(str, Enum):
    EXTERNAL_RESOLVER = "external_resolver"
    NONE = "none"
    OPAQUE_MAC = "opaque_mac"


class AttemptState(str, Enum):
    COMMIT_BLOCKED = "commit_blocked"
    COMMITTED = "committed"
    COMMITTING = "committing"
    FAILED = "failed"
    MATERIALIZING = "materializing"
    PREPARING = "preparing"
    RECEIVED = "received"
    VALIDATING = "validating"


class BlockPhase(str, Enum):
    OBSERVER_BLOCKED = "observer_blocked"
    PROJECTION_BLOCKED = "projection_blocked"


class ProcessingOutcome(str, Enum):
    BLOCKED = "blocked"
    COMMITTED = "committed"
    FAILED = "failed"
    IN_PROGRESS = "in_progress"
    NONE = "none"


class MigrationKind(str, Enum):
    CARRY = "carry"
    RESET = "reset"


class ProjectionIntentKind(str, Enum):
    DELIVERY_PUBLICATION = "delivery_publication"
    HEARTBEAT = "heartbeat"
    MIGRATION = "migration"
    PROCESSING = "processing"
    REMEDIATION_RELEASE = "remediation_release"
    TIMELINESS = "timeliness"
    WHOLE_DELIVERY_REPROCESSING = "whole_delivery_reprocessing"


class VisibilityKind(str, Enum):
    DELIVERY = "delivery"
    RELEASE = "release"
    REPROCESS = "reprocess"


class TimelinessState(str, Enum):
    AWAITING = "awaiting"
    LATE = "late"
    MISSING = "missing"
    NOT_DUE = "not_due"
    ON_TIME = "on_time"


class ErrorCategory(str, Enum):
    CAPABILITY = "capability"
    CONFIG = "config"
    CONFLICT = "conflict"
    INTEGRITY = "integrity"
    PERMANENT = "permanent"
    RETRYABLE = "retryable"
    USAGE = "usage"


class CommandStatus(str, Enum):
    FAILED = "failed"
    NOOP = "noop"
    OK = "ok"
    RETRYABLE = "retryable"


class BackupAction(str, Enum):
    CREATE = "create"
    RESTORE = "restore"


class FindingKind(str, Enum):
    BATCH_RULE = "batch_rule"
    PARSE = "parse"
    RULE = "rule"
    TYPE = "type"


class DiagnosticCode(str, Enum):
    ACCEPTED_VALUE_VIOLATION = "accepted_value_violation"
    COLUMN_COUNT_MISMATCH = "column_count_mismatch"
    DUPLICATE_KEY = "duplicate_key"
    INVALID_ENCODING = "invalid_encoding"
    INVALID_LOGICAL_TYPE = "invalid_logical_type"
    JSON_PARSE_ERROR = "json_parse_error"
    MISSING_REQUIRED_FIELD = "missing_required_field"
    NULL_NOT_ALLOWED = "null_not_allowed"
    RANGE_VIOLATION = "range_violation"
    ROW_COUNT_VIOLATION = "row_count_violation"
    UNEXPECTED_FIELD = "unexpected_field"


class DispositionStatus(str, Enum):
    ACCEPTED = "accepted"
    REJECTED = "rejected"


class ReadinessResult(str, Enum):
    READY = "ready"
    REJECTED = "rejected"


class GraphOccurrenceRole(str, Enum):
    PHASE = "phase"
    WRAPPER = "wrapper"
    POLICY = "policy"
    OBSERVER = "observer"
    BARRIER = "barrier"


class GraphEdgeRole(str, Enum):
    DATA = "data"
    VALIDATION = "validation"
    READINESS = "readiness"
    BARRIER = "barrier"
    OBSERVE = "observe"


class ContractLifecycleAction(str, Enum):
    REGISTER = "register"
    ACTIVATE = "activate"


class DeploymentLifecycleAction(str, Enum):
    REGISTER = "register"
    ACTIVATE = "activate"


class RemediationDecisionKind(str, Enum):
    EVALUATED = "evaluated"
    RELEASED = "released"


class LifecycleEventType(str, Enum):
    RECEIVED = "received"
    PREPARING = "preparing"
    VALIDATING = "validating"
    MATERIALIZING = "materializing"
    COMMITTING = "committing"
    COMMIT_BLOCKED = "commit_blocked"
    COMMITTED = "committed"
    FAILED = "failed"
    BRONZE_CONTRACT = "bronze.contract"
    BRONZE_SCHEMA = "bronze.schema"
    BRONZE_RECEIPT = "bronze.receipt"
    BRONZE_QUALITY = "bronze.quality"
    BRONZE_QUARANTINE = "bronze.quarantine"
    BRONZE_PUBLICATION = "bronze.publication"
    BRONZE_DELETION_EVIDENCE = "bronze.deletion_evidence"
    BRONZE_LINEAGE = "bronze.lineage"
    BRONZE_METADATA = "bronze.metadata"


class ContractActivationState(str, Enum):
    ACTIVE = "active"
    PENDING_BASELINE = "pending_baseline"


class EvidenceKind(str, Enum):
    ATTEMPT = "attempt"
    CONTRACT = "contract"
    DELETION_EVIDENCE = "deletion_evidence"
    LINEAGE = "lineage"
    METADATA = "metadata"
    PUBLICATION = "publication"
    QUALITY = "quality"
    QUARANTINE = "quarantine"
    RECEIPT = "receipt"
    SCHEMA = "schema"


class OutboxEntryKind(str, Enum):
    EVIDENCE = "evidence"
    LIFECYCLE = "lifecycle"
    PROJECTION = "projection"


class OutboxStatus(str, Enum):
    PENDING = "pending"
    LEASED = "leased"
    RETRYABLE = "retryable"
    DEAD_LETTER = "dead_letter"
    COMPLETE = "complete"


class OutboxFailureDisposition(str, Enum):
    DEAD_LETTER = "dead_letter"
    RETRYABLE = "retryable"


class SnapshotReconciliationStatus(str, Enum):
    NOT_APPLICABLE = "not_applicable"
    PENDING = "pending"
    LEASED = "leased"
    RETRYABLE = "retryable"
    DEAD_LETTER = "dead_letter"
    COMPLETE = "complete"


class QuarantineAction(str, Enum):
    LIST = "list"
    REVALIDATE = "revalidate"
    RELEASE = "release"


class RemediationActionStatus(str, Enum):
    LISTED = "listed"
    UNCHANGED_FINDING = "unchanged_finding"
    REVALIDATED = "revalidated"
    RELEASED = "released"


class TranslationRole(str, Enum):
    EXECUTION_OWNER = "execution_owner"
    OBSERVER = "observer"
    PROJECTION_CONSUMER = "projection_consumer"


class ConformanceResult(str, Enum):
    CONFORMANT = "conformant"
    REJECTED = "rejected"


class HandoffRecordType(str, Enum):
    RAW_EVIDENCE_HANDOFF = "RawEvidenceHandoff"
    VALIDATION_RESULT_HANDOFF = "ValidationResultHandoff"
    CONTRACT_CONFORMANCE_HANDOFF = "ContractConformanceHandoff"
    INTERFACE_READINESS_HANDOFF = "InterfaceReadinessHandoff"
    PUBLICATION_CONFIRMATION_HANDOFF = "PublicationConfirmationHandoff"


# The IDL's ``handoff_schema_bindings`` section: which record type crosses an edge
# carrying a given handoff schema id. ``_handoff_schema_binding_checks`` in
# ``ergasterion.ingestion.records`` verifies this pairing against the IDL exactly.
HANDOFF_SCHEMA_BINDINGS: dict[HandoffSchemaId, HandoffRecordType] = {
    HandoffSchemaId.RAW_EVIDENCE: HandoffRecordType.RAW_EVIDENCE_HANDOFF,
    HandoffSchemaId.VALIDATION_RESULT: HandoffRecordType.VALIDATION_RESULT_HANDOFF,
    HandoffSchemaId.CONTRACT_CONFORMANCE: HandoffRecordType.CONTRACT_CONFORMANCE_HANDOFF,
    HandoffSchemaId.INTERFACE_READINESS: HandoffRecordType.INTERFACE_READINESS_HANDOFF,
    HandoffSchemaId.PUBLICATION_CONFIRMATION: HandoffRecordType.PUBLICATION_CONFIRMATION_HANDOFF,
}


# --------------------------------------------------------------------------- shared low-level records
#
# ``Finding``/``FindingMetadata`` are shared vocabulary (validation, disposition, batch
# findings, conformance results all carry them), so they live in this base module rather
# than in the ingestion-side record family that depends on it.

class RawLocator(ClosedModel):
    frame_sequence: NonNegativeIntegerString
    byte_offset: NonNegativeIntegerString | None
    byte_length: NonNegativeIntegerString | None
    line_number: PositiveIntegerString | None


class FindingMetadata(ClosedModel):
    diagnostic_code: DiagnosticCode
    raw_locator: RawLocator | None
    expected_logical_type: LogicalTypeKind | None
    observed_logical_type: LogicalTypeKind | None
    observed_count: NonNegativeIntegerString | None
    expected_min_count: NonNegativeIntegerString | None
    expected_max_count: NonNegativeIntegerString | None
    duplicate_group_size: PositiveIntegerString | None


class Finding(ClosedModel):
    kind: FindingKind
    rule_id: Digest | None = None
    field_path: JsonPointer | None = None
    code: ErrorCode
    severity: Severity
    metadata: FindingMetadata

    _omittable_not_nullable = frozenset({"rule_id", "field_path"})


# --------------------------------------------------------------------------- typed scalars / logical type

class TypedBoolean(ClosedModel):
    logical_type: Literal["boolean"]
    value: BooleanScalar


class TypedInt64(ClosedModel):
    logical_type: Literal["int64"]
    value: IntegerString


class TypedDecimal(ClosedModel):
    logical_type: Literal["decimal"]
    unscaled: IntegerString
    scale: NonNegativeInteger


class TypedString(ClosedModel):
    logical_type: Literal["utf8_string"]
    value: StringScalar


class TypedDate(ClosedModel):
    logical_type: Literal["date"]
    value: DateScalar


class TypedUtcInstant(ClosedModel):
    logical_type: Literal["utc_instant"]
    value: UtcInstant


class TypedLocalDateTime(ClosedModel):
    logical_type: Literal["local_datetime"]
    value: StringScalar
    timezone: StringScalar


class TypedBinary(ClosedModel):
    logical_type: Literal["binary"]
    value: ByteStringBase64Url


TypedScalar = Annotated[
    Union[
        TypedBinary, TypedBoolean, TypedDate, TypedDecimal, TypedInt64,
        TypedLocalDateTime, TypedString, TypedUtcInstant,
    ],
    Field(discriminator="logical_type"),
]
"""The ``TypedScalar`` union (discriminator ``logical_type``, eight variants)."""


class DecimalType(ClosedModel):
    kind: Literal["decimal"]
    precision: PositiveInteger
    scale: NonNegativeInteger


class LocalDateTimeType(ClosedModel):
    kind: Literal["local_datetime"]
    timezone: StringScalar


LogicalType = Union[SimpleLogicalType, DecimalType, LocalDateTimeType]
"""The ``LogicalType`` union: ``token_or_object`` style. A bare token equal to a
``SimpleLogicalType`` member, or an object discriminated on ``kind`` for the two
parameterised variants (``DecimalType``, ``LocalDateTimeType``). Not a discriminated
union in the Pydantic sense (the token branch carries no discriminator field), so it is
typed as a plain ``Union``; Pydantic's smart-union mode resolves a string input against
the enum branch and a mapping input against the two object branches."""


class SourceField(ClosedModel):
    name: Identifier
    logical_type: LogicalType
    nullable: BooleanScalar


# --------------------------------------------------------------------------- identity / product facts

class LogicalIdentity(ClosedModel):
    estate_namespace: EstateNamespace
    source: Identifier
    table: Identifier


class ProductFacts(ClosedModel):
    product_version: SemVer
    display_name: StringScalar
    description: StringScalar
    owner: Token
    domain: Token
    support: Token
    classification: Token
    access_policy_ref: Token
    retention_policy_ref: Token


# --------------------------------------------------------------------------- codecs

class CsvCodec(ClosedModel):
    kind: Literal["csv"]
    version: Literal[1]
    charset: Literal["utf-8"]
    delimiter: StringScalar
    header: BooleanScalar
    quote: StringScalar
    escape: StringScalar
    newline: NewlineKind
    null_tokens: tuple[StringScalar, ...]
    trim_whitespace: Literal[False]


class JsonlCodec(ClosedModel):
    kind: Literal["jsonl"]
    version: Literal[1]
    charset: Literal["utf-8"]
    newline: NewlineKind
    top_level: Literal["object"]
    duplicate_keys: Literal["reject"]
    number_mode: Literal["exact_decimal"]
    allow_blank_lines: Literal[False]


Codec = Annotated[Union[CsvCodec, JsonlCodec], Field(discriminator="kind")]


# --------------------------------------------------------------------------- integration

class ManagedIntegration(ClosedModel):
    kind: Literal["managed"]


class ExternalTrustPolicy(ClosedModel):
    policy_ref: Token
    allowed_key_ids: tuple[Token, ...]
    future_clock_skew_seconds: NonNegativeInteger


class ExternalIntegration(ClosedModel):
    kind: Literal["external"]
    delivery_id_column: Identifier
    visibility_epoch_column: Identifier
    visibility_kind_column: Identifier
    visibility_id_column: Identifier
    raw_reference_field: Identifier
    candidate_reference_field: Identifier
    frame_index_reference_field: Identifier
    receipt_trust: ExternalTrustPolicy


Integration = Annotated[Union[ExternalIntegration, ManagedIntegration], Field(discriminator="kind")]


class ProjectionField(ClosedModel):
    source: Identifier
    name: Identifier
    logical_type: LogicalType
    nullable: BooleanScalar


class LandingContract(ClosedModel):
    kind: Literal["source"]
    source_name: Identifier
    identifier: Identifier
    integration: Integration
    content_encodings: tuple[ContentEncoding, ...]
    codec: Codec
    physical_columns: tuple[SourceField, ...]


# --------------------------------------------------------------------------- schedule / delivery policy

class IntervalSchedule(ClosedModel):
    kind: Literal["interval"]
    every_minutes: PositiveInteger
    anchor_at: UtcInstant


class CronSchedule(ClosedModel):
    kind: Literal["cron"]
    expression: StringScalar
    timezone: StringScalar
    starts_at: UtcInstant
    timezone_data_version: Literal["2026.2"]


Schedule = Annotated[Union[CronSchedule, IntervalSchedule], Field(discriminator="kind")]


class SlaContract(ClosedModel):
    warn_after_minutes: NonNegativeInteger
    error_after_minutes: PositiveInteger


class TimestampContract(ClosedModel):
    load_field: Identifier
    event_field: Identifier | None = None
    effective_field: Identifier | None = None

    _omittable_not_nullable = frozenset({"event_field", "effective_field"})


class SequenceProgress(ClosedModel):
    kind: Literal["sequence"]
    field: Identifier


class OpaqueBatchProgress(ClosedModel):
    kind: Literal["opaque_batch"]


ProgressContract = Annotated[Union[OpaqueBatchProgress, SequenceProgress], Field(discriminator="kind")]


class FingerprintScope(ClosedModel):
    scope_id: Token
    scope_parameters: dict[str, TypedScalar]


class RecordKeyContract(ClosedModel):
    fields: tuple[Identifier, ...]
    fingerprint_scope: FingerprintScope | None = None
    hmac_key_id: Token | None = None

    _omittable_not_nullable = frozenset({"fingerprint_scope", "hmac_key_id"})


class RetryPolicy(ClosedModel):
    max_attempts: PositiveInteger
    backoff: BackoffKind
    base_seconds: PositiveInteger
    cap_seconds: PositiveInteger


class NotNullRule(ClosedModel):
    kind: Literal["not_null"]
    field: Identifier
    severity: Severity


class UniqueKeyRule(ClosedModel):
    kind: Literal["unique_key"]
    fields: tuple[Identifier, ...]
    severity: Severity


class AcceptedValuesRule(ClosedModel):
    kind: Literal["accepted_values"]
    field: Identifier
    values: tuple[TypedScalar, ...]
    allow_null: BooleanScalar
    severity: Severity


class RangeRule(ClosedModel):
    kind: Literal["range"]
    field: Identifier
    min: TypedScalar | None = None
    max: TypedScalar | None = None
    allow_null: BooleanScalar
    severity: Severity

    _omittable_not_nullable = frozenset({"min", "max"})


class RowCountRule(ClosedModel):
    kind: Literal["row_count"]
    min: NonNegativeIntegerString | None = None
    max: NonNegativeIntegerString | None = None
    severity: Severity

    _omittable_not_nullable = frozenset({"min", "max"})


QualityRule = Annotated[
    Union[AcceptedValuesRule, NotNullRule, RangeRule, RowCountRule, UniqueKeyRule],
    Field(discriminator="kind"),
]


class QualityPolicy(ClosedModel):
    publication_mode: PublicationPolicy
    max_error_fraction: DecimalString
    rules: tuple[QualityRule, ...]


class TombstoneContract(ClosedModel):
    field: Identifier
    values: tuple[TypedScalar, ...]


class SnapshotContract(ClosedModel):
    scope_id: Token
    scope_parameters: dict[str, TypedScalar]
    attestation_policy_ref: Token
    allowed_key_ids: tuple[Token, ...]
    future_clock_skew_seconds: NonNegativeInteger


class DeliveryPolicy(ClosedModel):
    kind: Literal["production"]
    mode: DeliveryMode
    progress: ProgressContract
    delete_strategy: DeleteStrategy
    schedule: Schedule
    schedule_lateness: SlaContract
    maximum_age: SlaContract | None = None
    timestamps: TimestampContract
    record_key: RecordKeyContract
    tombstone: TombstoneContract | None = None
    snapshot: SnapshotContract | None = None
    quality: QualityPolicy
    retry: RetryPolicy

    _omittable_not_nullable = frozenset({"maximum_age", "tombstone", "snapshot"})


class BronzeInterfaces(ClosedModel):
    raw: Token
    source_native: Token
    published: Token
    quarantine: Token
    deletion_evidence: Token


class BronzeProductContract(ClosedModel):
    """The Bronze Product Contract: a source's identity, product facts, source-native
    schema/codec, delivery policy and published projection, authored once (IDL record
    ``BronzeProductContract``, schema token ``ergasterion.bronze-product/v1``)."""

    schema_: Literal["ergasterion.bronze-product/v1"] = Field(alias="schema")
    logical_identity: LogicalIdentity
    product: ProductFacts
    landing: LandingContract
    delivery: DeliveryPolicy
    projection: tuple[ProjectionField, ...]
    interfaces: BronzeInterfaces

    model_config = ConfigDict(extra="forbid", frozen=True, populate_by_name=True)


# --------------------------------------------------------------------------- execution graph (IDL wire shape)
#
# Distinct from ``ergasterion.framework.models.ExecutionPlan``, the framework's own
# internal graph-shape dataclass. This class is the IDL's wider wire record: it adds
# ``logical_identity``, ``product_version`` and the three digest fields the internal
# graph-shape digest does not cover. Access it as ``bronze_contract.ExecutionPlan`` to
# keep the two apart; neither module re-exports the other's ``ExecutionPlan`` under a
# shared unqualified name.

class GraphOccurrence(ClosedModel):
    occurrence_id: Token
    pattern_id: PatternId
    roles: tuple[GraphOccurrenceRole, ...]
    phase_ordinal: NonNegativeInteger
    members: tuple[Token, ...]
    execution_owner_required: BooleanScalar


class GraphEdge(ClosedModel):
    from_occurrence: Token
    to_occurrence: Token
    role: GraphEdgeRole
    handoff_schema_id: HandoffSchemaId


class HandoffSchema(ClosedModel):
    schema_id: HandoffSchemaId
    record_type: HandoffRecordType
    schema_digest: Digest


class ExecutionPlan(ClosedModel):
    schema_: Literal["ergasterion.execution-plan/v1"] = Field(alias="schema")
    logical_identity: LogicalIdentity
    product_version: SemVer
    contract_digest: Digest
    source_schema_digest: Digest
    published_schema_digest: Digest
    occurrences: tuple[GraphOccurrence, ...]
    edges: tuple[GraphEdge, ...]
    handoffs: tuple[HandoffSchema, ...]
    execution_plan_digest: Digest

    model_config = ConfigDict(extra="forbid", frozen=True, populate_by_name=True)


# --------------------------------------------------------------------------- graph handoff carriers
#
# Three of the five IDL handoff records need only contract-side/shared types
# (``LogicalIdentity``, ``Digest``, ``OpaqueRef``, ``Finding``, the enums above) and live
# here. ``InterfaceReadinessHandoff`` needs ``InterfaceReadiness`` (defined in
# ``runtime_binding.py``) and ``PublicationConfirmationHandoff`` needs
# ``ProjectionConfirmation`` (defined in ``ingestion/records.py``); those two live in the
# module that already declares their payload type, keeping the three-module dependency
# chain (``bronze_contract`` -> ``runtime_binding`` -> ``ingestion.records``) acyclic.

class RawEvidenceHandoff(ClosedModel):
    logical_identity: LogicalIdentity
    run_id: Digest
    attempt_id: Digest
    delivery_id: Token | None
    reprocessing_id: Digest | None
    remediation_evaluation_id: Digest | None
    claim_digest: Digest
    raw_receipt_digest: Digest
    raw_ref: OpaqueRef
    candidate_ref: OpaqueRef
    candidate_digest: Digest
    frame_index_ref: OpaqueRef
    frame_index_digest: Digest


class ValidationResultHandoff(ClosedModel):
    logical_identity: LogicalIdentity
    run_id: Digest
    attempt_id: Digest
    evaluation_id: Digest
    ruleset_digest: Digest
    validation_result_digest: Digest
    accepted_content_digest: Digest
    disposition_ref: OpaqueRef
    accepted_ref: OpaqueRef
    framed_count: NonNegativeIntegerString
    accepted_count: NonNegativeIntegerString
    error_count: NonNegativeIntegerString
    warning_count: NonNegativeIntegerString
    quarantined_count: NonNegativeIntegerString
    batch_findings: tuple[Finding, ...]
    error_numerator: NonNegativeIntegerString
    error_denominator: NonNegativeIntegerString
    publication_decision: PublicationDecision


class ContractConformanceHandoff(ClosedModel):
    logical_identity: LogicalIdentity
    run_id: Digest
    attempt_id: Digest
    product_version: SemVer
    contract_digest: Digest
    source_schema_digest: Digest
    published_schema_digest: Digest
    execution_plan_digest: Digest
    result: ConformanceResult
    findings: tuple[Finding, ...]


# --------------------------------------------------------------------------- migration

class Migration(ClosedModel):
    migration_id: Digest
    kind: MigrationKind
    from_contract_digest: Digest | None
    to_contract_digest: Digest
    activated_at: UtcInstant | None
    from_visibility_epoch: NonNegativeIntegerString
    to_visibility_epoch: NonNegativeIntegerString


# --------------------------------------------------------------------------- registry
#
# Every closed record class this module declares, keyed by its exact IDL record name.
# ``ergasterion.ingestion.records`` merges this with ``runtime_binding.RECORD_MODELS`` and
# its own to build the full-IDL schema bundle and equivalence report.

RECORD_MODELS: dict[str, type[BaseModel]] = {
    "LogicalIdentity": LogicalIdentity,
    "ProductFacts": ProductFacts,
    "DecimalType": DecimalType,
    "LocalDateTimeType": LocalDateTimeType,
    "TypedBoolean": TypedBoolean,
    "TypedInt64": TypedInt64,
    "TypedDecimal": TypedDecimal,
    "TypedString": TypedString,
    "TypedDate": TypedDate,
    "TypedUtcInstant": TypedUtcInstant,
    "TypedLocalDateTime": TypedLocalDateTime,
    "TypedBinary": TypedBinary,
    "SourceField": SourceField,
    "CsvCodec": CsvCodec,
    "JsonlCodec": JsonlCodec,
    "ProjectionField": ProjectionField,
    "LandingContract": LandingContract,
    "ManagedIntegration": ManagedIntegration,
    "ExternalTrustPolicy": ExternalTrustPolicy,
    "ExternalIntegration": ExternalIntegration,
    "IntervalSchedule": IntervalSchedule,
    "CronSchedule": CronSchedule,
    "SlaContract": SlaContract,
    "TimestampContract": TimestampContract,
    "SequenceProgress": SequenceProgress,
    "OpaqueBatchProgress": OpaqueBatchProgress,
    "FingerprintScope": FingerprintScope,
    "RecordKeyContract": RecordKeyContract,
    "RetryPolicy": RetryPolicy,
    "NotNullRule": NotNullRule,
    "UniqueKeyRule": UniqueKeyRule,
    "AcceptedValuesRule": AcceptedValuesRule,
    "RangeRule": RangeRule,
    "RowCountRule": RowCountRule,
    "QualityPolicy": QualityPolicy,
    "TombstoneContract": TombstoneContract,
    "SnapshotContract": SnapshotContract,
    "DeliveryPolicy": DeliveryPolicy,
    "BronzeInterfaces": BronzeInterfaces,
    "BronzeProductContract": BronzeProductContract,
    "GraphOccurrence": GraphOccurrence,
    "GraphEdge": GraphEdge,
    "HandoffSchema": HandoffSchema,
    "ExecutionPlan": ExecutionPlan,
    "RawEvidenceHandoff": RawEvidenceHandoff,
    "ValidationResultHandoff": ValidationResultHandoff,
    "ContractConformanceHandoff": ContractConformanceHandoff,
    "Migration": Migration,
    "FindingMetadata": FindingMetadata,
    "RawLocator": RawLocator,
    "Finding": Finding,
}

ENUM_MODELS: dict[str, type[Enum]] = {
    "DeliveryMode": DeliveryMode,
    "DeliveryInputKind": DeliveryInputKind,
    "IntegrationKind": IntegrationKind,
    "CodecKind": CodecKind,
    "CapabilityCodecKind": CapabilityCodecKind,
    "ContentEncoding": ContentEncoding,
    "NewlineKind": NewlineKind,
    "BackoffKind": BackoffKind,
    "MediaType": MediaType,
    "LogicalTypeKind": LogicalTypeKind,
    "SimpleLogicalType": SimpleLogicalType,
    "ProgressKind": ProgressKind,
    "DeleteStrategy": DeleteStrategy,
    "PublicationPolicy": PublicationPolicy,
    "PublicationDecision": PublicationDecision,
    "Severity": Severity,
    "RuleKind": RuleKind,
    "PortKind": PortKind,
    "ProfileClass": ProfileClass,
    "BackupRestoreCapability": BackupRestoreCapability,
    "SecretBoundary": SecretBoundary,
    "AttemptState": AttemptState,
    "BlockPhase": BlockPhase,
    "ProcessingOutcome": ProcessingOutcome,
    "MigrationKind": MigrationKind,
    "ProjectionIntentKind": ProjectionIntentKind,
    "VisibilityKind": VisibilityKind,
    "TimelinessState": TimelinessState,
    "ErrorCategory": ErrorCategory,
    "CommandStatus": CommandStatus,
    "BackupAction": BackupAction,
    "FindingKind": FindingKind,
    "DiagnosticCode": DiagnosticCode,
    "DispositionStatus": DispositionStatus,
    "ReadinessResult": ReadinessResult,
    "GraphOccurrenceRole": GraphOccurrenceRole,
    "GraphEdgeRole": GraphEdgeRole,
    "ContractLifecycleAction": ContractLifecycleAction,
    "DeploymentLifecycleAction": DeploymentLifecycleAction,
    "RemediationDecisionKind": RemediationDecisionKind,
    "LifecycleEventType": LifecycleEventType,
    "ContractActivationState": ContractActivationState,
    "PatternId": PatternIdEnum,
    "HandoffSchemaId": HandoffSchemaIdEnum,
    "EvidenceKind": EvidenceKind,
    "OutboxEntryKind": OutboxEntryKind,
    "OutboxStatus": OutboxStatus,
    "OutboxFailureDisposition": OutboxFailureDisposition,
    "SnapshotReconciliationStatus": SnapshotReconciliationStatus,
    "QuarantineAction": QuarantineAction,
    "RemediationActionStatus": RemediationActionStatus,
    "TranslationRole": TranslationRole,
    "ConformanceResult": ConformanceResult,
    "HandoffRecordType": HandoffRecordType,
}

UNION_MODELS: dict[str, object] = {
    "TypedScalar": TypedScalar,
    "LogicalType": LogicalType,
    "Codec": Codec,
    "Integration": Integration,
    "Schedule": Schedule,
    "ProgressContract": ProgressContract,
    "QualityRule": QualityRule,
}

SCALAR_PATTERNS: dict[str, str] = {
    "IntegerString": r"^(0|[1-9][0-9]*|-[1-9][0-9]*)$",
    "NonNegativeIntegerString": r"^(0|[1-9][0-9]*)$",
    "PositiveIntegerString": r"^[1-9][0-9]*$",
    "DecimalString": r"^-?(0|[1-9][0-9]*)(\.[0-9]+)?$",
    "Digest": r"^[0-9a-f]{64}$",
    "ContentId": r"^sha256:[0-9a-f]{64}$",
    "Base64Url": r"^[A-Za-z0-9_-]+$",
    "ByteStringBase64Url": r"^[A-Za-z0-9_-]*$",
    "Token": r"^[a-z][a-z0-9._:-]{0,126}$",
    "Identifier": r"^[a-z_][a-z0-9_]*$",
    "EstateNamespace": r"^(?=.{3,253}$)[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)+$",
    "SemVer": r"^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(?:-[0-9A-Za-z.-]+)?(?:\+[0-9A-Za-z.-]+)?$",
    "UtcInstant": r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{6}Z$",
    "Date": r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$",
    "FileMode": r"^[0-7]{6}$",
    "JsonPointer": r"^(|/(?:[^~/]|~[01])*)$",
}
for _name, _pattern in SCALAR_PATTERNS.items():
    re.compile(_pattern)  # fail fast (import time) on a malformed pattern transcription
del _name, _pattern

# Numeric range and string-length bounds for the scalars the IDL constrains that way
# instead of (or in addition to) a pattern: the three integer scalars (``minimum``/
# ``maximum``) and ``OpaqueRef`` (``min_length``/``max_length``). Read alongside
# ``SCALAR_PATTERNS`` by ``ergasterion.ingestion.records.generate_equivalence_report``
# to check every IDL ``scalars`` entry against this module's actual constraint, not
# merely that a same-named Python alias exists.
SCALAR_BOUNDS: dict[str, dict[str, int]] = {
    "SafeInteger": {"minimum": -9007199254740991, "maximum": 9007199254740991},
    "PositiveInteger": {"minimum": 1, "maximum": 9007199254740991},
    "NonNegativeInteger": {"minimum": 0, "maximum": 9007199254740991},
    "OpaqueRef": {"min_length": 1, "max_length": 512},
}
