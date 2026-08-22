"""Bronze Product Contract compiler: typed declaration loading, semantic
validation, the schedule engine, canonicalisation/digests, the compatibility
classifier and the candidate/active migration state machine.

``load_typed_declarations()`` is the SSOT for typed Bronze delivery intent. It
reads ``estate.yml``, ``domains/*.yml`` and ``declarations/*.yml`` -- the same
three authoring surfaces ``ergasterion.emit.load_declarations()`` reads -- and
resolves every source-backed table into either an explicit draft placeholder or
a validated ``ergasterion.framework.bronze_contract.BronzeProductContract``.
``ergasterion.emit.load_declarations()`` stays the deterministic legacy-dict
projection every current emitter and template consumes: a ``product``/``delivery``
block this module reads rides through that legacy loader as an ordinary untyped
dict entry nobody there looks at, and an authored projection column carrying the
typed ``source`` field gains its legacy ``expression`` there by projection, so
every current declaration, generated output and legacy consumer keeps its exact
byte-for-byte behaviour.

Every wire-shape record (``BronzeProductContract``, ``DeliveryPolicy``,
``LandingContract``, ``ProductFacts``, ``Migration``, ``MigrationKind``, ...) is
imported from the frozen IDL projections
``ergasterion.framework.bronze_contract`` and
``ergasterion.ingestion.records``; this module declares no wire type of its own.
It owns exactly the compiler concerns those modules reserve for the contract
compiler: list normalisation, RFC 8785 canonicalisation, the derived-digest
family, cross-field semantic validation the wire shapes cannot express, the
schedule engine, and the compatibility/migration state machine.

Semantic validation lives here alone. ``ergasterion.structure_gate``'s
``normalise_landing`` remains the single structural entry point for the landing
discriminator, widened only to tolerate the optional Bronze landing fields, and
no template performs validation of any kind.
"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from enum import Enum
from importlib import metadata as _metadata
from typing import Any, Literal
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import rfc8785
import yaml
from pydantic import BaseModel, ConfigDict, ValidationError

from ergasterion.estate import EstateContext
from ergasterion.framework.bronze_contract import (
    BronzeInterfaces,
    BronzeProductContract,
    ContractActivationState,
    CronSchedule,
    CsvCodec,
    DecimalType,
    DeleteStrategy,
    DeliveryMode,
    DeliveryPolicy,
    EstateNamespace,
    Identifier,
    IntervalSchedule,
    LandingContract,
    LocalDateTimeType,
    LogicalIdentity,
    Migration,
    MigrationKind,
    ProductFacts,
    ProjectionField,
    PublicationPolicy,
    SemVer,
    SimpleLogicalType,
    StringScalar,
    Token,
)

_DEFAULT_CTX = EstateContext.default()

CONTRACT_SCHEMA = "ergasterion.bronze-product/v1"
SOURCE_SCHEMA_SCHEMA = "ergasterion.source-schema/v1"
PUBLISHED_SCHEMA_SCHEMA = "ergasterion.published-schema/v1"
RULE_ID_SCHEMA = "ergasterion.rule-id/v1"
RULESET_SCHEMA = "ergasterion.ruleset/v1"

TIMEZONE_DATA_VERSION = "2026.2"
"""The tzdata release every ``CronSchedule`` pins through its own
``timezone_data_version`` literal. A cron schedule resolves local wall-clock
times against the installed zone database, so the compiler refuses to compile a
schedule whose declared release differs from the one this process would use."""


class ContractValidationError(ValueError):
    """Raised by the semantic validator; carries every violation found, not just
    the first, so a fixture author sees the whole mode-matrix mismatch at once."""

    def __init__(self, where: str, violations: list[str]) -> None:
        self.where = where
        self.violations = violations
        joined = "; ".join(violations)
        super().__init__(f"{where}: {joined}")


# ============================================================================ estate.yml

class _EstateBlock(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    namespace: EstateNamespace


def load_estate_namespace(ctx: EstateContext | None = None) -> str | None:
    """Read ``<root>/estate.yml``'s mandatory ``estate.namespace``.

    Returns ``None`` when the file is absent: a seed-only legacy estate may omit
    it (docs/specifications/bronze-product-v1.md), so absence is a valid state
    and only a malformed present file raises. The namespace grammar is validated
    through the same ``EstateNamespace`` type the wire schema module pins, so
    that grammar has exactly one expression.
    """
    ctx = ctx or EstateContext.default()
    path = ctx.estate_file
    if not path.is_file():
        return None
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict) or "estate" not in data:
        raise ValueError(f"{path}: expected a top-level 'estate:' mapping")
    unknown_top = sorted(set(data) - {"estate"})
    if unknown_top:
        raise ValueError(f"{path}: unknown top-level field(s): {', '.join(unknown_top)}")
    try:
        block = _EstateBlock.model_validate(data["estate"])
    except ValidationError as exc:
        raise ValueError(f"{path}: estate: {exc}") from exc
    return block.namespace


# ============================================================================ authored (non-wire) shapes
#
# The authoring-side shapes the wire modules do not declare because they never
# travel as a runtime record: the table `product` block (the wire ``ProductFacts``
# additionally carries a `domain`, resolved from the separate `bronze:`
# domain-membership block) and that `bronze:` block itself.

class _TableProductFacts(BaseModel):
    """Table `product:` block. Exactly the eight closed fields; `domain` is
    absent by design -- it is resolved from `bronze:` domain membership."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    product_version: SemVer
    display_name: StringScalar
    description: StringScalar
    owner: Token
    support: Token
    classification: Token
    access_policy_ref: Token
    retention_policy_ref: Token


class _BronzeDomainMeta(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    name: Identifier
    display_name: StringScalar


class _BronzeProductRef(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    source: Identifier
    table: Identifier


class _BronzeDomainBlock(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    domain: _BronzeDomainMeta
    products: tuple[_BronzeProductRef, ...]


class _DraftDelivery(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    kind: Literal["draft"]
    reason: Literal["delivery_contract_required"]


def _load_bronze_domain_membership(ctx: EstateContext) -> dict[tuple[str, str], str]:
    """Resolve every `domains/*.yml` `bronze:` block into `(source, table) ->
    domain name`. A file with no `bronze:` block contributes no membership, so
    current seed-only domain files stay byte-stable. A `(source, table)` pair
    named by more than one block fails loudly, naming both files.
    """
    membership: dict[tuple[str, str], str] = {}
    claimed_by: dict[tuple[str, str], str] = {}
    for path in sorted(ctx.domains_dir.glob("*.yml")):
        data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        if not isinstance(data, dict):
            raise ValueError(f"{path}: expected a YAML mapping")
        bronze = data.get("bronze")
        if bronze is None:
            continue
        try:
            block = _BronzeDomainBlock.model_validate(bronze)
        except ValidationError as exc:
            raise ValueError(f"{path}:bronze: {exc}") from exc
        for ref in block.products:
            key = (ref.source, ref.table)
            if key in membership:
                raise ValueError(
                    f"{path}: bronze product ({ref.source}, {ref.table}) is already a "
                    f"member of domain {membership[key]!r} (declared in {claimed_by[key]})"
                )
            membership[key] = block.domain.name
            claimed_by[key] = str(path)
    return membership


# ============================================================================ production delivery overlay

_MODE_SPECIFIC_DELIVERY_KEYS = ("progress", "timestamps", "delete_strategy", "tombstone", "snapshot")


def _overlay_production_delivery(
    defaults: dict[str, Any] | None, table_block: dict[str, Any]
) -> dict[str, Any]:
    """Overlay a table's production `delivery` block onto `source.delivery`
    defaults. `source.delivery`, when present, is always
    `{kind: production, ...}` (draft is table-scoped). Scalars and nested objects
    replace whole values; changing `mode` between the defaults and the table
    override clears every inherited mode-specific object, so two modes' fields
    can never mix silently.
    """
    if defaults is not None:
        if defaults.get("kind") != "production":
            raise ValueError("source.delivery must be exactly {kind: production, ...} when present")
    merged: dict[str, Any] = {}
    if defaults:
        merged.update({key: value for key, value in defaults.items() if key != "kind"})
    default_mode = (defaults or {}).get("mode")
    override_mode = table_block.get("mode")
    if override_mode is not None and default_mode is not None and override_mode != default_mode:
        for key in _MODE_SPECIFIC_DELIVERY_KEYS:
            merged.pop(key, None)
    for key, value in table_block.items():
        if key == "kind":
            continue
        merged[key] = value
    merged["kind"] = "production"
    return merged


# ============================================================================ derived lineage

def _token_safe(value: str) -> str:
    """`Token` forbids underscore; `Identifier` (source/table names) allows it.
    Hyphenation keeps two distinct identifiers distinct."""
    return value.replace("_", "-")


def derive_interfaces(source: str, table: str) -> BronzeInterfaces:
    """The raw/source-native/published/quarantine/deletion-evidence interface
    names, derived mechanically from logical identity. Lineage carries no
    authoring surface (docs/specifications/bronze-product-v1.md)."""
    base = f"bronze-{_token_safe(source)}-{_token_safe(table)}"
    return BronzeInterfaces(
        raw=f"{base}-raw",
        source_native=f"{base}-source-native",
        published=f"{base}-published",
        quarantine=f"{base}-quarantine",
        deletion_evidence=f"{base}-deletion-evidence",
    )


# ============================================================================ canonicalisation
#
# The IDL's canonicalization block is the authority for every rule applied here:
#   json              RFC 8785
#   absent_optional   omit
#   null              allowed only when the field declares nullable true
#   set_like_lists    reject duplicates then sort by canonical scalar bytes
#   ordered_lists     preserve declared or receiver order
#   derived_fields    fields marked digest_excluded are omitted from the
#                     enclosing record's own derived-ID or content-digest basis
#
# Each list field carries its own IDL ``ordering`` hint, and the digest basis
# applies exactly one normalisation per hint:
#   set        content_encodings, allowed_key_ids (both), tombstone values and
#              accepted_values -- reject duplicates, then sort by RFC 8785 bytes
#   rule_id    quality rules -- sort by the rule identity, the same order the
#              ruleset digest applies, so authored rule order reaches no digest
#   ordered    record_key.fields and unique_key.fields -- preserved, because the
#              composite-key encoding depends on position
#   authored   csv null_tokens -- preserved
#   declared   physical_columns and projection -- carried as declared on the
#              wire, and additionally sorted here (columns by lowercase name,
#              projection by output name) so that authoring order cannot move a
#              contract identity
# RFC 8785 already canonicalises object member order, so no mapping key is ever
# sorted by hand.


def _canonical_dump(value: Any) -> Any:
    """The single canonicalisation entry point: an RFC 8785-ready JSON
    projection of a wire record.

    ``model_dump(mode="json")`` emits an explicit ``null`` for every absent
    optional field. The IDL's rule is ``absent_optional: omit``, and a field the
    IDL marks ``required: false, nullable: false``
    (``ClosedModel._omittable_not_nullable``) rejects an explicit null on the way
    back in -- so bytes carrying that null describe a record that cannot re-parse
    into the one they came from. Walking the real model tree distinguishes an
    absent omittable field from a genuine null on a nullable one, which a
    post-processing pass over a flat dump cannot do.
    """
    if isinstance(value, BaseModel):
        omittable = getattr(type(value), "_omittable_not_nullable", frozenset())
        dumped: dict[str, Any] = {}
        for name, info in type(value).model_fields.items():
            raw = getattr(value, name)
            if raw is None and name in omittable:
                continue
            dumped[info.alias or name] = _canonical_dump(raw)
        return dumped
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, (list, tuple)):
        return [_canonical_dump(item) for item in value]
    if isinstance(value, Mapping):
        return {key: _canonical_dump(item) for key, item in value.items()}
    return value


def _normalise_declared_set(items: list[Any], *, where: str) -> list[Any]:
    """A set-like list: reject duplicates, then sort by canonical bytes."""
    keyed = [(rfc8785.dumps(item), item) for item in items]
    keys = [key for key, _ in keyed]
    if len(set(keys)) != len(keys):
        raise ContractValidationError(where, ["a declared set must not repeat a value"])
    keyed.sort(key=lambda pair: pair[0])
    return [item for _, item in keyed]


def canonical_contract_document(contract: BronzeProductContract) -> dict[str, Any]:
    """The exact envelope ``contract_digest`` hashes:
    ``{"schema": "ergasterion.bronze-product/v1", "contract": <normalised>}``.
    The normalised contract re-parses into a ``BronzeProductContract`` equal up
    to the sort this function applies to columns, projections and declared
    sets; authored order in those positions does not survive normalisation."""
    dumped = _canonical_dump(contract)

    landing = dumped["landing"]
    landing["physical_columns"] = sorted(
        landing["physical_columns"], key=lambda column: column["name"].lower()
    )
    landing["content_encodings"] = _normalise_declared_set(
        landing["content_encodings"], where="landing.content_encodings"
    )
    integration = landing["integration"]
    if integration.get("kind") == "external":
        integration["receipt_trust"]["allowed_key_ids"] = _normalise_declared_set(
            integration["receipt_trust"]["allowed_key_ids"],
            where="landing.integration.receipt_trust.allowed_key_ids",
        )

    dumped["projection"] = sorted(dumped["projection"], key=lambda entry: entry["name"])

    delivery = dumped["delivery"]
    tombstone = delivery.get("tombstone")
    if tombstone is not None:
        tombstone["values"] = _normalise_declared_set(
            tombstone["values"], where="delivery.tombstone.values"
        )
    snapshot = delivery.get("snapshot")
    if snapshot is not None:
        snapshot["allowed_key_ids"] = _normalise_declared_set(
            snapshot["allowed_key_ids"], where="delivery.snapshot.allowed_key_ids"
        )
    for rule in delivery["quality"]["rules"]:
        if rule.get("kind") == "accepted_values":
            rule["values"] = _normalise_declared_set(
                rule["values"], where=f"delivery.quality.rules[{rule.get('field')}].values"
            )
    # QualityPolicy.rules carries the IDL ordering hint `rule_id`, the same order
    # compute_ruleset_digest applies, so authored rule order reaches neither digest.
    keyed_rules = list(zip((compute_rule_id(rule) for rule in contract.delivery.quality.rules),
                           delivery["quality"]["rules"]))
    keyed_rules.sort(key=lambda pair: pair[0])
    delivery["quality"]["rules"] = [rule for _, rule in keyed_rules]

    return {"schema": CONTRACT_SCHEMA, "contract": dumped}


def compute_contract_digest(contract: BronzeProductContract) -> str:
    return hashlib.sha256(rfc8785.dumps(canonical_contract_document(contract))).hexdigest()


def canonical_source_schema_document(contract: BronzeProductContract) -> dict[str, Any]:
    """The source-native shape a delivery is parsed against: the landing
    coordinates, admitted encodings, codec and physical columns. It excludes
    every product fact and delivery rule, so two contracts that differ only in
    quality rules or ownership share one source-schema digest."""
    landing = canonical_contract_document(contract)["contract"]["landing"]
    source_schema = {
        "source_name": landing["source_name"],
        "identifier": landing["identifier"],
        "content_encodings": landing["content_encodings"],
        "codec": landing["codec"],
        "physical_columns": landing["physical_columns"],
    }
    return {"schema": SOURCE_SCHEMA_SCHEMA, "source_schema": source_schema}


def compute_source_schema_digest(contract: BronzeProductContract) -> str:
    return hashlib.sha256(rfc8785.dumps(canonical_source_schema_document(contract))).hexdigest()


def canonical_published_schema_document(contract: BronzeProductContract) -> dict[str, Any]:
    """The published shape a consumer binds to: output name, logical type and
    nullability. It excludes each field's `source`, so renaming which physical
    column feeds an unchanged published column leaves this digest equal."""
    projection = canonical_contract_document(contract)["contract"]["projection"]
    published_schema = [
        {"name": entry["name"], "logical_type": entry["logical_type"], "nullable": entry["nullable"]}
        for entry in projection
    ]
    return {"schema": PUBLISHED_SCHEMA_SCHEMA, "published_schema": published_schema}


def compute_published_schema_digest(contract: BronzeProductContract) -> str:
    return hashlib.sha256(rfc8785.dumps(canonical_published_schema_document(contract))).hexdigest()


def compute_rule_id(rule: Any) -> str:
    """SHA-256 over a rule's kind, fields and parameters with `severity`
    excluded: two rules differing only in severity carry one rule identity."""
    dumped = _canonical_dump(rule)
    dumped.pop("severity", None)
    if dumped.get("kind") == "accepted_values":
        dumped["values"] = _normalise_declared_set(dumped["values"], where="rule.values")
    return hashlib.sha256(rfc8785.dumps({"schema": RULE_ID_SCHEMA, "rule": dumped})).hexdigest()


def compute_ruleset_digest(
    source_schema_digest: str, published_schema_digest: str, rules: tuple[Any, ...]
) -> str:
    """The digest a validation run cites: both schema digests plus every rule,
    ordered by rule id, so authored rule order never changes the result."""
    dumped_rules = []
    for rule in rules:
        dumped = _canonical_dump(rule)
        if dumped.get("kind") == "accepted_values":
            dumped["values"] = _normalise_declared_set(dumped["values"], where="rule.values")
        dumped_rules.append((compute_rule_id(rule), dumped))
    dumped_rules.sort(key=lambda pair: pair[0])
    document = {
        "schema": RULESET_SCHEMA,
        "source_schema_digest": source_schema_digest,
        "published_schema_digest": published_schema_digest,
        "rules": [dumped for _, dumped in dumped_rules],
    }
    return hashlib.sha256(rfc8785.dumps(document)).hexdigest()


# ============================================================================ derived digests
#
# The IDL marks certain fields ``digest_excluded``: the field holding a record's
# own derived identifier or content digest is omitted from the basis that
# produces it, while every referenced record keeps its declared digests. This
# table is that rule as data, keyed by IDL record name;
# tests/python/test_source_delivery.py checks it against the frozen IDL exactly,
# so the table cannot drift from the interface it projects.

DERIVED_DIGEST_EXCLUSIONS: dict[str, frozenset[str]] = {
    "BackupEntryPage": frozenset({"page_digest"}),
    "BackupManifest": frozenset({"backup_id", "manifest_digest"}),
    "DeletionEvidence": frozenset({"deletion_evidence_digest"}),
    "DeletionEvidenceIntent": frozenset({"deletion_evidence_intent_digest"}),
    "DeliveryClaim": frozenset({"delivery_claim_digest"}),
    "ExecutionPlan": frozenset({"execution_plan_digest"}),
    "InterfaceReadiness": frozenset({"readiness_digest", "verified_at", "revoked_at"}),
    "LifecycleEvent": frozenset({"event_id"}),
    "LineageDescriptor": frozenset({"lineage_digest"}),
    "ManagedPayloadInput": frozenset({"payload_handle"}),
    "Migration": frozenset({"migration_id"}),
    "ProjectionConfirmation": frozenset({"target_result_digest"}),
    "ProjectionIntent": frozenset({"projection_intent_digest"}),
    "RawReadHandle": frozenset({"handle_ref"}),
    "RawReceipt": frozenset({"raw_receipt_digest"}),
    "RemediationCommitCheckpoint": frozenset({"checkpoint_digest"}),
    "RemediationDecision": frozenset({"decision_id", "decided_at"}),
    "RemediationEvaluation": frozenset({"remediation_evaluation_id"}),
    "RemediationRelease": frozenset({"release_id"}),
    "ReprocessingClaim": frozenset({"reprocessing_id"}),
    "RunLineage": frozenset({"run_lineage_digest"}),
    "RuntimeManifest": frozenset({"runtime_manifest_digest"}),
    "SnapshotReconciliation": frozenset({"reconciliation_digest"}),
    "ValidationResult": frozenset({"validation_result_digest"}),
    "VerificationKeyRecord": frozenset({"public_key_base64url", "trust_record_digest"}),
}


def compute_derived_digest(record_name: str, record: BaseModel | Mapping[str, Any]) -> str:
    """The derived identifier or content digest of one IDL record: SHA-256 over
    the RFC 8785 bytes of the record with its own ``digest_excluded`` fields
    removed. Accepts a built record or the field mapping of one still being
    built, which is what a caller computing a record's own id has to hand.
    """
    if record_name not in DERIVED_DIGEST_EXCLUSIONS:
        raise ValueError(
            f"{record_name!r} declares no digest_excluded field in the IDL, so it carries "
            "no derived digest"
        )
    excluded = DERIVED_DIGEST_EXCLUSIONS[record_name]
    dumped = _canonical_dump(record)
    basis = {key: value for key, value in dumped.items() if key not in excluded}
    return hashlib.sha256(rfc8785.dumps(basis)).hexdigest()


def compute_delivery_claim_digest(claim: BaseModel | Mapping[str, Any]) -> str:
    """``DeliveryClaim.delivery_claim_digest``: the identity a delivery is
    claimed under. Two deliveries with identical manifests claim the same
    digest, which is what makes a replayed claim detectable."""
    return compute_derived_digest("DeliveryClaim", claim)


def compute_raw_receipt_digest(receipt: BaseModel | Mapping[str, Any]) -> str:
    """``RawReceipt.raw_receipt_digest``: the content digest binding a claim to
    the preserved raw bytes and their manifest."""
    return compute_derived_digest("RawReceipt", receipt)


def compute_reprocessing_id(claim: BaseModel | Mapping[str, Any]) -> str:
    """``ReprocessingClaim.reprocessing_id``: the identity of reprocessing an
    already-preserved raw receipt against a named target contract version. It
    covers the original claim, the raw receipt and every target digest, so
    reprocessing the same bytes to the same target twice is one identity."""
    return compute_derived_digest("ReprocessingClaim", claim)


def compute_migration_id(migration: BaseModel | Mapping[str, Any]) -> str:
    """``Migration.migration_id``: the identity of one activation, over its
    kind, both contract digests, the activation instant and both visibility
    epochs."""
    return compute_derived_digest("Migration", migration)


# ============================================================================ semantic validator
#
# Cross-field rules the wire shapes cannot express by typing alone. This module
# is the only place they live: no template and no other module validates a
# Bronze contract.

_ALLOWED_PROGRESS_KIND: dict[DeliveryMode, frozenset[str]] = {
    DeliveryMode.CDC: frozenset({"sequence"}),
    DeliveryMode.APPEND_ONLY: frozenset({"sequence", "opaque_batch"}),
    DeliveryMode.COMPLETE_SNAPSHOT: frozenset({"opaque_batch"}),
}
_ALLOWED_DELETE_STRATEGY: dict[DeliveryMode, frozenset[DeleteStrategy]] = {
    DeliveryMode.CDC: frozenset({DeleteStrategy.NONE, DeleteStrategy.EXPLICIT_TOMBSTONE}),
    DeliveryMode.APPEND_ONLY: frozenset({DeleteStrategy.NONE}),
    DeliveryMode.COMPLETE_SNAPSHOT: frozenset({DeleteStrategy.SNAPSHOT_DIFF}),
}


def logical_type_token(logical_type: Any) -> str:
    """The ``LogicalTypeKind`` token a declared ``LogicalType`` resolves to. The
    union carries a bare token for the six simple types and a parameterised
    object for ``decimal`` and ``local_datetime``; both reduce to one token."""
    if isinstance(logical_type, SimpleLogicalType):
        return logical_type.value
    if isinstance(logical_type, (DecimalType, LocalDateTimeType)):
        return logical_type.kind
    raise TypeError(f"unsupported logical type {logical_type!r}")


def _typed_scalar_order_key(value: Any) -> tuple[str, Any]:
    dumped = _canonical_dump(value)
    kind = dumped["logical_type"]
    if kind == "decimal":
        return (kind, Decimal(dumped["unscaled"]).scaleb(-dumped["scale"]))
    if kind == "int64":
        return (kind, int(dumped["value"]))
    return (kind, dumped.get("value"))


def _validate_quality_rules(rules: tuple[Any, ...]) -> list[str]:
    violations: list[str] = []
    seen_ids: set[str] = set()
    for rule in rules:
        rule_id = compute_rule_id(rule)
        if rule_id in seen_ids:
            violations.append(f"duplicate quality rule (rule_id {rule_id})")
        seen_ids.add(rule_id)
        if rule.kind == "unique_key" and len(set(rule.fields)) != len(rule.fields):
            violations.append(f"unique_key rule on {list(rule.fields)} must not repeat a field")
        if rule.kind == "range":
            if rule.min is None and rule.max is None:
                violations.append(f"range rule on {rule.field!r} needs at least one bound")
            elif rule.min is not None and rule.max is not None:
                min_key, max_key = _typed_scalar_order_key(rule.min), _typed_scalar_order_key(rule.max)
                if min_key[0] != max_key[0]:
                    violations.append(
                        f"range rule on {rule.field!r}: min is {min_key[0]} and max is {max_key[0]}; "
                        "both bounds carry one logical type"
                    )
                elif min_key > max_key:
                    violations.append(f"range rule on {rule.field!r}: min must be at or below max")
        if rule.kind == "row_count":
            if rule.min is None and rule.max is None:
                violations.append("row_count rule needs at least one bound")
            elif rule.min is not None and rule.max is not None and int(rule.min) > int(rule.max):
                violations.append("row_count rule: min must be at or below max")
        if rule.kind == "accepted_values" and not rule.values:
            violations.append(f"accepted_values rule on {rule.field!r} needs a nonempty values set")
    return violations


def installed_timezone_data_version() -> str:
    """The zone-database release this process resolves ``ZoneInfo`` against."""
    return _metadata.version("tzdata")


def _validate_schedule(schedule: Any) -> list[str]:
    violations: list[str] = []
    if isinstance(schedule, CronSchedule):
        try:
            parse_cron_expression(schedule.expression)
        except ValueError as exc:
            violations.append(f"schedule.expression: {exc}")
        try:
            ZoneInfo(schedule.timezone)
        except (ZoneInfoNotFoundError, ValueError):
            violations.append(f"schedule.timezone {schedule.timezone!r} names no zone in tzdata")
        installed = installed_timezone_data_version()
        if schedule.timezone_data_version != installed:
            violations.append(
                f"schedule.timezone_data_version {schedule.timezone_data_version!r} differs from "
                f"the installed tzdata release {installed!r}"
            )
    return violations


def validate_delivery_policy(delivery: DeliveryPolicy, *, where: str = "delivery") -> None:
    """Cross-field semantic validation of a delivery policy: the production mode
    matrix (progress kind, required timestamps and delete strategy per mode),
    tombstone/snapshot admission, the fingerprint-scope requirement, record-key
    shape, publication-mode gating, service-level ordering, retry bounds, the
    schedule grammar and zone database, and quality-rule closedness. Raises
    ``ContractValidationError`` naming every violation found."""
    violations: list[str] = []
    mode = delivery.mode
    progress_kind = delivery.progress.kind
    if progress_kind not in _ALLOWED_PROGRESS_KIND[mode]:
        violations.append(
            f"progress.kind {progress_kind!r} is not allowed for mode {mode.value!r} "
            f"(allowed: {sorted(_ALLOWED_PROGRESS_KIND[mode])})"
        )
    if delivery.delete_strategy not in _ALLOWED_DELETE_STRATEGY[mode]:
        allowed = sorted(strategy.value for strategy in _ALLOWED_DELETE_STRATEGY[mode])
        violations.append(
            f"delete_strategy {delivery.delete_strategy.value!r} is not allowed for mode "
            f"{mode.value!r} (allowed: {allowed})"
        )
    timestamps = delivery.timestamps
    if mode == DeliveryMode.CDC and timestamps.event_field is None:
        violations.append("mode cdc requires timestamps.event_field")
    if mode == DeliveryMode.COMPLETE_SNAPSHOT and timestamps.effective_field is None:
        violations.append("mode complete_snapshot requires timestamps.effective_field")

    if delivery.delete_strategy == DeleteStrategy.EXPLICIT_TOMBSTONE:
        if delivery.tombstone is None:
            violations.append("delete_strategy explicit_tombstone requires a tombstone block")
    elif delivery.tombstone is not None:
        violations.append("tombstone is only admitted under delete_strategy explicit_tombstone")

    if delivery.delete_strategy == DeleteStrategy.SNAPSHOT_DIFF:
        if delivery.snapshot is None:
            violations.append("delete_strategy snapshot_diff requires a snapshot block")
    elif delivery.snapshot is not None:
        violations.append("snapshot is only admitted under delete_strategy snapshot_diff")

    record_key = delivery.record_key
    if not record_key.fields:
        violations.append("record_key.fields must be nonempty")
    elif len(set(record_key.fields)) != len(record_key.fields):
        violations.append("record_key.fields must not repeat a field")

    needs_fingerprint = delivery.delete_strategy in (
        DeleteStrategy.EXPLICIT_TOMBSTONE,
        DeleteStrategy.SNAPSHOT_DIFF,
    )
    has_fingerprint = record_key.fingerprint_scope is not None and record_key.hmac_key_id is not None
    has_any_fingerprint = record_key.fingerprint_scope is not None or record_key.hmac_key_id is not None
    if needs_fingerprint and not has_fingerprint:
        violations.append(
            "record_key.fingerprint_scope and record_key.hmac_key_id are both required for "
            f"delete_strategy {delivery.delete_strategy.value!r}"
        )
    if delivery.delete_strategy == DeleteStrategy.NONE and has_any_fingerprint:
        violations.append(
            "record_key.fingerprint_scope/hmac_key_id are forbidden under delete_strategy none"
        )

    if (
        delivery.snapshot is not None
        and record_key.fingerprint_scope is not None
        and (
            delivery.snapshot.scope_id != record_key.fingerprint_scope.scope_id
            or delivery.snapshot.scope_parameters != record_key.fingerprint_scope.scope_parameters
        )
    ):
        violations.append("snapshot scope must equal record_key.fingerprint_scope exactly")

    if delivery.quality.publication_mode == PublicationPolicy.PUBLISH_VALID_ROWS:
        if mode != DeliveryMode.APPEND_ONLY or progress_kind != "opaque_batch":
            violations.append(
                "publication_mode publish_valid_rows is only admitted for append_only "
                "delivery with opaque_batch progress"
            )
    elif delivery.quality.max_error_fraction != "0":
        violations.append("publication_mode all_or_nothing requires max_error_fraction '0'")

    if delivery.schedule_lateness.warn_after_minutes >= delivery.schedule_lateness.error_after_minutes:
        violations.append("schedule_lateness.warn_after_minutes must be below error_after_minutes")
    if delivery.maximum_age is not None and (
        delivery.maximum_age.warn_after_minutes >= delivery.maximum_age.error_after_minutes
    ):
        violations.append("maximum_age.warn_after_minutes must be below error_after_minutes")
    if delivery.retry.cap_seconds < delivery.retry.base_seconds:
        violations.append("retry.cap_seconds must be at least retry.base_seconds")

    violations.extend(_validate_schedule(delivery.schedule))
    violations.extend(_validate_quality_rules(delivery.quality.rules))

    if violations:
        raise ContractValidationError(where, violations)


def _validate_codec(codec: Any) -> list[str]:
    violations: list[str] = []
    if isinstance(codec, CsvCodec):
        for name in ("delimiter", "quote", "escape"):
            value = getattr(codec, name)
            if len(value) != 1:
                violations.append(f"landing.codec.{name} must be exactly one character, got {value!r}")
        separators = [codec.delimiter, codec.quote, codec.escape]
        if len(set(separators)) != len(separators):
            violations.append(
                "landing.codec.delimiter, quote and escape must each differ from the other two"
            )
        if len(set(codec.null_tokens)) != len(codec.null_tokens):
            violations.append("landing.codec.null_tokens must not repeat a token")
    return violations


def _validate_landing_and_projection(contract: BronzeProductContract) -> list[str]:
    """Every field a contract names resolves to a declared physical column, and
    every published column passes its source column through unchanged."""
    violations: list[str] = []
    landing = contract.landing
    columns = {column.name: column for column in landing.physical_columns}
    if len(columns) != len(landing.physical_columns):
        violations.append("landing.physical_columns must not repeat a column name")
    if not landing.physical_columns:
        violations.append("landing.physical_columns must be nonempty")
    if len(set(landing.content_encodings)) != len(landing.content_encodings):
        violations.append("landing.content_encodings must not repeat an encoding")

    violations.extend(_validate_codec(landing.codec))

    if landing.integration.kind == "external":
        reserved = {
            "delivery_id_column": landing.integration.delivery_id_column,
            "visibility_epoch_column": landing.integration.visibility_epoch_column,
            "visibility_kind_column": landing.integration.visibility_kind_column,
            "visibility_id_column": landing.integration.visibility_id_column,
        }
        for name, value in reserved.items():
            if value in columns:
                violations.append(
                    f"landing.integration.{name} {value!r} collides with a declared physical column; "
                    "the external integration adds that column itself"
                )
        if len(set(reserved.values())) != len(reserved):
            violations.append("landing.integration reference columns must each carry a distinct name")
        if len(set(landing.integration.receipt_trust.allowed_key_ids)) != len(
            landing.integration.receipt_trust.allowed_key_ids
        ):
            violations.append(
                "landing.integration.receipt_trust.allowed_key_ids must not repeat a key id"
            )
        if not landing.integration.receipt_trust.allowed_key_ids:
            violations.append(
                "landing.integration.receipt_trust.allowed_key_ids must name at least one key"
            )

    def require_column(name: str | None, where: str) -> None:
        if name is not None and name not in columns:
            violations.append(f"{where} {name!r} names no declared physical column")

    delivery = contract.delivery
    require_column(delivery.timestamps.load_field, "delivery.timestamps.load_field")
    require_column(delivery.timestamps.event_field, "delivery.timestamps.event_field")
    require_column(delivery.timestamps.effective_field, "delivery.timestamps.effective_field")
    if delivery.progress.kind == "sequence":
        require_column(delivery.progress.field, "delivery.progress.field")
    for key_field in delivery.record_key.fields:
        require_column(key_field, "delivery.record_key.fields")
    if delivery.tombstone is not None:
        require_column(delivery.tombstone.field, "delivery.tombstone.field")
        column = columns.get(delivery.tombstone.field)
        if column is not None:
            expected = logical_type_token(column.logical_type)
            for value in delivery.tombstone.values:
                if value.logical_type != expected:
                    violations.append(
                        f"delivery.tombstone.values carries a {value.logical_type} scalar for "
                        f"{delivery.tombstone.field!r}, a {expected} column"
                    )
    for rule in delivery.quality.rules:
        if rule.kind in ("not_null", "accepted_values", "range"):
            require_column(rule.field, f"delivery.quality.rules[{rule.kind}].field")
        if rule.kind == "unique_key":
            for rule_field in rule.fields:
                require_column(rule_field, "delivery.quality.rules[unique_key].fields")
        column = columns.get(getattr(rule, "field", None) or "")
        if column is None:
            continue
        expected = logical_type_token(column.logical_type)
        scalars = []
        if rule.kind == "accepted_values":
            scalars = list(rule.values)
        elif rule.kind == "range":
            scalars = [bound for bound in (rule.min, rule.max) if bound is not None]
        for scalar in scalars:
            if scalar.logical_type != expected:
                violations.append(
                    f"delivery.quality.rules[{rule.kind}] on {rule.field!r} carries a "
                    f"{scalar.logical_type} scalar for a {expected} column"
                )

    if not contract.projection:
        violations.append("projection must publish at least one column")
    published_names = [entry.name for entry in contract.projection]
    if len(set(published_names)) != len(published_names):
        violations.append("projection must not publish one output name twice")
    for entry in contract.projection:
        column = columns.get(entry.source)
        if column is None:
            violations.append(f"projection[{entry.name}].source {entry.source!r} names no declared physical column")
            continue
        if logical_type_token(entry.logical_type) != logical_type_token(column.logical_type):
            violations.append(
                f"projection[{entry.name}] declares {logical_type_token(entry.logical_type)} for "
                f"{entry.source!r}, a {logical_type_token(column.logical_type)} column; Bronze "
                "publishes a source column unchanged"
            )
        if column.nullable and not entry.nullable:
            violations.append(
                f"projection[{entry.name}] declares a non-nullable column over the nullable source "
                f"column {entry.source!r}"
            )

    expected_interfaces = derive_interfaces(
        contract.logical_identity.source, contract.logical_identity.table
    )
    if contract.interfaces != expected_interfaces:
        violations.append(
            "interfaces are derived from logical identity and carry no authoring surface"
        )
    return violations


def validate_contract(contract: BronzeProductContract, *, where: str = "contract") -> None:
    """The whole-contract semantic gate: the delivery policy plus every rule
    that crosses landing, delivery, projection and derived lineage. Raises
    ``ContractValidationError`` naming every violation found."""
    violations: list[str] = []
    try:
        validate_delivery_policy(contract.delivery, where="delivery")
    except ContractValidationError as exc:
        violations.extend(f"delivery: {violation}" for violation in exc.violations)
    violations.extend(_validate_landing_and_projection(contract))
    if violations:
        raise ContractValidationError(where, violations)


# ============================================================================ compatibility classifier

class ChangeClass(str, Enum):
    NONE = "none"
    PATCH = "patch"
    MINOR = "minor"
    MAJOR = "major"
    NEW_PRODUCT = "new_product"


def _column_map(columns: list[dict[str, Any]]) -> dict[str, tuple[bytes, bool]]:
    return {
        column["name"]: (rfc8785.dumps(column["logical_type"]), column["nullable"])
        for column in columns
    }


def _projection_map(projection: list[dict[str, Any]]) -> dict[str, tuple[bytes, bool]]:
    return _column_map(
        [
            {"name": entry["name"], "logical_type": entry["logical_type"], "nullable": entry["nullable"]}
            for entry in projection
        ]
    )


# The reference-column/reference-field set an external integration binds a
# consumer to. Rotating who is trusted to sign receipts (receipt_trust) is a
# separate, Minor concern handled by _integration_minor_change below.
_EXTERNAL_INTEGRATION_REFERENCE_FIELDS = (
    "delivery_id_column",
    "visibility_epoch_column",
    "visibility_kind_column",
    "visibility_id_column",
    "raw_reference_field",
    "candidate_reference_field",
    "frame_index_reference_field",
)


def _integration_major_change(prior: dict[str, Any], candidate: dict[str, Any]) -> bool:
    """Integration kind and the external reference-column/reference-field set
    are Major (the migration matrix's "integration kind" / logical landing-port
    handle row); receipt_trust is not compared here."""
    if prior["kind"] != candidate["kind"]:
        return True
    if prior["kind"] != "external":
        return False
    return any(
        prior[field] != candidate[field] for field in _EXTERNAL_INTEGRATION_REFERENCE_FIELDS
    )


def _integration_minor_change(prior: dict[str, Any], candidate: dict[str, Any]) -> bool:
    """Attestation issuer/trust policy (external receipt_trust) is Minor per
    the migration matrix, not Major."""
    if prior["kind"] != "external" or candidate["kind"] != "external":
        return False
    return prior["receipt_trust"] != candidate["receipt_trust"]


def _snapshot_major_change(prior: dict[str, Any] | None, candidate: dict[str, Any] | None) -> bool:
    """Delete/snapshot scope (scope_id, scope_parameters) is Major; snapshot
    attestation policy is not compared here."""
    if (prior is None) != (candidate is None):
        return True
    if prior is None or candidate is None:
        return False
    return (prior["scope_id"], prior["scope_parameters"]) != (
        candidate["scope_id"],
        candidate["scope_parameters"],
    )


def _snapshot_minor_change(prior: dict[str, Any] | None, candidate: dict[str, Any] | None) -> bool:
    """Attestation issuer/trust policy (snapshot attestation_policy_ref,
    allowed_key_ids, future_clock_skew_seconds) is Minor per the migration
    matrix, not Major."""
    if prior is None or candidate is None:
        return False
    trust_fields = ("attestation_policy_ref", "allowed_key_ids", "future_clock_skew_seconds")
    return any(prior[field] != candidate[field] for field in trust_fields)


def _has_major_change(prior: dict[str, Any], candidate: dict[str, Any]) -> bool:
    """A breaking change: an existing consumer or an existing delivery stops
    working. Identity coordinates, the delivery mechanics, and any removal,
    retype or narrowing of a column already published or already read."""
    if prior["product"]["domain"] != candidate["product"]["domain"]:
        return True
    prior_landing, candidate_landing = prior["landing"], candidate["landing"]
    if (prior_landing["source_name"], prior_landing["identifier"]) != (
        candidate_landing["source_name"],
        candidate_landing["identifier"],
    ):
        return True
    if prior_landing["codec"] != candidate_landing["codec"]:
        return True
    if prior_landing["content_encodings"] != candidate_landing["content_encodings"]:
        return True
    if _integration_major_change(prior_landing["integration"], candidate_landing["integration"]):
        return True

    prior_delivery, candidate_delivery = prior["delivery"], candidate["delivery"]
    for key in ("mode", "progress", "timestamps", "record_key", "delete_strategy"):
        if prior_delivery[key] != candidate_delivery[key]:
            return True
    if _snapshot_major_change(prior_delivery.get("snapshot"), candidate_delivery.get("snapshot")):
        return True
    if prior_delivery.get("tombstone") != candidate_delivery.get("tombstone"):
        return True

    prior_columns = _column_map(prior_landing["physical_columns"])
    candidate_columns = _column_map(candidate_landing["physical_columns"])
    for name, spec in prior_columns.items():
        if candidate_columns.get(name) != spec:
            return True  # removed, retyped or renullabled
    prior_published = _projection_map(prior["projection"])
    candidate_published = _projection_map(candidate["projection"])
    for name, spec in prior_published.items():
        if candidate_published.get(name) != spec:
            return True

    for name, (_type, nullable) in candidate_columns.items():
        if name not in prior_columns and not nullable:
            return True  # a new required column breaks every existing delivery
    for name, (_type, nullable) in candidate_published.items():
        if name not in prior_published and not nullable:
            return True
    return False


def _has_minor_change(prior: dict[str, Any], candidate: dict[str, Any]) -> bool:
    """An additive or policy change: new nullable columns, a new schedule, or a
    change to the facts and rules a consumer reads but does not bind to."""
    prior_columns = _column_map(prior["landing"]["physical_columns"])
    candidate_columns = _column_map(candidate["landing"]["physical_columns"])
    if set(candidate_columns) - set(prior_columns):
        return True  # additive field(s), already proven nullable above
    prior_published = _projection_map(prior["projection"])
    candidate_published = _projection_map(candidate["projection"])
    if set(candidate_published) - set(prior_published):
        return True
    if prior["delivery"]["schedule"] != candidate["delivery"]["schedule"]:
        return True
    for key in ("classification", "access_policy_ref", "retention_policy_ref", "display_name"):
        if prior["product"][key] != candidate["product"][key]:
            return True
    if prior["delivery"]["quality"] != candidate["delivery"]["quality"]:
        return True
    if _integration_minor_change(prior["landing"]["integration"], candidate["landing"]["integration"]):
        return True
    if _snapshot_minor_change(prior["delivery"].get("snapshot"), candidate["delivery"].get("snapshot")):
        return True
    return False


def classify_contract_change(
    prior: BronzeProductContract | None, candidate: BronzeProductContract
) -> ChangeClass:
    """Classify the SemVer bump a candidate demands, per the migration matrix in
    docs/specifications/bronze-product-v1.md. It compares canonical documents, so
    YAML formatting, comments, mapping order and declared-set order never
    register as a change."""
    if prior is None:
        return ChangeClass.NEW_PRODUCT
    prior_identity, candidate_identity = prior.logical_identity, candidate.logical_identity
    if (prior_identity.estate_namespace, prior_identity.source, prior_identity.table) != (
        candidate_identity.estate_namespace,
        candidate_identity.source,
        candidate_identity.table,
    ):
        return ChangeClass.NEW_PRODUCT

    prior_document = canonical_contract_document(prior)["contract"]
    candidate_document = canonical_contract_document(candidate)["contract"]
    if prior_document == candidate_document:
        return ChangeClass.NONE
    if _has_major_change(prior_document, candidate_document):
        return ChangeClass.MAJOR
    if _has_minor_change(prior_document, candidate_document):
        return ChangeClass.MINOR
    return ChangeClass.PATCH


_SEMVER_CORE_RE = re.compile(r"^(\d+)\.(\d+)\.(\d+)")


def _semver_core(version: str) -> tuple[int, int, int]:
    match = _SEMVER_CORE_RE.match(version)
    if not match:
        raise ValueError(f"{version!r} is not a valid SemVer string")
    return int(match.group(1)), int(match.group(2)), int(match.group(3))


def validate_semver_bump(prior_version: str, candidate_version: str, required: ChangeClass) -> None:
    """Raise unless ``candidate_version`` bumps ``prior_version`` by at least the
    class ``required`` demands. A bump that understates the class fails; a bump
    larger than required passes."""
    if required == ChangeClass.NONE:
        return
    prior_core = _semver_core(prior_version)
    candidate_core = _semver_core(candidate_version)
    if candidate_core <= prior_core:
        raise ContractValidationError(
            "product_version",
            [f"must increase for a changed contract: {prior_version} -> {candidate_version}"],
        )
    if required == ChangeClass.MAJOR and candidate_core[0] <= prior_core[0]:
        raise ContractValidationError(
            "product_version",
            [f"a breaking change requires a major bump: {prior_version} -> {candidate_version}"],
        )
    if required == ChangeClass.MINOR and candidate_core[:2] <= prior_core[:2]:
        raise ContractValidationError(
            "product_version",
            [f"an additive change requires at least a minor bump: {prior_version} -> {candidate_version}"],
        )


# ============================================================================ migration state machine
#
# This state machine is persistence-free and clock-free. Runtime adapters bind the
# transitions to a compare-and-swap store; the caller supplies the revision explicitly.
#
# A contract reaches production in two steps. `register_candidate` records a
# candidate against the registry; `activate_contract` promotes exactly that
# registered candidate. Both take the revision the caller read, so a concurrent
# activation that already advanced the registry loses the race with a named
# conflict rather than overwriting the winner.
#
# Visibility epochs express whether published history stays continuous. Every
# activation -- carry or reset -- opens the next visibility epoch, so two
# distinct product versions never share one epoch and `Migration.
# from_visibility_epoch`/`to_visibility_epoch` always differ. What a carry and a
# reset disagree on is `visibility_ancestry`, the epoch closure a consumer may
# read as one unbroken series: a carry's new epoch closure extends the prior
# epoch's closure, so published history stays continuous across the change; a
# reset's closure starts at the new epoch alone, so the prior epoch stays
# version-addressable but stops growing. This is the same closure
# `ProjectionRelations.visibility_ancestry` names as a row-bounded relation and
# `VersionInterface.root_visibility_epoch` reads per contract version.
#
# A reset additionally reaches ACTIVE in two compare-and-swap steps, because it
# opens a new versioned interface that must publish its own baseline before the
# active alias may point to it. `activate_contract` stages that baseline as
# `ContractActivationState.PENDING_BASELINE` with a pending `Migration` whose
# `activated_at` is null; `confirm_baseline_activation` is the second CAS that
# moves the registry to ACTIVE once the baseline publication is confirmed,
# stamping the one immutable UTC instant the plan requires. A carry never holds
# an intermediate contract version to protect, so it reaches ACTIVE in the one
# `activate_contract` call and must supply `activated_at` there.


class MigrationConflictError(ValueError):
    """Raised on a stale ``expected_revision`` (a concurrent activation already
    advanced the registry), an unregistered candidate, a stale caller-supplied
    ``prior_contract``, or an activation kind the classified change forbids."""


class CapacityExceededError(MigrationConflictError):
    """Raised when a carry's extended ``visibility_ancestry`` closure would
    exceed ``max_visibility_ancestry_rows`` or ``max_wire_record_bytes``, before
    any state change. Wire error code ``capacity_exceeded``
    (``ergasterion.framework.bronze_contract.ERROR_CODES``)."""


@dataclass(frozen=True)
class _PendingBaseline:
    """The staged-but-unconfirmed product of a reset's first CAS step: the
    values `confirm_baseline_activation` commits once the baseline publication
    is confirmed. Held on `ContractRegistryState` rather than recomputed, so
    confirmation binds exactly what `activate_contract` planned."""

    product_version: str
    visibility_epoch: int
    visibility_ancestry: tuple[int, ...]
    migration: Migration


@dataclass(frozen=True)
class ContractRegistryState:
    """The pure product of the migration state machine: no store, no clock. The
    operational state store persists exactly these fields under an
    optimistic-concurrency ``state_revision``.

    ``visibility_ancestry`` is the epoch closure the currently active contract's
    published history spans: the ascending visibility epochs a consumer may read
    together as one continuous series. It is not a digest history -- the
    contract digest history is the migration ledger's ``to_contract_digest``
    series -- but the epoch closure `ProjectionRelations.visibility_ancestry`
    names as a bounded relation.
    """

    active_contract_digest: str | None
    active_product_version: str | None
    activation_state: ContractActivationState
    candidate_contract_digest: str | None
    visibility_epoch: int
    visibility_ancestry: tuple[int, ...]
    state_revision: int
    pending_baseline: _PendingBaseline | None = None

    @classmethod
    def initial(cls) -> "ContractRegistryState":
        """A registry with nothing activated: epoch 0, empty ancestry."""
        return cls(
            active_contract_digest=None,
            active_product_version=None,
            activation_state=ContractActivationState.PENDING_BASELINE,
            candidate_contract_digest=None,
            visibility_epoch=0,
            visibility_ancestry=(),
            state_revision=0,
            pending_baseline=None,
        )


def required_migration_kind(change: ChangeClass) -> MigrationKind:
    """Per the migration matrix: an unchanged, patch or minor contract carries;
    a major change or a new product resets."""
    if change in (ChangeClass.NONE, ChangeClass.PATCH, ChangeClass.MINOR):
        return MigrationKind.CARRY
    return MigrationKind.RESET


@dataclass(frozen=True)
class MigrationPlan:
    change: ChangeClass
    kind: MigrationKind
    from_contract_digest: str | None
    to_contract_digest: str


def plan_migration(
    prior_state: ContractRegistryState,
    prior_contract: BronzeProductContract | None,
    candidate: BronzeProductContract,
) -> MigrationPlan:
    """Classify the candidate against ``prior_contract``, check its SemVer bump,
    and resolve the activation kind the change requires. Raises
    ``ContractValidationError`` on a bump that understates the change."""
    change = classify_contract_change(prior_contract, candidate)
    if prior_contract is not None:
        validate_semver_bump(
            prior_contract.product.product_version, candidate.product.product_version, change
        )
    return MigrationPlan(
        change=change,
        kind=required_migration_kind(change),
        from_contract_digest=prior_state.active_contract_digest,
        to_contract_digest=compute_contract_digest(candidate),
    )


def register_candidate(
    state: ContractRegistryState, candidate: BronzeProductContract, *, expected_revision: int
) -> ContractRegistryState:
    """Record a candidate contract against the registry. Registering the same
    candidate again is idempotent in effect and still advances the revision; a
    different candidate while one is already in flight raises rather than
    silently replacing the pending one."""
    if state.state_revision != expected_revision:
        raise MigrationConflictError(
            f"concurrent activation: expected state_revision {expected_revision}, "
            f"found {state.state_revision}"
        )
    digest = compute_contract_digest(candidate)
    if state.candidate_contract_digest not in (None, digest):
        raise MigrationConflictError(
            f"candidate {state.candidate_contract_digest} is already in flight; "
            f"activate or withdraw it before registering {digest}"
        )
    return ContractRegistryState(
        active_contract_digest=state.active_contract_digest,
        active_product_version=state.active_product_version,
        activation_state=state.activation_state,
        candidate_contract_digest=digest,
        visibility_epoch=state.visibility_epoch,
        visibility_ancestry=state.visibility_ancestry,
        state_revision=state.state_revision + 1,
        pending_baseline=state.pending_baseline,
    )


def _check_ancestry_capacity(
    ancestry: tuple[int, ...], *, max_visibility_ancestry_rows: int, max_wire_record_bytes: int
) -> None:
    """The explicit ancestry closure must fit both ceilings before a carry may
    activate; oversized authored work fails ``capacity_exceeded`` before any
    state change rather than growing past either bound."""
    if len(ancestry) > max_visibility_ancestry_rows:
        raise CapacityExceededError(
            "capacity_exceeded: carried visibility_ancestry needs "
            f"{len(ancestry)} rows, exceeding max_visibility_ancestry_rows "
            f"{max_visibility_ancestry_rows}; choose a reset or a larger reviewed binding"
        )
    encoded_bytes = len(rfc8785.dumps(list(ancestry)))
    if encoded_bytes > max_wire_record_bytes:
        raise CapacityExceededError(
            f"capacity_exceeded: carried visibility_ancestry needs {encoded_bytes} bytes, "
            f"exceeding max_wire_record_bytes {max_wire_record_bytes}; choose a reset or a "
            "larger reviewed binding"
        )


def activate_contract(
    state: ContractRegistryState,
    prior_contract: BronzeProductContract | None,
    candidate: BronzeProductContract,
    *,
    expected_revision: int,
    activated_at: str | None,
    max_visibility_ancestry_rows: int,
    max_wire_record_bytes: int,
) -> tuple[ContractRegistryState, Migration]:
    """Promote the registered candidate under compare-and-swap.

    Every activation opens the next visibility epoch. A carry keeps its
    extended ancestry closure continuous with the epoch it carries from --
    capacity-gated by ``max_visibility_ancestry_rows``/``max_wire_record_bytes``
    -- and reaches ``ACTIVE`` in this one call, so it requires ``activated_at``.
    A reset roots a new ancestry closure at the new epoch alone and reaches only
    ``PENDING_BASELINE`` here, with a pending ``Migration`` whose ``activated_at``
    is null; ``confirm_baseline_activation`` is the second CAS that promotes it
    to ``ACTIVE`` once the baseline publication is confirmed, and it alone
    supplies the immutable instant, so ``activated_at`` here must be ``None``
    for a reset.
    """
    if state.state_revision != expected_revision:
        raise MigrationConflictError(
            f"concurrent activation: expected state_revision {expected_revision}, "
            f"found {state.state_revision}"
        )
    prior_digest = compute_contract_digest(prior_contract) if prior_contract is not None else None
    if prior_digest != state.active_contract_digest:
        raise MigrationConflictError(
            f"stale prior_contract: digests to {prior_digest!r}, but the registry's active "
            f"contract is {state.active_contract_digest!r}; read the current active contract "
            "before planning a migration against it"
        )
    plan = plan_migration(state, prior_contract, candidate)
    if state.candidate_contract_digest != plan.to_contract_digest:
        raise MigrationConflictError(
            f"contract {plan.to_contract_digest} is not the registered candidate "
            f"({state.candidate_contract_digest}); register it before activating"
        )

    to_epoch = state.visibility_epoch + 1

    if plan.kind == MigrationKind.CARRY:
        if state.active_contract_digest is None:
            raise MigrationConflictError(
                "carry requires an active contract to carry from; the first activation is a reset"
            )
        if activated_at is None:
            raise ValueError("a carry reaches ACTIVE in this call and requires activated_at")
        ancestry = state.visibility_ancestry + (to_epoch,)
        _check_ancestry_capacity(
            ancestry,
            max_visibility_ancestry_rows=max_visibility_ancestry_rows,
            max_wire_record_bytes=max_wire_record_bytes,
        )
        migration_fields = {
            "kind": plan.kind.value,
            "from_contract_digest": plan.from_contract_digest,
            "to_contract_digest": plan.to_contract_digest,
            "activated_at": activated_at,
            "from_visibility_epoch": str(state.visibility_epoch),
            "to_visibility_epoch": str(to_epoch),
        }
        migration = Migration(migration_id=compute_migration_id(migration_fields), **migration_fields)
        new_state = ContractRegistryState(
            active_contract_digest=plan.to_contract_digest,
            active_product_version=candidate.product.product_version,
            activation_state=ContractActivationState.ACTIVE,
            candidate_contract_digest=None,
            visibility_epoch=to_epoch,
            visibility_ancestry=ancestry,
            state_revision=state.state_revision + 1,
            pending_baseline=None,
        )
        return new_state, migration

    if activated_at is not None:
        raise ValueError(
            "a reset does not accept activated_at at this step; call "
            "confirm_baseline_activation() with the instant once the baseline is confirmed"
        )
    ancestry = (to_epoch,)
    migration_fields = {
        "kind": plan.kind.value,
        "from_contract_digest": plan.from_contract_digest,
        "to_contract_digest": plan.to_contract_digest,
        "activated_at": None,
        "from_visibility_epoch": str(state.visibility_epoch),
        "to_visibility_epoch": str(to_epoch),
    }
    pending_migration = Migration(
        migration_id=compute_migration_id(migration_fields), **migration_fields
    )
    new_state = ContractRegistryState(
        active_contract_digest=state.active_contract_digest,
        active_product_version=state.active_product_version,
        activation_state=ContractActivationState.PENDING_BASELINE,
        candidate_contract_digest=state.candidate_contract_digest,
        visibility_epoch=state.visibility_epoch,
        visibility_ancestry=state.visibility_ancestry,
        state_revision=state.state_revision + 1,
        pending_baseline=_PendingBaseline(
            product_version=candidate.product.product_version,
            visibility_epoch=to_epoch,
            visibility_ancestry=ancestry,
            migration=pending_migration,
        ),
    )
    return new_state, pending_migration


def confirm_baseline_activation(
    state: ContractRegistryState, *, expected_revision: int, activated_at: str
) -> tuple[ContractRegistryState, Migration]:
    """The second compare-and-swap step for a reset: promote a
    ``PENDING_BASELINE`` registry to ``ACTIVE`` once its baseline publication is
    confirmed, stamping the pending migration's immutable ``activated_at``
    instant. A migration only reaches ``ACTIVE`` with a real instant; a null
    ``activated_at`` is rejected here rather than let through."""
    if state.state_revision != expected_revision:
        raise MigrationConflictError(
            f"concurrent activation: expected state_revision {expected_revision}, "
            f"found {state.state_revision}"
        )
    if state.activation_state != ContractActivationState.PENDING_BASELINE or state.pending_baseline is None:
        raise MigrationConflictError("no pending baseline to confirm")
    if activated_at is None:
        raise ValueError("confirm_baseline_activation requires a real activated_at instant")
    pending = state.pending_baseline
    confirmed_fields = {
        "kind": pending.migration.kind.value,
        "from_contract_digest": pending.migration.from_contract_digest,
        "to_contract_digest": pending.migration.to_contract_digest,
        "activated_at": activated_at,
        "from_visibility_epoch": pending.migration.from_visibility_epoch,
        "to_visibility_epoch": pending.migration.to_visibility_epoch,
    }
    confirmed = Migration(migration_id=compute_migration_id(confirmed_fields), **confirmed_fields)
    new_state = ContractRegistryState(
        active_contract_digest=pending.migration.to_contract_digest,
        active_product_version=pending.product_version,
        activation_state=ContractActivationState.ACTIVE,
        candidate_contract_digest=None,
        visibility_epoch=pending.visibility_epoch,
        visibility_ancestry=pending.visibility_ancestry,
        state_revision=state.state_revision + 1,
        pending_baseline=None,
    )
    return new_state, confirmed


# ============================================================================ schedule engine

_CRON_FIELD_PART_RE = re.compile(r"^(\*|\d+(?:-\d+)?)(?:/(\d+))?$")
_MAX_CRON_SEARCH_DAYS = 4 * 366 + 10  # bounds a once-every-leap-day (29 February) occurrence


def _parse_utc_instant(text: str) -> datetime:
    return datetime.strptime(text, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc)


def _parse_cron_field(token: str, low: int, high: int, *, where: str) -> frozenset[int]:
    values: set[int] = set()
    for part in token.split(","):
        match = _CRON_FIELD_PART_RE.match(part)
        if not match:
            raise ValueError(f"{where}: {part!r} is not a valid cron field part")
        base, step_text = match.group(1), match.group(2)
        step = int(step_text) if step_text else 1
        if step < 1:
            raise ValueError(f"{where}: step must be positive, got {part!r}")
        if base == "*":
            start, end = low, high
        elif "-" in base:
            start_text, end_text = base.split("-", 1)
            start, end = int(start_text), int(end_text)
            if not (low <= start <= high and low <= end <= high):
                raise ValueError(f"{where}: {base!r} is outside domain [{low},{high}]")
            if start > end:
                raise ValueError(f"{where}: {base!r} is a descending range; v1 admits ascending ranges")
        else:
            if step_text is not None:
                raise ValueError(f"{where}: a step requires '*' or a range base, got {part!r}")
            start = end = int(base)
            if not (low <= start <= high):
                raise ValueError(f"{where}: {base!r} is outside domain [{low},{high}]")
        values.update(range(start, end + 1, step))
    return frozenset(values)


@dataclass(frozen=True)
class ParsedCron:
    minutes: frozenset[int]
    hours: frozenset[int]
    days_of_month: frozenset[int]
    months: frozenset[int]
    days_of_week: frozenset[int]
    day_of_month_restricted: bool
    day_of_week_restricted: bool


def parse_cron_expression(expression: str) -> ParsedCron:
    """The deliberately limited v1 grammar: five fields, no seconds, no month or
    weekday names, no aliases, no descending ranges. It admits `*`, in-domain
    integers, ascending in-domain ranges, comma lists, and positive steps
    expanded within their domain."""
    fields = expression.split()
    if len(fields) != 5:
        raise ValueError(
            f"cron expression must have exactly five fields, got {len(fields)}: {expression!r}"
        )
    minute_token, hour_token, day_token, month_token, weekday_token = fields
    minutes = _parse_cron_field(minute_token, 0, 59, where="minute")
    hours = _parse_cron_field(hour_token, 0, 23, where="hour")
    days_of_month = _parse_cron_field(day_token, 1, 31, where="day-of-month")
    months = _parse_cron_field(month_token, 1, 12, where="month")
    days_of_week = _parse_cron_field(weekday_token, 0, 6, where="day-of-week")
    if not (minutes and hours and days_of_month and months and days_of_week):
        raise ValueError(f"cron expression resolves an empty domain: {expression!r}")
    return ParsedCron(
        minutes=minutes,
        hours=hours,
        days_of_month=days_of_month,
        months=months,
        days_of_week=days_of_week,
        day_of_month_restricted=day_token != "*",
        day_of_week_restricted=weekday_token != "*",
    )


def _day_matches(day: date, parsed: ParsedCron) -> bool:
    if day.month not in parsed.months:
        return False
    cron_weekday = (day.weekday() + 1) % 7  # Python Monday=0..Sunday=6 -> cron Sunday=0..Saturday=6
    if parsed.day_of_month_restricted and parsed.day_of_week_restricted:
        return day.day in parsed.days_of_month or cron_weekday in parsed.days_of_week
    if parsed.day_of_month_restricted:
        return day.day in parsed.days_of_month
    if parsed.day_of_week_restricted:
        return cron_weekday in parsed.days_of_week
    return True


def _local_to_utc_instants(naive: datetime, zone: ZoneInfo) -> list[datetime]:
    """PEP 495 fold-aware local-to-UTC resolution: zero instants for a
    spring-forward gap, where the local time never occurs; one for an ordinary
    local time; two distinct UTC instants for a fall-back repeated local time."""
    instants: set[datetime] = set()
    for fold in (0, 1):
        candidate = naive.replace(tzinfo=zone, fold=fold)
        instant = candidate.astimezone(timezone.utc)
        if instant.astimezone(zone).replace(tzinfo=None) == naive:
            instants.add(instant)
    return sorted(instants)


def _cron_day_occurrences(day: date, parsed: ParsedCron, zone: ZoneInfo) -> list[datetime]:
    occurrences: list[datetime] = []
    for hour in parsed.hours:
        for minute in parsed.minutes:
            occurrences.extend(
                _local_to_utc_instants(datetime(day.year, day.month, day.day, hour, minute), zone)
            )
    return sorted(occurrences)


def current_boundary_at(schedule: IntervalSchedule | CronSchedule, at: datetime) -> datetime | None:
    """The greatest eligible scheduled UTC occurrence at or before ``at``, or
    ``None`` before the schedule's lower bound (``anchor_at``/``starts_at``)."""
    if at.tzinfo is None:
        raise ValueError("current_boundary_at requires a timezone-aware datetime")
    at = at.astimezone(timezone.utc)
    if isinstance(schedule, IntervalSchedule):
        anchor = _parse_utc_instant(schedule.anchor_at)
        if at < anchor:
            return None
        step = timedelta(minutes=schedule.every_minutes)
        return anchor + ((at - anchor) // step) * step
    if isinstance(schedule, CronSchedule):
        starts_at = _parse_utc_instant(schedule.starts_at)
        if at < starts_at:
            return None
        zone = ZoneInfo(schedule.timezone)
        parsed = parse_cron_expression(schedule.expression)
        day = at.astimezone(zone).date()
        for _ in range(_MAX_CRON_SEARCH_DAYS):
            if _day_matches(day, parsed):
                candidates = [
                    instant
                    for instant in _cron_day_occurrences(day, parsed, zone)
                    if starts_at <= instant <= at
                ]
                if candidates:
                    return max(candidates)
            day = day - timedelta(days=1)
        return None
    raise TypeError(f"unsupported schedule type {type(schedule)!r}")


def next_boundary_after(schedule: IntervalSchedule | CronSchedule, at: datetime) -> datetime:
    """The least scheduled UTC occurrence strictly after ``at``. An activation
    uses it to prove a migrated schedule's first new boundary lands strictly
    after the activation instant, so no occurrence is evaluated retroactively."""
    if at.tzinfo is None:
        raise ValueError("next_boundary_after requires a timezone-aware datetime")
    at = at.astimezone(timezone.utc)
    if isinstance(schedule, IntervalSchedule):
        anchor = _parse_utc_instant(schedule.anchor_at)
        if at < anchor:
            return anchor
        step = timedelta(minutes=schedule.every_minutes)
        return anchor + ((at - anchor) // step + 1) * step
    if isinstance(schedule, CronSchedule):
        starts_at = _parse_utc_instant(schedule.starts_at)
        zone = ZoneInfo(schedule.timezone)
        parsed = parse_cron_expression(schedule.expression)
        floor = max(at, starts_at - timedelta(microseconds=1))
        day = floor.astimezone(zone).date()
        for _ in range(_MAX_CRON_SEARCH_DAYS):
            if _day_matches(day, parsed):
                candidates = [
                    instant
                    for instant in _cron_day_occurrences(day, parsed, zone)
                    if instant > at and instant >= starts_at
                ]
                if candidates:
                    return min(candidates)
            day = day + timedelta(days=1)
        raise ValueError(
            f"no cron occurrence found within {_MAX_CRON_SEARCH_DAYS} days of {at.isoformat()}"
        )
    raise TypeError(f"unsupported schedule type {type(schedule)!r}")


def is_eligible_boundary(
    schedule: IntervalSchedule | CronSchedule, boundary_at: datetime, at: datetime
) -> bool:
    """A boundary is eligible when it is a genuine schedule occurrence, sits at
    or before the evaluation time ``at``, and is no earlier than the schedule's
    lower bound."""
    if boundary_at.tzinfo is None or at.tzinfo is None:
        raise ValueError("is_eligible_boundary requires timezone-aware datetimes")
    boundary_at = boundary_at.astimezone(timezone.utc)
    at = at.astimezone(timezone.utc)
    if boundary_at > at:
        return False
    return current_boundary_at(schedule, boundary_at) == boundary_at


# ============================================================================ typed declaration loader

_PROJECTION_WIRE_KEYS = ("source", "name", "logical_type", "nullable")
_PROJECTION_LEGACY_KEYS = frozenset({"expression"})


@dataclass(frozen=True)
class TypedTable:
    """One table's resolved typed Bronze intent: either an explicit draft
    placeholder (``contract`` is ``None``) or a validated
    ``BronzeProductContract`` with its three canonical digests."""

    source_name: str
    table_name: str
    kind: Literal["draft", "production"]
    domain: str | None
    draft_reason: str | None
    contract: BronzeProductContract | None
    contract_digest: str | None
    source_schema_digest: str | None
    published_schema_digest: str | None
    ruleset_digest: str | None


@dataclass(frozen=True)
class TypedDeclarations:
    """The result of ``load_typed_declarations()``: the resolved estate
    namespace plus every source-backed table's typed intent, keyed by
    ``(source, table)``."""

    estate_namespace: str | None
    tables: dict[tuple[str, str], TypedTable] = field(default_factory=dict)

    def production_contracts(self) -> list[BronzeProductContract]:
        return [table.contract for table in self.tables.values() if table.contract is not None]

    def drafts(self) -> list[TypedTable]:
        return [table for table in self.tables.values() if table.kind == "draft"]


def _projection_field(entry: Any, where: str) -> ProjectionField:
    """Project one authored projection column onto the wire ``ProjectionField``.

    An authored column may carry the legacy ``expression`` alongside the typed
    fields; ``ergasterion.emit.load_declarations()`` reads that key and this
    loader ignores it, so one authored list serves both consumers.
    """
    if not isinstance(entry, dict):
        raise ValueError(f"{where}: expected a mapping")
    unknown = sorted(set(entry) - set(_PROJECTION_WIRE_KEYS) - _PROJECTION_LEGACY_KEYS)
    if unknown:
        raise ValueError(f"{where}: unknown field(s): {', '.join(unknown)}")
    missing = [key for key in _PROJECTION_WIRE_KEYS if key not in entry]
    if missing:
        raise ValueError(f"{where}: a production projection column needs {', '.join(missing)}")
    return ProjectionField.model_validate({key: entry[key] for key in _PROJECTION_WIRE_KEYS})


def load_typed_declarations(ctx: EstateContext | None = None) -> TypedDeclarations:
    """The SSOT typed loader. It reads ``estate.yml``, every ``domains/*.yml``
    ``bronze:`` membership block and every ``declarations/*.yml``, then resolves
    each ``landing.kind: source`` table into a draft placeholder or a validated,
    digested ``BronzeProductContract``. A ``landing.kind: seed`` table carries no
    Bronze contract in v1, so seed fixture meaning stays owned by the landing
    discriminator alone.

    It raises ``ValueError`` or ``ContractValidationError`` naming the file and
    table on any missing required fact, mode-matrix violation or domain
    membership problem. It never reads through or mutates
    ``ergasterion.emit.load_declarations()``: the two loaders read the same files
    independently and neither feeds the other.
    """
    ctx = ctx or EstateContext.default()
    namespace = load_estate_namespace(ctx)
    membership = _load_bronze_domain_membership(ctx)
    tables: dict[tuple[str, str], TypedTable] = {}

    for path in sorted(ctx.declarations_dir.glob("*.yml")):
        data = yaml.safe_load(path.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            raise ValueError(f"{path}: expected a YAML mapping")
        source_block = data.get("source") or {}
        source_name = source_block.get("name")
        source_defaults = source_block.get("delivery")

        for table_name, table in (data.get("tables") or {}).items():
            landing = table.get("landing") or {"kind": "seed"}
            if not isinstance(landing, dict) or landing.get("kind") != "source":
                continue

            where = f"{path}:{table_name}"
            delivery_raw = table.get("delivery")
            if delivery_raw is None:
                raise ValueError(
                    f"{where}: a source landing needs an explicit table delivery block "
                    "(draft or production) -- see docs/specifications/bronze-product-v1.md"
                )
            key = (source_name, table_name)
            delivery_kind = delivery_raw.get("kind") if isinstance(delivery_raw, dict) else None

            if delivery_kind == "draft":
                try:
                    draft = _DraftDelivery.model_validate(delivery_raw)
                except ValidationError as exc:
                    raise ValueError(f"{where}.delivery: {exc}") from exc
                tables[key] = TypedTable(
                    source_name=source_name,
                    table_name=table_name,
                    kind="draft",
                    domain=membership.get(key),
                    draft_reason=draft.reason,
                    contract=None,
                    contract_digest=None,
                    source_schema_digest=None,
                    published_schema_digest=None,
                    ruleset_digest=None,
                )
                continue

            if delivery_kind != "production":
                raise ValueError(
                    f"{where}.delivery.kind: expected 'draft' or 'production', got {delivery_kind!r}"
                )
            if namespace is None:
                raise ValueError(f"{where}: production delivery needs estate.yml's estate.namespace")
            domain = membership.get(key)
            if domain is None:
                raise ValueError(
                    f"{where}: no domains/*.yml bronze: block names ({source_name}, {table_name}); "
                    "production generation needs exactly one explicit domain membership"
                )
            product_raw = table.get("product")
            if product_raw is None:
                raise ValueError(f"{where}: production delivery needs a table 'product' block")
            try:
                product_facts = _TableProductFacts.model_validate(product_raw)
            except ValidationError as exc:
                raise ValueError(f"{where}.product: {exc}") from exc

            projection = tuple(
                _projection_field(entry, f"{where}.projection[{index}]")
                for index, entry in enumerate(table.get("projection") or [])
            )
            merged_delivery = _overlay_production_delivery(source_defaults, delivery_raw)
            try:
                contract = BronzeProductContract(
                    schema=CONTRACT_SCHEMA,
                    logical_identity=LogicalIdentity(
                        estate_namespace=namespace, source=source_name, table=table_name
                    ),
                    product=ProductFacts(domain=domain, **product_facts.model_dump()),
                    landing=LandingContract.model_validate(landing),
                    delivery=DeliveryPolicy.model_validate(merged_delivery),
                    projection=projection,
                    interfaces=derive_interfaces(source_name, table_name),
                )
            except ValidationError as exc:
                raise ValueError(f"{where}: {exc}") from exc
            validate_contract(contract, where=where)

            source_schema_digest = compute_source_schema_digest(contract)
            published_schema_digest = compute_published_schema_digest(contract)
            tables[key] = TypedTable(
                source_name=source_name,
                table_name=table_name,
                kind="production",
                domain=domain,
                draft_reason=None,
                contract=contract,
                contract_digest=compute_contract_digest(contract),
                source_schema_digest=source_schema_digest,
                published_schema_digest=published_schema_digest,
                ruleset_digest=compute_ruleset_digest(
                    source_schema_digest, published_schema_digest, contract.delivery.quality.rules
                ),
            )

    return TypedDeclarations(estate_namespace=namespace, tables=tables)
