"""Deterministic Bronze validation and runtime-owned publication policy.

The landing adapter persists the immutable disposition index; this module owns
the quality arithmetic, authored-rule evaluation, same-ruleset revalidation
and the publication decision. It never writes a catalogue, calls a quality
vendor, or puts a source value into a diagnostic. Uniqueness that would exceed
the declared validation-memory bound spills ordered tag pages through
``ScratchStorePort`` and is merged back in bounded pages.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

import rfc8785

from ergasterion.framework.bronze_contract import (
    BronzeProductContract,
    DeliveryMode,
    DiagnosticCode,
    DispositionStatus,
    Finding,
    FindingKind,
    FindingMetadata,
    PublicationDecision,
    PublicationPolicy,
    RawLocator,
    RemediationActionStatus,
    Severity,
    TypedScalar,
)
from ergasterion.ingestion.evidence import b64url_decode, b64url_encode
from ergasterion.ingestion.ports import ScratchStorePort
from ergasterion.ingestion.records import (
    CandidateFrame,
    Digest,
    Disposition,
    ScratchChunk,
    SnapshotAcceptance,
    ValidationResult,
)
from ergasterion.ingestion.runtime import PortError, canonical_digest
from ergasterion.source_delivery import (
    compute_contract_digest,
    compute_published_schema_digest,
    compute_rule_id,
    compute_ruleset_digest,
    compute_source_schema_digest,
)

VALIDATION_RESULT_SCHEMA = "ergasterion.validation-result/v1"
DISPOSITION_ID_SCHEMA = "ergasterion.disposition-id/v1"
UNIQUE_TAG_SCHEMA = "ergasterion.unique-key-tag/v1"

# Working-set accounting is encoded canonical bytes, not CPython object headers,
# so a declared memory bound is a content bound a spill can honour.


def _encoded_size(value: Any) -> int:
    return len(rfc8785.dumps(_dump(value)))


@dataclass(frozen=True)
class ValidationOutcome:
    """One evaluation's wire result plus the receiver-order dispositions that
    produced it. ``peak_memory_bytes`` is the largest uniqueness/frame working
    set retained at once; tests use it to prove the spill stayed inside the
    declared bound."""

    validation: ValidationResult
    dispositions: tuple[Disposition, ...]
    snapshot_acceptance: SnapshotAcceptance | None
    peak_memory_bytes: int
    spilled_uniqueness: bool


@dataclass
class _MemoryMeter:
    budget: int
    current: int = 0
    peak: int = 0

    def would_exceed(self, nbytes: int) -> bool:
        return self.current + nbytes > self.budget

    def add(self, nbytes: int) -> None:
        if nbytes < 0:
            raise ValueError("memory accounting cannot go negative")
        if self.would_exceed(nbytes):
            raise PortError(
                "capacity_exceeded",
                f"validation working set {self.current + nbytes} exceeds declared bound {self.budget}",
            )
        self.current += nbytes
        if self.current > self.peak:
            self.peak = self.current

    def release(self, nbytes: int) -> None:
        self.current = max(0, self.current - nbytes)


def _dump(value: Any) -> Any:
    if hasattr(value, "model_dump"):
        return value.model_dump(mode="json", by_alias=True)
    if isinstance(value, Mapping):
        return {key: _dump(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_dump(item) for item in value]
    return value


def _field_path(name: str) -> str:
    return "/" + name.replace("~", "~0").replace("/", "~1")


def _sort_findings(findings: Sequence[Finding]) -> tuple[Finding, ...]:
    """Reject duplicate finding identities, then sort by the closed key
    ``(kind, rule_id-or-empty, field-or-empty, diagnostic_code)``."""

    seen: set[tuple[str, str, str, str]] = set()
    unique: list[Finding] = []
    for finding in findings:
        identity = (
            finding.kind.value,
            finding.rule_id or "",
            finding.field_path or "",
            finding.metadata.diagnostic_code.value,
        )
        if identity in seen:
            continue
        seen.add(identity)
        unique.append(finding)
    unique.sort(
        key=lambda finding: (
            finding.kind.value,
            finding.rule_id or "",
            finding.field_path or "",
            finding.metadata.diagnostic_code.value,
        )
    )
    return tuple(unique)


def _finding(
    *,
    kind: FindingKind,
    diagnostic: DiagnosticCode,
    severity: Severity,
    locator: RawLocator | None,
    rule_id: Digest | None = None,
    field_path: str | None = None,
    expected_count: str | None = None,
    observed_count: str | None = None,
    expected_min: str | None = None,
    expected_max: str | None = None,
    duplicate_group_size: str | None = None,
) -> Finding:
    data: dict[str, Any] = {
        "kind": kind,
        "code": "contract_invalid",
        "severity": severity,
        "metadata": FindingMetadata(
            diagnostic_code=diagnostic,
            raw_locator=locator,
            expected_logical_type=None,
            observed_logical_type=None,
            observed_count=observed_count or expected_count,
            expected_min_count=expected_min,
            expected_max_count=expected_max,
            duplicate_group_size=duplicate_group_size,
        ),
    }
    if rule_id is not None:
        data["rule_id"] = rule_id
    if field_path is not None:
        data["field_path"] = field_path
    return Finding(**data)


def _frame_values(frame: CandidateFrame) -> dict[str, TypedScalar | None]:
    if frame.typed_fields is None:
        return {}
    return {field.name: field.value for field in frame.typed_fields}


def _is_parse_or_type_error(findings: Sequence[Finding]) -> bool:
    return any(
        finding.severity is Severity.ERROR
        and finding.kind in (FindingKind.PARSE, FindingKind.TYPE)
        for finding in findings
    )


def _is_batch_abort(findings: Sequence[Finding]) -> bool:
    abort_codes = {
        DiagnosticCode.INVALID_ENCODING,
        DiagnosticCode.JSON_PARSE_ERROR,
        DiagnosticCode.COLUMN_COUNT_MISMATCH,
    }
    return any(
        finding.severity is Severity.ERROR
        and (
            finding.kind is FindingKind.PARSE
            or finding.kind is FindingKind.BATCH_RULE
            or finding.metadata.diagnostic_code in abort_codes
        )
        and finding.metadata.raw_locator is None
        for finding in findings
    )


def batch_blocks_publication(findings: Sequence[Finding]) -> bool:
    """Batch-level errors and error-severity batch rules reject the delivery."""

    return _is_batch_abort(findings) or any(
        finding.severity is Severity.ERROR and finding.kind is FindingKind.BATCH_RULE
        for finding in findings
    )


def _scalar_order(value: TypedScalar) -> tuple[str, Any]:
    dumped = _dump(value)
    kind = dumped["logical_type"]
    if kind == "decimal":
        return kind, Decimal(dumped["unscaled"]).scaleb(-int(dumped["scale"]))
    if kind == "int64":
        return kind, int(dumped["value"])
    return kind, dumped.get("value")


def _scalars_equal(left: TypedScalar, right: TypedScalar) -> bool:
    return rfc8785.dumps(_dump(left)) == rfc8785.dumps(_dump(right))


def _unique_key_tag(fields: Sequence[str], values: Mapping[str, TypedScalar | None]) -> Digest | None:
    components = []
    for name in fields:
        value = values.get(name)
        if value is None:
            return None
        components.append(_dump(value))
    return canonical_digest({"schema": UNIQUE_TAG_SCHEMA, "fields": list(fields), "components": components})


def partial_publication_permitted(contract: BronzeProductContract) -> bool:
    """``publish_valid_rows`` is admitted only for append-only opaque batches."""

    delivery = contract.delivery
    return (
        delivery.quality.publication_mode is PublicationPolicy.PUBLISH_VALID_ROWS
        and delivery.mode is DeliveryMode.APPEND_ONLY
        and delivery.progress.kind == "opaque_batch"
    )


def decide_publication(
    *,
    publication_mode: PublicationPolicy,
    max_error_fraction: str,
    delivery_mode: DeliveryMode,
    progress_kind: str,
    framed_count: int,
    error_numerator: int,
    passing_count: int,
    batch_error: bool,
) -> PublicationDecision:
    """Closed quality arithmetic. Impossible policy/mode/decision combinations
    raise ``invalid_config`` rather than emitting a partial decision a mode
    cannot honour."""

    allows_partial = (
        publication_mode is PublicationPolicy.PUBLISH_VALID_ROWS
        and delivery_mode is DeliveryMode.APPEND_ONLY
        and progress_kind == "opaque_batch"
    )
    if publication_mode is PublicationPolicy.PUBLISH_VALID_ROWS and not allows_partial:
        raise PortError(
            "invalid_config",
            "publication_mode publish_valid_rows is prohibited except for append_only opaque_batch",
        )
    if publication_mode is PublicationPolicy.ALL_OR_NOTHING and Decimal(max_error_fraction) != 0:
        raise PortError("invalid_config", "all_or_nothing requires max_error_fraction 0")
    if allows_partial:
        bound = Decimal(max_error_fraction)
        if bound < 0 or bound >= 1:
            raise PortError("invalid_config", "publish_valid_rows max_error_fraction must be in [0,1)")

    if batch_error:
        return PublicationDecision.REJECT_DELIVERY
    if framed_count == 0:
        return PublicationDecision.PUBLISH_ALL
    if error_numerator == 0:
        return PublicationDecision.PUBLISH_ALL
    if not allows_partial:
        return PublicationDecision.REJECT_DELIVERY
    if passing_count < 1:
        return PublicationDecision.REJECT_DELIVERY
    fraction = Decimal(error_numerator) / Decimal(framed_count)
    if fraction > Decimal(max_error_fraction):
        return PublicationDecision.REJECT_DELIVERY
    return PublicationDecision.PUBLISH_VALID_ROWS


def outcome_digest(status: DispositionStatus, findings: Sequence[Finding]) -> Digest:
    return canonical_digest({
        "status": status.value,
        "findings": [_dump(finding) for finding in findings],
    })


def disposition_id_for(claim_digest: Digest, locator: RawLocator, ruleset_digest: Digest) -> Digest:
    return canonical_digest({
        "schema": DISPOSITION_ID_SCHEMA,
        "claim_digest": claim_digest,
        "raw_locator": _dump(locator),
        "ruleset_digest": ruleset_digest,
    })


def validation_result_digest(
    *,
    evaluation_claim: Digest,
    ruleset_digest: Digest,
    dispositions: Sequence[Disposition],
    batch_findings: Sequence[Finding],
    framed_count: str,
    accepted_count: str,
    error_count: str,
    warning_count: str,
    quarantined_count: str,
    error_numerator: str,
    error_denominator: str,
    publication_decision: PublicationDecision,
) -> Digest:
    """The single quality-result identity bound by accepted partition, intent,
    ledger and lineage. Dispositions enter only as receiver-order
    ``(disposition_id, outcome_digest)`` pairs, never as source values."""

    return canonical_digest({
        "schema": VALIDATION_RESULT_SCHEMA,
        "evaluation_claim": evaluation_claim,
        "ruleset_digest": ruleset_digest,
        "dispositions": [
            {"disposition_id": item.disposition_id, "outcome_digest": item.outcome_digest}
            for item in dispositions
        ],
        "batch_findings": [_dump(finding) for finding in batch_findings],
        "counts": {
            "framed": framed_count,
            "accepted": accepted_count,
            "error": error_count,
            "warning": warning_count,
            "quarantined": quarantined_count,
            "numerator": error_numerator,
            "denominator": error_denominator,
        },
        "publication_decision": publication_decision.value,
    })


def _evaluate_field_rules(
    contract: BronzeProductContract,
    values: Mapping[str, TypedScalar | None],
    locator: RawLocator,
    columns: Mapping[str, Any],
) -> list[Finding]:
    findings: list[Finding] = []
    for rule in contract.delivery.quality.rules:
        if rule.kind == "row_count" or rule.kind == "unique_key":
            continue
        rule_id = compute_rule_id(rule)
        field_name = rule.field
        value = values.get(field_name)
        column = columns.get(field_name)
        nullable = bool(getattr(column, "nullable", False)) if column is not None else False
        if rule.kind == "not_null":
            if value is None:
                findings.append(_finding(
                    kind=FindingKind.RULE, diagnostic=DiagnosticCode.NULL_NOT_ALLOWED,
                    severity=rule.severity, locator=locator, rule_id=rule_id,
                    field_path=_field_path(field_name),
                ))
            continue
        if rule.kind == "accepted_values":
            if value is None:
                if not (nullable and rule.allow_null):
                    findings.append(_finding(
                        kind=FindingKind.RULE, diagnostic=DiagnosticCode.NULL_NOT_ALLOWED,
                        severity=rule.severity, locator=locator, rule_id=rule_id,
                        field_path=_field_path(field_name),
                    ))
                continue
            if not any(_scalars_equal(value, allowed) for allowed in rule.values):
                findings.append(_finding(
                    kind=FindingKind.RULE, diagnostic=DiagnosticCode.ACCEPTED_VALUE_VIOLATION,
                    severity=rule.severity, locator=locator, rule_id=rule_id,
                    field_path=_field_path(field_name),
                ))
            continue
        if rule.kind == "range":
            if value is None:
                if not (nullable and rule.allow_null):
                    findings.append(_finding(
                        kind=FindingKind.RULE, diagnostic=DiagnosticCode.NULL_NOT_ALLOWED,
                        severity=rule.severity, locator=locator, rule_id=rule_id,
                        field_path=_field_path(field_name),
                    ))
                continue
            observed = _scalar_order(value)
            if rule.min is not None and observed < _scalar_order(rule.min):
                findings.append(_finding(
                    kind=FindingKind.RULE, diagnostic=DiagnosticCode.RANGE_VIOLATION,
                    severity=rule.severity, locator=locator, rule_id=rule_id,
                    field_path=_field_path(field_name),
                ))
                continue
            if rule.max is not None and observed > _scalar_order(rule.max):
                findings.append(_finding(
                    kind=FindingKind.RULE, diagnostic=DiagnosticCode.RANGE_VIOLATION,
                    severity=rule.severity, locator=locator, rule_id=rule_id,
                    field_path=_field_path(field_name),
                ))
    return findings


def _row_count_findings(contract: BronzeProductContract, framed_count: int) -> list[Finding]:
    findings: list[Finding] = []
    observed = str(framed_count)
    for rule in contract.delivery.quality.rules:
        if rule.kind != "row_count":
            continue
        lo = None if rule.min is None else int(rule.min)
        hi = None if rule.max is None else int(rule.max)
        if lo is not None and framed_count < lo:
            findings.append(_finding(
                kind=FindingKind.BATCH_RULE, diagnostic=DiagnosticCode.ROW_COUNT_VIOLATION,
                severity=rule.severity, locator=None, rule_id=compute_rule_id(rule),
                observed_count=observed, expected_min=rule.min, expected_max=rule.max,
            ))
        elif hi is not None and framed_count > hi:
            findings.append(_finding(
                kind=FindingKind.BATCH_RULE, diagnostic=DiagnosticCode.ROW_COUNT_VIOLATION,
                severity=rule.severity, locator=None, rule_id=compute_rule_id(rule),
                observed_count=observed, expected_min=rule.min, expected_max=rule.max,
            ))
    return findings


@dataclass
class _UniqueRuleState:
    fields: tuple[str, ...]
    rule_id: Digest
    severity: Severity
    groups: dict[str, list[tuple[str, RawLocator, int]]] = field(default_factory=dict)
    spilled: bool = False


@dataclass
class _PendingUnit:
    sequence: str
    locator: RawLocator
    structural: tuple[Finding, ...]
    rule_findings: list[Finding]
    parse_failed: bool
    nbytes: int


class _UnitBuffer:
    """Lightweight per-unit state without typed source values. Spills through
    scratch when retaining the next unit would cross the declared bound."""

    def __init__(
        self,
        meter: _MemoryMeter,
        scratch: ScratchStorePort | None,
        attempt_id: Digest,
        scratch_capacity_bytes: int,
    ) -> None:
        self.meter = meter
        self.scratch = scratch
        self.attempt_id = attempt_id
        self.scratch_capacity_bytes = scratch_capacity_bytes
        self.pending: list[_PendingUnit] = []
        self.scope = None
        self._next_sequence = 0
        self.spilled = False

    def _record(self, unit: _PendingUnit) -> dict[str, Any]:
        return {
            "sequence": unit.sequence,
            "locator": _dump(unit.locator),
            "structural": [_dump(item) for item in unit.structural],
            "rule_findings": [_dump(item) for item in unit.rule_findings],
            "parse_failed": unit.parse_failed,
        }

    def add(self, unit: _PendingUnit) -> None:
        if self.meter.would_exceed(unit.nbytes):
            self.spill_retained()
        if self.meter.would_exceed(unit.nbytes):
            raise PortError(
                "capacity_exceeded",
                "one framed unit exceeds the declared validation memory bound even after spill",
            )
        self.meter.add(unit.nbytes)
        self.pending.append(unit)

    def spill_retained(self) -> None:
        if not self.pending:
            return
        if self.scratch is None:
            raise PortError("capacity_exceeded", "unit-state spill requires a scratch store")
        if self.scope is None:
            self.scope = self.scratch.create_scope(
                self.attempt_id, str(max(self.scratch_capacity_bytes, 1)),
            )
        released = 0
        for unit in self.pending:
            encoded = rfc8785.dumps(self._record(unit))
            self.scratch.write_sequential(
                self.attempt_id,
                ScratchChunk(
                    scope_id=self.scope.scope_id,
                    sequence=str(self._next_sequence),
                    bytes_base64url=b64url_encode(encoded),
                ),
            )
            self._next_sequence += 1
            released += unit.nbytes
        self.pending.clear()
        self.meter.release(released)
        self.spilled = True

    def _restore(self, row: Mapping[str, Any]) -> _PendingUnit:
        return _PendingUnit(
            sequence=row["sequence"],
            locator=RawLocator.model_validate(row["locator"]),
            structural=tuple(Finding.model_validate(item) for item in row["structural"]),
            rule_findings=[Finding.model_validate(item) for item in row["rule_findings"]],
            parse_failed=bool(row["parse_failed"]),
            nbytes=0,
        )

    def materialize(
        self, unique_findings: Mapping[str, Sequence[Finding]], build,
    ) -> list[Disposition]:
        dispositions: list[Disposition] = []
        if not self.spilled:
            for unit in self.pending:
                extras = unique_findings.get(unit.sequence, ())
                dispositions.append(build(unit, extras))
            return dispositions
        self.spill_retained()
        if self.scope is None or self.scratch is None:
            return dispositions
        self.scratch.close_scope(self.attempt_id, self.scope.scope_id)
        after = "-1"
        while True:
            page = self.scratch.read_sequential(
                self.attempt_id, self.scope.scope_id, after, str(max(self.meter.budget, 1)),
            )
            if not page.chunks:
                break
            for chunk in page.chunks:
                row = json.loads(b64url_decode(chunk.bytes_base64url).decode("utf-8"))
                unit = self._restore(row)
                extras = unique_findings.get(unit.sequence, ())
                dispositions.append(build(unit, extras))
                after = chunk.sequence
            if page.next_sequence is None:
                break
        return dispositions

    def close(self) -> None:
        if self.scope is not None and self.scratch is not None:
            try:
                self.scratch.delete_scope(self.attempt_id, self.scope.scope_id)
            except PortError:
                pass


def _unique_record(rule_index: int, tag: Digest, sequence: str, locator: RawLocator) -> dict[str, Any]:
    return {
        "rule": rule_index,
        "tag": tag,
        "sequence": sequence,
        "locator": _dump(locator),
    }


def _tag_sort_key(row: Mapping[str, Any]) -> tuple[int, str, int]:
    return (int(row["rule"]), str(row["tag"]), int(row["sequence"]))


class _UniqueSpill:
    """Spillable in-delivery uniqueness: encoded tag records live in memory until
    the declared bound would be crossed, then as paged scratch chunks. Merge
    never reloads every tag into one map; it sorts runs and scans one group."""

    def __init__(
        self,
        rules: Sequence[_UniqueRuleState],
        meter: _MemoryMeter,
        scratch: ScratchStorePort | None,
        attempt_id: Digest,
        scratch_capacity_bytes: int,
    ) -> None:
        self.rules = list(rules)
        self.meter = meter
        self.scratch = scratch
        self.attempt_id = attempt_id
        self.scratch_capacity_bytes = scratch_capacity_bytes
        self.scope = None
        self._next_sequence = 0
        self.spilled = False
        self._work_scopes: list[str] = []

    def observe(self, rule: _UniqueRuleState, tag: Digest, sequence: str, locator: RawLocator) -> None:
        record = _unique_record(self.rules.index(rule), tag, sequence, locator)
        addition = _encoded_size(record)
        if self.meter.would_exceed(addition):
            self.spill_retained()
        if self.meter.would_exceed(addition):
            raise PortError(
                "capacity_exceeded",
                "one uniqueness tag exceeds the declared validation memory bound even after spill",
            )
        self.meter.add(addition)
        rule.groups.setdefault(tag, []).append((sequence, locator, addition))

    def _ensure_scope(self) -> None:
        if self.scratch is None:
            raise PortError("capacity_exceeded", "uniqueness spill requires a scratch store")
        if self.scope is None:
            capacity = str(max(self.scratch_capacity_bytes, 1))
            self.scope = self.scratch.create_scope(self.attempt_id, capacity)

    def _capacity(self) -> str:
        return str(max(self.scratch_capacity_bytes, 1))

    def _page_budget(self) -> str:
        return str(max(self.meter.budget, 1))

    def _write_row(self, scope_id: str, sequence: int, row: Mapping[str, Any]) -> None:
        assert self.scratch is not None
        encoded = rfc8785.dumps(_dump(row))
        self.scratch.write_sequential(
            self.attempt_id,
            ScratchChunk(
                scope_id=scope_id,
                sequence=str(sequence),
                bytes_base64url=b64url_encode(encoded),
            ),
        )

    def spill_retained(self) -> None:
        released = 0
        rows: list[dict[str, Any]] = []
        for index, rule in enumerate(self.rules):
            if not rule.groups:
                continue
            for tag, members in rule.groups.items():
                for sequence, locator, nbytes in members:
                    rows.append(_unique_record(index, tag, sequence, locator))
                    released += nbytes
            rule.groups.clear()
            rule.spilled = True
        if not rows:
            return
        self._ensure_scope()
        assert self.scope is not None
        for row in rows:
            self._write_row(self.scope.scope_id, self._next_sequence, row)
            self._next_sequence += 1
        self.meter.release(released)
        self.spilled = True

    def _read_one(
        self, scope_id: str, after: str, end: int,
    ) -> tuple[dict[str, Any] | None, str]:
        assert self.scratch is not None
        page = self.scratch.read_sequential(
            self.attempt_id, scope_id, after, self._page_budget(),
        )
        chunks = [chunk for chunk in page.chunks if int(chunk.sequence) <= end]
        if not chunks:
            return None, after
        chunk = chunks[0]
        row = json.loads(b64url_decode(chunk.bytes_base64url).decode("utf-8"))
        return row, chunk.sequence

    def _flush_sorted_run(
        self, scope_id: str, rows: list[dict[str, Any]], out_seq: int,
        runs: list[tuple[str, int, int]],
    ) -> int:
        rows.sort(key=_tag_sort_key)
        start = out_seq
        for row in rows:
            self._write_row(scope_id, out_seq, row)
            out_seq += 1
        runs.append((scope_id, start, out_seq - 1))
        return out_seq

    def _build_sorted_runs(self) -> list[tuple[str, int, int]]:
        assert self.scope is not None and self.scratch is not None
        work = self.scratch.create_scope(self.attempt_id, self._capacity())
        self._work_scopes.append(work.scope_id)
        runs: list[tuple[str, int, int]] = []
        buf: list[dict[str, Any]] = []
        buf_bytes = 0
        after = "-1"
        out_seq = 0
        while True:
            page = self.scratch.read_sequential(
                self.attempt_id, self.scope.scope_id, after, self._page_budget(),
            )
            if not page.chunks:
                break
            for chunk in page.chunks:
                raw = b64url_decode(chunk.bytes_base64url)
                row = json.loads(raw.decode("utf-8"))
                size = len(raw)
                if buf and buf_bytes + size > self.meter.budget:
                    out_seq = self._flush_sorted_run(work.scope_id, buf, out_seq, runs)
                    buf, buf_bytes = [], 0
                buf.append(row)
                buf_bytes += size
                after = chunk.sequence
            if page.next_sequence is None:
                break
        if buf:
            self._flush_sorted_run(work.scope_id, buf, out_seq, runs)
        self.scratch.close_scope(self.attempt_id, work.scope_id)
        return runs

    def _merge_runs(
        self, left: tuple[str, int, int], right: tuple[str, int, int],
    ) -> tuple[str, int, int]:
        assert self.scratch is not None
        out = self.scratch.create_scope(self.attempt_id, self._capacity())
        self._work_scopes.append(out.scope_id)
        out_seq = 0
        left_row, left_after = self._read_one(left[0], str(left[1] - 1), left[2])
        right_row, right_after = self._read_one(right[0], str(right[1] - 1), right[2])
        while left_row is not None or right_row is not None:
            take_left = right_row is None or (
                left_row is not None and _tag_sort_key(left_row) <= _tag_sort_key(right_row)
            )
            if take_left:
                assert left_row is not None
                self._write_row(out.scope_id, out_seq, left_row)
                left_row, left_after = self._read_one(left[0], left_after, left[2])
            else:
                assert right_row is not None
                self._write_row(out.scope_id, out_seq, right_row)
                right_row, right_after = self._read_one(right[0], right_after, right[2])
            out_seq += 1
        self.scratch.close_scope(self.attempt_id, out.scope_id)
        return (out.scope_id, 0, out_seq - 1)

    def _scan_run_for_duplicates(self, scope_id: str, start: int, end: int) -> dict[str, list[Finding]]:
        by_sequence: dict[str, list[Finding]] = {}
        current_key: tuple[int, str] | None = None
        members: list[tuple[str, RawLocator]] = []
        after = str(start - 1)

        def flush() -> None:
            nonlocal members, current_key
            if current_key is not None:
                grouped = {current_key[0]: {current_key[1]: members}}
                for sequence, findings in _unique_key_findings(grouped, self.rules).items():
                    by_sequence.setdefault(sequence, []).extend(findings)
            members = []
            current_key = None

        while True:
            row, after = self._read_one(scope_id, after, end)
            if row is None:
                break
            key = (int(row["rule"]), str(row["tag"]))
            if current_key is not None and key != current_key:
                flush()
            members.append((row["sequence"], RawLocator.model_validate(row["locator"])))
            current_key = key
        flush()
        return by_sequence

    def duplicate_findings(self) -> dict[str, list[Finding]]:
        if not self.spilled:
            grouped = {
                index: {tag: [(sequence, locator) for sequence, locator, _nbytes in members]
                        for tag, members in rule.groups.items()}
                for index, rule in enumerate(self.rules)
            }
            return _unique_key_findings(grouped, self.rules)
        self.spill_retained()
        if self.scope is None or self.scratch is None:
            return {}
        self.scratch.close_scope(self.attempt_id, self.scope.scope_id)
        runs = self._build_sorted_runs()
        if not runs:
            return {}
        while len(runs) > 1:
            merged: list[tuple[str, int, int]] = []
            for index in range(0, len(runs), 2):
                if index + 1 >= len(runs):
                    merged.append(runs[index])
                else:
                    merged.append(self._merge_runs(runs[index], runs[index + 1]))
            runs = merged
        scope_id, start, end = runs[0]
        return self._scan_run_for_duplicates(scope_id, start, end)

    def close(self) -> None:
        if self.scratch is None:
            return
        for scope_id in [self.scope.scope_id if self.scope is not None else None, *self._work_scopes]:
            if not scope_id:
                continue
            try:
                self.scratch.delete_scope(self.attempt_id, scope_id)
            except PortError:
                pass


def _unique_key_findings(
    duplicates: Mapping[int, Mapping[str, Sequence[tuple[str, RawLocator]]]],
    rules: Sequence[_UniqueRuleState],
) -> dict[str, list[Finding]]:
    by_sequence: dict[str, list[Finding]] = {}
    for index, rule in enumerate(rules):
        for members in duplicates.get(index, {}).values():
            if len(members) < 2:
                continue
            size = str(len(members))
            for sequence, locator in members:
                by_sequence.setdefault(sequence, []).append(_finding(
                    kind=FindingKind.RULE, diagnostic=DiagnosticCode.DUPLICATE_KEY,
                    severity=rule.severity, locator=locator, rule_id=rule.rule_id,
                    duplicate_group_size=size,
                ))
    return by_sequence


def validate_frames(
    contract: BronzeProductContract,
    frames: Iterable[CandidateFrame],
    *,
    claim_digest: Digest,
    delivery_id: str,
    evaluation_id: Digest,
    memory_budget_bytes: int,
    scratch_store: ScratchStorePort | None = None,
    attempt_id: Digest | None = None,
    batch_findings: tuple[Finding, ...] = (),
    product_version: str | None = None,
    scratch_capacity_bytes: int | None = None,
) -> ValidationOutcome:
    """Stream codec-framed units, apply authored rules, and emit one immutable
    ``ValidationResult``. Source values never enter findings; uniqueness that
    would exceed ``memory_budget_bytes`` spills through ``scratch_store``."""

    if memory_budget_bytes < 1:
        raise PortError("capacity_exceeded", "validation memory bound must be positive")
    source_schema_digest = compute_source_schema_digest(contract)
    published_schema_digest = compute_published_schema_digest(contract)
    contract_digest = compute_contract_digest(contract)
    ruleset_digest = compute_ruleset_digest(
        source_schema_digest, published_schema_digest, contract.delivery.quality.rules,
    )
    columns = {column.name: column for column in contract.landing.physical_columns}
    unique_rules = [
        _UniqueRuleState(fields=tuple(rule.fields), rule_id=compute_rule_id(rule), severity=rule.severity)
        for rule in contract.delivery.quality.rules
        if rule.kind == "unique_key"
    ]
    meter = _MemoryMeter(budget=memory_budget_bytes)
    capacity = scratch_capacity_bytes or max(memory_budget_bytes * 64, 1_048_576)
    owner = attempt_id or evaluation_id
    spill = _UniqueSpill(unique_rules, meter, scratch_store, owner, capacity)
    units = _UnitBuffer(meter, scratch_store, owner, capacity)

    framed = 0
    try:
        for frame in frames:
            framed += 1
            frame_record = {
                "locator": _dump(frame.raw_locator),
                "fields": _dump(frame.typed_fields) if frame.typed_fields is not None else None,
                "structural": [_dump(item) for item in frame.structural_findings],
            }
            frame_bytes = _encoded_size(frame_record)
            if meter.would_exceed(frame_bytes):
                spill.spill_retained()
                units.spill_retained()
            if meter.would_exceed(frame_bytes):
                raise PortError(
                    "capacity_exceeded",
                    "one framed unit exceeds the declared validation memory bound even after spill",
                )
            meter.add(frame_bytes)
            try:
                values = _frame_values(frame)
                structural = tuple(frame.structural_findings)
                parse_failed = _is_parse_or_type_error(structural) or frame.typed_fields is None
                rule_findings: list[Finding] = []
                if not parse_failed:
                    rule_findings.extend(_evaluate_field_rules(contract, values, frame.raw_locator, columns))
                    for rule in unique_rules:
                        tag = _unique_key_tag(rule.fields, values)
                        if tag is None:
                            rule_findings.append(_finding(
                                kind=FindingKind.RULE, diagnostic=DiagnosticCode.NULL_NOT_ALLOWED,
                                severity=rule.severity, locator=frame.raw_locator, rule_id=rule.rule_id,
                            ))
                        else:
                            spill.observe(rule, tag, frame.frame_sequence, frame.raw_locator)
                unit = _PendingUnit(
                    sequence=frame.frame_sequence,
                    locator=frame.raw_locator,
                    structural=structural,
                    rule_findings=rule_findings,
                    parse_failed=parse_failed,
                    nbytes=0,
                )
                unit.nbytes = _encoded_size(units._record(unit))
            finally:
                meter.release(frame_bytes)
            units.add(unit)
        unique_findings = spill.duplicate_findings()
    except Exception:
        spill.close()
        units.close()
        raise

    def _build(unit: _PendingUnit, extras: Sequence[Finding]) -> Disposition:
        findings = _sort_findings(tuple(unit.structural) + tuple(unit.rule_findings) + tuple(extras))
        has_error = unit.parse_failed or any(finding.severity is Severity.ERROR for finding in findings)
        status = DispositionStatus.REJECTED if has_error else DispositionStatus.ACCEPTED
        return Disposition(
            disposition_id=disposition_id_for(claim_digest, unit.locator, ruleset_digest),
            raw_ref=unit.locator.frame_sequence,
            raw_locator=unit.locator,
            delivery_id=delivery_id,
            claim_digest=claim_digest,
            ruleset_digest=ruleset_digest,
            product_version=product_version or contract.product.product_version,
            contract_digest=contract_digest,
            source_schema_digest=source_schema_digest,
            published_schema_digest=published_schema_digest,
            status=status,
            findings=findings,
            outcome_digest=outcome_digest(status, findings),
        )

    try:
        dispositions = units.materialize(unique_findings, _build)
    finally:
        spill.close()
        units.close()

    batch = _sort_findings(tuple(batch_findings) + tuple(_row_count_findings(contract, framed)))
    batch_error = batch_blocks_publication(batch)

    error_units = sum(1 for item in dispositions if item.status is DispositionStatus.REJECTED)
    accepted = sum(1 for item in dispositions if item.status is DispositionStatus.ACCEPTED)
    warning_units = sum(
        1 for item in dispositions
        if any(finding.severity is Severity.WARN for finding in item.findings)
    )
    quarantined = error_units

    denominator = framed
    numerator = error_units
    passing = accepted
    decision = decide_publication(
        publication_mode=contract.delivery.quality.publication_mode,
        max_error_fraction=contract.delivery.quality.max_error_fraction,
        delivery_mode=contract.delivery.mode,
        progress_kind=contract.delivery.progress.kind,
        framed_count=denominator,
        error_numerator=numerator,
        passing_count=passing,
        batch_error=batch_error,
    )
    snapshot_acceptance = None
    if contract.delivery.mode is DeliveryMode.COMPLETE_SNAPSHOT:
        accepted_complete = quarantined == 0 and decision is PublicationDecision.PUBLISH_ALL
        if not accepted_complete:
            decision = PublicationDecision.REJECT_DELIVERY
        snapshot_acceptance = SnapshotAcceptance(
            source_snapshot_complete=True,
            accepted_snapshot_complete=accepted_complete,
            framed_count=str(denominator),
            accepted_count=str(accepted),
            quarantined_count=str(quarantined),
            validation_result_digest="0" * 64,
            publication_decision=decision,
        )

    digest = validation_result_digest(
        evaluation_claim=evaluation_id,
        ruleset_digest=ruleset_digest,
        dispositions=dispositions,
        batch_findings=batch,
        framed_count=str(denominator),
        accepted_count=str(accepted),
        error_count=str(error_units),
        warning_count=str(warning_units),
        quarantined_count=str(quarantined),
        error_numerator=str(numerator),
        error_denominator=str(denominator),
        publication_decision=decision,
    )
    validation = ValidationResult(
        schema=VALIDATION_RESULT_SCHEMA,
        evaluation_id=evaluation_id,
        ruleset_digest=ruleset_digest,
        batch_findings=batch,
        framed_count=str(denominator),
        accepted_count=str(accepted),
        error_count=str(error_units),
        warning_count=str(warning_units),
        quarantined_count=str(quarantined),
        error_numerator=str(numerator),
        error_denominator=str(denominator),
        publication_decision=decision,
        validation_result_digest=digest,
    )
    if snapshot_acceptance is not None:
        snapshot_acceptance = snapshot_acceptance.model_copy(update={"validation_result_digest": digest})
    return ValidationOutcome(
        validation=validation,
        dispositions=tuple(dispositions),
        snapshot_acceptance=snapshot_acceptance,
        peak_memory_bytes=meter.peak,
        spilled_uniqueness=spill.spilled,
    )


def finding_identities(dispositions: Sequence[Disposition]) -> tuple[tuple[str, str, str, str], ...]:
    identities = []
    for disposition in dispositions:
        for finding in disposition.findings:
            identities.append((
                finding.kind.value,
                finding.rule_id or "",
                finding.field_path or "",
                finding.metadata.diagnostic_code.value,
            ))
    return tuple(sorted(identities))


def revalidate_frames(
    contract: BronzeProductContract,
    frames: Iterable[CandidateFrame],
    *,
    prior_ruleset_digest: Digest,
    prior_dispositions: Sequence[Disposition],
    claim_digest: Digest,
    delivery_id: str,
    evaluation_id: Digest,
    memory_budget_bytes: int,
    scratch_store: ScratchStorePort | None = None,
    attempt_id: Digest | None = None,
    batch_findings: tuple[Finding, ...] = (),
) -> tuple[RemediationActionStatus, ValidationOutcome]:
    """Re-read retained bytes. Same contract/ruleset cannot override a finding
    and yields ``unchanged_finding`` with no new publication identity."""

    source_schema_digest = compute_source_schema_digest(contract)
    published_schema_digest = compute_published_schema_digest(contract)
    current_ruleset = compute_ruleset_digest(
        source_schema_digest, published_schema_digest, contract.delivery.quality.rules,
    )
    outcome = validate_frames(
        contract, frames, claim_digest=claim_digest, delivery_id=delivery_id,
        evaluation_id=evaluation_id, memory_budget_bytes=memory_budget_bytes,
        scratch_store=scratch_store, attempt_id=attempt_id, batch_findings=batch_findings,
    )
    if current_ruleset == prior_ruleset_digest:
        if finding_identities(outcome.dispositions) != finding_identities(prior_dispositions):
            raise PortError("decision_conflict", "same-ruleset revalidation cannot override a finding")
        return RemediationActionStatus.UNCHANGED_FINDING, outcome
    return RemediationActionStatus.REVALIDATED, outcome


def diagnostics_are_metadata_only(payload: Any, forbidden_tokens: Sequence[str]) -> list[str]:
    """Walk a dumped finding/metadata/lifecycle payload and report any token
    that would smuggle a source value or protected membership identifier."""

    leaks: list[str] = []
    forbidden = tuple(token for token in forbidden_tokens if token)

    def walk(node: Any, path: str) -> None:
        if isinstance(node, str):
            for token in forbidden:
                if token and token in node:
                    leaks.append(f"{path} contains forbidden token")
            return
        if isinstance(node, Mapping):
            for key, value in node.items():
                walk(key, path)
                walk(value, f"{path}.{key}" if path else key)
            return
        if isinstance(node, (list, tuple)):
            for index, item in enumerate(node):
                walk(item, f"{path}[{index}]")

    walk(_dump(payload), "")
    return leaks


__all__ = [
    "VALIDATION_RESULT_SCHEMA",
    "ValidationOutcome",
    "batch_blocks_publication",
    "decide_publication",
    "diagnostics_are_metadata_only",
    "disposition_id_for",
    "finding_identities",
    "outcome_digest",
    "partial_publication_permitted",
    "revalidate_frames",
    "validate_frames",
    "validation_result_digest",
]
