"""The deterministic Bronze ingestion service: the state machine that drives one
logical delivery stream from a submitted managed/external delivery input
through landing, validation, quarantine and the two-phase publication commit,
entirely over the nine ``ergasterion.ingestion.ports`` protocols.

``IngestionRuntime`` holds no storage of its own and imports no backend
package -- every read and every write crosses a port. It owns exactly these
concerns: admission (port topology, adapter
implementation versions, plan/manifest agreement, schema readiness and the
aggregate memory/scratch budget, all checked in one place before any
execution), accepting managed/external delivery input, attempt lifecycle
transitions, delivery-mode validation, replay/conflict rules (a resubmitted
claim with the same digest replays idempotently; the same ``delivery_id``
claimed under a different digest conflicts), explicit due evaluation (outbox
leasing, scheduled-occurrence catch-up and retry against an injected clock,
not a background thread), compare-and-swap contract/deployment transitions
(delegated to ``DeliveryStateStorePort.contract_lifecycle`` /
``deployment_lifecycle``, which owns the actual revision counter), publication
intent/confirmation (a two-phase commit: accepted progress and the
publication intent land in one atomic ``state_transaction`` *before* the
target projection is attempted, so a post-intent target failure can never
lose or duplicate progress -- it leaves an invisible ``commit_blocked`` outbox
entry that ``run_due`` resumes exactly once the target is reachable again),
and ordered lifecycle envelopes (one ``LifecycleEvent`` per attempt-state
transition and per Bronze evidence kind, projected through
``LifecycleSinkPort``).

What this module deliberately does not do: parse a payload, evaluate a
quality rule, or decide what a row's disposition is -- that is
``LandingAdapterPort``'s business, whether the bound adapter is a real one or
the in-memory reference in ``ergasterion.ingestion.conformance``.
The runtime treats every port response as already-validated wire data; its
own job is purely the ordering, replay-safety and crash-safety of the calls
across those nine seams.

Two facts this module reuses rather than re-derives: the canonical digest
convention (RFC 8785 over the JSON projection, transcribed below because the
frozen wire-record family is this package's only record dependency) and the
schedule engine. Scheduled occurrences come from
``ergasterion.source_delivery``'s ``current_boundary_at`` /
``next_boundary_after`` / ``is_eligible_boundary``, which are the single
implementation of interval and cron boundary arithmetic; a second copy here
could disagree with the compiler about when a delivery was due.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Callable

from pydantic import BaseModel

import rfc8785

from ergasterion.ingestion.ports import PORT_PROTOCOLS, PortSet
from ergasterion.ingestion.records import (
    Attempt,
    AttemptLifecyclePayload,
    AttemptQuery,
    AttemptState,
    BlockPhase,
    BronzeEvidence,
    BronzeProductContract,
    CandidateReadQuery,
    ContractLifecycleRequest,
    ContractLifecycleTransitionResult,
    Digest,
    DeliveryInput,
    DeliveryManifest,
    DeliveryPublicationPayload,
    DeploymentLifecycleTransitionResult,
    Disposition,
    DispositionPage,
    DispositionStatus,
    ErrorCode,
    IngestionResult,
    LifecycleEvent,
    LifecycleEventBatch,
    LogicalIdentity,
    ManagedPayloadInput,
    MaterializationCompletion,
    MaterializationSession,
    MaterializedBronzeEvidence,
    OpaqueRef,
    OutboxCompletion,
    OutboxEntryKind,
    OutboxEnqueue,
    OutboxFailureDisposition,
    OutboxFailureTransaction,
    OutboxPayload,
    PORT_OPERATION_ORDER,
    ProgressClaim,
    ProjectionConfirmation,
    ProjectionIntent,
    ProjectionIntentKind,
    ProjectionOutboxPayload,
    ProjectionPayload,
    PublicationDecision,
    RawReceipt,
    ReleaseMaterializationRequest,
    ReleaseVisibilityIdentity,
    RemediationCommitCheckpoint,
    RemediationDecision,
    RemediationDecisionKind,
    RemediationDecisionQuery,
    RemediationEvaluation,
    RemediationRelease,
    RemediationReleasePayload,
    ReprocessingClaim,
    RetryDirective,
    StateOutboxTransaction,
    StreamState,
    TimelinessProjectionPayload,
    Token,
    ValidationResult,
    VisibilityIdentity,
)
from ergasterion.framework.bronze_contract import (
    LifecycleEventType,
    PublicationPolicy,
    ReadinessResult,
    TimelinessState,
)
from ergasterion.framework.runtime_binding import (
    AdapterCapabilities,
    DeploymentLifecycleRequest,
    InterfaceReadiness,
    RuntimeBinding,
    RuntimeDeployment,
)
from ergasterion.source_delivery import current_boundary_at, is_eligible_boundary, next_boundary_after

_UTC_FORMAT = "%Y-%m-%dT%H:%M:%S.%f"
LEASE_ITEM_LIMIT = 50


class PortError(Exception):
    """Raised by a port implementation (real or fake) to signal one of
    ``ergasterion.framework.bronze_contract.ERROR_CODES``. The runtime never
    raises a bare ``Exception`` for a domain failure -- every stop condition
    it detects itself (mode mismatch, replay conflict, gap violation, ...)
    raises this with the matching closed error code, exactly like a port
    would, so a caller handles both uniformly."""

    def __init__(self, code: ErrorCode, detail: str = "") -> None:
        super().__init__(f"{code}: {detail}" if detail else code)
        self.code = code
        self.detail = detail


def utc_now_string(dt: datetime | None = None) -> str:
    """Render a ``UtcInstant``-shaped string (microsecond precision, trailing
    ``Z``) from an aware or naive UTC ``datetime``. The runtime never reads the
    wall clock itself -- every caller supplies ``dt`` via its own injected
    clock; this is a pure formatting helper shared by the runtime and by any
    caller (including a fake adapter) building the same wire shape."""

    dt = dt or datetime.now(timezone.utc)
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt.strftime(_UTC_FORMAT) + "Z"


def parse_utc_instant(text: str) -> datetime:
    """Read a ``UtcInstant`` string back into an aware UTC ``datetime`` -- the
    exact inverse of ``utc_now_string``. Boundary arithmetic runs on
    ``datetime`` values, wire records carry the string form, and this pair is
    the only conversion between them."""

    return datetime.strptime(text, _UTC_FORMAT + "Z").replace(tzinfo=timezone.utc)


def digest_token(digest: Digest, prefix: str = "d") -> Token:
    """Reshape a lowercase-hex ``Digest`` into a valid ``Token`` (which must
    start with a lowercase letter, per the IDL's ``^[a-z][a-z0-9._:-]{0,126}$``
    pattern -- a bare hex digest can start with a numeral and so is not
    automatically a valid ``Token``, even though every character it contains
    individually is). Used wherever this module or its conformance reference
    needs an opaque but deterministic identifier of ``Token`` shape derived
    from a digest it already computed, rather than inventing a second,
    unrelated naming scheme."""

    return f"{prefix}-{digest}"


def canonical_digest(payload: object) -> Digest:
    """SHA-256 hex of the RFC 8785 (JCS) canonical bytes of ``payload`` (a
    plain JSON-compatible ``dict``/``list``/scalar, or a Pydantic model dumped
    via ``.model_dump(mode="json", by_alias=True)`` by the caller first). Used
    throughout this module for every digest the runtime itself computes
    (claim, intent, checkpoint, event); mirrors the canonicalisation
    convention ``ergasterion.framework.models.compute_plan_digest`` and
    ``ergasterion.source_delivery.compute_derived_digest`` already establish,
    kept local rather than imported so this package's only dependency stays
    the frozen wire-record family, not the contract compiler."""

    return hashlib.sha256(rfc8785.dumps(payload)).hexdigest()


def _dump(model) -> dict:
    return model.model_dump(mode="json", by_alias=True)


def _evolve(model: BaseModel, **changes: object) -> BaseModel:
    """``dataclasses.replace``'s equivalent for a frozen Pydantic model: return
    a copy of ``model`` with ``changes`` applied. Every wire record this module
    constructs is a frozen ``pydantic.BaseModel`` (``ClosedModel``), not a
    stdlib dataclass, so ``dataclasses.replace`` does not apply to it -- this
    thin wrapper around ``model_copy(update=...)`` is the one place that
    distinction is handled, so every transition site below reads the same
    regardless of which record kind it is evolving."""

    return model.model_copy(update=changes)


def _delivery_claim_digest(manifest: DeliveryManifest) -> Digest:
    return canonical_digest(_dump(manifest))


# --------------------------------------------------------------------------- admission

PORT_FIELD_ORDER: tuple[str, ...] = tuple(PORT_PROTOCOLS)
"""The nine port slot names in IDL ``PortKind`` declaration order, taken from
``ergasterion.ingestion.ports.PORT_PROTOCOLS`` so the runtime, the port
protocols and ``RuntimePortBindings`` can never drift into three orders."""


@dataclass(frozen=True)
class Admission:
    """What a passed admission asserts about one deployment: the exact nine
    port slots it bound, in order, and the aggregate memory and scratch bytes
    the declared operating envelope reserves. A caller keeps this record as the
    evidence that execution was admitted; it carries no permission to execute
    beyond having been produced without an error."""

    port_order: tuple[str, ...]
    aggregate_memory_bytes: int
    aggregate_scratch_bytes: int


def check_port_topology(
    binding: RuntimeBinding, capabilities: Mapping[str, AdapterCapabilities]
) -> tuple[str, ...]:
    """Assert the deployment's port topology is the Bronze topology: all nine
    slots bound in ``PORT_FIELD_ORDER``, each with an ``AdapterCapabilities``
    record naming that same ``port_kind``, declaring exactly the operations
    ``PORT_OPERATION_ORDER`` lists for the slot -- no missing operation and no
    operation the port does not have -- and an ``implementation_version``
    matching the capability the adapter published. Returns the admitted slot
    order so a caller can record it."""

    for field_name in PORT_FIELD_ORDER:
        capability = capabilities.get(field_name)
        if capability is None:
            raise PortError("capability_mismatch", f"port {field_name!r} declares no adapter capabilities")
        if capability.port_kind.value != field_name:
            raise PortError(
                "capability_mismatch",
                f"port {field_name!r} is bound to a capability for port_kind {capability.port_kind.value!r}",
            )
        expected_operations = PORT_OPERATION_ORDER[field_name]
        declared = tuple(capability.operations)
        missing = tuple(op for op in expected_operations if op not in declared)
        if missing:
            raise PortError(
                "capability_mismatch",
                f"port {field_name!r} declares no capability for operation(s) {', '.join(missing)}",
            )
        unknown = tuple(op for op in declared if op not in expected_operations)
        if unknown:
            raise PortError(
                "capability_mismatch",
                f"port {field_name!r} declares operation(s) {', '.join(unknown)} the port does not have",
            )
        bound = getattr(binding.ports, field_name)
        if bound.capability_digest != canonical_digest(_dump(capability)):
            raise PortError(
                "capability_mismatch",
                f"port {field_name!r} binds capability digest {bound.capability_digest!r}, "
                "which is not the digest of the capabilities the adapter published",
            )
    return PORT_FIELD_ORDER


def check_implementation_versions(
    binding: RuntimeBinding, implementation_versions: Mapping[str, str]
) -> None:
    """Assert every bound adapter is running the implementation version the
    binding names. A deployment that was authored against one adapter build and
    is executed against another is rejected here rather than discovered halfway
    through a delivery."""

    for field_name in PORT_FIELD_ORDER:
        bound = getattr(binding.ports, field_name)
        running = implementation_versions.get(field_name)
        if running is None:
            raise PortError("capability_mismatch", f"port {field_name!r} reports no implementation version")
        if running != bound.implementation_version:
            raise PortError(
                "capability_mismatch",
                f"port {field_name!r} binds implementation version {bound.implementation_version!r} "
                f"but the adapter reports {running!r}",
            )


def check_plan_and_manifest(
    binding: RuntimeBinding, deployment: RuntimeDeployment, execution_plan_digest: Digest,
    runtime_manifest_digest: Digest,
) -> None:
    """Assert the execution plan and runtime manifest an attempt is about to
    run under are the ones this deployment activated. A stale plan is a
    superseded contract; a stale manifest is a superseded deployment. Both fail
    before the attempt exists, so neither can leave partial work behind."""

    if binding.execution_plan_digest != execution_plan_digest:
        raise PortError(
            "superseded_contract",
            f"binding activates execution plan {binding.execution_plan_digest!r}, attempt carries {execution_plan_digest!r}",
        )
    if deployment.active_manifest_digest != runtime_manifest_digest:
        raise PortError(
            "superseded_deployment",
            f"deployment activates runtime manifest {deployment.active_manifest_digest!r}, "
            f"attempt carries {runtime_manifest_digest!r}",
        )


def check_readiness(
    readiness: InterfaceReadiness, contract_digest: Digest, runtime_manifest_digest: Digest, observed_at: str,
) -> None:
    """Assert the schema interface is ready for the contract and manifest in
    hand. A rejected readiness result, a readiness revoked at or before the
    observation instant, or a readiness verified against a different
    contract/manifest all fail closed -- an unready published schema must never
    receive a projection."""

    if readiness.result is not ReadinessResult.READY:
        raise PortError("schema_invalid", f"interface readiness is {readiness.result.value!r}")
    if readiness.revoked_at is not None and readiness.revoked_at <= observed_at:
        raise PortError("schema_invalid", f"interface readiness was revoked at {readiness.revoked_at!r}")
    if readiness.contract_digest != contract_digest:
        raise PortError(
            "capability_mismatch",
            f"interface readiness was verified against contract {readiness.contract_digest!r}, not {contract_digest!r}",
        )
    if readiness.runtime_manifest_digest != runtime_manifest_digest:
        raise PortError(
            "capability_mismatch",
            f"interface readiness was verified against manifest {readiness.runtime_manifest_digest!r}, "
            f"not {runtime_manifest_digest!r}",
        )


def admit_resources(
    binding: RuntimeBinding, capabilities: Mapping[str, AdapterCapabilities]
) -> tuple[int, int]:
    """Compute and admit the aggregate memory and scratch reservation the
    declared operating envelope needs, against the ceilings the bound adapters
    published.

    The formula is fixed: every one of ``max_parallel_attempts`` concurrent
    attempts may hold ``validation_memory_bytes`` at once, and the aggregate
    must fit inside ``process_memory_bytes``; each of those attempts may also
    reserve ``scratch_reservation_bytes`` of scratch. Aggregate memory may not
    exceed the smallest ``max_memory_bytes`` any bound adapter admits, and
    aggregate scratch may not exceed the scratch store's own
    ``max_scratch_bytes``. Returns the two admitted aggregates."""

    resources = binding.runtime_resources
    parallel = int(resources.max_parallel_attempts)
    aggregate_memory = parallel * int(resources.validation_memory_bytes)
    aggregate_scratch = parallel * int(resources.scratch_reservation_bytes)

    process_memory = int(resources.process_memory_bytes)
    if aggregate_memory > process_memory:
        raise PortError(
            "capacity_exceeded",
            f"{parallel} parallel attempts at {resources.validation_memory_bytes} validation bytes each "
            f"aggregate to {aggregate_memory}, over the declared process ceiling {process_memory}",
        )
    for field_name in PORT_FIELD_ORDER:
        capability = capabilities.get(field_name)
        if capability is None:
            raise PortError("capability_mismatch", f"port {field_name!r} declares no adapter capabilities")
        admitted_memory = int(capability.limits.max_memory_bytes)
        if aggregate_memory > admitted_memory:
            raise PortError(
                "capacity_exceeded",
                f"aggregate memory {aggregate_memory} exceeds the {admitted_memory} port {field_name!r} admits",
            )
    admitted_scratch = int(capabilities["scratch_store"].limits.max_scratch_bytes)
    if aggregate_scratch > admitted_scratch:
        raise PortError(
            "capacity_exceeded",
            f"aggregate scratch {aggregate_scratch} exceeds the {admitted_scratch} the scratch store admits",
        )
    return aggregate_memory, aggregate_scratch


def admit(
    binding: RuntimeBinding, deployment: RuntimeDeployment, capabilities: Mapping[str, AdapterCapabilities],
    implementation_versions: Mapping[str, str], readiness: InterfaceReadiness, contract: BronzeProductContract,
    execution_plan_digest: Digest, runtime_manifest_digest: Digest, observed_at: str,
) -> Admission:
    """The single admission gate every execution passes before it starts: port
    topology, adapter implementation versions, plan and manifest agreement,
    schema readiness, and the aggregate memory/scratch budget. Every failure is
    a ``PortError`` with a closed error code and happens before any attempt
    exists, so nothing admitted here can leave half-finished state behind."""

    contract_digest = canonical_digest(_dump(contract))
    if binding.contract_digest != contract_digest:
        raise PortError(
            "superseded_contract",
            f"binding activates contract {binding.contract_digest!r}, execution carries {contract_digest!r}",
        )
    port_order = check_port_topology(binding, capabilities)
    check_implementation_versions(binding, implementation_versions)
    check_plan_and_manifest(binding, deployment, execution_plan_digest, runtime_manifest_digest)
    check_readiness(readiness, contract_digest, runtime_manifest_digest, observed_at)
    aggregate_memory, aggregate_scratch = admit_resources(binding, capabilities)
    return Admission(
        port_order=port_order, aggregate_memory_bytes=aggregate_memory, aggregate_scratch_bytes=aggregate_scratch,
    )


# --------------------------------------------------------------------------- scheduled occurrences

def scheduled_occurrences(
    contract: BronzeProductContract, since: str | None, now: str, max_occurrences: int,
) -> tuple[str, ...]:
    """Every mandatory scheduled occurrence the contract's schedule places
    strictly after ``since`` and at or before ``now``, ascending, capped at
    ``max_occurrences``.

    ``since is None`` evaluates only the current boundary at ``now`` -- a
    stream that has never been evaluated does not retroactively backfill its
    whole history. Once a boundary is known, passing it as ``since`` yields
    every later occurrence: a clock that jumps forward by several intervals
    catches up on all of them in order rather than collapsing them into the
    latest one, which is what makes a missed occurrence recoverable instead of
    lost."""

    schedule = contract.delivery.schedule
    evaluated_through = parse_utc_instant(now)
    if since is None:
        current = current_boundary_at(schedule, evaluated_through)
        return () if current is None else (utc_now_string(current),)
    occurrences: list[str] = []
    cursor = parse_utc_instant(since)
    while len(occurrences) < max_occurrences:
        cursor = next_boundary_after(schedule, cursor)
        if cursor > evaluated_through:
            break
        if is_eligible_boundary(schedule, cursor, evaluated_through):
            occurrences.append(utc_now_string(cursor))
    return tuple(occurrences)


def timeliness_state(
    contract: BronzeProductContract, boundary_at: str, last_committed_at: str | None, now: str,
) -> TimelinessState:
    """The timeliness of one scheduled occurrence: satisfied by a commit at or
    after the boundary, otherwise late once the warning minutes have elapsed
    and missing once the error minutes have."""

    if last_committed_at is not None and last_committed_at >= boundary_at:
        return TimelinessState.ON_TIME
    boundary = parse_utc_instant(boundary_at)
    observed = parse_utc_instant(now)
    lateness = contract.delivery.schedule_lateness
    if observed >= boundary + timedelta(minutes=int(lateness.error_after_minutes)):
        return TimelinessState.MISSING
    if observed >= boundary + timedelta(minutes=int(lateness.warn_after_minutes)):
        return TimelinessState.LATE
    return TimelinessState.AWAITING


@dataclass(frozen=True)
class AppliedProjection:
    """The outcome of applying one staged projection intent at the target.

    A delivery publication carries the attempt it belongs to; a scheduled
    occurrence has no attempt of its own, which is why this is a runtime-local
    view rather than the ``IngestionResult`` wire record, whose ``attempt`` is
    mandatory. ``publish`` converts the one it produces into an
    ``IngestionResult``; ``run_due`` returns these directly, so one caller sees
    per-entry evidence for both kinds."""

    attempt: Attempt | None
    projection_confirmation: ProjectionConfirmation | None
    retry_directive: RetryDirective | None
    visibility: VisibilityIdentity | None


@dataclass
class _LandingProgress:
    """The attempt and stream state one in-flight landing has actually reached.

    ``_land_and_validate`` advances this in place at each transition, so the
    handler in ``land_and_validate`` can fail the attempt against the revision
    the state store really holds. Failing against the revision landing started
    from would be refused by the store's own compare-and-swap, and that
    ``stale_revision`` would replace the closed error code the port raised --
    the caller would learn the state was stale rather than why the delivery
    failed."""

    attempt: Attempt
    stream_state: StreamState

    def advance(self, attempt: Attempt, stream_state: StreamState) -> Attempt:
        self.attempt = attempt
        self.stream_state = stream_state
        return attempt


@dataclass(frozen=True)
class Clock:
    """A deterministic, injectable clock. ``now()`` returns a ``UtcInstant``
    string; the runtime never calls ``datetime.now`` itself. A test advances
    time by constructing a new ``Clock`` (or a subclass) with a fixed
    sequence -- the "trusted-clock catch-up" acceptance is proven by driving
    ``run_due`` across a sequence of clock values a test controls, never by
    sleeping."""

    now_fn: Callable[[], datetime]

    def now(self) -> str:
        return utc_now_string(self.now_fn())


class IngestionRuntime:
    """Drives one Bronze logical identity's delivery lifecycle over an
    injected ``PortSet`` and ``Clock``. Stateless itself: every method reads
    its starting ``StreamState``/``Attempt`` from the caller (typically freshly
    queried from ``state_store``) and returns the next one; nothing is cached
    across calls, so two ``IngestionRuntime`` instances built from the same
    ``PortSet`` behave identically -- there is no in-process runtime state a
    crash could lose."""

    def __init__(self, ports: PortSet, clock: Clock, lease_owner: Token = "runtime-worker-1") -> None:
        self.ports = ports
        self.clock = clock
        self.lease_owner = lease_owner

    # ----------------------------------------------------------------- submission

    def _iter_attempts(
        self, logical_identity: LogicalIdentity, *, claim_digest: Digest | None = None,
        nonterminal_only: bool = False, max_items: int = LEASE_ITEM_LIMIT,
    ) -> Iterator[Attempt]:
        """Every attempt matching the query, across every page.

        A stream outlives one page: an admission rule that only ever read the
        first ``max_items`` attempts would stop seeing the conflicting claim,
        the blocked predecessor or the prior reprocessing attempt as soon as the
        stream grew past that many, and would then admit exactly the duplicate
        it exists to refuse. Every scan below walks ``after_attempt_id`` until
        ``AttemptPage.more`` is false; a caller that only needs the first match
        still stops there, because this yields lazily."""

        after: Digest | None = None
        while True:
            page = self.ports.state_store.attempts(
                AttemptQuery(
                    logical_identity=logical_identity, claim_digest=claim_digest,
                    nonterminal_only=nonterminal_only, after_attempt_id=after, max_items=max_items,
                )
            )
            yield from page.attempts
            if not page.more or not page.attempts:
                return
            after = page.next_after_attempt_id or page.attempts[-1].attempt_id

    def _count_attempts(self, logical_identity: LogicalIdentity) -> int:
        return sum(1 for _ in self._iter_attempts(logical_identity))

    def _existing_attempt_for_claim(self, logical_identity: LogicalIdentity, claim_digest: Digest) -> Attempt | None:
        return next(self._iter_attempts(logical_identity, claim_digest=claim_digest), None)

    def _conflicting_delivery(
        self, logical_identity: LogicalIdentity, delivery_id: Token, claim_digest: Digest
    ) -> Attempt | None:
        return next(
            (
                attempt for attempt in self._iter_attempts(logical_identity)
                if attempt.delivery_id == delivery_id and attempt.claim_digest != claim_digest
            ),
            None,
        )

    def _blocking_predecessor(self, logical_identity: LogicalIdentity, claim_digest: Digest) -> Attempt | None:
        """A ``commit_blocked`` attempt for a *different* claim on this stream.
        Its accepted progress is reserved but not yet visible at the target, so
        a successor carrying different content cannot be admitted behind it --
        doing so would decide the stream's order before the blocked publication
        resolves. A successor carrying the *same* claim is the blocked
        publication itself and replays."""

        return next(
            (
                attempt for attempt in self._iter_attempts(logical_identity, nonterminal_only=True)
                if attempt.state == AttemptState.COMMIT_BLOCKED and attempt.claim_digest != claim_digest
            ),
            None,
        )

    def submit_managed(
        self, stream_state: StreamState, contract: BronzeProductContract, execution_plan_digest: Digest,
        runtime_manifest_digest: Digest, run_id: Digest, input: ManagedPayloadInput,
    ) -> tuple[Attempt, StreamState]:
        """Accept a managed payload submission: validate the delivery mode
        against the contract, detect replay (same claim digest -> the prior
        ``Attempt`` is returned unchanged, no new state written) and conflict
        (same ``delivery_id`` under a different claim digest -> ``claim_conflict``),
        then commit a fresh ``RECEIVED`` attempt. No progress advances here --
        this is the pre-intent phase; a failure at or before this point never
        touches ``accepted_progress``."""

        expected_contract_digest = canonical_digest(_dump(contract))
        if input.manifest.contract_digest != expected_contract_digest:
            raise PortError(
                "capability_mismatch",
                f"manifest declares contract_digest {input.manifest.contract_digest!r}, "
                f"active contract is {expected_contract_digest!r}",
            )
        self._validate_mode(contract, input)

        delivery_input: DeliveryInput = self.ports.source_connector.submit_managed(input)
        manifest = delivery_input.manifest
        claim_digest = _delivery_claim_digest(manifest)

        replay = self._existing_attempt_for_claim(contract.logical_identity, claim_digest)
        if replay is not None:
            return replay, stream_state

        conflict = self._conflicting_delivery(contract.logical_identity, manifest.delivery_id, claim_digest)
        if conflict is not None:
            raise PortError("claim_conflict", f"delivery_id {manifest.delivery_id!r} already claimed differently")

        blocked = self._blocking_predecessor(contract.logical_identity, claim_digest)
        if blocked is not None:
            raise PortError(
                "inflight_attempt",
                f"attempt {blocked.attempt_id!r} is commit_blocked on a different claim; "
                "its reserved progress must resolve before a successor is admitted",
            )

        # The claim digest is what makes an attempt this attempt: the replay
        # check above already returned any attempt carrying it, so deriving the
        # identifier from the claim alone is collision-free however long the
        # stream grows. A page-relative ordinal is not -- it repeats as soon as
        # the stream exceeds one page.
        ordinal = self._count_attempts(contract.logical_identity) + 1
        attempt_id = canonical_digest({
            "logical_identity": _dump(contract.logical_identity), "claim_digest": claim_digest,
        })
        attempt = Attempt(
            run_id=run_id, attempt_id=attempt_id, logical_identity=contract.logical_identity,
            claim_digest=claim_digest, delivery_id=manifest.delivery_id,
            scheduled_boundary_at=manifest.scheduled_boundary_at or self.clock.now(),
            attempt_ordinal=ordinal, state=AttemptState.RECEIVED, block_phase=None,
            reason_code=None, execution_plan_digest=execution_plan_digest,
            runtime_manifest_digest=runtime_manifest_digest,
            state_revision=str(int(stream_state.state_revision) + 1),
        )
        next_state = _evolve(stream_state, state_revision=str(int(stream_state.state_revision) + 1))
        committed = self.ports.state_store.state_transaction(
            StateOutboxTransaction(
                expected_state_revision=stream_state.state_revision, next_state=next_state,
                attempt_updates=(attempt,), deployment_update=None, projection_confirmation=None,
                enqueue=(), complete=(),
            )
        )
        self._emit_lifecycle(
            attempt, committed, LifecycleEventType.RECEIVED,
            AttemptLifecyclePayload(kind=AttemptState.RECEIVED, attempt=attempt, projection_confirmation=None),
        )
        return attempt, committed

    def _validate_mode(self, contract: BronzeProductContract, input: ManagedPayloadInput) -> None:
        """Delivery-mode validation, all of it, before any port beyond the
        connector is touched: a managed payload requires a managed integration,
        the progress claim's kind must be the kind the contract declares, and a
        complete-snapshot delivery must carry the snapshot attestation its mode
        exists to prove."""

        integration_kind = contract.landing.integration.kind
        if integration_kind != "managed":
            raise PortError(
                "invalid_manifest",
                f"a managed payload was submitted for a {integration_kind!r} integration",
            )
        claim: ProgressClaim = input.manifest.progress_claim
        expected_kind = contract.delivery.progress.kind
        if claim.kind != expected_kind:
            raise PortError(
                "invalid_manifest",
                f"delivery declares progress kind {expected_kind!r}, claim carries {claim.kind!r}",
            )
        if contract.delivery.mode.value == "complete_snapshot" and input.manifest.snapshot_attestation is None:
            raise PortError("invalid_manifest", "a complete_snapshot delivery carries no snapshot attestation")

    # ----------------------------------------------------------------- state transitions

    def _transition(
        self, attempt: Attempt, stream_state: StreamState, *, state: AttemptState,
        block_phase: BlockPhase | None = None, reason_code: ErrorCode | None = None,
    ) -> tuple[Attempt, StreamState]:
        next_attempt = _evolve(
            attempt, state=state, block_phase=block_phase, reason_code=reason_code,
            state_revision=str(int(stream_state.state_revision) + 1),
        )
        next_state = _evolve(stream_state, state_revision=str(int(stream_state.state_revision) + 1))
        committed = self.ports.state_store.state_transaction(
            StateOutboxTransaction(
                expected_state_revision=stream_state.state_revision, next_state=next_state,
                attempt_updates=(next_attempt,), deployment_update=None, projection_confirmation=None,
                enqueue=(), complete=(),
            )
        )
        self._emit_lifecycle(
            next_attempt, committed, LifecycleEventType(state.value),
            AttemptLifecyclePayload(kind=state, attempt=next_attempt, projection_confirmation=None),
        )
        return next_attempt, committed

    def _emit_lifecycle(self, attempt: Attempt, stream_state: StreamState, event_type: LifecycleEventType, payload) -> None:
        payload_digest = canonical_digest(_dump(payload))
        event = LifecycleEvent(
            event_id=canonical_digest({"attempt_id": attempt.attempt_id, "event_type": event_type.value,
                                        "state_revision": stream_state.state_revision}),
            event_type=event_type, logical_identity=attempt.logical_identity,
            state_revision=stream_state.state_revision, event_ordinal=stream_state.state_revision,
            attempt_id=attempt.attempt_id,
            execution_plan_digest=attempt.execution_plan_digest, runtime_manifest_digest=attempt.runtime_manifest_digest,
            payload=payload, payload_digest=payload_digest, created_at=self.clock.now(),
        )
        self.ports.lifecycle_sink.project_events(LifecycleEventBatch(events=(event,), max_items=1, bytes_supplied="0"))

    # ----------------------------------------------------------------- landing + validation

    def land_and_validate(
        self, attempt: Attempt, stream_state: StreamState, contract: BronzeProductContract, raw_receipt: RawReceipt,
        visibility: VisibilityIdentity, evaluation_id: Digest, ruleset_digest: Digest,
    ) -> tuple[Attempt, StreamState, MaterializedBronzeEvidence, ValidationResult]:
        """Drive one attempt from ``RECEIVED`` through ``PREPARING`` and
        ``MATERIALIZING`` to a completed ``ValidationResult``, entirely through
        ``RawStorePort`` and ``LandingAdapterPort``. Returns the attempt still in
        ``VALIDATING`` state -- the caller (``publish`` or the ``FAILED`` path)
        makes the accept/reject decision; this method never commits progress.

        A port failure anywhere in that sequence fails the attempt against the
        state it had actually reached, not the one landing started from, and
        re-raises the port's own closed error code."""

        attempt, stream_state = self._transition(attempt, stream_state, state=AttemptState.PREPARING)
        progress = _LandingProgress(attempt=attempt, stream_state=stream_state)
        try:
            return self._land_and_validate(
                progress, contract, raw_receipt, visibility, evaluation_id, ruleset_digest,
            )
        except PortError as exc:
            self.fail_attempt(progress.attempt, progress.stream_state, exc.code)
            raise

    def fail_attempt(
        self, attempt: Attempt, stream_state: StreamState, reason_code: ErrorCode,
    ) -> tuple[Attempt, StreamState]:
        """Drive one attempt to ``FAILED`` with its reason code. Reachable at
        any point before the publication intent lands: no progress has been
        reserved yet, so failing here leaves ``accepted_progress`` exactly as it
        was and stages no outbox entry."""

        return self._transition(attempt, stream_state, state=AttemptState.FAILED, reason_code=reason_code)

    def _land_and_validate(
        self, progress: "_LandingProgress", contract: BronzeProductContract, raw_receipt: RawReceipt,
        visibility: VisibilityIdentity, evaluation_id: Digest, ruleset_digest: Digest,
    ) -> tuple[Attempt, StreamState, MaterializedBronzeEvidence, ValidationResult]:
        attempt = progress.attempt
        handle = self.ports.raw_store.open_raw(raw_receipt.raw_receipt_digest)
        preparation = self.ports.landing_adapter.begin_prepare(attempt.attempt_id, raw_receipt, handle, contract, visibility)
        offset = "0"
        while not preparation.closed:
            page = self.ports.raw_store.read_raw(handle, offset, str(handle.byte_length))
            preparation = self.ports.landing_adapter.append_raw(preparation, page)
            if page.eof:
                break
            offset = page.next_offset or offset
        evidence: BronzeEvidence = self.ports.landing_adapter.finish_prepare(preparation)

        attempt = progress.advance(*self._transition(attempt, progress.stream_state, state=AttemptState.MATERIALIZING))

        session: MaterializationSession = self.ports.landing_adapter.begin_materialization(
            attempt.attempt_id, evidence, evaluation_id, ruleset_digest
        )
        dispositions: list[Disposition] = []
        after_sequence: str | None = None
        while True:
            frame_page = self.ports.landing_adapter.read_candidate(
                CandidateReadQuery(evidence=evidence, after_sequence=after_sequence, max_frames=1000, max_bytes="1000000")
            )
            page_dispositions = tuple(
                Disposition(
                    disposition_id=canonical_digest({"frame": frame.frame_sequence, "attempt": attempt.attempt_id}),
                    raw_ref=evidence.candidate_ref, raw_locator=frame.raw_locator, delivery_id=attempt.delivery_id or "",
                    claim_digest=attempt.claim_digest, ruleset_digest=ruleset_digest,
                    product_version=contract.product.product_version, contract_digest=canonical_digest(_dump(contract)),
                    source_schema_digest=canonical_digest({"schema": "source", "digest": evaluation_id}),
                    published_schema_digest=canonical_digest({"schema": "published", "digest": evaluation_id}),
                    status=DispositionStatus.REJECTED if frame.structural_findings else DispositionStatus.ACCEPTED,
                    findings=frame.structural_findings,
                    outcome_digest=canonical_digest({"frame": frame.frame_sequence, "findings": len(frame.structural_findings)}),
                )
                for frame in frame_page.frames
            )
            dispositions.extend(page_dispositions)
            if page_dispositions:
                session = self.ports.landing_adapter.append_dispositions(
                    session,
                    DispositionPage(
                        session_id=session.session_id, dispositions=page_dispositions,
                        first_frame_sequence=page_dispositions[0].raw_locator.frame_sequence,
                        next_frame_sequence=str(int(page_dispositions[-1].raw_locator.frame_sequence) + 1),
                        bytes_supplied="0",
                    ),
                )
            if not frame_page.more:
                break
            after_sequence = frame_page.next_after_sequence

        accepted = sum(1 for d in dispositions if d.status == DispositionStatus.ACCEPTED)
        rejected = len(dispositions) - accepted
        # Partial acceptance exists only where the contract admits it: under
        # ``all_or_nothing`` a single rejected row rejects the whole delivery.
        # The rejection arithmetic itself belongs to the validation engine
        # behind ``LandingAdapterPort``; the runtime enforces only which
        # publication decisions the contract's mode permits.
        partial_permitted = contract.delivery.quality.publication_mode is PublicationPolicy.PUBLISH_VALID_ROWS
        if dispositions and rejected == 0:
            decision = PublicationDecision.PUBLISH_ALL
        elif accepted > 0 and partial_permitted:
            decision = PublicationDecision.PUBLISH_VALID_ROWS
        else:
            decision = PublicationDecision.REJECT_DELIVERY
        validation = ValidationResult(
            schema="ergasterion.validation-result/v1", evaluation_id=evaluation_id, ruleset_digest=ruleset_digest,
            batch_findings=(), framed_count=str(len(dispositions)), accepted_count=str(accepted),
            error_count=str(rejected), warning_count="0", quarantined_count=str(rejected),
            error_numerator=str(rejected), error_denominator=str(max(len(dispositions), 1)),
            publication_decision=decision,
            validation_result_digest=canonical_digest({"evaluation_id": evaluation_id, "accepted": accepted, "rejected": rejected}),
        )
        completion = MaterializationCompletion(session=session, validation=validation, candidate_keyset=None, output_visibility=None)
        materialized: MaterializedBronzeEvidence = self.ports.landing_adapter.finish_materialization(completion)

        attempt = progress.advance(*self._transition(attempt, progress.stream_state, state=AttemptState.VALIDATING))
        for disposition in dispositions:
            if disposition.status == DispositionStatus.REJECTED:
                self.ports.remediation_repository.record_decision(
                    RemediationDecision(
                        schema="ergasterion.remediation-decision/v1",
                        decision_id=canonical_digest({"disposition": disposition.disposition_id, "kind": "evaluated"}),
                        kind=RemediationDecisionKind.EVALUATED,
                        evaluation=RemediationEvaluation(
                            schema="ergasterion.remediation-evaluation/v1", original_claim_digest=attempt.claim_digest,
                            raw_receipt_digest=raw_receipt.raw_receipt_digest, target_contract_digest=disposition.contract_digest,
                            target_source_schema_digest=disposition.source_schema_digest,
                            target_published_schema_digest=disposition.published_schema_digest,
                            target_ruleset_digest=ruleset_digest, execution_plan_digest=attempt.execution_plan_digest,
                            root_visibility_epoch=visibility.epoch,
                            remediation_evaluation_id=canonical_digest({"disposition": disposition.disposition_id}),
                        ),
                        disposition_ids=(disposition.disposition_id,), validation_result_digest=validation.validation_result_digest,
                        release=None, decided_at=self.clock.now(),
                    )
                )

        return attempt, progress.stream_state, materialized, validation

    # ----------------------------------------------------------------- publication (two-phase)

    def publish(
        self, attempt: Attempt, stream_state: StreamState, contract: BronzeProductContract, materialized: MaterializedBronzeEvidence,
        validation: ValidationResult, visibility: VisibilityIdentity, raw_receipt: RawReceipt,
        readiness: InterfaceReadiness,
    ) -> IngestionResult:
        """The publication two-phase commit. Phase one lands the
        ``ProjectionIntent`` and the advanced ``accepted_progress`` in a single
        ``state_transaction`` -- this is the durability point; nothing after this
        can lose the fact that this delivery was accepted. Phase two applies the
        intent at the target through ``ProjectionPublisherPort``: on success the
        outbox entry completes and the attempt reaches ``COMMITTED``; on a target
        failure the attempt is left ``COMMIT_BLOCKED`` with a retryable outbox
        entry ``run_due`` resumes -- the same intent, replayed idempotently,
        never re-derives progress.

        The published schema must be ready: an unready or revoked interface
        readiness fails the attempt before the intent is built, so a
        not-yet-ready schema can never receive reserved progress."""

        if validation.publication_decision == PublicationDecision.REJECT_DELIVERY:
            failed, stream_state = self.fail_attempt(attempt, stream_state, "contract_invalid")
            return IngestionResult(
                kind="ingestion", attempt=failed, visibility=None, publication=None,
                projection_confirmation=None, retry_directive=None,
            )

        contract_digest = canonical_digest(_dump(contract))
        try:
            check_readiness(readiness, contract_digest, attempt.runtime_manifest_digest, self.clock.now())
        except PortError as exc:
            self.fail_attempt(attempt, stream_state, exc.code)
            raise
        payload = DeliveryPublicationPayload(
            kind="delivery_publication", attempt_id=attempt.attempt_id, visibility=visibility,
            product_version=contract.product.product_version, contract_digest=contract_digest,
            source_schema_digest=canonical_digest({"schema": "source", "digest": attempt.attempt_id}),
            published_schema_digest=canonical_digest({"schema": "published", "digest": attempt.attempt_id}),
            readiness_digest=readiness.readiness_digest, delivery_claim_digest=attempt.claim_digest,
            transport_payload_digest=raw_receipt.payload.content_id.split(":", 1)[-1],
            raw_receipt_ref=materialized.prepared.candidate_ref, raw_receipt_digest=raw_receipt.raw_receipt_digest,
            bronze_partition_ref=materialized.accepted_ref, accepted_content_digest=materialized.accepted_content_digest,
            ruleset_digest=validation.ruleset_digest, validation_result_digest=validation.validation_result_digest,
            accepted_count=validation.accepted_count, progress_claim={"kind": "opaque_batch"},
            deletion_evidence=None, scheduled_boundary_at=attempt.scheduled_boundary_at,
            warning_deadline_at=attempt.scheduled_boundary_at, error_deadline_at=attempt.scheduled_boundary_at,
            prior_committed_at=stream_state.last_committed_at, lineage_digest=canonical_digest({"attempt": attempt.attempt_id}),
        )
        intent = self._build_intent(
            stream_state, contract_digest, ProjectionIntentKind.DELIVERY_PUBLICATION, payload,
            attempt.execution_plan_digest, attempt.runtime_manifest_digest,
        )

        committing, committed_state = self._transition(attempt, stream_state, state=AttemptState.COMMITTING)
        committed_state = _evolve(
            committed_state, active_contract_digest=contract_digest,
            accepted_progress={"accepted_count": {"logical_type": "int64", "value": validation.accepted_count}},
            last_committed_visibility=visibility, required_projection_revision=intent.projection_revision,
        )
        outbox_id = canonical_digest({"intent": intent.projection_intent_digest})
        staged = self.ports.state_store.state_transaction(
            StateOutboxTransaction(
                expected_state_revision=committing.state_revision, next_state=committed_state,
                attempt_updates=(committing,), deployment_update=None, projection_confirmation=None,
                enqueue=(OutboxEnqueue(
                    outbox_id=outbox_id, payload=ProjectionOutboxPayload(entry_kind="projection", intent=intent),
                    payload_digest=intent.projection_intent_digest, next_not_before=self.clock.now(),
                ),), complete=(),
            )
        )
        applied = self._apply_outbox_intent(
            committing, staged, outbox_id, intent, dispatch_ordinal=1,
            max_attempts=int(contract.delivery.retry.max_attempts),
        )
        return IngestionResult(
            kind="ingestion", attempt=applied.attempt or committing, visibility=applied.visibility,
            publication=None, projection_confirmation=applied.projection_confirmation,
            retry_directive=applied.retry_directive,
        )

    def _build_intent(
        self, stream_state: StreamState, contract_digest: Digest, kind: ProjectionIntentKind, payload: ProjectionPayload,
        execution_plan_digest: Digest, runtime_manifest_digest: Digest,
    ) -> ProjectionIntent:
        payload_digest = canonical_digest(_dump(payload))
        revision = str(int(stream_state.required_projection_revision) + 1)
        base = {
            "schema": "ergasterion.projection-intent/v1", "logical_identity": _dump(stream_state.logical_identity),
            "contract_digest": contract_digest, "projection_target": "bronze", "projection_revision": revision,
            "originating_state_revision": stream_state.state_revision, "kind": kind.value,
            "payload_digest": payload_digest,
        }
        intent_digest = canonical_digest(base)
        return ProjectionIntent(
            schema="ergasterion.projection-intent/v1", logical_identity=stream_state.logical_identity,
            contract_digest=contract_digest, projection_target="bronze", projection_revision=revision,
            originating_state_revision=stream_state.state_revision, kind=kind,
            execution_plan_digest=execution_plan_digest, runtime_manifest_digest=runtime_manifest_digest,
            payload=payload, payload_digest=payload_digest, projection_intent_digest=intent_digest,
        )

    def _apply_outbox_intent(
        self, attempt: Attempt | None, stream_state: StreamState, outbox_id: Digest, intent: ProjectionIntent,
        dispatch_ordinal: int, max_attempts: int,
    ) -> AppliedProjection:
        """Apply one already-staged intent at the target, then settle its outbox
        entry. ``attempt`` is ``None`` for a scheduled occurrence, which has no
        attempt of its own; a delivery publication carries one and moves it to
        ``COMMITTED`` or ``COMMIT_BLOCKED`` alongside the outbox settlement.

        A target failure at or past ``max_attempts`` dispatches is exhausted and
        dead-letters instead of staying retryable, so a permanently unreachable
        target stops consuming due evaluations while its reserved progress
        remains recorded and invisible."""

        try:
            confirmation: ProjectionConfirmation = self.ports.projection_publisher.apply_gap_ordered(intent)
        except PortError as exc:
            failure_time = self.clock.now()
            exhausted = dispatch_ordinal >= max_attempts
            disposition = OutboxFailureDisposition.DEAD_LETTER if exhausted else OutboxFailureDisposition.RETRYABLE
            if attempt is None:
                blocked, next_state = None, stream_state
                expected_revision = stream_state.state_revision
                attempt_updates: tuple[Attempt, ...] = ()
                ordinal = 1
            else:
                blocked, next_state = self._transition(
                    attempt, stream_state, state=AttemptState.COMMIT_BLOCKED,
                    block_phase=BlockPhase.PROJECTION_BLOCKED, reason_code=exc.code,
                )
                expected_revision = blocked.state_revision
                attempt_updates = (blocked,)
                ordinal = blocked.attempt_ordinal
            self.ports.state_store.fail_outbox(
                OutboxFailureTransaction(
                    expected_state_revision=expected_revision, next_state=next_state, attempt_updates=attempt_updates,
                    outbox_id=outbox_id, payload_digest=intent.projection_intent_digest, lease_owner=self.lease_owner,
                    failure_observed_at=failure_time, reason_code=exc.code, disposition=disposition,
                    next_not_before=failure_time,
                )
            )
            return AppliedProjection(
                attempt=blocked or attempt, projection_confirmation=None, visibility=None,
                retry_directive=RetryDirective(
                    attempt_ordinal=ordinal, error_code=exc.code, failure_observed_at=failure_time,
                    next_not_before=failure_time, exhausted=exhausted,
                ),
            )

        if attempt is None:
            committed, final_state = None, stream_state
            expected_revision = stream_state.state_revision
            attempt_updates = ()
        else:
            committed, final_state = self._transition(attempt, stream_state, state=AttemptState.COMMITTED)
            expected_revision = committed.state_revision
            attempt_updates = (committed,)
        final_state = _evolve(final_state, last_committed_at=confirmation.committed_at or self.clock.now())
        self.ports.state_store.state_transaction(
            StateOutboxTransaction(
                expected_state_revision=expected_revision, next_state=final_state, attempt_updates=attempt_updates,
                deployment_update=None, projection_confirmation=confirmation, enqueue=(),
                complete=(OutboxCompletion(
                    outbox_id=outbox_id, payload_digest=intent.projection_intent_digest, lease_owner=self.lease_owner,
                    completed_at=self.clock.now(),
                ),),
            )
        )
        return AppliedProjection(
            attempt=committed or attempt, projection_confirmation=confirmation,
            visibility=confirmation.visibility, retry_directive=None,
        )

    # ----------------------------------------------------------------- due evaluation / retry resume

    def run_due(
        self, logical_identity: LogicalIdentity, now: str, max_attempts: int, max_items: int = LEASE_ITEM_LIMIT,
    ) -> tuple[AppliedProjection, ...]:
        """Explicit due evaluation: lease every ``PROJECTION`` outbox entry whose
        ``next_not_before`` has passed, and replay its intent through
        ``ProjectionPublisherPort`` exactly once each. Never called on a timer by
        this module -- the caller (a CLI tick, a test driving a clock sequence)
        decides when "now" has arrived, which is what makes crash/retry and
        trusted-clock-catch-up behaviour reproducible without sleeping.

        Entries are replayed in staged order -- ascending
        ``ProjectionIntent.projection_revision``, which is the sequence the
        revisions were reserved in -- so a resumed publication and a resumed
        scheduled occurrence reach the target in the order they were staged and
        no projection revision is applied out of turn. The order is deliberately
        not ``OutboxEntry.dispatch_ordinal``: that counts delivery attempts at
        the target, so an entry that has failed and been retried several times
        sorts *after* a newer entry that has only been dispatched once, which is
        the reverse of the order the target must see."""

        entries = self.ports.state_store.lease_outbox(
            logical_identity, OutboxEntryKind.PROJECTION, self.lease_owner, now, max_items,
        )
        due: list[tuple[int, object, ProjectionIntent]] = []
        for entry in entries:
            if entry.next_not_before > now:
                continue
            payload: OutboxPayload = self.ports.state_store.load_outbox_payload(entry.outbox_id, entry.payload_digest)
            due.append((int(payload.intent.projection_revision), entry, payload.intent))
        results: list[AppliedProjection] = []
        for _revision, entry, intent in sorted(due, key=lambda item: item[0]):
            attempt = self._attempt_for_intent(logical_identity, intent, max_items)
            status = self.ports.state_store.status_query(logical_identity)
            results.append(self._apply_outbox_intent(
                attempt, status.state, entry.outbox_id, intent,
                dispatch_ordinal=entry.dispatch_ordinal, max_attempts=max_attempts,
            ))
        return tuple(results)

    def _attempt_for_intent(
        self, logical_identity: LogicalIdentity, intent: ProjectionIntent, max_items: int,
    ) -> Attempt | None:
        """The attempt a staged intent belongs to, whatever state it reached.
        A scheduled occurrence has no attempt and answers ``None``. Terminal
        attempts are included deliberately: a publication whose attempt already
        committed before its outbox entry settled is still that attempt's
        publication, and a resumption reports it rather than losing it."""

        attempt_id = getattr(intent.payload, "attempt_id", None)
        if attempt_id is None:
            return None
        return next(
            (
                attempt for attempt in self._iter_attempts(logical_identity, max_items=max_items)
                if attempt.attempt_id == attempt_id
            ),
            None,
        )

    # ----------------------------------------------------------------- scheduled occurrences

    def last_evaluated_occurrence(self, logical_identity: LogicalIdentity, max_items: int = LEASE_ITEM_LIMIT) -> str | None:
        """The latest scheduled boundary this stream has a confirmed timeliness
        projection for, read back through the confirmation log rather than held
        in memory -- so a restarted process resumes catch-up from exactly where
        the durable record says it stopped."""

        confirmed = {
            confirmation.projection_intent_digest
            for confirmation in self.ports.state_store.projection_confirmation_log(
                logical_identity, "0", max_items, "1000000",
            ).confirmations
        }
        boundaries = [
            intent.payload.scheduled_boundary_at
            for intent in self.ports.state_store.projection_log(
                logical_identity, "0", max_items, "1000000",
            ).intents
            if intent.kind is ProjectionIntentKind.TIMELINESS and intent.projection_intent_digest in confirmed
        ]
        return max(boundaries) if boundaries else None

    def run_scheduled(
        self, contract: BronzeProductContract, stream_state: StreamState, now: str, since: str | None,
        max_occurrences: int,
    ) -> tuple[StreamState, tuple[str, ...]]:
        """Evaluate every mandatory scheduled occurrence due at ``now``, in
        ascending boundary order, staging and applying exactly one timeliness
        projection for each. Returns the state reached and the boundaries
        actually projected.

        The loop stops at the first occurrence whose target application fails:
        that occurrence's outbox entry stays due, so the next evaluation
        retries it before moving on and no later occurrence is projected ahead
        of it. A failure therefore delays occurrences; it never skips one.

        It also stops *before* staging anything while a reserved projection
        revision has not reached the target. ``incomplete_outbox_count`` catches
        a still-retryable ``commit_blocked`` publication; it does not see a
        dead-lettered one, because a store that counts only pending, leased and
        retryable entries reports that stream as idle. The publisher cursor is
        the remaining gate: while ``required_projection_revision`` is ahead of
        the applied cursor, staging the next occurrence would reserve
        required+1, the target would refuse the gap, and the occurrence would
        burn its own retries for a failure that was never its own. The
        occurrence is simply not evaluated yet: it stays due, and the next call
        after the blockage drains (or is otherwise repaired) projects it in
        its proper place."""

        boundaries = scheduled_occurrences(contract, since, now, max_occurrences)
        contract_digest = canonical_digest(_dump(contract))
        max_attempts = int(contract.delivery.retry.max_attempts)
        projected: list[str] = []
        for boundary_at in boundaries:
            status = self.ports.state_store.status_query(contract.logical_identity)
            if int(status.incomplete_outbox_count) > 0:
                break
            cursor = self.ports.projection_publisher.read_cursor(contract.logical_identity, "bronze")
            if int(status.state.required_projection_revision) > int(cursor.projection_revision):
                break
            lateness = contract.delivery.schedule_lateness
            boundary = parse_utc_instant(boundary_at)
            payload = TimelinessProjectionPayload(
                kind="timeliness", scheduled_boundary_at=boundary_at,
                warning_deadline_at=utc_now_string(boundary + timedelta(minutes=int(lateness.warn_after_minutes))),
                error_deadline_at=utc_now_string(boundary + timedelta(minutes=int(lateness.error_after_minutes))),
                timeliness=timeliness_state(contract, boundary_at, stream_state.last_committed_at, now),
                evaluated_through_at=now, prior_committed_at=stream_state.last_committed_at,
            )
            intent = self._build_intent(
                stream_state, contract_digest, ProjectionIntentKind.TIMELINESS, payload,
                canonical_digest({"plan": _dump(contract.logical_identity)}),
                canonical_digest({"manifest": _dump(contract.logical_identity)}),
            )
            outbox_id = canonical_digest({"intent": intent.projection_intent_digest})
            staged = self.ports.state_store.state_transaction(
                StateOutboxTransaction(
                    expected_state_revision=stream_state.state_revision,
                    next_state=_evolve(stream_state, required_projection_revision=intent.projection_revision),
                    attempt_updates=(), deployment_update=None, projection_confirmation=None,
                    enqueue=(OutboxEnqueue(
                        outbox_id=outbox_id, payload=ProjectionOutboxPayload(entry_kind="projection", intent=intent),
                        payload_digest=intent.projection_intent_digest, next_not_before=now,
                    ),), complete=(),
                )
            )
            result = self._apply_outbox_intent(
                None, staged, outbox_id, intent, dispatch_ordinal=1, max_attempts=max_attempts,
            )
            stream_state = self.ports.state_store.status_query(contract.logical_identity).state
            if result.projection_confirmation is None:
                break
            projected.append(boundary_at)
        return stream_state, tuple(projected)

    # ----------------------------------------------------------------- quarantine release

    def release_quarantine(
        self, attempt: Attempt, stream_state: StreamState, contract: BronzeProductContract,
        evaluation: RemediationEvaluation, selected_locators: tuple, accepted_content_digest: Digest,
        prior_release_ruleset_digest: Digest | None, raw_ref: OpaqueRef | None = None,
    ) -> RemediationDecision:
        """Release a compatible-ruleset subset of previously quarantined rows
        exactly once. A release against the *same* ruleset digest that produced
        the original finding is refused (``decision_conflict``) -- only a
        genuinely new, compatible active ruleset may release a row. The
        remediation repository's own compare-and-swap on
        ``evaluation.remediation_evaluation_id`` is what makes "released once"
        durable: a second release of the same evaluation is refused there, and
        a crash after that compare-and-swap but before its checkpoint reaches
        the state store is finished by ``resume_release`` rather than by
        deciding again.

        ``raw_ref`` (the released frames' candidate partition -- carried on the
        disposition being released, not on the evaluation or the release
        record) is optional only for the pre-existing, locator-free callers
        below; a caller that names ``selected_locators`` supplies it so the
        checkpoint re-materializes those rows into the published projection
        rather than leaving the release a decision-only record."""

        current_ruleset = canonical_digest({"contract": canonical_digest(_dump(contract))})
        if prior_release_ruleset_digest is not None and prior_release_ruleset_digest == current_ruleset:
            raise PortError("decision_conflict", "release attempted under the same ruleset that produced the finding")

        release = RemediationRelease(
            schema="ergasterion.remediation-release/v1", remediation_evaluation_id=evaluation.remediation_evaluation_id,
            selected_locators=selected_locators, accepted_content_digest=accepted_content_digest,
            release_id=canonical_digest({"evaluation": evaluation.remediation_evaluation_id, "kind": "released"}),
        )
        decision = RemediationDecision(
            schema="ergasterion.remediation-decision/v1",
            decision_id=canonical_digest({"evaluation": evaluation.remediation_evaluation_id, "release": release.release_id}),
            kind=RemediationDecisionKind.RELEASED, evaluation=evaluation,
            disposition_ids=tuple(canonical_digest({"locator": i}) for i in range(len(selected_locators))),
            validation_result_digest=canonical_digest({"release": release.release_id}), release=release,
            decided_at=self.clock.now(),
        )
        recorded = self.ports.remediation_repository.record_decision(decision)
        return self._checkpoint_release(
            attempt, stream_state, contract, evaluation, recorded, release.release_id,
            accepted_content_digest, len(selected_locators), raw_ref,
        )

    def resume_release(
        self, attempt: Attempt, stream_state: StreamState, contract: BronzeProductContract,
        evaluation: RemediationEvaluation, accepted_content_digest: Digest, selected_locator_count: int,
        raw_ref: OpaqueRef | None = None,
    ) -> RemediationDecision:
        """Resume a release that crashed after the remediation repository's
        compare-and-swap but before its checkpoint reached the state store.

        The decision is already durable in the repository, so re-deciding it
        would be a second release attempt and would be refused. This reads the
        recorded decision back and writes only the missing checkpoint, so the
        release still happens exactly once. If the crash landed *before* the
        compare-and-swap there is nothing recorded, and the caller retries
        ``release_quarantine`` instead."""

        if attempt.remediation_commit_checkpoint is not None:
            return attempt.remediation_commit_checkpoint.decision
        page = self.ports.remediation_repository.decision_query(
            RemediationDecisionQuery(
                logical_identity=attempt.logical_identity, disposition_id=None,
                authorization_context_ref="operator", snapshot_token=None, after_cursor=None,
                max_items=LEASE_ITEM_LIMIT, max_bytes="1000000",
            )
        )
        recorded = next(
            (
                decision for decision in page.items
                if decision.kind is RemediationDecisionKind.RELEASED
                and decision.evaluation.remediation_evaluation_id == evaluation.remediation_evaluation_id
            ),
            None,
        )
        if recorded is None or recorded.release is None:
            raise PortError(
                "not_found",
                f"no recorded release for evaluation {evaluation.remediation_evaluation_id!r} to resume",
            )
        return self._checkpoint_release(
            attempt, stream_state, contract, evaluation, recorded, recorded.release.release_id,
            accepted_content_digest, selected_locator_count, raw_ref,
        )

    def _checkpoint_release(
        self, attempt: Attempt, stream_state: StreamState, contract: BronzeProductContract,
        evaluation: RemediationEvaluation, recorded: RemediationDecision, release_id: Digest,
        accepted_content_digest: Digest, selected_locator_count: int, raw_ref: OpaqueRef | None = None,
    ) -> RemediationDecision:
        if raw_ref is not None and recorded.release is not None and recorded.release.selected_locators:
            self.ports.landing_adapter.materialize_release(
                ReleaseMaterializationRequest(
                    raw_ref=raw_ref, selected_locators=recorded.release.selected_locators, release_id=release_id,
                    visibility=ReleaseVisibilityIdentity(epoch=stream_state.visibility_epoch, kind="release", id=release_id),
                    accepted_content_digest=recorded.release.accepted_content_digest,
                )
            )
        payload = RemediationReleasePayload(
            kind="remediation_release", attempt_id=attempt.attempt_id,
            remediation_evaluation_id=evaluation.remediation_evaluation_id, release_id=release_id,
            visibility={"epoch": stream_state.visibility_epoch, "kind": "release", "id": release_id},
            product_version=contract.product.product_version, contract_digest=canonical_digest(_dump(contract)),
            source_schema_digest=evaluation.target_source_schema_digest,
            published_schema_digest=evaluation.target_published_schema_digest,
            readiness_digest=canonical_digest({"readiness": "release"}), delivery_claim_digest=attempt.claim_digest,
            transport_payload_digest=canonical_digest({"release": release_id}),
            raw_receipt_ref="release-ref", raw_receipt_digest=evaluation.raw_receipt_digest,
            bronze_partition_ref="release-partition", accepted_content_digest=accepted_content_digest,
            ruleset_digest=evaluation.target_ruleset_digest, validation_result_digest=recorded.validation_result_digest,
            accepted_count=str(selected_locator_count), progress_claim={"kind": "opaque_batch"}, deletion_evidence=None,
            prior_committed_at=stream_state.last_committed_at, lineage_digest=canonical_digest({"release": release_id}),
        )
        checkpoint = RemediationCommitCheckpoint(
            schema="ergasterion.remediation-commit-checkpoint/v1", attempt_id=attempt.attempt_id, decision=recorded,
            release_projection_payload=payload,
            checkpoint_digest=canonical_digest({"decision": recorded.decision_id}),
        )
        checkpointed, next_state = self._transition(attempt, stream_state, state=attempt.state)
        checkpointed = _evolve(checkpointed, remediation_commit_checkpoint=checkpoint)
        self.ports.state_store.state_transaction(
            StateOutboxTransaction(
                expected_state_revision=next_state.state_revision, next_state=next_state,
                attempt_updates=(checkpointed,), deployment_update=None, projection_confirmation=None,
                enqueue=(), complete=(),
            )
        )
        return recorded

    # ----------------------------------------------------------------- whole-delivery reprocessing

    def reprocess_whole_delivery(
        self, stream_state: StreamState, claim: ReprocessingClaim, run_id: Digest,
    ) -> Attempt:
        """Consume a whole-delivery reprocessing claim exactly once, with the
        same replay and conflict rules an ordinary claim has.

        Resubmitting the identical claim -- same ``reprocessing_id`` over the
        same original delivery -- replays: the existing attempt comes back and
        no second attempt is created, so a caller that crashed mid-submission
        can safely repeat itself. The same ``reprocessing_id`` pointed at a
        *different* original delivery is a conflict, not a replay, and is
        rejected."""

        ordinal = 0
        for existing in self._iter_attempts(stream_state.logical_identity):
            ordinal += 1
            if existing.reprocessing_id != claim.reprocessing_id:
                continue
            if existing.claim_digest == claim.original_claim_digest:
                return existing
            raise PortError(
                "claim_conflict",
                f"reprocessing_id {claim.reprocessing_id!r} is already claimed over a different original delivery",
            )

        attempt_id = canonical_digest({
            "logical_identity": _dump(stream_state.logical_identity), "reprocessing_id": claim.reprocessing_id,
        })
        attempt = Attempt(
            run_id=run_id, attempt_id=attempt_id, logical_identity=stream_state.logical_identity,
            claim_digest=claim.original_claim_digest, reprocessing_id=claim.reprocessing_id,
            scheduled_boundary_at=self.clock.now(),
            attempt_ordinal=ordinal + 1, state=AttemptState.RECEIVED, block_phase=None, reason_code=None,
            execution_plan_digest=claim.execution_plan_digest, runtime_manifest_digest=canonical_digest({"manifest": "reprocess"}),
            state_revision=str(int(stream_state.state_revision) + 1),
        )
        next_state = _evolve(stream_state, state_revision=str(int(stream_state.state_revision) + 1))
        self.ports.state_store.state_transaction(
            StateOutboxTransaction(
                expected_state_revision=stream_state.state_revision, next_state=next_state,
                attempt_updates=(attempt,), deployment_update=None, projection_confirmation=None, enqueue=(), complete=(),
            )
        )
        return attempt

    # ----------------------------------------------------------------- contract / deployment CAS

    def contract_lifecycle(self, request: ContractLifecycleRequest) -> ContractLifecycleTransitionResult:
        """Thin, single entry point for contract register/activate: the
        compare-and-swap itself is owned by ``DeliveryStateStorePort``, which
        holds the authoritative revision counter and is the only thing that can
        make two concurrent activations resolve to exactly one winner. The
        runtime adds no logic here beyond being the one caller-facing seam, so a
        real state store and a fake one are proven against the identical call
        shape."""

        return self.ports.state_store.contract_lifecycle(request)

    def deployment_lifecycle(self, request: DeploymentLifecycleRequest) -> DeploymentLifecycleTransitionResult:
        return self.ports.state_store.deployment_lifecycle(request)


__all__ = [
    "Admission",
    "AppliedProjection",
    "Clock",
    "IngestionRuntime",
    "LEASE_ITEM_LIMIT",
    "PORT_FIELD_ORDER",
    "PortError",
    "admit",
    "admit_resources",
    "canonical_digest",
    "check_implementation_versions",
    "check_plan_and_manifest",
    "check_port_topology",
    "check_readiness",
    "digest_token",
    "parse_utc_instant",
    "scheduled_occurrences",
    "timeliness_state",
    "utc_now_string",
]
