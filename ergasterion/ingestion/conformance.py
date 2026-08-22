"""The packaged Bronze runtime-port conformance seam.

A data-driven harness proving ``IngestionRuntime`` (``ergasterion.ingestion.
runtime``) drives the nine ports correctly across the submission family:
contract-digest and delivery-mode mismatch, idempotent claim replay,
conflicting-claim detection, mixed accept/reject dispositions where the
contract admits partial publication, a rejected delivery where it does not, a
permanent landing failure, an unready published schema, exhausted retries, and
a target failure that leaves the attempt ``commit_blocked`` until ``run_due``
resumes it. Vectors live in ``ergasterion/conformance/adapter-v1.json`` as
plain data (mirroring ``ergasterion.framework.translator_conformance`` and its
``tests/fixtures/translator_conformance.json``): each names a small row set, an
optional fault to inject, and the expected outcome.

This module also carries ``build_memory_ports``: a minimal, dependency-free,
in-memory implementation of all nine port protocols (``MemoryStateStore``,
``FakeRawStore``, ...), built purely to exercise the runtime's own
control-flow -- CAS, replay, retry, two-phase commit, crash resumption -- not
to reproduce a real codec, quality engine or storage backend. A real state
store, landing adapter or raw store proves itself by passing the *same*
``run_adapter_conformance`` entry point with its own ``PortSet``, never by
importing anything from this module.

``run_adapter_conformance`` is the stable public seam entry point: it accepts
implementations explicitly -- the caller hands it the factory that builds the
``PortSet`` under test, real or fake -- rather than resolving one from a
mutable central registry, exactly as
``ergasterion.framework.translator_conformance.check_translator_conformance``
already does for translators. ``exercise_all_operations`` is the second seam:
it calls every operation of all nine ports once and reports which it reached,
so an implementation can prove its whole surface rather than only the paths one
delivery happens to take.

Fault and crash injection lives here, never in the runtime. ``SimulatedCrash``
is raised by a fake at an explicitly named seam and is deliberately not a
``PortError``: the runtime does not catch it, so it propagates like a lost
process, and the recovery path is whatever a fresh ``IngestionRuntime`` over
the same durable fakes can do.
"""

from __future__ import annotations

import base64
import hashlib
import json
from dataclasses import dataclass, field
from pathlib import Path

from ergasterion.ingestion.ports import PortSet
from ergasterion.ingestion.records import (
    Attempt,
    AttemptPage,
    AttemptQuery,
    AttemptState,
    BronzeEvidence,
    BronzeProductContract,
    CandidateField,
    CandidateFrame,
    CandidateFramePage,
    CandidateReadQuery,
    ContractLifecycleRequest,
    ContractLifecycleTransitionResult,
    DeletionEvidenceIntent,
    DeliveryManifest,
    DeliveryVisibilityIdentity,
    Digest,
    DispositionPage,
    DispositionQuery,
    DispositionQueryPage,
    EvidenceKind,
    EvidencePage,
    EvidenceQuery,
    ExternalReceiptInput,
    ExternalReceiptPayload,
    Finding,
    HeartbeatProjectionPayload,
    KeyCommitmentRecord,
    LandingPreparation,
    LifecycleEventBatch,
    LifecycleEventCursor,
    LifecycleEventLogPage,
    LifecycleEventLogQuery,
    LogicalIdentity,
    MacResult,
    ManagedPayloadInput,
    MaterializationCompletion,
    MaterializationSession,
    MaterializedBronzeEvidence,
    OperationalStatus,
    OutboxCompletion,
    OutboxEnqueue,
    OutboxEntry,
    OutboxEntryKind,
    OutboxFailureDisposition,
    OutboxFailureTransaction,
    OutboxPayload,
    OutboxStatus,
    PayloadDescriptor,
    ProcessingOutcome,
    ProjectionConfirmation,
    ProjectionConfirmationLogPage,
    ProjectionIntent,
    ProjectionIntentKind,
    ProjectionLogPage,
    ProjectionOutboxPayload,
    ProjectionReplayBatch,
    RawLocator,
    RawManifestObject,
    RawPayloadObject,
    RawReadHandle,
    RawReadPage,
    RawReceipt,
    RecordKeyTagPage,
    ReleaseMaterializationRequest,
    ReleaseVisibilityBinding,
    ReleaseVisibilityIdentity,
    RemediationDecision,
    RemediationDecisionKind,
    RemediationDecisionPage,
    RemediationDecisionQuery,
    RemediationEvaluation,
    RemediationRelease,
    ScratchChunk,
    ScratchReadPage,
    ScratchScope,
    SignedExternalReceipt,
    SnapshotKeyset,
    SnapshotKeysetCompletion,
    SnapshotKeysetRequest,
    SnapshotReconciliation,
    SnapshotReconciliationRequest,
    SnapshotReconciliationResult,
    SourceNativePage,
    SourceNativeQuery,
    StateOutboxTransaction,
    StreamState,
    Token,
    TombstoneEvidenceRequest,
    TombstoneKeyset,
    TombstoneKeysetCompletion,
    TombstoneKeysetRequest,
    TombstoneTag,
    TombstoneTagPage,
    UnitResult,
    VerificationKeyRecord,
    VisibilityIdentity,
)
from ergasterion.framework.bronze_contract import (
    BackupRestoreCapability,
    CapabilityCodecKind,
    ContentEncoding,
    DeleteStrategy,
    DeliveryInputKind,
    DeliveryMode,
    FingerprintScope,
    FindingMetadata,
    ManagedIntegration,
    LogicalTypeKind,
    PortKind,
    ProfileClass,
    PublicationPolicy,
    ReadinessResult,
    SecretBoundary,
    SnapshotReconciliationStatus,
)
from ergasterion.framework.runtime_binding import (
    AdapterCapabilities,
    CapabilityGuarantees,
    CapabilityLimits,
    DeploymentLifecycleRequest,
    InterfaceReadiness,
    OutboxBinding,
    PortBinding,
    ProjectionCursor,
    ProjectionRelations,
    ProtectionCapabilities,
    RetentionBinding,
    RuntimeBinding,
    RuntimeDeployment,
    RuntimePortBindings,
    RuntimeResources,
    SchedulerBinding,
)
from ergasterion.ingestion.records import PORT_OPERATION_ORDER, DeploymentLifecycleTransitionResult
from ergasterion.ingestion.runtime import (
    PORT_FIELD_ORDER,
    Clock,
    IngestionRuntime,
    PortError,
    _evolve,
    canonical_digest,
    digest_token,
    utc_now_string,
)

VECTORS_PATH = Path(__file__).resolve().parent.parent / "conformance" / "adapter-v1.json"

RELEASED_DECISION = RemediationDecisionKind.RELEASED


class SimulatedCrash(Exception):
    """A fake's stand-in for the process dying at one named seam.

    Not a ``PortError``: the runtime handles domain failures and must not
    handle this one. It escapes every layer, exactly as a lost process would,
    leaving the durable fakes holding whatever was committed before the seam
    and nothing after it. Recovery is then whatever a freshly constructed
    ``IngestionRuntime`` over those same fakes can complete."""

    def __init__(self, seam: str) -> None:
        super().__init__(f"simulated crash at seam {seam!r}")
        self.seam = seam


def _b64url(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _b64url_decode(text: str) -> bytes:
    padding = "=" * (-len(text) % 4)
    return base64.urlsafe_b64decode(text + padding)


# --------------------------------------------------------------------------- fake: SourceConnector

@dataclass
class FakeSourceConnector:
    """Identity pass-through: a managed/external input is already the wire
    shape ``DeliveryInput`` carries, so this fake's whole job is proving the
    seam exists. A real connector is where capability and manifest checks
    against a live source system belong."""

    def submit_managed(self, input: ManagedPayloadInput):
        return input

    def verify_external(self, input: ExternalReceiptInput):
        return input


# --------------------------------------------------------------------------- fake: RawStore

@dataclass
class FakeRawStore:
    content_by_handle: dict[str, list[dict]] = field(default_factory=dict)
    _receipts: dict[Digest, RawReceipt] = field(default_factory=dict)
    _content: dict[Digest, bytes] = field(default_factory=dict)

    def preserve(self, input: ManagedPayloadInput) -> RawReceipt:
        rows = self.content_by_handle[input.payload_handle]
        content_bytes = json.dumps(rows).encode("utf-8")
        content_hash = hashlib.sha256(content_bytes).hexdigest()
        claim_digest = canonical_digest(input.manifest.model_dump(mode="json", by_alias=True))
        raw_receipt_digest = canonical_digest({"content": content_hash, "claim": claim_digest})
        if raw_receipt_digest in self._receipts:
            return self._receipts[raw_receipt_digest]
        receipt = RawReceipt(
            schema="ergasterion.raw-receipt/v1", claim_digest=claim_digest,
            payload=RawPayloadObject(
                content_id=f"sha256:{content_hash}", algorithm="sha256", byte_length=str(len(content_bytes)),
                media_type=input.manifest.payload.media_type, content_encoding=input.manifest.payload.content_encoding,
            ),
            manifest=RawManifestObject(content_id=f"sha256:{content_hash}", algorithm="sha256", byte_length=str(len(content_bytes))),
            raw_receipt_digest=raw_receipt_digest,
        )
        self._receipts[raw_receipt_digest] = receipt
        self._content[raw_receipt_digest] = content_bytes
        return receipt

    def get_receipt(self, raw_receipt_digest: Digest) -> RawReceipt:
        if raw_receipt_digest not in self._receipts:
            raise PortError("not_found", raw_receipt_digest)
        return self._receipts[raw_receipt_digest]

    def open_raw(self, raw_receipt_digest: Digest) -> RawReadHandle:
        receipt = self.get_receipt(raw_receipt_digest)
        return RawReadHandle(
            raw_receipt_digest=raw_receipt_digest, content_id=receipt.payload.content_id,
            byte_length=receipt.payload.byte_length, handle_ref=raw_receipt_digest,
        )

    def read_raw(self, handle: RawReadHandle, offset: str, max_bytes: str) -> RawReadPage:
        content = self._content[handle.raw_receipt_digest]
        start = int(offset)
        chunk = content[start:]
        return RawReadPage(
            handle_ref=handle.handle_ref, offset=offset, bytes_base64url=_b64url(chunk),
            bytes_returned=str(len(chunk)), next_offset=None, eof=True,
        )

    def verify_open(self, input: ExternalReceiptInput) -> RawReceipt:
        payload = input.receipt.payload
        return RawReceipt(
            schema="ergasterion.raw-receipt/v1", claim_digest=payload.delivery_claim_digest,
            payload=RawPayloadObject(
                content_id=payload.raw_digest and f"sha256:{payload.raw_digest}" or "sha256:" + "0" * 64,
                algorithm="sha256", byte_length="0", media_type="application/x-ndjson", content_encoding="identity",
            ),
            manifest=RawManifestObject(content_id="sha256:" + "0" * 64, algorithm="sha256", byte_length="0"),
            raw_receipt_digest=payload.raw_digest,
        )


# --------------------------------------------------------------------------- fake: ScratchStore

@dataclass
class FakeScratchStore:
    _scopes: dict[str, ScratchScope] = field(default_factory=dict)
    _owner: dict[str, Digest] = field(default_factory=dict)
    _closed: set[str] = field(default_factory=set)
    _next_sequence: dict[str, int] = field(default_factory=dict)
    _used_bytes: dict[str, int] = field(default_factory=dict)
    _chunks: dict[str, list[ScratchChunk]] = field(default_factory=dict)

    def create_scope(self, attempt_id: Digest, capacity_bytes: str) -> ScratchScope:
        scope_id = digest_token(canonical_digest({"attempt": attempt_id, "n": len(self._scopes)}), "scope")
        if scope_id in self._scopes:
            raise PortError("scope_conflict", scope_id)
        scope = ScratchScope(scope_id=scope_id, attempt_id=attempt_id, capacity_bytes=capacity_bytes)
        self._scopes[scope_id] = scope
        self._owner[scope_id] = attempt_id
        self._next_sequence[scope_id] = 0
        self._used_bytes[scope_id] = 0
        self._chunks[scope_id] = []
        return scope

    def write_sequential(self, attempt_id: Digest, chunk: ScratchChunk) -> UnitResult:
        scope_id = chunk.scope_id
        if scope_id not in self._scopes:
            raise PortError("scope_owner_mismatch", scope_id)
        if self._owner[scope_id] != attempt_id:
            raise PortError("scope_owner_mismatch", scope_id)
        if scope_id in self._closed:
            raise PortError("scope_closed", scope_id)
        if int(chunk.sequence) != self._next_sequence[scope_id]:
            raise PortError("sequence_conflict", scope_id)
        raw_len = len(_b64url_decode(chunk.bytes_base64url))
        capacity = int(self._scopes[scope_id].capacity_bytes)
        if self._used_bytes[scope_id] + raw_len > capacity:
            raise PortError("capacity_exceeded", scope_id)
        self._used_bytes[scope_id] += raw_len
        self._next_sequence[scope_id] += 1
        self._chunks[scope_id].append(chunk)
        return UnitResult(ok=True)

    def read_sequential(self, attempt_id: Digest, scope_id: Token, after_sequence: str, max_bytes: str) -> ScratchReadPage:
        if scope_id not in self._scopes:
            raise PortError("not_found", scope_id)
        if self._owner[scope_id] != attempt_id:
            raise PortError("scope_owner_mismatch", scope_id)
        if scope_id not in self._closed:
            raise PortError("scope_open", scope_id)
        chunks = [c for c in self._chunks[scope_id] if int(c.sequence) > int(after_sequence)]
        return ScratchReadPage(chunks=tuple(chunks), bytes_returned=str(sum(
            len(_b64url_decode(c.bytes_base64url)) for c in chunks
        )))

    def close_scope(self, attempt_id: Digest, scope_id: Token) -> UnitResult:
        if scope_id not in self._scopes or self._owner[scope_id] != attempt_id:
            raise PortError("scope_owner_mismatch", scope_id)
        self._closed.add(scope_id)
        return UnitResult(ok=True)

    def delete_scope(self, attempt_id: Digest, scope_id: Token) -> UnitResult:
        if scope_id not in self._scopes or self._owner[scope_id] != attempt_id:
            raise PortError("scope_owner_mismatch", scope_id)
        del self._scopes[scope_id]
        return UnitResult(ok=True)

    def cleanup_orphans(self, active_attempt_ids: tuple[Digest, ...], max_scopes: int) -> tuple[Token, ...]:
        orphans = [sid for sid, owner in self._owner.items() if owner not in active_attempt_ids and sid in self._scopes]
        removed = tuple(orphans[:max_scopes])
        for sid in removed:
            del self._scopes[sid]
        return removed


# --------------------------------------------------------------------------- fake: DeliveryStateStore

@dataclass
class MemoryStateStore:
    """The durable half of the reference adapter set: attempts, the outbox and
    its payloads, the projection intent/confirmation logs, the lifecycle event
    log, contract and deployment lifecycles, and the snapshot/tombstone keysets.

    Everything a crash is supposed to survive lives here, so a test simulates a
    lost process by discarding the ``IngestionRuntime`` and building a new one
    over the same instance. ``crash_seams`` names the write seams that raise
    ``SimulatedCrash`` instead of committing; each seam fires once and then
    clears, so the resumption path runs against a store holding exactly the
    state committed before the crash."""

    stream_state: StreamState
    _attempts: dict[Digest, Attempt] = field(default_factory=dict)
    _outbox: dict[Digest, OutboxEntry] = field(default_factory=dict)
    _outbox_payload: dict[tuple[Digest, Digest], OutboxPayload] = field(default_factory=dict)
    _intents: list[ProjectionIntent] = field(default_factory=list)
    _confirmations: list[ProjectionConfirmation] = field(default_factory=list)
    events: list = field(default_factory=list)
    crash_seams: set[str] = field(default_factory=set)
    deployment: RuntimeDeployment | None = None
    deployment_revision: int = 0
    candidate_contract_digest: Digest | None = None
    _snapshot_keysets: dict[Digest, SnapshotKeyset] = field(default_factory=dict)
    _snapshot_tags: dict[Digest, list[Digest]] = field(default_factory=dict)
    _tombstone_keysets: dict[Digest, TombstoneKeyset] = field(default_factory=dict)
    _tombstone_tags: dict[Digest, list[TombstoneTag]] = field(default_factory=dict)

    def _crash(self, seam: str) -> None:
        if seam in self.crash_seams:
            self.crash_seams.discard(seam)
            raise SimulatedCrash(seam)

    def _check_revision(self, expected: str) -> None:
        if expected != self.stream_state.state_revision:
            raise PortError("stale_revision", f"expected {expected}, actual {self.stream_state.state_revision}")

    def state_transaction(self, transaction: StateOutboxTransaction) -> StreamState:
        self._check_revision(transaction.expected_state_revision)
        if transaction.complete:
            self._crash("outbox_completion")
        if any(attempt.remediation_commit_checkpoint is not None for attempt in transaction.attempt_updates):
            self._crash("remediation_checkpoint")
        for attempt in transaction.attempt_updates:
            self._attempts[attempt.attempt_id] = attempt
        for item in transaction.enqueue:
            self._outbox[item.outbox_id] = OutboxEntry(
                outbox_id=item.outbox_id, logical_identity=self.stream_state.logical_identity,
                entry_kind=item.payload.entry_kind, payload_ref=item.outbox_id, payload_digest=item.payload_digest,
                status=OutboxStatus.PENDING, dispatch_ordinal=1, next_not_before=item.next_not_before,
                lease_owner=None, lease_expires_at=None, reason_code=None, completed_at=None,
            )
            self._outbox_payload[(item.outbox_id, item.payload_digest)] = item.payload
            self._intents.append(item.payload.intent)
        for done in transaction.complete:
            entry = self._outbox[done.outbox_id]
            self._outbox[done.outbox_id] = _evolve(entry, status=OutboxStatus.COMPLETE, completed_at=done.completed_at, lease_owner=None)
        if transaction.projection_confirmation is not None:
            self._confirmations.append(transaction.projection_confirmation)
        self.stream_state = transaction.next_state
        return self.stream_state

    def fail_outbox(self, transaction: OutboxFailureTransaction) -> StreamState:
        self._check_revision(transaction.expected_state_revision)
        for attempt in transaction.attempt_updates:
            self._attempts[attempt.attempt_id] = attempt
        entry = self._outbox[transaction.outbox_id]
        next_status = OutboxStatus.DEAD_LETTER if transaction.disposition.value == "dead_letter" else OutboxStatus.RETRYABLE
        # The dispatch ordinal counts delivery attempts at the target, which is
        # what makes retry exhaustion observable rather than inferred.
        self._outbox[transaction.outbox_id] = _evolve(
            entry, status=next_status, next_not_before=transaction.next_not_before or entry.next_not_before,
            reason_code=transaction.reason_code, lease_owner=None, dispatch_ordinal=entry.dispatch_ordinal + 1,
        )
        self.stream_state = transaction.next_state
        return self.stream_state

    def lease_outbox(self, logical_identity: LogicalIdentity, entry_kind: OutboxEntryKind, lease_owner: Token,
                      observed_at: str, max_items: int) -> tuple[OutboxEntry, ...]:
        leased = []
        for outbox_id, entry in self._outbox.items():
            if entry.entry_kind != entry_kind:
                continue
            if entry.status not in (OutboxStatus.PENDING, OutboxStatus.RETRYABLE):
                continue
            if entry.next_not_before > observed_at:
                continue
            entry = _evolve(entry, status=OutboxStatus.LEASED, lease_owner=lease_owner)
            self._outbox[outbox_id] = entry
            leased.append(entry)
            if len(leased) >= max_items:
                break
        return tuple(leased)

    def load_outbox_payload(self, outbox_id: Digest, payload_digest: Digest) -> OutboxPayload:
        key = (outbox_id, payload_digest)
        if key not in self._outbox_payload:
            raise PortError("not_found", str(key))
        return self._outbox_payload[key]

    def attempts(self, query: AttemptQuery) -> AttemptPage:
        """One page of matching attempts in a stable order, honouring
        ``after_attempt_id`` as the cursor the previous page ended on. A store
        that ignored the cursor would answer the same first page forever, so a
        caller paging a stream longer than ``max_items`` would either loop or
        silently stop at the first page -- which is exactly the blindness the
        runtime's replay and conflict rules must not have."""

        items = [a for a in self._attempts.values() if a.logical_identity == query.logical_identity]
        if query.claim_digest is not None:
            items = [a for a in items if a.claim_digest == query.claim_digest]
        if query.nonterminal_only:
            items = [a for a in items if a.state not in (AttemptState.COMMITTED, AttemptState.FAILED)]
        items.sort(key=lambda a: (a.attempt_ordinal, a.attempt_id))
        if query.after_attempt_id is not None:
            start = next(
                (index + 1 for index, a in enumerate(items) if a.attempt_id == query.after_attempt_id), len(items),
            )
            items = items[start:]
        page = items[: query.max_items]
        more = len(items) > len(page)
        return AttemptPage(
            attempts=tuple(page), next_after_attempt_id=page[-1].attempt_id if page and more else None, more=more,
        )

    def status_query(self, logical_identity: LogicalIdentity) -> OperationalStatus:
        mine = [a for a in self._attempts.values() if a.logical_identity == logical_identity]
        mine.sort(key=lambda a: a.attempt_ordinal)
        latest = mine[-1] if mine else None
        pending = sum(1 for e in self._outbox.values() if e.status in (OutboxStatus.PENDING, OutboxStatus.LEASED, OutboxStatus.RETRYABLE))
        processing = ProcessingOutcome.NONE
        if latest is not None:
            processing = {
                AttemptState.COMMITTED: ProcessingOutcome.COMMITTED, AttemptState.FAILED: ProcessingOutcome.FAILED,
                AttemptState.COMMIT_BLOCKED: ProcessingOutcome.BLOCKED,
            }.get(latest.state, ProcessingOutcome.IN_PROGRESS)
        return OperationalStatus(
            state=self.stream_state, latest_attempt=latest, processing=processing,
            block_phase=latest.block_phase if latest else None, incomplete_outbox_count=str(pending),
        )

    def contract_lifecycle(self, request: ContractLifecycleRequest) -> ContractLifecycleTransitionResult:
        """Register records a candidate contract and leaves the active one
        alone; activate promotes exactly the candidate that was registered.
        Both compare-and-swap on the state revision, so of two concurrent
        activations one wins and the other sees ``stale_revision``."""

        self._check_revision(request.expected_state_revision)
        digest = canonical_digest(request.contract.model_dump(mode="json", by_alias=True))
        next_revision = str(int(self.stream_state.state_revision) + 1)
        if request.action.value == "register":
            self.candidate_contract_digest = digest
            next_state = _evolve(self.stream_state, state_revision=next_revision)
        else:
            if self.candidate_contract_digest is None:
                raise PortError("contract_conflict", "no candidate contract is registered to activate")
            if self.candidate_contract_digest != digest:
                raise PortError(
                    "contract_conflict",
                    f"registered candidate is {self.candidate_contract_digest!r}, activation carries {digest!r}",
                )
            self.candidate_contract_digest = None
            next_state = _evolve(self.stream_state, active_contract_digest=digest, state_revision=next_revision)
        self.stream_state = next_state
        return ContractLifecycleTransitionResult(state=next_state, deployment=self.deployment, fenced_attempt_ids=())

    def deployment_lifecycle(self, request: DeploymentLifecycleRequest) -> DeploymentLifecycleTransitionResult:
        """Register records a candidate manifest; activate promotes it and
        retires the manifest it replaced. Both compare-and-swap on the state
        revision *and* the deployment revision."""

        self._check_revision(request.expected_state_revision)
        if int(request.expected_deployment_revision) != self.deployment_revision:
            raise PortError("stale_revision", "deployment revision mismatch")
        if request.readiness.result is not ReadinessResult.READY:
            raise PortError("schema_invalid", f"deployment readiness is {request.readiness.result.value!r}")
        incoming = request.deployment
        if request.action.value == "register":
            self.deployment = _evolve(
                incoming, candidate_manifest_digest=incoming.candidate_manifest_digest,
                active_manifest_digest=self.deployment.active_manifest_digest if self.deployment else None,
                deployment_revision=str(self.deployment_revision + 1),
            )
        else:
            candidate = incoming.candidate_manifest_digest or (
                self.deployment.candidate_manifest_digest if self.deployment else None
            )
            if candidate is None:
                raise PortError("superseded_deployment", "no candidate manifest is registered to activate")
            retired = tuple(self.deployment.retired_manifest_digests) if self.deployment else ()
            previous = self.deployment.active_manifest_digest if self.deployment else None
            self.deployment = _evolve(
                incoming, candidate_manifest_digest=None, active_manifest_digest=candidate,
                retired_manifest_digests=retired + ((previous,) if previous else ()),
                deployment_revision=str(self.deployment_revision + 1),
            )
        self.deployment_revision += 1
        self.stream_state = _evolve(self.stream_state, state_revision=str(int(self.stream_state.state_revision) + 1))
        return DeploymentLifecycleTransitionResult(
            state=self.stream_state, deployment=self.deployment, catchup_cursor=request.catchup_cursor, fenced_attempt_ids=(),
        )

    def projection_log(self, logical_identity, after_revision, max_items, max_bytes) -> ProjectionLogPage:
        items = tuple(
            intent for intent in self._intents
            if int(intent.projection_revision) > int(after_revision)
        )[: max_items]
        return ProjectionLogPage(
            intents=items, next_after_revision=items[-1].projection_revision if items else None,
            bytes_returned="0", more=False,
        )

    def projection_confirmation_log(self, logical_identity, after_revision, max_items, max_bytes) -> ProjectionConfirmationLogPage:
        items = tuple(
            confirmation for confirmation in self._confirmations
            if int(confirmation.projection_revision) > int(after_revision)
        )[: max_items]
        return ProjectionConfirmationLogPage(
            confirmations=items, next_after_revision=items[-1].projection_revision if items else None,
            bytes_returned="0", more=False,
        )

    def lifecycle_event_log(self, query: LifecycleEventLogQuery) -> LifecycleEventLogPage:
        items = tuple(
            event for event in self.events
            if event.logical_identity == query.logical_identity
            and (query.after_cursor is None or int(event.event_ordinal) > int(query.after_cursor.event_ordinal))
        )[: query.max_items]
        cursor = None
        if items:
            last = items[-1]
            cursor = LifecycleEventCursor(
                state_revision=last.state_revision, event_ordinal=last.event_ordinal, event_id=last.event_id,
            )
        return LifecycleEventLogPage(events=items, next_cursor=cursor, bytes_returned="0", more=False)

    # --- snapshot keysets: a growing, ordered set of opaque record-key tags whose
    # completion is checked against the count and digest the caller expected.

    def begin_snapshot_keyset(self, request: SnapshotKeysetRequest) -> SnapshotKeyset:
        keyset_id = canonical_digest({
            "logical_identity": request.logical_identity.model_dump(mode="json"),
            "visibility": request.visibility.model_dump(mode="json"), "kind": "snapshot",
        })
        keyset = SnapshotKeyset(
            keyset_id=keyset_id, logical_identity=request.logical_identity, visibility=request.visibility,
            record_key_scope=request.record_key_scope, hmac_key_id=request.hmac_key_id, key_commitment=request.key_commitment,
            keyset_ref=keyset_id, keyset_digest=None, key_count="0", complete=False,
        )
        self._snapshot_keysets[keyset_id] = keyset
        self._snapshot_tags[keyset_id] = []
        return keyset

    def append_snapshot_keyset(self, attempt_id: Digest, page: RecordKeyTagPage) -> SnapshotKeyset:
        keyset = self._snapshot_keysets.get(page.keyset_id)
        if keyset is None:
            raise PortError("not_found", page.keyset_id)
        if keyset.complete:
            raise PortError("integrity_error", "a complete keyset cannot take more tags")
        tags = self._snapshot_tags[page.keyset_id]
        if int(page.first_frame_sequence) != len(tags):
            raise PortError("sequence_conflict", f"keyset holds {len(tags)} tags, page starts at {page.first_frame_sequence}")
        tags.extend(page.tags)
        keyset = _evolve(keyset, key_count=str(len(tags)))
        self._snapshot_keysets[page.keyset_id] = keyset
        return keyset

    def complete_snapshot_keyset(self, completion: SnapshotKeysetCompletion) -> SnapshotKeyset:
        keyset = self._snapshot_keysets.get(completion.keyset_id)
        if keyset is None:
            raise PortError("not_found", completion.keyset_id)
        tags = self._snapshot_tags[completion.keyset_id]
        if str(len(tags)) != completion.expected_key_count:
            raise PortError(
                "integrity_error",
                f"keyset holds {len(tags)} tags, completion expected {completion.expected_key_count}",
            )
        digest = canonical_digest({"tags": list(tags)})
        if digest != completion.expected_keyset_digest:
            raise PortError("integrity_error", "keyset digest does not match the expected digest")
        keyset = _evolve(keyset, complete=True, keyset_digest=digest)
        self._snapshot_keysets[completion.keyset_id] = keyset
        return keyset

    def get_snapshot_keyset(self, logical_identity: LogicalIdentity, visibility: VisibilityIdentity) -> SnapshotKeyset:
        for keyset in self._snapshot_keysets.values():
            if keyset.logical_identity == logical_identity and keyset.visibility == visibility:
                return keyset
        raise PortError("not_found", "no snapshot keyset for that visibility")

    def reconcile_snapshot(self, request: SnapshotReconciliationRequest) -> SnapshotReconciliationResult:
        """Compare a completed candidate snapshot keyset against the prior one:
        the tags present before and absent now are the deletions the snapshot
        implies, and they become one deletion-evidence intent."""

        candidate = request.candidate_keyset
        if not candidate.complete:
            raise PortError("integrity_error", "an incomplete candidate keyset cannot be reconciled")
        candidate_tags = set(self._snapshot_tags.get(candidate.keyset_id, []))
        prior_tags = set(self._snapshot_tags.get(request.prior_keyset.keyset_id, [])) if request.prior_keyset else set()
        deleted = sorted(prior_tags - candidate_tags)
        deleted_ref = canonical_digest({"deleted": deleted, "attempt": request.attempt_id})
        reconciliation_digest = canonical_digest({
            "candidate": candidate.keyset_id, "prior": request.prior_keyset.keyset_id if request.prior_keyset else None,
            "deleted": deleted,
        })
        intent = DeletionEvidenceIntent(
            logical_identity=candidate.logical_identity, visibility=candidate.visibility,
            delete_strategy=DeleteStrategy.SNAPSHOT_DIFF, claim_digest=request.claim_digest,
            attempt_id=request.attempt_id, event_sequence_low=None, event_sequence_high=None,
            record_key_scope=candidate.record_key_scope, hmac_key_id=candidate.hmac_key_id,
            key_commitment=candidate.key_commitment, deleted_keyset_ref=deleted_ref,
            deleted_keyset_digest=canonical_digest({"tags": deleted}), deleted_key_count=str(len(deleted)),
            reconciliation_digest=reconciliation_digest,
            deletion_evidence_intent_digest=canonical_digest({"reconciliation": reconciliation_digest}),
        )
        reconciliation = SnapshotReconciliation(
            schema="ergasterion.snapshot-reconciliation/v1", logical_identity=candidate.logical_identity,
            attempt_id=request.attempt_id,
            candidate_visibility=DeliveryVisibilityIdentity(
                epoch=candidate.visibility.epoch, kind="delivery", id=digest_token(candidate.keyset_id, "delivery"),
            ),
            prior_visibility=request.prior_keyset.visibility if request.prior_keyset else None,
            prior_keyset_ref=request.prior_keyset.keyset_ref if request.prior_keyset else None,
            candidate_keyset_ref=candidate.keyset_ref, status=SnapshotReconciliationStatus.COMPLETE,
            attempt_count="1", next_attempt_at=None, lease_owner=None, lease_expires_at=None, reason_code=None,
            deletion_evidence=intent, reconciliation_digest=reconciliation_digest,
        )
        return SnapshotReconciliationResult(reconciliation=reconciliation, deletion_evidence=intent)

    # --- tombstone keysets: the explicit-tombstone counterpart, ordered by the
    # source event sequence the tombstones arrived on.

    def begin_tombstone_keyset(self, request: TombstoneKeysetRequest) -> TombstoneKeyset:
        keyset_id = canonical_digest({
            "logical_identity": request.logical_identity.model_dump(mode="json"),
            "visibility": request.visibility.model_dump(mode="json"), "kind": "tombstone",
        })
        keyset = TombstoneKeyset(
            keyset_id=keyset_id, logical_identity=request.logical_identity, visibility=request.visibility,
            record_key_scope=request.record_key_scope, hmac_key_id=request.hmac_key_id,
            key_commitment=request.key_commitment, keyset_ref=keyset_id, keyset_digest=None, key_count="0",
            event_sequence_low=None, event_sequence_high=None, complete=False,
        )
        self._tombstone_keysets[keyset_id] = keyset
        self._tombstone_tags[keyset_id] = []
        return keyset

    def append_tombstone_keyset(self, attempt_id: Digest, page: TombstoneTagPage) -> TombstoneKeyset:
        keyset = self._tombstone_keysets.get(page.keyset_id)
        if keyset is None:
            raise PortError("not_found", page.keyset_id)
        if keyset.complete:
            raise PortError("integrity_error", "a complete keyset cannot take more tags")
        tags = self._tombstone_tags[page.keyset_id]
        for item in page.items:
            if tags and int(item.event_sequence) <= int(tags[-1].event_sequence):
                raise PortError("sequence_conflict", f"event sequence {item.event_sequence} is not ahead of the keyset")
            tags.append(item)
        keyset = _evolve(
            keyset, key_count=str(len(tags)),
            event_sequence_low=tags[0].event_sequence if tags else None,
            event_sequence_high=tags[-1].event_sequence if tags else None,
        )
        self._tombstone_keysets[page.keyset_id] = keyset
        return keyset

    def complete_tombstone_keyset(self, completion: TombstoneKeysetCompletion) -> TombstoneKeyset:
        keyset = self._tombstone_keysets.get(completion.keyset_id)
        if keyset is None:
            raise PortError("not_found", completion.keyset_id)
        tags = self._tombstone_tags[completion.keyset_id]
        if str(len(tags)) != completion.expected_key_count:
            raise PortError(
                "integrity_error",
                f"keyset holds {len(tags)} tags, completion expected {completion.expected_key_count}",
            )
        digest = canonical_digest({"tags": [tag.model_dump(mode="json") for tag in tags]})
        if digest != completion.expected_keyset_digest:
            raise PortError("integrity_error", "keyset digest does not match the expected digest")
        keyset = _evolve(
            keyset, complete=True, keyset_digest=digest,
            event_sequence_low=completion.event_sequence_low, event_sequence_high=completion.event_sequence_high,
        )
        self._tombstone_keysets[completion.keyset_id] = keyset
        return keyset

    def finalize_tombstone_evidence(self, request: TombstoneEvidenceRequest) -> DeletionEvidenceIntent:
        keyset = self._tombstone_keysets.get(request.keyset.keyset_id)
        if keyset is None:
            raise PortError("not_found", request.keyset.keyset_id)
        if not keyset.complete:
            raise PortError("integrity_error", "an incomplete tombstone keyset cannot be finalized")
        return DeletionEvidenceIntent(
            logical_identity=keyset.logical_identity, visibility=keyset.visibility,
            delete_strategy=DeleteStrategy.EXPLICIT_TOMBSTONE, claim_digest=request.claim_digest,
            attempt_id=request.attempt_id, event_sequence_low=keyset.event_sequence_low,
            event_sequence_high=keyset.event_sequence_high, record_key_scope=keyset.record_key_scope,
            hmac_key_id=keyset.hmac_key_id, key_commitment=keyset.key_commitment, deleted_keyset_ref=keyset.keyset_ref,
            deleted_keyset_digest=keyset.keyset_digest or canonical_digest({"tags": []}),
            deleted_key_count=keyset.key_count, reconciliation_digest=None,
            deletion_evidence_intent_digest=canonical_digest({"tombstone_keyset": keyset.keyset_id}),
        )


# --------------------------------------------------------------------------- fake: LandingAdapter

@dataclass
class FakeLandingAdapter:
    """Frames the preserved rows into candidate frames and records dispositions.

    ``finish_prepare_fault`` makes preparation fail permanently with a named
    error code: a pre-intent failure, so the attempt must end ``failed`` with
    no reserved progress and no staged outbox entry.

    ``finish_materialization_fault`` is the same kind of permanent failure one
    state transition later -- it fires after the attempt has already moved to
    ``materializing``. Still pre-intent, so still no reserved progress, but the
    attempt must now be failed against the state it actually reached rather
    than the one landing began from."""

    finish_prepare_fault: str | None = None
    finish_materialization_fault: str | None = None
    _prepared_receipt: dict[Digest, RawReceipt] = field(default_factory=dict)
    _rows: dict[Digest, list[dict]] = field(default_factory=dict)
    _sessions: dict[Digest, MaterializationSession] = field(default_factory=dict)
    _evidence: dict[Digest, BronzeEvidence] = field(default_factory=dict)
    _release_accepted: dict[Digest, MaterializedBronzeEvidence] = field(default_factory=dict)

    def begin_prepare(self, attempt_id: Digest, receipt: RawReceipt, raw: RawReadHandle,
                       contract: BronzeProductContract, visibility: VisibilityIdentity) -> LandingPreparation:
        preparation_id = canonical_digest({"attempt_id": attempt_id, "raw": receipt.raw_receipt_digest})
        self._prepared_receipt[preparation_id] = receipt
        return LandingPreparation(preparation_id=preparation_id, attempt_id=attempt_id,
                                   raw_receipt_digest=receipt.raw_receipt_digest, next_offset="0", closed=False)

    def append_raw(self, preparation: LandingPreparation, page: RawReadPage) -> LandingPreparation:
        rows = json.loads(_b64url_decode(page.bytes_base64url).decode("utf-8"))
        self._rows[preparation.preparation_id] = rows
        return _evolve(preparation, next_offset=page.next_offset or preparation.next_offset, closed=page.eof)

    def finish_prepare(self, preparation: LandingPreparation) -> BronzeEvidence:
        if self.finish_prepare_fault is not None:
            raise PortError(self.finish_prepare_fault, "landing preparation failed permanently")
        receipt = self._prepared_receipt[preparation.preparation_id]
        evidence = BronzeEvidence(
            raw_receipt=receipt, candidate_ref=preparation.preparation_id,
            candidate_digest=canonical_digest({"rows": self._rows.get(preparation.preparation_id, [])}),
            frame_index_ref=preparation.preparation_id, frame_index_digest=canonical_digest({"index": preparation.preparation_id}),
            visibility=DeliveryVisibilityIdentity(epoch="0", kind="delivery", id=digest_token(preparation.attempt_id, "delivery")),
        )
        self._evidence[preparation.preparation_id] = evidence
        return evidence

    def read_candidate(self, query: CandidateReadQuery) -> CandidateFramePage:
        rows = self._rows.get(query.evidence.candidate_ref, [])
        frames = []
        for i, row in enumerate(rows):
            findings = ()
            if not row.get("accept", True):
                findings = (Finding(
                    kind="rule", field_path="/key", code="row_attribution_error", severity="error",
                    metadata=FindingMetadata(
                        diagnostic_code="null_not_allowed", raw_locator=None, expected_logical_type=None,
                        observed_logical_type=None, observed_count=None, expected_min_count=None,
                        expected_max_count=None, duplicate_group_size=None,
                    ),
                ),)
            frames.append(CandidateFrame(
                frame_sequence=str(i), raw_locator=RawLocator(frame_sequence=str(i), byte_offset=None, byte_length=None, line_number=None),
                typed_fields=(CandidateField(name="record_key", value={"logical_type": "utf8_string", "value": row.get("key", "")}),),
                structural_findings=findings,
            ))
        return CandidateFramePage(frames=tuple(frames), next_after_sequence=None, bytes_returned="0", more=False)

    def begin_materialization(self, attempt_id: Digest, evidence: BronzeEvidence, evaluation_id: Digest,
                               ruleset_digest: Digest) -> MaterializationSession:
        session_id = canonical_digest({"attempt": attempt_id, "evaluation": evaluation_id})
        session = MaterializationSession(session_id=session_id, attempt_id=attempt_id, evaluation_id=evaluation_id,
                                          ruleset_digest=ruleset_digest, next_frame_sequence="0", closed=False)
        self._sessions[session_id] = session
        self._evidence[session_id] = evidence
        return session

    def append_dispositions(self, session: MaterializationSession, page: DispositionPage) -> MaterializationSession:
        updated = _evolve(session, next_frame_sequence=page.next_frame_sequence)
        self._sessions[session.session_id] = updated
        return updated

    def finish_materialization(self, completion: MaterializationCompletion) -> MaterializedBronzeEvidence:
        if self.finish_materialization_fault is not None:
            raise PortError(self.finish_materialization_fault, "landing materialization failed permanently")
        prepared = self._evidence[completion.session.session_id]
        accepted = int(completion.validation.accepted_count)
        return MaterializedBronzeEvidence(
            prepared=prepared, disposition_ref=completion.session.session_id,
            accepted_ref=f"{completion.session.session_id}-accepted",
            accepted_content_digest=canonical_digest({"session": completion.session.session_id, "accepted": accepted}),
            candidate_keyset=None, published_visibility=None,
        )

    def bind_release_visibility(self, binding: ReleaseVisibilityBinding) -> MaterializedBronzeEvidence:
        return _evolve(binding.materialized, published_visibility=binding.visibility)

    def materialize_release(self, request: ReleaseMaterializationRequest) -> MaterializedBronzeEvidence:
        cached = self._release_accepted.get(request.release_id)
        if cached is not None:
            return cached
        prepared = self._evidence[request.raw_ref]
        rows = self._rows.get(request.raw_ref, [])
        accepted_ref = f"release-{request.release_id}-accepted"
        materialized = MaterializedBronzeEvidence(
            prepared=prepared, disposition_ref=f"release-{request.release_id}", accepted_ref=accepted_ref,
            accepted_content_digest=canonical_digest({
                "release": request.release_id,
                "keys": [rows[int(locator.frame_sequence)].get("key", "") for locator in request.selected_locators
                         if int(locator.frame_sequence) < len(rows)],
            }),
            candidate_keyset=None, published_visibility=request.visibility,
        )
        self._release_accepted[request.release_id] = materialized
        return materialized

    def source_native_query(self, query: SourceNativeQuery) -> SourceNativePage:
        return SourceNativePage(items=(), next_frame_sequence=None, bytes_returned="0", more=False)

    def disposition_query(self, query: DispositionQuery) -> DispositionQueryPage:
        return DispositionQueryPage(items=(), snapshot_token="snapshot-0", next_cursor=None, bytes_returned="0", more=False)

    def verify_open(self, input: ExternalReceiptInput, visibility: DeliveryVisibilityIdentity) -> BronzeEvidence:
        payload = input.receipt.payload
        receipt = RawReceipt(
            schema="ergasterion.raw-receipt/v1", claim_digest=payload.delivery_claim_digest,
            payload=RawPayloadObject(content_id=f"sha256:{payload.raw_digest}", algorithm="sha256", byte_length="0",
                                      media_type="application/x-ndjson", content_encoding="identity"),
            manifest=RawManifestObject(content_id=f"sha256:{payload.manifest_digest}", algorithm="sha256", byte_length="0"),
            raw_receipt_digest=payload.raw_digest,
        )
        return BronzeEvidence(
            raw_receipt=receipt, candidate_ref=payload.candidate_ref, candidate_digest=payload.candidate_digest,
            frame_index_ref=payload.frame_index_ref, frame_index_digest=payload.frame_index_digest, visibility=visibility,
        )


# --------------------------------------------------------------------------- fake: RemediationRepository

@dataclass
class FakeRemediationRepository:
    """Records quarantine evaluations and releases, with a compare-and-swap on
    the evaluation identifier: one evaluation releases exactly once, and a
    second release of it is refused.

    ``crash_seams`` names two seams around that compare-and-swap.
    ``remediation_cas_before`` dies with nothing recorded, so retrying the
    release is the correct recovery. ``remediation_cas_after`` records the
    decision and then dies, so retrying would be a second release and the
    correct recovery is ``IngestionRuntime.resume_release``, which reads the
    recorded decision back instead of deciding again."""

    crash_seams: set[str] = field(default_factory=set)
    _decisions: dict[Digest, RemediationDecision] = field(default_factory=dict)
    _released_evaluations: set[Digest] = field(default_factory=set)

    def _crash(self, seam: str) -> None:
        if seam in self.crash_seams:
            self.crash_seams.discard(seam)
            raise SimulatedCrash(seam)

    def record_decision(self, decision: RemediationDecision) -> RemediationDecision:
        if decision.kind == RemediationDecisionKind.RELEASED:
            self._crash("remediation_cas_before")
            if decision.evaluation.remediation_evaluation_id in self._released_evaluations:
                raise PortError("release_conflict", decision.evaluation.remediation_evaluation_id)
            self._released_evaluations.add(decision.evaluation.remediation_evaluation_id)
            self._decisions[decision.decision_id] = decision
            self._crash("remediation_cas_after")
            return decision
        self._decisions[decision.decision_id] = decision
        return decision

    def decision_query(self, query: RemediationDecisionQuery) -> RemediationDecisionPage:
        items = tuple(
            decision for decision in self._decisions.values()
            if query.disposition_id is None or query.disposition_id in decision.disposition_ids
        )[: query.max_items]
        return RemediationDecisionPage(items=items, snapshot_token="snapshot-0", next_cursor=None,
                                        bytes_returned="0", more=False)


# --------------------------------------------------------------------------- fake: ProjectionPublisher

@dataclass
class FakeProjectionPublisher:
    """A projection target with an explicit, deterministic fault schedule: the
    first ``fail_first_n`` calls to ``apply_gap_ordered`` raise
    ``target_unavailable`` (simulating the target being unreachable); every
    call after that -- including a retry of the very same intent -- applies
    normally. A repeat of an *already-confirmed* intent digest is answered
    from the cache rather than re-applied, proving idempotent replay at the
    port boundary independent of the runtime's own attempt-level replay
    check."""

    cursor_revision: int = 0
    fail_first_n: int = 0
    logical_identity: LogicalIdentity | None = None
    _confirmations: dict[Digest, ProjectionConfirmation] = field(default_factory=dict)
    _calls: int = 0

    def apply_gap_ordered(self, intent: ProjectionIntent) -> ProjectionConfirmation:
        if intent.projection_intent_digest in self._confirmations:
            return self._confirmations[intent.projection_intent_digest]
        if self._calls < self.fail_first_n:
            self._calls += 1
            raise PortError("target_unavailable", intent.projection_intent_digest)
        self._calls += 1
        expected = str(self.cursor_revision + 1)
        if intent.projection_revision != expected:
            raise PortError("projection_gap", f"expected {expected}, got {intent.projection_revision}")
        self.cursor_revision += 1
        visibility = intent.payload.visibility if hasattr(intent.payload, "visibility") else None
        confirmation = ProjectionConfirmation(
            schema="ergasterion.projection-confirmation/v1", logical_identity=intent.logical_identity,
            contract_digest=intent.contract_digest, projection_target=intent.projection_target, kind=intent.kind,
            projection_intent_digest=intent.projection_intent_digest, projection_revision=intent.projection_revision,
            target_applied_at="2026-01-01T00:00:00.000000Z", committed_at="2026-01-01T00:00:00.000000Z",
            release_applied_at=None, timeliness=None, processing=ProcessingOutcome.COMMITTED, visibility=visibility,
            ledger_ref=None, deletion_evidence=None,
            target_result_digest=canonical_digest({"intent": intent.projection_intent_digest}),
        )
        self._confirmations[intent.projection_intent_digest] = confirmation
        return confirmation

    def read_cursor(self, logical_identity: LogicalIdentity, projection_target: Token) -> ProjectionCursor:
        return ProjectionCursor(logical_identity=logical_identity, projection_target=projection_target,
                                 projection_revision=str(self.cursor_revision), projection_intent_digest=None)

    def rebuild_read_models(self, batch: ProjectionReplayBatch) -> ProjectionCursor:
        if len(batch.intents) != len(batch.confirmations):
            raise PortError("unconfirmed_revision", "intents and confirmations count mismatch")
        revision = max((int(i.projection_revision) for i in batch.intents), default=self.cursor_revision)
        self.cursor_revision = revision
        identity = batch.intents[0].logical_identity if batch.intents else self.logical_identity
        return ProjectionCursor(logical_identity=identity, projection_target="bronze",
                                 projection_revision=str(revision), projection_intent_digest=None)


# --------------------------------------------------------------------------- fake: LifecycleSink

@dataclass
class FakeLifecycleSink:
    """Projects lifecycle envelopes in order and refuses a broken order.

    Two rules, both enforced here rather than asserted afterwards, so every
    vector proves them: an event identifier may not be reused for a different
    payload, and each stream's ``event_ordinal`` sequence must be gap-free --
    the next envelope carries the ordinal that follows the last one, or repeats
    it when a single state revision emits more than one envelope. A skipped
    ordinal means a lifecycle envelope was lost, which is exactly what this
    port exists to make impossible."""

    events: list = field(default_factory=list)
    _by_id: dict[Digest, "object"] = field(default_factory=dict)
    _last_ordinal: dict[str, int] = field(default_factory=dict)

    def project_events(self, batch: LifecycleEventBatch) -> tuple[Digest, ...]:
        ids = []
        for event in batch.events:
            existing = self._by_id.get(event.event_id)
            if existing is not None:
                if existing.payload_digest != event.payload_digest:
                    raise PortError("event_conflict", event.event_id)
                ids.append(event.event_id)
                continue
            stream = canonical_digest(event.logical_identity.model_dump(mode="json"))
            ordinal = int(event.event_ordinal)
            previous = self._last_ordinal.get(stream)
            if previous is not None and ordinal not in (previous, previous + 1):
                raise PortError(
                    "event_conflict",
                    f"event ordinal {ordinal} does not follow {previous}: a lifecycle envelope is missing",
                )
            self._last_ordinal[stream] = ordinal
            self._by_id[event.event_id] = event
            self.events.append(event)
            ids.append(event.event_id)
        return tuple(ids)

    def evidence_query(self, query: EvidenceQuery) -> EvidencePage:
        return EvidencePage(items=(), next_cursor=None, bytes_returned="0", more=False)


# --------------------------------------------------------------------------- fake: KeyResolver

@dataclass
class FakeKeyResolver:
    keys: dict[Token, VerificationKeyRecord] = field(default_factory=dict)
    commitments: dict[Token, KeyCommitmentRecord] = field(default_factory=dict)
    secret: bytes = b"conformance-only-fixed-test-secret"

    def resolve_verification_key(self, key_id: Token) -> VerificationKeyRecord:
        if key_id not in self.keys:
            raise PortError("key_not_found", key_id)
        record = self.keys[key_id]
        if record.revoked_at is not None:
            raise PortError("key_revoked", key_id)
        return record

    def key_commitment(self, key_id: Token) -> KeyCommitmentRecord:
        if key_id not in self.commitments:
            raise PortError("key_not_found", key_id)
        return self.commitments[key_id]

    def mac(self, key_id: Token, domain: str, message_base64url: str) -> MacResult:
        import hmac as _hmac

        if key_id not in self.keys and key_id not in self.commitments:
            raise PortError("key_not_found", key_id)
        digest = _hmac.new(self.secret, f"{domain}:{message_base64url}".encode("utf-8"), hashlib.sha256).hexdigest()
        return MacResult(algorithm="HMAC-SHA-256", key_id=key_id, tag_hex=digest)


# --------------------------------------------------------------------------- assembly

REFERENCE_KEY_ID: Token = "reference-hmac-key"
"""The key identifier the reference key resolver always holds."""


def reference_key_resolver(key_ids: tuple[Token, ...] = ()) -> FakeKeyResolver:
    """A key resolver holding one enabled verification key and one commitment
    for ``REFERENCE_KEY_ID`` plus every identifier a caller names, so the
    resolver's three operations answer rather than fail for a known key. An
    unknown identifier still fails ``key_not_found``, which is the behaviour a
    signature check depends on."""

    resolver = FakeKeyResolver()
    for key_id in (REFERENCE_KEY_ID, *key_ids):
        fingerprint = canonical_digest({"key_id": key_id})
        resolver.keys[key_id] = VerificationKeyRecord(
            key_id=key_id, algorithm="Ed25519", public_key_base64url=_b64url(bytes.fromhex(fingerprint)),
            public_key_fingerprint=fingerprint, enabled_at="2026-01-01T00:00:00.000000Z",
            authorized_policy_refs=("policy.reference",),
            trust_record_digest=canonical_digest({"trust": key_id}),
        )
        resolver.commitments[key_id] = KeyCommitmentRecord(
            key_id=key_id, algorithm="HMAC-SHA-256", commitment=canonical_digest({"commitment": key_id}),
        )
    return resolver


def build_memory_ports(
    logical_identity: LogicalIdentity, content_by_handle: dict[str, list[dict]] | None = None,
    fail_first_n: int = 0, crash_seams: frozenset[str] = frozenset(), finish_prepare_fault: str | None = None,
    finish_materialization_fault: str | None = None, key_ids: tuple[Token, ...] = (),
) -> tuple[PortSet, StreamState]:
    """Build one fresh, self-consistent in-memory ``PortSet`` for one logical
    identity, plus the ``StreamState`` a caller starts from (``state_revision
    "0"``, no active contract). Every fake below is independent -- no shared
    module-level state -- so two calls never interfere, exactly what a fresh
    conformance vector or test needs.

    ``fail_first_n`` seeds the projection publisher's deterministic fault
    schedule (see ``FakeProjectionPublisher``); ``finish_prepare_fault`` and
    ``finish_materialization_fault`` name a permanent landing failure before
    and after the ``materializing`` transition; ``crash_seams`` names the write seams that raise
    ``SimulatedCrash`` once instead of committing. The state store and the
    lifecycle sink share one event list, so the lifecycle log a caller reads
    back through the state store is the same ordered sequence the sink
    accepted."""

    stream_state = StreamState(
        logical_identity=logical_identity, active_contract_digest=None, visibility_epoch="0",
        accepted_progress={},
        snapshot_reconciliation=None, state_revision="0", required_projection_revision="0",
    )
    events: list = []
    ports = PortSet(
        source_connector=FakeSourceConnector(),
        raw_store=FakeRawStore(content_by_handle=content_by_handle or {}),
        scratch_store=FakeScratchStore(),
        state_store=MemoryStateStore(stream_state=stream_state, events=events, crash_seams=set(crash_seams)),
        landing_adapter=FakeLandingAdapter(
            finish_prepare_fault=finish_prepare_fault, finish_materialization_fault=finish_materialization_fault,
        ),
        remediation_repository=FakeRemediationRepository(crash_seams=set(crash_seams)),
        projection_publisher=FakeProjectionPublisher(fail_first_n=fail_first_n, logical_identity=logical_identity),
        lifecycle_sink=FakeLifecycleSink(events=events),
        key_resolver=reference_key_resolver(key_ids),
    )
    return ports, stream_state


# --------------------------------------------------------------------------- declared envelope

def build_adapter_capabilities(
    field_name: str, max_memory_bytes: str = "268435456", max_scratch_bytes: str = "134217728",
) -> AdapterCapabilities:
    """The capability envelope one reference port declares: its own port kind,
    exactly the operations ``PORT_OPERATION_ORDER`` lists for that port, and the
    ceilings admission checks the aggregate resource formula against."""

    return AdapterCapabilities(
        schema="ergasterion.adapter-capabilities/v1", port_kind=PortKind(field_name),
        operations=PORT_OPERATION_ORDER[field_name],
        input_kinds=(DeliveryInputKind.MANAGED_PAYLOAD, DeliveryInputKind.EXTERNAL_RECEIPT),
        delivery_modes=(DeliveryMode.CDC, DeliveryMode.APPEND_ONLY, DeliveryMode.COMPLETE_SNAPSHOT),
        codecs=(CapabilityCodecKind.CSV_V1, CapabilityCodecKind.JSONL_V1),
        content_encodings=(ContentEncoding.IDENTITY, ContentEncoding.GZIP),
        logical_types=tuple(LogicalTypeKind),
        guarantees=CapabilityGuarantees(
            immutable_write=True, compare_and_swap=True, atomic_projection=True, gap_free_revision=True,
            idempotent_replay=True, bounded_streaming=True,
        ),
        limits=CapabilityLimits(
            max_payload_bytes="16777216", max_uncompressed_bytes="67108864", max_expansion_ratio="10",
            max_batch_records="100000", max_memory_bytes=max_memory_bytes, max_scratch_bytes=max_scratch_bytes,
        ),
        protection=ProtectionCapabilities(
            profile_class=ProfileClass.SYNTHETIC_LOCAL_ONLY, encryption_at_rest=False, transport_encryption=False,
            access_policy_binding=True, audit_evidence=True, retention_enforcement=True,
            backup_restore=BackupRestoreCapability.VERIFIED, secret_boundary=SecretBoundary.OPAQUE_MAC,
        ),
    )


def build_capabilities(
    max_memory_bytes: str = "268435456", max_scratch_bytes: str = "134217728",
) -> dict[str, AdapterCapabilities]:
    """One capability record per port, keyed by port slot name."""

    return {
        field_name: build_adapter_capabilities(field_name, max_memory_bytes, max_scratch_bytes)
        for field_name in PORT_FIELD_ORDER
    }


def build_runtime_binding(
    contract: BronzeProductContract, capabilities: dict[str, AdapterCapabilities], execution_plan_digest: Digest,
    implementation_version: str = "1.0.0", max_parallel_attempts: int = 2,
    validation_memory_bytes: str = "67108864", scratch_reservation_bytes: str = "33554432",
    process_memory_bytes: str = "268435456",
) -> RuntimeBinding:
    """The reference deployment binding: all nine ports bound to the reference
    adapters, and the operating envelope whose aggregate memory and scratch
    admission checks."""

    relation_names = tuple(ProjectionRelations.model_fields)
    relations = ProjectionRelations(
        schema_ref="bronze",
        **{name: f"bronze.{name}" for name in relation_names if name not in ("database_ref", "schema_ref")},
    )
    return RuntimeBinding(
        schema="ergasterion.runtime-binding/v1", binding_id="reference-local", binding_version="1.0.0",
        environment="local", logical_identity=contract.logical_identity,
        contract_digest=canonical_digest(contract.model_dump(mode="json", by_alias=True)),
        execution_plan_digest=execution_plan_digest, projection_target="bronze",
        ports=RuntimePortBindings(**{
            field_name: PortBinding(
                adapter_id=f"reference-{field_name.replace('_', '-')}", implementation_version=implementation_version,
                capability_digest=canonical_digest(capabilities[field_name].model_dump(mode="json", by_alias=True)),
                endpoint_ref=f"memory://{field_name}", secret_resolver_refs=(),
            )
            for field_name in PORT_FIELD_ORDER
        }),
        landing_ports={}, projection_relations=relations,
        scheduler=SchedulerBinding(heartbeat_seconds=60, heartbeat_slo_seconds=300, max_due_transitions_per_call=16),
        outbox=OutboxBinding(
            max_attempts=int(contract.delivery.retry.max_attempts), lease_seconds=60, backoff="exponential",
            base_seconds=5, cap_seconds=60,
        ),
        runtime_resources=RuntimeResources(
            process_memory_bytes=process_memory_bytes, validation_memory_bytes=validation_memory_bytes,
            scratch_reservation_bytes=scratch_reservation_bytes, max_parallel_attempts=max_parallel_attempts,
            max_wire_record_bytes="1048576", max_quarantine_disposition_bytes="262144",
            max_quarantine_decision_bytes="262144", max_remediation_locators=1000, max_visibility_ancestry_rows=1000,
        ),
        retention=RetentionBinding(orphan_content_hours=24, deletion_keyset_days=30),
        protection_profile="synthetic_local_only",
    )


def build_readiness(
    contract: BronzeProductContract, runtime_manifest_digest: Digest,
    result: ReadinessResult = ReadinessResult.READY, revoked_at: str | None = None,
) -> InterfaceReadiness:
    """The readiness record publication checks: verified against exactly this
    contract and manifest, ready unless a caller asks for a rejected or revoked
    one."""

    digest = canonical_digest(contract.model_dump(mode="json", by_alias=True))
    return InterfaceReadiness(
        schema="ergasterion.interface-readiness/v1", logical_identity=contract.logical_identity,
        projection_target="bronze", runtime_manifest_digest=runtime_manifest_digest, contract_digest=digest,
        source_schema_digest=digest, published_schema_digest=digest, version_interface_ref="bronze.v1",
        capability_digest=digest, classification=contract.product.classification,
        access_policy_ref=contract.product.access_policy_ref, retention_policy_ref=contract.product.retention_policy_ref,
        protection_profile="synthetic_local_only", result=result, readiness_digest=digest,
        verified_at="2026-01-01T00:00:00.000000Z", revoked_at=revoked_at,
    )


def build_deployment(
    contract: BronzeProductContract, runtime_manifest_digest: Digest, candidate_manifest_digest: Digest | None = None,
) -> RuntimeDeployment:
    return RuntimeDeployment(
        logical_identity=contract.logical_identity,
        contract_digest=canonical_digest(contract.model_dump(mode="json", by_alias=True)),
        projection_target="bronze", candidate_manifest_digest=candidate_manifest_digest,
        active_manifest_digest=runtime_manifest_digest, retired_manifest_digests=(), deployment_revision="0",
    )


def contract_variant(
    contract: BronzeProductContract, integration_kind: str | None = None,
    publication_mode: PublicationPolicy | None = None, delivery_mode: DeliveryMode | None = None,
) -> BronzeProductContract:
    """A copy of ``contract`` carrying the delivery-shape switches vectors and
    tests select between: managed or external integration, whether the quality
    policy admits partial publication, and the delivery mode. Everything else --
    schedule, progress kind, retry policy, record key -- is the contract's own,
    so a variant remains one real contract rather than a hand-built stand-in."""

    landing = contract.landing
    if integration_kind is not None and landing.integration.kind != integration_kind:
        if integration_kind != "managed":
            raise PortError("invalid_config", f"no reference integration shape for kind {integration_kind!r}")
        landing = _evolve(landing, integration=ManagedIntegration(kind="managed"))
    delivery = contract.delivery
    if publication_mode is not None and delivery.quality.publication_mode is not publication_mode:
        delivery = _evolve(delivery, quality=_evolve(delivery.quality, publication_mode=publication_mode))
    if delivery_mode is not None and delivery.mode is not delivery_mode:
        delivery = _evolve(delivery, mode=delivery_mode)
    return _evolve(contract, landing=landing, delivery=delivery)


def fixed_clock(instant_dt=None) -> Clock:
    """A ``Clock`` that always answers the same instant unless a caller passes
    its own ``datetime``. Every conformance vector and most direct tests use
    this rather than the wall clock, so a run is byte-reproducible."""

    from datetime import datetime

    dt = instant_dt or datetime(2026, 1, 1)
    return Clock(now_fn=lambda: dt)


# --------------------------------------------------------------------------- whole-surface exercise

class _RecordingPort:
    """Delegates every attribute to one wrapped port and records the name of
    each declared operation actually called on it. Purely structural, so it
    stands in for any port implementation without knowing what it is."""

    def __init__(self, port: object, operations: tuple[str, ...], calls: list[str]) -> None:
        self._port = port
        self._operations = frozenset(operations)
        self._calls = calls

    def __getattr__(self, name: str):
        if name.startswith("_"):
            raise AttributeError(name)
        target = getattr(self._port, name)
        if name not in self._operations or not callable(target):
            return target

        def recorded(*args, **kwargs):
            result = target(*args, **kwargs)
            self._calls.append(name)
            return result

        return recorded


def record_port_calls(ports: PortSet) -> tuple[PortSet, dict[str, list[str]]]:
    """Wrap a ``PortSet`` so every declared operation call is recorded, and
    return the wrapper plus the live per-port call log. The wrapper is a
    drop-in for the original: ``IngestionRuntime`` cannot tell the difference,
    which is what makes the recorded coverage evidence about the runtime's real
    behaviour rather than about a separate replica of it."""

    calls: dict[str, list[str]] = {name: [] for name in PORT_FIELD_ORDER}
    wrapped = PortSet(**{
        name: _RecordingPort(getattr(ports, name), PORT_OPERATION_ORDER[name], calls[name])
        for name in PORT_FIELD_ORDER
    })
    return wrapped, calls


def exercise_all_operations(
    ports: PortSet, stream_state: StreamState, contract: BronzeProductContract, payload_handle: Token,
    clock: Clock | None = None, key_id: Token = REFERENCE_KEY_ID,
) -> dict[str, tuple[str, ...]]:
    """Call every operation of all nine ports at least once and report, per
    port, which were reached.

    The delivery operations are driven through ``IngestionRuntime`` itself, so
    the coverage is evidence about the runtime's real call sequence; the
    remaining operations -- the queries, the scratch scope lifecycle, the
    snapshot and tombstone keysets, the replay rebuild, the key resolver --
    are called directly here, in an order each one's preconditions allow. A
    caller compares the returned mapping against ``PORT_OPERATION_ORDER`` to
    prove an implementation answers its whole declared surface, not only the
    subset one delivery happens to touch.

    ``ports`` must be a set whose ``raw_store`` already holds ``payload_handle``
    and whose ``key_resolver`` already holds ``key_id``; nothing else is
    assumed about the implementations."""

    clock = clock or fixed_clock()
    recorded, calls = record_port_calls(ports)
    runtime = IngestionRuntime(recorded, clock)
    store = recorded.state_store
    identity = contract.logical_identity
    now = clock.now()
    contract_digest = canonical_digest(contract.model_dump(mode="json", by_alias=True))
    plan_digest = canonical_digest({"plan": "exercise"})
    manifest_digest = canonical_digest({"manifest": "exercise"})
    readiness = build_readiness(contract, manifest_digest)

    # --- contract and deployment lifecycle: register a candidate, then activate it.
    state = stream_state
    for action in ("register", "activate"):
        state = store.contract_lifecycle(ContractLifecycleRequest(
            schema="ergasterion.contract-lifecycle-request/v1", action=action,
            expected_state_revision=state.state_revision, expected_deployment_revision=None,
            contract=contract, migration=None, permit_pre_intent_fence=False,
        )).state
    cursor = recorded.projection_publisher.read_cursor(identity, "bronze")
    deployment_revision = "0"
    for action in ("register", "activate"):
        transition = store.deployment_lifecycle(DeploymentLifecycleRequest(
            schema="ergasterion.deployment-lifecycle-request/v1", action=action,
            expected_state_revision=state.state_revision, expected_deployment_revision=deployment_revision,
            deployment=build_deployment(contract, manifest_digest, candidate_manifest_digest=manifest_digest),
            readiness=readiness, catchup_cursor=cursor, permit_pre_intent_fence=False,
        ))
        state = transition.state
        deployment_revision = transition.deployment.deployment_revision

    # --- one whole delivery, driven by the runtime across seven of the nine ports.
    progress_claim = (
        {"kind": "opaque_batch"} if contract.delivery.progress.kind == "opaque_batch"
        else {"kind": "sequence", "high_watermark": "1", "event_count": "1"}
    )
    manifest = DeliveryManifest(
        schema="ergasterion.delivery-manifest/v1", logical_identity=identity,
        product_version=contract.product.product_version, contract_digest=contract_digest,
        delivery_id="exercise", batch_id=None, scheduled_boundary_at=None, effective_boundary_at=None,
        payload=PayloadDescriptor(media_type="application/x-ndjson", content_encoding="identity",
                                   codec_version=1, byte_length="2", sha256="0" * 64),
        frame_sequence_digest=None, progress_claim=progress_claim, declared_row_count="1",
        snapshot_attestation=None,
    )
    input_record = ManagedPayloadInput(kind="managed_payload", manifest=manifest, payload_handle=payload_handle)
    attempt, state = runtime.submit_managed(
        state, contract, plan_digest, manifest_digest, canonical_digest({"run": "exercise"}), input_record,
    )
    visibility = DeliveryVisibilityIdentity(epoch="0", kind="delivery", id=digest_token(attempt.attempt_id, "delivery"))
    receipt = recorded.raw_store.preserve(input_record)
    attempt, state, materialized, validation = runtime.land_and_validate(
        attempt, state, contract, receipt, visibility,
        canonical_digest({"evaluation": "exercise"}), canonical_digest({"ruleset": "exercise"}),
    )
    runtime.publish(attempt, state, contract, materialized, validation, visibility, receipt, readiness)
    state = store.status_query(identity).state

    # --- raw store reads and the remaining landing-adapter surface.
    handle = recorded.raw_store.open_raw(receipt.raw_receipt_digest)
    recorded.raw_store.get_receipt(receipt.raw_receipt_digest)
    recorded.raw_store.read_raw(handle, "0", handle.byte_length)
    external = ExternalReceiptInput(kind="external_receipt", receipt=SignedExternalReceipt(
        schema="ergasterion.external-receipt/v1", algorithm="Ed25519", key_id=key_id,
        payload=ExternalReceiptPayload(
            logical_identity=identity, contract_digest=contract_digest, claim=manifest,
            delivery_claim_digest=canonical_digest(manifest.model_dump(mode="json", by_alias=True)),
            visibility=visibility, adapter_capability_digest=canonical_digest({"capability": "exercise"}),
            raw_ref=materialized.prepared.candidate_ref, raw_digest=receipt.raw_receipt_digest,
            manifest_ref=materialized.prepared.candidate_ref, manifest_digest=receipt.manifest.content_id.split(":", 1)[-1],
            candidate_ref=materialized.prepared.candidate_ref, candidate_digest=materialized.prepared.candidate_digest,
            frame_index_ref=materialized.prepared.frame_index_ref,
            frame_index_digest=materialized.prepared.frame_index_digest, issued_at=now,
        ),
        signature=_b64url(b"exercise-signature"),
    ))
    recorded.source_connector.verify_external(external)
    recorded.raw_store.verify_open(external)
    recorded.landing_adapter.verify_open(external, visibility)
    recorded.landing_adapter.bind_release_visibility(ReleaseVisibilityBinding(
        materialized=materialized,
        visibility=ReleaseVisibilityIdentity(epoch="0", kind="release", id=canonical_digest({"release": "exercise"})),
    ))
    recorded.landing_adapter.source_native_query(SourceNativeQuery(
        logical_identity=identity, candidate_ref=materialized.prepared.candidate_ref, disposition_ref=None,
        authorization_context_ref="operator", after_frame_sequence=None, max_items=16, max_bytes="1000000",
    ))
    recorded.landing_adapter.disposition_query(DispositionQuery(
        logical_identity=identity, disposition_id=None, authorization_context_ref="operator", snapshot_token=None,
        after_cursor=None, max_items=16, max_bytes="1000000",
    ))

    # --- scratch scopes: one scope's whole lifecycle, then orphan cleanup.
    scope = recorded.scratch_store.create_scope(attempt.attempt_id, "4096")
    for sequence, payload in enumerate((b"first-chunk", b"second-chunk")):
        recorded.scratch_store.write_sequential(attempt.attempt_id, ScratchChunk(
            scope_id=scope.scope_id, sequence=str(sequence), bytes_base64url=_b64url(payload),
        ))
    recorded.scratch_store.close_scope(attempt.attempt_id, scope.scope_id)
    recorded.scratch_store.read_sequential(attempt.attempt_id, scope.scope_id, "0", "4096")
    recorded.scratch_store.delete_scope(attempt.attempt_id, scope.scope_id)
    recorded.scratch_store.create_scope(attempt.attempt_id, "4096")
    recorded.scratch_store.cleanup_orphans((), 4)

    # --- the read side of the state store and the lifecycle sink.
    store.attempts(AttemptQuery(logical_identity=identity, claim_digest=None, nonterminal_only=False,
                                after_attempt_id=None, max_items=16))
    store.projection_log(identity, "0", 16, "1000000")
    store.projection_confirmation_log(identity, "0", 16, "1000000")
    store.lifecycle_event_log(LifecycleEventLogQuery(
        logical_identity=identity, after_cursor=None, max_items=16, max_bytes="1000000",
    ))
    recorded.lifecycle_sink.evidence_query(EvidenceQuery(
        logical_identity=identity, evidence_kind=EvidenceKind.ATTEMPT, immutable_id=None,
        authorization_context_ref="operator", after_cursor=None, max_items=16, max_bytes="1000000",
    ))
    evaluation = RemediationEvaluation(
        schema="ergasterion.remediation-evaluation/v1", original_claim_digest=attempt.claim_digest,
        raw_receipt_digest=receipt.raw_receipt_digest, target_contract_digest=contract_digest,
        target_source_schema_digest=contract_digest, target_published_schema_digest=contract_digest,
        target_ruleset_digest=validation.ruleset_digest, execution_plan_digest=plan_digest,
        root_visibility_epoch=visibility.epoch,
        remediation_evaluation_id=canonical_digest({"evaluation": "exercise"}),
    )
    recorded.remediation_repository.record_decision(RemediationDecision(
        schema="ergasterion.remediation-decision/v1", decision_id=canonical_digest({"decision": "exercise"}),
        kind=RemediationDecisionKind.EVALUATED, evaluation=evaluation,
        disposition_ids=(canonical_digest({"disposition": "exercise"}),),
        validation_result_digest=validation.validation_result_digest, release=None, decided_at=now,
    ))
    recorded.remediation_repository.decision_query(RemediationDecisionQuery(
        logical_identity=identity, disposition_id=None, authorization_context_ref="operator", snapshot_token=None,
        after_cursor=None, max_items=16, max_bytes="1000000",
    ))

    # --- an outbox entry taken through failure, lease, load and application, which
    # is the one path that reaches every remaining outbox operation without a fault
    # having to be injected into the delivery above.
    heartbeat = HeartbeatProjectionPayload(kind="heartbeat", heartbeat_at=now, evaluated_through_at=now,
                                            prior_committed_at=state.last_committed_at)
    heartbeat_digest = canonical_digest(heartbeat.model_dump(mode="json", by_alias=True))
    revision = str(int(state.required_projection_revision) + 1)
    intent_base = {
        "schema": "ergasterion.projection-intent/v1", "logical_identity": identity.model_dump(mode="json"),
        "contract_digest": contract_digest, "projection_target": "bronze", "projection_revision": revision,
        "originating_state_revision": state.state_revision, "kind": "heartbeat",
        "payload_digest": heartbeat_digest,
    }
    intent = ProjectionIntent(
        schema="ergasterion.projection-intent/v1", logical_identity=identity, contract_digest=contract_digest,
        projection_target="bronze", projection_revision=revision, originating_state_revision=state.state_revision,
        kind=ProjectionIntentKind.HEARTBEAT, execution_plan_digest=plan_digest,
        runtime_manifest_digest=manifest_digest, payload=heartbeat, payload_digest=heartbeat_digest,
        projection_intent_digest=canonical_digest(intent_base),
    )
    outbox_id = canonical_digest({"intent": intent.projection_intent_digest})
    state = store.state_transaction(StateOutboxTransaction(
        expected_state_revision=state.state_revision,
        next_state=_evolve(state, required_projection_revision=revision), attempt_updates=(),
        deployment_update=None, projection_confirmation=None,
        enqueue=(OutboxEnqueue(outbox_id=outbox_id, payload=ProjectionOutboxPayload(entry_kind="projection", intent=intent),
                                payload_digest=intent.projection_intent_digest, next_not_before=now),),
        complete=(),
    ))
    state = store.fail_outbox(OutboxFailureTransaction(
        expected_state_revision=state.state_revision, next_state=state, attempt_updates=(), outbox_id=outbox_id,
        payload_digest=intent.projection_intent_digest, lease_owner=runtime.lease_owner, failure_observed_at=now,
        reason_code="target_unavailable", disposition=OutboxFailureDisposition.RETRYABLE, next_not_before=now,
    ))
    leased = store.lease_outbox(identity, OutboxEntryKind.PROJECTION, runtime.lease_owner, now, 16)
    entry = next(item for item in leased if item.outbox_id == outbox_id)
    payload = store.load_outbox_payload(entry.outbox_id, entry.payload_digest)
    confirmation = recorded.projection_publisher.apply_gap_ordered(payload.intent)
    state = store.state_transaction(StateOutboxTransaction(
        expected_state_revision=state.state_revision, next_state=state, attempt_updates=(), deployment_update=None,
        projection_confirmation=confirmation, enqueue=(),
        complete=(OutboxCompletion(outbox_id=outbox_id, payload_digest=entry.payload_digest,
                                    lease_owner=runtime.lease_owner, completed_at=now),),
    ))
    recorded.projection_publisher.rebuild_read_models(ProjectionReplayBatch(
        intents=(intent,), confirmations=(confirmation,), max_items=16, max_bytes="1000000", bytes_supplied="0",
    ))

    # --- snapshot and tombstone keysets, each from begin through the evidence it implies.
    record_key_scope = contract.delivery.record_key.fingerprint_scope or FingerprintScope(
        scope_id="reference-record-key", scope_parameters={},
    )
    commitment = recorded.key_resolver.key_commitment(key_id)
    recorded.key_resolver.resolve_verification_key(key_id)
    tag = recorded.key_resolver.mac(key_id, "record-key", _b64url(b"record-key-1")).tag_hex
    keyset = store.begin_snapshot_keyset(SnapshotKeysetRequest(
        attempt_id=attempt.attempt_id, logical_identity=identity, visibility=visibility,
        record_key_scope=record_key_scope, hmac_key_id=key_id, key_commitment=commitment.commitment,
    ))
    store.append_snapshot_keyset(attempt.attempt_id, RecordKeyTagPage(
        keyset_id=keyset.keyset_id, first_frame_sequence="0", next_frame_sequence="1", tags=(tag,), bytes_supplied="0",
    ))
    keyset = store.complete_snapshot_keyset(SnapshotKeysetCompletion(
        attempt_id=attempt.attempt_id, keyset_id=keyset.keyset_id, expected_key_count="1",
        expected_keyset_digest=canonical_digest({"tags": [tag]}),
    ))
    store.get_snapshot_keyset(identity, visibility)
    store.reconcile_snapshot(SnapshotReconciliationRequest(
        attempt_id=attempt.attempt_id, claim_digest=attempt.claim_digest, prior_keyset=None, candidate_keyset=keyset,
    ))
    tombstones = store.begin_tombstone_keyset(TombstoneKeysetRequest(
        attempt_id=attempt.attempt_id, logical_identity=identity, visibility=visibility,
        record_key_scope=record_key_scope, hmac_key_id=key_id, key_commitment=commitment.commitment,
    ))
    tombstone_tag = TombstoneTag(event_sequence="1", tag=tag)
    store.append_tombstone_keyset(attempt.attempt_id, TombstoneTagPage(
        keyset_id=tombstones.keyset_id, items=(tombstone_tag,), bytes_supplied="0",
    ))
    tombstones = store.complete_tombstone_keyset(TombstoneKeysetCompletion(
        attempt_id=attempt.attempt_id, keyset_id=tombstones.keyset_id, expected_key_count="1",
        expected_keyset_digest=canonical_digest({"tags": [tombstone_tag.model_dump(mode="json")]}),
        event_sequence_low="1", event_sequence_high="1",
    ))
    store.finalize_tombstone_evidence(TombstoneEvidenceRequest(
        attempt_id=attempt.attempt_id, claim_digest=attempt.claim_digest, keyset=tombstones,
    ))

    return {name: tuple(dict.fromkeys(reached)) for name, reached in calls.items()}


# --------------------------------------------------------------------------- vectors

@dataclass(frozen=True)
class VectorOutcome:
    vector_id: str
    passed: bool
    detail: str


def load_vectors(path: Path | None = None) -> tuple[dict, ...]:
    with open(path or VECTORS_PATH, encoding="utf-8") as fh:
        document = json.load(fh)
    return tuple(document["vectors"])


def memory_ports_factory(
    vector: dict, contract: BronzeProductContract, payload_handle: Token,
) -> tuple[PortSet, StreamState]:
    """The reference ``ports_factory``: read one vector's fault declarations and
    build the in-memory ``PortSet`` they describe. A real adapter set supplies
    its own factory of this shape and passes it to ``run_adapter_conformance``,
    which is the whole of what it must implement to be run against these same
    vectors."""

    return build_memory_ports(
        contract.logical_identity, content_by_handle={payload_handle: vector["rows"]},
        fail_first_n=int(vector.get("publisher_failures", 1 if vector.get("publisher_fault") == "fail_once" else 0)),
        finish_prepare_fault=vector.get("landing_fault"),
        finish_materialization_fault=vector.get("materialization_fault"),
    )


def run_adapter_conformance(
    vector: dict, contract: BronzeProductContract, ports_factory=memory_ports_factory,
) -> VectorOutcome:
    """Run one submission-family vector end to end through a fresh
    ``IngestionRuntime`` over the ``PortSet`` ``ports_factory`` returns, and
    compare the observed outcome against the vector's ``expect`` block.

    Implementations arrive explicitly, through ``ports_factory``: this function
    resolves nothing from a registry and imports no adapter, so a real state
    store, landing adapter or raw store proves itself against exactly these
    vectors by passing its own factory. ``memory_ports_factory`` is only the
    default."""

    rows = vector["rows"]
    handle = f"handle-{vector['id']}"
    contract = contract_variant(
        contract, integration_kind=vector.get("integration", "managed"),
        publication_mode=PublicationPolicy(vector["publication_mode"]) if "publication_mode" in vector else None,
    )
    identity = contract.logical_identity
    ports, stream_state = ports_factory(vector, contract, handle)
    clock = fixed_clock()
    runtime = IngestionRuntime(ports, clock)
    plan_digest = canonical_digest({"plan": "bronze"})
    manifest_digest = canonical_digest({"manifest": "bronze"})
    run_id = canonical_digest({"run": vector["id"]})

    contract_digest = canonical_digest(contract.model_dump(mode="json", by_alias=True))
    claim_contract_digest = vector.get("contract_digest_override") or contract_digest
    progress_kind = vector.get("progress_kind_override") or contract.delivery.progress.kind
    claim = (
        {"kind": "opaque_batch"} if progress_kind == "opaque_batch"
        else {"kind": "sequence", "high_watermark": str(len(rows)), "event_count": str(len(rows))}
    )
    manifest = DeliveryManifest(
        schema="ergasterion.delivery-manifest/v1", logical_identity=identity,
        product_version=contract.product.product_version, contract_digest=claim_contract_digest,
        delivery_id=vector.get("delivery_id", vector["id"]), batch_id=None, scheduled_boundary_at=None,
        effective_boundary_at=None,
        payload=PayloadDescriptor(media_type="application/x-ndjson", content_encoding="identity", codec_version=1,
                                   byte_length=str(len(json.dumps(rows))), sha256="0" * 64),
        frame_sequence_digest=None, progress_claim=claim, declared_row_count=str(len(rows)), snapshot_attestation=None,
    )
    input_record = ManagedPayloadInput(kind="managed_payload", manifest=manifest, payload_handle=handle)
    expect = vector["expect"]

    try:
        attempt, stream_state = runtime.submit_managed(
            stream_state, contract, plan_digest, manifest_digest, run_id, input_record,
        )
        if vector.get("resubmit"):
            replay_attempt, stream_state = runtime.submit_managed(
                stream_state, contract, plan_digest, manifest_digest, run_id, input_record,
            )
            if replay_attempt.attempt_id != attempt.attempt_id:
                return VectorOutcome(vector["id"], False, "resubmission produced a different attempt_id, replay is not idempotent")

        if "conflict_second_rows" in vector:
            second_handle = f"{handle}-conflict"
            ports.raw_store.content_by_handle[second_handle] = vector["conflict_second_rows"]
            second_manifest = DeliveryManifest(
                schema="ergasterion.delivery-manifest/v1", logical_identity=identity,
                product_version=contract.product.product_version, contract_digest=claim_contract_digest,
                delivery_id=manifest.delivery_id, batch_id=None, scheduled_boundary_at=None, effective_boundary_at=None,
                payload=PayloadDescriptor(media_type="application/x-ndjson", content_encoding="identity", codec_version=1,
                                           byte_length=str(len(json.dumps(vector["conflict_second_rows"]))), sha256="1" * 64),
                frame_sequence_digest=None, progress_claim=claim, declared_row_count=str(len(vector["conflict_second_rows"])),
                snapshot_attestation=None,
            )
            second_input = ManagedPayloadInput(kind="managed_payload", manifest=second_manifest, payload_handle=second_handle)
            runtime.submit_managed(stream_state, contract, plan_digest, manifest_digest, run_id, second_input)
            return VectorOutcome(vector["id"], False, "expected claim_conflict, second submission succeeded")

        visibility = DeliveryVisibilityIdentity(epoch="0", kind="delivery", id=digest_token(attempt.attempt_id, "delivery"))
        raw_receipt = ports.raw_store.preserve(input_record)
        attempt, stream_state, materialized, validation = runtime.land_and_validate(
            attempt, stream_state, contract, raw_receipt, visibility,
            canonical_digest({"evaluation": vector["id"]}), canonical_digest({"ruleset": vector["id"]}),
        )
        readiness = build_readiness(
            contract, manifest_digest, result=ReadinessResult(vector.get("readiness_result", "ready")),
            revoked_at=vector.get("readiness_revoked_at"),
        )
        result = runtime.publish(
            attempt, stream_state, contract, materialized, validation, visibility, raw_receipt, readiness,
        )
        outcome = result.attempt.state.value
        max_attempts = int(contract.delivery.retry.max_attempts)

        if outcome == "commit_blocked" and expect["outcome"] in ("committed_after_retry", "retry_exhausted"):
            resumed = runtime.run_due(identity, clock.now(), max_attempts)
            while resumed and resumed[-1].retry_directive is not None and not resumed[-1].retry_directive.exhausted:
                resumed = runtime.run_due(identity, clock.now(), max_attempts)
            if not resumed:
                return VectorOutcome(vector["id"], False, "expected a due entry to resume, none was leased")
            last = resumed[-1]
            if last.retry_directive is not None and last.retry_directive.exhausted:
                outcome = "retry_exhausted"
            elif last.attempt.state.value == "committed":
                outcome = "committed_after_retry"
            else:
                return VectorOutcome(vector["id"], False, f"expected retry to settle, got {last.attempt.state.value!r}")

        if expect["outcome"] != outcome:
            return VectorOutcome(vector["id"], False, f"expected outcome {expect['outcome']!r}, got {outcome!r}")
        if "accepted_count" in expect and validation.accepted_count != expect["accepted_count"]:
            return VectorOutcome(vector["id"], False,
                                  f"expected accepted_count {expect['accepted_count']!r}, got {validation.accepted_count!r}")
        return VectorOutcome(vector["id"], True, f"outcome matched: {outcome!r}")
    except PortError as exc:
        if expect.get("outcome") != "error" or expect.get("error_code") != exc.code:
            return VectorOutcome(vector["id"], False, f"unexpected error {exc.code!r}: {exc.detail}")
        # A vector may also pin the state the attempt was left in, which is the
        # only way to tell a failure that fenced the attempt from one that
        # merely propagated an error code while leaving the attempt in flight.
        if "attempt_state" in expect:
            latest = ports.state_store.status_query(identity).latest_attempt
            reached = latest.state.value if latest is not None else None
            if reached != expect["attempt_state"]:
                return VectorOutcome(
                    vector["id"], False, f"expected the attempt to reach {expect['attempt_state']!r}, got {reached!r}",
                )
        return VectorOutcome(vector["id"], True, f"error outcome matched: {exc.code!r}")


def run_all(vectors: tuple[dict, ...], contract: BronzeProductContract) -> tuple[VectorOutcome, ...]:
    return tuple(run_adapter_conformance(vector, contract) for vector in vectors)


__all__ = [
    "build_adapter_capabilities",
    "build_capabilities",
    "build_deployment",
    "build_memory_ports",
    "build_readiness",
    "build_runtime_binding",
    "contract_variant",
    "exercise_all_operations",
    "fixed_clock",
    "memory_ports_factory",
    "record_port_calls",
    "reference_key_resolver",
    "run_adapter_conformance",
    "run_all",
    "load_vectors",
    "REFERENCE_KEY_ID",
    "SimulatedCrash",
    "VectorOutcome",
    "VECTORS_PATH",
    "FakeSourceConnector",
    "FakeRawStore",
    "FakeScratchStore",
    "MemoryStateStore",
    "FakeLandingAdapter",
    "FakeRemediationRepository",
    "FakeProjectionPublisher",
    "FakeLifecycleSink",
    "FakeKeyResolver",
]
