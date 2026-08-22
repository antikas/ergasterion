"""The nine Bronze runtime port protocols and the ``PortSet`` bundle the
deterministic ingestion service (``ergasterion.ingestion.runtime``) is built
against.

Each ``Protocol`` below is a structural transcription of one
``ergasterion.ingestion.records.PORTS`` entry: same method names, same
parameter names, same request/response record types. It carries no behaviour
and no base class a real adapter must inherit from -- a SQLite state store, a
DuckDB landing adapter, a local file raw store or a fake built for a test all
satisfy a port by structural shape alone (Python's normal ``Protocol``
duck-typing), exactly as ``ergasterion.framework.routing.RoutableTranslator``
already does for translators. Nothing in this module reads or writes bytes;
it is safe to import from a wheel install with no backend present.

Nine ports, IDL ``PortKind`` order: ``SourceConnector``, ``RawStore``,
``ScratchStore``, ``DeliveryStateStore``, ``LandingAdapter``,
``RemediationRepository``, ``ProjectionPublisher``, ``LifecycleSink``,
``KeyResolver``. ``PortSet`` binds one implementation of each under the exact
field names ``ergasterion.framework.runtime_binding.RuntimePortBindings``
already uses, so a resolved ``RuntimeBinding`` and a ``PortSet`` name the same
nine slots.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol, runtime_checkable

from ergasterion.ingestion.records import (
    AttemptPage,
    AttemptQuery,
    Base64Url,
    BronzeEvidence,
    BronzeProductContract,
    CandidateFramePage,
    CandidateReadQuery,
    ContractLifecycleRequest,
    ContractLifecycleTransitionResult,
    DeletionEvidenceIntent,
    Digest,
    DeliveryInput,
    DeliveryVisibilityIdentity,
    DispositionPage,
    DispositionQuery,
    DispositionQueryPage,
    EvidencePage,
    EvidenceQuery,
    ExternalReceiptInput,
    KeyCommitmentRecord,
    LandingPreparation,
    LifecycleEventBatch,
    LifecycleEventLogPage,
    LifecycleEventLogQuery,
    LogicalIdentity,
    MacResult,
    ManagedPayloadInput,
    MaterializationCompletion,
    MaterializationSession,
    MaterializedBronzeEvidence,
    NonNegativeIntegerString,
    OperationalStatus,
    OutboxEntry,
    OutboxEntryKind,
    OutboxFailureTransaction,
    OutboxPayload,
    PositiveInteger,
    PositiveIntegerString,
    ProjectionConfirmation,
    ProjectionConfirmationLogPage,
    ProjectionCursor,
    ProjectionIntent,
    ProjectionLogPage,
    ProjectionReplayBatch,
    RawReadHandle,
    RawReadPage,
    RawReceipt,
    RecordKeyTagPage,
    ReleaseMaterializationRequest,
    ReleaseVisibilityBinding,
    RemediationDecision,
    RemediationDecisionPage,
    RemediationDecisionQuery,
    ScratchChunk,
    ScratchReadPage,
    ScratchScope,
    SnapshotKeyset,
    SnapshotKeysetCompletion,
    SnapshotKeysetRequest,
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
    TombstoneTagPage,
    UnitResult,
    UtcInstant,
    VerificationKeyRecord,
    VisibilityIdentity,
)
from ergasterion.framework.runtime_binding import (
    DeploymentLifecycleRequest,
)

# ``DeploymentLifecycleTransitionResult`` carries a ``StreamState`` field and so is
# defined in ``ergasterion.ingestion.records`` (see that module's own comment); import
# it from there rather than from ``runtime_binding``.
from ergasterion.ingestion.records import DeploymentLifecycleTransitionResult


# --------------------------------------------------------------------------- SourceConnector

@runtime_checkable
class SourceConnectorPort(Protocol):
    def submit_managed(self, input: ManagedPayloadInput) -> DeliveryInput: ...

    def verify_external(self, input: ExternalReceiptInput) -> DeliveryInput: ...


# --------------------------------------------------------------------------- RawStore

@runtime_checkable
class RawStorePort(Protocol):
    def get_receipt(self, raw_receipt_digest: Digest) -> RawReceipt: ...

    def open_raw(self, raw_receipt_digest: Digest) -> RawReadHandle: ...

    def read_raw(
        self, handle: RawReadHandle, offset: NonNegativeIntegerString, max_bytes: PositiveIntegerString
    ) -> RawReadPage: ...

    def preserve(self, input: ManagedPayloadInput) -> RawReceipt: ...

    def verify_open(self, input: ExternalReceiptInput) -> RawReceipt: ...


# --------------------------------------------------------------------------- ScratchStore

@runtime_checkable
class ScratchStorePort(Protocol):
    def create_scope(self, attempt_id: Digest, capacity_bytes: PositiveIntegerString) -> ScratchScope: ...

    def write_sequential(self, attempt_id: Digest, chunk: ScratchChunk) -> UnitResult: ...

    def read_sequential(
        self, attempt_id: Digest, scope_id: Token, after_sequence: NonNegativeIntegerString,
        max_bytes: PositiveIntegerString,
    ) -> ScratchReadPage: ...

    def close_scope(self, attempt_id: Digest, scope_id: Token) -> UnitResult: ...

    def delete_scope(self, attempt_id: Digest, scope_id: Token) -> UnitResult: ...

    def cleanup_orphans(self, active_attempt_ids: tuple[Digest, ...], max_scopes: PositiveInteger) -> tuple[Token, ...]: ...


# --------------------------------------------------------------------------- DeliveryStateStore

@runtime_checkable
class DeliveryStateStorePort(Protocol):
    def contract_lifecycle(self, request: ContractLifecycleRequest) -> ContractLifecycleTransitionResult: ...

    def deployment_lifecycle(self, request: DeploymentLifecycleRequest) -> DeploymentLifecycleTransitionResult: ...

    def attempts(self, query: AttemptQuery) -> AttemptPage: ...

    def state_transaction(self, transaction: StateOutboxTransaction) -> StreamState: ...

    def lease_outbox(
        self, logical_identity: LogicalIdentity, entry_kind: OutboxEntryKind, lease_owner: Token,
        observed_at: UtcInstant, max_items: PositiveInteger,
    ) -> tuple[OutboxEntry, ...]: ...

    def load_outbox_payload(self, outbox_id: Digest, payload_digest: Digest) -> OutboxPayload: ...

    def fail_outbox(self, transaction: OutboxFailureTransaction) -> StreamState: ...

    def projection_log(
        self, logical_identity: LogicalIdentity, after_revision: NonNegativeIntegerString,
        max_items: PositiveInteger, max_bytes: PositiveIntegerString,
    ) -> ProjectionLogPage: ...

    def projection_confirmation_log(
        self, logical_identity: LogicalIdentity, after_revision: NonNegativeIntegerString,
        max_items: PositiveInteger, max_bytes: PositiveIntegerString,
    ) -> ProjectionConfirmationLogPage: ...

    def lifecycle_event_log(self, query: LifecycleEventLogQuery) -> LifecycleEventLogPage: ...

    def status_query(self, logical_identity: LogicalIdentity) -> OperationalStatus: ...

    def begin_snapshot_keyset(self, request: SnapshotKeysetRequest) -> SnapshotKeyset: ...

    def append_snapshot_keyset(self, attempt_id: Digest, page: RecordKeyTagPage) -> SnapshotKeyset: ...

    def complete_snapshot_keyset(self, completion: SnapshotKeysetCompletion) -> SnapshotKeyset: ...

    def get_snapshot_keyset(self, logical_identity: LogicalIdentity, visibility: VisibilityIdentity) -> SnapshotKeyset: ...

    def reconcile_snapshot(self, request: SnapshotReconciliationRequest) -> SnapshotReconciliationResult: ...

    def begin_tombstone_keyset(self, request: TombstoneKeysetRequest) -> TombstoneKeyset: ...

    def append_tombstone_keyset(self, attempt_id: Digest, page: TombstoneTagPage) -> TombstoneKeyset: ...

    def complete_tombstone_keyset(self, completion: TombstoneKeysetCompletion) -> TombstoneKeyset: ...

    def finalize_tombstone_evidence(self, request: TombstoneEvidenceRequest) -> DeletionEvidenceIntent: ...


# --------------------------------------------------------------------------- LandingAdapter

@runtime_checkable
class LandingAdapterPort(Protocol):
    def begin_prepare(
        self, attempt_id: Digest, receipt: RawReceipt, raw: RawReadHandle, contract: BronzeProductContract,
        visibility: VisibilityIdentity,
    ) -> LandingPreparation: ...

    def append_raw(self, preparation: LandingPreparation, page: RawReadPage) -> LandingPreparation: ...

    def finish_prepare(self, preparation: LandingPreparation) -> BronzeEvidence: ...

    def read_candidate(self, query: CandidateReadQuery) -> CandidateFramePage: ...

    def begin_materialization(
        self, attempt_id: Digest, evidence: BronzeEvidence, evaluation_id: Digest, ruleset_digest: Digest
    ) -> MaterializationSession: ...

    def append_dispositions(self, session: MaterializationSession, page: DispositionPage) -> MaterializationSession: ...

    def finish_materialization(self, completion: MaterializationCompletion) -> MaterializedBronzeEvidence: ...

    def bind_release_visibility(self, binding: ReleaseVisibilityBinding) -> MaterializedBronzeEvidence: ...

    def materialize_release(self, request: ReleaseMaterializationRequest) -> MaterializedBronzeEvidence: ...

    def source_native_query(self, query: SourceNativeQuery) -> SourceNativePage: ...

    def disposition_query(self, query: DispositionQuery) -> DispositionQueryPage: ...

    def verify_open(self, input: ExternalReceiptInput, visibility: DeliveryVisibilityIdentity) -> BronzeEvidence: ...


# --------------------------------------------------------------------------- RemediationRepository

@runtime_checkable
class RemediationRepositoryPort(Protocol):
    def record_decision(self, decision: RemediationDecision) -> RemediationDecision: ...

    def decision_query(self, query: RemediationDecisionQuery) -> RemediationDecisionPage: ...


# --------------------------------------------------------------------------- ProjectionPublisher

@runtime_checkable
class ProjectionPublisherPort(Protocol):
    def apply_gap_ordered(self, intent: ProjectionIntent) -> ProjectionConfirmation: ...

    def read_cursor(self, logical_identity: LogicalIdentity, projection_target: Token) -> ProjectionCursor: ...

    def rebuild_read_models(self, batch: ProjectionReplayBatch) -> ProjectionCursor: ...


# --------------------------------------------------------------------------- LifecycleSink

@runtime_checkable
class LifecycleSinkPort(Protocol):
    def project_events(self, batch: LifecycleEventBatch) -> tuple[Digest, ...]: ...

    def evidence_query(self, query: EvidenceQuery) -> EvidencePage: ...


# --------------------------------------------------------------------------- KeyResolver

@runtime_checkable
class KeyResolverPort(Protocol):
    def resolve_verification_key(self, key_id: Token) -> VerificationKeyRecord: ...

    def key_commitment(self, key_id: Token) -> KeyCommitmentRecord: ...

    def mac(self, key_id: Token, domain: str, message_base64url: Base64Url) -> MacResult: ...


# --------------------------------------------------------------------------- bundle

@dataclass(frozen=True)
class PortSet:
    """One implementation of each of the nine Bronze runtime ports -- the exact
    field names ``RuntimePortBindings`` uses, so a resolved binding and a
    ``PortSet`` name the same nine slots. Passed to ``IngestionRuntime``
    (``ergasterion.ingestion.runtime``) and to
    ``ergasterion.ingestion.conformance.run_adapter_conformance``; both accept
    any object satisfying the matching ``*Port`` protocol above, real or fake,
    with no import of a concrete backend required in either direction."""

    source_connector: SourceConnectorPort
    raw_store: RawStorePort
    scratch_store: ScratchStorePort
    state_store: DeliveryStateStorePort
    landing_adapter: LandingAdapterPort
    remediation_repository: RemediationRepositoryPort
    projection_publisher: ProjectionPublisherPort
    lifecycle_sink: LifecycleSinkPort
    key_resolver: KeyResolverPort


PORT_PROTOCOLS: dict[str, type] = {
    "source_connector": SourceConnectorPort,
    "raw_store": RawStorePort,
    "scratch_store": ScratchStorePort,
    "state_store": DeliveryStateStorePort,
    "landing_adapter": LandingAdapterPort,
    "remediation_repository": RemediationRepositoryPort,
    "projection_publisher": ProjectionPublisherPort,
    "lifecycle_sink": LifecycleSinkPort,
    "key_resolver": KeyResolverPort,
}
"""Field name -> protocol class, in ``PortSet`` declaration order. Lets a caller
(the conformance runner, a future adapter's own test) verify structurally that
an object it was handed implements the port it claims to, via
``isinstance(candidate, PORT_PROTOCOLS[field_name])``."""


__all__ = [
    "SourceConnectorPort",
    "RawStorePort",
    "ScratchStorePort",
    "DeliveryStateStorePort",
    "LandingAdapterPort",
    "RemediationRepositoryPort",
    "ProjectionPublisherPort",
    "LifecycleSinkPort",
    "KeyResolverPort",
    "PortSet",
    "PORT_PROTOCOLS",
]
