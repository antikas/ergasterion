"""Closed Bronze operator commands for the local-ingestion runtime.

Human and ``--json`` views read public lifecycle and query ports. Plan compiles
a deterministic manifest; execution is a later ingest/reconcile step. Existing
``init`` / emit / import / lint / structure commands are not implemented here.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence

from ergasterion.estate import EstateContext
from ergasterion.framework.bronze_contract import (
    BackupAction,
    BronzeProductContract,
    CommandStatus,
    ContractActivationState,
    ContractLifecycleAction,
    DeploymentLifecycleAction,
    ErrorCategory,
    EvidenceKind,
    LifecycleEventType,
    Migration,
    MigrationKind,
    ProcessingOutcome,
    ProjectionIntentKind,
    QuarantineAction,
    RemediationActionStatus,
    SnapshotReconciliationStatus,
    TimelinessState,
)
from ergasterion.framework.models import Layer, compute_plan_digest
from ergasterion.framework.resolver import resolve
from ergasterion.framework.runtime_binding import (
    DeploymentLifecycleRequest,
    ProjectionCursor,
    RuntimeBinding,
    RuntimeDeployment,
    RuntimeManifest,
)
from ergasterion.framework.translator_conformance import check_translator_conformance
from ergasterion.ingestion.lifecycle import (
    bronze_execution_plan,
    build_lineage_descriptor,
    build_product_metadata,
    build_run_lineage,
    quality_handoff,
)
from ergasterion.ingestion.local_backup import BackupError, create_backup, restore_backup
from ergasterion.ingestion.records import (
    AttemptQuery,
    CommandEnvelope,
    CommandError,
    ContractActivationResult,
    ContractEvidenceItem,
    ContractLifecycleRequest,
    ContractRegisteredResult,
    DeliveryVisibilityIdentity,
    DeploymentActivationResult,
    DeploymentRegisteredResult,
    DispositionQuery,
    DueEvaluationResult,
    EvidencePage,
    EvidenceQuery,
    HeartbeatProjectionPayload,
    IngestionResult,
    InspectionResult,
    LifecycleEvent,
    LifecycleEventBatch,
    LineageLifecyclePayload,
    LocalBackupResult,
    MetadataLifecyclePayload,
    OutboxEnqueue,
    PlanCommandResult,
    ProjectionOutboxPayload,
    QualityLifecyclePayload,
    QuarantineItem,
    QuarantinePage,
    QuarantineResult,
    QuarantineSnapshot,
    ReceiptLifecyclePayload,
    ReconciliationResult,
    RemediationDecisionQuery,
    SchemaEvidenceItem,
    SourceNativeQuery,
    StateOutboxTransaction,
    StreamStatus,
    StreamStatusResult,
)
from ergasterion.ingestion.reference_runtime import (
    LocalRuntimeSession,
    admit_execution,
    aggregate_capability_digest,
    build_readiness,
    contract_digest as runtime_contract_digest,
    open_session,
)
from ergasterion.ingestion.runtime import (
    Clock,
    PortError,
    canonical_digest,
    digest_token,
    parse_utc_instant,
    scheduled_occurrences,
    timeliness_state,
    utc_now_string,
)
from ergasterion.ingestion.settings import (
    AUTHORIZATION_CONTEXT,
    MISSING_EXTRA_REMEDY,
    SYNTHETIC_ACCESS_POLICY,
    SYNTHETIC_CLASSIFICATION,
    SYNTHETIC_PROTECTION_PROFILE,
    SYNTHETIC_RETENTION_POLICY,
    LocalLayout,
    SettingsError,
    closed_local_binding,
    load_prior_binding,
    persist_prior_binding,
    reject_store_relocation,
    resolve_layout,
)
from ergasterion.ingestion.validation import validate_frames
from ergasterion.source_delivery import TypedDeclarations, compute_migration_id, load_typed_declarations
from ergasterion.translators.dbt import DbtTranslator
from ergasterion.translators.local_ingestion import (
    LocalIngestionTranslator,
    compile_runtime_manifest,
    runtime_binding_digest,
)

SHARED_HELP = (
    "Required inputs: --project-dir PATH --source NAME --table KEY --binding PATH "
    "--environment NAME. RuntimeBinding.environment is the source of truth; "
    "--environment is a mandatory assertion and a mismatch exits 2 before any "
    "generation or state change. Optional --json writes the closed "
    "ergasterion.command-result/v1 envelope to stdout; diagnostics stay on stderr."
)

BRONZE_INTRO = (
    "Bronze is the source-aligned product layer: an immutable received-batch "
    "receipt, typed source-native records with validation disposition, an accepted "
    "downstream projection, quarantine/remediation, and lineage/metadata. This "
    "command surface operates on a local file connector that consumes a sidecar "
    "manifest plus payload at the received-batch boundary. A direct connector "
    "implements the same source-connector port and preserves the same contract."
)

READ_ONLY = "This command is read-only inspection; it does not mutate delivery state."
MUTATING = "This command mutates contract, deployment, delivery or backup state."


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ergasterion",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=(
            f"{BRONZE_INTRO}\n\n{SHARED_HELP}\n\n"
            "Read-only commands: plan, status, inspect, quarantine --action list, "
            "ingest due --dry-run. Mutating commands: contract, deployment, ingest file, "
            "ingest due, reconcile, quarantine revalidate/release, local-backup."
        ),
        epilog=(
            "Safe next actions after plan: contract register then contract activate. "
            "After activation: deployment register/activate, then ingest file. "
            "Use status and inspect to read evidence. Use local-backup only when "
            "the runtime is quiescent (no in-flight attempt, complete outbox)."
        ),
    )
    sub = parser.add_subparsers(dest="command", required=True)

    plan = sub.add_parser(
        "plan",
        help="Compile the Bronze graph, routes and runtime manifest (read-only).",
        description=(
            f"{BRONZE_INTRO} {READ_ONLY} {SHARED_HELP}\n\n"
            "Safe next action: contract register."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    _add_shared(plan)

    contract = sub.add_parser(
        "contract",
        help="Register or activate a Bronze product contract (mutating).",
        description=f"{BRONZE_INTRO} {MUTATING} {SHARED_HELP}",
    )
    csub = contract.add_subparsers(dest="contract_action", required=True)
    register_c = csub.add_parser(
        "register",
        help="Register the compiled contract as a candidate. No active alias change.",
        description=f"{MUTATING} Safe next action: contract activate --candidate-digest SHA256 --migration carry|reset.",
    )
    _add_shared(register_c)
    activate_c = csub.add_parser(
        "activate",
        help="Carry or reset-activate a registered candidate contract.",
        description=f"{MUTATING} Carry keeps visibility progress; reset authorises a new baseline.",
    )
    _add_shared(activate_c)
    activate_c.add_argument("--candidate-digest", required=True, metavar="SHA256")
    activate_c.add_argument("--migration", required=True, choices=("carry", "reset"))

    deployment = sub.add_parser(
        "deployment",
        help="Register or activate a binding-only runtime deployment (mutating).",
        description=f"{BRONZE_INTRO} {MUTATING} {SHARED_HELP} Binding-only relocation cannot move durable stores.",
    )
    dsub = deployment.add_subparsers(dest="deployment_action", required=True)
    register_d = dsub.add_parser(
        "register",
        help="Register a candidate runtime manifest for the active contract.",
        description=f"{MUTATING} Safe next action: deployment activate --manifest-digest SHA256.",
    )
    _add_shared(register_d)
    activate_d = dsub.add_parser(
        "activate",
        help="Activate a caught-up binding-only relocation.",
        description=f"{MUTATING} Does not change product progress or the active contract.",
    )
    _add_shared(activate_d)
    activate_d.add_argument("--manifest-digest", required=True, metavar="SHA256")

    ingest = sub.add_parser(
        "ingest",
        help="Ingest a received file or evaluate due transitions.",
        description=(
            f"{BRONZE_INTRO} {MUTATING} {SHARED_HELP} "
            "ingest file writes a receipt from a sidecar manifest plus payload. "
            "ingest due writes trusted-clock due transitions; --at is only valid with --dry-run."
        ),
    )
    isub = ingest.add_subparsers(dest="ingest_action", required=True)
    ingest_file = isub.add_parser(
        "file",
        help="Preserve a received-batch sidecar and payload, then land, validate and publish.",
        description=f"{MUTATING} Required extra inputs: --manifest PATH --payload PATH.",
    )
    _add_shared(ingest_file)
    ingest_file.add_argument("--manifest", required=True, metavar="PATH")
    ingest_file.add_argument("--payload", required=True, metavar="PATH")
    ingest_due = isub.add_parser(
        "due",
        help="Evaluate due heartbeats and schedule catch-up. Dry-run is read-only.",
        description="Without --dry-run this mutates projection intents. --at is valid only with --dry-run.",
    )
    _add_shared(ingest_due)
    ingest_due.add_argument("--dry-run", action="store_true")
    ingest_due.add_argument("--at", metavar="UTC")

    reconcile = sub.add_parser(
        "reconcile",
        help="Resume commit-blocked projection and rebuild lagging target cursors (mutating).",
        description=f"{MUTATING} {SHARED_HELP}",
    )
    _add_shared(reconcile)
    reconcile.add_argument("--target", metavar="ID")

    backup = sub.add_parser(
        "local-backup",
        help="Create or restore a verified copy of the complete local runtime root.",
        description=(
            f"{MUTATING} {SHARED_HELP} Create refuses a nonterminal attempt or incomplete "
            "outbox. Restore requires an empty staged root and never creates a ReprocessingClaim."
        ),
    )
    _add_shared(backup)
    backup.add_argument("--action", required=True, choices=("create", "restore"))
    backup.add_argument("--destination", metavar="PATH")
    backup.add_argument("--manifest", metavar="PATH")

    status = sub.add_parser(
        "status",
        help="Read-only stream and operator status.",
        description=f"{READ_ONLY} {SHARED_HELP}",
    )
    _add_shared(status)

    inspect = sub.add_parser(
        "inspect",
        help="Read-only contract, schema, receipt, quality and lineage evidence.",
        description=f"{READ_ONLY} {SHARED_HELP} Optional --delivery-id selects one received batch.",
    )
    _add_shared(inspect)
    inspect.add_argument("--delivery-id", metavar="ID")

    quarantine = sub.add_parser(
        "quarantine",
        help="List (read-only) or revalidate/release quarantined rows.",
        description=(
            f"{BRONZE_INTRO} list is read-only; revalidate and release mutate remediation "
            f"state. Row-level mode is invalid and exits 2. {SHARED_HELP}"
        ),
    )
    _add_shared(quarantine)
    quarantine.add_argument("--action", required=True, choices=("list", "revalidate", "release"))
    quarantine.add_argument("--disposition-id", metavar="ID")
    quarantine.add_argument("--ruleset-digest", metavar="SHA256")
    quarantine.add_argument("--row-level", action="store_true")
    return parser


def _add_shared(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--project-dir", required=True, metavar="PATH")
    parser.add_argument("--source", required=True, metavar="NAME")
    parser.add_argument("--table", required=True, metavar="KEY")
    parser.add_argument("--binding", required=True, metavar="PATH")
    parser.add_argument("--environment", required=True, metavar="NAME")
    parser.add_argument("--json", action="store_true")


def _dump(value: Any) -> Any:
    if hasattr(value, "model_dump"):
        return value.model_dump(mode="json", by_alias=True)
    return value


def _error(code: str, message: str, *, field_path: str | None = None, safe_ref: str | None = None) -> CommandError:
    retryable = code in {
        "target_unavailable", "throttled", "transient_io", "inflight_attempt", "capacity_exceeded",
    }
    if code in {"invalid_usage", "missing_extra"}:
        category = ErrorCategory.USAGE
    elif code in {"invalid_config", "production_policy_adapter_required"}:
        category = ErrorCategory.CONFIG if code == "invalid_config" else ErrorCategory.CAPABILITY
    elif code in {"capability_mismatch", "capacity_exceeded", "schema_invalid"}:
        category = ErrorCategory.CAPABILITY
    elif code in {
        "superseded_contract", "superseded_deployment", "contract_conflict", "migration_conflict",
        "claim_conflict", "stale_revision", "decision_conflict", "intent_conflict",
    }:
        category = ErrorCategory.CONFLICT
    elif code in {"integrity_error", "bronze_store_restore_required"}:
        category = ErrorCategory.INTEGRITY
    elif retryable:
        category = ErrorCategory.RETRYABLE
    else:
        category = ErrorCategory.PERMANENT
    payload: dict[str, Any] = {
        "code": code, "category": category, "retryable": retryable, "message": message,
    }
    if field_path is not None:
        payload["field_path"] = field_path
    if safe_ref is not None:
        payload["safe_ref"] = safe_ref
    return CommandError(**payload)


def _exit_for(errors: tuple[CommandError, ...], status: CommandStatus) -> int:
    if status is CommandStatus.OK or status is CommandStatus.NOOP:
        return 0
    if status is CommandStatus.RETRYABLE:
        return 5
    if any(item.category in {ErrorCategory.USAGE, ErrorCategory.CONFIG, ErrorCategory.CAPABILITY} for item in errors):
        return 2
    return 4


def _strip_omittable_nulls(model: object, data: object) -> object:
    if not isinstance(data, dict):
        return data
    omit = getattr(type(model), "_omittable_not_nullable", frozenset())
    for name in omit:
        wire = "schema" if name == "schema_" else name
        if data.get(wire) is None:
            data.pop(wire, None)
    fields = getattr(type(model), "model_fields", None)
    if not fields:
        return data
    for name, field in fields.items():
        wire = field.alias or name
        if name == "schema_":
            wire = "schema"
        if wire not in data:
            continue
        inner = getattr(model, name, None)
        if inner is not None and hasattr(type(inner), "model_fields"):
            _strip_omittable_nulls(inner, data[wire])
        elif isinstance(inner, (list, tuple)) and isinstance(data[wire], list):
            for item, dumped in zip(inner, data[wire]):
                if hasattr(type(item), "model_fields"):
                    _strip_omittable_nulls(item, dumped)
    return data


def _emit(envelope: CommandEnvelope, *, as_json: bool, stream=None) -> int:
    if stream is None:
        stream = sys.stdout
    if as_json:
        payload = envelope.model_dump(mode="json", by_alias=True)
        _strip_omittable_nulls(envelope, payload)
        stream.write(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    else:
        stream.write(_human(envelope))
        if not _human(envelope).endswith("\n"):
            stream.write("\n")
    return _exit_for(envelope.errors, envelope.status)


def _human(envelope: CommandEnvelope) -> str:
    lines = [
        f"command: {envelope.command}",
        f"status: {envelope.status.value}",
    ]
    if envelope.logical_identity is not None:
        ident = envelope.logical_identity
        lines.append(f"product: {ident.estate_namespace}.{ident.source}.{ident.table}")
    if envelope.contract_digest:
        lines.append(f"contract_digest: {envelope.contract_digest}")
    if envelope.execution_plan_digest:
        lines.append(f"execution_plan_digest: {envelope.execution_plan_digest}")
    if envelope.runtime_manifest_digest:
        lines.append(f"runtime_manifest_digest: {envelope.runtime_manifest_digest}")
    if envelope.result is not None:
        kind = envelope.result.kind
        lines.append(f"result: {kind}")
        dumped = envelope.result.model_dump(mode="json", by_alias=True)
        for key in (
            "runtime_manifest_digest", "candidate_contract_digest", "active_contract_digest",
            "activation_state", "migration", "backup_id", "action", "more_due",
            "transitions_applied", "processing", "timeliness",
        ):
            if key in dumped and dumped[key] not in (None, {}, []):
                lines.append(f"{key}: {dumped[key]}")
    for err in envelope.errors:
        lines.append(f"error {err.code}: {err.message}")
    return "\n".join(lines) + "\n"


def _envelope(
    command: str,
    *,
    status: CommandStatus = CommandStatus.OK,
    identity=None,
    contract_digest: str | None = None,
    execution_plan_digest: str | None = None,
    runtime_manifest_digest: str | None = None,
    result=None,
    errors: Sequence[CommandError] = (),
) -> CommandEnvelope:
    ordered = tuple(sorted(errors, key=lambda item: (item.code, item.field_path or "", item.safe_ref or "")))
    return CommandEnvelope(
        schema="ergasterion.command-result/v1",
        command=command,
        status=status,
        logical_identity=identity,
        contract_digest=contract_digest,
        execution_plan_digest=execution_plan_digest,
        runtime_manifest_digest=runtime_manifest_digest,
        result=result,
        errors=ordered,
    )


def _fail(command: str, exc: Exception, *, as_json: bool, identity=None) -> int:
    if isinstance(exc, SettingsError):
        err = _error(exc.code, str(exc), field_path=exc.field_path)
    elif isinstance(exc, PortError):
        err = _error(exc.code, exc.detail or str(exc))
    elif isinstance(exc, argparse.ArgumentError):
        err = _error("invalid_usage", str(exc))
    else:
        err = _error("invalid_config", str(exc))
    status = CommandStatus.RETRYABLE if err.retryable else CommandStatus.FAILED
    sys.stderr.write(f"{err.code}: {err.message}\n")
    return _emit(
        _envelope(command, status=status, identity=identity, errors=(err,)),
        as_json=as_json,
    )


class _Context:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.project_dir = Path(args.project_dir).resolve()
        self.estate = EstateContext.resolve(estate_root=self.project_dir)
        self.typed: TypedDeclarations = load_typed_declarations(self.estate)
        key = (args.source, args.table)
        table = self.typed.tables.get(key)
        if table is None or table.contract is None:
            raise SettingsError(
                "invalid_config",
                f"no production Bronze contract for source {args.source!r} table {args.table!r}",
            )
        self.contract: BronzeProductContract = table.contract
        self.layout: LocalLayout = resolve_layout(
            project_dir=self.project_dir,
            binding_path=Path(args.binding),
            environment=args.environment,
        )
        if self.layout.binding.logical_identity != self.contract.logical_identity:
            raise SettingsError("invalid_config", "binding logical identity does not match --source/--table")
        product = self.contract.product
        if (
            product.classification != SYNTHETIC_CLASSIFICATION
            or product.access_policy_ref != SYNTHETIC_ACCESS_POLICY
            or product.retention_policy_ref != SYNTHETIC_RETENTION_POLICY
        ):
            raise SettingsError("production_policy_adapter_required", "local commands admit only the synthetic-local policy tuple")
        self.graph = resolve(Layer.BRONZE)
        self.plan_digest = compute_plan_digest(self.graph)
        self.contract_digest = runtime_contract_digest(self.contract)
        self.wire_plan = bronze_execution_plan(self.contract, execution_plan_digest=self.plan_digest)
        self.manifest: RuntimeManifest = compile_runtime_manifest(self.graph, self.layout.binding)
        self.binding_digest = runtime_binding_digest(self.layout.binding)

    def session(self) -> LocalRuntimeSession:
        return open_session(self.layout, self.contract)

    def translators(self) -> tuple[LocalIngestionTranslator, DbtTranslator]:
        return (
            LocalIngestionTranslator(binding=self.layout.binding, plan_digest=self.plan_digest),
            DbtTranslator(
                typed=self.typed,
                bound={(self.args.source, self.args.table): self.layout.binding},
                plan_digest=self.plan_digest,
            ),
        )


def _cmd_plan(ctx: _Context) -> CommandEnvelope:
    local, dbt = ctx.translators()
    check_translator_conformance(ctx.graph, [local, dbt])
    findings = tuple()
    return _envelope(
        "plan",
        identity=ctx.contract.logical_identity,
        contract_digest=ctx.contract_digest,
        execution_plan_digest=ctx.plan_digest,
        runtime_manifest_digest=ctx.manifest.runtime_manifest_digest,
        result=PlanCommandResult(
            kind="plan",
            execution_plan=ctx.wire_plan,
            runtime_manifest=ctx.manifest,
            runtime_manifest_digest=ctx.manifest.runtime_manifest_digest,
            findings=findings,
        ),
    )


def _active_deployment(session: LocalRuntimeSession, ctx: _Context) -> RuntimeDeployment | None:
    status = session.runtime.ports.state_store.status_query(ctx.contract.logical_identity)
    with session.state_store._tx() as conn:
        stored = session.state_store._stream_row(conn, ctx.contract.logical_identity)
        if stored["deployment_json"]:
            return RuntimeDeployment.model_validate_json(stored["deployment_json"])
    return None


def _readiness(ctx: _Context, session: LocalRuntimeSession, manifest_digest: str):
    return build_readiness(
        ctx.contract, manifest_digest, now=session.now(), capability_digest=aggregate_capability_digest(),
    )


def _cmd_contract_register(ctx: _Context) -> CommandEnvelope:
    with ctx.session() as session:
        state = session.runtime.ports.state_store.status_query(ctx.contract.logical_identity).state
        request = ContractLifecycleRequest(
            schema="ergasterion.contract-lifecycle-request/v1",
            action=ContractLifecycleAction.REGISTER,
            expected_state_revision=state.state_revision,
            expected_deployment_revision=None,
            contract=ctx.contract,
            migration=None,
            permit_pre_intent_fence=False,
        )
        result = session.runtime.contract_lifecycle(request)
        readiness = _readiness(ctx, session, ctx.manifest.runtime_manifest_digest)
        return _envelope(
            "contract.register",
            identity=ctx.contract.logical_identity,
            contract_digest=ctx.contract_digest,
            execution_plan_digest=ctx.plan_digest,
            runtime_manifest_digest=ctx.manifest.runtime_manifest_digest,
            result=ContractRegisteredResult(
                kind="contract_registered",
                contract_digest=ctx.contract_digest,
                source_schema_digest=ctx.wire_plan.source_schema_digest,
                published_schema_digest=ctx.wire_plan.published_schema_digest,
                execution_plan_digest=ctx.plan_digest,
                runtime_manifest_digest=ctx.manifest.runtime_manifest_digest,
                readiness_digest=readiness.readiness_digest,
                state_revision=result.state.state_revision,
            ),
        )


def _migration(ctx: _Context, kind: str, from_digest: str | None, from_epoch: str, now: str) -> Migration:
    to_epoch = from_epoch if kind == "carry" else str(int(from_epoch) + 1)
    body = {
        "kind": kind,
        "from_contract_digest": from_digest,
        "to_contract_digest": ctx.contract_digest,
        "activated_at": now,
        "from_visibility_epoch": from_epoch,
        "to_visibility_epoch": to_epoch,
    }
    migration_id = compute_migration_id(body)
    return Migration(
        migration_id=migration_id,
        kind=MigrationKind.CARRY if kind == "carry" else MigrationKind.RESET,
        from_contract_digest=from_digest,
        to_contract_digest=ctx.contract_digest,
        activated_at=now,
        from_visibility_epoch=from_epoch,
        to_visibility_epoch=to_epoch,
    )


def _cmd_contract_activate(ctx: _Context) -> CommandEnvelope:
    if ctx.args.candidate_digest != ctx.contract_digest:
        raise SettingsError(
            "contract_conflict",
            "candidate-digest does not match the compiled contract",
            field_path="/candidate-digest",
        )
    with ctx.session() as session:
        status = session.runtime.ports.state_store.status_query(ctx.contract.logical_identity)
        state = status.state
        now = session.now()
        migration = _migration(
            ctx, ctx.args.migration, state.active_contract_digest, state.visibility_epoch, now,
        )
        # The store compares activate.migration to the registered candidate
        # migration. Register is migration-free; persist the operator choice
        # onto the same candidate immediately before activation.
        registered = session.runtime.contract_lifecycle(
            ContractLifecycleRequest(
                schema="ergasterion.contract-lifecycle-request/v1",
                action=ContractLifecycleAction.REGISTER,
                expected_state_revision=state.state_revision,
                expected_deployment_revision=None,
                contract=ctx.contract,
                migration=migration,
                permit_pre_intent_fence=False,
            )
        )
        request = ContractLifecycleRequest(
            schema="ergasterion.contract-lifecycle-request/v1",
            action=ContractLifecycleAction.ACTIVATE,
            expected_state_revision=registered.state.state_revision,
            expected_deployment_revision=None,
            contract=ctx.contract,
            migration=migration,
            permit_pre_intent_fence=True,
        )
        result = session.runtime.contract_lifecycle(request)
        activation = (
            ContractActivationState.ACTIVE
            if result.state.active_contract_digest == ctx.contract_digest
            else ContractActivationState.PENDING_BASELINE
        )
        return _envelope(
            "contract.activate",
            identity=ctx.contract.logical_identity,
            contract_digest=ctx.contract_digest,
            execution_plan_digest=ctx.plan_digest,
            runtime_manifest_digest=ctx.manifest.runtime_manifest_digest,
            result=ContractActivationResult(
                kind="contract_activation",
                migration=migration,
                activation_state=activation,
                candidate_contract_digest=ctx.contract_digest,
                active_contract_digest=result.state.active_contract_digest,
                fenced_attempt_ids=result.fenced_attempt_ids,
                state_revision=result.state.state_revision,
            ),
        )


def _deployment_record(ctx: _Context, candidate: str | None, active: str | None, revision: str) -> RuntimeDeployment:
    return RuntimeDeployment(
        logical_identity=ctx.contract.logical_identity,
        contract_digest=ctx.contract_digest,
        projection_target=ctx.layout.binding.projection_target,
        candidate_manifest_digest=candidate,
        active_manifest_digest=active,
        retired_manifest_digests=(),
        deployment_revision=revision,
    )


def _cmd_deployment_register(ctx: _Context) -> CommandEnvelope:
    with ctx.session() as session:
        current = _active_deployment(session, ctx)
        reject_store_relocation(_prior_binding_from_active(session, ctx), ctx.layout.binding)
        status = session.runtime.ports.state_store.status_query(ctx.contract.logical_identity)
        revision = current.deployment_revision if current is not None else "0"
        cursor = session.runtime.ports.projection_publisher.read_cursor(
            ctx.contract.logical_identity, ctx.layout.binding.projection_target,
        )
        incoming = _deployment_record(ctx, ctx.manifest.runtime_manifest_digest, current.active_manifest_digest if current else None, revision)
        readiness = _readiness(ctx, session, ctx.manifest.runtime_manifest_digest)
        request = DeploymentLifecycleRequest(
            schema="ergasterion.deployment-lifecycle-request/v1",
            action=DeploymentLifecycleAction.REGISTER,
            expected_state_revision=status.state.state_revision,
            expected_deployment_revision=revision,
            deployment=incoming,
            readiness=readiness,
            catchup_cursor=cursor,
            permit_pre_intent_fence=False,
        )
        result = session.runtime.deployment_lifecycle(request)
        persist_prior_binding(ctx.layout, ctx.layout.binding)
        return _envelope(
            "deployment.register",
            identity=ctx.contract.logical_identity,
            contract_digest=ctx.contract_digest,
            execution_plan_digest=ctx.plan_digest,
            runtime_manifest_digest=ctx.manifest.runtime_manifest_digest,
            result=DeploymentRegisteredResult(
                kind="deployment_registered",
                runtime_manifest_digest=ctx.manifest.runtime_manifest_digest,
                capability_digests=tuple(
                    getattr(ctx.layout.binding.ports, name).capability_digest
                    for name in ctx.layout.binding.ports.model_fields
                ),
                readiness_digest=readiness.readiness_digest,
                catchup_cursor=result.catchup_cursor,
            ),
        )


def _prior_binding_from_active(session: LocalRuntimeSession, ctx: _Context) -> RuntimeBinding:
    stored = load_prior_binding(ctx.layout)
    if stored is not None:
        return stored
    _active_deployment(session, ctx)
    return closed_local_binding(ctx.layout.binding)


def _cmd_deployment_activate(ctx: _Context) -> CommandEnvelope:
    if ctx.args.manifest_digest != ctx.manifest.runtime_manifest_digest:
        raise SettingsError(
            "superseded_deployment",
            "manifest-digest does not match the compiled binding",
            field_path="/manifest-digest",
        )
    with ctx.session() as session:
        reject_store_relocation(_prior_binding_from_active(session, ctx), ctx.layout.binding)
        current = _active_deployment(session, ctx)
        if current is None or current.candidate_manifest_digest != ctx.args.manifest_digest:
            raise PortError("superseded_deployment", "no matching candidate manifest is registered")
        status = session.runtime.ports.state_store.status_query(ctx.contract.logical_identity)
        cursor = session.runtime.ports.projection_publisher.read_cursor(
            ctx.contract.logical_identity, ctx.layout.binding.projection_target,
        )
        incoming = current.model_copy(update={"candidate_manifest_digest": ctx.args.manifest_digest})
        readiness = _readiness(ctx, session, ctx.args.manifest_digest)
        request = DeploymentLifecycleRequest(
            schema="ergasterion.deployment-lifecycle-request/v1",
            action=DeploymentLifecycleAction.ACTIVATE,
            expected_state_revision=status.state.state_revision,
            expected_deployment_revision=current.deployment_revision,
            deployment=incoming,
            readiness=readiness,
            catchup_cursor=cursor,
            permit_pre_intent_fence=True,
        )
        result = session.runtime.deployment_lifecycle(request)
        persist_prior_binding(ctx.layout, ctx.layout.binding)
        previous = current.active_manifest_digest or ctx.args.manifest_digest
        return _envelope(
            "deployment.activate",
            identity=ctx.contract.logical_identity,
            contract_digest=ctx.contract_digest,
            execution_plan_digest=ctx.plan_digest,
            runtime_manifest_digest=result.deployment.active_manifest_digest,
            result=DeploymentActivationResult(
                kind="deployment_activation",
                deployment=result.deployment,
                previous_manifest_digest=previous,
                fenced_attempt_ids=result.fenced_attempt_ids,
                active_cursor=result.catchup_cursor,
            ),
        )


def _project_event(
    session: LocalRuntimeSession, ctx: _Context, event_type: LifecycleEventType, payload,
    *, ordinal: str, attempt_id: str | None = None,
) -> None:
    state = session.runtime.ports.state_store.status_query(ctx.contract.logical_identity).state
    payload_digest = canonical_digest(payload.model_dump(mode="json", by_alias=True))
    body: dict[str, Any] = {
        "event_id": canonical_digest({
            "event_type": event_type.value, "state_revision": state.state_revision,
            "event_ordinal": ordinal, "payload": payload_digest, "attempt": attempt_id,
        }),
        "event_type": event_type,
        "logical_identity": ctx.contract.logical_identity,
        "state_revision": state.state_revision,
        "event_ordinal": ordinal,
        "execution_plan_digest": ctx.plan_digest,
        "runtime_manifest_digest": ctx.manifest.runtime_manifest_digest,
        "payload": payload,
        "payload_digest": payload_digest,
        "created_at": session.now(),
    }
    if attempt_id is not None:
        body["attempt_id"] = attempt_id
    session.runtime.ports.lifecycle_sink.project_events(
        LifecycleEventBatch(events=(LifecycleEvent(**body),), max_items=1, bytes_supplied="0")
    )


def _emit_event(
    session: LocalRuntimeSession, ctx: _Context, event_type: LifecycleEventType, payload, *,
    ordinal: str, attempt_id: str | None = None,
) -> None:
    _project_event(session, ctx, event_type, payload, ordinal=ordinal, attempt_id=attempt_id)


def _cmd_ingest_file(ctx: _Context) -> CommandEnvelope:
    with ctx.session() as session:
        current = _active_deployment(session, ctx)
        if current is None or current.active_manifest_digest is None:
            raise PortError("superseded_deployment", "no active runtime deployment")
        managed = session.ports.source_connector.open_managed(ctx.args.manifest, ctx.args.payload)
        payload_bytes = Path(ctx.args.payload).read_bytes()
        session.ports.raw_store.register_payload(managed.payload_handle, payload_bytes)
        session.ports.raw_store.register_manifest_bytes(managed.payload_handle, Path(ctx.args.manifest).read_bytes())
        status = session.runtime.ports.state_store.status_query(ctx.contract.logical_identity)
        existing = None
        claim_digest = canonical_digest(managed.manifest.model_dump(mode="json", by_alias=True))
        page = session.runtime.ports.state_store.attempts(
            AttemptQuery(
                logical_identity=ctx.contract.logical_identity, claim_digest=claim_digest,
                nonterminal_only=False, after_attempt_id=None, max_items=16,
            )
        )
        if page.attempts:
            existing = page.attempts[0]
        readiness = _readiness(ctx, session, ctx.manifest.runtime_manifest_digest)
        if existing is None:
            admit_execution(
                session, ctx.layout.binding, current, readiness, ctx.plan_digest,
                ctx.manifest.runtime_manifest_digest,
            )
        attempt, state = session.runtime.submit_managed(
            status.state, ctx.contract, ctx.plan_digest, ctx.manifest.runtime_manifest_digest,
            canonical_digest({"run": managed.manifest.delivery_id}), managed,
        )
        if existing is not None:
            return _envelope(
                "ingest.file",
                status=CommandStatus.NOOP,
                identity=ctx.contract.logical_identity,
                contract_digest=ctx.contract_digest,
                execution_plan_digest=ctx.plan_digest,
                runtime_manifest_digest=ctx.manifest.runtime_manifest_digest,
                result=IngestionResult(
                    kind="ingestion", attempt=attempt, visibility=None, publication=None,
                    projection_confirmation=None, retry_directive=None,
                ),
            )
        receipt = session.ports.raw_store.preserve(managed)
        visibility = DeliveryVisibilityIdentity(
            epoch=state.visibility_epoch, kind="delivery", id=digest_token(attempt.attempt_id, "delivery"),
        )
        evaluation_id = canonical_digest({"attempt": attempt.attempt_id, "evaluation": "land"})
        ruleset_digest = canonical_digest({"contract": ctx.contract_digest})
        attempt, state, materialized, validation = session.runtime.land_and_validate(
            attempt, state, ctx.contract, receipt, visibility, evaluation_id, ruleset_digest,
        )
        ingested = session.runtime.publish(
            attempt, state, ctx.contract, materialized, validation, visibility, receipt, readiness,
        )
        state = session.runtime.ports.state_store.status_query(ctx.contract.logical_identity).state
        ordinal = state.state_revision
        _emit_event(
            session, ctx, LifecycleEventType.BRONZE_RECEIPT,
            ReceiptLifecyclePayload(kind="bronze.receipt", receipt=receipt),
            ordinal=ordinal, attempt_id=attempt.attempt_id,
        )
        handoff = quality_handoff(
            logical_identity=ctx.contract.logical_identity,
            run_id=attempt.run_id, attempt_id=attempt.attempt_id, evaluation_id=evaluation_id,
            ruleset_digest=ruleset_digest, validation_result_digest=validation.validation_result_digest,
            accepted_content_digest=materialized.accepted_content_digest,
            disposition_ref=materialized.disposition_ref, accepted_ref=materialized.accepted_ref,
            framed_count=validation.framed_count, accepted_count=validation.accepted_count,
            error_count=validation.error_count, warning_count=validation.warning_count,
            quarantined_count=validation.quarantined_count, batch_findings=validation.batch_findings,
            error_numerator=validation.error_numerator, error_denominator=validation.error_denominator,
            publication_decision=validation.publication_decision,
        )
        _emit_event(
            session, ctx, LifecycleEventType.BRONZE_QUALITY,
            QualityLifecyclePayload(kind="bronze.quality", validation=handoff),
            ordinal=ordinal, attempt_id=attempt.attempt_id,
        )
        lineage = build_lineage_descriptor(ctx.contract, ctx.plan_digest)
        run_lineage = build_run_lineage(
            contract=ctx.contract, run_id=attempt.run_id, attempt_id=attempt.attempt_id,
            delivery_id=attempt.delivery_id, reprocessing_id=None, remediation_evaluation_id=None,
            transport_payload_digest=receipt.payload.content_id.split(":", 1)[-1],
            delivery_claim_digest=attempt.claim_digest, ruleset_digest=ruleset_digest,
            validation_result_digest=validation.validation_result_digest,
            accepted_count=validation.accepted_count, quarantined_count=validation.quarantined_count,
            execution_plan_digest=ctx.plan_digest, runtime_manifest_digest=ctx.manifest.runtime_manifest_digest,
            landing_ref=materialized.prepared.candidate_ref,
            confirmation=None,
            result=ProcessingOutcome.BLOCKED if ingested.retry_directive else ProcessingOutcome.IN_PROGRESS,
            committed_at=ingested.projection_confirmation.committed_at if ingested.projection_confirmation else None,
        )
        _emit_event(
            session, ctx, LifecycleEventType.BRONZE_LINEAGE,
            LineageLifecyclePayload(kind="bronze.lineage", lineage=lineage, run_lineage=run_lineage),
            ordinal=ordinal, attempt_id=attempt.attempt_id,
        )
        metadata = build_product_metadata(
            ctx.contract, latest_stream_status_ref="bronze.stream_status",
            latest_publication_ref=ingested.projection_confirmation.projection_intent_digest if ingested.projection_confirmation else None,
        )
        _emit_event(
            session, ctx, LifecycleEventType.BRONZE_METADATA,
            MetadataLifecyclePayload(kind="bronze.metadata", metadata=metadata),
            ordinal=ordinal, attempt_id=attempt.attempt_id,
        )
        status_out = CommandStatus.RETRYABLE if ingested.retry_directive and not ingested.retry_directive.exhausted else CommandStatus.OK
        if ingested.retry_directive and ingested.retry_directive.exhausted:
            status_out = CommandStatus.FAILED
        return _envelope(
            "ingest.file",
            status=status_out,
            identity=ctx.contract.logical_identity,
            contract_digest=ctx.contract_digest,
            execution_plan_digest=ctx.plan_digest,
            runtime_manifest_digest=ctx.manifest.runtime_manifest_digest,
            result=ingested,
        )


def _stage_heartbeat(session: LocalRuntimeSession, ctx: _Context, now: str) -> None:
    status = session.runtime.ports.state_store.status_query(ctx.contract.logical_identity)
    if int(status.incomplete_outbox_count) > 0:
        return
    cursor = session.runtime.ports.projection_publisher.read_cursor(
        ctx.contract.logical_identity, ctx.layout.binding.projection_target,
    )
    if int(status.state.required_projection_revision) > int(cursor.projection_revision):
        return
    payload = HeartbeatProjectionPayload(
        kind="heartbeat", heartbeat_at=now, evaluated_through_at=now,
        prior_committed_at=status.state.last_committed_at,
    )
    intent = session.runtime._build_intent(
        status.state, ctx.contract_digest, ProjectionIntentKind.HEARTBEAT, payload,
        ctx.plan_digest, ctx.manifest.runtime_manifest_digest,
    )
    outbox_id = canonical_digest({"intent": intent.projection_intent_digest})
    staged = session.runtime.ports.state_store.state_transaction(
        StateOutboxTransaction(
            expected_state_revision=status.state.state_revision,
            next_state=status.state.model_copy(update={"required_projection_revision": intent.projection_revision}),
            attempt_updates=(), deployment_update=None, projection_confirmation=None,
            enqueue=(OutboxEnqueue(
                outbox_id=outbox_id, payload=ProjectionOutboxPayload(entry_kind="projection", intent=intent),
                payload_digest=intent.projection_intent_digest, next_not_before=now,
            ),),
            complete=(),
        )
    )
    session.runtime._apply_outbox_intent(
        None, staged, outbox_id, intent, dispatch_ordinal=1,
        max_attempts=int(ctx.contract.delivery.retry.max_attempts),
    )


def _due_result(
    *,
    evaluated_through_at: str,
    transitions_applied: str,
    state_revision: str,
    projection_revisions: tuple[str, ...],
    more_due: bool,
    continuation_after: str | None,
) -> DueEvaluationResult:
    body: dict[str, Any] = {
        "kind": "due_evaluation",
        "evaluated_through_at": evaluated_through_at,
        "transitions_applied": transitions_applied,
        "state_revision": state_revision,
        "projection_revisions": projection_revisions,
        "more_due": more_due,
    }
    if continuation_after is not None:
        body["continuation_after"] = continuation_after
    return DueEvaluationResult(**body)


def _cmd_ingest_due(ctx: _Context) -> CommandEnvelope:
    dry = bool(ctx.args.dry_run)
    at = getattr(ctx.args, "at", None)
    if at and not dry:
        raise SettingsError("invalid_usage", "--at is valid only with --dry-run")
    with ctx.session() as session:
        now = at if (dry and at) else session.now()
        status = session.runtime.ports.state_store.status_query(ctx.contract.logical_identity)
        since = session.runtime.last_evaluated_occurrence(ctx.contract.logical_identity)
        max_occ = int(ctx.layout.binding.scheduler.max_due_transitions_per_call)
        boundaries = scheduled_occurrences(ctx.contract, None if dry and since is None else since, now, max_occ)
        if dry:
            more = len(boundaries) >= max_occ
            return _envelope(
                "ingest.due",
                identity=ctx.contract.logical_identity,
                contract_digest=ctx.contract_digest,
                execution_plan_digest=ctx.plan_digest,
                runtime_manifest_digest=ctx.manifest.runtime_manifest_digest,
                result=_due_result(
                    evaluated_through_at=now, transitions_applied="0",
                    state_revision=status.state.state_revision, projection_revisions=(),
                    more_due=more, continuation_after=boundaries[-1] if more and boundaries else None,
                ),
            )
        applied = session.runtime.run_due(
            ctx.contract.logical_identity, now, int(ctx.contract.delivery.retry.max_attempts),
        )
        _stage_heartbeat(session, ctx, now)
        status = session.runtime.ports.state_store.status_query(ctx.contract.logical_identity)
        since = session.runtime.last_evaluated_occurrence(ctx.contract.logical_identity)
        state, projected = session.runtime.run_scheduled(ctx.contract, status.state, now, since, max_occ)
        revisions = tuple(
            item.projection_confirmation.projection_revision
            for item in applied if item.projection_confirmation is not None
        ) + tuple(str(index) for index, _ in enumerate(projected, start=1))
        more = len(projected) >= max_occ
        return _envelope(
            "ingest.due",
            identity=ctx.contract.logical_identity,
            contract_digest=ctx.contract_digest,
            execution_plan_digest=ctx.plan_digest,
            runtime_manifest_digest=ctx.manifest.runtime_manifest_digest,
            result=_due_result(
                evaluated_through_at=now,
                transitions_applied=str(len(projected) + len(applied)),
                state_revision=state.state_revision, projection_revisions=revisions,
                more_due=more, continuation_after=projected[-1] if more and projected else None,
            ),
        )


def _cmd_reconcile(ctx: _Context) -> CommandEnvelope:
    target = getattr(ctx.args, "target", None) or ctx.layout.binding.projection_target
    with ctx.session() as session:
        applied = session.runtime.run_due(
            ctx.contract.logical_identity, session.now(), int(ctx.contract.delivery.retry.max_attempts),
        )
        status = session.runtime.ports.state_store.status_query(ctx.contract.logical_identity)
        cursor = session.runtime.ports.projection_publisher.read_cursor(ctx.contract.logical_identity, target)
        log = session.runtime.ports.state_store.projection_log(
            ctx.contract.logical_identity, "0", 1000, "1000000",
        )
        confirmed = {
            item.projection_intent_digest
            for item in session.runtime.ports.state_store.projection_confirmation_log(
                ctx.contract.logical_identity, "0", 1000, "1000000",
            ).confirmations
        }
        confirmations = []
        remaining = []
        actions = ["run_due"]
        for intent in log.intents:
            if intent.projection_intent_digest in confirmed:
                continue
            outbox_id = canonical_digest({"intent": intent.projection_intent_digest})
            latest = status.latest_attempt
            result = session.runtime._apply_outbox_intent(
                latest, status.state, outbox_id, intent, dispatch_ordinal=1,
                max_attempts=int(ctx.contract.delivery.retry.max_attempts),
            )
            actions.append("resume_blocked")
            if result.projection_confirmation is None:
                remaining.append(latest.attempt_id if latest else intent.projection_intent_digest)
            else:
                confirmations.append(result.projection_confirmation)
            status = session.runtime.ports.state_store.status_query(ctx.contract.logical_identity)
        cursor = session.runtime.ports.projection_publisher.read_cursor(ctx.contract.logical_identity, target)
        for item in applied:
            if item.projection_confirmation is not None:
                confirmations.append(item.projection_confirmation)
            elif item.attempt is not None:
                remaining.append(item.attempt.attempt_id)
        return _envelope(
            "reconcile",
            identity=ctx.contract.logical_identity,
            contract_digest=ctx.contract_digest,
            execution_plan_digest=ctx.plan_digest,
            runtime_manifest_digest=ctx.manifest.runtime_manifest_digest,
            result=ReconciliationResult(
                kind="reconciliation",
                target_cursors=(cursor,),
                actions=tuple(actions),
                remaining_blocks=tuple(remaining),
                confirmations=tuple(confirmations),
            ),
        )


def _cmd_backup(ctx: _Context) -> CommandEnvelope:
    action = BackupAction(ctx.args.action)
    if action is BackupAction.CREATE:
        if not ctx.args.destination:
            raise SettingsError("invalid_usage", "local-backup create requires --destination PATH")
        with ctx.session() as session:
            manifest = create_backup(
                session, ctx.layout, Path(ctx.args.destination),
                runtime_binding_digest=ctx.binding_digest,
                runtime_manifest_digest=ctx.manifest.runtime_manifest_digest,
            )
        return _envelope(
            "local-backup.create",
            identity=ctx.contract.logical_identity,
            contract_digest=ctx.contract_digest,
            execution_plan_digest=ctx.plan_digest,
            runtime_manifest_digest=ctx.manifest.runtime_manifest_digest,
            result=LocalBackupResult(
                kind="local_backup", action=action,
                manifest_path=str(Path(ctx.args.destination).resolve() / "backup-manifest.json"),
                manifest=manifest, verification_digest=manifest.manifest_digest, reconciliation=None,
            ),
        )
    if not ctx.args.manifest:
        raise SettingsError("invalid_usage", "local-backup restore requires --manifest PATH")
    restored = restore_backup(ctx.layout, Path(ctx.args.manifest))
    recon = _cmd_reconcile(ctx)
    return _envelope(
        "local-backup.restore",
        identity=ctx.contract.logical_identity,
        contract_digest=ctx.contract_digest,
        execution_plan_digest=ctx.plan_digest,
        runtime_manifest_digest=ctx.manifest.runtime_manifest_digest,
        result=LocalBackupResult(
            kind="local_backup", action=action, manifest_path=str(Path(ctx.args.manifest).resolve()),
            manifest=restored, verification_digest=restored.manifest_digest,
            reconciliation=recon.result if recon.result and recon.result.kind == "reconciliation" else None,
        ),
    )


def _cmd_status(ctx: _Context) -> CommandEnvelope:
    with ctx.session() as session:
        operational = session.runtime.ports.state_store.status_query(ctx.contract.logical_identity)
        cursor = session.runtime.ports.projection_publisher.read_cursor(
            ctx.contract.logical_identity, ctx.layout.binding.projection_target,
        )
        now = session.now()
        boundary = operational.latest_attempt.scheduled_boundary_at if operational.latest_attempt else now
        timeliness = timeliness_state(ctx.contract, boundary, operational.state.last_committed_at, now)
        lag = str(max(0, int(operational.state.required_projection_revision) - int(cursor.projection_revision)))
        stream = StreamStatus(
            logical_identity=ctx.contract.logical_identity,
            contract_digest=ctx.contract_digest,
            projection_target=ctx.layout.binding.projection_target,
            projection_revision=cursor.projection_revision,
            projected_at=now,
            scheduled_boundary_at=boundary,
            processing=operational.processing,
            timeliness=timeliness,
            latest_attempt=operational.latest_attempt,
            committed_at=operational.state.last_committed_at,
            accepted_progress=operational.state.accepted_progress,
            latest_snapshot_visibility=operational.state.last_committed_visibility,
            snapshot_reconciliation=SnapshotReconciliationStatus.NOT_APPLICABLE,
            heartbeat_at=now,
            evaluated_through_at=now,
        )
        return _envelope(
            "status",
            identity=ctx.contract.logical_identity,
            contract_digest=ctx.contract_digest,
            execution_plan_digest=ctx.plan_digest,
            runtime_manifest_digest=ctx.manifest.runtime_manifest_digest,
            result=StreamStatusResult(
                kind="stream_status", stream_status=stream, operational_status=operational,
                target_cursor=cursor, projection_lag=lag,
            ),
        )


def _cmd_inspect(ctx: _Context) -> CommandEnvelope:
    delivery_id = getattr(ctx.args, "delivery_id", None)
    with ctx.session() as session:
        attempts = session.runtime.ports.state_store.attempts(
            AttemptQuery(
                logical_identity=ctx.contract.logical_identity, claim_digest=None,
                nonterminal_only=False, after_attempt_id=None, max_items=1000,
            )
        ).attempts
        claim_digests = {
            item.claim_digest for item in attempts if delivery_id is None or item.delivery_id == delivery_id
        }
        attempt_ids = {
            item.attempt_id for item in attempts if delivery_id is None or item.delivery_id == delivery_id
        }
        metadata = build_product_metadata(
            ctx.contract, latest_stream_status_ref="bronze.stream_status", latest_publication_ref=None,
        )
        items = [
            ContractEvidenceItem(kind="contract", contract=ctx.contract),
            SchemaEvidenceItem(kind="schema", metadata=metadata),
        ]
        for kind in (EvidenceKind.RECEIPT, EvidenceKind.QUALITY, EvidenceKind.LINEAGE, EvidenceKind.METADATA):
            page = session.runtime.ports.lifecycle_sink.evidence_query(
                EvidenceQuery(
                    logical_identity=ctx.contract.logical_identity, evidence_kind=kind,
                    immutable_id=None, authorization_context_ref=AUTHORIZATION_CONTEXT,
                    after_cursor=None, max_items=200, max_bytes="1000000",
                )
            )
            for item in page.items:
                if kind is EvidenceKind.METADATA:
                    items.append(item)
                    continue
                if delivery_id is None:
                    items.append(item)
                    continue
                if kind is EvidenceKind.RECEIPT and item.receipt.claim_digest in claim_digests:
                    items.append(item)
                elif kind is EvidenceKind.QUALITY and item.validation.attempt_id in attempt_ids:
                    items.append(item)
                elif kind is EvidenceKind.LINEAGE and (
                    item.run_lineage.delivery_id == delivery_id or item.run_lineage.attempt_id in attempt_ids
                ):
                    items.append(item)
        evidence = EvidencePage(items=tuple(items), next_cursor=None, bytes_returned=str(len(items)), more=False)
        return _envelope(
            "inspect",
            identity=ctx.contract.logical_identity,
            contract_digest=ctx.contract_digest,
            execution_plan_digest=ctx.plan_digest,
            runtime_manifest_digest=ctx.manifest.runtime_manifest_digest,
            result=InspectionResult(kind="inspection", evidence=evidence),
        )


def _revalidate_quarantine(session: LocalRuntimeSession, ctx: _Context, items: list) -> RemediationActionStatus:
    if not items:
        raise PortError("not_found", "no quarantined disposition is available to revalidate")
    selected = items
    if ctx.args.disposition_id:
        selected = [item for item in items if item.disposition.disposition_id == ctx.args.disposition_id]
        if not selected:
            raise PortError("not_found", ctx.args.disposition_id)
    current_ruleset = canonical_digest({"contract": ctx.contract_digest})
    requested = getattr(ctx.args, "ruleset_digest", None)
    if requested is not None and requested != current_ruleset:
        raise SettingsError(
            "invalid_usage",
            "--ruleset-digest does not match the active contract ruleset",
            field_path="/ruleset-digest",
        )
    statuses: list[RemediationActionStatus] = []
    seen_refs: set[str] = set()
    budget = int(ctx.layout.binding.runtime_resources.validation_memory_bytes)
    for item in selected:
        candidate_ref = item.disposition.raw_ref
        if candidate_ref in seen_refs:
            continue
        seen_refs.add(candidate_ref)
        frames: list = []
        after = None
        while True:
            native = session.runtime.ports.landing_adapter.source_native_query(
                SourceNativeQuery(
                    logical_identity=ctx.contract.logical_identity,
                    candidate_ref=candidate_ref,
                    disposition_ref=None,
                    authorization_context_ref=AUTHORIZATION_CONTEXT,
                    after_frame_sequence=after,
                    max_items=200,
                    max_bytes="1000000",
                )
            )
            frames.extend(row.frame for row in native.items)
            if not native.more or native.next_frame_sequence is None:
                break
            after = native.next_frame_sequence
        if not frames:
            raise PortError("not_found", candidate_ref)
        evaluation_id = canonical_digest({"revalidate": candidate_ref, "ruleset": current_ruleset})
        validate_frames(
            ctx.contract, frames, claim_digest=item.disposition.claim_digest,
            delivery_id=item.disposition.delivery_id, evaluation_id=evaluation_id,
            memory_budget_bytes=budget, scratch_store=session.ports.scratch_store,
            attempt_id=None, batch_findings=(),
        )
        if current_ruleset == item.disposition.ruleset_digest:
            statuses.append(RemediationActionStatus.UNCHANGED_FINDING)
        else:
            statuses.append(RemediationActionStatus.REVALIDATED)
    if any(status is RemediationActionStatus.REVALIDATED for status in statuses):
        return RemediationActionStatus.REVALIDATED
    return RemediationActionStatus.UNCHANGED_FINDING


def _cmd_quarantine(ctx: _Context) -> CommandEnvelope:
    if getattr(ctx.args, "row_level", False):
        raise SettingsError("invalid_usage", "row-level quarantine mode is not a v1 operation")
    action = QuarantineAction(ctx.args.action)
    with ctx.session() as session:
        query = DispositionQuery(
            logical_identity=ctx.contract.logical_identity,
            disposition_id=ctx.args.disposition_id,
            authorization_context_ref=AUTHORIZATION_CONTEXT,
            snapshot_token=None, after_cursor=None, max_items=50, max_bytes="1000000",
        )
        dispositions = session.runtime.ports.landing_adapter.disposition_query(query)
        items = []
        for disposition in dispositions.items:
            decisions = session.runtime.ports.remediation_repository.decision_query(
                RemediationDecisionQuery(
                    logical_identity=ctx.contract.logical_identity,
                    disposition_id=disposition.disposition_id,
                    authorization_context_ref=AUTHORIZATION_CONTEXT,
                    snapshot_token=None, after_cursor=None, max_items=50, max_bytes="1000000",
                )
            )
            items.append(QuarantineItem(disposition=disposition, decision_page=decisions))
        snapshot = QuarantineSnapshot(
            query_digest=canonical_digest({"quarantine": ctx.contract_digest}),
            disposition_snapshot_token=dispositions.snapshot_token,
            remediation_snapshot_token=items[0].decision_page.snapshot_token if items else "0",
        )
        page = QuarantinePage(
            items=tuple(items), snapshot=snapshot, next_cursor=None,
            bytes_returned=dispositions.bytes_returned, more=dispositions.more,
        )
        decision = None
        status = RemediationActionStatus.LISTED
        if action is QuarantineAction.REVALIDATE:
            status = _revalidate_quarantine(session, ctx, items)
        elif action is QuarantineAction.RELEASE:
            if not ctx.args.disposition_id:
                raise SettingsError("invalid_usage", "quarantine release requires --disposition-id")
            target = next((item for item in items if item.disposition.disposition_id == ctx.args.disposition_id), None)
            if target is None:
                raise PortError("not_found", ctx.args.disposition_id)
            operational = session.runtime.ports.state_store.status_query(ctx.contract.logical_identity)
            attempts = session.runtime.ports.state_store.attempts(
                AttemptQuery(
                    logical_identity=ctx.contract.logical_identity,
                    claim_digest=target.disposition.claim_digest,
                    nonterminal_only=False, after_attempt_id=None, max_items=16,
                )
            )
            attempt = attempts.attempts[0] if attempts.attempts else operational.latest_attempt
            if attempt is None:
                raise PortError("not_found", "no attempt available for release")
            evaluated = next(
                (item for item in target.decision_page.items if item.release is None),
                target.decision_page.items[0] if target.decision_page.items else None,
            )
            evaluation = evaluated.evaluation if evaluated is not None else None
            if evaluation is None:
                raise PortError("not_found", "no evaluation is recorded for this disposition")
            decision = session.runtime.release_quarantine(
                attempt, operational.state, ctx.contract, evaluation,
                (target.disposition.raw_locator,),
                canonical_digest({"release": target.disposition.disposition_id}),
                None,
                raw_ref=target.disposition.raw_ref,
            )
            status = RemediationActionStatus.RELEASED
        return _envelope(
            "quarantine",
            identity=ctx.contract.logical_identity,
            contract_digest=ctx.contract_digest,
            execution_plan_digest=ctx.plan_digest,
            runtime_manifest_digest=ctx.manifest.runtime_manifest_digest,
            result=QuarantineResult(
                kind="quarantine", action=action, status=status, evidence=page, decision=decision,
            ),
        )


def dispatch(args: argparse.Namespace) -> CommandEnvelope:
    ctx = _Context(args)
    command = args.command
    if command == "plan":
        return _cmd_plan(ctx)
    if command == "contract":
        if args.contract_action == "register":
            return _cmd_contract_register(ctx)
        return _cmd_contract_activate(ctx)
    if command == "deployment":
        if args.deployment_action == "register":
            return _cmd_deployment_register(ctx)
        return _cmd_deployment_activate(ctx)
    if command == "ingest":
        if args.ingest_action == "file":
            return _cmd_ingest_file(ctx)
        return _cmd_ingest_due(ctx)
    if command == "reconcile":
        return _cmd_reconcile(ctx)
    if command == "local-backup":
        return _cmd_backup(ctx)
    if command == "status":
        return _cmd_status(ctx)
    if command == "inspect":
        return _cmd_inspect(ctx)
    if command == "quarantine":
        return _cmd_quarantine(ctx)
    raise SettingsError("invalid_usage", f"unknown command {command!r}")


def main(argv: list[str] | None = None) -> int:
    parser = _parser()
    try:
        args = parser.parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)
    as_json = bool(getattr(args, "json", False))
    command = args.command
    if command == "contract":
        command = f"contract.{args.contract_action}"
    elif command == "deployment":
        command = f"deployment.{args.deployment_action}"
    elif command == "ingest":
        command = f"ingest.{args.ingest_action}"
    elif command == "local-backup":
        command = f"local-backup.{args.action}"
    try:
        envelope = dispatch(args)
        return _emit(envelope, as_json=as_json)
    except (SettingsError, PortError, BackupError, ValueError) as exc:
        return _fail(command, exc, as_json=as_json)


if __name__ == "__main__":
    raise SystemExit(main())
