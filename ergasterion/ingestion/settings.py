"""Local runtime settings: binding YAML, endpoint layout and resource envelope.

A ``RuntimeBinding`` carries only opaque non-secret endpoint references. This
module maps those tokens onto the local runtime root (SQLite, DuckDB, raw
objects, scratch) so operator commands never embed machine paths in product
identity or contract digests.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import yaml
from pydantic import ValidationError

from ergasterion.framework.runtime_binding import RuntimeBinding

DEFAULT_RUNTIME_ROOT = Path("runtime") / "data"
LOCAL_ENVIRONMENT = "local"
SYNTHETIC_PROTECTION_PROFILE = "synthetic-local"
SYNTHETIC_CLASSIFICATION = "synthetic"
SYNTHETIC_ACCESS_POLICY = "local-process-user"
SYNTHETIC_RETENTION_POLICY = "local-ephemeral"
LOCAL_IMPLEMENTATION_VERSION = "1.0.0"
MISSING_EXTRA_REMEDY = "pip install ergasterion-factory[local-ingestion]"
SYNTHETIC_HMAC_KEY_ID = "synthetic-local-hmac"
SYNTHETIC_HMAC_SECRET = b"ergasterion-synthetic-local-hmac-v1"
AUTHORIZATION_CONTEXT = "local-process-user"

# Closed local adapter identities. Endpoint tokens are opaque; paths live here.
LOCAL_ADAPTER_IDS: dict[str, str] = {
    "source_connector": "file_source",
    "raw_store": "local_raw_store",
    "scratch_store": "local_scratch_store",
    "state_store": "sqlite_state_store",
    "landing_adapter": "duckdb_landing_adapter",
    "remediation_repository": "duckdb_remediation_repository",
    "projection_publisher": "duckdb_projection_publisher",
    "lifecycle_sink": "duckdb_lifecycle_sink",
    "key_resolver": "sqlite_key_resolver",
}

# Durable stores that v1 cannot relocate. Changing these endpoint tokens fails
# ``unsupported_store_migration``.
IMMUTABLE_STORE_PORTS: tuple[str, ...] = (
    "raw_store",
    "state_store",
    "landing_adapter",
    "remediation_repository",
)

# Closed local non-secret endpoint tokens. Durable-store ports must stay on this set.
LOCAL_ENDPOINT_REFS: dict[str, str] = {
    "source_connector": "local-file",
    "raw_store": "local-raw",
    "scratch_store": "local-scratch",
    "state_store": "local-sqlite",
    "landing_adapter": "local-duckdb",
    "remediation_repository": "local-duckdb",
    "projection_publisher": "local-duckdb",
    "lifecycle_sink": "local-duckdb",
    "key_resolver": "local-hmac",
}
CLOSED_PROJECTION_TARGET = "bronze"
PRIOR_BINDING_FILENAME = "activated-runtime-binding.json"

DEFAULT_ENDPOINT_PATHS: dict[str, str] = {
    "local-raw": "raw",
    "local-scratch": "scratch",
    "local-sqlite": "ergasterion.sqlite",
    "local-duckdb": "ergasterion.duckdb",
}

# Connector / key-resolver tokens are process-local; they have no runtime-root path.
PATHLESS_ENDPOINTS: frozenset[str] = frozenset({"local-file", "local-file-relocated", "local-hmac"})


class SettingsError(ValueError):
    """Usage or config failure before any runtime mutation."""

    def __init__(self, code: str, message: str, *, field_path: str | None = None) -> None:
        super().__init__(message)
        self.code = code
        self.field_path = field_path


@dataclass(frozen=True)
class LocalLayout:
    """Resolved local filesystem layout for one binding."""

    project_dir: Path
    runtime_root: Path
    binding_path: Path
    binding: RuntimeBinding
    sqlite_path: Path
    duckdb_path: Path
    raw_root: Path
    scratch_root: Path


def load_runtime_binding(path: Path) -> RuntimeBinding:
    if not path.is_file():
        raise SettingsError("invalid_config", f"runtime binding not found: {path}")
    try:
        data = yaml.safe_load(path.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError) as exc:
        raise SettingsError("invalid_config", f"runtime binding is not readable YAML: {exc}") from exc
    if not isinstance(data, dict):
        raise SettingsError("invalid_config", "runtime binding must be a YAML mapping")
    try:
        return RuntimeBinding.model_validate(data)
    except ValidationError as exc:
        raise SettingsError("invalid_config", f"runtime binding is not a closed RuntimeBinding: {exc}") from exc


def assert_environment(binding: RuntimeBinding, environment: str) -> None:
    if binding.environment != environment:
        raise SettingsError(
            "invalid_config",
            f"RuntimeBinding.environment {binding.environment!r} does not match "
            f"--environment {environment!r}",
            field_path="/environment",
        )


def assert_local_policy(binding: RuntimeBinding) -> None:
    if binding.protection_profile != SYNTHETIC_PROTECTION_PROFILE:
        raise SettingsError(
            "production_policy_adapter_required",
            "this campaign admits only protection_profile synthetic-local",
            field_path="/protection_profile",
        )


def runtime_root_for(project_dir: Path) -> Path:
    return (project_dir / DEFAULT_RUNTIME_ROOT).resolve()


def _endpoint_path(runtime_root: Path, endpoint_ref: str) -> Path | None:
    if endpoint_ref in PATHLESS_ENDPOINTS:
        return None
    relative = DEFAULT_ENDPOINT_PATHS.get(endpoint_ref)
    if relative is None:
        raise SettingsError(
            "invalid_config",
            f"endpoint_ref {endpoint_ref!r} is not a known local non-secret reference",
        )
    return runtime_root / relative


def resolve_layout(
    *,
    project_dir: Path,
    binding_path: Path,
    environment: str,
) -> LocalLayout:
    project_dir = project_dir.resolve()
    binding_path = binding_path if binding_path.is_absolute() else project_dir / binding_path
    binding = load_runtime_binding(binding_path)
    assert_environment(binding, environment)
    assert_local_policy(binding)
    root = runtime_root_for(project_dir)
    sqlite = _endpoint_path(root, binding.ports.state_store.endpoint_ref)
    duckdb = _endpoint_path(root, binding.ports.landing_adapter.endpoint_ref)
    raw = _endpoint_path(root, binding.ports.raw_store.endpoint_ref)
    scratch = _endpoint_path(root, binding.ports.scratch_store.endpoint_ref)
    if sqlite is None or duckdb is None or raw is None or scratch is None:
        raise SettingsError("invalid_config", "state, landing, raw and scratch ports require path endpoints")
    projection = _endpoint_path(root, binding.ports.projection_publisher.endpoint_ref)
    remediation = _endpoint_path(root, binding.ports.remediation_repository.endpoint_ref)
    lifecycle = _endpoint_path(root, binding.ports.lifecycle_sink.endpoint_ref)
    for other in (projection, remediation, lifecycle):
        if other is not None and other != duckdb:
            raise SettingsError(
                "unsupported_store_migration",
                "local landing, remediation, projection and lifecycle share one DuckDB file",
            )
    key_path = _endpoint_path(root, binding.ports.key_resolver.endpoint_ref)
    if key_path is not None and key_path != sqlite:
        raise SettingsError(
            "unsupported_store_migration",
            "local state store and key resolver share one SQLite file",
        )
    return LocalLayout(
        project_dir=project_dir,
        runtime_root=root,
        binding_path=binding_path,
        binding=binding,
        sqlite_path=sqlite,
        duckdb_path=duckdb,
        raw_root=raw,
        scratch_root=scratch,
    )


def closed_local_binding(candidate: RuntimeBinding) -> RuntimeBinding:
    """Candidate with durable-store tokens and projection_target reset to the closed local set."""

    port_updates = {}
    for name in IMMUTABLE_STORE_PORTS:
        port = getattr(candidate.ports, name)
        expected = LOCAL_ENDPOINT_REFS[name]
        if port.endpoint_ref != expected:
            port_updates[name] = port.model_copy(update={"endpoint_ref": expected})
    ports = candidate.ports.model_copy(update=port_updates) if port_updates else candidate.ports
    return candidate.model_copy(update={"ports": ports, "projection_target": CLOSED_PROJECTION_TARGET})


def prior_binding_path(layout: LocalLayout) -> Path:
    return layout.runtime_root / PRIOR_BINDING_FILENAME


def load_prior_binding(layout: LocalLayout) -> RuntimeBinding | None:
    path = prior_binding_path(layout)
    if not path.is_file():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return RuntimeBinding.model_validate(data)
    except (OSError, json.JSONDecodeError, ValidationError):
        return None


def persist_prior_binding(layout: LocalLayout, binding: RuntimeBinding) -> None:
    layout.runtime_root.mkdir(parents=True, exist_ok=True)
    prior_binding_path(layout).write_text(
        json.dumps(binding.model_dump(mode="json", by_alias=True), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def reject_store_relocation(active: RuntimeBinding, candidate: RuntimeBinding) -> None:
    for field_name in IMMUTABLE_STORE_PORTS:
        prior = getattr(active.ports, field_name).endpoint_ref
        incoming = getattr(candidate.ports, field_name).endpoint_ref
        if prior != incoming:
            raise SettingsError(
                "unsupported_store_migration",
                f"{field_name} endpoint relocation is not supported in v1",
            )
    if active.projection_target != candidate.projection_target:
        raise SettingsError(
            "unsupported_secondary_target",
            "changing the logical projection_target is not supported in v1",
        )
