"""Assert-script tests for verified local backup create/restore.

Usage:
    python tests/python/test_local_backup.py
"""

from __future__ import annotations

import hashlib
import json
import shutil
import sys
import tempfile
import traceback
from pathlib import Path

if __package__ in (None, ""):
    import os as _os
    import sys as _sys
    _sys.path.insert(0, _os.path.dirname(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))))

from ergasterion.framework.bronze_contract import BronzeProductContract
from ergasterion.framework.models import Layer, compute_plan_digest
from ergasterion.framework.resolver import resolve
from ergasterion.ingestion.local_backup import (
    CANONICAL_FILE_MODE,
    BackupError,
    create_backup,
    paths_overlap,
    restore_backup,
)
from ergasterion.ingestion.reference_runtime import open_session
from ergasterion.ingestion.runtime import canonical_digest
from ergasterion.ingestion.settings import resolve_layout
from ergasterion.translators.local_ingestion import build_local_binding, compile_runtime_manifest, runtime_binding_digest

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
VECTORS = REPO_ROOT / "tests" / "fixtures" / "source_delivery_vectors.json"


def _contract() -> BronzeProductContract:
    document = json.loads(VECTORS.read_text(encoding="utf-8"))
    for entry in document["positive"]:
        if entry["case"] == "append_only_managed_opaque_batch":
            return BronzeProductContract.model_validate(entry["payload"])
    raise AssertionError("append_only_managed_opaque_batch missing")


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_binding(path: Path, binding) -> None:
    import yaml
    dumped = binding.model_dump(mode="json", by_alias=True)

    def omit(value):
        if isinstance(value, dict):
            return {key: omit(item) for key, item in value.items() if item is not None}
        if isinstance(value, list):
            return [omit(item) for item in value]
        return value

    path.write_text(yaml.safe_dump(omit(dumped)), encoding="utf-8")


def test_paths_overlap_detects_ancestor() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp) / "runtime" / "data"
        root.mkdir(parents=True)
        assert paths_overlap(root, root)
        assert paths_overlap(root / "raw", root)
        assert paths_overlap(root, root.parent)
        other = Path(tmp) / "backup"
        other.mkdir()
        assert not paths_overlap(root, other)


def test_create_refuses_destination_inside_runtime_root() -> None:
    contract = _contract()
    plan = resolve(Layer.BRONZE)
    binding = build_local_binding(
        contract, execution_plan_digest=compute_plan_digest(plan),
        contract_digest=canonical_digest(contract.model_dump(mode="json", by_alias=True)),
    )
    with tempfile.TemporaryDirectory() as tmp:
        project = Path(tmp)
        (project / "runtime").mkdir()
        _write_binding(project / "runtime" / "local.yml", binding)
        layout = resolve_layout(project_dir=project, binding_path=project / "runtime" / "local.yml", environment="local")
        session = open_session(layout, contract)
        try:
            try:
                create_backup(
                    session, layout, layout.runtime_root / "backup-dest",
                    runtime_binding_digest=runtime_binding_digest(binding),
                    runtime_manifest_digest=compile_runtime_manifest(plan, binding).runtime_manifest_digest,
                )
            except BackupError as exc:
                assert exc.code == "invalid_config"
            else:
                raise AssertionError("create must refuse a destination overlapping the runtime root")
        finally:
            session.close()


def test_quiescent_backup_restores_mode_size_digest() -> None:
    contract = _contract()
    plan = resolve(Layer.BRONZE)
    binding = build_local_binding(
        contract, execution_plan_digest=compute_plan_digest(plan),
        contract_digest=canonical_digest(contract.model_dump(mode="json", by_alias=True)),
    )
    with tempfile.TemporaryDirectory() as tmp:
        project = Path(tmp)
        dest = Path(tmp) / "backup-out"
        (project / "runtime").mkdir()
        _write_binding(project / "runtime" / "local.yml", binding)
        layout = resolve_layout(project_dir=project, binding_path=project / "runtime" / "local.yml", environment="local")
        session = open_session(layout, contract)
        marker = layout.runtime_root / "raw" / "objects" / "marker.bin"
        marker.parent.mkdir(parents=True, exist_ok=True)
        marker.write_bytes(b"ergasterion-backup-marker")
        status = session.runtime.ports.state_store.status_query(contract.logical_identity)
        cursor = session.runtime.ports.projection_publisher.read_cursor(
            contract.logical_identity, binding.projection_target,
        )
        manifest = compile_runtime_manifest(plan, binding)
        created = create_backup(
            session, layout, dest,
            runtime_binding_digest=runtime_binding_digest(binding),
            runtime_manifest_digest=manifest.runtime_manifest_digest,
        )
        assert created.backup_id == created.manifest_digest
        assert created.state_revision == status.state.state_revision
        assert created.projection_revision == cursor.projection_revision
        expected = {path.relative_to(layout.runtime_root).as_posix(): (_sha(path), path.stat().st_size) for path in layout.runtime_root.rglob("*") if path.is_file() and path.name != ".maintenance-fence"}
        shutil.rmtree(layout.runtime_root)
        restored = restore_backup(layout, dest / "backup-manifest.json")
        assert restored.backup_id == created.backup_id
        for relative, (digest, size) in expected.items():
            path = layout.runtime_root / relative
            assert path.is_file(), relative
            assert _sha(path) == digest
            assert path.stat().st_size == size
        page = json.loads((dest / "pages" / "0.json").read_text(encoding="utf-8")) if created.page_count != "0" else {"entries": []}
        for entry in page.get("entries", []):
            assert entry["mode"] == CANONICAL_FILE_MODE
        session2 = open_session(layout, contract)
        try:
            after = session2.runtime.ports.state_store.status_query(contract.logical_identity)
            assert after.state.state_revision == status.state.state_revision
            assert after.state.visibility_epoch == status.state.visibility_epoch
            assert after.state.accepted_progress == status.state.accepted_progress
        finally:
            session2.close()


TESTS = [
    test_paths_overlap_detects_ancestor,
    test_create_refuses_destination_inside_runtime_root,
    test_quiescent_backup_restores_mode_size_digest,
]


def main() -> int:
    failures = 0
    for test in TESTS:
        try:
            test()
            sys.stdout.write(f"PASS {test.__name__}\n")
        except Exception:
            failures += 1
            sys.stdout.write(f"FAIL {test.__name__}\n")
            traceback.print_exc()
    if failures:
        sys.stdout.write(f"{failures} failed\n")
        return 1
    sys.stdout.write(f"{len(TESTS)} passed\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
