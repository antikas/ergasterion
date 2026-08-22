"""Verified local backup of the complete runtime root.

Create acquires a maintenance fence, refuses a nonterminal attempt or incomplete
outbox, checkpoints SQLite and DuckDB, and streams sorted relative paths into
bounded ``BackupEntryPage`` records. Restore writes into an empty staged sibling,
verifies every page link, then atomically switches the root. A restore never
creates a ``ReprocessingClaim``.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
from pathlib import Path

from ergasterion.framework.bronze_contract import AttemptState
from ergasterion.ingestion.records import BackupEntry, BackupEntryPage, BackupManifest
from ergasterion.ingestion.runtime import PortError, utc_now_string
from ergasterion.ingestion.settings import LocalLayout
from ergasterion.source_delivery import compute_derived_digest

CANONICAL_FILE_MODE = "100644"
FENCE_NAME = ".maintenance-fence"
PAGES_DIR = "pages"
MANIFEST_NAME = "backup-manifest.json"


class BackupError(PortError):
    """A closed backup/restore failure."""


def paths_overlap(left: Path, right: Path) -> bool:
    a = left.resolve()
    b = right.resolve()
    if a == b:
        return True
    try:
        a.relative_to(b)
        return True
    except ValueError:
        pass
    try:
        b.relative_to(a)
        return True
    except ValueError:
        return False


def _posix_relative(root: Path, path: Path) -> str:
    relative = path.resolve().relative_to(root.resolve())
    text = relative.as_posix()
    if not text or text.startswith("/") or text.startswith("\\") or ".." in relative.parts or "." in relative.parts:
        raise BackupError("integrity_error", f"backup path is not a safe relative path: {text}")
    return text


def _iter_files(root: Path) -> list[Path]:
    files: list[Path] = []
    if not root.exists():
        return files
    for dirpath, dirnames, filenames in os.walk(root, followlinks=False):
        dirnames[:] = sorted(name for name in dirnames if name != FENCE_NAME)
        for name in sorted(filenames):
            if name == FENCE_NAME:
                continue
            path = Path(dirpath) / name
            if path.is_symlink():
                raise BackupError("integrity_error", "backup refuses symbolic links")
            files.append(path)
    return files


def _sha256_file(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as stream:
        while True:
            chunk = stream.read(1024 * 1024)
            if not chunk:
                break
            hasher.update(chunk)
    return hasher.hexdigest()


def _encoded_size(record) -> int:
    return len(json.dumps(record.model_dump(mode="json", by_alias=True), separators=(",", ":")).encode("utf-8"))


def _page_digest(page_index: str, previous: str | None, entries: tuple[BackupEntry, ...]) -> str:
    body = {
        "schema": "ergasterion.local-backup-entry-page/v1",
        "page_index": page_index,
        "previous_page_digest": previous,
        "entries": [entry.model_dump(mode="json", by_alias=True) for entry in entries],
    }
    return compute_derived_digest("BackupEntryPage", body)


def _manifest_digest(body: dict) -> str:
    return compute_derived_digest("BackupManifest", body)


def acquire_fence(runtime_root: Path) -> Path:
    runtime_root.mkdir(parents=True, exist_ok=True)
    fence = runtime_root / FENCE_NAME
    try:
        fd = os.open(str(fence), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError as exc:
        raise BackupError("inflight_attempt", "a local maintenance fence is already held") from exc
    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        handle.write("held\n")
    return fence


def release_fence(fence: Path) -> None:
    try:
        fence.unlink()
    except FileNotFoundError:
        pass


def refuse_live_work(session) -> None:
    status = session.runtime.ports.state_store.status_query(session.contract.logical_identity)
    latest = status.latest_attempt
    if latest is not None and latest.state not in (AttemptState.COMMITTED, AttemptState.FAILED):
        raise BackupError(
            "inflight_attempt",
            f"attempt {latest.attempt_id} is {latest.state.value}; backup requires a terminal attempt",
        )
    if int(status.incomplete_outbox_count) != 0:
        raise BackupError(
            "inflight_attempt",
            "backup refuses an incomplete outbox",
        )


def create_backup(
    session,
    layout: LocalLayout,
    destination: Path,
    *,
    runtime_binding_digest: str,
    runtime_manifest_digest: str,
) -> BackupManifest:
    destination = destination.resolve()
    if paths_overlap(destination, layout.runtime_root):
        raise BackupError(
            "invalid_config",
            "backup destination must sit outside and not overlap the local runtime root",
        )
    refuse_live_work(session)
    fence = acquire_fence(layout.runtime_root)
    try:
        session.checkpoint()
        status = session.runtime.ports.state_store.status_query(session.contract.logical_identity)
        cursor = session.runtime.ports.projection_publisher.read_cursor(
            session.contract.logical_identity, layout.binding.projection_target,
        )
        created_at = session.now() if hasattr(session, "now") else utc_now_string()
        session.close()
        files = _iter_files(layout.runtime_root)
        entries = tuple(
            BackupEntry(
                relative_path=_posix_relative(layout.runtime_root, path),
                mode=CANONICAL_FILE_MODE,
                size_bytes=str(path.stat().st_size),
                sha256=_sha256_file(path),
            )
            for path in sorted(files, key=lambda item: _posix_relative(layout.runtime_root, item))
        )
        max_bytes = int(layout.binding.runtime_resources.max_wire_record_bytes)
        destination.mkdir(parents=True, exist_ok=True)
        pages_dir = destination / PAGES_DIR
        pages_dir.mkdir(exist_ok=True)
        pages: list[BackupEntryPage] = []
        previous: str | None = None
        bucket: list[BackupEntry] = []
        for entry in entries:
            candidate = tuple(bucket + [entry])
            trial = BackupEntryPage(
                schema="ergasterion.local-backup-entry-page/v1",
                page_index=str(len(pages)),
                previous_page_digest=previous,
                entries=candidate,
                page_digest="0" * 64,
            )
            if bucket and _encoded_size(trial) > max_bytes:
                digest = _page_digest(str(len(pages)), previous, tuple(bucket))
                page = BackupEntryPage(
                    schema="ergasterion.local-backup-entry-page/v1",
                    page_index=str(len(pages)),
                    previous_page_digest=previous,
                    entries=tuple(bucket),
                    page_digest=digest,
                )
                (pages_dir / f"{page.page_index}.json").write_text(
                    json.dumps(page.model_dump(mode="json", by_alias=True), separators=(",", ":"), sort_keys=True),
                    encoding="utf-8",
                )
                pages.append(page)
                previous = digest
                bucket = [entry]
            else:
                bucket.append(entry)
        if bucket:
            digest = _page_digest(str(len(pages)), previous, tuple(bucket))
            page = BackupEntryPage(
                schema="ergasterion.local-backup-entry-page/v1",
                page_index=str(len(pages)),
                previous_page_digest=previous,
                entries=tuple(bucket),
                page_digest=digest,
            )
            (pages_dir / f"{page.page_index}.json").write_text(
                json.dumps(page.model_dump(mode="json", by_alias=True), separators=(",", ":"), sort_keys=True),
                encoding="utf-8",
            )
            pages.append(page)
            previous = digest
        body = {
            "schema": "ergasterion.local-backup/v1",
            "runtime_binding_digest": runtime_binding_digest,
            "runtime_manifest_digest": runtime_manifest_digest,
            "state_revision": status.state.state_revision,
            "projection_revision": cursor.projection_revision,
            "created_at": created_at,
            "entry_count": str(len(entries)),
            "page_count": str(len(pages)),
            "entry_pages_ref": "local-backup-pages",
            "final_entry_page_digest": previous,
        }
        digest = _manifest_digest(body)
        manifest = BackupManifest(
            schema="ergasterion.local-backup/v1",
            backup_id=digest,
            runtime_binding_digest=runtime_binding_digest,
            runtime_manifest_digest=runtime_manifest_digest,
            state_revision=body["state_revision"],
            projection_revision=body["projection_revision"],
            created_at=body["created_at"],
            entry_count=body["entry_count"],
            page_count=body["page_count"],
            entry_pages_ref=body["entry_pages_ref"],
            final_entry_page_digest=previous,
            manifest_digest=digest,
        )
        (destination / MANIFEST_NAME).write_text(
            json.dumps(manifest.model_dump(mode="json", by_alias=True), separators=(",", ":"), sort_keys=True),
            encoding="utf-8",
        )
        objects = destination / "objects"
        objects.mkdir(exist_ok=True)
        for path in files:
            relative = _posix_relative(layout.runtime_root, path)
            target = objects / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(path, target)
        return manifest
    finally:
        release_fence(fence)


def _load_pages(destination: Path, manifest: BackupManifest) -> tuple[BackupEntryPage, ...]:
    pages_dir = destination / PAGES_DIR
    pages: list[BackupEntryPage] = []
    previous: str | None = None
    count = int(manifest.page_count)
    if count == 0:
        if manifest.final_entry_page_digest is not None:
            raise BackupError("integrity_error", "empty backup must carry a null final page digest")
        return ()
    for index in range(count):
        path = pages_dir / f"{index}.json"
        if not path.is_file():
            raise BackupError("integrity_error", "backup page chain is truncated")
        page = BackupEntryPage.model_validate_json(path.read_text(encoding="utf-8"))
        if int(page.page_index) != index:
            raise BackupError("integrity_error", "backup pages are reordered")
        if page.previous_page_digest != previous:
            raise BackupError("integrity_error", "backup page chain digest does not match")
        expected = _page_digest(page.page_index, page.previous_page_digest, page.entries)
        if page.page_digest != expected:
            raise BackupError("integrity_error", "backup page digest does not match")
        if not page.entries:
            raise BackupError("integrity_error", "a backup page must be nonempty")
        pages.append(page)
        previous = page.page_digest
    if previous != manifest.final_entry_page_digest:
        raise BackupError("integrity_error", "backup final page digest does not match")
    return tuple(pages)


def restore_backup(layout: LocalLayout, manifest_path: Path) -> BackupManifest:
    manifest_path = manifest_path.resolve()
    destination = manifest_path.parent
    manifest = BackupManifest.model_validate_json(manifest_path.read_text(encoding="utf-8"))
    body = manifest.model_dump(mode="json", by_alias=True)
    expected = _manifest_digest(body)
    if manifest.manifest_digest != expected or manifest.backup_id != expected:
        raise BackupError("integrity_error", "backup manifest digest does not match")
    if paths_overlap(destination, layout.runtime_root):
        raise BackupError(
            "invalid_config",
            "backup archive must sit outside and not overlap the local runtime root",
        )
    pages = _load_pages(destination, manifest)
    entries: list[BackupEntry] = []
    seen: set[str] = set()
    folded: set[str] = set()
    previous_path = ""
    for page in pages:
        for entry in page.entries:
            if entry.relative_path in seen:
                raise BackupError("integrity_error", "backup contains a duplicate path")
            if entry.relative_path.casefold() in folded:
                raise BackupError("integrity_error", "backup contains a case-folded duplicate path")
            if previous_path and entry.relative_path <= previous_path:
                raise BackupError("integrity_error", "backup entries are not in strict path order")
            seen.add(entry.relative_path)
            folded.add(entry.relative_path.casefold())
            previous_path = entry.relative_path
            entries.append(entry)
    if str(len(entries)) != manifest.entry_count:
        raise BackupError("integrity_error", "backup entry count does not match")
    parent = layout.runtime_root.parent
    parent.mkdir(parents=True, exist_ok=True)
    stage = parent / f"{layout.runtime_root.name}.restore-stage"
    if stage.exists():
        raise BackupError("integrity_error", "restore staging directory already exists")
    stage.mkdir()
    objects = destination / "objects"
    for entry in entries:
        source = objects / entry.relative_path
        if not source.is_file() or source.is_symlink():
            raise BackupError("integrity_error", "backup object is missing or is a reparse point")
        if str(source.stat().st_size) != entry.size_bytes or _sha256_file(source) != entry.sha256:
            raise BackupError("integrity_error", "backup object size or digest does not match")
        target = stage / entry.relative_path
        if not str(target.resolve()).startswith(str(stage.resolve())):
            raise BackupError("integrity_error", "backup entry escapes the staged root")
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, target)
    if layout.runtime_root.exists():
        raise BackupError(
            "integrity_error",
            "restore never overwrites an existing runtime root; delete or relocate it first",
        )
    os.replace(stage, layout.runtime_root)
    return manifest
