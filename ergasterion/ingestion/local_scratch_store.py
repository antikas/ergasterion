"""Capacity-bounded local scratch store, isolated by attempt.

Scratch is never a receipt, checkpoint or publication source. Successful,
failed and restarted attempts delete their own scopes; orphan cleanup never
touches the raw store or any other attempt's live scope.
"""

from __future__ import annotations

import json
import os
import shutil
from pathlib import Path

from ergasterion.ingestion.evidence import b64url_decode
from ergasterion.ingestion.records import (
    Digest,
    NonNegativeIntegerString,
    PositiveInteger,
    PositiveIntegerString,
    ScratchChunk,
    ScratchReadPage,
    ScratchScope,
    Token,
    UnitResult,
)
from ergasterion.ingestion.runtime import PortError, canonical_digest, digest_token

META_NAME = "scope.json"


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    tmp = path.with_suffix(".tmp")
    tmp.write_bytes(encoded)
    os.replace(tmp, path)


def _read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


class LocalScratchStore:
    """Directory-backed ``ScratchStorePort``. Aggregate reserved capacity across live
    scopes cannot exceed ``max_scratch_bytes``; each scope also has its own
    ``capacity_bytes`` reservation."""

    def __init__(self, root: str | Path, *, max_scratch_bytes: int = 128 * 1024 * 1024) -> None:
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.max_scratch_bytes = max_scratch_bytes
        self._created = 0

    def _scope_dir(self, attempt_id: Digest, scope_id: Token) -> Path:
        return self.root / attempt_id / scope_id

    def _meta(self, directory: Path) -> dict:
        path = directory / META_NAME
        if not path.is_file():
            raise PortError("not_found", str(directory))
        return _read_json(path)

    def _reserved_aggregate(self) -> int:
        total = 0
        if not self.root.exists():
            return 0
        for meta_path in self.root.glob("*/*/scope.json"):
            try:
                total += int(_read_json(meta_path)["capacity_bytes"])
            except (OSError, KeyError, ValueError, json.JSONDecodeError):
                continue
        return total

    def create_scope(self, attempt_id: Digest, capacity_bytes: PositiveIntegerString) -> ScratchScope:
        capacity = int(capacity_bytes)
        if capacity < 1:
            raise PortError("capacity_exceeded", "scratch capacity must be positive")
        if self._reserved_aggregate() + capacity > self.max_scratch_bytes:
            raise PortError("capacity_exceeded", "aggregate scratch ceiling would be exceeded")
        self._created += 1
        scope_id = digest_token(canonical_digest({"attempt": attempt_id, "n": self._created}), "scope")
        directory = self._scope_dir(attempt_id, scope_id)
        if directory.exists():
            raise PortError("scope_conflict", scope_id)
        _write_json(directory / META_NAME, {
            "scope_id": scope_id,
            "attempt_id": attempt_id,
            "capacity_bytes": capacity_bytes,
            "used_bytes": 0,
            "next_sequence": 0,
            "closed": False,
        })
        return ScratchScope(scope_id=scope_id, attempt_id=attempt_id, capacity_bytes=capacity_bytes)

    def _require_owner(
        self, attempt_id: Digest, scope_id: Token, *, missing: str = "not_found",
    ) -> tuple[Path, dict]:
        directory = self._scope_dir(attempt_id, scope_id)
        if not (directory / META_NAME).is_file():
            foreign = list(self.root.glob(f"*/{scope_id}/scope.json"))
            if foreign:
                raise PortError("scope_owner_mismatch", scope_id)
            raise PortError(missing, scope_id)
        meta = self._meta(directory)
        if meta["attempt_id"] != attempt_id:
            raise PortError("scope_owner_mismatch", scope_id)
        return directory, meta

    def write_sequential(self, attempt_id: Digest, chunk: ScratchChunk) -> UnitResult:
        directory, meta = self._require_owner(attempt_id, chunk.scope_id, missing="scope_owner_mismatch")
        if meta["closed"]:
            raise PortError("scope_closed", chunk.scope_id)
        if int(chunk.sequence) != int(meta["next_sequence"]):
            raise PortError("sequence_conflict", chunk.scope_id)
        raw = b64url_decode(chunk.bytes_base64url)
        used = int(meta["used_bytes"]) + len(raw)
        if used > int(meta["capacity_bytes"]):
            raise PortError("capacity_exceeded", chunk.scope_id)
        (directory / f"{int(chunk.sequence):020d}.bin").write_bytes(raw)
        meta["used_bytes"] = used
        meta["next_sequence"] = int(meta["next_sequence"]) + 1
        _write_json(directory / META_NAME, meta)
        return UnitResult(ok=True)

    def read_sequential(
        self,
        attempt_id: Digest,
        scope_id: Token,
        after_sequence: NonNegativeIntegerString,
        max_bytes: PositiveIntegerString,
    ) -> ScratchReadPage:
        directory, meta = self._require_owner(attempt_id, scope_id)
        if not meta["closed"]:
            raise PortError("scope_open", scope_id)
        budget = int(max_bytes)
        after = int(after_sequence)
        chunks: list[ScratchChunk] = []
        returned = 0
        next_sequence = None
        files = sorted(path for path in directory.glob("*.bin"))
        for path in files:
            sequence = int(path.stem)
            if sequence <= after:
                continue
            payload = path.read_bytes()
            if len(payload) > budget:
                raise PortError("item_too_large", scope_id)
            if returned + len(payload) > budget and chunks:
                next_sequence = str(sequence)
                break
            if returned + len(payload) > budget:
                raise PortError("item_too_large", scope_id)
            from ergasterion.ingestion.evidence import b64url_encode

            chunks.append(ScratchChunk(
                scope_id=scope_id, sequence=str(sequence), bytes_base64url=b64url_encode(payload),
            ))
            returned += len(payload)
        page: dict = {
            "chunks": tuple(chunks),
            "bytes_returned": str(returned),
        }
        if next_sequence is not None:
            page["next_sequence"] = next_sequence
        return ScratchReadPage.model_validate(page)

    def close_scope(self, attempt_id: Digest, scope_id: Token) -> UnitResult:
        directory, meta = self._require_owner(attempt_id, scope_id)
        meta["closed"] = True
        _write_json(directory / META_NAME, meta)
        return UnitResult(ok=True)

    def delete_scope(self, attempt_id: Digest, scope_id: Token) -> UnitResult:
        directory, _meta = self._require_owner(attempt_id, scope_id, missing="scope_owner_mismatch")
        shutil.rmtree(directory)
        attempt_dir = self.root / attempt_id
        if attempt_dir.exists() and not any(attempt_dir.iterdir()):
            attempt_dir.rmdir()
        return UnitResult(ok=True)

    def cleanup_orphans(self, active_attempt_ids: tuple[Digest, ...], max_scopes: PositiveInteger) -> tuple[Token, ...]:
        active = set(active_attempt_ids)
        removed: list[Token] = []
        limit = int(max_scopes)
        for meta_path in sorted(self.root.glob("*/*/scope.json")):
            if len(removed) >= limit:
                break
            try:
                meta = _read_json(meta_path)
            except (OSError, json.JSONDecodeError):
                continue
            if meta["attempt_id"] in active:
                continue
            scope_id = meta["scope_id"]
            shutil.rmtree(meta_path.parent)
            removed.append(scope_id)
        for attempt_dir in list(self.root.iterdir()):
            if attempt_dir.is_dir() and not any(attempt_dir.iterdir()):
                attempt_dir.rmdir()
        return tuple(removed)


__all__ = ["LocalScratchStore"]
