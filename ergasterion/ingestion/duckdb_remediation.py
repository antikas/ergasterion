"""DuckDB ``RemediationRepository``: immutable decisions and the released-locator claim index.

Decisions are keyed by disposition identity. A release owns the claim index
``(root_visibility_epoch, original_claim_digest, structured_raw_locator)`` so
overlapping locator sets conflict while exact replay returns the prior record.
Paged queries capture an immutable snapshot whose digest excludes page bounds.
"""

from __future__ import annotations

from pathlib import Path

from ergasterion.framework.bronze_contract import RemediationDecisionKind
from ergasterion.ingestion.duckdb_bronze import (
    DuckDBStore,
    cursor_token,
    dumps,
    encoded_size,
    identity_key,
    loads,
    parse_cursor_token,
    query_digest,
)
from ergasterion.ingestion.records import (
    Disposition,
    RemediationDecision,
    RemediationDecisionPage,
    RemediationDecisionQuery,
)
from ergasterion.ingestion.runtime import PortError, canonical_digest

DECISION_QUERY_SCHEMA = "ergasterion.remediation-decision-query/v1"


def decision_query_digest(query: RemediationDecisionQuery) -> str:
    return query_digest(
        schema=DECISION_QUERY_SCHEMA,
        identity=query.logical_identity,
        disposition_id=query.disposition_id,
        authorization_context_ref=query.authorization_context_ref,
    )


def _locator_key(locator) -> str:
    return dumps(locator)


class DuckDBRemediationRepository:
    """``RemediationRepositoryPort`` over the shared DuckDB Bronze file."""

    def __init__(self, store: DuckDBStore | str | Path) -> None:
        self.store = store if isinstance(store, DuckDBStore) else DuckDBStore(store)

    def close(self) -> None:
        self.store.close()

    def record_decision(self, decision: RemediationDecision) -> RemediationDecision:
        self.store.require_available()
        if not decision.disposition_ids:
            raise PortError("integrity_error", "a remediation decision requires disposition_ids")
        identity = ""
        if hasattr(decision.evaluation, "logical_identity") and decision.evaluation.logical_identity is not None:
            identity = identity_key(decision.evaluation.logical_identity)
        if not identity:
            partition = self.store.fetchone(
                "SELECT identity_key FROM dispositions WHERE disposition_id = ?",
                [decision.disposition_ids[0]],
            )
            identity = partition["identity_key"] if partition else ""
        existing = self.store.fetchone(
            "SELECT json FROM remediation_decisions WHERE decision_id = ?", [decision.decision_id],
        )
        if existing is not None:
            prior = loads(RemediationDecision, existing["json"])
            if dumps(prior) != dumps(decision):
                raise PortError("decision_conflict", decision.decision_id)
            return prior
        kind = decision.kind.value if hasattr(decision.kind, "value") else decision.kind
        self.store.begin()
        try:
            if kind == RemediationDecisionKind.RELEASED.value:
                if decision.release is None:
                    raise PortError("integrity_error", "a released decision requires a release record")
                self._claim_locators(decision, identity)
            self.store.execute(
                """INSERT INTO remediation_decisions(
                       decision_id, identity_key, kind, evaluation_id, decided_at, json
                   ) VALUES (?, ?, ?, ?, ?, ?)""",
                [
                    decision.decision_id, identity, kind, decision.evaluation.remediation_evaluation_id,
                    decision.decided_at, dumps(decision),
                ],
            )
            for disposition_id in decision.disposition_ids:
                self.store.execute(
                    "INSERT INTO remediation_decision_dispositions(decision_id, disposition_id) VALUES (?, ?)",
                    [decision.decision_id, disposition_id],
                )
            self.store.commit()
        except PortError:
            self.store.rollback()
            raise
        except Exception:
            self.store.rollback()
            raise
        return decision

    def decision_query(self, query: RemediationDecisionQuery) -> RemediationDecisionPage:
        self.store.require_available()
        if not query.authorization_context_ref:
            raise PortError("access_denied", "authorization_context_ref is required")
        identity = identity_key(query.logical_identity)
        digest = decision_query_digest(query)
        snapshot = self.store.resolve_query_snapshot(
            "decision", digest, identity, query.disposition_id, query.authorization_context_ref,
            query.snapshot_token,
            high_water_sql="""SELECT COUNT(*) AS n FROM remediation_decisions d
               JOIN remediation_decision_dispositions x ON x.decision_id = d.decision_id
               WHERE d.identity_key = ?""",
            high_water_params=[identity],
        )
        after = parse_cursor_token(query.after_cursor)
        sql = """SELECT d.json FROM remediation_decisions d
                 JOIN remediation_decision_dispositions x ON x.decision_id = d.decision_id
                 WHERE d.identity_key = ?"""
        params: list = [identity]
        if query.disposition_id is not None:
            sql += " AND x.disposition_id = ?"
            params.append(query.disposition_id)
        sql += " ORDER BY d.decided_at, d.decision_id"
        rows = self.store.fetchall(sql, params)
        items: list[RemediationDecision] = []
        bytes_returned = 0
        max_bytes = int(query.max_bytes)
        more = False
        next_cursor = None
        seen: set[str] = set()
        sequential = 0
        for row in rows:
            decision = loads(RemediationDecision, row["json"])
            if decision.decision_id in seen:
                continue
            seen.add(decision.decision_id)
            if after is not None and sequential <= after:
                sequential += 1
                continue
            size = encoded_size(decision)
            if size > max_bytes and not items:
                raise PortError("item_too_large", decision.decision_id)
            if len(items) >= query.max_items or bytes_returned + size > max_bytes:
                more = True
                break
            items.append(decision)
            bytes_returned += size
            next_cursor = cursor_token(sequential)
            sequential += 1
        if not more:
            next_cursor = None
        return RemediationDecisionPage(
            items=tuple(items), snapshot_token=snapshot, next_cursor=next_cursor,
            bytes_returned=str(bytes_returned), more=more,
        )

    def _claim_locators(self, decision: RemediationDecision, identity: str) -> None:
        evaluation = decision.evaluation
        locators = decision.release.selected_locators if decision.release is not None else ()
        decision_digest = canonical_digest(decision.model_dump(mode="json", by_alias=True))
        for locator in locators:
            key = _locator_key(locator)
            prior = self.store.fetchone(
                """SELECT decision_id, decision_digest FROM release_claims
                   WHERE root_visibility_epoch = ? AND original_claim_digest = ? AND locator_key = ?""",
                [evaluation.root_visibility_epoch, evaluation.original_claim_digest, key],
            )
            if prior is None:
                self.store.execute(
                    """INSERT INTO release_claims(
                           root_visibility_epoch, original_claim_digest, locator_key, decision_id, decision_digest
                       ) VALUES (?, ?, ?, ?, ?)""",
                    [
                        evaluation.root_visibility_epoch, evaluation.original_claim_digest, key,
                        decision.decision_id, decision_digest,
                    ],
                )
                continue
            if prior["decision_id"] == decision.decision_id and prior["decision_digest"] == decision_digest:
                continue
            raise PortError("release_conflict", key)
        # A reset-root evaluation cannot claim locators whose dispositions live
        # under a different visibility epoch with no ancestry link. Scope the
        # frame lookup to this original claim, identity and candidate; a bare
        # frame_sequence LIMIT 1 can pick another delivery's overlapping index.
        placeholders = ",".join("?" for _ in decision.disposition_ids)
        for locator in locators:
            frame = int(locator.frame_sequence)
            matches = self.store.fetchall(
                f"""SELECT c.visibility_epoch, f.identity_key, d.json AS disposition_json
                    FROM candidate_frames f
                    JOIN candidate_partitions c ON c.candidate_ref = f.candidate_ref
                    JOIN dispositions d
                      ON d.candidate_ref = f.candidate_ref
                     AND d.frame_sequence = f.frame_sequence
                    WHERE f.frame_sequence = ?
                      AND f.identity_key = ?
                      AND d.disposition_id IN ({placeholders})""",
                [frame, identity, *decision.disposition_ids],
            )
            row = None
            for match in matches:
                if identity and match["identity_key"] != identity:
                    continue
                disposition = loads(Disposition, match["disposition_json"])
                if disposition.claim_digest != evaluation.original_claim_digest:
                    continue
                row = match
                break
            if row is None:
                continue
            epoch = str(row["visibility_epoch"])
            if epoch == evaluation.root_visibility_epoch:
                continue
            # identity_key is on the ancestry primary key; an epoch pair from
            # another identity in the same DuckDB file must not authorize a
            # reset-root release. Target-specific rows also carry projection_target.
            ancestors = self.store.fetchall(
                """SELECT identity_key, projection_target, ancestor_epoch FROM visibility_ancestry
                   WHERE descendant_epoch = ? AND ancestor_epoch = ?
                     AND identity_key = ?""",
                [evaluation.root_visibility_epoch, epoch, identity],
            )
            authorized = False
            for candidate in ancestors:
                if identity and candidate["identity_key"] != identity:
                    continue
                target = getattr(evaluation, "projection_target", None)
                row_target = candidate["projection_target"]
                if target and row_target and row_target != target:
                    continue
                authorized = True
                break
            if not authorized:
                raise PortError("ancestry_mismatch", evaluation.root_visibility_epoch)


__all__ = [
    "DECISION_QUERY_SCHEMA",
    "DuckDBRemediationRepository",
    "decision_query_digest",
]
