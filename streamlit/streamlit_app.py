# Snowflake-hosted management console for entity-resolution review, deal approvals,
# and the deal-pipeline browser.
#
# Entity-resolution decisions and deal decisions are inserted into append-only tables
# that dbt does not materialise or truncate. The application overlays new decisions on
# the current view immediately; downstream models incorporate them on the next dbt build.
# Review-tier golden keys are previews computed with the same deterministic formula used
# by the dbt models. The application never writes directly to a golden-record table.

from __future__ import annotations

import hashlib
from datetime import datetime, timezone

import pandas as pd
import streamlit as st
from snowflake.snowpark.context import get_active_session

# Browser title for the tabbed console.
st.set_page_config(page_title="Ergasterion: Management Console", layout="wide")

session = get_active_session()

DATABASE = session.get_current_database() or '"ERGASTERION"'


def quote_snowflake_identifier(value):
    """Validate and quote one Snowflake identifier without trusting caller text."""
    raw = str(value).strip()
    if not raw:
        raise ValueError("Snowflake identifier must not be empty")
    if raw.startswith('"') or raw.endswith('"'):
        if not (raw.startswith('"') and raw.endswith('"')):
            raise ValueError("Snowflake quoted identifier has unmatched quotes")
        inner = raw[1:-1]
        if not inner:
            raise ValueError("Snowflake quoted identifier must not be empty")
        decoded = []
        index = 0
        while index < len(inner):
            if inner[index] == '"':
                if index + 1 >= len(inner) or inner[index + 1] != '"':
                    raise ValueError("Snowflake quoted identifier has an unescaped quote")
                decoded.append('"')
                index += 2
            else:
                decoded.append(inner[index])
                index += 1
        return '"' + ''.join(decoded).replace('"', '""') + '"'
    allowed_initial = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz_"
    allowed_remaining = allowed_initial + "0123456789$"
    if raw[0] not in allowed_initial or any(
        character not in allowed_remaining for character in raw[1:]
    ):
        raise ValueError("Snowflake identifier contains unsafe characters")
    return f'"{raw.upper()}"'


def relation_fqn(database, schema, table):
    """Return one safely quoted database.schema.table identifier."""
    return ".".join(
        quote_snowflake_identifier(part) for part in (database, schema, table)
    )


def schema_names(schema_prefix):
    """Derive the app's dbt schemas from its one sidebar-selected target prefix."""
    prefix = str(schema_prefix).strip()
    if not prefix or prefix.startswith('"') or prefix.endswith('"'):
        raise ValueError("Target schema prefix must be a non-empty unquoted identifier")
    # dbt creates unquoted Snowflake schema names, so target prefixes always carry
    # Snowflake's uppercase semantics (for example, `dev` resolves to `DEV_RAW`).
    canonical_prefix = quote_snowflake_identifier(prefix)[1:-1]
    return {
        "resolution": f"{canonical_prefix}_RESOLUTION",
        "raw": f"{canonical_prefix}_RAW",
        "deal_approvals": f"{canonical_prefix}_DEAL_APPROVALS",
        "marts": f"{canonical_prefix}_MARTS",
        "canonical": f"{canonical_prefix}_CANONICAL",
    }


st.sidebar.header("Connection context")
schema_prefix = st.sidebar.text_input(
    "Target schema prefix (dbt target, e.g. DEV)", value="DEV",
    help="Matches whatever --target/profile schema the dbt build used. "
         "Resolves to <prefix>_RESOLUTION for review_queue and <prefix>_RAW for entity_resolution_decisions_log.",
)
try:
    SCHEMAS = schema_names(schema_prefix)
    TARGET_SELECTOR_ERROR = None
except ValueError as exc:
    SCHEMAS = {}
    TARGET_SELECTOR_ERROR = str(exc)
resolution_schema = SCHEMAS.get("resolution")
raw_schema = SCHEMAS.get("raw")
deal_approvals_schema = SCHEMAS.get("deal_approvals")
marts_schema = SCHEMAS.get("marts")
canonical_schema = SCHEMAS.get("canonical")

FALLBACK_SCORING_CONFIG = {
    "weights": {"string": 0.4, "sector": 0.2, "date": 0.2, "value": 0.2},
    "threshold_auto": 0.85,
    "threshold_review": 0.65,
}


def scoring_config_from_rows(rows):
    """Strictly turn authoritative seed rows into a deterministic console config."""
    by_entity_type = {}
    for row in rows:
        values = {}
        for column, value in (row.as_dict() if hasattr(row, "as_dict") else row).items():
            normalized_column = str(column).lower()
            if normalized_column in values:
                raise ValueError(f"seed row has normalized metric-key collision: {normalized_column}")
            values[normalized_column] = value
        metric_columns = {
            column for column in values if column.startswith("weight_") or column.startswith("threshold_")
        }
        required_metrics = {
            "weight_string", "weight_sector", "weight_date", "weight_value",
            "threshold_auto", "threshold_review",
        }
        if metric_columns != required_metrics:
            raise ValueError(
                "seed metrics must be exactly "
                f"{sorted(required_metrics)}; received {sorted(metric_columns)}"
            )
        entity_raw = values.get("entity_type")
        if entity_raw is None or not str(entity_raw).strip():
            raise ValueError("seed row has a missing entity_type")
        entity_type = str(entity_raw).strip().lower()
        if entity_type in by_entity_type:
            raise ValueError(f"seed has duplicate entity_type: {entity_type}")
        numeric = {}
        for metric in required_metrics:
            raw_value = values.get(metric)
            if raw_value is None or isinstance(raw_value, bool):
                raise ValueError(f"seed {entity_type} has invalid {metric}")
            try:
                numeric[metric] = float(raw_value)
            except (TypeError, ValueError) as exc:
                raise ValueError(f"seed {entity_type} has non-numeric {metric}") from exc
            if numeric[metric] != numeric[metric] or abs(numeric[metric]) == float("inf"):
                raise ValueError(f"seed {entity_type} has non-finite {metric}")
        weights = [
            numeric["weight_string"], numeric["weight_sector"],
            numeric["weight_date"], numeric["weight_value"],
        ]
        if any(weight < 0 or weight > 1 for weight in weights):
            raise ValueError(f"seed {entity_type} has weight outside [0, 1]")
        if abs(sum(weights) - 1) > 0.000000001:
            raise ValueError(f"seed {entity_type} weights must sum to 1 within 1e-9")
        if not 0 <= numeric["threshold_review"] < numeric["threshold_auto"] <= 1:
            raise ValueError(f"seed {entity_type} has invalid review/auto thresholds")
        by_entity_type[entity_type] = {
            "weights": {
                "string": numeric["weight_string"],
                "sector": numeric["weight_sector"],
                "date": numeric["weight_date"],
                "value": numeric["weight_value"],
            },
            "threshold_auto": numeric["threshold_auto"],
            "threshold_review": numeric["threshold_review"],
        }

    if not by_entity_type:
        raise ValueError("entity_resolution_scoring_config has no rows")

    by_entity_type = dict(sorted(by_entity_type.items()))
    profiles = list(by_entity_type.values())
    thresholds = {
        (profile["threshold_review"], profile["threshold_auto"])
        for profile in profiles
    }
    if len(thresholds) != 1:
        raise ValueError("seed thresholds must be uniform for the global-threshold console")
    threshold_review, threshold_auto = next(iter(thresholds))
    weights_are_uniform = all(profile["weights"] == profiles[0]["weights"] for profile in profiles[1:])
    return {
        "is_uniform": weights_are_uniform,
        "weights": profiles[0]["weights"],
        "threshold_auto": threshold_auto,
        "threshold_review": threshold_review,
        "by_entity_type": by_entity_type,
    }


def weight_metric_label(component):
    """Render the active weight, expanding to per-entity values when needed."""
    title = component.title()
    if SCORING_CONFIG["is_uniform"]:
        return f"{title} ({WEIGHTS[component]:.1f})"
    values = ", ".join(
        f"{entity_type}: {config['weights'][component]:.1f}"
        for entity_type, config in SCORING_CONFIG["by_entity_type"].items()
    )
    return f"{title} ({values})"


def select_scoring_config(row_loader, fallback):
    """Separate an unavailable seed lookup from invalid authoritative seed data."""
    try:
        rows = row_loader()
    except Exception as exc:  # noqa: BLE001 -- Snowflake/session/table availability boundary
        return {"is_uniform": True, **fallback, "by_entity_type": {}}, "unavailable", str(exc)
    try:
        return scoring_config_from_rows(rows), "loaded", None
    except ValueError as exc:
        return {"is_uniform": True, **fallback, "by_entity_type": {}}, "invalid", str(exc)


def _load_seed_rows():
    return session.table(
        relation_fqn(DATABASE, raw_schema, "ENTITY_RESOLUTION_SCORING_CONFIG")
    ).collect()


if TARGET_SELECTOR_ERROR:
    SCORING_CONFIG = {"is_uniform": True, **FALLBACK_SCORING_CONFIG, "by_entity_type": {}}
    _seed_status, _seed_detail = "invalid", TARGET_SELECTOR_ERROR
else:
    SCORING_CONFIG, _seed_status, _seed_detail = select_scoring_config(
        _load_seed_rows, FALLBACK_SCORING_CONFIG
    )
if _seed_status == "loaded":
    SCORING_CONFIG_SOURCE = "weights loaded from seed table"
else:
    SCORING_CONFIG_SOURCE = "seed-config fallback (static)"
    SEED_CONFIG_WARNING = (
        "Seed scoring config is unavailable" if _seed_status == "unavailable"
        else "Seed scoring config is invalid"
    ) + f": {_seed_detail}"

# Tier thresholds and weights are loaded above from the deployed seed table. These
# names preserve the frozen ER tab's existing reads without retaining a live literal.
THRESHOLD_AUTO = SCORING_CONFIG["threshold_auto"]
THRESHOLD_REVIEW = SCORING_CONFIG["threshold_review"]
WEIGHTS = SCORING_CONFIG["weights"]

DECISION_APPROVE = "approved_merge"
DECISION_REJECT = "rejected"

# Investment-authorisation decision vocabulary for the deal pipeline.
# The deal_id accepted_values test in models/deal_approvals/_deal_approvals.yml is the
# SSOT for these four strings. Terminal decisions each
# derive the deal's NEXT stage (int_deal_stage_from_decision.sql) and so drop the deal
# out of the pending queue; defer is explicitly non-terminal. ---
DEAL_DECISION_APPROVE = "approve"
DEAL_DECISION_APPROVE_WITH_CONDITIONS = "approve_with_conditions"
DEAL_DECISION_DECLINE = "decline"
DEAL_DECISION_DEFER = "defer"
DEAL_TERMINAL_DECISIONS = {
    DEAL_DECISION_APPROVE, DEAL_DECISION_APPROVE_WITH_CONDITIONS, DEAL_DECISION_DECLINE,
}


def golden_key_preview(entity_type: str, source_system: str, source_id: str,
                        matched_source_system: str, matched_source_id: str) -> str:
    """Mirrors macros/normalisation.sql:stable_golden_key exactly:
    md5(entity_type || '|' || least(source_system||'|'||source_id,
                                     matched_source_system||'|'||matched_source_id))
    Snowflake's md5() returns lower-case hex, same as hashlib's hexdigest()."""
    left = f"{source_system}|{source_id}"
    right = f"{matched_source_system}|{matched_source_id}"
    least_pair = min(left, right)
    payload = f"{entity_type}|{least_pair}"
    return hashlib.md5(payload.encode("utf-8")).hexdigest()


@st.cache_resource(ttl=60)
def _current_user() -> str:
    row = session.sql("select current_user() as u").collect()
    return row[0]["U"] if row else "UNKNOWN"


def load_table(schema: str, table: str) -> pd.DataFrame:
    fqn = relation_fqn(DATABASE, schema, table)
    df = session.table(fqn).to_pandas()
    df.columns = [c.lower() for c in df.columns]
    return df


def overlay_live_labels(queue_df: pd.DataFrame, labels_df: pd.DataFrame) -> pd.DataFrame:
    """``review_queue`` is a materialised table, so its
    human_decision/queue_status columns are only as fresh as the LAST dbt run, so a
    decision written by this app would not visibly leave the pending list until the
    next `dbt build`. This function therefore re-applies review_queue.sql's left-join logic (entity_type + unordered
    source pair) AND int_entity_resolution_latest_decision.sql's latest-wins dedup
    (the `.sort_values("reviewed_at").iloc[-1]` below) against a freshly-queried
    entity_resolution_decisions_log on every render -- the append-only table itself,
    not the dbt-materialized dedup view, so this reflects a decision immediately,
    even one written a moment ago in this same session, without needing a rebuild in
    between. Matches on the same unordered (entity_type, source pair) key the model
    uses."""
    if labels_df.empty:
        return queue_df
    df = queue_df.copy()
    for col in ("human_decision", "human_matched_entity_key", "reviewed_by", "reviewed_at", "notes"):
        if col not in df.columns:
            df[col] = None
    for idx, row in df.iterrows():
        same_order = (
            (labels_df["entity_type"] == row["entity_type"])
            & (labels_df["source_system_a"] == row["source_system"])
            & (labels_df["source_id_a"] == row["source_id"])
            & (labels_df["source_system_b"] == row["matched_source_system"])
            & (labels_df["source_id_b"] == row["matched_source_id"])
        )
        swapped_order = (
            (labels_df["entity_type"] == row["entity_type"])
            & (labels_df["source_system_a"] == row["matched_source_system"])
            & (labels_df["source_id_a"] == row["matched_source_id"])
            & (labels_df["source_system_b"] == row["source_system"])
            & (labels_df["source_id_b"] == row["source_id"])
        )
        matches = labels_df[same_order | swapped_order]
        if matches.empty:
            continue
        latest = matches.sort_values("reviewed_at").iloc[-1]
        df.at[idx, "human_decision"] = latest["decision"]
        df.at[idx, "human_matched_entity_key"] = latest["matched_entity_key"]
        df.at[idx, "reviewed_by"] = latest["reviewed_by"]
        df.at[idx, "reviewed_at"] = latest["reviewed_at"]
        df.at[idx, "notes"] = latest["notes"]
    df["queue_status"] = df["human_decision"].apply(lambda d: "pending_review" if pd.isna(d) else "reviewed")
    return df


def write_decision(raw_schema: str, row: pd.Series, decision: str, reviewed_by: str, notes: str) -> None:
    # entity_resolution_decisions_log is a plain Snowflake table, created
    # once, idempotently, by the on-run-start hooks in dbt_project.yml
    # (macros/entity_resolution_decisions.sql) -- NEVER a dbt seed or model, so no
    # `dbt build`/`dbt seed`/`dbt run` truncates it. This is a pure INSERT, never an
    # UPDATE or DELETE, so re-adjudicating a pair appends a
    # new row rather than overwriting the prior one; latest-wins dedup for anything
    # that reads the durable dbt-side view happens in
    # models/entity_resolution/int_entity_resolution_latest_decision.sql.
    fqn = relation_fqn(DATABASE, raw_schema, "ENTITY_RESOLUTION_DECISIONS_LOG")
    matched_key = None
    if decision == DECISION_APPROVE:
        matched_key = golden_key_preview(
            row["entity_type"], row["source_system"], row["source_id"],
            row["matched_source_system"], row["matched_source_id"],
        )
    # reviewed_at is a native TIMESTAMP_LTZ column on this table -- it is hand-DDL'd
    # with explicit types up front (macros/entity_resolution_decisions.sql), so the
    # committed-empty-seed zero-row NUMBER(38,0) type-inference trap the old seed hit
    # does not apply here; current_timestamp() inserts directly, no string cast needed.
    insert_sql = f"""
        insert into {fqn}
            (entity_type, source_system_a, source_id_a, source_system_b, source_id_b,
             decision, matched_entity_key, reviewed_by, reviewed_at, notes)
        select ?, ?, ?, ?, ?, ?, ?, ?, current_timestamp(), ?
    """
    session.sql(
        insert_sql,
        params=[
            row["entity_type"], row["source_system"], row["source_id"],
            row["matched_source_system"], row["matched_source_id"],
            decision, matched_key, reviewed_by, notes or None,
        ],
    ).collect()


def generate_deal_decision_id(external_deal_id: str) -> str:
    """Deterministic decision_id scheme: DEC-<external_deal_id>-<UTC
    timestamp to the microsecond>. decision_id is the business-assigned identifier
    int_deal_latest_decision.sql uses as its tie-break after decided_at. It never uses
    load_datetime.
    Deterministic in shape and practically unique per click (microsecond resolution);
    never collides with the hand-assigned DEC-<deal>-01/-02 fixture rows that same
    macro seeds, since those use a plain numeric suffix, never a timestamp."""
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%f")
    return f"DEC-{external_deal_id}-{stamp}"


def write_deal_decision(raw_schema: str, external_deal_id: str, decision: str,
                         conditions: str | None, actor: str) -> str:
    """Mirrors write_decision() mechanics: the same session and
    connection, a parameterised `insert ... select ?, ?, ...` -- never an f-string of
    analyst-typed text (conditions and actor are both free text from the UI, exactly
    like write_decision()'s notes/reviewed_by) -- and current_timestamp() inserted
    directly for decided_at, a native TIMESTAMP_LTZ column hand-DDL'd by
    macros/entity_resolution_decisions.sql (dpf_deal_decision_log_columns). Pure INSERT,
    never UPDATE/DELETE, so re-deciding a deal appends a new row -- durable
    latest-wins dedup happens in models/deal_approvals/int_deal_latest_decision.sql;
    this app's own overlay (overlay_live_deal_decisions, below) re-applies that same
    latest-wins logic directly against the freshly-queried log so the pending queue
    reflects the decision without waiting for the next dbt build. Returns the
    decision_id written, for any caller that wants to surface it."""
    fqn = relation_fqn(DATABASE, raw_schema, "DEAL_DECISION_LOG")
    decision_id = generate_deal_decision_id(external_deal_id)
    insert_sql = f"""
        insert into {fqn}
            (decision_id, external_deal_id, decision, conditions, actor, decided_at)
        select ?, ?, ?, ?, ?, current_timestamp()
    """
    session.sql(
        insert_sql,
        params=[decision_id, external_deal_id, decision, conditions or None, actor],
    ).collect()
    return decision_id


def overlay_live_deal_decisions(queue_df: pd.DataFrame, decisions_df: pd.DataFrame) -> pd.DataFrame:
    """Mirrors overlay_live_labels above for the deal pipeline:
    deal_approval_queue is `materialized: table` (dbt_project.yml's deal_approvals
    config), not a view, so a decision written by write_deal_decision() does not
    retroactively change its latest_decision/stage columns until the next `dbt build`.
    This function re-queries
    deal_decision_log (the append-only SOURCE table, not the dbt-materialized
    int_deal_latest_decision dedup view) on every render and re-applies that model's
    own latest-wins tie-break (decided_at desc, decision_id desc, never
    load_datetime) client-side, so an analyst clicking a decision button sees it
    reflected immediately:
      - a TERMINAL decision (approve / approve_with_conditions / decline) derives the
        deal's NEXT stage per int_deal_stage_from_decision.sql, so the deal drops out
        of the pending queue at once here, mirroring deal_approval_queue.sql's own
        "still at DECISION" precondition;
      - defer is explicitly non-terminal, so the deal stays visible with its
        decision/conditions/actor/decided_at updated to the new defer.
    The derived STAGE ROW itself (dim_deal_stage / fact_deal_pipeline) is NOT
    recomputed here -- it genuinely only lands on the next dbt build; this overlay
    only fixes up the pending-queue VIEW, and the UI says so explicitly at the point
    of decision (render_deal_approvals_tab, below) so a "the deal vanished from the
    queue" moment is never mistaken for "the pipeline mart moved too"."""
    if decisions_df.empty:
        return queue_df
    df = queue_df.copy()
    for col in ("latest_decision", "conditions", "actor", "decided_at"):
        if col not in df.columns:
            df[col] = None
    keep = []
    for idx, row in df.iterrows():
        matches = decisions_df[decisions_df["external_deal_id"] == row["external_deal_id"]]
        if matches.empty:
            keep.append(True)
            continue
        latest = matches.sort_values(["decided_at", "decision_id"]).iloc[-1]
        df.at[idx, "latest_decision"] = latest["decision"]
        df.at[idx, "conditions"] = latest["conditions"]
        df.at[idx, "actor"] = latest["actor"]
        df.at[idx, "decided_at"] = latest["decided_at"]
        keep.append(latest["decision"] not in DEAL_TERMINAL_DECISIONS)
    df["_pending_after_overlay"] = keep
    return df[df["_pending_after_overlay"]].drop(columns=["_pending_after_overlay"])


# --- Sidebar: connection context (dpf connection has no default schema -- see
# RUNBOOK.md section 3/4 -- so this app fully-qualifies every table it touches; the
# dbt default generate_schema_name macro produces "<target-schema>_<custom-schema>",
# e.g. target "DEV" + models/entity_resolution's `+schema: resolution` -> "DEV_RESOLUTION").
# Display-only: DATABASE remains quoted and relation_fqn quotes it on query paths.
# DATABASE_DISPLAY only unquotes the caption
# text so the FQN renders uniformly unquoted alongside the already-unquoted schema
# and table names below, instead of the query path's mixed-quoted form.
DATABASE_DISPLAY = DATABASE.strip('"')
st.sidebar.caption(f"Database: `{DATABASE_DISPLAY}`")
st.sidebar.caption(f"review_queue: `{DATABASE_DISPLAY}.{resolution_schema}.REVIEW_QUEUE`")
st.sidebar.caption(f"entity_resolution_decisions_log: `{DATABASE_DISPLAY}.{raw_schema}.ENTITY_RESOLUTION_DECISIONS_LOG`")

# Deal-pipeline schema resolution uses the same schema prefix. See the
# deal_approvals, marts, and canonical schema configuration in dbt_project.yml.
st.sidebar.caption(f"deal_approval_queue: `{DATABASE_DISPLAY}.{deal_approvals_schema}.DEAL_APPROVAL_QUEUE`")
st.sidebar.caption(f"deal_decision_log: `{DATABASE_DISPLAY}.{raw_schema}.DEAL_DECISION_LOG`")
st.sidebar.caption(f"fact_deal_pipeline: `{DATABASE_DISPLAY}.{marts_schema}.FACT_DEAL_PIPELINE`")

st.caption(SCORING_CONFIG_SOURCE)
if _seed_status != "loaded":
    st.warning(SEED_CONFIG_WARNING)
if TARGET_SELECTOR_ERROR:
    st.error("Target schema prefix is invalid. Enter an unquoted Snowflake identifier before querying data.")
    st.stop()

reviewed_by = st.sidebar.text_input("Reviewing as", value=_current_user())
show_reviewed = st.sidebar.checkbox("Also show already-reviewed pairs", value=False)


def render_er_review_queue_tab() -> None:
    """Render candidate scores, golden-key previews, and decision controls."""
    st.title("Entity resolution: review queue")
    st.caption(
        f"Tier-2 governance: composite ≥ {THRESHOLD_AUTO} auto-merges, "
        f"{THRESHOLD_REVIEW} to {THRESHOLD_AUTO} lands here for review, "
        f"< {THRESHOLD_REVIEW} is rejected. Composite = "
        f"{WEIGHTS['string']} × string + {WEIGHTS['sector']} × sector + "
        f"{WEIGHTS['date']} × date + {WEIGHTS['value']} × value."
    )

    try:
        queue_df = load_table(resolution_schema, "REVIEW_QUEUE")
        labels_df = load_table(raw_schema, "ENTITY_RESOLUTION_DECISIONS_LOG")
    except Exception as exc:  # noqa: BLE001 -- surface the real Snowflake error to the analyst
        st.error(
            f"Could not read {DATABASE}.{resolution_schema}.REVIEW_QUEUE or "
            f"{DATABASE}.{raw_schema}.ENTITY_RESOLUTION_DECISIONS_LOG; check the target schema "
            f"prefix in the sidebar. Underlying error: {exc}"
        )
        return

    queue_df = overlay_live_labels(queue_df, labels_df)

    if not show_reviewed:
        queue_df = queue_df[queue_df["queue_status"] == "pending_review"]

    # Defence in depth: review_queue.sql already filters to tier2_disposition = 'review'
    # (composite 0.65-0.85), but re-assert the band here since this UI's contract is
    # specifically "the middle band" -- if the underlying model's filter ever drifts,
    # this catches it instead of silently rendering rows outside the stated tier.
    queue_df = queue_df[
        queue_df["composite_score"].isna()
        | queue_df["composite_score"].between(THRESHOLD_REVIEW, THRESHOLD_AUTO)
    ]
    queue_df = queue_df.sort_values("composite_score", ascending=False, na_position="last")

    st.metric("Pairs in view", len(queue_df))

    if queue_df.empty:
        st.info("No candidate pairs in the middle band right now.")
        return

    for _, row in queue_df.iterrows():
        header = (
            f"{row['entity_type'].upper()}: {row['source_system']}:{row['source_id']} "
            f"↔ {row['matched_source_system']}:{row['matched_source_id']}  "
            f"(composite {row['composite_score']:.3f} · {row['queue_status']})"
        )
        with st.expander(header, expanded=not show_reviewed):
            left, right = st.columns(2)
            with left:
                st.markdown("**This record**")
                st.write({"source_system": row["source_system"], "source_id": row["source_id"],
                          "entity_name": row["entity_name"]})
            with right:
                st.markdown("**Candidate match**")
                st.write({"source_system": row["matched_source_system"], "source_id": row["matched_source_id"],
                          "entity_name": row["matched_entity_name"]})

            st.markdown("**Composite-score decomposition**")
            score_cols = st.columns(4)
            score_cols[0].metric(weight_metric_label("string"), f"{row['string_score']:.3f}" if pd.notna(row["string_score"]) else "Not available")
            score_cols[1].metric(weight_metric_label("sector"), f"{row['sector_score']:.3f}" if pd.notna(row["sector_score"]) else "Not available")
            score_cols[2].metric(weight_metric_label("date"), f"{row['date_score']:.3f}" if pd.notna(row["date_score"]) else "Not available")
            score_cols[3].metric(weight_metric_label("value"), f"{row['value_score']:.3f}" if pd.notna(row["value_score"]) else "Not available")
            st.progress(min(max(float(row["composite_score"] or 0.0), 0.0), 1.0),
                        text=f"Composite {row['composite_score']:.3f}")

            preview_key = golden_key_preview(
                row["entity_type"], row["source_system"], row["source_id"],
                row["matched_source_system"], row["matched_source_id"],
            )
            st.markdown("**Resolved golden-record preview (if approved)**")
            st.write({
                "golden_entity_key (preview)": preview_key,
                "entity_type": row["entity_type"],
                "reconciled_entity_name": row["entity_name"] or row["matched_entity_name"],
                "contributing_sources": [
                    f"{row['source_system']}:{row['source_id']}",
                    f"{row['matched_source_system']}:{row['matched_source_id']}",
                ],
                "resolution_tier (if approved)": "tier_2_probabilistic_analyst_approved",
            })

            if row["queue_status"] == "reviewed":
                st.success(
                    f"Already reviewed: **{row['human_decision']}** by {row['reviewed_by']} "
                    f"at {row['reviewed_at']}"
                    + (f". Notes: “{row['notes']}”" if row.get("notes") else "")
                )
                continue

            notes = st.text_area("Notes (optional)", key=f"notes_{row['source_system']}_{row['source_id']}_"
                                                           f"{row['matched_source_system']}_{row['matched_source_id']}")
            approve_col, reject_col = st.columns(2)
            row_key = f"{row['entity_type']}|{row['source_system']}|{row['source_id']}|" \
                      f"{row['matched_source_system']}|{row['matched_source_id']}"
            if approve_col.button("Approve merge", key=f"approve_{row_key}", type="primary"):
                write_decision(raw_schema, row, DECISION_APPROVE, reviewed_by, notes)
                st.rerun()
            if reject_col.button("Reject", key=f"reject_{row_key}"):
                write_decision(raw_schema, row, DECISION_REJECT, reviewed_by, notes)
                st.rerun()


def render_deal_approvals_tab() -> None:
    """Render the investment-authorisation queue for the deal pipeline.

    The pending queue comes from deal_approval_queue, with per-deal
    expander with stage history (dim_deal_stage), provenance (canonical_deal's own
    source columns and record_source. Deal identity resolution is intra-source only.
    The golden view comes from dim_deal. The four
    decision actions each write ONE row to deal_decision_log via write_deal_decision(),
    which mirrors write_decision()'s mechanics exactly (same connection, a
    parameterised insert, no f-string interpolation of analyst-typed text).

    Overlay-live (mirrors overlay_live_labels for the ER tab, see
    overlay_live_deal_decisions above for the full mechanics): a decision written here
    is reflected in the pending list immediately, even though the underlying
    deal_approval_queue table and the derived dim_deal_stage/fact_deal_pipeline stage
    row only catch up on the NEXT dbt build -- flagged explicitly in the UI copy below,
    never silently implied as an immediate stage move."""
    st.title("Deal approvals: investment authorisation queue")
    st.caption(
        "Pending deals sitting at the DECISION stage without a terminal decision. "
        "Deal identity resolution in this factory is intra-source only (dedup within "
        "the single ORIGO CRM feed). Fund matching is a separate cross-source process. "
        "cascade; see docs/architecture/deal-master-data.md."
    )

    try:
        queue_df = load_table(deal_approvals_schema, "DEAL_APPROVAL_QUEUE")
        decisions_df = load_table(raw_schema, "DEAL_DECISION_LOG")
    except Exception as exc:  # noqa: BLE001 -- surface the real Snowflake error to the analyst
        st.error(
            f"Could not read {DATABASE}.{deal_approvals_schema}.DEAL_APPROVAL_QUEUE or "
            f"{DATABASE}.{raw_schema}.DEAL_DECISION_LOG; check the target schema prefix "
            f"in the sidebar. Underlying error: {exc}"
        )
        return

    queue_df = overlay_live_deal_decisions(queue_df, decisions_df)

    st.metric("Deals pending approval", len(queue_df))
    if queue_df.empty:
        st.info("No deals are pending an Investment Authorisation decision right now.")
        return

    queue_df = queue_df.sort_values("stage_effective_from")
    st.dataframe(
        queue_df[[
            "external_deal_id", "deal_name", "strategy", "stage_code", "stage_effective_from",
            "days_in_current_stage", "latest_decision", "conditions", "actor", "decided_at",
        ]],
        use_container_width=True,
        hide_index=True,
    )

    try:
        canonical_df = load_table(canonical_schema, "CANONICAL_DEAL")
        golden_df = load_table(marts_schema, "DIM_DEAL")
        stage_df = load_table(marts_schema, "DIM_DEAL_STAGE")
    except Exception as exc:  # noqa: BLE001 -- surface the real Snowflake error to the analyst
        st.error(
            f"Could not read canonical_deal / dim_deal / dim_deal_stage from "
            f"{DATABASE}.{canonical_schema} / {DATABASE}.{marts_schema}; check the target "
            f"schema prefix in the sidebar. Underlying error: {exc}"
        )
        return

    for _, row in queue_df.iterrows():
        header = (
            f"{row['deal_name']}: {row['external_deal_id']} "
            f"({row['stage_code']}, {row['days_in_current_stage']} days) "
            f"[{row['latest_decision'] or 'no decision yet'}]"
        )
        with st.expander(header, expanded=False):
            deal_stage_history = stage_df[stage_df["deal_id"] == row["deal_id"]].sort_values("effective_from")
            deal_canonical = canonical_df[canonical_df["deal_id"] == row["deal_id"]]
            deal_golden = golden_df[golden_df["deal_id"] == row["deal_id"]]

            st.markdown("**Stage history**")
            if deal_stage_history.empty:
                st.write("No stage history rows found for this deal.")
            else:
                st.dataframe(
                    deal_stage_history[[
                        "stage_code", "stage_name", "effective_from", "effective_to",
                        "is_current", "stage_duration_days", "record_source",
                    ]],
                    use_container_width=True,
                    hide_index=True,
                )

            st.markdown("**Provenance** (intra-source resolution)")
            if deal_canonical.empty:
                st.write("No canonical_deal row found for this deal.")
            else:
                prov = deal_canonical.iloc[0]
                st.write({
                    "source_deal_id": prov["source_deal_id"],
                    "external_deal_id": prov["external_deal_id"],
                    "deal_resolution_tier": prov["deal_resolution_tier"],
                    "deal_resolution_confidence": prov["deal_resolution_confidence"],
                    "hub_record_source": prov["hub_record_source"],
                    "deal_name (source)": prov["deal_name__source"],
                    "sourced_date (source)": prov["sourced_date__source"],
                })

            st.markdown("**Golden view (dim_deal)**")
            if deal_golden.empty:
                st.write("No dim_deal row found for this deal.")
            else:
                gold = deal_golden.iloc[0]
                st.write({
                    "deal_id": gold["deal_id"],
                    "deal_name": gold["deal_name"],
                    "strategy": gold["strategy"],
                    "originating_team": gold["originating_team"],
                    "source_channel": gold["source_channel"],
                    "sourced_date": gold["sourced_date"],
                    "structure": gold["structure"],
                    "target_company_id": gold["target_company_id"],
                    "converted_fund_id": gold["converted_fund_id"],
                    "converted_record_type": gold["converted_record_type"],
                })

            if row["latest_decision"] in DEAL_TERMINAL_DECISIONS:
                st.success(
                    f"Already decided: **{row['latest_decision']}** by {row['actor']} at "
                    f"{row['decided_at']}"
                    + (f". Conditions: “{row['conditions']}”" if row.get("conditions") else "")
                    + ". The next stage row lands on the next pipeline build, not immediately."
                )
                continue

            if row["latest_decision"] == DEAL_DECISION_DEFER:
                st.info(
                    f"Currently deferred by {row['actor']} at {row['decided_at']}; pending "
                    "IC reconvene. Recording a new decision below appends a new row "
                    "(latest-wins); it does not overwrite this one."
                )

            conditions = st.text_area(
                "Conditions (used only for 'Approve with conditions')",
                key=f"conditions_{row['external_deal_id']}",
            )
            st.caption(
                "Approving here writes the decision to deal_decision_log immediately; the "
                "derived NEXT STAGE row (dim_deal_stage / fact_deal_pipeline) only appears "
                "after the next `dbt build`; this queue reflects the decision now via the "
                "same overlay-live pattern the ER tab uses, not a same-session stage move."
            )
            approve_col, conditions_col, decline_col, defer_col = st.columns(4)
            if approve_col.button("Approve", key=f"approve_{row['external_deal_id']}", type="primary"):
                write_deal_decision(raw_schema, row["external_deal_id"], DEAL_DECISION_APPROVE, None, reviewed_by)
                st.rerun()
            if conditions_col.button("Approve with conditions", key=f"conditions_btn_{row['external_deal_id']}"):
                write_deal_decision(
                    raw_schema, row["external_deal_id"], DEAL_DECISION_APPROVE_WITH_CONDITIONS,
                    conditions, reviewed_by,
                )
                st.rerun()
            if decline_col.button("Decline", key=f"decline_{row['external_deal_id']}"):
                write_deal_decision(raw_schema, row["external_deal_id"], DEAL_DECISION_DECLINE, None, reviewed_by)
                st.rerun()
            if defer_col.button("Defer", key=f"defer_{row['external_deal_id']}"):
                write_deal_decision(raw_schema, row["external_deal_id"], DEAL_DECISION_DEFER, None, reviewed_by)
                st.rerun()


def render_pipeline_browser_tab() -> None:
    """Show one row per
    resolved deal, its current stage as of today plus the funnel's cycle-time
    measures -- with a stage filter. Read-only: no write-back, and nothing here
    overlays live decisions (that is the Deal Approvals tab's job); a decision
    recorded there only appears in THIS mart's numbers after the next pipeline
    build."""
    st.title("Pipeline browser: fact_deal_pipeline")
    st.caption(
        "Current-state deal funnel snapshot, stage as of today via a "
        "half-open date-range join, with cycle-time measures. A decision recorded in "
        "the Deal Approvals tab lands here only after the next dbt build; this "
        "browser is read-only over the last build's materialised mart."
    )
    try:
        pipeline_df = load_table(marts_schema, "FACT_DEAL_PIPELINE")
    except Exception as exc:  # noqa: BLE001 -- surface the real Snowflake error to the analyst
        st.error(
            f"Could not read {DATABASE}.{marts_schema}.FACT_DEAL_PIPELINE; check the "
            f"target schema prefix in the sidebar. Underlying error: {exc}"
        )
        return

    stage_options = ["(all)"] + sorted(pipeline_df["current_stage_code"].dropna().unique().tolist())
    stage_filter = st.selectbox("Stage filter", stage_options)
    if stage_filter != "(all)":
        pipeline_df = pipeline_df[pipeline_df["current_stage_code"] == stage_filter]

    st.metric("Deals in view", len(pipeline_df))
    cycle_cols = st.columns(3)
    live_pipeline_df = pipeline_df[~(pipeline_df["is_committed"] | pipeline_df["is_declined"])]
    has_stage_days = not live_pipeline_df.empty and live_pipeline_df["days_in_current_stage"].notna().any()
    has_cycle_time = not pipeline_df.empty and pipeline_df["total_cycle_time_days"].notna().any()
    cycle_cols[0].metric(
        "Avg days in stage (live pipeline)",
        f"{live_pipeline_df['days_in_current_stage'].mean():.1f}" if has_stage_days else "Not available",
    )
    cycle_cols[1].metric(
        "Avg days since sourced",
        f"{pipeline_df['days_since_sourced'].mean():.1f}" if not pipeline_df.empty else "Not available",
    )
    cycle_cols[2].metric(
        "Avg total cycle time (decided deals)",
        f"{pipeline_df['total_cycle_time_days'].mean():.1f}" if has_cycle_time else "Not available",
    )

    st.dataframe(
        pipeline_df[[
            "deal_id", "deal_name", "strategy", "sourced_date", "current_stage_code",
            "current_stage_name", "days_in_current_stage", "days_since_sourced",
            "is_committed", "is_declined", "is_converted", "decision_date",
            "total_cycle_time_days", "converted_fund_id",
        ]],
        use_container_width=True,
        hide_index=True,
    )


tab_er, tab_deal, tab_pipeline = st.tabs(
    ["ER Review Queue", "Deal Approvals", "Pipeline Browser"]
)

with tab_er:
    render_er_review_queue_tab()

with tab_deal:
    render_deal_approvals_tab()

with tab_pipeline:
    render_pipeline_browser_tab()
