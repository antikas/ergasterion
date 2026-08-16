-- Latest-wins dedup over the append-only deal_decision_log. The investment-authorisation
-- shape (decision / conditions / actor / decided_at) is applied
-- to the deal pipeline -- the same append-only, dbt-unmanaged pattern
-- entity_resolution_decisions_log uses, through the shared
-- parameterised ensure-macro (macros/entity_resolution_decisions.sql), never a copy.
-- Mirrors int_entity_resolution_latest_decision.sql's shape for the ER log.
--
-- Keyed on external_deal_id -- the deal's OWN business key from its single ORIGO
-- source. Intra-source resolution never renames it. The model uses a
-- deterministic tie-break: decided_at (the decision's own business timestamp,
-- analyst-supplied) desc, then decision_id desc. decision_id is a business-assigned
-- identifier for the decision EVENT itself, never a load-time/wall-clock artifact --
-- so two decisions recorded with an identical decided_at still resolve
-- deterministically without falling back to load_datetime.
--
-- Every downstream consumer (int_deal_stage_from_decision.sql, deal_approval_queue.sql)
-- reads THIS model, never the raw log directly, so latest-wins dedup is enforced
-- once, here, not re-implemented at every call site.

-- deal_decision_log is created empty at on-run-start and its
-- deterministic fixture rows are merged in by the post-hook on the deal_decision_log_fixtures
-- seed. This depends_on edge makes that seed (and its fixture post-hook) run BEFORE this
-- model, so the source read below sees the fixtures on a fresh build. The read still
-- uses the append-only log source, never the seed.
-- depends_on: {{ ref('deal_decision_log_fixtures') }}
with decisions as (
    select * from {{ source('deal_decision_raw', 'deal_decision_log') }}
),

ranked as (
    select
        decisions.*,
        row_number() over (
            partition by external_deal_id
            order by decided_at desc, decision_id desc
        ) as decision_recency_rank
    from decisions
)

select
    decision_id,
    external_deal_id,
    decision,
    conditions,
    actor,
    decided_at
from ranked
where decision_recency_rank = 1
