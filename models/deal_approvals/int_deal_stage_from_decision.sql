-- Deterministic stage derivation from an approvals decision: an approved
-- decision produces the NEXT stage-classification row for its deal -- additive only,
-- never a mutation of any existing dim_deal_stage/sat_deal_stage_history row. Feeds
-- INTO sat_deal_stage_history.sql via a UNION ALL alongside the seed-sourced history
-- so dim_deal_stage.sql reads one composed stage history.
--
-- Mapping (decision vocabulary -> DEAL_STAGE classification_value_code):
--   approve / approve_with_conditions -> COMMITTED (the deal converts)
--   decline                           -> DECLINED
--   defer                             -> no row (the deal stays at its current stage;
--                                        defer is explicitly NOT a terminal decision)
--
-- Guard: only derives a row for a deal whose CURRENT stage, per the seed-sourced
-- history ALONE (deliberately excluding any decision-derived row -- there is no
-- circularity here, this CTE never reads its own output), is DECISION -- the same
-- precondition deal_approval_queue.sql uses to define "pending." A decision recorded
-- against a deal not currently at DECISION (already resolved by the seed directly, or
-- not yet arrived there) is ignored here by construction, so this derivation can
-- never conflict with or duplicate a stage the seed already carries (e.g.
-- ORIGO-EXT-002/003, both resolved to a terminal stage directly in the seed, modelling
-- deals decided before this approvals workflow existed).
--
-- RETROACTIVE DERIVED-HISTORY DISCLOSURE: this model reads int_deal_latest_decision
-- (latest-wins) and is fully recomputed on every build -- it is a derivation, not
-- itself an append-only log. deal_decision_log (the raw log) never loses a row: a
-- later non-terminal decision (e.g. the IC reconvenes and defers after a prior
-- approve) simply becomes the new "latest," and eligible_decision below then excludes
-- the deal entirely (defer is not in the terminal-decision list), so the
-- previously-derived COMMITTED/DECLINED row this model produced is ABSENT on the next
-- build -- removed from sat_deal_stage_history.sql/dim_deal_stage.sql downstream, not
-- retained as history. The log keeps the full decision trail; this derived view (and
-- everything built on it) shows current truth only.

with seed_history as (
    select
        entity_type,
        entity_external_id,
        classification_type_code,
        {{ dpf_safe_cast('effective_from', 'date') }} as effective_from,
        classification_value_code,
        {{ dpf_safe_cast('stage_recorded_at', 'string') }} as stage_recorded_at
    from {{ ref('deal_stage_history_seed') }}
    where classification_type_code = 'DEAL_STAGE'
),

current_seeded_stage as (
    -- One row per deal: its LAST seed-sourced stage code -- the precondition for
    -- whether an approvals decision is even eligible to move the funnel. Same-day
    -- determinism: ordered on (effective_from, stage_recorded_at,
    -- classification_value_code) -- the same ordering principle as
    -- dim_deal_stage.sql's SCD2 window, with classification_value_code (the only
    -- other stable, discriminating column this seed-only CTE carries -- no
    -- surrogate key exists at this grain) standing in for that window's surrogate
    -- deal_stage_history_key as the final deterministic tie-break.
    select
        entity_external_id,
        classification_value_code as current_stage_code,
        row_number() over (
            partition by entity_external_id
            order by effective_from desc, stage_recorded_at desc, classification_value_code desc
        ) as recency_rank
    from seed_history
),

decisions as (
    select * from {{ ref('int_deal_latest_decision') }}
),

eligible_decision as (
    select
        decisions.external_deal_id,
        decisions.decision,
        decisions.decided_at
    from decisions
    inner join current_seeded_stage
        on current_seeded_stage.entity_external_id = decisions.external_deal_id
        and current_seeded_stage.recency_rank = 1
        and current_seeded_stage.current_stage_code = 'DECISION'
    where decisions.decision in ('approve', 'approve_with_conditions', 'decline')
)

select
    'DEAL' as entity_type,
    external_deal_id as entity_external_id,
    'DEAL_STAGE' as classification_type_code,
    case
        when decision in ('approve', 'approve_with_conditions') then 'COMMITTED'
        when decision = 'decline' then 'DECLINED'
    end as classification_value_code,
    {{ dpf_safe_cast('decided_at', 'date') }} as effective_from,
    'dpf_deal_approval_derivation' as record_source,
    {{ dpf_safe_cast('decided_at', 'string') }} as load_datetime,
    -- Same-day determinism: the intra-day ordering column
    -- sat_deal_stage_history.sql/dim_deal_stage.sql use to break an effective_from
    -- tie against a seed row recorded on the SAME calendar day (e.g. a decision
    -- decided_at the same date a deal's seed row put it at DECISION). Deliberately
    -- decided_at's own full timestamp, not load_datetime -- this project never
    -- orders on load_datetime (a technical load-time artifact, not a business
    -- timestamp; see the 2026-07-13 plan, delta 5).
    {{ dpf_safe_cast('decided_at', 'string') }} as stage_recorded_at
from eligible_decision
