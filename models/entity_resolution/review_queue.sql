-- Human-review surface: the middle probabilistic-ER band (composite score
-- 0.65-0.85 inclusive) from res_entity_resolution_tier2, NOT auto-merged. Left-joins
-- int_entity_resolution_latest_decision.sql -- the latest-wins view over
-- entity_resolution_decisions_log, the append-only table a steward's decision
-- actually lands in (a plain table, never a dbt seed) -- keyed on the unordered
-- (entity_type, source pair), so a pair a human has already adjudicated carries its
-- decision here instead of silently reappearing as an open item every run. This is
-- the ONE feedback-loop store for tier-2 review outcomes in this factory; extend it
-- (new columns/rows), do not fork a second labels table. Because the log is
-- append-only, a re-adjudicated pair could otherwise appear more than once here --
-- int_entity_resolution_latest_decision.sql already collapsed it to its single
-- latest-reviewed_at row before this join, so this LEFT JOIN can never fan out
-- (verified by a dbt_utils.unique_combination_of_columns test over entity_type,
-- source_system, and source_id together in _entity_resolution.yml).

with review as (
    select * from {{ ref('res_entity_resolution_tier2') }}
    where tier2_disposition = 'review'
),

labels as (
    select * from {{ ref('int_entity_resolution_latest_decision') }}
)

select
    review.entity_type as entity_type,
    review.source_system as source_system,
    review.source_id as source_id,
    review.entity_name as entity_name,
    review.matched_source_system as matched_source_system,
    review.matched_source_id as matched_source_id,
    review.matched_entity_name as matched_entity_name,
    review.string_score as string_score,
    review.sector_score as sector_score,
    review.date_score as date_score,
    review.value_score as value_score,
    review.composite_score as composite_score,
    labels.decision as human_decision,
    labels.matched_entity_key as human_matched_entity_key,
    labels.reviewed_by as reviewed_by,
    labels.reviewed_at as reviewed_at,
    labels.notes as notes,
    case when labels.decision is null then 'pending_review' else 'reviewed' end as queue_status
from review
left join labels
    on labels.entity_type = review.entity_type
    and (
        (
            labels.source_system_a = review.source_system and labels.source_id_a = review.source_id
            and labels.source_system_b = review.matched_source_system and labels.source_id_b = review.matched_source_id
        )
        or
        (
            labels.source_system_a = review.matched_source_system and labels.source_id_a = review.matched_source_id
            and labels.source_system_b = review.source_system and labels.source_id_b = review.source_id
        )
    )
