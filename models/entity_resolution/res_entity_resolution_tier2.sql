-- Tier-2 threshold routing: applies the 3-way band from
-- seeds/entity_resolution_scoring_config.csv (>0.85 auto / 0.65-0.85 review /
-- <0.65 reject) to each tier-0/1 leftover's single best-scoring candidate pair
-- (int_entity_resolution_candidate_pairs). LEFT JOIN so a pending record with zero
-- candidates (the only pending record of its entity_type, or no cross-source
-- counterpart at all) still gets a row here -- disposition 'reject', composite_score
-- null, which leaves the record unresolved without inventing a match.
-- Only 'auto' rows carry a golden_entity_key / resolution_tier / confidence, same
-- column shape as res_fund/res_gp/res_portfolio_company's tier-0/1 output,
-- generalised across entity_type since this model spans all three. 'review' rows are
-- exposed unmerged via review_queue.sql, a filtered, human-facing view of this same
-- routing.

with pending as (
    select * from {{ ref('res_pending_probabilistic') }}
),

best_match as (
    select * from {{ ref('int_entity_resolution_candidate_pairs') }}
),

routed as (
    select
        pending.entity_type as entity_type,
        pending.source_system as source_system,
        pending.source_id as source_id,
        pending.entity_name as entity_name,
        pending.normalised_name as normalised_name,
        best_match.matched_source_system as matched_source_system,
        best_match.matched_source_id as matched_source_id,
        best_match.matched_entity_name as matched_entity_name,
        best_match.string_score as string_score,
        best_match.sector_score as sector_score,
        best_match.date_score as date_score,
        best_match.value_score as value_score,
        best_match.composite_score as composite_score,
        best_match.threshold_auto as threshold_auto,
        best_match.threshold_review as threshold_review,
        case
            when best_match.composite_score is not null and best_match.composite_score > best_match.threshold_auto then 'auto'
            when best_match.composite_score is not null and best_match.composite_score >= best_match.threshold_review then 'review'
            else 'reject'
        end as tier2_disposition
    from pending
    left join best_match
        on best_match.entity_type = pending.entity_type
        and best_match.source_system = pending.source_system
        and best_match.source_id = pending.source_id
)

select
    entity_type,
    source_system,
    source_id,
    entity_name,
    normalised_name,
    matched_source_system,
    matched_source_id,
    matched_entity_name,
    string_score,
    sector_score,
    date_score,
    value_score,
    composite_score,
    tier2_disposition,
    case
        when tier2_disposition = 'auto'
            then {{ dpf_hash_hex("concat(entity_type, '|', least(concat(source_system, '|', source_id), concat(matched_source_system, '|', matched_source_id)))") }}
    end as golden_entity_key,
    case
        when tier2_disposition = 'auto' then 'tier_2_probabilistic_auto'
    end as resolution_tier,
    case
        when tier2_disposition = 'auto' then composite_score
    end as confidence,
    tier2_disposition != 'auto' as pending_probabilistic
from routed
