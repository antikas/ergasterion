-- Bonus regression -- not one of the three named acceptance tests, but
-- verifies that an approved decision deterministically produces the next
-- stage-classification row without mutation. The seeded
-- ORIGO-EXT-DUP-B deal sits at DECISION (seed row effective_from 2025-05-05) with no
-- seed-provided terminal stage. Its latest decision
-- from int_deal_latest_decision.sql resolves to
-- approve_with_conditions decided 2025-05-20 (see
-- tests/assert_deal_latest_decision_dedup.sql), so
-- int_deal_stage_from_decision.sql derives a NEW COMMITTED row effective_from
-- 2025-05-20 -- additive: the original DECISION row is never rewritten, only its
-- effective_to (a dim_deal_stage-computed SCD2 boundary over the raw satellite rows,
-- never a rewrite of the satellite row itself) advances to meet the new row.
--
-- Singular test: PASSES when it returns zero rows.

with dup_b as (
    select deal_id
    from {{ ref('canonical_deal') }}
    where external_deal_id = 'ORIGO-EXT-DUP-B'
),

current_stage as (
    select stage.*
    from {{ ref('dim_deal_stage') }} as stage
    inner join dup_b on dup_b.deal_id = stage.deal_id
    where stage.is_current
),

decision_stage as (
    select stage.*
    from {{ ref('dim_deal_stage') }} as stage
    inner join dup_b on dup_b.deal_id = stage.deal_id
    where stage.stage_code = 'DECISION'
)

select 'no_dup_b_deal' as failure_type
where not exists (select 1 from dup_b)

union all

select 'no_current_stage_row' as failure_type
where not exists (select 1 from current_stage)

union all

select 'current_stage_not_committed' as failure_type
from current_stage
where stage_code != 'COMMITTED' or effective_from != date '2025-05-20'

union all

select 'no_decision_stage_row' as failure_type
where not exists (select 1 from decision_stage)

union all

select 'decision_row_mutated' as failure_type
from decision_stage
where effective_from != date '2025-05-05'
