-- Named test: same-day stage-transition determinism.
--
-- The audit finding: a decision decided_at the SAME calendar day a deal entered the
-- DECISION stage unions two stage rows at one effective_from into
-- sat_deal_stage_history, and dim_deal_stage's SCD2 window broke the tie on the
-- surrogate deal_stage_history_key -- semantically arbitrary, so "current stage"
-- could resolve to the WRONG side of the transition. The fix
-- (models/raw_vault/satellites/sat_deal_stage_history.sql,
-- models/marts/dim_deal_stage.sql) threads a stage_recorded_at intra-day ordering
-- column through the satellite and orders the SCD2 window on
-- (effective_from, stage_recorded_at, deal_stage_history_key).
--
-- This test proves it deterministically WITHOUT depending on the approvals-decision
-- path: seeds/deal_stage_history_seed.csv now carries a same-day transition for
-- ORIGO-EXT-004 (a mid-funnel deal with no decision fixtures -- see
-- macros/entity_resolution_decisions.sql for the only two deals that DO carry
-- decision fixtures, ORIGO-EXT-DUP-B and ORIGO-EXT-001, neither of which this test
-- touches): SCREENING at effective_from 2025-03-25, stage_recorded_at
-- 2025-03-25 00:00:00 (midnight -- the seed only ever knew the calendar date), and
-- DILIGENCE at the SAME effective_from 2025-03-25, stage_recorded_at
-- 2025-03-25 12:00:00 (noon -- the later-recorded row). Before the fix, which of
-- these two same-effective_from rows "won" the current-stage slot depended on the
-- surrogate key hash -- correct only by chance. After the fix, DILIGENCE
-- deterministically supersedes SCREENING same-day:
--   - the SCREENING row's window collapses to zero width
--     (effective_from = effective_to = 2025-03-25) rather than extending indefinitely
--     or overlapping DILIGENCE's window;
--   - the DILIGENCE row is current (is_current = true, effective_to = 9999-12-31).
--
-- Singular test: PASSES when it returns zero rows.
--
-- Test-strength honesty: this test asserts the OUTCOME (the seeded noon-vs-midnight
-- pair resolves deterministically to DILIGENCE) -- it does not directly assert the
-- ordering clause itself. It will catch an ordering regression only when the
-- surrogate-key hash order happens to disagree with the semantic order for THIS
-- specific pair; a regression where the hash still happens to sort DILIGENCE after
-- SCREENING would pass here by coincidence even with the ordering fix reverted. The
-- binding invariant is the (effective_from, stage_recorded_at, deal_stage_history_key)
-- ordering clause itself in dim_deal_stage.sql and int_deal_stage_from_decision.sql's
-- recency window, not this test in isolation.

with deal as (
    select deal_id
    from {{ ref('canonical_deal') }}
    where external_deal_id = 'ORIGO-EXT-004'
),

screening_row as (
    select stage.*
    from {{ ref('dim_deal_stage') }} as stage
    inner join deal on deal.deal_id = stage.deal_id
    where stage.stage_code = 'SCREENING'
),

diligence_row as (
    select stage.*
    from {{ ref('dim_deal_stage') }} as stage
    inner join deal on deal.deal_id = stage.deal_id
    where stage.stage_code = 'DILIGENCE'
),

current_row as (
    select stage.*
    from {{ ref('dim_deal_stage') }} as stage
    inner join deal on deal.deal_id = stage.deal_id
    where stage.is_current
)

select 'no_deal' as failure_type, cast(null as {{ dbt.type_string() }}) as failure_detail
where not exists (select 1 from deal)

union all

select 'no_screening_row' as failure_type, cast(null as {{ dbt.type_string() }}) as failure_detail
where not exists (select 1 from screening_row)

union all

select 'no_diligence_row' as failure_type, cast(null as {{ dbt.type_string() }}) as failure_detail
where not exists (select 1 from diligence_row)

union all

select
    'screening_window_not_zero_width' as failure_type,
    cast(effective_from as {{ dbt.type_string() }}) || ' -> ' || cast(effective_to as {{ dbt.type_string() }}) as failure_detail
from screening_row
where effective_from != date '2025-03-25'
   or effective_to != date '2025-03-25'

union all

select
    'diligence_not_current' as failure_type,
    cast(effective_from as {{ dbt.type_string() }}) || ' current=' || cast(is_current as {{ dbt.type_string() }}) as failure_detail
from diligence_row
where effective_from != date '2025-03-25'
   or effective_to != date '9999-12-31'
   or not is_current

union all

select
    'wrong_current_stage' as failure_type,
    stage_code as failure_detail
from current_row
where stage_code != 'DILIGENCE'
