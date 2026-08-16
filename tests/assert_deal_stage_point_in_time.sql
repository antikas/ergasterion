-- Deal-stage point-in-time correctness using half-open SCD2 ranges for the deal funnel.
--
-- The seeded Orion Credit Facility deal (external_deal_id ORIGO-EXT-002) moves
-- SOURCED (2025-02-20) -> SCREENING (2025-03-01) -> DILIGENCE (2025-03-20) ->
-- DECISION (2025-04-10) -> COMMITTED (2025-04-25). A read at 2025-03-10 must land in
-- SCREENING and a read at 2025-04-15 must land in DECISION. Across ALL seeded deals,
-- dim_deal_stage's half-open ranges must be contiguous (no gap, no overlap) and no
-- UNKNOWN stage may leak into a period covered by seeded history.

with orion_expected as (
    select date '2025-03-10' as as_of_date, 'SCREENING' as expected_stage_code
    union all
    select date '2025-04-15' as as_of_date, 'DECISION' as expected_stage_code
),

orion_deal as (
    select deal_id
    from {{ ref('canonical_deal') }}
    where external_deal_id = 'ORIGO-EXT-002'
),

orion_actual as (
    select
        expected.as_of_date,
        stage.stage_code
    from orion_expected as expected
    cross join orion_deal
    left join {{ ref('dim_deal_stage') }} as stage
        on stage.deal_id = orion_deal.deal_id
        and expected.as_of_date >= stage.effective_from
        and expected.as_of_date < stage.effective_to
),

wrong_attribution as (
    select
        expected.as_of_date,
        expected.expected_stage_code,
        actual.stage_code
    from orion_expected as expected
    left join orion_actual as actual
        on actual.as_of_date = expected.as_of_date
    where actual.stage_code is distinct from expected.expected_stage_code
),

range_overlap as (
    select
        a.deal_id,
        a.effective_from as a_effective_from,
        a.effective_to as a_effective_to,
        b.effective_from as b_effective_from,
        b.effective_to as b_effective_to
    from {{ ref('dim_deal_stage') }} as a
    inner join {{ ref('dim_deal_stage') }} as b
        on b.deal_id = a.deal_id
        and b.deal_stage_key > a.deal_stage_key
        and a.effective_from < b.effective_to
        and b.effective_from < a.effective_to
    where a.deal_id != 'UNKNOWN'
),

ordered_ranges as (
    -- Same ordering fix as dim_deal_stage.sql's own SCD2 window: a
    -- same-day stage transition ties on effective_from, so stage_recorded_at (the
    -- intra-day ordering column sat_deal_stage_history now carries) is the second
    -- ordering term here too, ahead of the surrogate deal_stage_history_key --
    -- otherwise this gap arm could itself report a false gap/overlap on exactly the
    -- same-day case the dim's window is now disambiguating.
    select
        deal_id,
        effective_to,
        lead(effective_from) over (
            partition by deal_id
            order by effective_from, stage_recorded_at, deal_stage_history_key
        ) as next_effective_from
    from {{ ref('dim_deal_stage') }}
    where deal_id != 'UNKNOWN'
),

range_gap as (
    select *
    from ordered_ranges
    where next_effective_from is not null
      and effective_to != next_effective_from
),

history_bounds as (
    select
        entity_key as deal_id,
        min(effective_from) as first_effective_from
    from {{ ref('sat_deal_stage_history') }}
    group by entity_key
),

unknown_leak as (
    select
        stage.deal_id,
        stage.effective_from as as_of_date,
        stage.stage_code
    from {{ ref('dim_deal_stage') }} as stage
    inner join history_bounds
        on history_bounds.deal_id = stage.deal_id
        and stage.effective_from >= history_bounds.first_effective_from
    where stage.deal_id != 'UNKNOWN'
      and stage.stage_code = 'UNKNOWN'
)

select 'wrong_attribution' as failure_type, cast(as_of_date as {{ dbt.type_string() }}) as failure_key
from wrong_attribution
union all
select 'range_overlap' as failure_type, deal_id as failure_key
from range_overlap
union all
select 'range_gap' as failure_type, deal_id as failure_key
from range_gap
union all
select 'unknown_leak' as failure_type, deal_id as failure_key
from unknown_leak
