-- Point-in-time classification attribution.
--
-- The seeded Apex transfer moves from Private Equity to Institutional Growth on
-- 2025-07-01. Valuation/performance observations before and after that date must
-- join to the correct SCD2 row, with contiguous half-open ranges and no UNKNOWN
-- classification leaking into periods covered by seeded history.

with apex_expected as (
    select date '2025-06-30' as performance_as_of_date, 'PRIVATE_EQUITY' as expected_department_code
    union all
    select date '2025-09-30' as performance_as_of_date, 'INSTITUTIONAL_GROWTH' as expected_department_code
),

apex_actual as (
    select
        fact.performance_as_of_date,
        fact.department_code
    from {{ ref('fact_investment_classification') }} as fact
    where fact.shared_external_id = 'OPENIM-FUND-APEX-II'
      and fact.performance_as_of_date in (date '2025-06-30', date '2025-09-30')
),

wrong_attribution as (
    select
        expected.performance_as_of_date,
        expected.expected_department_code,
        actual.department_code
    from apex_expected as expected
    left join apex_actual as actual
        on actual.performance_as_of_date = expected.performance_as_of_date
    where actual.department_code is distinct from expected.expected_department_code
),

range_overlap as (
    select
        a.fund_id,
        a.classification_type_code,
        a.effective_from as a_effective_from,
        a.effective_to as a_effective_to,
        b.effective_from as b_effective_from,
        b.effective_to as b_effective_to
    from {{ ref('dim_investment_classification') }} as a
    inner join {{ ref('dim_investment_classification') }} as b
        on b.fund_id = a.fund_id
        and b.classification_type_code = a.classification_type_code
        and b.investment_classification_key > a.investment_classification_key
        and a.effective_from < b.effective_to
        and b.effective_from < a.effective_to
    where a.fund_id != 'UNKNOWN'
),

ordered_ranges as (
    select
        fund_id,
        classification_type_code,
        effective_to,
        lead(effective_from) over (
            partition by fund_id, classification_type_code
            order by effective_from
        ) as next_effective_from
    from {{ ref('dim_investment_classification') }}
    where fund_id != 'UNKNOWN'
),

range_gap as (
    select *
    from ordered_ranges
    where next_effective_from is not null
      and effective_to != next_effective_from
),

history_bounds as (
    select
        entity_key as fund_id,
        classification_type_code,
        min(effective_from) as first_effective_from
    from {{ ref('sat_investment_classification_history') }}
    group by entity_key, classification_type_code
),

unknown_leak as (
    select
        fact.fund_id,
        fact.performance_as_of_date,
        fact.department_code,
        fact.sector_code
    from {{ ref('fact_investment_classification') }} as fact
    inner join history_bounds as dept_bounds
        on dept_bounds.fund_id = fact.fund_id
        and dept_bounds.classification_type_code = 'DEPT'
        and fact.performance_as_of_date >= dept_bounds.first_effective_from
    inner join history_bounds as sector_bounds
        on sector_bounds.fund_id = fact.fund_id
        and sector_bounds.classification_type_code = 'SECTOR'
        and fact.performance_as_of_date >= sector_bounds.first_effective_from
    where fact.department_code = 'UNKNOWN'
       or fact.sector_code = 'UNKNOWN'
)

select 'wrong_attribution' as failure_type, cast(performance_as_of_date as {{ dbt.type_string() }}) as failure_key
from wrong_attribution
union all
select 'range_overlap' as failure_type, fund_id as failure_key
from range_overlap
union all
select 'range_gap' as failure_type, fund_id as failure_key
from range_gap
union all
select 'unknown_leak' as failure_type, fund_id as failure_key
from unknown_leak
