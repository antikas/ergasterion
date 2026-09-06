-- What this proves: an exclusion is on the record rather than silent.
--
-- Two products declare a named predicate. The department history keeps only the
-- fund department rows out of a delivered history that also states sectors and
-- deal stages, and the served summary keeps only the valuation dates that carry
-- a value. Each product writes a filter log naming the predicate and counting
-- what it excluded, and each count has to be the number of rows the predicate
-- actually removes from its own input.
--
-- Singular test: it passes when it returns no rows.
with department_log as (
    select predicate_name, excluded_count
    from {{ ref('reference__fund_department__filter_log') }}
),

department_expected as (
    select count(*) as excluded
    from {{ ref('reference__investment_classification_history') }}
    where not (entity_type = 'FUND' and classification_type_code = 'DEPT')
),

department as (
    select
        'the department history filter did not log what it excluded' as disagreement,
        cast(department_log.excluded_count as varchar) as subject
    from department_log, department_expected
    where department_log.predicate_name <> 'fund_department_history_only'
       or department_log.excluded_count <> department_expected.excluded
),

summary_log as (
    select predicate_name, excluded_count
    from {{ ref('investment__fund_performance_summary__filter_log') }}
),

summary_expected as (
    select count(*) as excluded
    from {{ ref('investment__fund_performance') }}
    where source_nav_usd is null
),

summary as (
    select
        'the served summary filter did not log what it excluded' as disagreement,
        cast(summary_log.excluded_count as varchar) as subject
    from summary_log, summary_expected
    where summary_log.predicate_name <> 'valuation_dates_with_a_stated_value'
       or summary_log.excluded_count <> summary_expected.excluded
)

select disagreement, subject from department
union all
select disagreement, subject from summary
