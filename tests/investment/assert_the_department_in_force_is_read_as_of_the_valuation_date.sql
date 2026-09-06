-- What this proves: an as-of read takes the version in force on the date being
-- read, not the latest version.
--
-- One fund moved department on 1 July 2025. It is valued at the end of each
-- quarter of that year. The two valuations before the move have to carry the
-- department it was in before, and the two after it the department it moved to.
-- A read that took the latest version would report all four the same way.
--
-- Singular test: it passes when it returns no rows.
with expected (valued_on, department_code) as (
    values
        (date '2025-03-31', 'PRIVATE_EQUITY'),
        (date '2025-06-30', 'PRIVATE_EQUITY'),
        (date '2025-09-30', 'INSTITUTIONAL_GROWTH'),
        (date '2025-12-31', 'INSTITUTIONAL_GROWTH')
),

reported as (
    select source_valued_on, fund_department_code
    from {{ ref('investment__fund_performance') }}
    where fund_external_id = 'OPENIM-FUND-APEX-II'
)

select
    'the department read is not the one in force on the valuation date' as disagreement,
    cast(expected.valued_on as varchar) || ' expected '
        || expected.department_code || ', read '
        || coalesce(reported.fund_department_code, 'nothing') as subject
from expected
left join reported on reported.source_valued_on = expected.valued_on
where reported.fund_department_code is distinct from expected.department_code
