-- What this proves: the second as-of read, on a reference product that changes
-- every month rather than twice in five years.
--
-- The public market index is delivered monthly, on the first of each month. A
-- fund valued on the last day of a quarter has to carry the level in force on
-- that day, which is the level published at the start of that month, not the one
-- published at the start of the next.
--
-- Singular test: it passes when it returns no rows.
with expected (valued_on, index_value) as (
    values
        (date '2025-03-31', 739.6200),
        (date '2025-06-30', 751.4000),
        (date '2025-09-30', 779.5500),
        (date '2025-12-31', 809.8700)
),

reported as (
    select source_valued_on, benchmark_index_value, benchmark_label
    from {{ ref('investment__fund_performance') }}
    where fund_external_id = 'OPENIM-FUND-APEX-II'
),

levels as (
    select
        'the benchmark level read is not the one in force on the valuation date' as disagreement,
        cast(expected.valued_on as varchar) as subject
    from expected
    left join reported on reported.source_valued_on = expected.valued_on
    where reported.benchmark_index_value is distinct from cast(expected.index_value as decimal(18, 4))
),

labelled as (
    select
        'the benchmark read carries no label from the reference product' as disagreement,
        cast(source_valued_on as varchar) as subject
    from reported
    where benchmark_label is null
)

select disagreement, subject from levels
union all
select disagreement, subject from labelled
