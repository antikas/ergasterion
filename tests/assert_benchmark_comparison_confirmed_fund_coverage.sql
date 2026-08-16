-- Row-count coverage test. fact_benchmark_comparison must carry at least one
-- row for every ER-confirmed fund in fact_fund_performance, even when a fund's
-- performance_as_of_date falls outside the seeded benchmark-index window (2025-01..
-- 2025-12 today). Before this fix the model inner-joined the benchmark-index window, so
-- an out-of-window fund silently dropped out of the mart entirely -- no error, no
-- warning, just a missing row. This test turns that silent drop into a failing build.
--
-- Singular test: PASSES when it returns zero rows. It returns a row if the distinct
-- fund_id count in fact_benchmark_comparison does not match the distinct fund_id count
-- of ER-confirmed funds in fact_fund_performance.
with confirmed_funds as (
    select count(distinct fund_id) as fund_count
    from {{ ref('fact_fund_performance') }}
),

comparison_funds as (
    select count(distinct fund_id) as fund_count
    from {{ ref('fact_benchmark_comparison') }}
)

select
    confirmed_funds.fund_count as confirmed_fund_count,
    comparison_funds.fund_count as benchmark_comparison_fund_count
from confirmed_funds
cross join comparison_funds
where confirmed_funds.fund_count <> comparison_funds.fund_count
