{{ config(materialized='table', schema='marts') }}

-- Benchmark-relative metrics: pme, alpha_bps, and quartile_rank. The nine fund-intrinsic metrics
-- live in fact_fund_performance; this mart REFERENCES fund_performance.net_irr (single
-- SSOT rate), it does not recompute any fund-intrinsic metric.
--
--   pme          Kaplan-Schoar public-market equivalent. Each contribution/distribution
--                is future-valued to the as-of date by the benchmark index ratio
--                (end_index / index_at_flow_month); pme = (FV distributions + NAV) /
--                FV contributions. pme > 1 => the fund beat the public market.
--   alpha_bps    Annualised excess of net_irr over the benchmark's annualised return
--                across the fund's life, in basis points.
--   quartile_rank NTILE(4) within the (vintage_year, strategy) peer cohort ordered by
--                net_irr descending; 1 = top quartile.
--
-- Governance carries through: fact_fund_performance already emits only ER-confirmed funds
-- (dpf_is_er_confirmed), so this mart inherits the confirmed gate via that ref. The
-- benchmark join is evaluated from the declared dbt relations at query time, rather
-- than from a separate benchmark snapshot. Correlated subqueries are invalid in Snowflake views, so every step
-- is CTE + join.
--
-- The seeded benchmark-index window covers
-- 2025-01..2025-12. A confirmed fund whose performance_as_of_date (or individual cash-
-- flow months) falls outside that window is never dropped from this mart -- it still
-- gets a row (quartile_rank still populates, since peer-cohort ranking is benchmark-
-- independent) but benchmark_id/pme/alpha_bps surface as honest NULL rather than a
-- silently-substituted neutral value. See fund_benchmark's left join and pme_scaled's
-- uncovered_flow_months guard below, and the row-count coverage test
-- (tests/assert_benchmark_comparison_confirmed_fund_coverage.sql).

with fund_performance as (
    select * from {{ ref('fact_fund_performance') }}
),

fund as (
    select fund_id, vintage_year, strategy from {{ ref('dim_fund') }}
),

benchmark_month as (
    select
        benchmark_id,
        benchmark_name,
        index_date as index_month,
        index_value
    from {{ ref('stg_benchmark_index_returns') }}
),

date_dim as (
    select * from {{ ref('dim_date') }}
),

-- One (fund x benchmark) row anchored at the fund's as-of month, carrying the end index.
-- LEFT join: a confirmed fund whose as-of month falls outside the seeded
-- benchmark-index window must still surface as a row in this mart -- with benchmark_id/
-- benchmark_name/end_index as honest NULL -- rather than silently vanishing from PME,
-- alpha, and quartile entirely the way the prior inner join dropped it.
fund_benchmark as (
    select
        fund_performance.fund_id,
        fund_performance.fund_key,
        fund_performance.performance_as_of_date,
        fund_performance.paid_in_usd,
        fund_performance.distributions_usd,
        fund_performance.latest_nav_usd,
        fund_performance.tvpi as fund_tvpi,
        fund_performance.dpi as fund_dpi,
        fund_performance.moic as fund_moic,
        fund_performance.net_irr as fund_net_irr,
        benchmark_month.benchmark_id,
        benchmark_month.benchmark_name,
        benchmark_month.index_value as end_index
    from fund_performance
    left join benchmark_month
        on benchmark_month.index_month = {{ dpf_date_trunc('month', 'fund_performance.performance_as_of_date') }}
),

-- Cash flows bucketed to their calendar month, for benchmark future-valuation.
cash_flow_month as (
    select
        cash_flow.fund_id,
        {{ dpf_date_trunc('month', 'cash_flow.cash_flow_date') }} as flow_month,
        cash_flow.called_amount_usd,
        cash_flow.distribution_amount_usd
    from {{ ref('fact_fund_cash_flow') }} as cash_flow
),

-- Kaplan-Schoar PME: future-value every flow by (end_index / index at the flow month).
-- : no coalesce-to-1.0 fallback. A flow month the benchmark index does not cover
-- must never be silently treated as an implicit 1.0 scaling factor -- that quietly
-- degrades PME toward TVPI under the same metric name. Instead every flow's coverage is
-- tracked (uncovered_flow_months); the final select nulls pme outright for a fund with
-- any uncovered flow, rather than a partial sum that looks precise but silently drops
-- the uncovered months from the ratio.
pme_scaled as (
    select
        fund_benchmark.fund_id,
        fund_benchmark.benchmark_id,
        sum(case when bench.index_value is null then 1 else 0 end) as uncovered_flow_months,
        sum(
            cash_flow_month.called_amount_usd
            * ({{ dpf_safe_divide('fund_benchmark.end_index', 'bench.index_value') }})
        ) as fv_contributions,
        sum(
            cash_flow_month.distribution_amount_usd
            * ({{ dpf_safe_divide('fund_benchmark.end_index', 'bench.index_value') }})
        ) as fv_distributions
    from fund_benchmark
    inner join cash_flow_month
        on cash_flow_month.fund_id = fund_benchmark.fund_id
    left join benchmark_month as bench
        on bench.benchmark_id = fund_benchmark.benchmark_id
        and bench.index_month = cash_flow_month.flow_month
    group by fund_benchmark.fund_id, fund_benchmark.benchmark_id
),

-- First flow date + the benchmark start index for the annualised alpha window.
fund_first_flow as (
    select
        cash_flow.fund_id,
        min(cash_flow.cash_flow_date) as first_flow_date
    from {{ ref('fact_fund_cash_flow') }} as cash_flow
    group by cash_flow.fund_id
),

alpha_base as (
    select
        fund_benchmark.fund_id,
        fund_benchmark.benchmark_id,
        fund_benchmark.fund_net_irr,
        fund_benchmark.end_index,
        start_bench.index_value as start_index,
        {{ dpf_date_diff_days('fund_benchmark.performance_as_of_date', 'fund_first_flow.first_flow_date') }} as life_days
    from fund_benchmark
    inner join fund_first_flow
        on fund_first_flow.fund_id = fund_benchmark.fund_id
    left join benchmark_month as start_bench
        on start_bench.benchmark_id = fund_benchmark.benchmark_id
        and start_bench.index_month = {{ dpf_date_trunc('month', 'fund_first_flow.first_flow_date') }}
),

benchmark_alpha as (
    select
        fund_id,
        benchmark_id,
        case
            when fund_net_irr is null or start_index is null or start_index = 0 or life_days <= 0
                then cast(null as numeric)
            else round(
                (
                    fund_net_irr
                    - (power(end_index / start_index, 365.0 / life_days) - 1)
                ) * 10000
            )
        end as alpha_bps
    from alpha_base
),

-- Peer-cohort quartile: NTILE(4) over (vintage_year, strategy) by net_irr desc. Cohort is
-- benchmark-independent, so it is the same for every benchmark row of a given fund.
fund_quartile as (
    select
        fund_performance.fund_id,
        ntile(4) over (
            partition by fund.vintage_year, fund.strategy
            order by fund_performance.net_irr desc
        ) as quartile_rank
    from fund_performance
    inner join fund
        on fund.fund_id = fund_performance.fund_id
    where fund_performance.net_irr is not null
)

select
    {{ dbt_utils.generate_surrogate_key([
        'fund_benchmark.fund_id',
        'fund_benchmark.benchmark_id',
        dpf_safe_cast('fund_benchmark.performance_as_of_date', 'string')
    ]) }} as benchmark_comparison_key,
    fund_benchmark.fund_key,
    date_dim.date_key,
    fund_benchmark.fund_id,
    fund_benchmark.benchmark_id,
    fund_benchmark.benchmark_name,
    fund_benchmark.performance_as_of_date as comparison_date,
    fund_benchmark.paid_in_usd,
    fund_benchmark.distributions_usd,
    fund_benchmark.latest_nav_usd,
    fund_benchmark.fund_tvpi,
    fund_benchmark.fund_dpi,
    fund_benchmark.fund_moic,
    fund_benchmark.fund_net_irr,
    fund_benchmark.end_index as benchmark_index_value,
    -- 10..12: benchmark-relative metric catalogue, each formula defined exactly once here.
    -- : any uncovered flow month (or no benchmark match at all -- pme_scaled is
    -- simply absent for the fund) nulls pme outright rather than silently under-summing.
    case
        when pme_scaled.uncovered_flow_months > 0 then cast(null as numeric)
        else {{ dpf_safe_divide(
            'pme_scaled.fv_distributions + fund_benchmark.latest_nav_usd',
            'pme_scaled.fv_contributions'
        ) }}
    end as pme,
    benchmark_alpha.alpha_bps,
    fund_quartile.quartile_rank
from fund_benchmark
inner join date_dim
    on date_dim.date_day = fund_benchmark.performance_as_of_date
left join pme_scaled
    on pme_scaled.fund_id = fund_benchmark.fund_id
    and pme_scaled.benchmark_id = fund_benchmark.benchmark_id
left join benchmark_alpha
    on benchmark_alpha.fund_id = fund_benchmark.fund_id
    and benchmark_alpha.benchmark_id = fund_benchmark.benchmark_id
left join fund_quartile
    on fund_quartile.fund_id = fund_benchmark.fund_id
