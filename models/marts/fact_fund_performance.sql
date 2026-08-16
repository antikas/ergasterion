{{ config(materialized='table', schema='marts') }}

-- fund-performance metric catalogue. Thin wrapper: the
-- nine fund-intrinsic metrics all still land on this one mart at the same grain, same
-- column names, but the computation is now split so the four ratio metrics are
-- DECLARATIVE, matching cf_fund_hurdle and cf_fund_dpi_gap:
--   - fact_fund_performance_base.sql computes everything that is NOT a derived
--     multiple (paid_in_usd, distributions_usd, latest_nav_usd, nav, called_pct,
--     unfunded_commitment, net_irr, gross_irr, irr), each formula defined once there.
--   - cf_fund_multiples.sql (a calculated_field spec, macros/calculated_field.sql) is
--     the SINGLE declaration of tvpi/dpi/rvpi/moic, computed from the base model's
--     already-materialised inputs.
--   - this model re-joins the two 1:1 on fund_performance_key so every downstream
--     consumer (cf_fund_hurdle, cf_fund_dpi_gap, cf_fund_metric_reconciliation, the
--     expected-value tests, _marts.yml) keeps reading the same columns off
--     fact_fund_performance, unchanged.
--
-- The three benchmark-relative metrics (pme/alpha_bps/quartile_rank) live in
-- fact_benchmark_comparison, each defined once.

select
    base.fund_performance_key,
    base.fund_key,
    base.fund_id,
    base.shared_external_id,
    base.performance_as_of_date,
    base.paid_in_usd,
    base.distributions_usd,
    base.latest_nav_usd,
    base.nav,
    multiples.tvpi,
    multiples.dpi,
    multiples.rvpi,
    multiples.moic,
    base.called_pct,
    base.unfunded_commitment,
    base.invested_capital_usd,
    base.net_irr,
    base.gross_irr,
    base.irr
from {{ ref('fact_fund_performance_base') }} as base
inner join {{ ref('cf_fund_multiples') }} as multiples
    on multiples.fund_performance_key = base.fund_performance_key
