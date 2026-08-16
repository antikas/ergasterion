{{ config(materialized='table', schema='marts') }}

-- Fund-performance BASE facts. Split out of fact_fund_performance so the
-- four ratio metrics (tvpi/dpi/rvpi/moic) can be declared ONCE as a calculated_field
-- spec (models/calculated_fields/cf_fund_multiples.sql) instead of hand-written inline
-- divides in a fact table's SELECT -- matching how cf_fund_hurdle and cf_fund_dpi_gap
-- already work, so the "declarative multiples" claim (criterion-5) is literally true.
--
-- This model carries every fund-intrinsic metric/input that is NOT a derived multiple:
-- the three raw components (paid_in_usd, distributions_usd, latest_nav_usd) the
-- multiples are computed FROM, plus nav/called_pct/unfunded_commitment/net_irr/
-- gross_irr/irr. The paid_in/distributions/nav components and the ratios derived off
-- committed capital are defined once here. net_irr is the deterministic XIRR solve, now
-- computed in the materialised helper chain (int_fund_xirr_flows -> int_fund_xirr) and
-- read back by a single left join below -- the solve moved OUT of this model's SELECT so
-- the fact table's joins and the 20-step bisection compile as separate,
-- bounded plans (the inline solve chain was what pushed this mart past
-- Snowflake's 1-hour SQL-compilation ceiling, error 000649/57014; : the depth
-- cut from 34 to 20 halvings takes the native build single-digit-minutes fast).
-- fact_fund_performance.sql is a thin wrapper that re-joins this table with
-- cf_fund_multiples's emitted tvpi/dpi/rvpi/moic, so every downstream consumer keeps
-- reading the same column names at the same grain.
--
-- Governance: only ER-CONFIRMED golden funds feed metric calculation. The confirmed
-- predicate is the SSOT macro dpf_is_er_confirmed (macros/governance.sql); an
-- unconfirmed fund identity could pool cash flows across the wrong fund, so it is
-- excluded here (and, transitively, from int_fund_xirr_flows) rather than downstream.

with confirmed_fund as (
    select
        fund.fund_key,
        fund.fund_id,
        fund.shared_external_id,
        fund.committed_capital_usd
    from {{ ref('dim_fund') }} as fund
    where {{ dpf_is_er_confirmed('fund.fund_resolution_confidence') }}
),

cash_flow_summary as (
    select
        cash_flow.fund_id,
        sum(cash_flow.called_amount_usd) as paid_in_usd,
        sum(cash_flow.distribution_amount_usd) as distributions_usd
    from {{ ref('fact_fund_cash_flow') }} as cash_flow
    inner join confirmed_fund
        on confirmed_fund.fund_id = cash_flow.fund_id
    group by cash_flow.fund_id
),

latest_valuation as (
    select
        valuation.fund_id,
        valuation.valuation_date,
        valuation.nav_usd
    from {{ ref('fact_fund_valuation') }} as valuation
    inner join confirmed_fund
        on confirmed_fund.fund_id = valuation.fund_id
    qualify row_number() over (
        partition by valuation.fund_id
        order by valuation.valuation_date desc
    ) = 1
),

-- net_irr comes from the materialised deterministic XIRR solve. It is NULL where the
-- fund's cash-flow stream is not bracketed by a root (see int_fund_xirr.sql bracket
-- guard), never a fabricated 0 or a ceiling-pinned rate. gross_irr == net_irr today
--: the canonical cash-flow stream carries no fee/carry event subtype, so
-- there is no distinct gross flow basis to solve. Seam for the future: when fee/carry
-- events are onboarded, add a distinct gross flow basis (a second int_fund_xirr_flows
-- shape with fee/carry legs added back) and solve it separately -- do not re-alias.
fund_irr as (
    select
        int_fund_xirr.xirr_entity_key as fund_id,
        int_fund_xirr.net_irr,
        int_fund_xirr.net_irr as gross_irr
    from {{ ref('int_fund_xirr') }} as int_fund_xirr
),

-- Invested-capital basis. Read-time reference seed keyed by the fund
-- business id (the same pattern as hurdle_config): the recallable return-of-capital
-- and fee-offset amounts subtracted from paid-in to give invested capital. A fund
-- with no row here contributes 0 offsets, so its invested capital defaults to paid-in
-- and its MOIC equals TVPI. This is what makes MOIC a genuine second basis rather than
-- a byte-for-byte copy of TVPI (the tautology the panel flagged).
invested_basis as (
    select
        fund_external_id,
        {{ dpf_safe_cast('recallable_distributions_usd', 'numeric') }} as recallable_distributions_usd,
        {{ dpf_safe_cast('fee_offset_usd', 'numeric') }} as fee_offset_usd
    from {{ ref('fund_invested_capital_basis') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['confirmed_fund.fund_id']) }} as fund_performance_key,
    confirmed_fund.fund_key,
    confirmed_fund.fund_id,
    confirmed_fund.shared_external_id,
    latest_valuation.valuation_date as performance_as_of_date,
    cash_flow_summary.paid_in_usd,
    cash_flow_summary.distributions_usd,
    coalesce(latest_valuation.nav_usd, cast(0 as numeric)) as latest_nav_usd,
    -- nav/called_pct/unfunded_commitment: each formula defined exactly once here.
    -- tvpi/dpi/rvpi/moic are NOT computed here -- they are calculated_field outputs
    -- joined back in by fact_fund_performance.sql.
    coalesce(latest_valuation.nav_usd, cast(0 as numeric)) as nav,
    {{ dpf_safe_divide(
        'cash_flow_summary.paid_in_usd',
        'confirmed_fund.committed_capital_usd'
    ) }} as called_pct,
    confirmed_fund.committed_capital_usd - cash_flow_summary.paid_in_usd as unfunded_commitment,
    -- Invested capital = paid-in less the recallable/fee-offset portion.
    -- Structurally <= paid_in because the offsets are non-negative, so MOIC (which
    -- divides by this) is always >= TVPI (which divides by paid-in). Funds with no
    -- seeded basis get 0 offsets, i.e. invested = paid-in and MOIC = TVPI.
    cash_flow_summary.paid_in_usd
        - coalesce(invested_basis.recallable_distributions_usd, cast(0 as {{ dbt.type_numeric() }}))
        - coalesce(invested_basis.fee_offset_usd, cast(0 as {{ dbt.type_numeric() }})) as invested_capital_usd,
    fund_irr.net_irr,
    fund_irr.gross_irr,
    -- Backward-compatible alias: cf_fund_hurdle.sql reads `irr` as the net-IRR return
    -- metric for the query-time hurdle comparison. net_irr is the single SSOT rate.
    fund_irr.net_irr as irr
from confirmed_fund
inner join cash_flow_summary
    on cash_flow_summary.fund_id = confirmed_fund.fund_id
left join latest_valuation
    on latest_valuation.fund_id = confirmed_fund.fund_id
left join fund_irr
    on fund_irr.fund_id = confirmed_fund.fund_id
left join invested_basis
    on invested_basis.fund_external_id = confirmed_fund.shared_external_id
