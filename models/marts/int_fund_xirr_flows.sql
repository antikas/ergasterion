{{ config(materialized='table', schema='marts') }}

-- Signed, time-indexed cash-flow stream for the deterministic XIRR solve,
-- MATERIALISED as a physical table. This is the single most load-bearing part of the
-- XIRR compile fix: macros/xirr.sql references the flows once per bisection step, so if
-- the flows were an inline CTE reaching back into the cash-flow/valuation join subtree,
-- the compiler would re-expand that whole subtree at every step -- the deep re-expansion
-- that drove fact_fund_performance_base past Snowflake's 1-hour SQL-compilation ceiling
-- (error 000649/57014). Landing the flows as a table means each of the 20 halving steps
-- is a bounded scan.
--
-- Grain: one row per (confirmed fund, flow event). Columns are exactly the dpf_xirr
-- contract: xirr_entity_key, t_years (numeric years from the fund's earliest flow),
-- amount (calls negative, distributions/terminal-NAV positive). The flow-building logic
-- lived inline in fact_fund_performance_base.sql before this split; it is defined once
-- here now and the base model reads the solved rate from int_fund_xirr.
--
-- Governance: only ER-CONFIRMED golden funds (dpf_is_er_confirmed) feed the solve, so an
-- unconfirmed identity can never pool cash flows across the wrong fund.

with confirmed_fund as (
    select fund.fund_id
    from {{ ref('dim_fund') }} as fund
    where {{ dpf_is_er_confirmed('fund.fund_resolution_confidence') }}
),

-- Signed cash-flow legs: capital calls out (negative), distributions in (positive).
cash_flow_legs as (
    select
        cash_flow.fund_id,
        cash_flow.cash_flow_date as flow_date,
        case
            when cash_flow.called_amount_usd > 0 then -cash_flow.called_amount_usd
            when cash_flow.distribution_amount_usd > 0 then cash_flow.distribution_amount_usd
            else cast(0 as {{ dpf_type('numeric') }})
        end as amount
    from {{ ref('fact_fund_cash_flow') }} as cash_flow
    inner join confirmed_fund
        on confirmed_fund.fund_id = cash_flow.fund_id
    where cash_flow.called_amount_usd > 0
       or cash_flow.distribution_amount_usd > 0
),

-- Terminal NAV enters the solve as a positive flow at the latest valuation date.
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

nav_leg as (
    select
        latest_valuation.fund_id,
        latest_valuation.valuation_date as flow_date,
        latest_valuation.nav_usd as amount
    from latest_valuation
    where latest_valuation.nav_usd is not null
),

net_flows as (
    select fund_id, flow_date, amount from cash_flow_legs
    union all
    select fund_id, flow_date, amount from nav_leg
),

first_flow as (
    select
        fund_id,
        min(flow_date) as first_flow_date
    from net_flows
    group by fund_id
)

select
    net_flows.fund_id as xirr_entity_key,
    cast({{ dpf_date_diff_days('net_flows.flow_date', 'first_flow.first_flow_date') }} as {{ dpf_type('numeric') }}) / 365.0 as t_years,
    net_flows.amount
from net_flows
inner join first_flow
    on first_flow.fund_id = net_flows.fund_id
