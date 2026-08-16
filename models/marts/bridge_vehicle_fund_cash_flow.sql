{{ config(materialized='table', schema='marts') }}

-- Vehicle-to-fund cash-flow aggregation bridge. Rolls vehicle-grain cash
-- flows up to fund grain (each vehicle maps to its parent fund via canonical_legal_vehicle)
-- and sets that roll-up beside the fund's OWN canonical cash-flow total for the same
-- golden fund key. The gap between the two grains is reconciliation_delta; the named
-- test assert_vehicle_to_fund_cash_flow_conservation asserts it stays within epsilon.
-- Grain: one row per fund that has vehicles. Inner join -- only funds with vehicles.

with vehicle_flows as (
    select
        veh.fund_id,
        cf.amount_usd as amount_usd
    from {{ ref('canonical_legal_vehicle_cash_flow') }} as cf
    inner join {{ ref('canonical_legal_vehicle') }} as veh
        on veh.vehicle_id = cf.vehicle_id
),

vehicle_rollup as (
    select
        fund_id,
        sum(amount_usd) as vehicle_cash_flow_total,
        count(*) as vehicle_cash_flow_count
    from vehicle_flows
    group by fund_id
),

fund_rollup as (
    select
        fund_id,
        sum(amount_usd) as fund_cash_flow_total
    from {{ ref('canonical_fund_cash_flow') }}
    group by fund_id
)

select
    vehicle_rollup.fund_id,
    vehicle_rollup.vehicle_cash_flow_total,
    fund_rollup.fund_cash_flow_total,
    vehicle_rollup.vehicle_cash_flow_total - fund_rollup.fund_cash_flow_total
        as reconciliation_delta,
    vehicle_rollup.vehicle_cash_flow_count
from vehicle_rollup
inner join fund_rollup
    on fund_rollup.fund_id = vehicle_rollup.fund_id
