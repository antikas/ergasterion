-- Order-line -> order-header revenue conservation test.
--
-- Structural claim: revenue is CONSERVED across the order-line -> order-header grain
-- change -- the domain's conserved measure, the assert_bridge_identity pattern
-- already used for the investment domain's vehicle-to-fund cash-flow roll-up
-- (assert_vehicle_to_fund_cash_flow_conservation.sql; cf. the identity form,
-- assert_fund_bridge_identity_conservation.sql). fact_order's line-grain
-- line_revenue, summed per order, must reconcile to that same order's header
-- order_total (int_order_header, survived from bv_order_golden_record) within a
-- materiality epsilon. The seeded lines reconcile exactly (no injected discrepancy
-- for this domain, unlike the vehicle/fund pair's deliberate 125.0-vs-124.4 pattern)
-- -- a real break (a doubled line, a line mapped to the wrong order, a dropped
-- roll-up) moves the delta past epsilon and fails here.
--
-- Singular test: PASSES when it returns zero rows.
{% set epsilon = 0.01 %}

with line_totals as (
    select
        order_id,
        sum(line_revenue) as line_revenue_total
    from {{ ref('fact_order') }}
    group by order_id
),

header_totals as (
    select
        order_id,
        order_total as header_revenue_total
    from {{ ref('int_order_header') }}
)

select
    header_totals.order_id,
    line_totals.line_revenue_total,
    header_totals.header_revenue_total,
    line_totals.line_revenue_total - header_totals.header_revenue_total as reconciliation_delta
from header_totals
left join line_totals
    on line_totals.order_id = header_totals.order_id
where abs(coalesce(line_totals.line_revenue_total, 0) - header_totals.header_revenue_total) > {{ epsilon }}
