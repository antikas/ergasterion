{{ config(materialized='table', schema='marts') }}

-- Customer-grain order summary: one row per customer who placed at
-- least one order, built off int_order_header (order grain) -- the correct base for
-- the repeat-purchase-rate metric, which must count CUSTOMERS, not order-lines or
-- orders. repeat_purchase_flag is 1.0/0.0 (not boolean) so it can feed a MetricFlow
-- `average` measure directly: averaging a 0/1 flag across customers IS the repeat
-- purchase rate, the same idiom this repo already uses for hurdle_cleared
-- (sum_boolean) elsewhere -- here expressed as an average so the metric needs no
-- separate numerator/denominator ratio definition.

with orders as (
    select *
    from {{ ref('int_order_header') }}
    where customer_id is not null
)

select
    customer_id,
    count(*) as order_count,
    sum(order_total) as total_revenue,
    min(order_date) as first_order_date,
    max(order_date) as last_order_date,
    case when count(*) > 1 then cast(1 as {{ dpf_type('numeric') }}) else cast(0 as {{ dpf_type('numeric') }}) end as repeat_purchase_flag
from orders
group by customer_id
