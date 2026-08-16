{{ config(materialized='table', schema='marts') }}

-- Order-HEADER rollup: one row per golden order, built off
-- bv_order_golden_record (the survived cross-source order header -- order_date,
-- order_total, status, channel) rather than re-deriving header attributes from the
-- per-source satellites directly. Feeds the header-grain metrics fact_order's
-- line-grain rows cannot answer correctly on their own (average order value,
-- repeat-purchase rate) and is the SCD2 point-in-time revenue-attribution join: this
-- is where "an order's revenue attributes to the segment the customer was in AT
-- order date" actually happens (LEFT JOIN dim_customer_segment on the half-open
-- date range), not on the line-grain fact.
--
-- No dim_date join: dim_date's spine is derived from investment cash-flow/valuation
-- event dates only (models/marts/dim_date.sql). order_date stays a plain date column
-- here so the e-commerce model does not depend on the investment date spine.
--
-- LEFT joins throughout, not INNER: a customer or segment link that failed to
-- resolve surfaces as a null column (caught loudly by the not_null/relationships
-- tests in models/marts/_marts.yml), never a silently dropped order row -- this
-- repo's own recent hardening convention (loud guards over vanishing records).

with order_header as (
    select * from {{ ref('bv_order_golden_record') }}
),

order_customer_link as (
    select order_hk, customer_hk from {{ ref('link_order_customer') }}
),

customer_hub as (
    select customer_hk, golden_customer_key from {{ ref('hub_customer') }}
),

segment as (
    select * from {{ ref('dim_customer_segment') }}
)

select
    order_header.golden_order_key as order_id,
    customer_hub.golden_customer_key as customer_id,
    order_header.order_date,
    order_header.order_status,
    order_header.currency,
    order_header.order_total,
    order_header.channel,
    segment.segment_code as customer_segment_at_order_date,
    order_header.hub_record_source,
    order_header.hub_load_datetime
from order_header
left join order_customer_link
    on order_customer_link.order_hk = order_header.order_hk
left join customer_hub
    on customer_hub.customer_hk = order_customer_link.customer_hk
left join segment
    on segment.customer_id = customer_hub.golden_customer_key
    and order_header.order_date >= segment.effective_from
    and order_header.order_date < segment.effective_to
