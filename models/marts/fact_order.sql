{{ config(materialized='table', schema='marts') }}

-- Order-line fact: one row per source order-line record (CARTIVO or
-- MERCARO), the grain the domain's conserved measure (line revenue) is asserted at.
-- order_line_lhk (link_order_line_product's hash of golden_order_key +
-- golden_product_key) is NOT unique per source line on its own -- it is a LINK key,
-- shared by every line of the same order that resolved to the same product -- so the
-- surrogate key here is built from (source_system, source_record_id), the true
-- per-line natural key, not order_line_lhk.
--
-- INNER joins to link_order_line_product / hub_order / hub_product: an order-line
-- with no resolved product is out of scope for this fact by construction (the raw
-- vault bridge already drops it, br_dv_*_order_lines.sql's own inner join + not-null
-- filter -- see that model's header). LEFT join to the customer, mirroring
-- int_order_header.sql: a resolution gap there surfaces as a null customer_id
-- (caught by the not_null/relationships tests), never a silently dropped line.
--
-- No dim_date join -- see int_order_header.sql's header for why.

with cartivo_lines as (
    select
        'cartivo' as source_system,
        source_record_id,
        order_line_lhk,
        line_number,
        quantity,
        unit_price,
        line_revenue,
        currency,
        effective_from as order_date
    from {{ ref('sat_order_line_cartivo') }}
),

mercaro_lines as (
    select
        'mercaro' as source_system,
        source_record_id,
        order_line_lhk,
        line_number,
        quantity,
        unit_price,
        line_revenue,
        currency,
        effective_from as order_date
    from {{ ref('sat_order_line_mercaro') }}
),

all_lines as (
    select * from cartivo_lines
    union all
    select * from mercaro_lines
),

link as (
    select order_line_lhk, order_hk, product_hk
    from {{ ref('link_order_line_product') }}
),

order_hub as (
    select order_hk, golden_order_key
    from {{ ref('hub_order') }}
),

product_hub as (
    select product_hk, golden_product_key
    from {{ ref('hub_product') }}
),

order_customer_link as (
    select order_hk, customer_hk
    from {{ ref('link_order_customer') }}
),

customer_hub as (
    select customer_hk, golden_customer_key
    from {{ ref('hub_customer') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['all_lines.source_system', 'all_lines.source_record_id']) }} as order_line_key,
    order_hub.golden_order_key as order_id,
    customer_hub.golden_customer_key as customer_id,
    product_hub.golden_product_key as product_id,
    all_lines.order_date,
    all_lines.line_number,
    all_lines.quantity,
    all_lines.unit_price,
    all_lines.line_revenue,
    all_lines.currency,
    all_lines.source_system,
    all_lines.source_record_id
from all_lines
inner join link
    on link.order_line_lhk = all_lines.order_line_lhk
inner join order_hub
    on order_hub.order_hk = link.order_hk
inner join product_hub
    on product_hub.product_hk = link.product_hk
left join order_customer_link
    on order_customer_link.order_hk = link.order_hk
left join customer_hub
    on customer_hub.customer_hk = order_customer_link.customer_hk
