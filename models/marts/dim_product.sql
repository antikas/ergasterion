{{ config(materialized='table', schema='marts') }}

-- Product dimension, current-state (type-1), keyed by the
-- e-commerce domain's golden product id. Mirrors dim_portfolio_company.sql's shape.

with product as (
    select * from {{ ref('canonical_product') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['product_id']) }} as product_key,
    product_id,
    shared_product_code,
    product_name,
    brand,
    category,
    list_price,
    currency,
    product_status,
    as_of_date,
    product_resolution_tier,
    product_resolution_confidence
from product
