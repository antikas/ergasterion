{{ config(materialized='table', schema='marts') }}

-- Customer dimension, current-state (type-1), keyed by the
-- e-commerce domain's golden customer id. Mirrors dim_portfolio_company.sql's
-- shape: a thin surrogate-keyed read over the canonical view, no history here --
-- time-varying segment/loyalty-tier is dim_customer_segment (SCD2), a separate
-- dimension, not folded into this one.

with customer as (
    select * from {{ ref('canonical_customer') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['customer_id']) }} as customer_key,
    customer_id,
    loyalty_id,
    email,
    full_name,
    phone,
    address_line,
    city,
    postal_code,
    country,
    marketing_consent,
    preferred_channel,
    buyer_segment,
    customer_status,
    as_of_date,
    customer_resolution_tier,
    customer_resolution_confidence
from customer
