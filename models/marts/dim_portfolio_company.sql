{{ config(materialized='table', schema='marts') }}

with portfolio_company as (
    select * from {{ ref('canonical_portfolio_company') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['company_id']) }} as portfolio_company_key,
    company_id,
    entity_id,
    company_name,
    entity_name,
    entity_role,
    entity_type,
    sector,
    sub_sector,
    country,
    lei,
    status,
    first_seen_at,
    last_reviewed_at,
    portfolio_company_resolution_tier,
    portfolio_company_resolution_confidence
from portfolio_company
