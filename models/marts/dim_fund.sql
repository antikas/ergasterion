{{ config(materialized='table', schema='marts') }}

with fund as (
    select * from {{ ref('canonical_fund') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['fund_id']) }} as fund_key,
    fund_id,
    shared_external_id,
    fund_name,
    fund_family_id,
    family_name,
    vehicle_type,
    gp_id,
    asset_class,
    strategy,
    vintage_year,
    committed_capital_usd,
    currency,
    domicile,
    fund_status,
    fund_resolution_tier,
    fund_resolution_confidence,
    last_reviewed_at
from fund
