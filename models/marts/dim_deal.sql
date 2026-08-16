{{ config(materialized='table', schema='marts') }}

-- Deal dimension, the same thin surrogate-keyed wrapper pattern as
-- dim_fund/dim_gp/dim_portfolio_company. deal_id is the golden_deal_key produced by
-- res_deal's intra-source resolution; target_company_id and
-- converted_fund_id are the funnel's two exit edges and are nullable by construction.

with deal as (
    select * from {{ ref('canonical_deal') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['deal_id']) }} as deal_key,
    deal_id,
    external_deal_id,
    deal_name,
    target_entity_id,
    target_company_id,
    strategy,
    originating_team,
    source_channel,
    sourced_date,
    structure,
    converted_record_type,
    converted_record_id,
    converted_fund_id,
    deal_resolution_tier,
    deal_resolution_confidence
from deal
