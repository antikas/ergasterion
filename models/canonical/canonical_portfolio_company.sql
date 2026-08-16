{{ config(materialized='view', schema='canonical') }}

with portfolio_company as (
    select * from {{ ref('bv_portfolio_company_golden_record') }}
)

select
    portfolio_company.golden_portfolio_company_key as company_id,
    portfolio_company.golden_portfolio_company_key as entity_id,
    portfolio_company.portfolio_company_hk,
    portfolio_company.company_name,
    portfolio_company.company_name as entity_name,
    'portfolio_company' as entity_role,
    'corporation' as entity_type,
    {{ dpf_array(['portfolio_company.company_name']) }} as known_aliases,
    portfolio_company.sector,
    portfolio_company.sub_sector,
    portfolio_company.country,
    portfolio_company.lei,
    {{ dpf_to_json_object([
        ['source_company_id', 'portfolio_company.source_company_id'],
        ['source_fund_id', 'portfolio_company.source_fund_id'],
        ['shared_external_id', 'portfolio_company.shared_external_id']
    ]) }} as external_ids,
    portfolio_company.status,
    cast(null as string) as successor_company_id,
    {{ dpf_empty_array('string') }} as gp_relationships,
    {{ dpf_safe_cast('portfolio_company.first_seen_at', 'date') }} as first_seen_at,
    {{ dpf_safe_cast('portfolio_company.first_seen_at', 'date') }} as last_reviewed_at,
    cast(null as string) as reviewed_by,
    portfolio_company.portfolio_company_resolution_tier,
    portfolio_company.portfolio_company_resolution_confidence,
    portfolio_company.hub_load_datetime,
    portfolio_company.hub_record_source,
    portfolio_company.company_name__source,
    portfolio_company.company_name__load_datetime,
    portfolio_company.sector__source,
    portfolio_company.sector__load_datetime,
    portfolio_company.status__source,
    portfolio_company.status__load_datetime
from portfolio_company
