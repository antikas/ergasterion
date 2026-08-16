{{ config(materialized='view', schema='canonical') }}

-- OpenIM PM-15 Deal / Investment Opportunity canonical view over the business-vault
-- golden deal record. Hand-authored, not templated. deal_id is the
-- golden_deal_key produced by res_deal's INTRA-SOURCE resolution (dedup within the
-- single ORIGO CRM feed). Fund resolution is a separate cross-source process. Deal
-- stage history and investment-committee decisions remain separate from the deal
-- master so each keeps its own grain and lifecycle.
--
-- The two conversion edges are read off the raw-vault links: target_company_id from
-- link_deal_target_company -> hub_portfolio_company, converted_fund_id from
-- link_deal_fund_conversion -> hub_fund. Both are left joins picked to one row per
-- deal (a deal has at most one of each), so a deal with no resolved target or that
-- never converted simply carries nulls -- the funnel's memory, exactly as PM-15 says.

with deal as (
    select * from {{ ref('bv_deal_golden_record') }}
),

deal_target as (
    select
        link.deal_hk,
        company.golden_portfolio_company_key,
        row_number() over (
            partition by link.deal_hk
            order by company.golden_portfolio_company_key
        ) as company_rank
    from {{ ref('link_deal_target_company') }} as link
    inner join {{ ref('hub_portfolio_company') }} as company
        on company.portfolio_company_hk = link.portfolio_company_hk
),

deal_conversion as (
    select
        link.deal_hk,
        fund.golden_fund_key,
        row_number() over (
            partition by link.deal_hk
            order by fund.golden_fund_key
        ) as fund_rank
    from {{ ref('link_deal_fund_conversion') }} as link
    inner join {{ ref('hub_fund') }} as fund
        on fund.fund_hk = link.fund_hk
)

select
    deal.golden_deal_key as deal_id,
    deal.source_deal_id,
    deal.external_deal_id,
    deal.deal_name,
    deal.target_entity_id,
    deal_target.golden_portfolio_company_key as target_company_id,
    deal.strategy,
    deal.originating_team,
    deal.source_channel,
    {{ dpf_safe_cast('deal.sourced_date', 'date') }} as sourced_date,
    deal.structure,
    deal.converted_record_type,
    deal.converted_record_id,
    deal_conversion.golden_fund_key as converted_fund_id,
    deal.deal_resolution_tier,
    deal.deal_resolution_confidence,
    deal.hub_load_datetime,
    deal.hub_record_source,
    deal.deal_name__source,
    deal.sourced_date__source
from deal
left join deal_target
    on deal_target.deal_hk = deal.deal_hk
    and deal_target.company_rank = 1
left join deal_conversion
    on deal_conversion.deal_hk = deal.deal_hk
    and deal_conversion.fund_rank = 1
