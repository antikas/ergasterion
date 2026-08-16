{{ config(materialized='table', schema='marts') }}

-- Look-through exposure: rolls each fund's portfolio-company valuations up into
-- sector / geography exposure at (fund, company, date) grain, beyond the
-- fund-level NAV aggregation already done in fact_fund_valuation. There is no
-- position-level instrument-type field on any source (checked staging +
-- canonical_portfolio_company): the closest already-modelled proxy for
-- "instrument" is the fund's own vehicle_type/asset_class (the wrapper the
-- look-through exposure is held through), so that is what is exposed here
-- rather than inventing an unsupported source field.

with valuation as (
    select * from {{ ref('canonical_fund_valuation') }}
),

-- Canonical valuations are deduped per (fund_id, company_id, valuation_date,
-- method, valuation_level), so a company can still carry more than one method
-- on the same date (same pattern fact_fund_valuation already sums through at
-- fund grain). Collapse to one row per (fund, company, date) here too.
company_valuation as (
    select
        valuation.fund_id,
        valuation.company_id,
        valuation.valuation_date,
        sum(valuation.value_usd) as exposure_value_usd,
        {{ dpf_string_agg('valuation.method', "', '", order_by='valuation.method', distinct=true) }} as valuation_methods,
        max(valuation.load_datetime) as load_datetime
    from valuation
    group by valuation.fund_id, valuation.company_id, valuation.valuation_date
),

fund as (
    select * from {{ ref('dim_fund') }}
),

portfolio_company as (
    select * from {{ ref('dim_portfolio_company') }}
),

date_dim as (
    select * from {{ ref('dim_date') }}
),

fund_nav as (
    select * from {{ ref('fact_fund_valuation') }}
)

select
    {{ dbt_utils.generate_surrogate_key([
        'company_valuation.fund_id',
        'company_valuation.company_id',
        dpf_safe_cast('company_valuation.valuation_date', 'string')
    ]) }} as portfolio_exposure_key,
    fund.fund_key,
    portfolio_company.portfolio_company_key,
    date_dim.date_key,
    company_valuation.fund_id,
    company_valuation.company_id,
    company_valuation.valuation_date as exposure_date,
    company_valuation.exposure_value_usd,
    company_valuation.valuation_methods,
    portfolio_company.sector as exposure_sector,
    portfolio_company.sub_sector as exposure_sub_sector,
    portfolio_company.country as exposure_country,
    fund.vehicle_type as exposure_instrument_type,
    fund.asset_class as exposure_asset_class,
    fund_nav.nav_usd as fund_total_nav_usd,
    {{ dpf_safe_divide('company_valuation.exposure_value_usd', 'fund_nav.nav_usd') }} as pct_of_fund_nav,
    company_valuation.load_datetime
from company_valuation
inner join fund
    on fund.fund_id = company_valuation.fund_id
inner join date_dim
    on date_dim.date_day = company_valuation.valuation_date
left join portfolio_company
    on portfolio_company.company_id = company_valuation.company_id
left join fund_nav
    on fund_nav.fund_id = company_valuation.fund_id
    and fund_nav.date_key = date_dim.date_key
