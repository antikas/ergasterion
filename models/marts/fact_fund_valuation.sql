{{ config(materialized='table', schema='marts') }}

with valuation as (
    select * from {{ ref('canonical_fund_valuation') }}
),

fund as (
    select * from {{ ref('dim_fund') }}
),

portfolio_company as (
    select * from {{ ref('dim_portfolio_company') }}
),

date_dim as (
    select * from {{ ref('dim_date') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['valuation.fund_id', dpf_safe_cast('valuation.valuation_date', 'string')]) }} as fund_valuation_key,
    fund.fund_key,
    date_dim.date_key,
    valuation.fund_id,
    valuation.valuation_date,
    sum(valuation.value_usd) as nav_usd,
    count(distinct valuation.company_id) as valued_company_count,
    {{ dpf_string_agg('valuation.method', "', '", order_by='valuation.method', distinct=true) }} as valuation_methods,
    min(portfolio_company.portfolio_company_key) as representative_portfolio_company_key,
    avg(valuation.confidence_score) as average_confidence_score,
    max(valuation.load_datetime) as load_datetime
from valuation
inner join fund
    on fund.fund_id = valuation.fund_id
inner join date_dim
    on date_dim.date_day = valuation.valuation_date
left join portfolio_company
    on portfolio_company.company_id = valuation.company_id
group by
    fund_valuation_key,
    fund.fund_key,
    date_dim.date_key,
    valuation.fund_id,
    valuation.valuation_date
