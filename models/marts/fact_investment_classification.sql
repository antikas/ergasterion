{{ config(materialized='table', schema='marts') }}

with valuation as (
    select * from {{ ref('fact_fund_valuation') }}
),

fund as (
    select * from {{ ref('dim_fund') }}
),

department as (
    select * from {{ ref('dim_investment_classification') }}
    where classification_type_code = 'DEPT'
),

sector as (
    select * from {{ ref('dim_investment_classification') }}
    where classification_type_code = 'SECTOR'
),

unknown_department as (
    select investment_classification_key
    from department
    where fund_id = 'UNKNOWN'
      and classification_value_code = 'UNKNOWN'
),

unknown_sector as (
    select investment_classification_key
    from sector
    where fund_id = 'UNKNOWN'
      and classification_value_code = 'UNKNOWN'
)

select
    valuation.fund_valuation_key as investment_classification_fact_key,
    valuation.fund_key,
    valuation.date_key,
    valuation.fund_id,
    fund.shared_external_id,
    valuation.valuation_date as performance_as_of_date,
    coalesce(department.investment_classification_key, unknown_department.investment_classification_key) as department_classification_key,
    coalesce(department.classification_value_code, 'UNKNOWN') as department_code,
    coalesce(department.classification_value_name, 'Unknown') as department_name,
    coalesce(sector.investment_classification_key, unknown_sector.investment_classification_key) as sector_classification_key,
    coalesce(sector.classification_value_code, 'UNKNOWN') as sector_code,
    coalesce(sector.classification_value_name, 'Unknown') as sector_name,
    valuation.nav_usd,
    valuation.valued_company_count
from valuation
inner join fund
    on fund.fund_id = valuation.fund_id
left join department
    on department.fund_id = valuation.fund_id
    and valuation.valuation_date >= department.effective_from
    and valuation.valuation_date < department.effective_to
left join sector
    on sector.fund_id = valuation.fund_id
    and valuation.valuation_date >= sector.effective_from
    and valuation.valuation_date < sector.effective_to
cross join unknown_department
cross join unknown_sector
