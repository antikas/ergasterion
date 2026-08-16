{{ config(materialized='view', schema='canonical') }}

with source_valuations as (
    select
        sat.*
    from {{ ref('sat_fund_valuation_vantora') }} as sat
),

conformed as (
    select
        {{ dbt_utils.generate_surrogate_key([
            'hub_fund.golden_fund_key',
            'source_valuations.golden_portfolio_company_key',
            dpf_safe_cast('source_valuations.valuation_date', 'string'),
            'source_valuations.method',
            'source_valuations.valuation_level'
        ]) }} as valuation_id,
        cast(null as string) as position_id,
        hub_fund.golden_fund_key as instrument_id,
        cast(null as string) as unit_class_id,
        hub_fund.golden_fund_key as fund_id,
        source_valuations.golden_portfolio_company_key as company_id,
        source_valuations.valuation_id as source_valuation_id,
        source_valuations.source_fund_id,
        source_valuations.source_company_id,
        {{ dpf_safe_cast('source_valuations.valuation_date', 'date') }} as valuation_date,
        {{ dpf_safe_cast('source_valuations.value_usd', 'numeric') }} as value_usd,
        source_valuations.method,
        source_valuations.valuation_level,
        source_valuations.record_source as source,
        {{ dpf_safe_cast('source_valuations.confidence_score', 'float') }} as confidence_score,
        source_valuations.load_datetime,
        source_valuations.fund_resolution_tier,
        source_valuations.fund_resolution_confidence,
        source_valuations.portfolio_company_resolution_tier,
        source_valuations.portfolio_company_resolution_confidence
    from source_valuations
    inner join {{ ref('hub_fund') }} as hub_fund
        on hub_fund.fund_hk = source_valuations.fund_hk
),

deduped as (
    select *
    from conformed
    qualify row_number() over (
        partition by fund_id, company_id, valuation_date, method, valuation_level
        order by load_datetime desc, source_valuation_id
    ) = 1
)

select *
from deduped
