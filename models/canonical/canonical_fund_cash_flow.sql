{{ config(materialized='view', schema='canonical') }}

with source_cash_flows as (
    select
        'VANTORA' as source_system,
        1 as source_priority,
        sat.*
    from {{ ref('sat_fund_cash_flow_vantora') }} as sat

    union all

    select
        'MERIDEX' as source_system,
        2 as source_priority,
        sat.*
    from {{ ref('sat_fund_cash_flow_meridex') }} as sat

    union all

    select
        'PORTIQ' as source_system,
        3 as source_priority,
        sat.*
    from {{ ref('sat_fund_cash_flow_portiq') }} as sat
),

conformed as (
    select
        {{ dbt_utils.generate_surrogate_key([
            'hub.golden_fund_key',
            dpf_safe_cast('source_cash_flows.cash_flow_date', 'string'),
            'lower(source_cash_flows.cash_flow_type)',
            'cast(' ~ dpf_safe_cast('source_cash_flows.amount_usd', 'numeric') ~ ' as string)',
            'source_cash_flows.currency'
        ]) }} as cash_flow_id,
        hub.golden_fund_key as fund_id,
        cast(null as string) as portfolio_id,
        hub.golden_fund_key as instrument_id,
        cast(null as string) as transaction_id,
        source_cash_flows.cash_flow_id as source_cash_flow_id,
        source_cash_flows.source_fund_id,
        {{ dpf_safe_cast('source_cash_flows.cash_flow_date', 'date') }} as cash_flow_date,
        case
            when lower(source_cash_flows.cash_flow_type) in ('capital_call', 'call', 'contribution')
                or lower(source_cash_flows.direction) = 'outflow'
                then 'contribution'
            when lower(source_cash_flows.cash_flow_type) in ('distribution', 'return_of_capital', 'income', 'gain')
                or lower(source_cash_flows.direction) = 'inflow'
                then 'distribution'
            else lower(source_cash_flows.cash_flow_type)
        end as cash_flow_type,
        case
            when lower(source_cash_flows.cash_flow_type) in ('capital_call', 'call', 'contribution')
                or lower(source_cash_flows.direction) = 'outflow'
                then 'capital_call'
            when lower(source_cash_flows.cash_flow_type) in ('distribution', 'return_of_capital', 'income', 'gain')
                or lower(source_cash_flows.direction) = 'inflow'
                then 'distribution'
            else lower(source_cash_flows.cash_flow_type)
        end as private_markets_event_type,
        lower(source_cash_flows.direction) as direction,
        case
            when lower(source_cash_flows.direction) = 'outflow'
                then -abs({{ dpf_safe_cast('source_cash_flows.amount_usd', 'numeric') }})
            else abs({{ dpf_safe_cast('source_cash_flows.amount_usd', 'numeric') }})
        end as amount,
        case
            when lower(source_cash_flows.direction) = 'outflow'
                then -abs({{ dpf_safe_cast('source_cash_flows.amount_usd', 'numeric') }})
            else abs({{ dpf_safe_cast('source_cash_flows.amount_usd', 'numeric') }})
        end as amount_usd,
        source_cash_flows.currency,
        source_cash_flows.source_system as source,
        source_cash_flows.record_source,
        source_cash_flows.load_datetime,
        source_cash_flows.fund_resolution_tier,
        source_cash_flows.fund_resolution_confidence,
        source_cash_flows.source_priority
    from source_cash_flows
    inner join {{ ref('hub_fund') }} as hub
        on hub.fund_hk = source_cash_flows.fund_hk
),

deduped as (
    select *
    from conformed
    qualify row_number() over (
        partition by fund_id, cash_flow_date, cash_flow_type, amount_usd, currency
        order by source_priority, load_datetime desc, source_cash_flow_id
    ) = 1
)

select
    cash_flow_id,
    fund_id,
    portfolio_id,
    instrument_id,
    transaction_id,
    source_cash_flow_id,
    source_fund_id,
    cash_flow_date,
    cash_flow_type,
    private_markets_event_type,
    direction,
    amount,
    amount_usd,
    currency,
    source,
    record_source,
    load_datetime,
    fund_resolution_tier,
    fund_resolution_confidence
from deduped
