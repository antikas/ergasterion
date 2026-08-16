{{ config(materialized='view', schema='canonical') }}

-- Vehicle-grain cash-flow canonical view. Conformed from the vehicle
-- cash-flow satellite, keyed to the vehicle golden key via hub_legal_vehicle. Amounts
-- are signed the same way canonical_fund_cash_flow signs them (capital calls / outflows
-- negative, distributions / inflows positive) so the vehicle->fund aggregation bridge
-- can reconcile the two grains on one consistent measure.

with source_flows as (
    select
        'VANTORA' as source_system,
        sat.*
    from {{ ref('sat_legal_vehicle_cash_flow_vantora') }} as sat
),

signed as (
    select
        {{ dbt_utils.generate_surrogate_key([
            'hub.golden_legal_vehicle_key',
            'source_flows.cash_flow_id'
        ]) }} as vehicle_cash_flow_id,
        hub.golden_legal_vehicle_key as vehicle_id,
        source_flows.cash_flow_id as source_cash_flow_id,
        {{ dpf_safe_cast('source_flows.cash_flow_date', 'date') }} as cash_flow_date,
        case
            when lower(source_flows.cash_flow_type) in ('capital_call', 'call', 'contribution')
                or lower(source_flows.direction) = 'outflow'
                then 'contribution'
            when lower(source_flows.cash_flow_type) in ('distribution', 'return_of_capital', 'income', 'gain')
                or lower(source_flows.direction) = 'inflow'
                then 'distribution'
            else lower(source_flows.cash_flow_type)
        end as cash_flow_type,
        lower(source_flows.direction) as direction,
        case
            when lower(source_flows.direction) = 'outflow'
                then -abs({{ dpf_safe_cast('source_flows.amount_usd', 'numeric') }})
            else abs({{ dpf_safe_cast('source_flows.amount_usd', 'numeric') }})
        end as amount_usd,
        source_flows.currency,
        source_flows.record_source,
        source_flows.load_datetime
    from source_flows
    inner join {{ ref('hub_legal_vehicle') }} as hub
        on hub.legal_vehicle_hk = source_flows.legal_vehicle_hk
),

-- The vehicle cash-flow satellite is keyed on legal_vehicle_hk with MANY flows per
-- vehicle (the fund_cash_flow / fund_hk pattern). AutomateDV's insert-by-hashdiff
-- satellite keeps only one "current" hashdiff per hash key, so on every incremental
-- run it re-inserts the non-current flows of a vehicle -- duplicate rows accumulate.
-- canonical_fund_cash_flow collapses the same effect with a dedupe; we do too, one
-- row per logical vehicle cash flow (vehicle + source flow id), latest load wins.
deduped as (
    select *
    from signed
    qualify row_number() over (
        partition by vehicle_id, source_cash_flow_id
        order by load_datetime desc
    ) = 1
)

select
    vehicle_cash_flow_id,
    vehicle_id,
    source_cash_flow_id,
    cash_flow_date,
    cash_flow_type,
    direction,
    amount_usd,
    currency,
    record_source,
    load_datetime
from deduped
