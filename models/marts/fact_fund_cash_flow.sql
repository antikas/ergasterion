{{ config(materialized='table', schema='marts') }}

with cash_flow as (
    select * from {{ ref('canonical_fund_cash_flow') }}
),

fund as (
    select * from {{ ref('dim_fund') }}
),

date_dim as (
    select * from {{ ref('dim_date') }}
)

select
    cash_flow.cash_flow_id as fund_cash_flow_key,
    fund.fund_key,
    date_dim.date_key,
    cash_flow.fund_id,
    cash_flow.cash_flow_date,
    cash_flow.cash_flow_type,
    cash_flow.private_markets_event_type,
    cash_flow.direction,
    cash_flow.amount_usd,
    case
        when cash_flow.private_markets_event_type = 'capital_call'
            then abs(cash_flow.amount_usd)
        else cast(0 as numeric)
    end as called_amount_usd,
    case
        when cash_flow.private_markets_event_type = 'distribution'
            then abs(cash_flow.amount_usd)
        else cast(0 as numeric)
    end as distribution_amount_usd,
    cash_flow.currency,
    cash_flow.source,
    cash_flow.source_cash_flow_id,
    cash_flow.record_source,
    cash_flow.load_datetime
from cash_flow
inner join fund
    on fund.fund_id = cash_flow.fund_id
inner join date_dim
    on date_dim.date_day = cash_flow.cash_flow_date
