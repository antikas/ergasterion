{{ config(materialized='table', schema='marts') }}

with event_dates as (
    select cash_flow_date as date_day
    from {{ ref('canonical_fund_cash_flow') }}

    union all

    select valuation_date as date_day
    from {{ ref('canonical_fund_valuation') }}
),

date_bounds as (
    select
        coalesce(min(date_day), date '2000-01-01') as min_date,
        coalesce(max(date_day), current_date()) as max_date
    from event_dates
),

date_spine as (
    {{ dpf_date_series('(select min_date from date_bounds)', '(select max_date from date_bounds)') }}
)

select
    {{ dpf_date_key('date_day') }} as date_key,
    date_day,
    extract(year from date_day) as calendar_year,
    extract(quarter from date_day) as calendar_quarter,
    extract(month from date_day) as calendar_month,
    extract(day from date_day) as day_of_month,
    {{ dpf_date_trunc('month', 'date_day') }} as month_start_date,
    {{ dpf_date_trunc('quarter', 'date_day') }} as quarter_start_date,
    {{ dpf_date_trunc('year', 'date_day') }} as year_start_date
from date_spine
