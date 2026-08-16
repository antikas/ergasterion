{{ config(materialized='table', schema='business_vault') }}

with hub as (
    select
        fund_hk,
        golden_fund_key,
        load_datetime as hub_load_datetime
    from {{ ref('hub_fund') }}
),

load_events as (
    select fund_hk, hub_load_datetime as load_datetime from hub
    union all
    select fund_hk, load_datetime from {{ ref('sat_fund_vantora') }}
    union all
    select fund_hk, load_datetime from {{ ref('sat_fund_meridex') }}
    union all
    select fund_hk, load_datetime from {{ ref('sat_fund_portiq') }}
),

as_of_points as (
    select
        fund_hk,
        max(load_datetime) as as_of_datetime
    from load_events
    group by fund_hk
)

select
    hub.fund_hk,
    hub.golden_fund_key,
    as_of_points.as_of_datetime,
    (
        select max(sat.load_datetime)
        from {{ ref('sat_fund_vantora') }} as sat
        where sat.fund_hk = hub.fund_hk
          and sat.load_datetime <= as_of_points.as_of_datetime
    ) as sat_fund_vantora_load_datetime,
    (
        select max(sat.load_datetime)
        from {{ ref('sat_fund_meridex') }} as sat
        where sat.fund_hk = hub.fund_hk
          and sat.load_datetime <= as_of_points.as_of_datetime
    ) as sat_fund_meridex_load_datetime,
    (
        select max(sat.load_datetime)
        from {{ ref('sat_fund_portiq') }} as sat
        where sat.fund_hk = hub.fund_hk
          and sat.load_datetime <= as_of_points.as_of_datetime
    ) as sat_fund_portiq_load_datetime
from hub
left join as_of_points
    on as_of_points.fund_hk = hub.fund_hk
