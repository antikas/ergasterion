{{ config(materialized='view', schema='canonical') }}

-- OpenIM PM-05 Legal Vehicle canonical view over the business-vault golden vehicle
-- record. Hand-authored, not templated. The vehicle's parent fund comes
-- from link_investment_vehicle -> hub_fund (the golden_fund_key the emitter resolved
-- through res_fund); the vehicle's OWN identity carries NO entity resolution -- its
-- golden_legal_vehicle_key is the bridge-select hash of its declared natural id.
-- parent_vehicle_id is re-expressed as the parent's golden key so the SPV nesting is
-- a self-reference on this same key space (one seeded feeder exercises it).

with vehicle as (
    select * from {{ ref('bv_legal_vehicle_golden_record') }}
),

vehicle_fund as (
    select
        link.legal_vehicle_hk,
        fund.golden_fund_key,
        row_number() over (
            partition by link.legal_vehicle_hk
            order by fund.golden_fund_key
        ) as fund_rank
    from {{ ref('link_investment_vehicle') }} as link
    inner join {{ ref('hub_fund') }} as fund
        on fund.fund_hk = link.fund_hk
)

select
    vehicle.golden_legal_vehicle_key as vehicle_id,
    vehicle.vehicle_natural_id,
    vehicle_fund.golden_fund_key as fund_id,
    vehicle.source_fund_id,
    case
        when vehicle.parent_vehicle_id is not null
            then {{ stable_golden_key('legal_vehicle', 'vehicle.parent_vehicle_id') }}
    end as parent_vehicle_id,
    vehicle.parent_vehicle_id as parent_vehicle_natural_id,
    vehicle.vehicle_name,
    vehicle.vehicle_type,
    vehicle.jurisdiction,
    {{ dpf_safe_cast('vehicle.incorporation_date', 'date') }} as incorporation_date,
    vehicle.lei,
    vehicle.hub_load_datetime,
    vehicle.hub_record_source,
    vehicle.vehicle_name__source,
    vehicle.incorporation_date__source
from vehicle
left join vehicle_fund
    on vehicle_fund.legal_vehicle_hk = vehicle.legal_vehicle_hk
    and vehicle_fund.fund_rank = 1
