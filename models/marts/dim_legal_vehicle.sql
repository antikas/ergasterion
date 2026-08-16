{{ config(materialized='table', schema='marts') }}

-- Legal-vehicle (SPV) dimension keyed by the OpenIM golden vehicle id.
-- parent_legal_vehicle_key is a self-reference into this same dimension: the seeded
-- feeder vehicle points at its parent SPV, so the SPV nesting is navigable here.

with vehicle as (
    select * from {{ ref('canonical_legal_vehicle') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['vehicle_id']) }} as legal_vehicle_key,
    vehicle_id,
    fund_id,
    case
        when parent_vehicle_id is not null
            then {{ dbt_utils.generate_surrogate_key(['parent_vehicle_id']) }}
    end as parent_legal_vehicle_key,
    parent_vehicle_id,
    vehicle_name,
    vehicle_type,
    jurisdiction,
    incorporation_date,
    lei
from vehicle
