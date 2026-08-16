{{ config(materialized='table', schema='marts') }}

-- GP dimension, extended with manager-succession lineage columns.
-- predecessor_gp_key / successor_gp_key are self-references into this same
-- dimension -- the dim_legal_vehicle parent_legal_vehicle_key pattern: a GP that
-- is a succession PREDECESSOR carries its successor's key; a GP that is a
-- succession SUCCESSOR carries its predecessor's key. lineage_status is 'active'
-- for a GP untouched by any succession event, 'superseded' for a predecessor,
-- 'current' for a successor.
--
-- predecessor_links/successor_links are pre-aggregated (max, not a plain join)
-- so a GP appearing on both sides of separate events (a future chain A->B->C)
-- still yields exactly one dim_gp row per GP -- no join fanout. MAX is exact
-- for the seeded single event and for pure chains (each GP has at most one
-- predecessor and one successor). Under a FORK -- one predecessor to multiple
-- successors, or the symmetric merge case -- MAX keeps only one arbitrary
-- (lexically max) key per GP; full lineage in that case needs a dedicated
-- bridge, not this aggregation. No fork is seeded today.

with gp as (
    select * from {{ ref('canonical_gp') }}
),

predecessor_links as (
    select
        predecessor_gp_id as gp_id,
        max(successor_gp_id) as successor_gp_id
    from {{ ref('canonical_manager_succession') }}
    group by predecessor_gp_id
),

successor_links as (
    select
        successor_gp_id as gp_id,
        max(predecessor_gp_id) as predecessor_gp_id
    from {{ ref('canonical_manager_succession') }}
    group by successor_gp_id
)

select
    {{ dbt_utils.generate_surrogate_key(['gp.gp_id']) }} as gp_key,
    gp.gp_id,
    gp.entity_id,
    gp.gp_name,
    gp.entity_name,
    gp.entity_role,
    gp.entity_type,
    gp.lei,
    gp.domicile,
    gp.relationship_start_date,
    gp.gp_resolution_tier,
    gp.gp_resolution_confidence,
    case
        when predecessor_links.successor_gp_id is not null
            then {{ dbt_utils.generate_surrogate_key(['predecessor_links.successor_gp_id']) }}
    end as successor_gp_key,
    case
        when successor_links.predecessor_gp_id is not null
            then {{ dbt_utils.generate_surrogate_key(['successor_links.predecessor_gp_id']) }}
    end as predecessor_gp_key,
    case
        when predecessor_links.successor_gp_id is not null then 'superseded'
        when successor_links.predecessor_gp_id is not null then 'current'
        else 'active'
    end as lineage_status
from gp
left join predecessor_links
    on predecessor_links.gp_id = gp.gp_id
left join successor_links
    on successor_links.gp_id = gp.gp_id
