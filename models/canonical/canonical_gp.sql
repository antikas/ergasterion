{{ config(materialized='view', schema='canonical') }}

with gp as (
    select * from {{ ref('bv_gp_golden_record') }}
)

select
    gp.golden_gp_key as gp_id,
    gp.golden_gp_key as entity_id,
    gp.gp_hk,
    gp.gp_name,
    gp.gp_name as entity_name,
    'manager' as entity_role,
    'management_company' as entity_type,
    {{ dpf_array(['gp.gp_name']) }} as known_aliases,
    gp.lei,
    gp.domicile,
    {{ dpf_safe_cast('gp.relationship_start_date', 'date') }} as relationship_start_date,
    {{ dpf_to_json_object([
        ['source_gp_id', 'gp.source_gp_id'],
        ['source_fund_id', 'gp.source_fund_id'],
        ['shared_external_id', 'gp.shared_external_id']
    ]) }} as external_ids,
    gp.gp_resolution_tier,
    gp.gp_resolution_confidence,
    gp.hub_load_datetime,
    gp.hub_record_source,
    gp.gp_name__source,
    gp.gp_name__load_datetime,
    gp.lei__source,
    gp.lei__load_datetime,
    gp.domicile__source,
    gp.domicile__load_datetime,
    gp.relationship_start_date__source,
    gp.relationship_start_date__load_datetime
from gp
