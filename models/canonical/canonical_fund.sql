{{ config(materialized='view', schema='canonical') }}

with fund as (
    select * from {{ ref('bv_fund_golden_record') }}
),

fund_gp as (
    select
        link.fund_hk,
        gp.golden_gp_key,
        row_number() over (
            partition by link.fund_hk
            order by link.load_datetime desc, gp.golden_gp_key
        ) as relationship_rank
    from {{ ref('link_fund_gp') }} as link
    inner join {{ ref('hub_gp') }} as gp
        on gp.gp_hk = link.gp_hk
),

-- Cross-source identity union: known_aliases/external_ids are
-- DERIVED from the res_fund-grain per-source union (int_fund_identity_union),
-- never re-taken from the survivorship-collapsed `fund` CTE above.
fund_identity as (
    select * from {{ ref('int_fund_identity_union') }}
)

select
    fund.golden_fund_key as fund_id,
    fund.fund_hk,
    fund.shared_external_id,
    fund.fund_name,
    case
        when fund.fund_family_name is not null
            then {{ stable_golden_key('fund_family', 'fund.fund_family_name') }}
    end as fund_family_id,
    fund.fund_family_name as family_name,
    fund.vehicle_type,
    fund_gp.golden_gp_key as gp_id,
    cast(null as string) as administrator_id,
    fund.asset_class,
    fund.strategy,
    {{ dpf_safe_cast('fund.vintage_year', 'int') }} as vintage_year,
    {{ dpf_safe_cast('fund.committed_capital_usd', 'numeric') }} as committed_capital_usd,
    fund.currency,
    fund.domicile,
    fund.fund_status,
    coalesce(fund_identity.known_aliases, {{ dpf_empty_array('string') }}) as known_aliases,
    fund.lei,
    coalesce(fund_identity.external_ids, '{}') as external_ids,
    {{ dpf_safe_cast('fund.as_of_date', 'date') }} as last_reviewed_at,
    fund.fund_resolution_tier,
    fund.fund_resolution_confidence,
    fund.hub_load_datetime,
    fund.hub_record_source,
    fund.fund_name__source,
    fund.fund_name__load_datetime,
    fund.gp_name__source,
    fund.gp_name__load_datetime,
    fund.asset_class__source,
    fund.asset_class__load_datetime,
    fund.strategy__source,
    fund.strategy__load_datetime,
    fund.committed_capital_usd__source,
    fund.committed_capital_usd__load_datetime,
    fund.fund_status__source,
    fund.fund_status__load_datetime
from fund
left join fund_gp
    on fund_gp.fund_hk = fund.fund_hk
    and fund_gp.relationship_rank = 1
left join fund_identity
    on fund_identity.golden_fund_key = fund.golden_fund_key
