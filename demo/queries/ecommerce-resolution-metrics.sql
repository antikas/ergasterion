with ava_source_rows as (
    select
        manifest.source_system,
        manifest.source_record_id,
        resolved.golden_customer_key
    from ${CATALOG}.${RAW_SCHEMA}.customer_er_overlap_manifest manifest
    join ${CATALOG}.${RESOLUTION_SCHEMA}.res_customer resolved
        on resolved.source_system = manifest.source_system
        and resolved.source_record_id = manifest.source_record_id
    where manifest.true_customer_external_id = 'CUST-AVA'
),
collapse as (
    select
        'ava_tri_source_collapse' as check_name,
        cast(count(*) as varchar) as expected,
        cast(count(*) as varchar) || ' source rows -> '
            || cast(count(distinct golden_customer_key) as varchar) || ' golden key(s)' as actual,
        case when count(*) = 3 and count(distinct golden_customer_key) = 1
            then 'PASS' else 'FAIL' end as result
    from ava_source_rows
),
crm_wins as (
    select
        'crm_wins_contact_city' as check_name,
        'Manchester (RELATIO)' as expected,
        coalesce(canonical.city, 'NULL') || ' (' || coalesce(canonical.city__source, 'NULL') || ')' as actual,
        case when canonical.city = 'Manchester' and canonical.city__source = 'RELATIO'
            then 'PASS' else 'FAIL' end as result
    from ${CATALOG}.${CANONICAL_SCHEMA}.canonical_customer canonical
    where canonical.customer_id in (select distinct golden_customer_key from ava_source_rows)
)
select check_name, expected, actual, result from collapse
union all
select check_name, expected, actual, result from crm_wins;
