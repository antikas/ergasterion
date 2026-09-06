-- Two results about one customer who arrives from three source systems.
--
-- The first counts the source records the resolution placed under one entity
-- and the entities they were placed under. Three records, one entity, is the
-- collapse.
--
-- The second reads the customer the estate publishes and names the system
-- whose contact values survived. The declared survivorship rule gives contact
-- details to the customer relationship system.
with placed as (
    select
        manifest.source_system,
        manifest.source_record_id,
        resolution.resolution_entity_key
    from ${CATALOG}.${SCHEMA}.reference__customer_overlap_manifest as manifest
    inner join ${CATALOG}.${SCHEMA}.ecommerce__customer__resolution as resolution
        on resolution.customer_source_system = manifest.source_system
       and resolution.customer_record_id = manifest.source_record_id
    where manifest.true_customer_external_id = 'CUST-AVA'
),

collapse as (
    select
        'source records collapsed into one customer' as result,
        cast(count(*) as varchar) || ' records -> '
            || cast(count(distinct resolution_entity_key) as varchar) || ' entity' as value
    from placed
),

surviving as (
    select
        'contact values published for CUST-AVA' as result,
        customer_city || ', ' || customer_phone || ', ' || customer_email
            || ' (from ' || customer_source_system || ')' as value
    from ${CATALOG}.${SCHEMA}.ecommerce__customer
    where customer_external_id = 'CUST-AVA'
)

select result, value from collapse
union all
select result, value from surviving;
