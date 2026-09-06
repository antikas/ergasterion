-- What this proves: the deterministic resolution the curated customer
-- declares merges the customer records the three delivered feeds state more
-- than once, and merges nothing else.
--
-- The delivered feeds carry eight customer records for five customers. One
-- customer is stated by all three systems under one loyalty identifier and
-- three different email addresses; one is stated by two systems under no
-- loyalty identifier at all and one email address the two spell with
-- different capitals; the other three are stated once each. The resolution
-- declares two ranked keys, the loyalty identifier and the normalised
-- address, and this asserts what each of them merged: three records into one
-- customer on the loyalty identifier, two into one on the address, and every
-- other record left as the one record it is.
--
-- Singular test: it passes when it returns no rows.
with resolved as (
    select
        customer_external_id,
        resolution_entity_key,
        resolution_record_key,
        resolution_key_name,
        resolution_pending
    from {{ ref('ecommerce__customer__resolution') }}
),

observed as (
    select
        customer_external_id,
        count(*) as delivered_records,
        count(distinct resolution_entity_key) as entities,
        min(resolution_key_name) as merged_on,
        max(case when resolution_pending then 1 else 0 end) as any_pending
    from resolved
    group by customer_external_id
),

expected (customer_external_id, delivered_records, entities, merged_on, any_pending) as (
    values
        ('CUST-AVA', 3, 1, 'customer_loyalty_id', 0),
        ('CUST-BEN', 2, 1, 'customer_normalised_email', 0),
        ('CUST-CHLOE', 1, 1, 'customer_loyalty_id', 1),
        ('CUST-DIEGO', 1, 1, 'customer_loyalty_id', 1),
        ('CUST-ELENA', 1, 1, 'customer_loyalty_id', 1)
),

per_customer as (
    select
        coalesce(observed.customer_external_id, expected.customer_external_id) as customer_external_id,
        'resolution disagrees with the delivered overlap' as disagreement
    from observed
    full join expected
        on observed.customer_external_id = expected.customer_external_id
    where observed.customer_external_id is null
       or expected.customer_external_id is null
       or observed.delivered_records <> expected.delivered_records
       or observed.entities <> expected.entities
       or observed.merged_on <> expected.merged_on
       or observed.any_pending <> expected.any_pending
),

totals as (
    select
        'the estate resolved a different population' as disagreement,
        count(*) as delivered_records,
        count(distinct resolution_entity_key) as entities
    from resolved
)

select customer_external_id, disagreement from per_customer
union all
select cast(null as varchar) as customer_external_id, disagreement
from totals
where delivered_records <> 8 or entities <> 5
