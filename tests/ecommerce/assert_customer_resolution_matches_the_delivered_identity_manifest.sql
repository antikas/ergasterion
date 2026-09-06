-- What this proves: the resolution is right, judged against a statement of
-- the answer that the resolution never reads.
--
-- The estate publishes a delivered cross-reference from each source system's
-- customer record to the identity the business holds it to be. The
-- resolution does not use it: it merges on the loyalty identifier and the
-- normalised address alone. So the cross-reference can be used to judge the
-- merge, in the two directions that matter.
--
--   No entity the resolution formed spans two identities. That is precision:
--   nothing was merged that is not one customer.
--
--   No identity is spread across two entities. That is recall: nothing that
--   is one customer was left in two pieces.
--
-- A third arm keeps the test from passing on an empty join: every record the
-- cross-reference states must be a record the resolution placed, and the
-- customer the estate publishes must be one row per identity.
--
-- Singular test: it passes when it returns no rows.
with resolved as (
    select
        customer_source_system,
        customer_record_id,
        resolution_entity_key
    from {{ ref('ecommerce__customer__resolution') }}
),

delivered as (
    select
        source_system,
        source_record_id,
        true_customer_external_id
    from {{ ref('reference__customer_overlap_manifest') }}
),

judged as (
    select
        resolved.resolution_entity_key,
        delivered.true_customer_external_id
    from resolved
    inner join delivered
        on resolved.customer_source_system = delivered.source_system
       and resolved.customer_record_id = delivered.source_record_id
),

false_merge as (
    select
        'an entity spans two identities' as disagreement,
        resolution_entity_key as subject
    from judged
    group by resolution_entity_key
    having count(distinct true_customer_external_id) > 1
),

split_identity as (
    select
        'an identity is spread across two entities' as disagreement,
        true_customer_external_id as subject
    from judged
    group by true_customer_external_id
    having count(distinct resolution_entity_key) > 1
),

unplaced as (
    select
        'the cross-reference states a record the resolution did not place' as disagreement,
        delivered.source_record_id as subject
    from delivered
    left join resolved
        on resolved.customer_source_system = delivered.source_system
       and resolved.customer_record_id = delivered.source_record_id
    where resolved.customer_record_id is null
),

published as (
    select
        'the published customer is not one row per identity' as disagreement,
        cast(count(*) as varchar) as subject
    from {{ ref('ecommerce__customer') }}
    having count(*) <> (select count(distinct true_customer_external_id) from delivered)
        or count(*) <> count(distinct customer_external_id)
)

select disagreement, subject from false_merge
union all
select disagreement, subject from split_identity
union all
select disagreement, subject from unplaced
union all
select disagreement, subject from published
