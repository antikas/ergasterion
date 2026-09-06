-- What this proves: the deal resolution is right in both directions, judged
-- against a statement of the answer the resolution never reads.
--
-- Ten delivered deal records are eight deals: two pairs share an external deal
-- identifier and the other six records are one deal each. The resolution merges
-- on that identifier alone; the delivered cross-reference states the answer and
-- takes no part in the merge.
--
--   No entity spans two deal identities. That is precision.
--
--   No deal identity is spread across two entities. That is recall.
--
--   The vault publishes one surviving deal per identity, which is the count the
--   cross-reference states.
--
-- Singular test: it passes when it returns no rows.
with resolved as (
    select
        base.deal_source_system,
        base.deal_record_id,
        resolution.resolution_entity_key
    from {{ ref('investment__deal_vault__resolution') }} as resolution
    inner join {{ ref('investment__deal_vault__base') }} as base
        on base.deal_identity_key = resolution.resolution_record_key
),

delivered as (
    select
        identity_source_system,
        identity_source_record_id,
        deal_external_identity
    from {{ ref('reference__deal_identity') }}
),

judged as (
    select
        resolved.resolution_entity_key,
        delivered.deal_external_identity
    from resolved
    inner join delivered
        on resolved.deal_source_system = delivered.identity_source_system
       and resolved.deal_record_id = delivered.identity_source_record_id
),

false_merge as (
    select
        'an entity spans two deal identities' as disagreement,
        resolution_entity_key as subject
    from judged
    group by resolution_entity_key
    having count(distinct deal_external_identity) > 1
),

split_identity as (
    select
        'a deal identity is spread across two entities' as disagreement,
        deal_external_identity as subject
    from judged
    group by deal_external_identity
    having count(distinct resolution_entity_key) > 1
),

published as (
    select
        'the vault does not publish one surviving deal per identity' as disagreement,
        cast(count(*) as varchar) as subject
    from {{ ref('investment__deal_vault__golden_deal') }}
    having count(*) <> (select count(distinct deal_external_identity) from delivered)
)

select disagreement, subject from false_merge
union all
select disagreement, subject from split_identity
union all
select disagreement, subject from published
