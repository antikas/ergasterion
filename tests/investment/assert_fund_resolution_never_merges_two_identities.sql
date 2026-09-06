-- What this proves: the fund resolution is right where it acts, judged against
-- a statement of the answer the resolution never reads.
--
-- The estate publishes a delivered cross-reference from each source system's
-- fund record to the identity the business holds it to be. The resolution does
-- not use it: it merges on the legal entity identifier and the identifier the
-- source systems share, and nothing else. So the cross-reference can judge the
-- merge, in the direction the declared keys can actually be held to.
--
--   No entity the resolution formed spans two identities. That is precision:
--   nothing was merged that is not one fund.
--
--   Every record the cross-reference states is a record the resolution placed.
--   Without that arm the test would pass on an empty join.
--
-- Recall is deliberately not asserted here. Five records of one fund and two of
-- another carry neither declared identifier, so no declared key can merge them;
-- what the estate does with those is asserted in
-- assert_the_fund_pairs_no_declared_key_merged_are_scored_and_banded.
--
-- Singular test: it passes when it returns no rows.
with resolved as (
    select
        fund_source_system,
        fund_record_id,
        resolution_entity_key
    from {{ ref('investment__fund_vault__resolution') }}
),

delivered as (
    select
        identity_source_system,
        identity_source_record_id,
        fund_external_identity
    from {{ ref('reference__fund_identity') }}
),

judged as (
    select
        resolved.resolution_entity_key,
        delivered.fund_external_identity
    from resolved
    inner join delivered
        on resolved.fund_source_system = delivered.identity_source_system
       and resolved.fund_record_id = delivered.identity_source_record_id
),

false_merge as (
    select
        'an entity spans two fund identities' as disagreement,
        resolution_entity_key as subject
    from judged
    group by resolution_entity_key
    having count(distinct fund_external_identity) > 1
),

unplaced as (
    select
        'the cross-reference states a record the resolution did not place' as disagreement,
        delivered.identity_source_record_id as subject
    from delivered
    left join resolved
        on resolved.fund_source_system = delivered.identity_source_system
       and resolved.fund_record_id = delivered.identity_source_record_id
    where resolved.fund_record_id is null
)

select disagreement, subject from false_merge
union all
select disagreement, subject from unplaced
