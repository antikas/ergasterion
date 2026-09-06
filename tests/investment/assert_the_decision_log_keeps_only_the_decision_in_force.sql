-- What this proves: latest wins, on an append-only log.
--
-- The delivered decisions carry three events over two items. One item was
-- deferred and then, ten days later, approved with conditions; the other was
-- deferred once. The derivation ranks each item's events by when they were
-- decided and keeps the first, so the item with two events is in force as
-- approved with conditions and the superseded event is excluded and counted.
--
-- Four arms:
--
--   One row per item the log carries, and no more.
--
--   The item with two events is in force as the later of the two.
--
--   The superseded event is still in the landed log, so the append-only history
--   is intact rather than overwritten.
--
--   The predicate logged what it excluded.
--
-- Singular test: it passes when it returns no rows.
with current as (
    select deal_external_identity, decision_outcome, decision_decided_at, decision_is_final
    from {{ ref('investment__deal_decision_current') }}
),

landed as (
    select external_deal_id, decision_id, decision, decided_at
    from {{ ref('investment__deal_decision_log') }}
),

one_per_item as (
    select
        'the decision in force is not one row per item the log carries' as disagreement,
        cast(count(*) as varchar) as subject
    from current
    having count(*) <> (select count(distinct external_deal_id) from landed)
       or count(*) <> count(distinct deal_external_identity)
),

latest_wins as (
    select
        'the decision in force is not the latest event for that item' as disagreement,
        current.deal_external_identity as subject
    from current
    inner join (
        select external_deal_id, max(decided_at) as latest_decided_at
        from landed
        group by external_deal_id
    ) as newest
        on newest.external_deal_id = current.deal_external_identity
    where cast(current.decision_decided_at as varchar) <> newest.latest_decided_at
),

named as (
    select
        'the item that was deferred and then approved is not in force as approved' as disagreement,
        'ORIGO-EXT-DUP-B' as subject
    where not exists (
        select 1 from current
        where deal_external_identity = 'ORIGO-EXT-DUP-B'
          and decision_outcome = 'approve_with_conditions'
          and decision_is_final
    )
),

history_intact as (
    select
        'the superseded decision is no longer in the landed log' as disagreement,
        'DEC-ORIGO-EXT-DUP-B-01' as subject
    where not exists (
        select 1 from landed
        where decision_id = 'DEC-ORIGO-EXT-DUP-B-01' and decision = 'defer'
    )
),

logged as (
    select
        'the latest-wins predicate did not log what it excluded' as disagreement,
        cast(excluded_count as varchar) as subject
    from {{ ref('investment__deal_decision_current__filter_log') }}
    where predicate_name <> 'latest_decision_per_item_only'
       or excluded_count <> (
            (select count(*) from landed) - (select count(distinct external_deal_id) from landed)
       )
)

select disagreement, subject from one_per_item
union all
select disagreement, subject from latest_wins
union all
select disagreement, subject from named
union all
select disagreement, subject from history_intact
union all
select disagreement, subject from logged
