-- What this proves: the worklist is the deals still waiting on somebody, and
-- nothing else.
--
-- The estate holds eight deals. One of them carries a decision that settles it,
-- so it is off the worklist; one carries a deferral, which settles nothing, so it
-- stays on; the other six carry no decision at all. Seven deals therefore reach
-- the worklist, and the worklist plus the settled deals account for every deal
-- the estate holds.
--
-- The stage in force travels with each deal, so a person reads how far the deal
-- got beside the fact that nobody has decided it.
--
-- Singular test: it passes when it returns no rows.
with worklist as (
    select deal_business_key, deal_decision_state, stage_value_code, deal_awaiting_decision
    from {{ ref('investment__deal_review_worklist') }}
),

deals as (
    select deal_business_key from {{ ref('investment__deal_vault__golden_deal') }}
),

settled as (
    select deal_external_identity
    from {{ ref('investment__deal_decision_current') }}
    where decision_is_final
),

accounted as (
    select
        'the worklist and the settled deals do not account for every deal' as disagreement,
        cast((select count(*) from worklist) as varchar) || ' + '
            || cast((select count(*) from settled) as varchar) as subject
    where (select count(*) from worklist) + (select count(*) from settled)
        <> (select count(*) from deals)
),

settled_present as (
    select
        'a deal a decision settles is on the worklist' as disagreement,
        worklist.deal_business_key as subject
    from worklist
    inner join settled on settled.deal_external_identity = worklist.deal_business_key
),

deferred_kept as (
    select
        'the deferred deal is not on the worklist' as disagreement,
        'ORIGO-EXT-001' as subject
    where not exists (
        select 1 from worklist
        where deal_business_key = 'ORIGO-EXT-001'
          and deal_decision_state = 'defer'
          and stage_value_code = 'DECISION'
    )
),

undecided_kept as (
    select
        'a deal nobody has decided is not on the worklist' as disagreement,
        deals.deal_business_key as subject
    from deals
    left join {{ ref('investment__deal_decision_current') }} as decided
        on decided.deal_external_identity = deals.deal_business_key
    left join worklist on worklist.deal_business_key = deals.deal_business_key
    where decided.deal_external_identity is null
      and worklist.deal_business_key is null
),

awaiting as (
    select
        'a row on the worklist is not marked as awaiting a decision' as disagreement,
        deal_business_key as subject
    from worklist
    where deal_awaiting_decision is distinct from true
)

select disagreement, subject from accounted
union all
select disagreement, subject from settled_present
union all
select disagreement, subject from deferred_kept
union all
select disagreement, subject from undecided_kept
union all
select disagreement, subject from awaiting
