-- What this proves: the second relationship a deal states, on the same route.
--
-- One of the ten delivered deal records converted into a fund investment and
-- names the fund it became by the identifier the source systems share; the rest
-- name nothing. The fund vault identifies that fund by its legal entity
-- identifier, so the quoted identifier is resolved through the fund index before
-- the association is keyed, and the link carries one row.
--
-- The arm that matters is the last one. A link is only an association if both
-- ends hash the same identity: the vault that states the relationship and the
-- vault that owns the entity have to agree on the business key, because the
-- identity key is hashed from that key and nothing else. Keying the association
-- on the identifier the delivered feed quotes would hash to something the owning
-- vault has never seen, and the link would count the right number of rows while
-- joining to nothing. So every identity key this link carries is looked up in
-- the owning vault's own hub, by the hash, and the business key behind it is
-- compared as well.
--
-- Singular test: it passes when it returns no rows.
with stated as (
    select count(*) as converted_count
    from {{ ref('investment__origo_deal_extract') }}
    where converted_record_type = 'fund_investment'
      and converted_record_id is not null
      and trim(converted_record_id) <> ''
),

link_rows as (
    select count(*) as row_count
    from {{ ref('investment__deal_vault__link_deal_fund') }}
),

counted as (
    select
        'the link does not hold one row per deal that converted into a fund' as disagreement,
        cast(link_rows.row_count as varchar) || ' of ' || cast(stated.converted_count as varchar) as subject
    from link_rows, stated
    where link_rows.row_count <> stated.converted_count
),

named as (
    select
        'the linked fund is not the fund the deal converted into' as disagreement,
        'ORIGO-EXT-002 to the fund delivered as OPENIM-FUND-ORION-I' as subject
    where not exists (
        select 1
        from {{ ref('investment__deal_vault__link_deal_fund') }} as link
        inner join {{ ref('investment__deal_vault__hub_deal') }} as hub_deal
            on hub_deal.deal_hk = link.deal_hk
        inner join {{ ref('investment__deal_vault__hub_fund') }} as hub_fund
            on hub_fund.fund_hk = link.fund_hk
        where hub_deal.deal_business_key = 'ORIGO-EXT-002'
          and hub_fund.deal_fund_business_key = '549300ORIONCREDIT001'
    )
),

unresolved as (
    select
        'a fund key on the link resolves in no fund vault identity' as disagreement,
        coalesce(carried.deal_fund_business_key, carried.fund_hk) as subject
    from (
        select distinct hub.fund_hk, hub.deal_fund_business_key
        from {{ ref('investment__deal_vault__link_deal_fund') }} as link
        inner join {{ ref('investment__deal_vault__hub_fund') }} as hub
            on hub.fund_hk = link.fund_hk
    ) as carried
    left join {{ ref('investment__fund_vault__hub_fund') }} as owner
        on owner.fund_hk = carried.fund_hk
       and owner.fund_business_key = carried.deal_fund_business_key
    where owner.fund_hk is null
)

select disagreement, subject from counted
union all
select disagreement, subject from named
union all
select disagreement, subject from unresolved
