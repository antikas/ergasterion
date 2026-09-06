-- What this proves: the relationship between a deal and the company it targets
-- is a link inside the vault, it holds exactly the deals that state one, and it
-- reaches the company the company vault resolved.
--
-- The delivered feed states a target company for one of its ten deal records and
-- leaves the column blank on the other nine, quoting the identifier the source
-- systems share. The company vault identifies that company by its legal entity
-- identifier instead, so the quoted identifier is resolved through the company
-- index before the association is keyed. A link is a row only where both
-- business keys are present, so the link carries one row.
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
    select count(*) as target_count
    from {{ ref('investment__origo_deal_extract') }}
    where target_entity_id is not null and trim(target_entity_id) <> ''
),

link_rows as (
    select count(*) as row_count
    from {{ ref('investment__deal_vault__link_deal_portfolio_company') }}
),

counted as (
    select
        'the link does not hold one row per deal that states a target company' as disagreement,
        cast(link_rows.row_count as varchar) || ' of ' || cast(stated.target_count as varchar) as subject
    from link_rows, stated
    where link_rows.row_count <> stated.target_count
),

named as (
    select
        'the linked deal is not the deal the feed states a target for' as disagreement,
        'ORIGO-EXT-001 to the company delivered as OPENIM-PC-LUMINA' as subject
    where not exists (
        select 1
        from {{ ref('investment__deal_vault__link_deal_portfolio_company') }} as link
        inner join {{ ref('investment__deal_vault__hub_deal') }} as hub_deal
            on hub_deal.deal_hk = link.deal_hk
        inner join {{ ref('investment__deal_vault__hub_portfolio_company') }} as hub_company
            on hub_company.portfolio_company_hk = link.portfolio_company_hk
        where hub_deal.deal_business_key = 'ORIGO-EXT-001'
          and hub_company.deal_company_business_key = '549300LUMINAHEALTH01'
    )
),

unresolved as (
    select
        'a company key on the link resolves in no company vault identity' as disagreement,
        coalesce(carried.deal_company_business_key, carried.portfolio_company_hk) as subject
    from (
        select distinct hub.portfolio_company_hk, hub.deal_company_business_key
        from {{ ref('investment__deal_vault__link_deal_portfolio_company') }} as link
        inner join {{ ref('investment__deal_vault__hub_portfolio_company') }} as hub
            on hub.portfolio_company_hk = link.portfolio_company_hk
    ) as carried
    left join {{ ref('investment__portfolio_company_vault__hub_portfolio_company') }} as owner
        on owner.portfolio_company_hk = carried.portfolio_company_hk
       and owner.company_business_key = carried.deal_company_business_key
    where owner.portfolio_company_hk is null
)

select disagreement, subject from counted
union all
select disagreement, subject from named
union all
select disagreement, subject from unresolved
