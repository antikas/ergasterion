-- What this proves: the manager association a fund states is a link inside the
-- fund vault, and it reaches the manager the manager vault resolved.
--
-- Every fund the estate recognises names a manager, so the link carries one row
-- per fund and the manager hub one key per distinct manager those funds name.
-- Three of the four source systems state no manager identifier beside the name,
-- so the name is resolved through the manager index before the association is
-- keyed; without that resolution three of the eight manager keys would be a
-- normalised name the manager vault does not know.
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
with funds as (
    select count(*) as fund_count from {{ ref('investment__fund_vault__golden_fund') }}
),

link_rows as (
    select count(*) as row_count from {{ ref('investment__fund_vault__link_fund_gp') }}
),

counted as (
    select
        'the link does not hold one row per fund the estate recognises' as disagreement,
        cast(link_rows.row_count as varchar) || ' of ' || cast(funds.fund_count as varchar) as subject
    from link_rows, funds
    where link_rows.row_count <> funds.fund_count
),

hubbed as (
    select
        'the manager hub does not hold one key per manager the funds name' as disagreement,
        cast(count(*) as varchar) as subject
    from {{ ref('investment__fund_vault__hub_gp') }}
    having count(*) <> (
        select count(distinct fund_gp_business_key)
        from {{ ref('investment__fund_vault__golden_fund') }}
    )
),

unresolved as (
    select
        'a manager key on the link resolves in no manager vault identity' as disagreement,
        coalesce(named.fund_gp_business_key, named.gp_hk) as subject
    from (
        select distinct hub.gp_hk, hub.fund_gp_business_key
        from {{ ref('investment__fund_vault__link_fund_gp') }} as link
        inner join {{ ref('investment__fund_vault__hub_gp') }} as hub
            on hub.gp_hk = link.gp_hk
    ) as named
    left join {{ ref('investment__gp_vault__hub_gp') }} as owner
        on owner.gp_hk = named.gp_hk
       and owner.gp_business_key = named.fund_gp_business_key
    where owner.gp_hk is null
)

select disagreement, subject from counted
union all
select disagreement, subject from hubbed
union all
select disagreement, subject from unresolved
