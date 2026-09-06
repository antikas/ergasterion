-- What this proves: a relationship between two entities the estate resolved
-- separately, carried as a link inside the vault.
--
-- Every legal vehicle both systems deliver belongs to a fund, and the fund was
-- resolved before the vehicle was curated, so the link is between two agreed
-- identities rather than between two source identifiers. All three vehicles
-- belong to the same fund, so the link carries three rows and the fund hub one.
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
with vehicles as (
    select count(*) as vehicle_count
    from {{ ref('investment__legal_vehicle_vault__golden_legal_vehicle') }}
    where vehicle_fund_business_key is not null
),

link_rows as (
    select count(*) as row_count
    from {{ ref('investment__legal_vehicle_vault__link_legal_vehicle_fund') }}
),

fund_keys as (
    select count(distinct vehicle_fund_business_key) as fund_count
    from {{ ref('investment__legal_vehicle_vault__golden_legal_vehicle') }}
    where vehicle_fund_business_key is not null
),

counted as (
    select
        'the link does not hold one row per vehicle that belongs to a fund' as disagreement,
        cast(link_rows.row_count as varchar) || ' of ' || cast(vehicles.vehicle_count as varchar) as subject
    from link_rows, vehicles
    where link_rows.row_count <> vehicles.vehicle_count
),

hubbed as (
    select
        'the fund hub does not hold one key per fund the vehicles belong to' as disagreement,
        cast(count(*) as varchar) as subject
    from {{ ref('investment__legal_vehicle_vault__hub_fund') }}
    having count(*) <> (select fund_count from fund_keys)
),

unresolved as (
    select
        'a fund key on the link resolves in no fund vault identity' as disagreement,
        coalesce(carried.vehicle_fund_business_key, carried.fund_hk) as subject
    from (
        select distinct hub.fund_hk, hub.vehicle_fund_business_key
        from {{ ref('investment__legal_vehicle_vault__link_legal_vehicle_fund') }} as link
        inner join {{ ref('investment__legal_vehicle_vault__hub_fund') }} as hub
            on hub.fund_hk = link.fund_hk
    ) as carried
    left join {{ ref('investment__fund_vault__hub_fund') }} as owner
        on owner.fund_hk = carried.fund_hk
       and owner.fund_business_key = carried.vehicle_fund_business_key
    where owner.fund_hk is null
)

select disagreement, subject from counted
union all
select disagreement, subject from hubbed
union all
select disagreement, subject from unresolved
