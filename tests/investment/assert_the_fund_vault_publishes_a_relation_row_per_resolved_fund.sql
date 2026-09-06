-- What this proves: every relation the fund vault renders carries the entities
-- the composition resolved, and not some other number.
--
-- The resolution forms one entity per fund it recognises. The hub stores one
-- row per entity, each satellite one version per entity on the first build, the
-- point-in-time relation one snapshot per entity, and the surviving record one
-- row per entity. The manager hub stores one row per distinct manager the
-- surviving funds name, which is fewer, because several funds name one manager.
--
-- Every count is read from the resolution rather than stated here, so the test
-- keeps its meaning if the delivered population changes.
--
-- Singular test: it passes when it returns no rows.
with entities as (
    select count(distinct resolution_entity_key) as entity_count
    from {{ ref('investment__fund_vault__resolution') }}
),

counted (relation_name, row_count) as (
    values
        ('hub_fund', (select count(*) from {{ ref('investment__fund_vault__hub_fund') }})),
        ('sat_fund_detail', (select count(*) from {{ ref('investment__fund_vault__sat_fund_detail') }})),
        ('sat_fund_observed', (select count(*) from {{ ref('investment__fund_vault__sat_fund_observed') }})),
        ('pit_fund', (select count(*) from {{ ref('investment__fund_vault__pit_fund') }})),
        ('golden_fund', (select count(*) from {{ ref('investment__fund_vault__golden_fund') }})),
        ('link_fund_gp', (select count(*) from {{ ref('investment__fund_vault__link_fund_gp') }}))
),

per_entity as (
    select
        'the relation does not carry one row per resolved fund' as disagreement,
        relation_name || ' has ' || cast(row_count as varchar) as subject
    from counted, entities
    where row_count <> entities.entity_count
),

manager_hub as (
    select
        'the manager hub does not carry one row per distinct manager the funds name' as disagreement,
        cast(count(*) as varchar) as subject
    from {{ ref('investment__fund_vault__hub_gp') }}
    having count(*) <> (
        select count(distinct fund_gp_business_key)
        from {{ ref('investment__fund_vault__golden_fund') }}
    )
)

select disagreement, subject from per_entity
union all
select disagreement, subject from manager_hub
