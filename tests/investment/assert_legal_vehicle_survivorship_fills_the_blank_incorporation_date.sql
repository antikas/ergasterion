-- What this proves: a value only one system states survives, and a blank cell
-- in a delivered extract is read as nothing rather than as a date.
--
-- Both systems deliver the same legal vehicle under a name neither identifies
-- with a legal entity identifier, so the normalised name merges them. One of the
-- two delivers a blank incorporation date. The declared strategy for that column
-- is first_non_null, so the date the other system states survives.
--
-- Singular test: it passes when it returns no rows.
with surviving as (
    select
        'the surviving incorporation date is not the one the other system states' as disagreement,
        coalesce(cast(vehicle_incorporated_on as varchar), 'nothing') as subject
    from {{ ref('investment__legal_vehicle_vault__golden_legal_vehicle') }}
    where vehicle_business_key = 'ORIONCREDITSPVB'
      and (vehicle_incorporated_on is null or vehicle_incorporated_on <> date '2023-05-20')
),

blank_read_as_nothing as (
    select
        'the blank incorporation date was read as something' as disagreement,
        vehicle_identity_key as subject
    from {{ ref('investment__legal_vehicle_conformed') }}
    where vehicle_identity_key = 'meridex:VEH-ORION-SPV-B'
      and source_incorporated_on is not null
),

merged as (
    select
        'the two deliveries of that vehicle are not one entity' as disagreement,
        cast(count(*) as varchar) as subject
    from {{ ref('investment__legal_vehicle_vault__golden_legal_vehicle') }}
    where vehicle_business_key = 'ORIONCREDITSPVB'
    having count(*) <> 1
)

select disagreement, subject from surviving
union all
select disagreement, subject from blank_read_as_nothing
union all
select disagreement, subject from merged
