-- What this proves: the named normalisation rule the declaration references is
-- the one the resolution actually merges on.
--
-- Two systems state one logistics company, one as Blue Rail Logistics and the
-- other as BlueRail Logistics. Neither states a legal entity identifier or the
-- identifier the systems share, so the only key left is the normalised name, and
-- the rule strips everything that is not a letter or a digit. The two records
-- are therefore one company.
--
-- The second arm keeps the first honest: two companies whose names really do
-- differ are not merged by the same rule.
--
-- Singular test: it passes when it returns no rows.
with merged as (
    select
        'the two spellings of one company are not one entity' as disagreement,
        cast(count(*) as varchar) as subject
    from {{ ref('investment__portfolio_company_vault__golden_portfolio_company') }}
    where company_business_key = 'BLUERAILLOGISTICS'
    having count(*) <> 1
),

contributors as (
    select
        'the merged company was not stated by two source systems' as disagreement,
        cast(count(distinct company_source_system) as varchar) as subject
    from {{ ref('investment__portfolio_company_conformed') }}
    where company_business_key = 'BLUERAILLOGISTICS'
    having count(distinct company_source_system) <> 2
),

not_over_merged as (
    select
        'two companies with different names were merged' as disagreement,
        company_business_key as subject
    from {{ ref('investment__portfolio_company_conformed') }}
    where company_business_key in ('BLUERAILLOGISTICS', 'ATLASROBOTICS', 'ATLASROBOTICSLTD')
    group by company_business_key
    having count(distinct company_normalised_name) > 1
)

select disagreement, subject from merged
union all
select disagreement, subject from contributors
union all
select disagreement, subject from not_over_merged
