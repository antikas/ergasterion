-- A converted deal's conversion edge links to a real fund golden key.
--
-- The seeded Orion Credit Facility deal (external_deal_id ORIGO-EXT-002) reaches
-- COMMITTED and converts to fund OPENIM-FUND-ORION-I. This asserts on that deal
-- deliberately. A claimed conversion whose fund lookup misses shows a
-- NULL edge, a known silent-degradation risk for OTHER deals with unresolved
-- conversions -- guarded separately, not here). This test proves the positive case:
-- fact_deal_pipeline.converted_fund_id is populated for Orion AND the populated key
-- actually exists in dim_fund as a real golden fund, not a dangling id.

with orion_pipeline as (
    select
        deal_id,
        converted_record_type,
        converted_record_id,
        converted_fund_id
    from {{ ref('fact_deal_pipeline') }}
    where deal_id in (
        select deal_id
        from {{ ref('canonical_deal') }}
        where external_deal_id = 'ORIGO-EXT-002'
    )
),

no_orion_row as (
    select 'orion_deal_missing_from_pipeline' as failure_type, 'ORIGO-EXT-002' as failure_key
    where not exists (select 1 from orion_pipeline)
),

missing_conversion as (
    select 'orion_conversion_not_populated' as failure_type, 'ORIGO-EXT-002' as failure_key
    from orion_pipeline
    where converted_fund_id is null
),

dangling_conversion as (
    select
        'converted_fund_id_not_a_real_golden_fund' as failure_type,
        orion_pipeline.deal_id as failure_key
    from orion_pipeline
    left join {{ ref('dim_fund') }} as fund
        on fund.fund_id = orion_pipeline.converted_fund_id
    where orion_pipeline.converted_fund_id is not null
      and fund.fund_id is null
)

select failure_type, failure_key from no_orion_row
union all
select failure_type, failure_key from missing_conversion
union all
select failure_type, failure_key from dangling_conversion
