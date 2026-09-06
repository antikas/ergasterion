-- What this proves: the seeded near-duplicate case reaches a person rather than
-- being merged or dropped.
--
-- Two delivered deal records, D-009 and D-010, carry no external deal
-- identifier, so no declared key merges them. They name the same asset two ways
-- (Riverstone Logistics and Riverstone Logistix), they were sourced on the same
-- day by the same desk, and they disagree on strategy. The declared rule scores
-- the pair on those three attributes, and the score falls between the two band
-- edges the estate declares, so the pair is for review.
--
-- Three arms:
--
--   The pair is in the evidence and its band is review.
--
--   It is the only pair in that band, so the band is not a bucket everything
--   falls into.
--
--   The two records are still two entities. A pair for review is not a merge.
--
-- Singular test: it passes when it returns no rows.
with pairs as (
    select record_key_a, record_key_b, match_score, review_band
    from {{ ref('investment__deal_vault__pending_keys') }}
),

the_pair as (
    select
        'the near-duplicate deal pair is not in the review band' as disagreement,
        'origo:D-009 with origo:D-010' as subject
    where not exists (
        select 1 from pairs
        where record_key_a = 'origo:D-009'
          and record_key_b = 'origo:D-010'
          and review_band = 'review'
          and match_score > 0.65
          and match_score < 0.85
    )
),

only_pair as (
    select
        'another pair is in the review band' as disagreement,
        record_key_a || ' with ' || record_key_b as subject
    from pairs
    where review_band = 'review'
      and not (record_key_a = 'origo:D-009' and record_key_b = 'origo:D-010')
),

not_merged as (
    select
        'the reviewed pair was merged into one deal' as disagreement,
        cast(count(distinct base.deal_business_key) as varchar) as subject
    from {{ ref('investment__deal_vault__resolution') }} as resolution
    inner join {{ ref('investment__deal_vault__base') }} as base
        on base.deal_identity_key = resolution.resolution_record_key
    where resolution.resolution_record_key in ('origo:D-009', 'origo:D-010')
    having count(distinct base.deal_business_key) <> 2
)

select disagreement, subject from the_pair
union all
select disagreement, subject from only_pair
union all
select disagreement, subject from not_merged
