-- What this proves: what the declared keys could not settle is visible rather
-- than guessed.
--
-- Nine fund records carry neither a legal entity identifier nor the identifier
-- the source systems share, so no declared key merges them and each stays an
-- entity of its own. The probabilistic tier scores every pair of them that
-- shares an asset class, which is what the resolution is blocked on, and puts
-- each pair in a band.
--
-- Three things are known about the result on the seeded population:
--
--   Six pairs reach the accepted band. Four of them are the same infrastructure
--   fund delivered by three systems that identify it by nothing, so the estate
--   can see the merge its keys could not make.
--
--   No pair is unscored. Every pair compares a name, an asset class and a
--   report date, so a null score would mean an attribute went missing.
--
--   Every record in the evidence is a record the resolution left unmerged. A
--   scored pair over a record that did merge would mean the evidence and the
--   resolution disagree.
--
-- Singular test: it passes when it returns no rows.
with pairs as (
    select record_key_a, record_key_b, match_score, review_band
    from {{ ref('investment__fund_vault__pending_keys') }}
),

unresolved as (
    select resolution_record_key
    from {{ ref('investment__fund_vault__resolution') }}
    where resolution_pending
),

accepted_count as (
    select
        'the accepted band does not hold the six pairs the evidence should carry' as disagreement,
        cast(count(*) as varchar) as subject
    from pairs
    where review_band = 'accepted'
    having count(*) <> 6
),

named_pair as (
    select
        'the infrastructure fund three systems identify by nothing is not accepted' as disagreement,
        'chrono:CHR-FUND-003 with portiq:PORTIQ-55' as subject
    where not exists (
        select 1 from pairs
        where record_key_a = 'chrono:CHR-FUND-003'
          and record_key_b = 'portiq:PORTIQ-55'
          and review_band = 'accepted'
    )
),

unscored as (
    select
        'a candidate pair reached the evidence unscored' as disagreement,
        record_key_a as subject
    from pairs
    where match_score is null or review_band = 'unscored'
),

merged_record as (
    select
        'the evidence scores a record the resolution merged' as disagreement,
        pairs.record_key_a as subject
    from pairs
    left join unresolved on unresolved.resolution_record_key = pairs.record_key_a
    where unresolved.resolution_record_key is null
)

select disagreement, subject from accepted_count
union all
select disagreement, subject from named_pair
union all
select disagreement, subject from unscored
union all
select disagreement, subject from merged_record
