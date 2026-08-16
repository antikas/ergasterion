-- Named test: pipeline covers all golden deals.
--
-- fact_deal_pipeline has two quiet-fallback failure modes:
--   (a) a resolved deal with NO stage-history rows silently reads
--       current_stage_code = 'UNKNOWN' via the unconditional `cross join unknown_stage`
--       fallback -- no error, no distinguishing signal from a deal that genuinely has
--       an UNKNOWN-only history.
--   (b) fact_deal_pipeline inner-joins dim_date on deal.sourced_date -- a resolved
--       deal whose sourced_date falls outside the dim_date spine is dropped from the
--       fact table entirely, with no error.
--
-- Two arms; either produces failing rows (the test PASSES on zero rows):
--
--   1. deal_missing_from_pipeline / deal_duplicated_in_pipeline -- every golden deal
--      in canonical_deal must appear in fact_deal_pipeline EXACTLY ONCE. Absence
--      catches (b) (the dim_date inner join silently dropping an out-of-spine deal);
--      duplication catches an unexpected fan-out of the dim_date join.
--
--   2. deal_missing_stage_history -- the real invariant behind (a). fact_deal_pipeline
--      itself cannot distinguish "no stage history" from "history that happens to be
--      UNKNOWN", so this arm checks the actual invariant one layer upstream, directly
--      against sat_deal_stage_history: every golden deal must carry at least one
--      stage-history row. The seeded data guarantees this today (every deal gets a
--      SOURCED row on entry); a deal onboarded without ever getting a first stage row
--      would silently read UNKNOWN today and is named here instead.

with golden_deals as (
    select deal_id
    from {{ ref('canonical_deal') }}
),

pipeline_counts as (
    select
        deal_id,
        count(*) as row_count
    from {{ ref('fact_deal_pipeline') }}
    group by deal_id
),

deal_missing_from_pipeline as (
    select
        'deal_missing_from_pipeline' as failure_type,
        golden_deals.deal_id as failure_key
    from golden_deals
    left join pipeline_counts
        on pipeline_counts.deal_id = golden_deals.deal_id
    where pipeline_counts.deal_id is null
),

deal_duplicated_in_pipeline as (
    select
        'deal_duplicated_in_pipeline' as failure_type,
        pipeline_counts.deal_id as failure_key
    from pipeline_counts
    where pipeline_counts.row_count > 1
),

stage_history_deal_ids as (
    select distinct entity_key as deal_id
    from {{ ref('sat_deal_stage_history') }}
),

deal_missing_stage_history as (
    select
        'deal_missing_stage_history' as failure_type,
        golden_deals.deal_id as failure_key
    from golden_deals
    left join stage_history_deal_ids
        on stage_history_deal_ids.deal_id = golden_deals.deal_id
    where stage_history_deal_ids.deal_id is null
)

select failure_type, failure_key from deal_missing_from_pipeline
union all
select failure_type, failure_key from deal_duplicated_in_pipeline
union all
select failure_type, failure_key from deal_missing_stage_history
