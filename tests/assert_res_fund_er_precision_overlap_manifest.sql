-- Ground-truth ER precision test.
--
-- correctness claim: the deterministic entity resolution in res_fund
-- achieves 100% precision on the KNOWN cross-source overlap deliberately built
-- into the synthetic VANTORA/MERIDEX/PORTIQ/CHRONO fund feeds. Precision = of the
-- source records the resolver merged under a shared golden_fund_key, none are
-- merged across two different real-world funds (no false merge).
--
-- Ground truth lives in seeds/entity_resolution_overlap_manifest.csv: one row
-- per resolvable source fund record keyed by (source_system, source_record_id),
-- carrying the human-known TRUE fund identity (true_fund_external_id). This is
-- the manifest referenced by 's briefing; it is authored from the
-- deliberate overlap design of the seeds, not derived from the pipeline it tests.
--
-- Two violations are reported (either fails the test):
--   1. precision breach -- a single golden_fund_key spans two distinct true
--      identities (a false merge / over-clustering);
--   2. coverage gap -- res_fund assigned a golden_fund_key to a source record
--      that the manifest does not label (a new/unlabelled record slipped in, so
--      precision can no longer be asserted for it). This guard also stops the
--      test passing vacuously on an empty join.
--
-- Singular test: PASSES when it returns zero rows.
with resolved as (
    select
        source_system,
        source_record_id,
        golden_fund_key
    from {{ ref('res_fund') }}
    where golden_fund_key is not null
),

manifest as (
    select
        source_system,
        source_record_id,
        true_fund_external_id
    from {{ ref('entity_resolution_overlap_manifest') }}
),

labelled as (
    select
        resolved.golden_fund_key,
        manifest.true_fund_external_id
    from resolved
    inner join manifest
        on manifest.source_system = resolved.source_system
        and manifest.source_record_id = resolved.source_record_id
),

precision_violations as (
    select
        golden_fund_key,
        'golden_key_merges_multiple_true_identities' as violation
    from labelled
    group by golden_fund_key
    having count(distinct true_fund_external_id) > 1
),

coverage_gaps as (
    select
        resolved.golden_fund_key,
        'resolved_record_missing_from_manifest' as violation
    from resolved
    left join manifest
        on manifest.source_system = resolved.source_system
        and manifest.source_record_id = resolved.source_record_id
    where manifest.source_record_id is null
)

select golden_fund_key, violation from precision_violations
union all
select golden_fund_key, violation from coverage_gaps
