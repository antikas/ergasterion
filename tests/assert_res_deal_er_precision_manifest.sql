-- Ground-truth deal entity-resolution precision test.
--
-- Correctness claim: the INTRA-SOURCE deterministic resolution in res_deal achieves
-- 100% precision on the KNOWN duplicate structure deliberately built into the ORIGO
-- CRM feed. Precision = of the deal records res_deal merged under a shared
-- golden_deal_key, none merge two different real-world deals (no false merge). Deal ER
-- here is intra-source only (dedup within the single CRM feed) and is deliberately NOT
-- separate from the cross-source fund-resolution cascade.
--
-- Ground truth lives in seeds/deal_er_overlap_manifest.csv (a SEPARATE seed from the
-- res_fund-scoped entity_resolution_overlap_manifest): one row per ORIGO deal record
-- keyed by (source_system, source_record_id), carrying the human-known TRUE deal
-- identity (true_deal_external_id), authored from the deliberate duplicate design of
-- the seed, not derived from the pipeline it tests.
--
-- Three violations are reported (any one fails the test):
--   1. precision breach -- a single golden_deal_key spans two distinct true deal
--      identities (a false merge);
--   2. coverage gap -- res_deal assigned a golden_deal_key to a record the manifest
--      does not label (an unlabelled record slipped in);
--   3. vacuity gap -- the manifest's known intra-source duplicate pairs (true
--      deal external ids appearing more than once in the manifest -- BLACKPINE and
--      CEDAR) are not fully present in the resolved set, or are present but split
--      across more than one golden_deal_key. This is the arm that stops the test
--      passing vacuously when res_deal resolves nothing: arms 1 and 2 are both
--      empty-set no-ops against an empty `resolved` CTE (an empty join proves
--      nothing), whereas this arm asserts the expected merges POSITIVELY exist.
--
-- The middle-band pair (D-009/D-010) is pending_probabilistic (null golden_deal_key),
-- so it is correctly absent from `resolved` and neither arm asserts on it -- it is
-- surfaced for human review instead (assert_deal_review_queue_intra_source.sql).
--
-- Singular test: PASSES when it returns zero rows.
with resolved as (
    select
        source_system,
        source_record_id,
        golden_deal_key
    from {{ ref('res_deal') }}
    where golden_deal_key is not null
),

manifest as (
    select
        source_system,
        source_record_id,
        true_deal_external_id
    from {{ ref('deal_er_overlap_manifest') }}
),

labelled as (
    select
        resolved.golden_deal_key,
        manifest.true_deal_external_id
    from resolved
    inner join manifest
        on manifest.source_system = resolved.source_system
        and manifest.source_record_id = resolved.source_record_id
),

precision_violations as (
    select
        golden_deal_key,
        'golden_key_merges_multiple_true_identities' as violation
    from labelled
    group by golden_deal_key
    having count(distinct true_deal_external_id) > 1
),

coverage_gaps as (
    select
        resolved.golden_deal_key,
        'resolved_record_missing_from_manifest' as violation
    from resolved
    left join manifest
        on manifest.source_system = resolved.source_system
        and manifest.source_record_id = resolved.source_record_id
    where manifest.source_record_id is null
),

manifest_duplicate_ids as (
    select
        true_deal_external_id,
        count(*) as manifest_pair_count
    from manifest
    group by true_deal_external_id
    having count(*) > 1
),

resolved_duplicate_merges as (
    select
        manifest.true_deal_external_id,
        count(*) as resolved_pair_count,
        count(distinct resolved.golden_deal_key) as distinct_golden_keys
    from manifest
    inner join resolved
        on resolved.source_system = manifest.source_system
        and resolved.source_record_id = manifest.source_record_id
    where manifest.true_deal_external_id in (
        select true_deal_external_id from manifest_duplicate_ids
    )
    group by manifest.true_deal_external_id
),

vacuity_gaps as (
    select
        manifest_duplicate_ids.true_deal_external_id as golden_deal_key,
        'expected_merge_not_resolved' as violation
    from manifest_duplicate_ids
    left join resolved_duplicate_merges
        on resolved_duplicate_merges.true_deal_external_id = manifest_duplicate_ids.true_deal_external_id
    where resolved_duplicate_merges.true_deal_external_id is null
        or resolved_duplicate_merges.resolved_pair_count <> manifest_duplicate_ids.manifest_pair_count
        or resolved_duplicate_merges.distinct_golden_keys <> 1
)

select golden_deal_key, violation from precision_violations
union all
select golden_deal_key, violation from coverage_gaps
union all
select golden_deal_key, violation from vacuity_gaps
