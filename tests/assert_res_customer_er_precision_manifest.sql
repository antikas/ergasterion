-- Ground-truth customer ER precision test. The NAMED test for the
-- e-commerce customer-360 domain -- the second-domain instance of the same precision
-- discipline the fund and deal tests apply (assert_res_fund_er_precision_overlap_manifest,
-- assert_res_deal_er_precision_manifest).
--
-- Correctness claim: the deterministic tier-1 resolution in res_customer achieves 100%
-- precision on the KNOWN cross-feed overlap deliberately built into the synthetic
-- CARTIVO / MERCARO / RELATIO customer feeds. A customer merges across feeds on a shared
-- loyalty id (Ava, tri-source) or a normalised email (Ben, case-variant across two
-- feeds, no loyalty). Precision = of the source records res_customer merged under a
-- shared golden_customer_key, none merge two different real-world customers (no false
-- merge).
--
-- Ground truth lives in seeds/customer_er_overlap_manifest.csv: one row per resolvable
-- source customer record keyed by (source_system, source_record_id), carrying the
-- human-known TRUE customer identity (true_customer_external_id). Authored from the
-- deliberate overlap design of the seeds, not derived from the pipeline it tests.
--
-- Three violations are reported (any one fails the test):
--   1. precision breach -- a single golden_customer_key spans two distinct true
--      identities (a false merge / over-clustering);
--   2. coverage gap -- res_customer assigned a golden_customer_key to a record the
--      manifest does not label (an unlabelled record slipped in, so precision can no
--      longer be asserted for it; also stops the test passing vacuously on an empty join);
--   3. vacuity gap -- the manifest's known cross-feed duplicates (true customer ids
--      appearing more than once -- CUST-AVA x3, CUST-BEN x2) are not fully present in the
--      resolved set, or are present but split across more than one golden_customer_key.
--      This arm asserts the expected merges POSITIVELY exist (acceptance #4: a seeded
--      cross-feed duplicate resolves to EXACTLY ONE golden key), so the test cannot pass
--      by resolving nothing.
--
-- Singular test: PASSES when it returns zero rows.
with resolved as (
    select
        source_system,
        source_record_id,
        golden_customer_key
    from {{ ref('res_customer') }}
    where golden_customer_key is not null
),

manifest as (
    select
        source_system,
        source_record_id,
        true_customer_external_id
    from {{ ref('customer_er_overlap_manifest') }}
),

labelled as (
    select
        resolved.golden_customer_key,
        manifest.true_customer_external_id
    from resolved
    inner join manifest
        on manifest.source_system = resolved.source_system
        and manifest.source_record_id = resolved.source_record_id
),

precision_violations as (
    select
        golden_customer_key,
        'golden_key_merges_multiple_true_identities' as violation
    from labelled
    group by golden_customer_key
    having count(distinct true_customer_external_id) > 1
),

coverage_gaps as (
    select
        resolved.golden_customer_key,
        'resolved_record_missing_from_manifest' as violation
    from resolved
    left join manifest
        on manifest.source_system = resolved.source_system
        and manifest.source_record_id = resolved.source_record_id
    where manifest.source_record_id is null
),

manifest_duplicate_ids as (
    select
        true_customer_external_id,
        count(*) as manifest_pair_count
    from manifest
    group by true_customer_external_id
    having count(*) > 1
),

resolved_duplicate_merges as (
    select
        manifest.true_customer_external_id,
        count(*) as resolved_pair_count,
        count(distinct resolved.golden_customer_key) as distinct_golden_keys
    from manifest
    inner join resolved
        on resolved.source_system = manifest.source_system
        and resolved.source_record_id = manifest.source_record_id
    where manifest.true_customer_external_id in (
        select true_customer_external_id from manifest_duplicate_ids
    )
    group by manifest.true_customer_external_id
),

vacuity_gaps as (
    select
        manifest_duplicate_ids.true_customer_external_id as golden_customer_key,
        'expected_merge_not_resolved' as violation
    from manifest_duplicate_ids
    left join resolved_duplicate_merges
        on resolved_duplicate_merges.true_customer_external_id = manifest_duplicate_ids.true_customer_external_id
    where resolved_duplicate_merges.true_customer_external_id is null
        or resolved_duplicate_merges.resolved_pair_count <> manifest_duplicate_ids.manifest_pair_count
        or resolved_duplicate_merges.distinct_golden_keys <> 1
)

select golden_customer_key, violation from precision_violations
union all
select golden_customer_key, violation from coverage_gaps
union all
select golden_customer_key, violation from vacuity_gaps
