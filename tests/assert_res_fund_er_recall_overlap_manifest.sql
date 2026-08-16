-- Ground-truth ER recall test.
--
-- correctness claim: the deterministic entity resolution in res_fund
-- achieves 100% recall on the KNOWN cross-source overlap deliberately built into
-- the synthetic VANTORA/MERIDEX/PORTIQ/CHRONO fund feeds. Recall = every source
-- record the manifest labels as the SAME true fund identity actually lands under
-- the SAME golden_fund_key -- the true identity is not split.
--
-- Why this is not redundant with the sibling tests:
--   * precision (assert_res_fund_er_precision_overlap_manifest.sql) checks a
--     golden_fund_key never spans two DIFFERENT true identities (no false merge).
--   * convergence (assert_res_fund_er_merge_convergence.sql) checks the min-label
--     propagation fully converged: no single IDENTIFIER VALUE (a shared lei,
--     external id, vantora id, or name) straddles two golden_fund_keys.
--   * neither catches a fund that splits into two golden keys where the split
--     shares NO surviving identifier at all -- e.g. two source records for the
--     same real fund carry no common lei/external id/vantora id and their names
--     don't normalise identically (a typo, an abbreviation, a unit suffix), so
--     they form two disjoint identifier-edge components. Each component passes
--     precision (it maps to one true id) and convergence (no id value straddles
--     components, because none is shared) -- the split is invisible to both.
--   * recall closes that gap directly at the manifest's TRUE-IDENTITY grain: for
--     every true_fund_external_id, the golden_fund_key(s) assigned to the source
--     records the manifest labels under it must collapse to exactly one.
--
-- Ground truth lives in seeds/entity_resolution_overlap_manifest.csv, same
-- manifest as the precision test (authored from the deliberate overlap design of
-- the seeds, not derived from the pipeline it tests).
--
-- LEFT JOIN (not the precision test's inner join): a true_fund_external_id whose
-- source records never resolved at all (golden_fund_key null / no resolved row)
-- must also fail recall -- count(distinct golden_fund_key) = 0 in that case, and
-- 0 != 1 same as 2+ != 1. count(distinct ...) already ignores the NULLs a LEFT
-- JOIN's non-match produces, so an all-unresolved identity still reports 0, not 1.
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
        manifest.true_fund_external_id,
        resolved.golden_fund_key
    from manifest
    left join resolved
        on resolved.source_system = manifest.source_system
        and resolved.source_record_id = manifest.source_record_id
),

recall_violations as (
    select
        true_fund_external_id,
        count(distinct golden_fund_key) as distinct_golden_keys,
        'true_identity_not_collapsed_to_single_golden_key' as violation
    from labelled
    group by true_fund_external_id
    having count(distinct golden_fund_key) != 1
)

select true_fund_external_id, distinct_golden_keys, violation
from recall_violations
