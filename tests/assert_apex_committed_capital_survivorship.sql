-- Expected-value survivorship determinism test.
--
-- Apex Growth II's committed_capital_usd must survive from CHRONO (business
-- effective date 2025-12-31), beating VANTORA (latest snapshot 2025-09-30) --
-- i.e. the most_recent winner is chosen by the source's business effective date
-- (effective_from), not by dbt build wall-clock (load_datetime). This value is
-- stable across repeated fresh builds because effective_from is a data fact and
-- source_priority is a deterministic tie-break; there is no build-order race.
--
-- Value re-baselined by the believable re-seed: the cross-source
-- disagreement is now subtle, not theatrical. VANTORA reports 124,400,000 as of its
-- Q3 snapshot (2025-09-30); CHRONO reports 125,000,000 as of year-end (2025-12-31),
-- a 0.6M timing difference from a subsequent close. The most-recent rule picks
-- CHRONO's year-end figure, so the surviving value is 125000000 from CHRONO (the
-- winner source is unchanged; only the value moved off the old theatrical 45M).
--
-- Singular test: PASSES when it returns zero rows. It returns a row if the
-- surviving committed_capital_usd is not 125000000 or its source is not CHRONO.
select
    shared_external_id,
    committed_capital_usd,
    committed_capital_usd__source
from {{ ref('bv_fund_golden_record') }}
where shared_external_id = 'OPENIM-FUND-APEX-II'
  and (
        committed_capital_usd is distinct from 125000000
     or committed_capital_usd__source is distinct from 'CHRONO'
  )
