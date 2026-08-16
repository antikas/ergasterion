-- Expected-value cross-source alias-union test.
--
-- canonical_fund.known_aliases must be the DISTINCT UNION of source-provided
-- fund names across every contributing source for Apex Growth II, not the
-- single survivorship-winning source's name:
--   VANTORA: "Apex Growth Fund II"
--   MERIDEX:  "Apex Growth II, LP"
--   PORTIQ:   "Apex Growth Fund II"  (duplicate of VANTORA -- collapses under DISTINCT)
--   CHRONO: "Apex Growth II Feeder"
-- Union has 3 distinct names.
--
-- Singular test: PASSES when it returns zero rows. It returns a row if fewer
-- than 3 distinct aliases survived the union (i.e. the fix regressed to a
-- single-survivor projection).
select
    fund_id,
    lei,
    {{ dpf_array_length('known_aliases') }} as alias_count
from {{ ref('canonical_fund') }}
where lei = '549300APEXGROWTH20001'
  and {{ dpf_array_length('known_aliases') }} < 3
