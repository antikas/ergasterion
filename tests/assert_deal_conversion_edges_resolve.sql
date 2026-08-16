-- Named test: deal conversion edges resolve.
--
-- Failure mode (docs/architecture/deal-master-data.md, "Why the two conversion edges
-- resolve read-only"): br_dv_origo_deal_fund_conversion left-joins int_deal_conversion_fund
-- on the deal's declared converted_record_id, then filters the result to
-- fund_lookup.golden_fund_key is not null -- effectively an inner join. A deal whose
-- converted_record_id is populated but does not match a resolved fund's external id
-- (typo'd id, a fund still pending resolution, a fund res_fund never saw) is dropped
-- from link_deal_fund_conversion entirely. canonical_deal.converted_fund_id then reads
-- NULL for that deal via the left join back to hub_fund -- indistinguishable, without
-- this test, from a deal that never claimed a conversion at all.
--
-- Invariant: every canonical_deal row that claims a FUND conversion
-- (converted_record_type = 'fund_investment', converted_record_id populated) must
-- ALSO carry a non-null converted_fund_id. No exception list -- add one only if a
-- real deal needs it; today none does.
--
-- Scoped to fund conversions deliberately: converted_record_id/converted_record_type
-- are a generic pair on the deal record -- 'fund_investment' is the only value the
-- seed carries today, and it is the only edge canonical_deal resolves
-- (converted_fund_id via int_deal_conversion_fund / hub_fund). A future conversion
-- target (e.g. a deal converting to a portfolio company) would carry a different
-- converted_record_type and NOT resolve through this fund lookup -- it needs its own
-- arm (or its own test) when that path exists; it must not false-fail this one.
--
-- Singular test: PASSES when it returns zero rows. Renaming/mistyping the seeded
-- Orion deal's converted_record_id (OPENIM-FUND-ORION-I), or a future deal claiming a
-- fund conversion whose external id int_deal_conversion_fund cannot resolve, would
-- appear here, naming the deal id.

select
    deal_id,
    converted_record_id
from {{ ref('canonical_deal') }}
where converted_record_type = 'fund_investment'
  and converted_record_id is not null
  and converted_fund_id is null
