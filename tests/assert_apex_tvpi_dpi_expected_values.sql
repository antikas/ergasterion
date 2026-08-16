-- Expected-value TVPI/DPI test.
--
-- correctness claim: the fund-performance ratio metrics in
-- fact_fund_performance are computed to their EXPECTED VALUES for the named demo
-- fund Apex Growth II (shared_external_id OPENIM-FUND-APEX-II), not merely
-- structurally present. Expected values are derived from the seeds:
--
--   Cash flows (VANTORA/MERIDEX/PORTIQ carry IDENTICAL Apex streams; canonical
--   dedup on (fund_id, date, type, amount, currency) collapses the triplicates
-- to one each). The dates are widened across vintages so the fund's
--   XIRR is a sane multi-year rate rather than a sub-year extreme, but the AMOUNTS
--   (which is all tvpi/dpi depend on) are unchanged, so the confirmed Apex fund has:
--     capital calls  : 5,000,000 (2021-09-30) + 2,500,000 (2022-09-30) = 7,500,000
--     distributions  : 1,800,000 (2023-09-30) + 2,200,000 (2024-09-30) = 4,000,000
--   Valuations (canonical NAV sources VANTORA only), latest by valuation_date:
--     2025-09-30 -> 8,300,000  (beats the earlier, higher 2025-06-30 mark of
--                               14,900,000 -- the LATEST mark wins, not the max)
--
--   => paid_in_usd        = 7,500,000
--      distributions_usd  = 4,000,000
--      latest_nav_usd     = 8,300,000
--      tvpi = (distributions + nav) / paid_in = 12,300,000 / 7,500,000 = 1.64
--      dpi  =  distributions        / paid_in =  4,000,000 / 7,500,000 = 0.5333...
--
-- MOIC basis: invested capital = paid_in less the seeded basis offset
--   (recallable distributions 700,000 + fee offset 200,000 = 900,000, from
--   seeds/fund_invested_capital_basis.csv) = 6,600,000, so
--      invested_capital_usd = 6,600,000
--      moic = (distributions + nav) / invested = 12,300,000 / 6,600,000 = 1.8636...
--   MOIC is now DISTINCT from tvpi (1.64) and strictly greater, because invested
--   capital (6.6M) is below paid-in (7.5M). This is the pin that would break if moic
--   ever regressed to the paid-in basis (it would read 1.64, equal to tvpi again).
--
-- The cash/nav/invested components are pinned to their exact integer expected
-- values (safe: sums of integer seeds less integer offsets). The three ratios are
-- checked to their expected values within a small tolerance (a ratio stored as a
-- scaled decimal must not be equality-compared to a repeating literal); the tolerance
-- is far tighter than any real formula regression (dropping NAV, or dividing by
-- committed/paid-in rather than the correct basis, shifts the ratio by order 0.1+).
--
-- Singular test: PASSES when it returns zero rows. It returns Apex's row if any
-- component or ratio departs from its expected value.
with apex as (
    select
        shared_external_id,
        paid_in_usd,
        distributions_usd,
        latest_nav_usd,
        invested_capital_usd,
        tvpi,
        dpi,
        moic
    from {{ ref('fact_fund_performance') }}
    where shared_external_id = 'OPENIM-FUND-APEX-II'
)

select
    shared_external_id,
    paid_in_usd,
    distributions_usd,
    latest_nav_usd,
    invested_capital_usd,
    tvpi,
    dpi,
    moic
from apex
where paid_in_usd is distinct from 7500000
   or distributions_usd is distinct from 4000000
   or latest_nav_usd is distinct from 8300000
   or invested_capital_usd is distinct from 6600000
   or abs(tvpi - 1.64) > 0.0001
   or abs(dpi - (cast(4000000 as numeric) / cast(7500000 as numeric))) > 0.0001
   or abs(moic - (cast(12300000 as numeric) / cast(6600000 as numeric))) > 0.0001
