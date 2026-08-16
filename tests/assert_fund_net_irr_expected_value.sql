-- Expected-value net-IRR test (tolerance re-stated for the 20-halving solve).
--
-- Replaces the tautological accepted_range(-1..100) tests on net_irr/gross_irr, which
-- were co-extensive with the XIRR solver's own bracket [-99.99%, +10,000%] and so could
-- never fail. This asserts two real, independently-derivable facts:
--
-- 1. POSITIVE CASE -- Apex Growth II (OPENIM-FUND-APEX-II) has a hand-computable XIRR.
--    Its confirmed net cash-flow stream, widened across vintages so sub-year
--    annualisation no longer manufactures an extreme rate, is (first flow 2021-09-30):
--        2021-09-30  -5,000,000   capital call        (t = 0.000)
--        2022-09-30  -2,500,000   capital call        (t = 1.000)
--        2023-09-30  +1,800,000   distribution        (t = 2.000)
--        2024-09-30  +2,200,000   distribution        (t = 3.003)
--        2025-09-30  +8,300,000   terminal NAV        (t = 4.003)
--    The rate r solving NPV(r) = sum(amount * (1+r)^-t) = 0 is r = 0.168881 (16.8881%),
--    verified by an independent bisection off the same actual/365 day-count. Pinned to a
-- 0.001 (1e-3) absolute tolerance. cut the solver to 20 halvings, whose
--    resolution over the [-0.9999, 100.0] bracket is ~ 100.9999 / 2^20 ~= 9.6e-5 -- an
--    order of magnitude INSIDE this 1e-3 pin, so the reduced depth cannot drift the
--    assertion. The 1e-3 pin is itself far tighter than any real formula regression
--    (dropping terminal NAV, or discounting by committed rather than paid-in capital,
-- shifts the rate by order 0.1+). gross_irr must equal net_irr (the alias).
--
-- 2. BRACKET-GUARD CASE -- Northstar Buyout IV (OPENIM-FUND-NORTHSTAR-IV) is NOT
--    bracketed: a leading distribution (3,600,000 at t=0) plus a ~24x sub-year multiple
--    leave NPV positive at BOTH bracket bounds, so no root exists in [-99.99%, +10,000%].
-- Before the bisection walked to the +10,000% ceiling and reported it as a
--    solved rate on a green build; the bracket-validity guard now returns NULL. This
--    check fails if net_irr is non-null for Northstar (i.e. the ceiling clamp regressed).
--
-- Singular test: PASSES when it returns zero rows.

with perf as (
    select
        shared_external_id,
        net_irr,
        gross_irr
    from {{ ref('fact_fund_performance') }}
),

apex_violation as (
    select
        'apex_net_irr_expected_value' as check_name,
        shared_external_id,
        net_irr,
        gross_irr
    from perf
    where shared_external_id = 'OPENIM-FUND-APEX-II'
      and (
            net_irr is null
         or abs(net_irr - 0.168881) > 0.001
         or gross_irr is distinct from net_irr
      )
),

northstar_violation as (
    select
        'northstar_bracket_guard_null' as check_name,
        shared_external_id,
        net_irr,
        gross_irr
    from perf
    where shared_external_id = 'OPENIM-FUND-NORTHSTAR-IV'
      and net_irr is not null
)

select check_name, shared_external_id, net_irr, gross_irr from apex_violation
union all
select check_name, shared_external_id, net_irr, gross_irr from northstar_violation
