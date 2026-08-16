{{ config(materialized='table', schema='marts') }}

-- Deterministic XIRR solve per confirmed fund -- STAGE 4 of 4 (halvings 16-20 of 20)
-- plus the bracket-validity guard. The bisection is STAGED across four physical models
-- of five halvings each: a 20-deep inlined chain compiled but still executed
-- ~29 minutes single-model, because the optimizer inlines the whole chain into one
-- nested plan. Five-deep plans over two small physical tables execute in seconds. See
-- int_fund_xirr_s05.sql for the staging rationale and macros/xirr.sql for the method.
--
-- Output grain: one row per confirmed fund that has flows. net_irr is the solved net
-- internal rate of return, or NULL where the stream is UNBRACKETED. The bracket-validity
-- guard is the correctness fix: a stream whose NPV shares the same sign at
-- both bracket bounds (s_lo = s_hi) has no root in [-99.99%, +10,000%] -- e.g. Northstar
-- Buyout IV, a leading distribution plus a sub-year super-multiple whose NPV is positive
-- everywhere. That result is NULL, as is any solve that pins within epsilon of either bound
-- (a degenerate result, never a real rate).
--
-- Precision: 20 total halvings resolve the bracket [-0.9999, 100.0] to
-- ~ 100.9999 / 2^20 ~= 9.6e-5 -- basis-point precision, tighter than the 4dp any reported
-- IRR is quoted to. epsilon (the degenerate-pin guard below) is 0.005, well outside that
-- resolution, so a genuinely bracketed root is never mistaken for a bound-pin.

{% set epsilon = 0.005 %}
{% set low = -0.9999 %}
{% set high = 100.0 %}

with flows_src as (
    select xirr_entity_key, t_years, amount
    from {{ ref('int_fund_xirr_flows') }}
),

prior as (
    select xirr_entity_key, lo, hi, s_lo, s_hi
    from {{ ref('int_fund_xirr_s15') }}
),

{{ dpf_xirr_halvings('flows_src', 'prior', 'xirr_bounds', 5) }}

xirr_result as (
    select
        xirr_entity_key,
        cast((lo + hi) / 2 as {{ dpf_type('numeric') }}) as raw_irr,
        s_lo,
        s_hi
    from xirr_bounds_5
)

select
    xirr_entity_key,
    case
        -- No sign change across the bracket: the stream is not bracketed, so the
        -- bisection midpoint is meaningless -- return NULL, never a ceiling-pinned rate.
        when s_lo = s_hi then cast(null as {{ dpf_type('numeric') }})
        -- Degenerate pin against a bound: treat as a failed solve, not a real rate.
        when abs(raw_irr - cast({{ low }} as {{ dpf_type('numeric') }})) < {{ epsilon }} then cast(null as {{ dpf_type('numeric') }})
        when abs(raw_irr - cast({{ high }} as {{ dpf_type('numeric') }})) < {{ epsilon }} then cast(null as {{ dpf_type('numeric') }})
        else raw_irr
    end as net_irr
from xirr_result
