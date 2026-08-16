{{ config(materialized='table', schema='marts') }}

-- XIRR bisection, STAGE 1 of 4 (halvings 1-5 of 20; staged materialisation).
-- WHY STAGED: a 20-deep chain of inlined halving CTEs compiles (fixed that) but
-- still EXECUTED ~29 minutes single-model on the XS warehouse, because the optimizer
-- inlines the whole chain into one nested plan (CTEs are not materialised on Snowflake).
-- Four physical stage tables of five halvings each keep every plan five levels deep:
-- each stage is a bounded aggregation over two small physical tables (the prior stage's
-- bounds and the flows), which compiles in seconds and executes in seconds. Recursive
-- CTEs were evaluated and rejected with vendor evidence: both warehouses forbid
-- aggregates inside the recursive clause, and the per-halving NPV is an aggregate.
--
-- Grain: one row per fund with flows -- xirr_entity_key, lo, hi, s_lo, s_hi. The seed
-- computes NPV sign at both bracket bounds once (the bracket-validity guard's
-- raw material); the final model (int_fund_xirr) applies the guard.

{% set low = -0.9999 %}
{% set high = 100.0 %}

with flows_src as (
    select xirr_entity_key, t_years, amount
    from {{ ref('int_fund_xirr_flows') }}
),

{{ dpf_xirr_seed('flows_src', low=low, high=high) }}
{{ dpf_xirr_halvings('flows_src', 'xirr_seed', 'xirr_bounds', 5) }}

final as (
    select xirr_entity_key, lo, hi, s_lo, s_hi
    from xirr_bounds_5
)

select * from final
