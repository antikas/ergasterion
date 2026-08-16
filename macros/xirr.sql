{#-
  Deterministic, dialect-neutral XIRR (irregular-interval internal rate of return)
  solver. ; bracket-validity guard + helper-model materialisation
 ; bisection depth reduced to a minutes-fast native build.

  Snowflake has NO native XIRR/IRR function, and BigQuery has none either, so the
  solve is built here in pure SQL rather than hand-rolled per-adapter. Every construct
  emitted (power(), arithmetic, case, sum, group by) is ANSI and renders identically on
  BigQuery and Snowflake -- there are NO per-adapter branches, so the dialect-lint gate
  (ergasterion/dialect_lint.py) has nothing to flag. This macro lives under macros/ (the one
  sanctioned home for solver logic) exactly as the adapter-dispatch primitives
  do; callers stay dialect-free.

  Method: bracketed BISECTION, not Newton-Raphson. Bisection is chosen deliberately
  because it is DETERMINISTIC and unconditionally convergent for a conventional
  private-markets cash-flow stream (one sign change: capital calls out early, then
  distributions + terminal NAV in), whereas Newton can diverge or oscillate depending on
  the seed. NPV(r) = sum( amount_i * (1 + r)^(-t_i) ) crosses zero once on a properly
  bracketed stream, so halving the interval a fixed number of times converges to the root
  with no data-dependent iteration count -- the same inputs always yield the same rate.

  BRACKET-VALIDITY GUARD. Bisection is only meaningful when NPV changes sign
  across the bracket. dpf_xirr_seed computes NPV at BOTH bounds ONCE and records their
  signs (s_lo, s_hi). A stream whose discounted NPV is net-positive (or net-negative)
  EVERYWHERE on the bracket -- e.g. a fund with a large leading distribution and a
  sub-year super-multiple (Northstar Buyout IV) -- has s_lo = s_hi: no root brackets it,
  so the bisection would otherwise walk the interval hard against a bound and report the
  bracket ceiling (up to +10,000%) as if it were a solved rate. The caller
  (int_fund_xirr.sql) turns s_lo = s_hi into a NULL, and ALSO NULLs any solve that
  converges within epsilon of either bound (a degenerate pin, not a real internal rate).
  This SUPERSEDES the old sign-MIX-only gate, which passed unbracketed streams straight
  through to the ceiling.

  COMPILE STRUCTURE (+). The iteration is UNROLLED by a Jinja loop into
  a chain of CTEs (one per halving) rather than a recursive CTE, because recursive-CTE
  syntax and row-generation differ across adapters. Each halving re-aggregates NPV at the
  interval midpoint over the flows. CRITICALLY, the flows must be passed as an already-
  MATERIALISED relation (a physical table -- see int_fund_xirr_flows.sql), NOT an inline
  CTE that reaches back into the multi-join staging/canonical subtree: every step
  references the flows once, so an inline flow CTE is re-expanded by the compiler at every
  step, and that deep re-expansion is what drove fact_fund_performance_base past
  Snowflake's 1-hour SQL-compilation ceiling (error 000649/57014). Scanning a physical
  flows table instead keeps each step a bounded aggregation. cut the depth from
  34 to 20 halvings. then STAGED the 20 halvings across FOUR physical models of
  five each (int_fund_xirr_s05 / _s10 / _s15 / int_fund_xirr), because even a 20-deep
  inlined chain EXECUTED ~29 minutes single-model: the optimizer inlines the whole chain
  into one nested plan, and plan cost grows explosively with inline depth. Each stage
  reads the prior stage's physical table, so every plan is five levels deep and runs in
  seconds. A genuine recursive CTE was evaluated and rejected with vendor evidence: both
  Snowflake and BigQuery forbid aggregates inside the recursive clause, and the
  per-halving NPV is an aggregate.

  Contract (SSOT for the caller): pass the NAME of a CTE/relation exposing exactly:
      xirr_entity_key  -- the grain the IRR is solved per (e.g. fund_id)
      t_years          -- numeric years from the entity's earliest flow to this flow
      amount           -- signed cash flow (calls negative, distributions/NAV positive)
  Compose in a model's WITH list: dpf_xirr_seed('flows_rel') then
  dpf_xirr_halvings('flows_rel', 'xirr_seed', 'xirr_bounds', 20); read (lo + hi) / 2 from
  xirr_bounds_20 and apply the guard (s_lo = s_hi -> NULL; within-epsilon-of-bound ->
  NULL). Both macros emit comma-TERMINATED CTEs.

  Bracket: [low, high] = [-0.9999, 100.0] (i.e. -99.99% to +10,000% IRR). low stays above
  -1 so (1 + r) is strictly positive and power() never takes a negative base. 20 halvings
  give a rate resolution of ~ (100.9999) / 2^20 ~= 9.6e-5 -- basis-point precision,
  tighter than the 4-decimal convention any reported IRR is quoted to, and the deliberate
  trade: enough halvings to be exact to a basis point, few enough to compile in
  seconds and run in single-digit minutes.
-#}

{#- NPV at `rate_expr`, aggregated over the flows aliased `f`. rate_expr is any SQL scalar. -#}
{% macro dpf_xirr_npv(rate_expr) -%}
sum(f.amount * power(cast(1 as {{ dpf_type('numeric') }}) + ({{ rate_expr }}), -f.t_years))
{%- endmacro %}

{#- Seed CTE `xirr_seed`: the bracket bounds and the NPV sign at EACH bound, computed once. -#}
{% macro dpf_xirr_seed(flows, low=-0.9999, high=100.0) -%}
xirr_seed as (
    select
        f.xirr_entity_key,
        cast({{ low }} as {{ dpf_type('numeric') }}) as lo,
        cast({{ high }} as {{ dpf_type('numeric') }}) as hi,
        case when {{ dpf_xirr_npv('cast(' ~ low ~ ' as ' ~ dpf_type('numeric') ~ ')') }} >= 0
             then 1 else -1 end as s_lo,
        case when {{ dpf_xirr_npv('cast(' ~ high ~ ' as ' ~ dpf_type('numeric') ~ ')') }} >= 0
             then 1 else -1 end as s_hi
    from {{ flows }} as f
    group by f.xirr_entity_key
),
{%- endmacro %}

{#- `iterations` halving CTEs named {{ out_prefix }}_1 .. {{ out_prefix }}_{{ iterations }},
    each starting from `in_cte` (which must expose xirr_entity_key, lo, hi, s_lo, s_hi).
    The kept half is the one whose midpoint NPV shares the sign of the lower bound (s_lo),
    so lo always carries sign s_lo and hi the opposite -- robust even where NPV is not
    globally monotone in r. -#}
{% macro dpf_xirr_halvings(flows, in_cte, out_prefix, iterations) -%}
{% for k in range(1, iterations + 1) -%}
{%- set prev = in_cte if k == 1 else out_prefix ~ '_' ~ (k - 1) -%}
{%- set mid = '((b.lo + b.hi) / 2)' -%}
{{ out_prefix }}_{{ k }} as (
    select
        b.xirr_entity_key,
        case when (case when {{ dpf_xirr_npv(mid) }} >= 0 then 1 else -1 end) = b.s_lo
             then {{ mid }} else b.lo end as lo,
        case when (case when {{ dpf_xirr_npv(mid) }} >= 0 then 1 else -1 end) = b.s_lo
             then b.hi else {{ mid }} end as hi,
        b.s_lo,
        b.s_hi
    from {{ prev }} as b
    inner join {{ flows }} as f
        on f.xirr_entity_key = b.xirr_entity_key
    group by b.xirr_entity_key, b.lo, b.hi, b.s_lo, b.s_hi
),
{% endfor -%}
{%- endmacro %}
