{{ config(materialized='table', schema='marts') }}

-- XIRR bisection, STAGE 2 of 4 (halvings 6-10 of 20; staged materialisation).
-- Continues the bracket from the physical stage-1 table. See int_fund_xirr_s05.sql for
-- why the chain is staged across four models.

with flows_src as (
    select xirr_entity_key, t_years, amount
    from {{ ref('int_fund_xirr_flows') }}
),

prior as (
    select xirr_entity_key, lo, hi, s_lo, s_hi
    from {{ ref('int_fund_xirr_s05') }}
),

{{ dpf_xirr_halvings('flows_src', 'prior', 'xirr_bounds', 5) }}

final as (
    select xirr_entity_key, lo, hi, s_lo, s_hi
    from xirr_bounds_5
)

select * from final
