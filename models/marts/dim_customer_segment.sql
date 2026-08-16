{{ config(materialized='table', schema='marts') }}

-- SCD2 customer-segment (loyalty-tier) dimension for the e-commerce
-- domain's instance of the reusable half-open-range SCD2 pattern
-- (docs/architecture/scd2-classification.md). Half-open ranges:
-- effective_from <= event date < effective_to; current rows carry
-- effective_to = 9999-12-31 and is_current = true -- identical mechanics to
-- dim_investment_classification / dim_deal_stage, keyed on golden_customer_key
-- instead of fund_id / deal_id.
--
-- Same-day determinism: the lead() window below orders on
-- (effective_from, segment_recorded_at, customer_segment_history_key), not
-- (effective_from, customer_segment_history_key) alone. The ordering key resolves a
-- same-day tie deterministically.
--
-- No classification_type/classification_value reference join here (unlike
-- DEPT/SECTOR/DEAL_STAGE): segment is a plain domain-2 column, not folded into the
-- investment-domain classification registry. No reserved
-- UNKNOWN member either -- every order-linked customer in this seeded dataset has
-- segment history covering it (see fact_order / int_order_header, which LEFT JOIN
-- this dimension so an uncovered order-date would surface a null segment rather
-- than fabricate one, not silently break).

with history as (
    select * from {{ ref('sat_customer_segment_history') }}
),

typed_history as (
    select
        history.customer_segment_history_key,
        history.golden_customer_key as customer_id,
        history.segment as segment_code,
        history.effective_from,
        history.segment_recorded_at,
        lead(history.effective_from) over (
            partition by history.golden_customer_key
            order by history.effective_from, history.segment_recorded_at, history.customer_segment_history_key
        ) as next_effective_from,
        history.record_source
    from history
)

select
    {{ dbt_utils.generate_surrogate_key([
        'customer_id',
        'segment_code',
        dpf_safe_cast('effective_from', 'string')
    ]) }} as customer_segment_key,
    customer_segment_history_key,
    customer_id,
    segment_code,
    effective_from,
    segment_recorded_at,
    coalesce(next_effective_from, date '9999-12-31') as effective_to,
    next_effective_from is null as is_current,
    record_source
from typed_history
