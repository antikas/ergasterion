{{ config(materialized='table', schema='marts') }}

-- SCD2 deal-stage dimension, the DEAL_STAGE instance of the reusable
-- classification pattern (docs/architecture/scd2-classification.md). Half-open
-- ranges: effective_from <= event date < effective_to; current rows carry
-- effective_to = 9999-12-31 and is_current = true -- same mechanics as
-- dim_investment_classification, keyed on golden_deal_key instead of fund_id.
--
-- SAME-DAY DETERMINISM: the lead() window below orders on
-- (effective_from, stage_recorded_at, deal_stage_history_key) rather than
-- (effective_from, deal_stage_history_key) alone. A same-day stage transition --
-- e.g. an approvals decision decided_at the same calendar day the deal's seed row
-- put it at DECISION, or (as proven here) two seed rows sharing an effective_from --
-- ties on effective_from; without stage_recorded_at as the second ordering term the
-- tie broke on the surrogate deal_stage_history_key, which is semantically
-- arbitrary (a same-day decision could resolve "current stage" to the WRONG side of
-- the transition). stage_recorded_at carries the intra-day order sat_deal_stage_history
-- now threads through (midnight-of-effective_from for seed rows, the decision's own
-- decided_at timestamp for decision-derived rows), so the later-recorded row always
-- deterministically supersedes the earlier one on a same-day tie, never the hash.
-- See docs/architecture/scd2-classification.md and
-- tests/assert_deal_stage_same_day_determinism.sql.

with history as (
    select * from {{ ref('sat_deal_stage_history') }}
),

typed_history as (
    select
        history.deal_stage_history_key,
        history.entity_type,
        history.entity_key as deal_id,
        history.classification_type_code,
        classification_type.classification_type_name,
        history.classification_value_code as stage_code,
        classification_value.classification_value_name as stage_name,
        classification_value.is_unknown,
        history.effective_from,
        history.stage_recorded_at,
        lead(history.effective_from) over (
            partition by history.entity_key, history.classification_type_code
            order by history.effective_from, history.stage_recorded_at, history.deal_stage_history_key
        ) as next_effective_from,
        history.record_source,
        history.load_datetime
    from history
    inner join {{ ref('classification_type') }} as classification_type
        on classification_type.classification_type_code = history.classification_type_code
    inner join {{ ref('classification_value') }} as classification_value
        on classification_value.classification_type_code = history.classification_type_code
        and classification_value.classification_value_code = history.classification_value_code
),

unknown_members as (
    select
        cast(null as {{ dbt.type_string() }}) as deal_stage_history_key,
        'DEAL' as entity_type,
        'UNKNOWN' as deal_id,
        classification_type.classification_type_code,
        classification_type.classification_type_name,
        classification_value.classification_value_code as stage_code,
        classification_value.classification_value_name as stage_name,
        classification_value.is_unknown,
        date '1900-01-01' as effective_from,
        cast(null as {{ dbt.type_string() }}) as stage_recorded_at,
        cast(null as date) as next_effective_from,
        'reserved_unknown_member' as record_source,
        cast(null as {{ dbt.type_string() }}) as load_datetime
    from {{ ref('classification_type') }} as classification_type
    inner join {{ ref('classification_value') }} as classification_value
        on classification_value.classification_type_code = classification_type.classification_type_code
        and classification_value.classification_value_code = 'UNKNOWN'
    where classification_type.classification_type_code = 'DEAL_STAGE'
),

scd2_rows as (
    select * from typed_history
    union all
    select * from unknown_members
)

select
    {{ dbt_utils.generate_surrogate_key([
        'deal_id',
        'classification_type_code',
        'stage_code',
        dpf_safe_cast('effective_from', 'string')
    ]) }} as deal_stage_key,
    deal_stage_history_key,
    entity_type,
    deal_id,
    classification_type_code,
    classification_type_name,
    stage_code,
    stage_name,
    {{ dpf_safe_cast('is_unknown', 'boolean') }} as is_unknown,
    effective_from,
    stage_recorded_at,
    coalesce(next_effective_from, date '9999-12-31') as effective_to,
    next_effective_from is null as is_current,
    {{ dpf_date_diff_days("least(coalesce(next_effective_from, date '9999-12-31'), current_date())", 'effective_from') }} as stage_duration_days,
    record_source,
    load_datetime
from scd2_rows
