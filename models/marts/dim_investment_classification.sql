{{ config(materialized='table', schema='marts') }}

with history as (
    select * from {{ ref('sat_investment_classification_history') }}
),

typed_history as (
    select
        history.investment_classification_history_key,
        history.entity_type,
        history.entity_key as fund_id,
        history.classification_type_code,
        classification_type.classification_type_name,
        history.classification_value_code,
        classification_value.classification_value_name,
        classification_value.is_unknown,
        history.effective_from,
        lead(history.effective_from) over (
            partition by history.entity_key, history.classification_type_code
            order by history.effective_from, history.investment_classification_history_key
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
        cast(null as {{ dbt.type_string() }}) as investment_classification_history_key,
        'FUND' as entity_type,
        'UNKNOWN' as fund_id,
        classification_type.classification_type_code,
        classification_type.classification_type_name,
        classification_value.classification_value_code,
        classification_value.classification_value_name,
        classification_value.is_unknown,
        date '1900-01-01' as effective_from,
        cast(null as date) as next_effective_from,
        'reserved_unknown_member' as record_source,
        cast(null as {{ dbt.type_string() }}) as load_datetime
    from {{ ref('classification_type') }} as classification_type
    inner join {{ ref('classification_value') }} as classification_value
        on classification_value.classification_type_code = classification_type.classification_type_code
        and classification_value.classification_value_code = 'UNKNOWN'
    where classification_type.classification_type_code in ('DEPT', 'SECTOR')
),

scd2_rows as (
    select * from typed_history
    union all
    select * from unknown_members
)

select
    {{ dbt_utils.generate_surrogate_key([
        'fund_id',
        'classification_type_code',
        'classification_value_code',
        dpf_safe_cast('effective_from', 'string')
    ]) }} as investment_classification_key,
    investment_classification_history_key,
    entity_type,
    fund_id,
    classification_type_code,
    classification_type_name,
    classification_value_code,
    classification_value_name,
    {{ dpf_safe_cast('is_unknown', 'boolean') }} as is_unknown,
    effective_from,
    coalesce(next_effective_from, date '9999-12-31') as effective_to,
    next_effective_from is null as is_current,
    record_source,
    load_datetime
from scd2_rows
