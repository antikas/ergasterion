{{ config(materialized='table', schema='business_vault') }}

with history as (
    select * from {{ ref('sat_investment_classification_history') }}
),

ranked as (
    select
        history.*,
        row_number() over (
            partition by entity_key, classification_type_code
            order by effective_from desc, investment_classification_history_key desc
        ) as current_rank
    from history
    where effective_from <= current_date()
)

select
    entity_type,
    entity_key,
    classification_type_code,
    investment_classification_history_key as current_classification_history_key,
    classification_value_code as current_classification_value_code,
    effective_from as current_effective_from
from ranked
where current_rank = 1
