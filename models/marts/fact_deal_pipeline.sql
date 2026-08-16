{{ config(materialized='table', schema='marts') }}

-- Deal-pipeline funnel snapshot. One row per resolved deal: its stage as
-- of today via a half-open date-range join against dim_deal_stage (the same join
-- shape fact_investment_classification uses against its SCD2 dimension, here against
-- current_date() rather than an observed valuation date), the funnel's conversion
-- exit edge, and cycle-time measures. UNKNOWN reserved-member coalesce mirrors
-- fact_investment_classification's non-null-FK discipline, though in this seeded
-- dataset every golden deal carries stage history from its SOURCED row onward.

with deal as (
    select * from {{ ref('dim_deal') }}
),

stage_history as (
    select * from {{ ref('dim_deal_stage') }}
    where deal_id != 'UNKNOWN'
),

date_dim as (
    select * from {{ ref('dim_date') }}
),

current_stage as (
    -- Stage-at-date via a half-open date-range join against the report date
    -- (today): current_date() >= effective_from and < effective_to picks exactly
    -- one stage row per deal, using the same predicate shape as the classification fact.
    select *
    from stage_history
    where current_date() >= effective_from
      and current_date() < effective_to
),

terminal_stage as (
    -- First (and, by construction of this append-only funnel, only) entry into a
    -- funnel-exit stage. Drives total sourced-to-decision cycle time.
    select
        deal_id,
        stage_code as terminal_stage_code,
        effective_from as terminal_effective_from
    from stage_history
    where stage_code in ('COMMITTED', 'DECLINED')
),

unknown_stage as (
    select deal_stage_key
    from {{ ref('dim_deal_stage') }}
    where deal_id = 'UNKNOWN'
      and stage_code = 'UNKNOWN'
)

select
    {{ dbt_utils.generate_surrogate_key(['deal.deal_id']) }} as deal_pipeline_key,
    deal.deal_key,
    date_dim.date_key,
    deal.deal_id,
    deal.deal_name,
    deal.strategy,
    deal.sourced_date,
    coalesce(current_stage.deal_stage_key, unknown_stage.deal_stage_key) as current_deal_stage_key,
    coalesce(current_stage.stage_code, 'UNKNOWN') as current_stage_code,
    coalesce(current_stage.stage_name, 'Unknown') as current_stage_name,
    current_stage.effective_from as current_stage_effective_from,
    coalesce(current_stage.is_current, false) as is_current,
    current_stage.stage_duration_days as days_in_current_stage,
    {{ dpf_date_diff_days('current_date()', 'deal.sourced_date') }} as days_since_sourced,
    coalesce(current_stage.stage_code, 'UNKNOWN') = 'COMMITTED' as is_committed,
    coalesce(current_stage.stage_code, 'UNKNOWN') = 'DECLINED' as is_declined,
    deal.converted_fund_id is not null as is_converted,
    terminal_stage.terminal_stage_code,
    terminal_stage.terminal_effective_from as decision_date,
    {{ dpf_date_diff_days('terminal_stage.terminal_effective_from', 'deal.sourced_date') }} as total_cycle_time_days,
    deal.converted_record_type,
    deal.converted_record_id,
    deal.converted_fund_id
from deal
left join current_stage
    on current_stage.deal_id = deal.deal_id
left join terminal_stage
    on terminal_stage.deal_id = deal.deal_id
cross join unknown_stage
inner join date_dim
    on date_dim.date_day = deal.sourced_date
