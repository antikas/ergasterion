{{ config(materialized='table', schema='raw_vault') }}

-- Deal-stage history satellite using the reusable SCD2 classification
-- pattern (docs/architecture/scd2-classification.md) as a third instance of the
-- classification_type/classification_value reference pair (DEAL_STAGE alongside
-- DEPT/SECTOR). Deliberately NOT sat_investment_classification_history's fund-typed
-- twin: that model inner-joins canonical_fund on shared_external_id, a cross-source
-- identifier that can be absent (its documented orphan limitation). Deals are
-- single-source ORIGO with their own golden keys from res_deal, so this satellite
-- keys directly on canonical_deal.deal_id (the golden_deal_key) via the deal's own
-- external_deal_id -- an identifier every resolved golden deal in this dataset
-- carries by construction, so there is no equivalent orphan risk here.
--
-- SAME-DAY DETERMINISM: stage_recorded_at is an intra-day ordering
-- column, distinct from load_datetime (a technical load timestamp this project
-- never uses for ordering -- see dim_deal_stage.sql). For seed_history it is
-- midnight of effective_from (the seed only ever knew the calendar date). For
-- decision_derived_history it is the decision's own decided_at business
-- timestamp. When a decision lands on the SAME calendar day a deal's seed row
-- put it at DECISION, effective_from ties between the two rows; without a second
-- ordering term dim_deal_stage.sql's SCD2 window would break that tie on the
-- surrogate deal_stage_history_key -- semantically arbitrary. Ordering the SCD2
-- window on (effective_from, stage_recorded_at, deal_stage_history_key) instead
-- makes the later-recorded row deterministically supersede the earlier one. See
-- docs/architecture/scd2-classification.md and
-- tests/assert_deal_stage_same_day_determinism.sql.

with seed_history as (
    select
        entity_type,
        entity_external_id,
        classification_type_code,
        classification_value_code,
        {{ dpf_safe_cast('effective_from', 'date') }} as effective_from,
        record_source,
        {{ dpf_safe_cast('load_datetime', 'string') }} as load_datetime,
        {{ dpf_safe_cast('stage_recorded_at', 'string') }} as stage_recorded_at
    from {{ ref('deal_stage_history_seed') }}
),

decision_derived_history as (
    -- Additive union: an approved/declined decision deterministically
    -- produces the deal's NEXT stage-classification row here, alongside (never
    -- replacing) the seed-sourced history above -- the composition choice for this
    -- item. See models/deal_approvals/int_deal_stage_from_decision.sql for the
    -- derivation logic and its no-mutation guard (it only fires for a deal whose
    -- seed-sourced current stage is DECISION, so it can never conflict with a stage
    -- the seed already carries directly).
    --
    -- RETROACTIVE DERIVED-HISTORY DISCLOSURE: unlike seed_history above,
    -- decision_derived_history is NOT append-only -- it is recomputed in full, every
    -- build, from each deal's LATEST decision. A later non-terminal decision (the IC
    -- reconvenes and defers after a prior approve) makes the previously-derived
    -- COMMITTED/DECLINED row disappear from THIS satellite on the next build (and so
    -- from dim_deal_stage.sql downstream too). deal_decision_log itself never loses a
    -- row -- the full decision trail always survives there -- but this satellite, and
    -- the dimension built on it, show current derived truth, not a history of every
    -- derivation that was ever computed.
    select
        entity_type,
        entity_external_id,
        classification_type_code,
        classification_value_code,
        effective_from,
        record_source,
        load_datetime,
        stage_recorded_at
    from {{ ref('int_deal_stage_from_decision') }}
),

raw_history as (
    select * from seed_history
    union all
    select * from decision_derived_history
),

deal as (
    select
        deal_id,
        external_deal_id
    from {{ ref('canonical_deal') }}
    where external_deal_id is not null
),

valid_value as (
    select
        classification_type_code,
        classification_value_code
    from {{ ref('classification_value') }}
)

select
    {{ dbt_utils.generate_surrogate_key([
        'deal.deal_id',
        'raw_history.classification_type_code',
        "coalesce(valid_value.classification_value_code, 'UNKNOWN')",
        dpf_safe_cast('raw_history.effective_from', 'string')
    ]) }} as deal_stage_history_key,
    raw_history.entity_type,
    deal.deal_id as entity_key,
    raw_history.entity_external_id,
    raw_history.classification_type_code,
    coalesce(valid_value.classification_value_code, 'UNKNOWN') as classification_value_code,
    raw_history.effective_from,
    raw_history.record_source,
    raw_history.load_datetime,
    raw_history.stage_recorded_at
from raw_history
inner join deal
    on deal.external_deal_id = raw_history.entity_external_id
left join valid_value
    on valid_value.classification_type_code = raw_history.classification_type_code
    and valid_value.classification_value_code = raw_history.classification_value_code
