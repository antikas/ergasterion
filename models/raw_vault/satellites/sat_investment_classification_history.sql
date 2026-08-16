{{ config(materialized='table', schema='raw_vault') }}

-- Rekeyed: attach classification history via the ER golden key
-- (res_fund resolution), never shared_external_id. The prior join --
-- `canonical_fund.shared_external_id = raw_history.entity_external_id`, canonical_fund
-- filtered to `shared_external_id is not null` -- could never match a fund that only
-- resolves by NAME through the ER cascade (res_fund / int_vantora_fund_id_normalised):
-- shared_external_id is null for a name-only resolution by construction, so the inner
-- join could never fire regardless of what history was seeded. Harbor Infrastructure
-- III and SummitBridge are exactly that case (see
-- seeds/entity_resolution_overlap_manifest.csv: both are "no lei/external id" /
-- "name-only" entries), so shared_external_id alone cannot resolve their classifications.
--
-- Join through entity_resolution_overlap_manifest.csv, the ER ground-truth
-- crosswalk already maintained for res_fund's own recall/precision tests (
-- reused here, not forked). It carries the human-assigned true_fund_external_id
-- (the same label this seed's entity_external_id uses) against every
-- (source_system, source_record_id) pair that belongs to it, for every fund in this
-- dataset -- LEI-keyed, external-id-keyed, and name-only alike. Joining that
-- crosswalk to res_fund resolves the golden_fund_key regardless of whether
-- shared_external_id ever survived resolution.
--
-- min(golden_fund_key) per true_fund_external_id is a defensive collapse, not a new
-- SCD2 tie-break: tests/assert_res_fund_er_recall_overlap_manifest.sql already
-- asserts every true identity resolves to exactly one golden_fund_key, so the
-- aggregate is a no-op under a passing pipeline and only guards a silent 1:many
-- fan-out if that invariant is ever violated.
--
-- This satellite is hand-authored because its classification-history shape spans
-- authored source history and the ER crosswalk. Its dimension, fact, and PIT consumers
-- all read this table as the single source for dated investment classification.

with raw_history as (
    select
        entity_type,
        entity_external_id,
        classification_type_code,
        classification_value_code,
        {{ dpf_safe_cast('effective_from', 'date') }} as effective_from,
        record_source,
        {{ dpf_safe_cast('load_datetime', 'string') }} as load_datetime
    from {{ ref('investment_classification_history_seed') }}
),

fund_crosswalk as (
    select
        manifest.true_fund_external_id as entity_external_id,
        min(resolved.golden_fund_key) as fund_id
    from {{ ref('entity_resolution_overlap_manifest') }} as manifest
    inner join {{ ref('res_fund') }} as resolved
        on resolved.source_system = manifest.source_system
        and resolved.source_record_id = manifest.source_record_id
    where resolved.golden_fund_key is not null
    group by manifest.true_fund_external_id
),

valid_value as (
    select
        classification_type_code,
        classification_value_code
    from {{ ref('classification_value') }}
)

select
    {{ dbt_utils.generate_surrogate_key([
        'fund_crosswalk.fund_id',
        'raw_history.classification_type_code',
        'coalesce(valid_value.classification_value_code, ' ~ "'UNKNOWN'" ~ ')',
        dpf_safe_cast('raw_history.effective_from', 'string')
    ]) }} as investment_classification_history_key,
    raw_history.entity_type,
    fund_crosswalk.fund_id as entity_key,
    raw_history.entity_external_id,
    raw_history.classification_type_code,
    coalesce(valid_value.classification_value_code, 'UNKNOWN') as classification_value_code,
    raw_history.effective_from,
    raw_history.record_source,
    raw_history.load_datetime
from raw_history
inner join fund_crosswalk
    on fund_crosswalk.entity_external_id = raw_history.entity_external_id
left join valid_value
    on valid_value.classification_type_code = raw_history.classification_type_code
    and valid_value.classification_value_code = raw_history.classification_value_code
