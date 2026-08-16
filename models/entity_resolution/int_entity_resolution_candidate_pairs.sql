-- Pairwise probabilistic ER candidate scoring. Self-joins the tier-0/1
-- leftovers (res_pending_probabilistic) within an entity_type across DIFFERENT
-- source systems -- cross-source matching is the ER goal; a source's own staging model
-- already gives it one row per distinct source_id, so no within-source pairing is
-- needed -- and scores every candidate pair via the four sub-scores in
-- macros/entity_resolution_scoring.sql, weighted by dpf_composite_er_score using
-- seeds/entity_resolution_scoring_config.csv (joined in per entity_type, not
-- hardcoded). QUALIFY keeps only the single best-scoring counterpart per left-hand
-- record. NULLS LAST is explicit on the tie-break: BigQuery and Snowflake disagree on
-- the DEFAULT null-ordering direction for DESC (BigQuery sorts NULLs last on DESC,
-- Snowflake sorts them first), so a bare `order by composite_score desc` would pick a
-- record with zero comparable sub-scores as the "best match" on Snowflake.

with pending as (
    select * from {{ ref('res_pending_probabilistic') }}
),

config as (
    select * from {{ ref('entity_resolution_scoring_config') }}
),

pairs as (
    select
        l.entity_type as entity_type,
        l.source_system as source_system,
        l.source_id as source_id,
        l.entity_name as entity_name,
        r.source_system as matched_source_system,
        r.source_id as matched_source_id,
        r.entity_name as matched_entity_name,
        {{ dpf_string_similarity_score('l.normalised_name', 'r.normalised_name') }} as string_score,
        {{ dpf_categorical_match_score('l.sector_proxy', 'r.sector_proxy') }} as sector_score,
        {{ dpf_date_proximity_score('l.date_proxy', 'r.date_proxy', 'cfg.date_tolerance_days') }} as date_score,
        {{ dpf_value_proximity_score('l.value_proxy', 'r.value_proxy', 'cfg.value_tolerance_pct') }} as value_score,
        cfg.weight_string as weight_string,
        cfg.weight_sector as weight_sector,
        cfg.weight_date as weight_date,
        cfg.weight_value as weight_value,
        cfg.threshold_auto as threshold_auto,
        cfg.threshold_review as threshold_review
    from pending as l
    inner join pending as r
        on r.entity_type = l.entity_type
        and (
            -- Cross-source is the ER goal for every entity fed from >1 source
            -- (fund/gp/portfolio_company). Unchanged: for those entity types the
            -- entity-type-gated arm below is always false, so this predicate is
            -- byte-for-byte the original cross-source-only join.
            r.source_system != l.source_system
            -- Deal ER is INTRA-SOURCE ONLY: ORIGO is a single CRM feed,
            -- so a deal's duplicates share its source_system. Pair distinct deal
            -- records within the same source; the QUALIFY below still keeps each
            -- record's single best counterpart. Gated on entity_type = 'deal' so no
            -- other entity's cross-source behaviour changes. Fund matching remains a
            -- separate cross-source process.
            or (l.entity_type = 'deal' and r.source_id != l.source_id)
        )
    inner join config as cfg
        on cfg.entity_type = l.entity_type
),

scored as (
    select
        *,
        {{ dpf_composite_er_score('string_score', 'sector_score', 'date_score', 'value_score', 'weight_string', 'weight_sector', 'weight_date', 'weight_value') }} as composite_score
    from pairs
)

select
    entity_type,
    source_system,
    source_id,
    entity_name,
    matched_source_system,
    matched_source_id,
    matched_entity_name,
    string_score,
    sector_score,
    date_score,
    value_score,
    composite_score,
    threshold_auto,
    threshold_review
from scored
qualify row_number() over (
    partition by entity_type, source_system, source_id
    order by composite_score desc nulls last, matched_source_system asc, matched_source_id asc
) = 1
