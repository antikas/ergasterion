-- Input surface for the probabilistic tier-2 scorer (models/entity_resolution/
-- int_entity_resolution_candidate_pairs.sql + res_entity_resolution_tier2.sql). Records
-- tier 0/1 could not resolve, widened with the four comparison attributes the composite
-- score needs: entity_name/normalised_name (string), sector_proxy (sector), date_proxy
-- (date), value_proxy (value). All four are drawn from columns res_fund/res_gp/
-- res_portfolio_company already expose -- no declarations/emitter change -- so:
--   * sector_proxy is a per-entity-type nearest categorical attribute: fund ->
--     strategy (falls back to asset_class), gp -> domicile, portfolio_company -> sector
--     (the only literal "sector" among the three).
--   * date_proxy is fund -> vintage_year (widened to 1 Jan of that year so it is
--     comparable as a DATE), gp -> relationship_start_date; portfolio_company carries
--     no date attribute at the entity-resolution grain, so date_proxy is NULL.
--   * value_proxy is NULL for all three entity types: no source pipes a monetary
--     amount through to entity resolution (see the header note in
--     macros/entity_resolution_scoring.sql). The composite-score macro renormalises
--     weights across whichever sub-scores are non-NULL, so this degrades gracefully
--     rather than dragging every score down with a fabricated 0.

select
    'fund' as entity_type,
    source_system,
    source_id,
    fund_name as entity_name,
    normalised_name,
    deterministic_match_key,
    exact_key_type,
    coalesce(strategy, asset_class) as sector_proxy,
    case
        when vintage_year is not null
            then {{ dpf_safe_cast("concat(cast(vintage_year as string), '-01-01')", 'date') }}
    end as date_proxy,
    cast(null as numeric) as value_proxy
from {{ ref('res_fund') }}
where pending_probabilistic

union all

select
    'gp' as entity_type,
    source_system,
    source_id,
    gp_name as entity_name,
    normalised_name,
    deterministic_match_key,
    exact_key_type,
    domicile as sector_proxy,
    relationship_start_date as date_proxy,
    cast(null as numeric) as value_proxy
from {{ ref('res_gp') }}
where pending_probabilistic

union all

select
    'portfolio_company' as entity_type,
    source_system,
    source_id,
    company_name as entity_name,
    normalised_name,
    deterministic_match_key,
    exact_key_type,
    sector as sector_proxy,
    cast(null as date) as date_proxy,
    cast(null as numeric) as value_proxy
from {{ ref('res_portfolio_company') }}
where pending_probabilistic

union all

-- deal. INTRA-SOURCE deal resolution: a tier-1 leftover is a deal record
-- with no external id that matched no other record on normalised name alone. Its
-- proxies: strategy (sector), sourced_date (date), no monetary value at this grain.
-- Deal candidate pairing is intra-source (single CRM source ORIGO), enabled by the
-- entity-type-gated arm in int_entity_resolution_candidate_pairs.sql. Fund matching
-- remains a separate cross-source process.
select
    'deal' as entity_type,
    source_system,
    source_id,
    deal_name as entity_name,
    normalised_name,
    deterministic_match_key,
    exact_key_type,
    strategy as sector_proxy,
    sourced_date as date_proxy,
    cast(null as numeric) as value_proxy
from {{ ref('res_deal') }}
where pending_probabilistic
