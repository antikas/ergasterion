{{ config(materialized='table', schema='marts') }}

-- Fund-to-GP attribution as of a date (MILESTONE FOLLOW-UP to).
-- Joins link_gp_succession's continuity claim into the dim_investment_classification
-- half-open date-range pattern (docs/architecture/scd2-classification.md):
-- "which GP does this fund attribute to on date D" is answered by joining THIS model
-- to any dated fact on fund_id + (effective_from <= date < effective_to), instead of
-- being recomputed inline inside a test. See docs/architecture/manager-succession.md,
-- "Fund attribution boundary" for the gap this closes and
-- tests/assert_gp_succession_continuity.sql for the named test now querying this
-- surface directly.

with base_attribution as (
    -- The fund's as-sourced GP -- constant, because a succession event changes the
    -- SUCCESSION link, never the fund's own source record (manager-succession.md,
    -- "Fund attribution boundary"). This is the "since always" default assignment,
    -- overridden below wherever a succession event fires against it. date
    -- '1900-01-01' is the same reserved epoch sentinel dim_investment_classification
    -- uses for its UNKNOWN members -- there is no earlier real business date in this
    -- vault.
    select
        fund.fund_id,
        fund.gp_id,
        fund.hub_record_source as record_source,
        fund.hub_load_datetime as load_datetime
    from {{ ref('canonical_fund') }} as fund
    where fund.gp_id is not null
),

succession_overlay as (
    -- One additional row per succession event whose PREDECESSOR is this fund's base
    -- GP -- the fund flips to the successor from effective_date onward. Chained
    -- successions (the successor itself later becoming a predecessor of a further
    -- event) are NOT resolved transitively here -- the same single-hop scope
    -- dim_gp.sql's own predecessor_gp_key/successor_gp_key MAX-aggregation
    -- discloses (no fork or chain is seeded today; if one is added, this CTE needs
    -- the same recursive walk dim_gp would need).
    select
        base_attribution.fund_id,
        succession.successor_gp_id as gp_id,
        succession.effective_date as effective_from,
        succession.record_source,
        succession.load_datetime
    from base_attribution
    inner join {{ ref('canonical_manager_succession') }} as succession
        on succession.predecessor_gp_id = base_attribution.gp_id
),

history as (
    select
        fund_id,
        gp_id,
        date '1900-01-01' as effective_from,
        -- row_priority is a MEANINGFUL tie-break, not a bare surrogate key
        -- (trap): on a same-date collision the explicit, dated succession
        -- event outranks the eternal default, so its window is the one that
        -- extends forward. 0/1 order below is deliberate, not arbitrary.
        0 as row_priority,
        record_source,
        load_datetime
    from base_attribution

    union all

    select
        fund_id,
        gp_id,
        effective_from,
        1 as row_priority,
        record_source,
        load_datetime
    from succession_overlay
),

ranged as (
    select
        history.*,
        lead(effective_from) over (
            partition by fund_id
            -- SCD2 tie-break ordering (hardening): effective_from, then the
            -- meaningful row_priority above, then gp_id (a real business key, never a
            -- generated surrogate hash) as the final deterministic differentiator.
            order by effective_from, row_priority, gp_id
        ) as next_effective_from
    from history
)

select
    {{ dbt_utils.generate_surrogate_key([
        'ranged.fund_id',
        'ranged.gp_id',
        dpf_safe_cast('ranged.effective_from', 'string')
    ]) }} as fund_gp_attribution_key,
    ranged.fund_id,
    {{ dbt_utils.generate_surrogate_key(['ranged.fund_id']) }} as fund_key,
    ranged.gp_id,
    {{ dbt_utils.generate_surrogate_key(['ranged.gp_id']) }} as gp_key,
    gp.gp_name,
    gp.entity_name as gp_entity_name,
    gp.lineage_status as gp_lineage_status,
    case
        when ranged.row_priority = 0 then 'fund_source_record'
        else 'gp_succession_event'
    end as attribution_source_type,
    ranged.effective_from,
    coalesce(ranged.next_effective_from, date '9999-12-31') as effective_to,
    ranged.next_effective_from is null as is_current,
    ranged.record_source,
    ranged.load_datetime
from ranged
left join {{ ref('dim_gp') }} as gp
    on gp.gp_id = ranged.gp_id
