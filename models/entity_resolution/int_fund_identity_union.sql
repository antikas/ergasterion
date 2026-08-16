-- Cross-source identity union at res_fund grain.
--
-- res_fund already carries the resolved cross-source union at
-- (golden_fund_key, source_system, source_id, fund_name) grain -- every source
-- record that matched into a given golden fund. This model only aggregates that
-- existing union up to one row per golden_fund_key; it does not re-run entity
-- resolution or discard anything res_fund already produced.
--
-- known_aliases and external_ids on canonical_fund are DERIVED denormalised
-- read-caches regenerated from this union, never taken from
-- bv_fund_golden_record's survivorship-collapsed single-winner row.
--
-- known_aliases (built here) is the DISTINCT set of every source-provided
-- fund_name for the resolved fund. external_ids (built here) is a PER-SOURCE
-- map ({record_source: source_id, ...}) covering every contributing source, not
-- just the survivorship winner.
--
-- known_aliases and external_ids are aggregated in separate CTEs and combined
-- with a plain JOIN (never a correlated subquery in the SELECT list): Snowflake
-- rejects a scalar correlated subquery when the enclosing model is a VIEW
-- (002031 "Unsupported subquery type cannot be evaluated inside VIEW object") --
-- the same trap macros/survivorship.sql already avoids for the most_recent
-- winner join.

with resolved as (
    select
        golden_fund_key,
        source_system,
        source_id,
        fund_name
    from {{ ref('res_fund') }}
    where golden_fund_key is not null
),

-- One representative source_id per (golden_fund_key, source_system): a single
-- source can contribute more than one raw record to the same resolved fund
-- (e.g. VANTORA quarterly snapshots that share a normalised vantora fund id, or
-- multiple source_record_id rows for one source_fund_id), and external_ids is a
-- per-SOURCE map -- one key per contributing source -- not a per-record one.
-- Deterministic pick (lowest source_id) so the map is stable across repeated
-- builds regardless of source-row order.
per_source as (
    select
        golden_fund_key,
        source_system,
        source_id
    from resolved
    qualify row_number() over (
        partition by golden_fund_key, source_system
        order by source_id asc
    ) = 1
),

aliases as (
    select
        golden_fund_key,
        {{ dpf_array_agg_distinct('fund_name') }} as known_aliases
    from resolved
    group by golden_fund_key
),

ids as (
    select
        golden_fund_key,
        {{ dpf_map_agg('source_system', 'source_id') }} as external_ids
    from per_source
    group by golden_fund_key
)

select
    aliases.golden_fund_key,
    aliases.known_aliases,
    ids.external_ids
from aliases
inner join ids
    on ids.golden_fund_key = aliases.golden_fund_key
