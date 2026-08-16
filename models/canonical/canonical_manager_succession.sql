{{ config(materialized='view', schema='canonical') }}

-- Manager succession event canonical view. OpenIM PM-11 Manager
-- Succession Event conformed from the gp_succession link + event satellite.
-- Self-referencing GP-to-GP relationship: predecessor_gp_id and successor_gp_id
-- are both golden_gp_key values (resolved via hub_gp), and are DISTINCT by design
-- -- predecessor and successor are different legal entities linked by the event,
-- never merged by a shared identifier (the seeded predecessor carries no
-- lei/shared_external_id; the seeded successor's lei is freshly minted and never
-- reused elsewhere). assert_gp_succession_continuity.sql asserts this holds.

with source_events as (
    select
        'VANTORA' as source_system,
        sat.*
    from {{ ref('sat_gp_succession_vantora') }} as sat
),

joined as (
    select
        {{ dbt_utils.generate_surrogate_key([
            'predecessor_hub.golden_gp_key',
            'successor_hub.golden_gp_key',
            'source_events.succession_event_id'
        ]) }} as succession_id,
        predecessor_hub.golden_gp_key as predecessor_gp_id,
        successor_hub.golden_gp_key as successor_gp_id,
        source_events.succession_event_id as source_succession_event_id,
        lower(source_events.event_type) as event_type,
        {{ dpf_safe_cast('source_events.effective_date', 'date') }} as effective_date,
        source_events.record_source,
        source_events.load_datetime
    from source_events
    inner join {{ ref('link_gp_succession') }} as link
        on link.gp_succession_lhk = source_events.gp_succession_lhk
    inner join {{ ref('hub_gp') }} as predecessor_hub
        on predecessor_hub.gp_hk = link.predecessor_gp_hk
    inner join {{ ref('hub_gp') }} as successor_hub
        on successor_hub.gp_hk = link.successor_gp_hk
),

-- Same AutomateDV incremental-duplicate hazard as canonical_legal_vehicle_cash_flow
-- (a link-satellite variant of the same MANY-rows-per-hash-key pattern): the event
-- satellite is keyed on gp_succession_lhk. If this predecessor/successor pair ever
-- carries more than one dated event, the "current hashdiff only" comparison
-- re-inserts non-current rows on subsequent incremental runs. Dedupe one row per
-- logical event (predecessor + successor + source event id), latest load wins --
-- also what makes the targeted build idempotent under a second run.
deduped as (
    select *
    from joined
    qualify row_number() over (
        partition by predecessor_gp_id, successor_gp_id, source_succession_event_id
        order by load_datetime desc
    ) = 1
)

select
    succession_id,
    predecessor_gp_id,
    successor_gp_id,
    source_succession_event_id,
    event_type,
    effective_date,
    record_source,
    load_datetime
from deduped
