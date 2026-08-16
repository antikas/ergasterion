-- Named test: no dropped succession events.
--
-- Failure mode (docs/architecture/manager-succession.md, "Failure mode: unresolvable
-- event sides"): br_dv_vantora_succession_events inner-joins res_gp TWICE -- once for
-- the predecessor's natural id, once for the successor's -- and filters BOTH golden
-- keys to non-null. An event where either side fails GP resolution is dropped from
-- link_gp_succession with no error raised.
--
-- This compares the full staged succession-event set (stg_vantora_succession_events,
-- upstream of any resolution filtering) against the set that actually survived into
-- sat_gp_succession_vantora -- the link satellite that hangs off link_gp_succession's
-- own hash key (gp_succession_lhk) and is populated 1:1 from the same filtered bridge,
-- so satellite membership is exactly link_gp_succession membership, with the
-- succession_event_id needed to name the dropped row still attached.
--
-- Singular test: PASSES when it returns zero rows. A staged event whose predecessor
-- or successor natural id is renamed, mistyped, or otherwise fails res_gp resolution
-- (today every seeded event's both sides resolve) would appear here, naming the
-- dropped succession_event_id.

with staged_events as (
    select
        source_system,
        succession_event_id
    from {{ ref('stg_vantora_succession_events') }}
),

surviving_events as (
    select distinct
        succession_event_id
    from {{ ref('sat_gp_succession_vantora') }}
)

select
    'dropped_succession_event' as failure_type,
    staged_events.succession_event_id as failure_key
from staged_events
left join surviving_events
    on surviving_events.succession_event_id = staged_events.succession_event_id
where surviving_events.succession_event_id is null
