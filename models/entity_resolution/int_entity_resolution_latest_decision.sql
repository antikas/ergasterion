-- Latest-wins dedup over the append-only entity_resolution_decisions_log.
-- The log is append-only BY DESIGN (streamlit/streamlit_app.py's write_decision()
-- only ever INSERTs, never UPDATEs/DELETEs) -- so an analyst who re-adjudicates a
-- pair (changes their mind, or a decision gets re-submitted) leaves every PRIOR
-- decision row in place and appends a new one. Left unhandled, that would fan a
-- single reviewed pair out into multiple rows wherever it gets joined.
--
-- This model collapses that history to exactly ONE row per unordered
-- (entity_type, source pair) -- the most recent reviewed_at wins -- using the SAME
-- unordered-pair key review_queue.sql's join already treats as one pair (least/
-- greatest over the concatenated source_system|source_id on each side, so a pair
-- decided as A-vs-B or B-vs-A collapses identically). Every downstream consumer
-- (review_queue.sql, and any future one) reads THIS model, never the raw log
-- directly, so latest-wins dedup is enforced once, here, not re-implemented at every
-- call site.

with decisions as (
    select * from {{ source('entity_resolution_raw', 'entity_resolution_decisions_log') }}
),

ranked as (
    select
        decisions.*,
        row_number() over (
            partition by
                entity_type,
                least(
                    source_system_a || '|' || source_id_a,
                    source_system_b || '|' || source_id_b
                ),
                greatest(
                    source_system_a || '|' || source_id_a,
                    source_system_b || '|' || source_id_b
                )
            order by reviewed_at desc
        ) as decision_recency_rank
    from decisions
)

select
    entity_type,
    source_system_a,
    source_id_a,
    source_system_b,
    source_id_b,
    decision,
    matched_entity_key,
    reviewed_by,
    reviewed_at,
    notes
from ranked
where decision_recency_rank = 1
