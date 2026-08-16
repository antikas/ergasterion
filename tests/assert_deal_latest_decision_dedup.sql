-- Named test: latest-wins dedup over deal_decision_log
-- (int_deal_latest_decision.sql). The seeded ORIGO-EXT-DUP-B deal carries TWO
-- competing decision rows: an earlier defer (2025-05-10, DEC-ORIGO-EXT-DUP-B-01) and
-- a later approve_with_conditions (2025-05-20, DEC-ORIGO-EXT-DUP-B-02) -- the IC
-- reconvening after its initial deferral. Latest-wins (decided_at desc, decision_id
-- desc as the deterministic tie-break) must collapse this to exactly ONE row: the
-- later approve_with_conditions decision, carrying its conditions text -- never the
-- earlier defer, and never a fan-out to two rows.
--
-- Singular test: PASSES when it returns zero rows. It returns a row if
-- int_deal_latest_decision does not resolve ORIGO-EXT-DUP-B to exactly one row with
-- the expected (later) decision, decision_id, and conditions.

with latest as (
    select *
    from {{ ref('int_deal_latest_decision') }}
    where external_deal_id = 'ORIGO-EXT-DUP-B'
)

select 'row_count' as failure_type, cast(count(*) as {{ dbt.type_string() }}) as failure_detail
from latest
having count(*) != 1

union all

select 'wrong_decision_won' as failure_type, decision as failure_detail
from latest
where decision != 'approve_with_conditions'

union all

select 'wrong_decision_id_won' as failure_type, decision_id as failure_detail
from latest
where decision_id != 'DEC-ORIGO-EXT-DUP-B-02'

union all

select 'conditions_not_carried' as failure_type, coalesce(conditions, 'NULL') as failure_detail
from latest
where conditions is distinct from 'Subject to satisfactory final reference calls on the management team'
