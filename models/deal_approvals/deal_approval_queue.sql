-- Pending-approvals worklist: resolved deals sitting at the DECISION stage
-- without a terminal decision -- the approver's queue. "Terminal" means approve /
-- approve_with_conditions / decline, each of which derives the deal's next stage
-- (int_deal_stage_from_decision.sql) and so moves the deal OUT of DECISION by
-- construction; defer is explicitly non-terminal, so a deferred deal stays visible
-- here with its decision/conditions/actor/decided_at carried through for context. A
-- deal with no decision at all also stays visible (latest_decision is null).

with deal as (
    select * from {{ ref('dim_deal') }}
),

current_stage as (
    select * from {{ ref('dim_deal_stage') }}
    where is_current
      and deal_id != 'UNKNOWN'
),

latest_decision as (
    select * from {{ ref('int_deal_latest_decision') }}
)

select
    deal.deal_key,
    deal.deal_id,
    deal.external_deal_id,
    deal.deal_name,
    deal.strategy,
    deal.sourced_date,
    current_stage.stage_code,
    current_stage.stage_name,
    current_stage.effective_from as stage_effective_from,
    current_stage.stage_duration_days as days_in_current_stage,
    latest_decision.decision as latest_decision,
    latest_decision.conditions,
    latest_decision.actor,
    latest_decision.decided_at,
    deal.deal_resolution_tier,
    deal.deal_resolution_confidence
from deal
inner join current_stage
    on current_stage.deal_id = deal.deal_id
left join latest_decision
    on latest_decision.external_deal_id = deal.external_deal_id
where current_stage.stage_code = 'DECISION'
order by deal.sourced_date
