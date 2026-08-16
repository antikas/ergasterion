-- Vehicle-to-fund cash-flow conservation test.
--
-- Structural claim: cash is CONSERVED across the vehicle->fund grain change. The
-- SPV/legal-vehicle layer carries cash flows at vehicle grain; the vehicle->fund
-- aggregation bridge (bridge_vehicle_fund_cash_flow) rolls them up to the fund the
-- vehicles belong to. Conservation means that roll-up reconciles to the fund's OWN
-- fund-grain cash-flow total within a materiality epsilon -- the vehicle detail does
-- not invent, lose, or mis-route cash relative to the fund it aggregates into. This
-- is the assert_bridge_identity pattern applied to a monetary measure rather than to
-- identity keys (cf. assert_fund_bridge_identity_conservation for the identity form).
--
-- The seeded vehicle flows are deliberately off the fund total by a subtle amount
-- (the 125.0-vs-124.4 convention used across this repo's cross-source seeds), well
-- inside epsilon. A real break -- a doubled vehicle flow, a vehicle mapped to the
-- wrong fund, a dropped roll-up -- moves the delta far past epsilon and fails here.
--
-- Singular test: PASSES when it returns zero rows.
{% set epsilon = 50000 %}

select
    fund_id,
    vehicle_cash_flow_total,
    fund_cash_flow_total,
    reconciliation_delta
from {{ ref('bridge_vehicle_fund_cash_flow') }}
where abs(reconciliation_delta) > {{ epsilon }}
