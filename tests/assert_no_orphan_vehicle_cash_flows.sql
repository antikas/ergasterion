-- Named test: no orphan vehicle cash flows.
--
-- Failure mode (docs/architecture/spv-legal-vehicle.md): br_dv_vantora_legal_vehicles
-- / br_dv_meridex_legal_vehicles inner-join res_fund on the vehicle's declared parent
-- fund and filter to golden_fund_key is not null, so a vehicle whose parent fund
-- fails fund resolution is dropped from hub_legal_vehicle entirely. Its cash flows
-- still land in sat_legal_vehicle_cash_flow_vantora (gated only on the vehicle's own
-- natural id, never on fund resolution) and canonical_legal_vehicle_cash_flow's
-- inner join back to hub_legal_vehicle then silently drops them -- no error raised.
--
-- Two arms; either produces failing rows (the test PASSES on zero rows):
--
--   (a) orphan_cash_flow -- a satellite row whose legal_vehicle_hk has no matching
--      row in hub_legal_vehicle. Names the vehicle natural id and source cash-flow
--      id so the offending flow is traceable back to the seed/source record.
--
--   (b) fund_missing_from_bridge -- the conservation blind spot. bridge_vehicle_fund_cash_flow
--      inner-joins a vehicle-grain rollup to a fund-grain rollup (canonical_fund_cash_flow),
--      so it only ever covers a fund that has BOTH grains. This arm asserts exactly that
--      contract: every RESOLVED fund with vehicle-grain cash flows (via the satellite's
--      declared source fund id, resolved through res_fund directly -- the same
--      resolution br_dv_vantora_legal_vehicles applies -- independently of
--      hub_legal_vehicle/canonical_legal_vehicle, so it still catches the whole-fund
--      blind spot when every vehicle was orphaned) AND fund-grain cash flows in
--      canonical_fund_cash_flow must appear in the bridge -- catching the case where a
--      resolvable fund's rollup disappears from the bridge entirely even though both
--      grains of data exist for it. A resolved fund with vehicle flows but NO fund-grain
--      flows is legitimately absent from the bridge (the inner join's real contract, not
--      a bug) and is correctly excluded from this arm's expected set. The unresolvable-fund
--      case -- a vehicle whose parent fund never resolves at all -- is caught by arm (a)
--      (orphan_cash_flow), not this one.

with satellite_flows as (
    select
        sat.legal_vehicle_hk,
        sat.vehicle_natural_id,
        sat.cash_flow_id,
        'vantora' as source_system
    from {{ ref('sat_legal_vehicle_cash_flow_vantora') }} as sat
),

hub_keys as (
    select legal_vehicle_hk from {{ ref('hub_legal_vehicle') }}
),

orphan_cash_flow as (
    select
        'orphan_cash_flow' as failure_type,
        satellite_flows.vehicle_natural_id || ':' || satellite_flows.cash_flow_id as failure_key
    from satellite_flows
    left join hub_keys
        on hub_keys.legal_vehicle_hk = satellite_flows.legal_vehicle_hk
    where hub_keys.legal_vehicle_hk is null
),

-- Independent expected-fund-coverage path: satellite vehicle -> declared source fund
-- id (via stg_vantora_legal_vehicles, NOT via the filtered br_dv_vantora_legal_vehicles
-- bridge) -> res_fund. This resolves a golden fund key even when the vehicle itself
-- was dropped from hub_legal_vehicle, so it can catch the whole-fund blind spot.
vehicle_source_funds as (
    select distinct
        legal_vehicle.source_system,
        legal_vehicle.vehicle_natural_id,
        legal_vehicle.source_fund_id
    from {{ ref('stg_vantora_legal_vehicles') }} as legal_vehicle
),

-- bridge_vehicle_fund_cash_flow's own contract: it inner-joins the vehicle rollup to
-- canonical_fund_cash_flow's fund-grain rollup, so a fund without fund-level cash
-- flows can never appear there regardless of vehicle coverage. Mirror that here so a
-- vehicle-only fund (real absence, not a drop) does not false-fail this arm.
funds_with_fund_level_flows as (
    select distinct fund_id
    from {{ ref('canonical_fund_cash_flow') }}
),

expected_funds as (
    select distinct
        res_fund.golden_fund_key
    from satellite_flows
    inner join vehicle_source_funds
        on vehicle_source_funds.source_system = satellite_flows.source_system
        and vehicle_source_funds.vehicle_natural_id = satellite_flows.vehicle_natural_id
    inner join {{ ref('res_fund') }} as res_fund
        on res_fund.source_system = vehicle_source_funds.source_system
        and res_fund.source_id = vehicle_source_funds.source_fund_id
    inner join funds_with_fund_level_flows
        on funds_with_fund_level_flows.fund_id = res_fund.golden_fund_key
    where res_fund.golden_fund_key is not null
),

bridge_funds as (
    select distinct fund_id from {{ ref('bridge_vehicle_fund_cash_flow') }}
),

fund_missing_from_bridge as (
    select
        'fund_missing_from_bridge' as failure_type,
        cast(expected_funds.golden_fund_key as {{ dbt.type_string() }}) as failure_key
    from expected_funds
    left join bridge_funds
        on bridge_funds.fund_id = expected_funds.golden_fund_key
    where bridge_funds.fund_id is null
)

select failure_type, failure_key from orphan_cash_flow
union all
select failure_type, failure_key from fund_missing_from_bridge
