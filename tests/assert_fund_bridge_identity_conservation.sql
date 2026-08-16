-- Bridge-identity conservation test.
--
-- correctness claim: fund identity is CONSERVED across the Data Vault
-- bridge step. The four per-source resolution bridges (br_dv_*_funds) each carry
-- the resolved golden_fund_key for every source fund record that resolved; the
-- AutomateDV stage models hash golden_fund_key into fund_hk and hub_fund
-- deduplicates on that hash. Conservation means the hash step neither invents,
-- drops, nor splits an identity -- the classic Data Vault hub/bridge
-- identity-conservation property (row/identity conservation across a bridge).
--
-- Three violations are reported (any one fails the test):
--   1. lost      -- a golden_fund_key present in the bridges is absent from
--                   hub_fund (an identity was dropped in the load);
--   2. fabricated-- a golden_fund_key in hub_fund has no bridge origin (an
--                   identity appeared from nowhere);
--   3. split     -- one golden_fund_key maps to more than one fund_hk in the hub
--                   (the business-key -> hash mapping is not 1:1, e.g. a hash
--                   collision or non-deterministic key build).
--
-- Singular test: PASSES when it returns zero rows.
with bridge_identities as (
    select golden_fund_key from {{ ref('br_dv_vantora_funds') }} where golden_fund_key is not null
    union
    select golden_fund_key from {{ ref('br_dv_meridex_funds') }} where golden_fund_key is not null
    union
    select golden_fund_key from {{ ref('br_dv_portiq_funds') }} where golden_fund_key is not null
    union
    select golden_fund_key from {{ ref('br_dv_chrono_funds') }} where golden_fund_key is not null
),

hub_identities as (
    select
        golden_fund_key,
        fund_hk
    from {{ ref('hub_fund') }}
),

lost as (
    select
        bridge_identities.golden_fund_key,
        'lost_from_hub' as violation
    from bridge_identities
    left join hub_identities
        on hub_identities.golden_fund_key = bridge_identities.golden_fund_key
    where hub_identities.golden_fund_key is null
),

fabricated as (
    select
        hub_identities.golden_fund_key,
        'fabricated_in_hub' as violation
    from hub_identities
    left join bridge_identities
        on bridge_identities.golden_fund_key = hub_identities.golden_fund_key
    where bridge_identities.golden_fund_key is null
),

split as (
    select
        golden_fund_key,
        'split_across_multiple_hashes' as violation
    from hub_identities
    group by golden_fund_key
    having count(distinct fund_hk) > 1
)

select golden_fund_key, violation from lost
union all
select golden_fund_key, violation from fabricated
union all
select golden_fund_key, violation from split
