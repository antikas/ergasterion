-- Named test: succession continuity (PM-11).
--
-- Three independent assertions; any violation fails (the test PASSES on zero rows):
--   1. distinct_golden_keys -- predecessor_gp_id and successor_gp_id in every
--      canonical_manager_succession row resolve to DIFFERENT golden GP keys. They
--      are different legal entities linked by the event; a shared alias/identifier
--      must never merge them into one.
--   2. dangling_reference -- both sides of the event are real, resolved GPs
--      actually present in dim_gp (the entity-resolution + golden-record path
--      produced both, not a reference to nothing).
--   3. fund_attribution_boundary -- for a real fund attributed (via link_fund_gp)
--      to a GP that is a succession predecessor, "who manages this fund" flips
--      from predecessor to successor exactly AT the succession's effective_date:
--      the day before attributes to the predecessor, the day of (and after)
--      attributes to the successor using the same half-open >= convention as the
--      SCD2 uses. The demo scenario ships exactly one such fund (Harbor
--      Infrastructure III, attributed to CEP-GP-03 pre-rebrand); the no-funds
--      guard below makes sure that non-vacuous precondition actually holds.
--      This arm queries dim_fund_gp_attribution by its
--      half-open effective_from/effective_to range instead of recomputing the
--      predecessor/successor flip inline -- the succession pattern's central
--      claim is an observable model behaviour, this test just proves it.

with succession as (
    select * from {{ ref('canonical_manager_succession') }}
),

distinct_key_violations as (
    select
        succession_id,
        'predecessor_and_successor_share_golden_key' as violation
    from succession
    where predecessor_gp_id is null
       or successor_gp_id is null
       or predecessor_gp_id = successor_gp_id
),

dim_gp_keys as (
    select gp_id from {{ ref('dim_gp') }}
),

dangling_reference_violations as (
    select
        succession.succession_id,
        'succession_references_gp_missing_from_dim_gp' as violation
    from succession
    left join dim_gp_keys as predecessor_dim
        on predecessor_dim.gp_id = succession.predecessor_gp_id
    left join dim_gp_keys as successor_dim
        on successor_dim.gp_id = succession.successor_gp_id
    where predecessor_dim.gp_id is null
       or successor_dim.gp_id is null
),

-- Real funds attributed, via the raw-vault fund<->GP link, to a GP that is a
-- succession predecessor.
predecessor_funds as (
    select distinct
        hub_fund.golden_fund_key,
        succession.succession_id,
        succession.predecessor_gp_id,
        succession.successor_gp_id,
        succession.effective_date
    from {{ ref('link_fund_gp') }} as link
    inner join {{ ref('hub_gp') }} as hub_gp
        on hub_gp.gp_hk = link.gp_hk
    inner join {{ ref('hub_fund') }} as hub_fund
        on hub_fund.fund_hk = link.fund_hk
    inner join succession
        on succession.predecessor_gp_id = hub_gp.golden_gp_key
),

-- Two literal reference dates bracketing the seeded 2025-01-01 rebrand, in the
-- style of assert_investment_classification_point_in_time.sql's hardcoded
-- expected values -- ANSI date literals, no dialect-specific date arithmetic.
attribution_scenarios as (
    select
        golden_fund_key, succession_id,
        date '2024-12-31' as as_of_date, predecessor_gp_id as expected_gp_id
    from predecessor_funds

    union all

    select
        golden_fund_key, succession_id,
        date '2025-01-01' as as_of_date, successor_gp_id as expected_gp_id
    from predecessor_funds
),

-- Query the shipped product surface directly instead of
-- recomputing the predecessor/successor flip inline: the half-open range join
-- is the same >= .. < convention dim_investment_classification's own facts use.
surfaced as (
    select
        scenarios.golden_fund_key,
        scenarios.succession_id,
        scenarios.as_of_date,
        scenarios.expected_gp_id,
        attribution.gp_id as attributed_gp_id
    from attribution_scenarios as scenarios
    left join {{ ref('dim_fund_gp_attribution') }} as attribution
        on attribution.fund_id = scenarios.golden_fund_key
        and scenarios.as_of_date >= attribution.effective_from
        and scenarios.as_of_date < attribution.effective_to
),

attribution_violations as (
    select
        cast(golden_fund_key as {{ dbt.type_string() }}) as succession_id,
        'wrong_gp_attribution_for_as_of_date' as violation
    from surfaced
    where attributed_gp_id is distinct from expected_gp_id
),

-- Non-vacuous guard: at least one real fund must be attributed to a succession
-- predecessor, or the attribution-boundary assertion above passes trivially on
-- zero rows.
no_funds_guard as (
    select
        cast(null as {{ dbt.type_string() }}) as succession_id,
        'no_predecessor_attributed_funds_found' as violation
    where (select count(*) from predecessor_funds) = 0
)

select succession_id, violation from distinct_key_violations
union all
select succession_id, violation from dangling_reference_violations
union all
select succession_id, violation from attribution_violations
union all
select succession_id, violation from no_funds_guard
