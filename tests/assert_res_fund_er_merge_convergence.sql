-- Convergence / residual-unmerged guard for the transitive tier-1 ER.
--
-- res_fund resolves fund identity by computing connected components over the
-- record<->identifier-value graph: two records sharing ANY ranked identifier value
-- (LEI, shared external id, normalised VANTORA id, or normalised name) must land in
-- the same cluster, transitively, and therefore under the SAME golden_fund_key. The
-- components are built by a FIXED-iteration min-label propagation (5 rounds), which
-- converges in as many rounds as a component's diameter. The seeded population's
-- components are diameter <= 3, so 5 rounds carry a margin -- but a fixed bound is
-- only safe if silent under-convergence is caught, not assumed.
--
-- This is that guard. If propagation stopped before a component fully merged, two
-- resolved records that share an identifier value would still carry DIFFERENT
-- golden_fund_keys. We reconstruct the identifier edges from the resolved rows and
-- assert that no single identifier value maps to more than one golden_fund_key. It
-- also catches the inverse pathology: a false-merge design that left a real shared
-- identifier straddling two keys.
--
-- Singular test: PASSES when it returns zero rows.
with resolved as (
    select
        golden_fund_key,
        lei,
        shared_external_id,
        canonical_vantora_fund_id,
        normalised_name
    from {{ ref('res_fund') }}
    where golden_fund_key is not null
),

identifier_edges as (
    select concat('lei:', upper(lei)) as id_value, golden_fund_key
    from resolved
    where lei is not null

    union all

    select concat('external:', upper(shared_external_id)) as id_value, golden_fund_key
    from resolved
    where shared_external_id is not null

    union all

    select concat('vantora:', canonical_vantora_fund_id) as id_value, golden_fund_key
    from resolved
    where canonical_vantora_fund_id is not null

    union all

    select concat('name:', normalised_name) as id_value, golden_fund_key
    from resolved
    where normalised_name is not null
),

under_converged as (
    select
        id_value,
        count(distinct golden_fund_key) as distinct_golden_keys
    from identifier_edges
    group by id_value
    having count(distinct golden_fund_key) > 1
)

select id_value, distinct_golden_keys
from under_converged
