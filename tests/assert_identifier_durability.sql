-- Durable-identifier named test.
--
-- Claim under test: a fund's golden key is derived from its DURABLE, ranked
-- identifiers (lei, then shared_external_id -- res_fund's identifier_edges
-- rank 1/2), never from a source-internal id that a source can reissue or
-- reformat between loads. Seeded source-internal ID churn must be absorbed by
-- the alias machinery (known_aliases / external_ids, res_fund's own
-- per-source union), not split into a new identity.
--
-- Ground truth, already in the seeds (no new rows added -- prefers
-- asserting on existing data per its brief): Apex Growth Fund II is seeded in
-- VANTORA as three quarterly-snapshot rows (raw_vantora_funds.csv). Each
-- snapshot carries a DIFFERENT source_record_id -- CEP-FUND-001 / -002 / -003,
-- res_fund's own record-identity component -- and a differently-FORMATTED
-- source-internal fund id each time -- CPS-1001 / CPS-1001-A / CPS1001,
-- res_fund's source_id. That id-format churn is the SEPARATE vendor-id-
-- normalisation pattern's concern (normalise_prefixed_id collapses it to
-- one canonical_vantora_fund_id); this test does not depend on that
-- normalisation succeeding. What makes the three snapshots one fund, by
-- design, is that all three carry the SAME lei (549300APEXGROWTH20001) and
-- the SAME shared_external_id (OPENIM-FUND-APEX-II) -- durable,
-- source-external identifiers that never change across the churn.
--
-- Assertion, non-vacuous in the same query: this fund must resolve from
-- VANTORA under more than one distinct source-internal id AND more than one
-- distinct source-internal record id (the churn actually happened in the
-- seed, so the test cannot pass by there being nothing to absorb) AND
-- collapse to EXACTLY one golden_fund_key (the durable identifiers absorbed
-- it) AND res_fund's own exposed winning-edge column (exact_key_type) must
-- agree, singularly, that 'lei' -- not the id-normalisation edge, not name --
-- is the identifier that actually won the merge for this fund (so the churn
-- being absorbed and durable identifiers being WHY it was absorbed are both
-- asserted, not just the former) AND that golden key's known_aliases on
-- canonical_fund must be non-empty (the alias machinery actually recorded
-- something for it, rather than the fund silently vanishing). A regression
-- that keyed the golden key off source_id or source_record_id instead of the
-- ranked identifiers would split this fund into two or three golden keys and
-- this test would fail; a regression that let a lower-ranked edge win the
-- merge instead of lei would leave golden_fund_key collapsed but flip
-- exact_key_type, and this test would catch that too.
--
-- Singular test: PASSES when it returns zero rows.

with vantora_apex as (
    select
        source_id,
        source_record_id,
        golden_fund_key,
        exact_key_type
    from {{ ref('res_fund') }}
    where source_system = 'vantora'
      and lei = '549300APEXGROWTH20001'
),

churn_check as (
    select
        count(*) as row_count,
        count(distinct source_id) as distinct_source_ids,
        count(distinct source_record_id) as distinct_source_record_ids,
        count(distinct golden_fund_key) as distinct_golden_keys,
        count(distinct exact_key_type) as distinct_winning_key_types,
        min(golden_fund_key) as the_golden_key,
        min(exact_key_type) as the_winning_key_type
    from vantora_apex
),

alias_check as (
    select
        churn_check.*,
        {{ dpf_array_length('canonical_fund.known_aliases') }} as alias_count
    from churn_check
    left join {{ ref('canonical_fund') }} as canonical_fund
        on canonical_fund.fund_id = churn_check.the_golden_key
)

select *
from alias_check
where row_count = 0
   or distinct_source_ids < 2
   or distinct_source_record_ids < 2
   or distinct_golden_keys != 1
   or distinct_winning_key_types != 1
   or coalesce(the_winning_key_type, '') != 'lei'
   or coalesce(alias_count, 0) = 0
