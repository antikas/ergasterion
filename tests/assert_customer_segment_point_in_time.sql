-- Customer-segment SCD2 point-in-time attribution using half-open ranges.
-- (docs/architecture/scd2-classification.md) for the e-commerce customer-360 domain.
--
-- The seeded CUST-AVA moves silver (2025-01-01) -> gold (2025-05-01) --
-- seeds/customer_segment_history_seed.csv. A read the day BEFORE the transition
-- (2025-04-30) must land in silver; a read ON the transition date (2025-05-01,
-- the half-open lower bound of the gold interval) must land in gold. Across ALL
-- seeded customers, dim_customer_segment's half-open ranges must be contiguous
-- (no gap, no overlap).
--
-- Singular test: PASSES when it returns zero rows.

with ava_key as (
    -- Same manifest -> res_customer two-hop as sat_customer_segment_history.sql /
    -- assert_res_customer_er_precision_manifest.sql, reused, not forked.
    select distinct resolved.golden_customer_key
    from {{ ref('customer_er_overlap_manifest') }} as manifest
    inner join {{ ref('res_customer') }} as resolved
        on resolved.source_system = manifest.source_system
        and resolved.source_record_id = manifest.source_record_id
    where manifest.true_customer_external_id = 'CUST-AVA'
      and resolved.golden_customer_key is not null
),

ava_expected as (
    select date '2025-04-30' as as_of_date, 'silver' as expected_segment_code
    union all
    select date '2025-05-01' as as_of_date, 'gold' as expected_segment_code
),

ava_actual as (
    select
        expected.as_of_date,
        segment.segment_code
    from ava_expected as expected
    cross join ava_key
    left join {{ ref('dim_customer_segment') }} as segment
        on segment.customer_id = ava_key.golden_customer_key
        and expected.as_of_date >= segment.effective_from
        and expected.as_of_date < segment.effective_to
),

wrong_attribution as (
    select
        expected.as_of_date,
        expected.expected_segment_code,
        actual.segment_code
    from ava_expected as expected
    left join ava_actual as actual
        on actual.as_of_date = expected.as_of_date
    where actual.segment_code is distinct from expected.expected_segment_code
),

range_overlap as (
    select
        a.customer_id,
        a.effective_from as a_effective_from,
        a.effective_to as a_effective_to,
        b.effective_from as b_effective_from,
        b.effective_to as b_effective_to
    from {{ ref('dim_customer_segment') }} as a
    inner join {{ ref('dim_customer_segment') }} as b
        on b.customer_id = a.customer_id
        and b.customer_segment_key > a.customer_segment_key
        and a.effective_from < b.effective_to
        and b.effective_from < a.effective_to
),

ordered_ranges as (
    select
        customer_id,
        effective_to,
        lead(effective_from) over (
            partition by customer_id
            order by effective_from, segment_recorded_at, customer_segment_history_key
        ) as next_effective_from
    from {{ ref('dim_customer_segment') }}
),

range_gap as (
    select *
    from ordered_ranges
    where next_effective_from is not null
      and effective_to != next_effective_from
)

select 'no_ava_key' as failure_type, cast(null as {{ dbt.type_string() }}) as failure_key
where not exists (select 1 from ava_key)

union all

select 'wrong_attribution' as failure_type, cast(as_of_date as {{ dbt.type_string() }}) as failure_key
from wrong_attribution

union all

select 'range_overlap' as failure_type, customer_id as failure_key
from range_overlap

union all

select 'range_gap' as failure_type, customer_id as failure_key
from range_gap
