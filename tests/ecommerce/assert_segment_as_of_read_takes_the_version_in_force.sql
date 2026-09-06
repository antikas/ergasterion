-- What this proves: the as-of read of the segment history picks the version
-- in force on the day of the order, not the first version, not the last one
-- delivered and not all of them.
--
-- One customer has two versions in the delivered history: they were in the
-- lower segment from the first of January and moved to the higher one on the
-- first of May. Both of their orders were placed in July. A read that took
-- the earliest version would put them in the lower segment; a read that took
-- every version would double their lines. The served fact puts their two
-- orders in the higher segment and carries each of their lines once, and
-- both halves are asserted here.
--
-- The delivered history is read directly, so the test states the two
-- versions exist rather than assuming they do: it cannot pass because the
-- history was never landed.
--
-- Singular test: it passes when it returns no rows.
with history as (
    select
        customer_external_id,
        segment,
        cast(effective_from as date) as effective_from
    from {{ ref('reference__customer_segment_history') }}
    where customer_external_id = 'CUST-AVA'
),

versions_delivered as (
    select 'the delivered history does not carry the two versions this asserts' as disagreement
    from history
    having count(*) <> 2
        or count(*) filter (where segment = 'silver' and effective_from = date '2025-01-01') <> 1
        or count(*) filter (where segment = 'gold' and effective_from = date '2025-05-01') <> 1
),

lines as (
    select
        fact.order_id,
        fact.line_number,
        dimension.segment_name
    from {{ ref('ecommerce__order_star__fact_order_line') }} as fact
    inner join {{ ref('ecommerce__order_star__dim_customer_segment') }} as dimension
        on fact.customer_business_key = dimension.customer_business_key
       and fact.line_order_date >= dimension.effective_from
       and (dimension.effective_to is null or fact.line_order_date < dimension.effective_to)
    where fact.customer_business_key = 'LOY-1001'
),

read_the_wrong_version as (
    select 'a line was analysed under a segment that was not in force on its order date' as disagreement
    from lines
    where segment_name <> 'gold'
),

counted as (
    select 'the as-of read did not carry each line exactly once' as disagreement
    from lines
    having count(*) <> 4
        or count(distinct (order_id, line_number)) <> 4
)

select disagreement from versions_delivered
union all
select disagreement from read_the_wrong_version
union all
select disagreement from counted
