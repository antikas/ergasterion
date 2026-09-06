-- What this proves: the segment dimension carries the segment each customer
-- was in when they ordered, as one version per customer with a half-open
-- effective range that is open at the end.
--
-- The delivered segment history states six versions across five customers,
-- and one customer moved segment in the middle of the year. Every order in
-- this estate was placed after that move, so the served dimension carries
-- one version per ordering customer and each of them is open: there is no
-- later version to close it. What the version says is the point of the
-- test, and it is asserted per customer against the history that was
-- delivered.
--
-- The customer who has never ordered is not in the dimension at all, which
-- is asserted too: a dimension of a fact carries the members the fact
-- refers to.
--
-- Singular test: it passes when it returns no rows.
with dimension as (
    select
        customer_business_key,
        segment_name,
        effective_from,
        effective_to
    from {{ ref('ecommerce__order_star__dim_customer_segment') }}
),

expected (customer_business_key, segment_name, effective_from) as (
    values
        ('LOY-1001', 'gold', date '2025-07-01'),
        ('LOY-1003', 'silver', date '2025-07-03'),
        ('LOY-1004', 'bronze', date '2025-07-06'),
        ('ben.carter@example.com', 'bronze', date '2025-07-07')
),

per_customer as (
    select
        coalesce(dimension.customer_business_key, expected.customer_business_key) as customer_business_key,
        'the segment version is not the one in force when the customer ordered' as disagreement
    from dimension
    full join expected
        on dimension.customer_business_key = expected.customer_business_key
    where dimension.customer_business_key is null
       or expected.customer_business_key is null
       or dimension.segment_name <> expected.segment_name
       or dimension.effective_from <> expected.effective_from
),

not_open as (
    select
        customer_business_key,
        'a version is closed although no later version starts' as disagreement
    from dimension
    where effective_to is not null
),

non_ordering_member as (
    select
        customer_business_key,
        'the dimension carries a customer the fact never refers to' as disagreement
    from dimension
    where customer_business_key = 'LOY-1005'
)

select customer_business_key, disagreement from per_customer
union all
select customer_business_key, disagreement from not_open
union all
select customer_business_key, disagreement from non_ordering_member
