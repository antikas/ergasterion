-- What this proves: the order to customer relationship keeps every row both
-- sides brought, including the customer who has never placed an order, and
-- that the two statements of the customer identity it carries agree.
--
-- A merge is not a lookup. The point of declaring the relationship as a
-- consolidating product rather than an enrichment is that a customer with no
-- order is still a row, and it is visible as one rather than being dropped
-- into the difference between two counts nobody takes. This asserts that row
-- exists, that it carries the customer and no order, and that the estate
-- carries no order without a customer.
--
-- The order also carries the customer identity it read on its way through,
-- conformed onto a name of its own so the merge did not have to pick one of
-- two. Where an order is present, the two must agree; that is asserted here
-- because a disagreement would mean the key an order joined on and the
-- customer it merged with are not the same customer.
--
-- Singular test: it passes when it returns no rows.
with relationship as (
    select
        order_id,
        customer_business_key,
        customer_external_id,
        order_customer_external_id,
        customer_name
    from {{ ref('ecommerce__order_customer') }}
),

population as (
    select
        cast(null as varchar) as subject,
        'the relationship is not every order beside every customer' as disagreement
    from relationship
    having count(*) <> 6
        or count(*) filter (where order_id is null) <> 1
        or count(*) filter (where customer_business_key is null) <> 0
),

the_customer_with_no_order as (
    select
        'CUST-ELENA' as subject,
        'the customer who has never ordered is not published with no order' as disagreement
    from relationship
    where customer_external_id = 'CUST-ELENA'
    having count(*) <> 1
        or count(*) filter (where order_id is null and customer_name = 'Elena Petrova') <> 1
),

identity_disagreement as (
    select
        order_id as subject,
        'the identity the order read is not the identity it merged with' as disagreement
    from relationship
    where order_id is not null
      and order_customer_external_id is distinct from customer_external_id
)

select subject, disagreement from population
union all
select subject, disagreement from the_customer_with_no_order
union all
select subject, disagreement from identity_disagreement
