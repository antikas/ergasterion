-- What this proves: where two source systems state a contact attribute
-- differently, the declared survivorship precedence settles it, and it
-- settles it the way the estate declared rather than the way the row order
-- happened to fall.
--
-- The customer all three systems state carries three different email
-- addresses, two different cities and two different telephone numbers. The
-- curated customer declares the contact system first in its source
-- precedence, so the contact system's value survives each of them. This
-- asserts the surviving value is the contact system's, and asserts
-- separately that it is not the storefront's, so a change that stopped
-- applying precedence at all could not pass by accident.
--
-- Singular test: it passes when it returns no rows.
with surviving as (
    select
        customer_external_id,
        customer_city,
        customer_phone,
        customer_email,
        customer_address_line,
        customer_postal_code,
        customer_source_system
    from {{ ref('ecommerce__customer') }}
    where customer_external_id = 'CUST-AVA'
),

expected as (
    select
        'Manchester' as customer_city,
        '+44-16-1000-0001' as customer_phone,
        'ava.thompson@example.com' as customer_email,
        '88 Deansgate' as customer_address_line,
        'M3 2ER' as customer_postal_code
),

took_the_wrong_value as (
    select 'the contact system did not win the conflict' as disagreement
    from surviving
    cross join expected
    where surviving.customer_city <> expected.customer_city
       or surviving.customer_phone <> expected.customer_phone
       or surviving.customer_email <> expected.customer_email
       or surviving.customer_address_line <> expected.customer_address_line
       or surviving.customer_postal_code <> expected.customer_postal_code
),

took_the_storefront_value as (
    select 'the storefront value survived a conflict the contact system also states' as disagreement
    from surviving
    where customer_city = 'London'
       or customer_phone = '+44-20-7000-0001'
       or customer_email = 'ava@example.com'
),

absent as (
    select 'the customer the three systems all state is not published' as disagreement
    from (select count(*) as rows_published from surviving) as counted
    where counted.rows_published <> 1
)

select disagreement from took_the_wrong_value
union all
select disagreement from took_the_storefront_value
union all
select disagreement from absent
