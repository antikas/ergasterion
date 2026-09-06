-- What this proves: the two named rules the estate declares are applied by
-- the generated models, and applied to every row, on the adapter the estate
-- executes.
--
-- One of them bands an order by the value its source system states. It is a
-- rule and not an inline expression because the estate wants the bands
-- reviewed and versioned as code; the declarations name it and never say
-- what a band is. The bands the seeded totals fall into are asserted per
-- order, so a rule whose implementation changed would be reported here
-- rather than in a report.
--
-- The other takes the domain out of a customer's address. It is the rule
-- that is spelled differently on each declared adapter, so its
-- implementation dispatches and this is where the dispatch is proved to have
-- resolved to something that runs and returns the right answer. It is
-- checked against the surviving address itself, not only against a literal,
-- so it cannot pass by returning a constant.
--
-- Singular test: it passes when it returns no rows.
with banded as (
    select order_id, order_total, order_value_band
    from {{ ref('ecommerce__order') }}
),

expected_band (order_id, order_value_band) as (
    values
        ('CART-O-001', 'medium'),
        ('CART-O-002', 'medium'),
        ('MERC-O-001', 'medium'),
        ('MERC-O-002', 'large'),
        ('MERC-O-003', 'small')
),

wrong_band as (
    select
        coalesce(banded.order_id, expected_band.order_id) as subject,
        'the value band the named rule produced is not the declared band' as disagreement
    from banded
    full join expected_band
        on banded.order_id = expected_band.order_id
    where banded.order_id is null
       or expected_band.order_id is null
       or banded.order_value_band <> expected_band.order_value_band
),

unbanded as (
    select
        order_id as subject,
        'an order reached the curated product with no value band' as disagreement
    from banded
    where order_value_band is null
),

domains as (
    select
        customer_external_id,
        customer_email,
        customer_email_domain
    from {{ ref('ecommerce__customer') }}
),

wrong_domain as (
    select
        customer_external_id as subject,
        'the email domain the named rule produced is not the domain of the surviving address' as disagreement
    from domains
    where customer_email_domain is null
       or customer_email is null
       or customer_email not like '%@' || customer_email_domain
),

domain_coverage as (
    select
        cast(null as varchar) as subject,
        'the named rule did not reach every customer' as disagreement
    from domains
    having count(*) <> count(customer_email_domain) or count(*) <> 5
)

select subject, disagreement from wrong_band
union all
select subject, disagreement from unbanded
union all
select subject, disagreement from wrong_domain
union all
select subject, disagreement from domain_coverage
