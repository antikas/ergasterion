-- What this proves: the canonical interfaces publish one relation per
-- declared entity over the curated products, one row per key in each, with
-- the values the curation settled.
--
-- The customer interface declares two entities over one curated product, so
-- the two views carry the same customers and different columns; the article
-- interface declares one. The row counts are asserted against the curated
-- products rather than against a number alone, so an interface that lost a
-- row would be reported even if the count typed here were changed to match.
--
-- One value in each interface is asserted as well, because a view that
-- selected the right number of rows from the wrong column would otherwise
-- pass on counts.
--
-- Singular test: it passes when it returns no rows.
with counts as (
    select
        (select count(*) from {{ ref('ecommerce__customer_interface__customer') }}) as interface_customers,
        (select count(*) from {{ ref('ecommerce__customer_interface__customer_contact') }}) as interface_contacts,
        (select count(*) from {{ ref('ecommerce__product_interface__product') }}) as interface_articles,
        (select count(*) from {{ ref('ecommerce__customer') }}) as curated_customers,
        (select count(*) from {{ ref('ecommerce__product') }}) as curated_articles
),

population as (
    select 'an interface does not publish one row per curated key' as disagreement
    from counts
    where interface_customers <> curated_customers
       or interface_contacts <> curated_customers
       or interface_articles <> curated_articles
       or curated_customers <> 5
       or curated_articles <> 4
),

customer_value as (
    select 'the customer interface does not carry the surviving customer values' as disagreement
    from {{ ref('ecommerce__customer_interface__customer') }}
    where customer_business_key = 'LOY-1001'
      and (customer_external_id <> 'CUST-AVA'
        or customer_name <> 'Ava Thompson'
        or customer_buyer_segment <> 'gold')
),

contact_value as (
    select 'the contact interface does not carry the surviving contact values' as disagreement
    from {{ ref('ecommerce__customer_interface__customer_contact') }}
    where customer_business_key = 'LOY-1001'
      and (customer_city <> 'Manchester' or customer_email_domain <> 'example.com')
),

article_value as (
    select 'the article interface does not carry the surviving article values' as disagreement
    from {{ ref('ecommerce__product_interface__product') }}
    where product_code = 'GTIN-1004'
      and (product_name <> 'Coffee Grinder'
        or product_list_price <> cast(91.00 as decimal(12, 2))
        or product_category <> 'kitchen')
)

select disagreement from population
union all
select disagreement from customer_value
union all
select disagreement from contact_value
union all
select disagreement from article_value
