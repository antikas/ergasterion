-- What this proves: survivorship keeps a value only one source system
-- states, whichever system that is, and leaves a column empty when no
-- system states it.
--
-- This is the other half of the precedence proof. The contact system is
-- first in the declared order, so a naive read of the rule would take its
-- value and nothing else. The customer stated by the storefront and the
-- marketplace is not covered by the contact system at all, and the two
-- systems that do state it state different halves: only the storefront
-- states a telephone number and an address, only the marketplace states a
-- buyer segment. Both survive.
--
-- The last arm is the case nobody states: the customer only the storefront
-- covers has no buyer segment anywhere, so the column stays empty rather
-- than being filled with something.
--
-- Singular test: it passes when it returns no rows.
with published as (
    select
        customer_external_id,
        customer_phone,
        customer_address_line,
        customer_buyer_segment,
        customer_country
    from {{ ref('ecommerce__customer') }}
),

cross_feed as (
    select 'a value only one system states did not survive' as disagreement
    from published
    where customer_external_id = 'CUST-BEN'
      and (customer_phone is distinct from '+44-20-7000-0002'
        or customer_address_line is distinct from '5 Oak Lane'
        or customer_buyer_segment is distinct from 'silver'
        or customer_country is distinct from 'GB')
),

nothing_stated as (
    select 'a column no system states was filled in' as disagreement
    from published
    where customer_external_id = 'CUST-CHLOE'
      and customer_buyer_segment is not null
),

marketplace_only as (
    select 'the marketplace-only customer lost the contact columns it never had' as disagreement
    from published
    where customer_external_id = 'CUST-DIEGO'
      and (customer_phone is not null or customer_address_line is not null)
),

covered as (
    select 'the three customers this asserts are not all published' as disagreement
    from published
    where customer_external_id in ('CUST-BEN', 'CUST-CHLOE', 'CUST-DIEGO')
    having count(*) <> 3
)

select disagreement from cross_feed
union all
select disagreement from nothing_stated
union all
select disagreement from marketplace_only
union all
select disagreement from covered
