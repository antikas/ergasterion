-- Survivorship expectation pin: CRM (RELATIO) wins the contact
-- attribute on a seeded conflict.
--
-- bv_customer_golden_record's survivorship rules (models/business_vault/
-- bv_customer_golden_record.sql, emitted from domains/ecommerce.yml at) put
-- RELATIO first in source_priority for every first_non_null attribute, including
-- city -- so RELATIO, the partial-coverage CRM feed, wins contact attributes over
-- CARTIVO/MERCARO on any conflict. The seeded conflict: CUST-AVA's city is
-- "London" in both CARTIVO (CART-CR-001) and MERCARO (MERC-CR-001), but "Manchester"
-- in RELATIO (RELA-CR-001) -- seeds/raw_relatio_customers.csv. The golden
-- canonical_customer.city for CUST-AVA must resolve to "Manchester" (RELATIO), never
-- "London" (the two non-CRM feeds), and city__source must read 'RELATIO'.
--
-- Singular test: PASSES when it returns zero rows.

with ava_key as (
    -- Same manifest -> res_customer two-hop as the other CUST-AVA-keyed tests in
    -- this domain, reused, not forked.
    select distinct resolved.golden_customer_key
    from {{ ref('customer_er_overlap_manifest') }} as manifest
    inner join {{ ref('res_customer') }} as resolved
        on resolved.source_system = manifest.source_system
        and resolved.source_record_id = manifest.source_record_id
    where manifest.true_customer_external_id = 'CUST-AVA'
      and resolved.golden_customer_key is not null
),

ava_golden as (
    select customer.*
    from {{ ref('canonical_customer') }} as customer
    inner join ava_key
        on ava_key.golden_customer_key = customer.customer_id
)

select 'no_ava_golden_record' as failure_type, cast(null as {{ dbt.type_string() }}) as failure_detail
where not exists (select 1 from ava_golden)

union all

select
    'crm_did_not_win_city' as failure_type,
    coalesce(city, 'NULL') || ' (source=' || coalesce(city__source, 'NULL') || ')' as failure_detail
from ava_golden
where city != 'Manchester'
   or city__source != 'RELATIO'
