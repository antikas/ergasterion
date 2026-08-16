{{ config(materialized='view', schema='canonical') }}

-- E-commerce customer-360 canonical view over the business-vault golden customer
-- record. Hand-authored, not templated, and built
-- DIRECTLY off the raw-vault golden key (hub_customer.golden_customer_key via
-- bv_customer_golden_record) with NO canonical_mappings / OpenIM model-repo
-- dependency: this domain has no OpenIM PM entity to validate against, so there is
-- nothing to bind to here. customer_id is golden_customer_key, the tier-1
-- deterministic key res_customer produces (shared loyalty id or normalised email).
--
-- Contact-attribute __source columns are exposed (not just the survived value) so
-- the CRM-wins-contact-attribute survivorship rule (RELATIO first in source_priority
-- for every first_non_null attribute -- macros/survivorship.sql via
-- bv_customer_golden_record) is auditable straight off this model, not just provable
-- by re-deriving it. tests/assert_customer_survivorship_crm_wins_contact.sql verifies it.

with customer as (
    select * from {{ ref('bv_customer_golden_record') }}
)

select
    customer.golden_customer_key as customer_id,
    customer.customer_hk,
    customer.source_record_id,
    customer.source_customer_id,
    customer.loyalty_id,
    customer.email,
    customer.full_name,
    customer.phone,
    customer.address_line,
    customer.city,
    customer.postal_code,
    customer.country,
    customer.marketing_consent,
    customer.preferred_channel,
    customer.buyer_segment,
    customer.customer_status,
    customer.as_of_date,
    customer.customer_resolution_tier,
    customer.customer_resolution_confidence,
    customer.hub_load_datetime,
    customer.hub_record_source,
    customer.email__source,
    customer.phone__source,
    customer.address_line__source,
    customer.city__source,
    customer.postal_code__source,
    customer.full_name__source
from customer
