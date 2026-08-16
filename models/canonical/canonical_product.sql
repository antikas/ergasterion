{{ config(materialized='view', schema='canonical') }}

-- E-commerce customer-360 canonical view over the business-vault
-- golden product record. Hand-authored, built DIRECTLY off the raw-vault golden key
-- (hub_product.golden_product_key via bv_product_golden_record) -- no
-- canonical_mappings / OpenIM model-repo dependency, same no-model-repo skip path as
-- canonical_customer. product_id IS golden_product_key, the tier-1 deterministic key
-- res_product produces (shared_product_code, a cross-source SKU/GTIN-style id).

with product as (
    select * from {{ ref('bv_product_golden_record') }}
)

select
    product.golden_product_key as product_id,
    product.product_hk,
    product.source_record_id,
    product.source_product_id,
    product.shared_product_code,
    product.product_name,
    product.brand,
    product.category,
    product.list_price,
    product.currency,
    product.product_status,
    product.as_of_date,
    product.product_resolution_tier,
    product.product_resolution_confidence,
    product.hub_load_datetime,
    product.hub_record_source
from product
