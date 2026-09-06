-- What this proves: where the two catalogues disagree about what an article
-- lists for, the declared survivorship takes the price of the more recently
-- extracted catalogue, and it takes it in both directions.
--
-- The two shared articles are the case. One is listed cheaper by the later
-- catalogue and one dearer, so a rule that always took the lower price, or
-- always took the first catalogue, would fail on one of them. The article
-- each catalogue lists alone keeps its own price, which is the control.
--
-- Singular test: it passes when it returns no rows.
with published as (
    select
        product_code,
        product_list_price,
        product_source_system,
        product_as_of_date
    from {{ ref('ecommerce__product') }}
),

expected (product_code, product_list_price, product_source_system) as (
    values
        ('GTIN-1001', 57.50, 'mercaro'),
        ('GTIN-1002', 19.99, 'cartivo'),
        ('GTIN-1003', 34.99, 'mercaro'),
        ('GTIN-1004', 91.00, 'mercaro')
)

select
    coalesce(published.product_code, expected.product_code) as product_code,
    'the surviving price is not the later catalogue price' as disagreement
from published
full join expected
    on published.product_code = expected.product_code
where published.product_code is null
   or expected.product_code is null
   or published.product_list_price <> cast(expected.product_list_price as decimal(12, 2))
   or published.product_source_system <> expected.product_source_system
