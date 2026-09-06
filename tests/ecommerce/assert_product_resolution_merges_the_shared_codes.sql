-- What this proves: the curated article resolves the two catalogues onto the
-- code they share, so an article both channels sell is one article.
--
-- The two delivered catalogues carry six records for four articles: two
-- articles are listed by both channels under one shared code, and two are
-- listed by one channel only. The resolution declares that shared code as
-- its one key, and this asserts the population it produced, per article and
-- in total, so neither an over-merge nor a missed merge can pass.
--
-- Singular test: it passes when it returns no rows.
with resolved as (
    select
        product_code,
        resolution_entity_key,
        resolution_row_count
    from {{ ref('ecommerce__product__resolution') }}
),

observed as (
    select
        product_code,
        count(*) as delivered_records,
        count(distinct resolution_entity_key) as entities
    from resolved
    group by product_code
),

expected (product_code, delivered_records, entities) as (
    values
        ('GTIN-1001', 2, 1),
        ('GTIN-1002', 1, 1),
        ('GTIN-1003', 1, 1),
        ('GTIN-1004', 2, 1)
),

per_article as (
    select
        coalesce(observed.product_code, expected.product_code) as product_code,
        'the catalogues resolved to a different set of articles' as disagreement
    from observed
    full join expected
        on observed.product_code = expected.product_code
    where observed.product_code is null
       or expected.product_code is null
       or observed.delivered_records <> expected.delivered_records
       or observed.entities <> expected.entities
),

published as (
    select
        cast(null as varchar) as product_code,
        'the published article is not one row per shared code' as disagreement
    from {{ ref('ecommerce__product') }}
    having count(*) <> 4 or count(distinct product_code) <> 4
)

select product_code, disagreement from per_article
union all
select product_code, disagreement from published
