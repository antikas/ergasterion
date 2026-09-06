-- What this proves: the served order line fact is at the grain it declares,
-- and that grain covers exactly the lines the two channels delivered.
--
-- The declared grain is the order and the line number together. Two things
-- can go wrong with a fact table at a declared grain: it can carry a grain
-- twice, which the generated uniqueness test already catches, and it can
-- carry fewer rows than the source stated, which nothing catches unless the
-- count is asserted against the delivered relations. This asserts the second
-- against the two landing products the lines entered through, so a line lost
-- anywhere in the four generations between them is reported here.
--
-- Singular test: it passes when it returns no rows.
with fact as (
    select order_id, line_number
    from {{ ref('ecommerce__order_star__fact_order_line') }}
),

delivered as (
    select cartivo_record_id as delivered_record_id
    from {{ ref('ecommerce__cartivo_order_line_extract') }}
    union all
    select mercaro_record_id as delivered_record_id
    from {{ ref('ecommerce__mercaro_order_line_extract') }}
),

counted as (
    select
        (select count(*) from fact) as fact_rows,
        (select count(distinct (order_id, line_number)) from fact) as fact_grains,
        (select count(*) from delivered) as delivered_rows
)

select
    'the fact is not one row per delivered order line' as disagreement,
    fact_rows,
    fact_grains,
    delivered_rows
from counted
where fact_rows <> 7
   or fact_grains <> fact_rows
   or fact_rows <> delivered_rows
