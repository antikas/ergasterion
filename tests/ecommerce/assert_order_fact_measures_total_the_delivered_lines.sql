-- What this proves: the measures the served fact carries add up to what the
-- source systems delivered, so nothing was lost, doubled or recomputed on
-- the way through four generations of products.
--
-- Revenue and units are asserted against the delivered line extracts rather
-- than against a number typed here alone, and against the typed number as
-- well, so a change that altered both the fact and the extract read would
-- still be caught.
--
-- The list value is the one measure no source system states: it is computed
-- in the served product from the line quantity and the surviving list price
-- of the resolved article. Its total is the arithmetic of the resolved
-- prices, which is why it differs from revenue, and it is asserted so the
-- calculated measure is proved and not merely present.
--
-- Singular test: it passes when it returns no rows.
with fact as (
    select line_quantity, line_revenue, line_list_value
    from {{ ref('ecommerce__order_star__fact_order_line') }}
),

delivered as (
    select
        cast(quantity as integer) as delivered_quantity,
        cast(line_revenue as decimal(12, 2)) as delivered_revenue
    from {{ ref('ecommerce__cartivo_order_line_extract') }}
    union all
    select
        cast(quantity as integer) as delivered_quantity,
        cast(line_revenue as decimal(12, 2)) as delivered_revenue
    from {{ ref('ecommerce__mercaro_order_line_extract') }}
),

totals as (
    select
        (select sum(line_revenue) from fact) as fact_revenue,
        (select sum(line_quantity) from fact) as fact_units,
        (select sum(line_list_value) from fact) as fact_list_value,
        (select sum(delivered_revenue) from delivered) as delivered_revenue,
        (select sum(delivered_quantity) from delivered) as delivered_units
)

select
    'the fact measures do not total the delivered lines' as disagreement,
    fact_revenue,
    fact_units,
    fact_list_value
from totals
where fact_revenue <> delivered_revenue
   or fact_units <> delivered_units
   or fact_revenue <> cast(499.44 as decimal(12, 2))
   or fact_units <> 9
   or fact_list_value <> cast(497.96 as decimal(12, 2))
