-- What this proves: the order summary adds up. The revenue of an order's
-- lines, aggregated at the order grain from the served fact, is the total
-- the source system stated for that order on its own header.
--
-- The two sides of this comparison come down two different routes. The
-- header total travels through the order lane, the curated order and the
-- consolidating products; the line revenue travels through the line lane,
-- the consolidated lines, the article merge and the served star. They meet
-- here for the first time, so a fault in either route shows up as an order
-- whose summary does not match its header.
--
-- Singular test: it passes when it returns no rows.
with summary as (
    select
        order_id,
        order_line_count,
        order_units,
        order_revenue
    from {{ ref('ecommerce__order_summary') }}
),

headers as (
    select
        order_id,
        order_total
    from {{ ref('ecommerce__order') }}
),

per_order as (
    select
        coalesce(summary.order_id, headers.order_id) as order_id,
        'the summarised revenue is not the order total the source stated' as disagreement
    from summary
    full join headers
        on summary.order_id = headers.order_id
    where summary.order_id is null
       or headers.order_id is null
       or summary.order_revenue <> headers.order_total
),

expected (order_id, order_line_count, order_units) as (
    values
        ('CART-O-001', 2, 3),
        ('CART-O-002', 1, 1),
        ('MERC-O-001', 2, 2),
        ('MERC-O-002', 1, 2),
        ('MERC-O-003', 1, 1)
),

per_count as (
    select
        coalesce(summary.order_id, expected.order_id) as order_id,
        'the summary counts a different number of lines or units' as disagreement
    from summary
    full join expected
        on summary.order_id = expected.order_id
    where summary.order_id is null
       or expected.order_id is null
       or summary.order_line_count <> expected.order_line_count
       or summary.order_units <> expected.order_units
)

select order_id, disagreement from per_order
union all
select order_id, disagreement from per_count
