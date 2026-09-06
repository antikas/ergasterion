-- What this proves: the measure the estate's revenue metric is computed from
-- aggregates to a known answer, both in total and cut by the conformed
-- segment dimension the semantic layer declares.
--
-- A metric is a declared aggregation of a measure over a fact, so the thing
-- to prove on the built adapter is that the measure column the metric names
-- carries the values the metric would report. This aggregates it the way the
-- metric declares, sum, over the whole fact and then per segment, joining
-- the segment dimension on its effective range exactly as a query of the
-- semantic layer would.
--
-- The per-segment figures are the arithmetic of the seeded orders: the
-- higher segment is one customer's two orders, the middle segment is one
-- order, and the lower segment is two customers' orders. Their sum is the
-- whole, which is the second thing asserted, so a cut that dropped a line
-- would fail even if each cut looked plausible on its own.
--
-- Singular test: it passes when it returns no rows.
with per_segment as (
    select
        dimension.segment_name,
        sum(fact.line_revenue) as total_line_revenue
    from {{ ref('ecommerce__order_star__fact_order_line') }} as fact
    inner join {{ ref('ecommerce__order_star__dim_customer_segment') }} as dimension
        on fact.customer_business_key = dimension.customer_business_key
       and fact.line_order_date >= dimension.effective_from
       and (dimension.effective_to is null or fact.line_order_date < dimension.effective_to)
    group by dimension.segment_name
),

expected (segment_name, total_line_revenue) as (
    values
        ('gold', 192.46),
        ('silver', 89.99),
        ('bronze', 216.99)
),

per_cut as (
    select
        coalesce(per_segment.segment_name, expected.segment_name) as segment_name,
        'the revenue measure does not total the known answer for this segment' as disagreement
    from per_segment
    full join expected
        on per_segment.segment_name = expected.segment_name
    where per_segment.segment_name is null
       or expected.segment_name is null
       or per_segment.total_line_revenue <> cast(expected.total_line_revenue as decimal(12, 2))
),

whole as (
    select
        cast(null as varchar) as segment_name,
        'the segment cuts do not add up to the measure over the whole fact' as disagreement
    from (
        select
            (select sum(total_line_revenue) from per_segment) as cut_total,
            (select sum(line_revenue) from {{ ref('ecommerce__order_star__fact_order_line') }}) as fact_total
    ) as totals
    where cut_total <> fact_total
)

select segment_name, disagreement from per_cut
union all
select segment_name, disagreement from whole
