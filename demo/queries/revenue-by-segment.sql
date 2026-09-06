-- Revenue and units from the served fact, cut by the conformed segment
-- dimension that the serving product's semantic model declares. The join is
-- the effective-range join a query of the semantic layer would make, so the
-- cut reads the segment that was in force on the day of each line.
select
    dimension.segment_name as segment,
    count(*) as order_lines,
    sum(fact.line_quantity) as units,
    sum(fact.line_revenue) as revenue,
    round(avg(fact.line_revenue), 2) as average_line_revenue
from ${CATALOG}.${SCHEMA}.ecommerce__order_star__fact_order_line as fact
inner join ${CATALOG}.${SCHEMA}.ecommerce__order_star__dim_customer_segment as dimension
    on fact.customer_business_key = dimension.customer_business_key
   and fact.line_order_date >= dimension.effective_from
   and (dimension.effective_to is null or fact.line_order_date < dimension.effective_to)
group by dimension.segment_name
order by revenue desc;
