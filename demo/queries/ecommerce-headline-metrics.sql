select
    date_trunc('month', order_date) as order_month,
    customer_segment_at_order_date as segment,
    count(*) as order_count,
    sum(order_total) as revenue_by_segment,
    avg(order_total) as average_order_value
from ${CATALOG}.${MARTS_SCHEMA}.int_order_header
where customer_segment_at_order_date is not null
group by 1, 2
order by 1, 2;
