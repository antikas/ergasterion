-- Every order the estate publishes, its summarised lines, and the total the
-- source system stated for the same order. The consolidation reconciles to
-- the header it never recomputed, so the two revenue columns agree row by row.
select
    summary.order_id,
    summary.order_line_count as lines,
    summary.order_units as units,
    summary.order_revenue as summarised_revenue,
    header.order_total as stated_order_total,
    case when summary.order_revenue = header.order_total then 'reconciled' else 'DIVERGED' end
        as reconciliation
from ${CATALOG}.${SCHEMA}.ecommerce__order_summary as summary
inner join ${CATALOG}.${SCHEMA}.ecommerce__order as header
    on summary.order_id = header.order_id
order by summary.order_id;
