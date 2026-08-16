select
    f.fund_name,
    f.strategy,
    f.vintage_year,
    h.tvpi,
    h.dpi,
    h.rvpi,
    h.moic,
    h.hurdle_type,
    h.hurdle_rate,
    h.hurdle_return_metric,
    h.hurdle_cleared,
    h.excess_over_hurdle
from ${CATALOG}.${CALC_SCHEMA}.cf_fund_hurdle h
join ${CATALOG}.${MARTS_SCHEMA}.dim_fund f
    on f.fund_key = h.fund_key
order by f.fund_name;
