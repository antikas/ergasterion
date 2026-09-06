-- What this proves: the served aggregate is at the grain it declares, and its
-- measures are the ones that grain admits.
--
-- Capital called and capital distributed are running totals in the delivered
-- data, so the figure for a year is the highest the year reached, not the sum of
-- its quarters. One fund is valued four times in the year and reaches a closing
-- value the last of those four states.
--
--   The grain holds: one row per fund and calendar year.
--
--   The counts and the closing figures are the ones the delivered valuations
--   state.
--
--   The valuation dates the fund was valued on are the ones the summary counted,
--   read back from the product it aggregates.
--
-- Singular test: it passes when it returns no rows.
with expected (external_id, calendar_year, valuation_count, closing_nav, called_capital) as (
    values
        ('OPENIM-FUND-APEX-II', 2025, 4, 136100000.00, 44500000.00),
        ('OPENIM-FUND-NORTHSTAR-IV', 2025, 3, 176500000.00, 99200000.00),
        ('OPENIM-FUND-ORION-I', 2025, 1, 83500000.00, 3500000.00)
),

summary as (
    select fund_external_id, calendar_year, fund_valuation_count,
           fund_closing_nav_usd, fund_called_capital_usd, fund_business_key
    from {{ ref('investment__fund_performance_summary') }}
),

figures as (
    select
        'the summary does not state the figures the valuations state' as disagreement,
        expected.external_id as subject
    from expected
    left join summary
        on summary.fund_external_id = expected.external_id
       and summary.calendar_year = expected.calendar_year
    where summary.fund_external_id is null
       or summary.fund_valuation_count <> expected.valuation_count
       or summary.fund_closing_nav_usd <> cast(expected.closing_nav as decimal(18, 2))
       or summary.fund_called_capital_usd <> cast(expected.called_capital as decimal(18, 2))
),

grain as (
    select
        'the summary carries more than one row for a fund and year' as disagreement,
        fund_business_key as subject
    from summary
    group by fund_business_key, calendar_year
    having count(*) > 1
),

counted as (
    select
        'the summary counted a different number of valuation dates' as disagreement,
        summary.fund_business_key as subject
    from summary
    inner join (
        select fund_business_key, calendar_year, count(*) as valued_dates
        from {{ ref('investment__fund_performance') }}
        where source_nav_usd is not null
        group by 1, 2
    ) as source_counts
        on source_counts.fund_business_key = summary.fund_business_key
       and source_counts.calendar_year = summary.calendar_year
    where source_counts.valued_dates <> summary.fund_valuation_count
)

select disagreement, subject from figures
union all
select disagreement, subject from grain
union all
select disagreement, subject from counted
