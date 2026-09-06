-- What this proves: the configuration the estate declares is the configuration
-- the figures are computed from.
--
-- Two things are declared per fund: the part of the called capital that does not
-- count as invested, and the return the fund has to clear. One fund carries an
-- override of its own; the rest take the hurdle their strategy carries.
--
--   The invested capital of that fund is its called capital less the recallable
--   distributions and the fee offset the estate declares for it.
--
--   Its hurdle is the override, not the hurdle its strategy would give it, and
--   the product says which of the two it took.
--
--   The multiple is the total value over the invested capital, and it is left
--   unstated wherever invested capital is not positive.
--
-- Singular test: it passes when it returns no rows.
with performance as (
    select
        fund_external_id,
        source_valued_on,
        source_called_capital_usd,
        source_nav_usd,
        source_distributed_capital_usd,
        fund_recallable_distributions_usd,
        fund_fee_offset_usd,
        fund_invested_capital_usd,
        fund_hurdle_rate,
        fund_total_value_multiple,
        fund_clears_hurdle
    from {{ ref('investment__fund_performance') }}
),

basis as (
    select
        'the declared basis was not taken out of the called capital' as disagreement,
        fund_external_id || ' at ' || cast(source_valued_on as varchar) as subject
    from performance
    where fund_invested_capital_usd
        <> source_called_capital_usd - fund_recallable_distributions_usd - fund_fee_offset_usd
),

declared_amounts as (
    select
        'the declared basis for that fund is not the one the estate states' as disagreement,
        fund_external_id as subject
    from performance
    where fund_external_id = 'OPENIM-FUND-APEX-II'
      and (fund_recallable_distributions_usd <> 700000.00 or fund_fee_offset_usd <> 200000.00)
),

override as (
    select
        'the fund override hurdle was not the one applied' as disagreement,
        cast(fund_hurdle_rate as varchar) as subject
    from performance
    where fund_external_id = 'OPENIM-FUND-APEX-II'
      and fund_hurdle_rate <> 1.3000
),

multiple as (
    select
        'the multiple is not the total value over the invested capital' as disagreement,
        fund_external_id || ' at ' || cast(source_valued_on as varchar) as subject
    from performance
    where fund_invested_capital_usd > 0
      and abs(
            fund_total_value_multiple
            - (source_nav_usd + source_distributed_capital_usd) / fund_invested_capital_usd
          ) > 0.0001
),

unstated as (
    select
        'a multiple was computed over an invested capital that is not positive' as disagreement,
        fund_external_id as subject
    from performance
    where fund_invested_capital_usd <= 0
      and fund_total_value_multiple is not null
),

cleared as (
    select
        'the hurdle verdict does not follow the multiple and the hurdle' as disagreement,
        fund_external_id || ' at ' || cast(source_valued_on as varchar) as subject
    from performance
    where fund_hurdle_rate is not null
      and fund_invested_capital_usd > 0
      and fund_clears_hurdle is distinct from (fund_total_value_multiple >= fund_hurdle_rate)
)

select disagreement, subject from basis
union all
select disagreement, subject from declared_amounts
union all
select disagreement, subject from override
union all
select disagreement, subject from multiple
union all
select disagreement, subject from unstated
union all
select disagreement, subject from cleared
