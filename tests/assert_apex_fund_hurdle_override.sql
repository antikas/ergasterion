-- Expected-value fund-hurdle-override join test.
--
-- cf_fund_hurdle's fund_hurdle_config join used to compare performance.fund_id
-- (the MD5-style survivorship-collapsed golden_fund_key) against
-- seeds/hurdle_config.csv's raw business id -- the two never matched, so the
-- 'fund' scope override always fell through to 'strategy'. The join now keys
-- on performance.shared_external_id (the business id, threaded through
-- canonical_fund -> dim_fund -> fact_fund_performance), so Apex Growth II's
-- fund-level override (hurdle_config_scope='fund', hurdle_rate=1.30 from
-- seeds/hurdle_config.csv row 7) must resolve, not the growth-strategy
-- default (hurdle_config_scope='strategy', hurdle_rate=1.25).
--
-- Singular test: PASSES when it returns zero rows. It returns a row if
-- Apex Growth II's cf_fund_hurdle row does not resolve to the fund-scope
-- override.
select
    hurdle.shared_external_id,
    hurdle.hurdle_config_scope,
    hurdle.hurdle_rate
from {{ ref('cf_fund_hurdle') }} as hurdle
where hurdle.shared_external_id = 'OPENIM-FUND-APEX-II'
  and (
        hurdle.hurdle_config_scope is distinct from 'fund'
     or hurdle.hurdle_rate is distinct from 1.30
  )
