-- What this proves: the neutral type each declaration states reaches the built
-- relation on the reference adapter, for a vault hub, a vault satellite and the
-- served relation.
--
-- A declaration says decimal with a precision and a scale, integer, date or
-- string, and the adapter's own conventions say what each of those is on the
-- platform. Nothing between the two shows up in a row count or a sum: a capital
-- figure built as a floating point number would still add up here and would stop
-- adding up somewhere else later. So the built types are read from the adapter's
-- own catalogue.
--
-- Singular test: it passes when it returns no rows.
with expected (relation_name, column_name, data_type) as (
    values
        ('{{ ref('investment__fund_vault__hub_fund').identifier }}', 'fund_hk', 'VARCHAR'),
        ('{{ ref('investment__fund_vault__hub_fund').identifier }}', 'fund_business_key', 'VARCHAR'),
        ('{{ ref('investment__fund_vault__hub_fund').identifier }}', 'load_datetime', 'TIMESTAMP'),
        ('{{ ref('investment__fund_vault__sat_fund_observed').identifier }}', 'fund_vintage_year', 'INTEGER'),
        ('{{ ref('investment__fund_vault__sat_fund_observed').identifier }}', 'fund_committed_capital_usd', 'DECIMAL(18,2)'),
        ('{{ ref('investment__fund_vault__sat_fund_observed').identifier }}', 'effective_from', 'DATE'),
        ('{{ ref('investment__fund_performance_summary').identifier }}', 'calendar_year', 'INTEGER'),
        ('{{ ref('investment__fund_performance_summary').identifier }}', 'fund_valuation_count', 'INTEGER'),
        ('{{ ref('investment__fund_performance_summary').identifier }}', 'fund_closing_nav_usd', 'DECIMAL(18,2)'),
        ('{{ ref('investment__fund_performance_summary').identifier }}', 'fund_invested_capital_usd', 'DECIMAL(18,2)')
),

built as (
    select table_name, column_name, upper(data_type) as data_type
    from information_schema.columns
)

select
    expected.relation_name as disagreement,
    expected.column_name || ' is not the declared type' as subject
from expected
left join built
    on built.table_name = expected.relation_name
   and built.column_name = expected.column_name
where built.column_name is null
   or built.data_type <> expected.data_type
