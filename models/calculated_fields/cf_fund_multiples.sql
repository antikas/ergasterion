{{ config(materialized='table', schema='calculated_fields') }}

-- Declarative fund-performance multiples, built on the
-- calculated_field macro (one formula definition per output, no one-off inline SQL).
-- Each multiple is defined exactly once here. fact_fund_performance.sql joins this
-- output onto fact_fund_performance_base while preserving the mart's public grain.
--
-- moic now divides total value by INVESTED capital, not paid-in. tvpi is
-- (distributions + NAV) / paid-in; moic is (distributions + NAV) / invested capital,
-- where invested capital = paid-in less recallable return-of-capital and fee offsets
-- (fact_fund_performance_base, seeded per fund in fund_invested_capital_basis). Because
-- invested capital <= paid-in, moic >= tvpi, and the two are no longer byte-identical --
-- closing the tautology the panel flagged (moic == tvpi made moic_identity_residual
-- vacuous). Funds with no seeded basis have invested = paid-in, so moic = tvpi for them.
-- cf_fund_metric_reconciliation now asserts the basis-consistency identity between the two.

{% set multiples_spec = {
    'name': 'fund_multiples',
    'base_model': 'fact_fund_performance_base',
    'base_alias': 'performance',
    'grain': [
        'fund_performance_key',
        'fund_key',
        'fund_id',
        'performance_as_of_date'
    ],
    'inputs': {
        'paid_in_usd': 'performance.paid_in_usd',
        'distributions_usd': 'performance.distributions_usd',
        'latest_nav_usd': 'performance.latest_nav_usd',
        'invested_capital_usd': 'performance.invested_capital_usd'
    },
    'outputs': [
        {
            'name': 'tvpi',
            'expression': dpf_safe_divide('distributions_usd + latest_nav_usd', 'paid_in_usd'),
            'output_type': 'numeric'
        },
        {
            'name': 'dpi',
            'expression': dpf_safe_divide('distributions_usd', 'paid_in_usd'),
            'output_type': 'numeric'
        },
        {
            'name': 'rvpi',
            'expression': dpf_safe_divide('latest_nav_usd', 'paid_in_usd'),
            'output_type': 'numeric'
        },
        {
            'name': 'moic',
            'expression': dpf_safe_divide('distributions_usd + latest_nav_usd', 'invested_capital_usd'),
            'output_type': 'numeric'
        }
    ]
} %}

{{ calculated_field(multiples_spec) }}
