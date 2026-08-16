{{ config(materialized='table', schema='calculated_fields') }}

-- Declarative metric-conservation reconciliation, built on the
-- calculated_field macro (one formula definition per output, no one-off inline SQL).
-- Each output is a residual that MUST hold to a tolerance across the 12-metric catalogue;
-- the accompanying expected-value tests in _calculated_fields.yml assert the identities:
--   tvpi_identity_residual   tvpi = dpi + rvpi                 -> residual ~ 0
--   moic_basis_residual      moic * (invested / paid_in) = tvpi -> residual ~ 0
--   called_identity_residual called_pct * committed = paid_in  -> residual ~ 0
--                            (committed reconstructed as paid_in + unfunded_commitment)
--
-- moic_basis_residual REPLACES the old moic_identity_residual (moic - tvpi = 0),
-- which was tautological once moic was computed on the SAME paid-in basis as tvpi -- it
-- asserted 0 = 0 and could never fail (the panel finding). moic now divides total value by
-- INVESTED capital (paid-in less recallable/fee offsets, cf_fund_multiples), so moic and
-- tvpi sit on different bases and the check has real content: both must reconstruct the
-- same total value, i.e. moic * invested = tvpi * paid_in, rearranged to the order-1 form
-- moic * (invested / paid_in) - tvpi ~ 0. It FAILS if moic ever regresses back to the
-- paid-in basis (residual becomes tvpi * (invested/paid_in - 1), non-zero wherever invested
-- < paid_in). Funds with no seeded invested basis have invested = paid_in, so the identity
-- holds trivially (moic = tvpi) for them and non-trivially for the rest.
--
-- net_irr and gross_irr share the same declared cash-flow basis in
-- fact_fund_performance_base.sql, so a gross-minus-net assertion would always be zero
-- and would provide no evidence. Both values pass through for downstream visibility.

{% set reconciliation_spec = {
    'name': 'fund_metric_reconciliation',
    'base_model': 'fact_fund_performance',
    'base_alias': 'performance',
    'grain': [
        'fund_performance_key',
        'fund_key',
        'fund_id',
        'performance_as_of_date'
    ],
    'inputs': {
        'tvpi': 'performance.tvpi',
        'dpi': 'performance.dpi',
        'rvpi': 'performance.rvpi',
        'moic': 'performance.moic',
        'called_pct': 'performance.called_pct',
        'paid_in_usd': 'performance.paid_in_usd',
        'invested_capital_usd': 'performance.invested_capital_usd',
        'unfunded_commitment': 'performance.unfunded_commitment',
        'net_irr': 'performance.net_irr',
        'gross_irr': 'performance.gross_irr'
    },
    'pass_through': [
        'tvpi',
        'dpi',
        'rvpi',
        'moic',
        'called_pct',
        'paid_in_usd',
        'invested_capital_usd',
        'unfunded_commitment',
        'net_irr',
        'gross_irr'
    ],
    'outputs': [
        {
            'name': 'tvpi_identity_residual',
            'expression': 'case when tvpi is null then null else tvpi - (coalesce(dpi, cast(0 as numeric)) + coalesce(rvpi, cast(0 as numeric))) end',
            'output_type': 'numeric'
        },
        {
            'name': 'moic_basis_residual',
            'expression': 'case when moic is null or tvpi is null then null else moic * (' ~ dpf_safe_divide('invested_capital_usd', 'paid_in_usd') ~ ') - tvpi end',
            'output_type': 'numeric'
        },
        {
            'name': 'called_identity_residual',
            'expression': 'case when called_pct is null then null else (called_pct * (paid_in_usd + unfunded_commitment)) - paid_in_usd end',
            'output_type': 'numeric'
        }
    ]
} %}

{{ calculated_field(reconciliation_spec) }}
