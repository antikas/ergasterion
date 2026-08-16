-- Components-sum-to-delta identity test (structural criterion).
--
-- The decomposition contract (fact_committed_capital_decomposition) is that the
-- named explained components plus the unexplained residual account for the FULL
-- cross-source gap and nothing more:
--
--   component_unit_scale
--     + component_fx_currency
--     + component_as_of_timing
--     + component_scope_universe
--     + component_methodology
--     + unexplained_residual
--   = total_delta
--
-- That identity is the audit mechanism the investment-data-patterns value bridge
-- rests on ("these must sum to the total change; that identity is the audit
-- mechanism"). It guards two failure modes at once: a future edit that adds a
-- real explained component without netting it out of the residual, and any
-- arithmetic drift in how residual is derived.
--
-- Keyed to STRUCTURE, not to numeric values: it asserts the identity holds for
-- every row regardless of which committed-capital values the seeds contain.
-- A tolerance of 0.01 absorbs warehouse numeric rounding without letting
-- a genuine mis-decomposition through.
--
-- Singular test: PASSES when no row violates the identity; returns the offending
-- rows (with the size of the identity error) otherwise.
select
    committed_capital_decomposition_key,
    total_delta,
    (
        component_unit_scale
        + component_fx_currency
        + component_as_of_timing
        + component_scope_universe
        + component_methodology
        + unexplained_residual
    ) as component_sum,
    abs(
        total_delta
        - (
            component_unit_scale
            + component_fx_currency
            + component_as_of_timing
            + component_scope_universe
            + component_methodology
            + unexplained_residual
        )
    ) as identity_error
from {{ ref('fact_committed_capital_decomposition') }}
where abs(
        total_delta
        - (
            component_unit_scale
            + component_fx_currency
            + component_as_of_timing
            + component_scope_universe
            + component_methodology
            + unexplained_residual
        )
    ) > 0.01
