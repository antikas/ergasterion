{#-
  Metric-governance predicates. .

  SSOT for the "only ER-confirmed golden records feed metric calculation" gate. The
  entity-resolution pipeline (res_fund) scores every fund's identity resolution on a
  0.00-1.00 confidence scale: 1.00 = deterministic exact-id match, 0.95 = deterministic
  normalised-name match, and the probabilistic tier-2 band auto-merges at or above the
  configured auto threshold (threshold_auto = 0.85 in
  seeds/entity_resolution_scoring_config.csv). Anything below that is unconfirmed
  (pending probabilistic review or left in the human review queue) and MUST NOT feed
  performance metrics -- an unconfirmed fund identity means cash flows and valuations may
  belong to the wrong (or a split) fund, so its IRR/TVPI/PME would be computed over the
  wrong stream.

  Defined once here and referenced by both metric marts (fact_fund_performance and
  fact_benchmark_comparison) so the confirmed threshold has a single home; changing the
  policy is a one-file edit, never a grep-and-replace across marts.

  `confidence_col` is an already-qualified column expression (e.g.
  `fund.fund_resolution_confidence`). Renders a boolean predicate.
-#}

{% macro dpf_is_er_confirmed(confidence_col) -%}
    {%- set dpf_er_confirmed_threshold = 0.85 -%}
    ({{ confidence_col }} is not null and {{ confidence_col }} >= {{ dpf_er_confirmed_threshold }})
{%- endmacro %}
