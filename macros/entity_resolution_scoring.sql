{#-
  Probabilistic entity-resolution scoring primitives.

  Composes the adapter-dispatch primitives in cross_db.sql (dpf_edit_distance,
  dpf_date_diff_days, dpf_safe_divide) into the four named sub-scores the tier-2
  composite requires -- string / sector / date / value -- plus the weighted
  composite itself. Mirrors the relationship normalisation.sql has to
  cross_db.sql: this file carries no dialect-specific literal, only composition.

  Every sub-score is normalised to the closed interval [0, 1], or NULL when
  either input is NULL -- a NULL sub-score means "this attribute is not
  comparable for this pair", not "no match". dpf_composite_er_score renormalises
  the declared weights across whichever sub-scores are actually non-NULL for a
  given pair, so an entity type that does not carry one of the four comparison
  attributes (for example, no monetary value at the entity-identity grain)
  degrades gracefully instead of forcing a meaningless 0 into the average.
-#}

{#- Normalised string similarity via edit distance, scaled by the longer of the
    two strings' length so the score is length-invariant. NULL if either side
    is NULL. Floors at 0.0 (edit distance can exceed length for very different
    strings once normalisation, substitution etc. are involved). -#}
{% macro dpf_string_similarity_score(expr_a, expr_b) -%}
    case
        when {{ expr_a }} is null or {{ expr_b }} is null then cast(null as numeric)
        else greatest(
            0.0,
            1.0 - ({{ dpf_safe_divide(dpf_edit_distance(expr_a, expr_b) ~ ' * 1.0', 'greatest(length(' ~ expr_a ~ '), length(' ~ expr_b ~ '), 1)') }})
        )
    end
{%- endmacro %}

{#- Binary categorical match: 1.0 when equal (case/whitespace-insensitive),
    0.0 when both present and different, NULL when either side is NULL. -#}
{% macro dpf_categorical_match_score(expr_a, expr_b) -%}
    case
        when {{ expr_a }} is null or {{ expr_b }} is null then cast(null as numeric)
        when upper(trim(cast({{ expr_a }} as string))) = upper(trim(cast({{ expr_b }} as string))) then 1.0
        else 0.0
    end
{%- endmacro %}

{#- Date proximity: 1.0 at zero days apart, linearly decaying to 0.0 at
    `tolerance_days` apart (an already-rendered SQL expression -- a config-table
    column reference, not a hardcoded literal), floored at 0.0 beyond that.
    NULL if either side is NULL. -#}
{% macro dpf_date_proximity_score(expr_a, expr_b, tolerance_days) -%}
    case
        when {{ expr_a }} is null or {{ expr_b }} is null then cast(null as numeric)
        else greatest(
            0.0,
            1.0 - ({{ dpf_safe_divide('abs(' ~ dpf_date_diff_days(expr_a, expr_b) ~ ') * 1.0', tolerance_days) }})
        )
    end
{%- endmacro %}

{#- Value/amount proximity: 1.0 when equal, linearly decaying to 0.0 once the
    absolute difference reaches `tolerance_pct` (already-rendered SQL expression,
    a config-table column reference) of the larger magnitude. NULL if either
    side is NULL. Implemented as a first-
    class sub-score so the composite's weight_value column is a real, wired
    comparator the moment an entity type gains a comparable value attribute. -#}
{% macro dpf_value_proximity_score(expr_a, expr_b, tolerance_pct) -%}
    case
        when {{ expr_a }} is null or {{ expr_b }} is null then cast(null as numeric)
        when {{ expr_a }} = 0 and {{ expr_b }} = 0 then 1.0
        else greatest(
            0.0,
            1.0 - ({{ dpf_safe_divide('abs(' ~ expr_a ~ ' - ' ~ expr_b ~ ')', 'greatest(abs(' ~ expr_a ~ '), abs(' ~ expr_b ~ '), 1e-9) * ' ~ tolerance_pct) }})
        )
    end
{%- endmacro %}

{#- Composite score = weighted average of the four sub-scores, per the spec
    weights (string 0.4 + sector 0.2 + date 0.2 + value 0.2), RENORMALISED
    across whichever sub-scores are non-NULL for the row. All six inputs are
    already-rendered SQL expressions: the four score args reference the
    sub-score columns computed above; the four weight args reference the
    scoring-config columns (seeds/entity_resolution_scoring_config.csv) joined
    into the same query -- weights are data, not compile-time literals, so the
    composite is configurable without a code change. -#}
{% macro dpf_composite_er_score(string_score, sector_score, date_score, value_score, weight_string, weight_sector, weight_date, weight_value) -%}
    {%- set weighted_sum -%}
        (coalesce({{ string_score }}, 0) * {{ weight_string }}
         + coalesce({{ sector_score }}, 0) * {{ weight_sector }}
         + coalesce({{ date_score }}, 0) * {{ weight_date }}
         + coalesce({{ value_score }}, 0) * {{ weight_value }})
    {%- endset -%}
    {%- set weight_present_sum -%}
        (case when {{ string_score }} is not null then {{ weight_string }} else 0 end
         + case when {{ sector_score }} is not null then {{ weight_sector }} else 0 end
         + case when {{ date_score }} is not null then {{ weight_date }} else 0 end
         + case when {{ value_score }} is not null then {{ weight_value }} else 0 end)
    {%- endset -%}
    {{ dpf_safe_divide(weighted_sum, weight_present_sum) }}
{%- endmacro %}
