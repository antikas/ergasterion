{#-
  This estate's implementation of the named rule customer_match_score_v1.
  Architecture section 3.2: a rule's implementations are code, held beside
  the translator that renders them, never in a declaration. The declaration
  names the rule and its version; this macro is what the generated model
  calls.

  It composes the engine's own scoring primitives
  (macros/entity_resolution_scoring.sql), so the dialect divergence stays
  where the dispatch macros already keep it. The composite is deliberately
  NOT null-guarded: a sub-score is null when the attribute is not comparable
  for that pair, and a pair with nothing comparable must reach the emitted
  evidence as unscored rather than as a low score somebody could read as a
  rejection.
-#}

{% macro customer_match_score_v1(full_name_a, full_name_b, postcode_a, postcode_b) -%}
    (
        0.7 * ({{ dpf_string_similarity_score(full_name_a, full_name_b) }})
        + 0.3 * ({{ dpf_categorical_match_score(postcode_a, postcode_b) }})
    )
{%- endmacro %}
