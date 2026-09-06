{#-
  This estate's implementations of the named rules the investment products
  reference (rules/investment.yml). Architecture section 3.2: a rule's
  implementations are code, held beside the translator that renders them, never
  in a declaration. A declaration names the rule and its version; these macros
  are what the generated models call.

  Both files compose the engine's own primitives, so every dialect-specific
  spelling stays where the dispatch macros already keep it:
  macros/normalisation.sql for the regular expression replacement and
  macros/entity_resolution_scoring.sql for the sub-scores and the composite.
-#}

{#- The one normalisation both the entity name rule and the manager name rule
    resolve to. Two rules over one macro is deliberate: a rule's inputs bind to
    the columns they name, and a fund lane carries the manager name under a
    different column from its own. -#}
{% macro dpf_investment_normalised_name(source_name) -%}
    {{ normalise_name(source_name) }}
{%- endmacro %}

{#- The probabilistic tier's score for one candidate pair.

    Three sub-scores: how close the two normalised names are, whether the two
    records agree on the categorical attribute the pair was compared on, and
    how close together the two records were reported, decaying to nothing at a
    year apart. The estate compares no monetary value at entity-identity grain,
    so the fourth sub-score the composite takes is passed as absent; the
    composite renormalises the declared weights over whichever sub-scores are
    comparable, so an absent attribute never reads as a rejection.

    The composite is deliberately NOT null-guarded: a pair with nothing
    comparable must reach the emitted evidence as unscored rather than as a low
    score somebody could read as a decision. -#}
{% macro dpf_investment_entity_match_score(normalised_name_a, normalised_name_b, category_a, category_b, reported_on_a, reported_on_b) -%}
    {{ dpf_composite_er_score(
        dpf_string_similarity_score(normalised_name_a, normalised_name_b),
        dpf_categorical_match_score(category_a, category_b),
        dpf_date_proximity_score(reported_on_a, reported_on_b, '365'),
        'cast(null as numeric)',
        '0.4', '0.2', '0.2', '0.2') }}
{%- endmacro %}
