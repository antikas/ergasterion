{#-
  This estate's implementation of the named rule order_band_v1. Architecture
  section 3.2: a rule's implementations are code, held beside the translator
  that renders them, never in a declaration. The declaration names the rule
  and its version; this macro is what the generated model calls.
-#}

{% macro order_band_v1(order_total) -%}
    case
        when {{ order_total }} >= 1000 then 'large'
        when {{ order_total }} >= 100 then 'medium'
        else 'small'
    end
{%- endmacro %}
