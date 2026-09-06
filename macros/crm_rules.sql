{#-
  The e-commerce estate's implementations of the named rules its
  declarations reference (rules/ecommerce.yml). Architecture section 3.2:
  a rule's implementations are code, held beside the translator that
  renders them, never in a declaration. A declaration names the rule and
  its version; these macros are what the generated models call.

  dpf_crm_email_domain is adapter-specific: the two declared adapters
  spell "the part after the at sign" differently, so this macro dispatches
  and each arm carries one adapter's spelling. That dispatch is the only
  place either spelling appears.

  dpf_crm_order_value_band is not: one CASE expression is valid on both
  declared adapters, so the rule has one implementation and no dispatch.
-#}

{% macro dpf_crm_email_domain(email) -%}
    {{ return(adapter.dispatch('dpf_crm_email_domain', 'ergasterion')(email)) }}
{%- endmacro %}

{% macro default__dpf_crm_email_domain(email) -%}
    nullif(split({{ email }}, '@')[safe_ordinal(2)], '')
{%- endmacro %}

{% macro duckdb__dpf_crm_email_domain(email) -%}
    nullif(split_part({{ email }}, '@', 2), '')
{%- endmacro %}

{% macro dpf_crm_order_value_band(order_total) -%}
    case
        when {{ order_total }} >= 150 then 'large'
        when {{ order_total }} >= 50 then 'medium'
        else 'small'
    end
{%- endmacro %}
