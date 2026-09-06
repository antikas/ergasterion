{#-
  This estate's implementations of its two named rules. Architecture section
  3.2: a rule's implementations are code, held beside the translator that
  renders them, never in a declaration. The declaration names the rule and
  its version; these macros are what the generated model calls.
-#}

{% macro ticket_priority_v1(open_minutes) -%}
    case
        when {{ open_minutes }} >= 480 then 'urgent'
        when {{ open_minutes }} >= 60 then 'high'
        else 'normal'
    end
{%- endmacro %}

{% macro ticket_is_open_v1(ticket_state) -%}
    {{ ticket_state }} in ('new', 'active')
{%- endmacro %}
