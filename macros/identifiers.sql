{#-
  Identifier addressing: the two macros that let a generated model store and
  read a name whose exact spelling the estate does not own.

  A declaration keeps plain lower-case logical names. Where an interface has
  to reproduce an externally required physical name, the declaration states
  that name beside the logical one and the renderer writes it through
  dpf_quote(...). The renderer never writes a quote character of its own:
  each adapter's dbt implementation owns how an identifier is wrapped so its
  exact case survives, and adapter.quote(...) is that implementation.

  generate_schema_name is dbt's own hook for resolving a model's schema, and
  a project may define it only once, so this one is deliberately narrow. It
  changes the answer for exactly one kind of node: a relation whose product
  declared a stored schema, which the renderer marks with the meta flag
  below. Every other node -- a seed under `+schema: raw`, a model a project
  configures a schema for itself, a package's model -- falls straight through
  to dbt's own default__generate_schema_name and keeps resolving
  <target.schema>_<custom schema> exactly as it did. Without the flag the
  default would prefix a declared physical schema, storing the relation under
  a name nothing outside the project can address.
-#}

{#- The model config key the renderer marks a declared physical schema with.
    Read by generate_schema_name below and by nothing else. -#}
{% macro dpf_physical_schema_flag() -%}dpf.physical_schema{%- endmacro %}


{#- A declared physical identifier, wrapped the way the running adapter
    wraps one. -#}
{% macro dpf_quote(name) -%}
    {{ adapter.quote(name) }}
{%- endmacro %}


{#- The schema one node is built in: the exact schema its product declared,
    for a relation the renderer marked as carrying one, and dbt's own answer
    for every other node. -#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set meta = (node.config.get('meta') if node is not none and node.config is not none else none) or {} -%}
    {%- if custom_schema_name is not none and meta.get(dpf_physical_schema_flag(), false) -%}
        {{ custom_schema_name | trim }}
    {%- else -%}
        {{ dbt.default__generate_schema_name(custom_schema_name, node) }}
    {%- endif -%}
{%- endmacro %}
