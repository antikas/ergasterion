{#-
  Normalisation helpers. All dialect-specific SQL (regexp_* family, to_hex+md5) is
  routed through the cross-db adapter-dispatch macros in cross_db.sql so these
  render correctly on BigQuery, Snowflake, and DuckDB. Patterns are plain quoted
  literals -- never BigQuery r'...' raw strings.
-#}

{% macro blank_to_null(expression) -%}
    nullif(trim(cast({{ expression }} as string)), '')
{%- endmacro %}

{% macro normalise_name(expression) -%}
    nullif({{ dpf_regexp_replace('upper(trim(cast(' ~ expression ~ ' as string)))', "'[^A-Z0-9]'", "''") }}, '')
{%- endmacro %}

{#- Prefix-aware identifier normalisation (generalised from the source-named
    original. Canonicalises an identifier that may carry a known
    alpha `prefix` immediately followed by digits (e.g. 'CPS12345' -> 'CPS-12345'),
    stripping non-alphanumerics first; anything not matching the prefix pattern is
    upper/trim-normalised. The prefix is a PARAMETER, not baked in, so the same
    engine macro serves any source whose ids carry a stable prefix -- a consumer
    copying macros/ inherits the mechanism, never this estate's worked-domain
    prefix. Passing prefix='CPS' renders SQL byte-identical to the pre-generalisation
    macro. -#}
{% macro normalise_prefixed_id(expression, prefix) -%}
    {%- set normalised = dpf_regexp_replace('cast(' ~ expression ~ ' as string)', "'[^A-Z0-9]'", "''") -%}
    case
        when {{ expression }} is null or trim(cast({{ expression }} as string)) = '' then null
        when {{ dpf_regexp_contains('upper(' ~ normalised ~ ')', "'^" ~ prefix ~ "[0-9]+'") }} then
            concat(
                '{{ prefix }}-',
                {{ dpf_regexp_extract('upper(' ~ normalised ~ ')', "'^" ~ prefix ~ "([0-9]+)'") }}
            )
        else upper(trim(cast({{ expression }} as string)))
    end
{%- endmacro %}

{% macro stable_golden_key(entity_type, key_expression) -%}
    {{ dpf_hash_hex("concat('" ~ entity_type ~ "', '|', coalesce(cast(" ~ key_expression ~ " as string), ''))") }}
{%- endmacro %}
