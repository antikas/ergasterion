{#-
  The Data Publish pattern's one dialect-divergent primitive.

  Architecture section 4 gives Data Publish three emitted artefacts: atomic
  publication, a current pointer and an SLA record. Publication is the
  materialisation the generated model configures, and the pointer and the record
  are generated relations whose SQL is ordinary and portable. The one place the
  two declared adapters genuinely differ is how they spell the publication
  instant, so that is the one thing that lives behind dispatch: BigQuery writes
  the ANSI function with parentheses, DuckDB without.
-#}

{% macro dpf_publish_timestamp() -%}
    {{ return(adapter.dispatch('dpf_publish_timestamp', 'ergasterion')()) }}
{%- endmacro %}

{% macro default__dpf_publish_timestamp() -%}
    current_timestamp()
{%- endmacro %}

{% macro duckdb__dpf_publish_timestamp() -%}
    current_timestamp
{%- endmacro %}


{#- The incremental strategy the running adapter republishes a keyed relation with.
    `strategies` maps adapter name to strategy, written into the generated model
    from each declared adapter's own conventions.yml: the two declared adapters
    accept different tokens (DuckDB delete+insert, BigQuery merge) and neither
    accepts the other's, so one hard-coded token would not compile on both. An
    adapter the mapping does not carry fails the compile rather than falling back
    to a strategy nobody declared. -#}
{% macro dpf_incremental_strategy(strategies) -%}
    {%- set strategy = strategies.get(target.type) -%}
    {%- if strategy is none -%}
        {{ exceptions.raise_compiler_error("dpf_incremental_strategy: no strategy is declared for adapter '" ~ target.type ~ "'") }}
    {%- endif -%}
    {{ return(strategy) }}
{%- endmacro %}
