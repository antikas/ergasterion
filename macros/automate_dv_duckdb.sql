{#-
  DuckDB dispatch arms for the AutomateDV leaf macros the installed package resolves
  through adapter.dispatch. The package is a declared dbt dependency (packages.yml),
  and its own macros fail closed on DuckDB without these arms, so they belong to the
  estate that declares the package rather than to any product route.
-#}

{%- macro duckdb__get_escape_characters() -%}
    {%- do return(('"', '"')) -%}
{%- endmacro -%}

{%- macro duckdb__cast_date(column_str, as_string=false, alias=none) -%}
    {%- if as_string -%}
        CAST('{{ column_str }}' AS DATE)
    {%- else -%}
        CAST({{ column_str }} AS DATE)
    {%- endif -%}
    {%- if alias %} AS {{ alias }}{%- endif -%}
{%- endmacro -%}

{%- macro duckdb__cast_datetime(column_str, as_string=false, alias=none, date_type=none) -%}
    CAST({{ column_str }} AS TIMESTAMP)
    {%- if alias %} AS {{ alias }}{%- endif -%}
{%- endmacro -%}

{%- macro duckdb__type_binary(for_dbt_compare=false) -%}
    VARCHAR
{%- endmacro -%}

{%- macro duckdb__type_timestamp() -%}
    TIMESTAMP
{%- endmacro -%}

{%- macro duckdb__cast_binary(column_str, alias=none, quote=true) -%}
    {%- if quote -%}
        CAST('{{ column_str }}' AS {{ automate_dv.type_binary() }})
    {%- else -%}
        CAST({{ column_str }} AS {{ automate_dv.type_binary() }})
    {%- endif -%}
    {%- if alias %} AS {{ alias }}{%- endif -%}
{%- endmacro -%}

{% macro duckdb__hash_alg_md5() -%}
    {%- do return(automate_dv.cast_binary('UPPER(MD5([HASH_STRING_PLACEHOLDER]))', quote=false)) -%}
{%- endmacro %}

{% macro duckdb__hash_alg_sha256() -%}
    {%- do return(automate_dv.cast_binary('UPPER(SHA256([HASH_STRING_PLACEHOLDER]))', quote=false)) -%}
{%- endmacro %}

{% macro duckdb__hash_alg_sha1() -%}
    {%- do return(automate_dv.cast_binary('UPPER(SHA1([HASH_STRING_PLACEHOLDER]))', quote=false)) -%}
{%- endmacro %}
