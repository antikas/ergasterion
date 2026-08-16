{# DuckDB dispatch implementations for the AutomateDV 0.11.5 leaf macros used by this project. #}

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

{%- macro duckdb__sat(src_pk, src_hashdiff, src_payload, src_extra_columns, src_eff, src_ldts, src_source, source_model) -%}
    {#
      Local DuckDB builds replay the complete staged history on every run. Keep
      AutomateDV's satellite SQL authoritative, then suppress only semantic
      versions already stored by business key, hashdiff, and effective time.
    #}
    {%- set generated_satellite_sql = automate_dv.default__sat(
        src_pk=src_pk,
        src_hashdiff=src_hashdiff,
        src_payload=src_payload,
        src_extra_columns=src_extra_columns,
        src_eff=src_eff,
        src_ldts=src_ldts,
        src_source=src_source,
        source_model=source_model
    ) -%}

    {%- if automate_dv.is_any_incremental() -%}
        {%- set pk_columns = automate_dv.expand_column_list(columns=[src_pk]) -%}
        {%- set effective_columns = automate_dv.expand_column_list(columns=[src_eff]) -%}
        SELECT candidate.*
        FROM (
            {{ generated_satellite_sql }}
        ) AS candidate
        WHERE NOT EXISTS (
            SELECT 1
            FROM {{ this }} AS existing
            WHERE
                {%- for column in pk_columns %}
                {{ automate_dv.prefix([column], 'existing', alias_target='target') }} =
                    {{ automate_dv.prefix([column], 'candidate', alias_target='target') }}
                    AND
                {%- endfor %}
                {{ automate_dv.prefix([src_hashdiff], 'existing', alias_target='target') }} IS NOT DISTINCT FROM
                    {{ automate_dv.prefix([src_hashdiff], 'candidate', alias_target='target') }}
                {%- for column in effective_columns %}
                    AND {{ automate_dv.prefix([column], 'existing', alias_target='target') }} IS NOT DISTINCT FROM
                        {{ automate_dv.prefix([column], 'candidate', alias_target='target') }}
                {%- endfor %}
        )
    {%- else -%}
        {{ generated_satellite_sql }}
    {%- endif -%}
{%- endmacro %}
