{#-
  Source-delivery integrity, schedule-timeliness and optional maximum-age
  freshness. Adapter-specific SQL lives here (the sanctioned dialect home);
  generated sources call these macros and never override dbt's stock
  collect_freshness / collect_freshness_custom_sql collectors.
-#}

{% macro dpf_json_text(expr, path) -%}
    {{ return(adapter.dispatch('dpf_json_text', 'ergasterion')(expr, path)) }}
{%- endmacro %}

{% macro default__dpf_json_text(expr, path) -%}
    json_value({{ expr }}, '$.{{ path }}')
{%- endmacro %}

{% macro snowflake__dpf_json_text(expr, path) -%}
    to_varchar(get_path(try_parse_json({{ expr }}), '{{ path }}'))
{%- endmacro %}

{% macro duckdb__dpf_json_text(expr, path) -%}
    json_extract_string(cast({{ expr }} as json), '$.{{ path }}')
{%- endmacro %}


{% macro dpf_parse_utc_instant(expr) -%}
    {{ return(adapter.dispatch('dpf_parse_utc_instant', 'ergasterion')(expr)) }}
{%- endmacro %}

{% macro default__dpf_parse_utc_instant(expr) -%}
    timestamp({{ expr }})
{%- endmacro %}

{% macro snowflake__dpf_parse_utc_instant(expr) -%}
    to_timestamp_tz({{ expr }})
{%- endmacro %}

{% macro duckdb__dpf_parse_utc_instant(expr) -%}
    cast({{ expr }} as timestamptz)
{%- endmacro %}


{% macro dpf_age_seconds(earlier_expr) -%}
    {{ return(adapter.dispatch('dpf_age_seconds', 'ergasterion')(earlier_expr)) }}
{%- endmacro %}

{% macro default__dpf_age_seconds(earlier_expr) -%}
    timestamp_diff(current_timestamp(), {{ earlier_expr }}, second)
{%- endmacro %}

{% macro snowflake__dpf_age_seconds(earlier_expr) -%}
    datediff('second', {{ earlier_expr }}, current_timestamp())
{%- endmacro %}

{% macro duckdb__dpf_age_seconds(earlier_expr) -%}
    date_diff('second', {{ earlier_expr }}, current_timestamp)
{%- endmacro %}


{% macro dpf_poison_timestamp() -%}
    {{ return(adapter.dispatch('dpf_poison_timestamp', 'ergasterion')()) }}
{%- endmacro %}

{% macro default__dpf_poison_timestamp() -%}
    timestamp('1970-01-01 00:00:00+00')
{%- endmacro %}

{% macro snowflake__dpf_poison_timestamp() -%}
    to_timestamp_tz('1970-01-01T00:00:00Z')
{%- endmacro %}

{% macro duckdb__dpf_poison_timestamp() -%}
    cast('1970-01-01 00:00:00+00' as timestamptz)
{%- endmacro %}


{% macro dpf_compact_identity_key(identity) -%}
{"estate_namespace":"{{ identity['estate_namespace'] }}","source":"{{ identity['source'] }}","table":"{{ identity['table'] }}"}
{%- endmacro %}


{% macro dpf_active_source_identity(identity_key=none) -%}
    {%- if identity_key is not none and identity_key | length > 0 -%}
        {%- set ident = fromjson(identity_key) -%}
        {%- set ns = namespace(meta=none) -%}
        {%- for src in graph.sources.values() -%}
            {%- if src.source_name == ident.source and src.name == ident.table -%}
                {%- set ns.meta = src.meta.get('dpf.identity') -%}
            {%- endif -%}
        {%- endfor -%}
        {%- if ns.meta is none and model.meta is defined -%}
            {%- set ns.meta = model.meta.get('dpf.identity') -%}
        {%- endif -%}
        {%- if ns.meta is none or ns.meta.get('contract_digest') is none -%}
            {{ exceptions.raise_compiler_error(
                'could not resolve active contract digest for '
                ~ ident.source ~ '.' ~ ident.table
            ) }}
        {%- endif -%}
        {{ return(ns.meta) }}
    {%- else -%}
        {%- set meta_identity = model.meta.get('dpf.identity') -%}
        {%- if meta_identity is none -%}
            {{ exceptions.raise_compiler_error('dpf_source_freshness_query requires source meta dpf.identity') }}
        {%- endif -%}
        {{ return(meta_identity) }}
    {%- endif -%}
{%- endmacro %}


{% macro dpf_stream_status_relation(identity) -%}
    {{ return(source('bronze_' ~ identity['source'] ~ '_' ~ identity['table'], 'stream_status')) }}
{%- endmacro %}


{% macro dpf_heartbeat_slo_seconds(heartbeat_slo_seconds=none) -%}
    {%- if heartbeat_slo_seconds is not none -%}
        {{ return(heartbeat_slo_seconds | int) }}
    {%- else -%}
        {{ return(var('dpf_heartbeat_slo_seconds', 120) | int) }}
    {%- endif -%}
{%- endmacro %}


{% macro dpf_configured_projection_target(identity, projection_target=none) -%}
    {%- if projection_target is not none and projection_target | string | length > 0 -%}
        {{ return(projection_target | string) }}
    {%- endif -%}
    {%- set compact_key = dpf_compact_identity_key(identity) -%}
    {%- set ns = namespace(target=none) -%}
    {%- set source_uid = model.unique_id if model is defined and model.unique_id is defined else none -%}
    {%- for node in graph.nodes.values() -%}
        {%- if node.resource_type == 'test' and node.test_metadata is defined and node.test_metadata is not none -%}
            {%- if node.test_metadata.name == 'dpf_projection_integrity' -%}
                {%- set kwargs = node.test_metadata.kwargs if node.test_metadata.kwargs is defined else {} -%}
                {%- set attached = node.attached_node if node.attached_node is defined else none -%}
                {%- if kwargs.get('projection_target')
                     and (
                         kwargs.get('identity_key') == compact_key
                         or (source_uid is not none and attached == source_uid)
                     ) -%}
                    {%- set ns.target = kwargs.get('projection_target') -%}
                {%- endif -%}
            {%- endif -%}
        {%- endif -%}
    {%- endfor -%}
    {%- if ns.target is none or ns.target | string | length == 0 -%}
        {{ exceptions.raise_compiler_error(
            'could not resolve projection_target for '
            ~ identity['source'] ~ '.' ~ identity['table']
            ~ '; pass it to dpf_source_freshness_query or define dpf_projection_integrity on the source'
        ) }}
    {%- endif -%}
    {{ return(ns.target | string) }}
{%- endmacro %}


{% macro dpf_projection_fault_sql(identity_key, projection_target=none, heartbeat_slo_seconds=none) -%}
    {%- set identity = dpf_active_source_identity(identity_key) -%}
    {%- set target = dpf_configured_projection_target(identity, projection_target) -%}
    {%- set stream = dpf_stream_status_relation(identity) -%}
    {%- set compact_key = dpf_compact_identity_key(identity) -%}
    {%- set digest = identity['contract_digest'] -%}
    {%- set slo = dpf_heartbeat_slo_seconds(heartbeat_slo_seconds) -%}
    {%- set json_col = adapter.quote('json') -%}
with matched as (
    select
        stream_row.contract_digest as contract_digest,
        {{ dpf_json_text('stream_row.' ~ json_col, 'committed_at') }} as committed_at_text,
        {{ dpf_json_text('stream_row.' ~ json_col, 'heartbeat_at') }} as heartbeat_at_text
    from {{ stream }} as stream_row
    where stream_row.identity_key = '{{ compact_key }}'
      and stream_row.projection_target = '{{ target }}'
),
stats as (
    select
        (select count(*) from matched) as match_count,
        (select count(*) from matched where contract_digest = '{{ digest }}') as expected_count,
        (select committed_at_text from matched where contract_digest = '{{ digest }}' limit 1) as committed_at_text,
        (select heartbeat_at_text from matched where contract_digest = '{{ digest }}' limit 1) as heartbeat_at_text
),
classified as (
    select
        case
            when expected_count = 0 and match_count = 0 then 'missing_projection'
            when expected_count > 1 then 'duplicate_projection'
            when expected_count = 0 then 'wrong_digest'
            when committed_at_text is null
                 or lower(trim(committed_at_text)) in ('null', '') then 'null_committed_at'
            when heartbeat_at_text is not null
                 and lower(trim(heartbeat_at_text)) not in ('null', '')
                 and {{ dpf_age_seconds(dpf_parse_utc_instant('heartbeat_at_text')) }} > {{ slo }} then 'stale_heartbeat'
            else cast(null as {{ dbt.type_string() }})
        end as reason,
        committed_at_text
    from stats
)
{%- endmacro %}


{% macro dpf_source_freshness_query(identity_key=none, projection_target=none, heartbeat_slo_seconds=none) -%}
    {%- set identity = dpf_active_source_identity(identity_key) -%}
    {%- set target = dpf_configured_projection_target(identity, projection_target) -%}
    {{ dpf_projection_fault_sql(dpf_compact_identity_key(identity), target, heartbeat_slo_seconds) }}
select
    case
        when reason is null then {{ dpf_parse_utc_instant('committed_at_text') }}
        else {{ dpf_poison_timestamp() }}
    end as committed_at
from classified
union all
select {{ dpf_poison_timestamp() }} as committed_at
from classified
where reason is not null
{%- endmacro %}


{% test dpf_projection_integrity(model, identity_key, projection_target, heartbeat_slo_seconds=none) %}
    {%- if execute -%}
        {{ dpf_projection_fault_sql(identity_key, projection_target, heartbeat_slo_seconds) }}
select reason
from classified
where reason is not null
    {%- else -%}
select cast(null as {{ dbt.type_string() }}) as reason
where 1 = 0
    {%- endif -%}
{% endtest %}


{% test dpf_schedule_timeliness(model, identity_key, projection_target) %}
    {%- if execute -%}
        {%- set identity = dpf_active_source_identity(identity_key) -%}
        {%- set stream = dpf_stream_status_relation(identity) -%}
        {%- set compact_key = dpf_compact_identity_key(identity) -%}
        {%- set digest = identity['contract_digest'] -%}
        {%- set json_col = adapter.quote('json') -%}
select
    {{ dpf_json_text('stream_row.' ~ json_col, 'timeliness') }} as reason
from {{ stream }} as stream_row
where stream_row.identity_key = '{{ compact_key }}'
  and stream_row.projection_target = '{{ projection_target }}'
  and stream_row.contract_digest = '{{ digest }}'
  and {{ dpf_json_text('stream_row.' ~ json_col, 'timeliness') }} in ('late', 'missing')
    {%- else -%}
select cast(null as {{ dbt.type_string() }}) as reason
where 1 = 0
    {%- endif -%}
{% endtest %}
