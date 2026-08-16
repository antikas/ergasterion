{#-
  Shared append-only decision-log macros.

  `entity_resolution_decisions_log` and `deal_decision_log` are plain tables created
  idempotently by dbt on-run-start hooks. They are declared as sources, never as dbt
  seeds or models, so normal builds cannot truncate analyst decisions.

  Both tables use the target's generated raw schema. Snowflake and DuckDB execute the
  DDL and fixture merge; BigQuery returns a no-op `select 1`. The target configuration
  below owns the supported target set and timestamp type.
-#}

{% macro dpf_append_only_log_raw_schema() %}
{{ return(generate_schema_name('raw', none)) }}
{% endmacro %}

{#- One source of truth for the append-only-log target allow-set and timestamp type.
    Unsupported targets retain timestamp_ltz only as an inert column-list render
    default: every schema/table/fixture statement is guarded by `supported`, so
    BigQuery continues to execute `select 1` and never consumes that type token. -#}
{% macro dpf_append_only_log_target_config() %}
{%- set timestamp_type_by_target = {
    'snowflake': 'timestamp_ltz',
    'duckdb': 'timestamp'
} -%}
{{ return({
    'supported': target.type in timestamp_type_by_target,
    'timestamp_type': timestamp_type_by_target.get(target.type, 'timestamp_ltz')
}) }}
{% endmacro %}

{#- DuckDB introduced MERGE INTO in 1.4. The append-only fixture path deliberately
    keeps the same single MERGE used on Snowflake. An INSERT ... WHERE NOT EXISTS
    anti-join is the documented fallback for older DuckDB engines, but is not carried
    as a second implementation: fail loud instead, so the append-only contract has one
    statement shape. Parse major/minor numerically (not lexicographically: 1.10 > 1.4)
    before any DuckDB DDL or MERGE is returned. -#}
{% macro dpf_assert_append_only_log_engine_version() %}
{%- if target.type == 'duckdb' and execute -%}
    {%- set version_result = run_query('select version() as duckdb_version') -%}
    {%- set raw_version = version_result.columns[0].values()[0] | string -%}
    {%- set version_parts = (raw_version | lower | replace('v', '')).split('.') -%}
    {%- set major = version_parts[0] | int if version_parts | length > 0 else 0 -%}
    {%- set minor = version_parts[1] | int if version_parts | length > 1 else 0 -%}
    {%- if major < 1 or (major == 1 and minor < 4) -%}
        {{ exceptions.raise_compiler_error(
            'Append-only decision logs require DuckDB >= 1.4 for MERGE INTO; found ' ~ raw_version ~
            '. The documented older-engine fallback is INSERT with an anti-join, but this project ' ~
            'keeps one MERGE implementation. Upgrade DuckDB instead.'
        ) }}
    {%- endif -%}
{%- endif -%}
{{ return('') }}
{% endmacro %}

{% macro dpf_ensure_append_only_log_schema() %}
{%- set target_config = dpf_append_only_log_target_config() -%}
{%- if target_config['supported'] -%}
{{- dpf_assert_append_only_log_engine_version() -}}
create schema if not exists {{ target.database }}.{{ dpf_append_only_log_raw_schema() }}
{%- else -%}
select 1
{%- endif -%}
{% endmacro %}

{#- Shared parameterised ensure-macro. `table_name`: the bare table
    name, created in the raw schema above. `columns`: a list of "column_name type"
    strings, rendered one per line, 4-space indented, comma-separated, no trailing
    comma on the last. -#}
{% macro dpf_ensure_append_only_log_table(table_name, columns) %}
{%- set columns_sql = "\n    " ~ columns | join(",\n    ") -%}
{%- set target_config = dpf_append_only_log_target_config() -%}
{%- if target_config['supported'] -%}
{{- dpf_assert_append_only_log_engine_version() -}}
create table if not exists {{ target.database }}.{{ dpf_append_only_log_raw_schema() }}.{{ table_name }} ({{ columns_sql }}
)
{%- else -%}
select 1
{%- endif -%}
{% endmacro %}

{#- ---------------------------------------------------------------------------
    entity_resolution_decisions_log. DuckDB substitutes its native timestamp token.
    --------------------------------------------------------------------------- -#}

{% macro dpf_ensure_er_decisions_schema() %}
{{- dpf_ensure_append_only_log_schema() -}}
{% endmacro %}

{% macro dpf_er_decisions_log_columns() %}
{% set target_config = dpf_append_only_log_target_config() %}
{{ return([
    'entity_type string',
    'source_system_a string',
    'source_id_a string',
    'source_system_b string',
    'source_id_b string',
    'decision string',
    'matched_entity_key string',
    'reviewed_by string',
    'reviewed_at ' ~ target_config['timestamp_type'],
    'notes string'
]) }}
{% endmacro %}

{% macro dpf_ensure_er_decisions_table() %}
{{- dpf_ensure_append_only_log_table('entity_resolution_decisions_log', dpf_er_decisions_log_columns()) -}}
{% endmacro %}

{#- Compatible alias for the raw-schema helper. -#}
{% macro dpf_er_decisions_raw_schema() %}
{{- dpf_append_only_log_raw_schema() -}}
{% endmacro %}

{#- ---------------------------------------------------------------------------
    deal_decision_log: the investment-authorisation shape applied to
    the deal pipeline's approvals workflow. Same append-only, dbt-unmanaged,
    on-run-start-created pattern as the ER log above, sharing the SAME ensure-macro
    (never a copy). Decision values are approve / approve_with_conditions / decline /
    defer, plus:
      - decision_id: a business-assigned identifier for the decision EVENT itself --
        never a load-time or wall-clock artifact, so latest-wins dedup
        (models/deal_approvals/int_deal_latest_decision.sql) has a deterministic
        secondary tie-break on ties in decided_at that never falls back to
        load_datetime.
      - external_deal_id: the deal's own business key from its single ORIGO source
        which intra-source resolution never renames, expressed as the business key the log is keyed on (resolved to the
        golden deal_id downstream, the same design deal_stage_history_seed already
        uses for its entity_external_id column).
      - decision / conditions: the decision and any attached conditions.
      - actor / decided_at: who decided and when.
    --------------------------------------------------------------------------- -#}

{% macro dpf_deal_decision_log_columns() %}
{% set target_config = dpf_append_only_log_target_config() %}
{{ return([
    'decision_id string',
    'external_deal_id string',
    'decision string',
    'conditions string',
    'actor string',
    'decided_at ' ~ target_config['timestamp_type']
]) }}
{% endmacro %}

{% macro dpf_ensure_deal_decision_log_table() %}
{{- dpf_ensure_append_only_log_table('deal_decision_log', dpf_deal_decision_log_columns()) -}}
{% endmacro %}

{#- Generic seed-to-append-only-log fixture MERGE.
    The append-only deal_decision_log is created empty at on-run-start (never a dbt
    seed/model, so dbt never truncates a real row). Deterministic fixture rows that the
    survival, deduplication, and derivation tests exercise live in a dbt seed
    (seeds/deal_decision_log_fixtures.csv), NOT hardcoded here in the engine macros. A
    consumer copying macros/ into a new scaffold inherits the generic MERGE mechanism
    without this estate's worked-domain fixture data.

    This macro renders the seed -> log MERGE from PARAMETERS only:
      * `target_table`     -- the append-only log's bare name, in the shared raw schema;
      * `source_relation`  -- the fixture seed's relation, threaded via `this` from the
                              seed's +post-hook in dbt_project.yml;
      * `key_column`       -- the append-only key. `when not matched then insert` ONLY,
                              never a matched-update branch (append-only discipline, see the
                              ensure-macro docstring above), so re-running inserts nothing
                              once the fixtures exist.
      * `columns`          -- the log's "name type" column spec (reused from the log's own
                              column macro, e.g. dpf_deal_decision_log_columns()). Each
                              source column is mapped blank -> null (blank_to_null) then cast
                              to its declared type, so a blank CSV cell lands as a typed null.

    Snowflake and DuckDB execute the same single MERGE; BigQuery remains a no-op
    (`select 1`). DuckDB is version-gated at >= 1.4 before the MERGE is returned.

    ORDERING: the log table is created at on-run-start, which dbt runs BEFORE seeds; this
    post-hook merges the fixtures during the seed phase; and each consuming model
    (models/deal_approvals/int_deal_latest_decision.sql) plus the survival test carry a
    `-- depends_on` edge on the fixture seed, so they run AFTER this merge and see the
    fixtures on a fresh build -- preserving deal_decision_log read semantics. -#}
{% macro dpf_merge_seed_into_append_only_log(target_table, source_relation, key_column, columns) %}
{%- set target_config = dpf_append_only_log_target_config() -%}
{%- if target_config['supported'] -%}
{{- dpf_assert_append_only_log_engine_version() -}}
{%- set col_names = [] -%}
{%- set select_exprs = [] -%}
{%- for c in columns -%}
    {%- set name = c.split(' ')[0] -%}
    {%- set col_type = c.split(' ', 1)[1] -%}
    {%- do col_names.append(name) -%}
    {%- do select_exprs.append('cast(' ~ blank_to_null('src.' ~ name) ~ ' as ' ~ col_type ~ ') as ' ~ name) -%}
{%- endfor -%}
merge into {{ target.database }}.{{ dpf_append_only_log_raw_schema() }}.{{ target_table }} as tgt
using (
    select {{ select_exprs | join(', ') }}
    from {{ source_relation }} as src
) as fixture
on tgt.{{ key_column }} = fixture.{{ key_column }}
when not matched then insert ({{ col_names | join(', ') }})
values ({% for n in col_names %}fixture.{{ n }}{{ ', ' if not loop.last }}{% endfor %})
{%- else -%}
select 1
{%- endif -%}
{% endmacro %}
