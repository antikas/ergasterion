-- Named test: ER-decisions-log regression. After refactoring
-- macros/entity_resolution_decisions.sql's hardcoded single-purpose form into the
-- shared parameterised ensure-macro (dpf_ensure_append_only_log_table), the ER
-- surface's on-run-start create statement and its 10-column shape must be
-- byte-unchanged on Snowflake; DuckDB has the same shape with its native TIMESTAMP
-- token. This compares the ACTUAL macro-rendered DDL text
-- (dpf_ensure_er_decisions_table(), the exact same macro call
-- dbt_project.yml's on-run-start hook invokes) against the pinned EXPECTED DDL text
-- that documents the pre-refactor shape -- a compile-time Jinja string comparison
-- (no string-literal embedding, no warehouse round trip needed for the comparison
-- itself), so it proves the macro TEXT itself is unchanged rather than trusting
-- `create ... if not exists` to (silently) no-op identically. That silent no-op is
-- exactly the risk named in the 2026-07-13 plan: a warm dev schema already holding
-- the pre-refactor table would never re-run the DDL at all, so an idempotent create
-- alone could mask a divergence the refactor introduced -- this test still catches
-- it, because it compares the rendered TEXT, not the table's actual state.
--
-- Singular test: PASSES when it returns zero rows.

{% set actual_ddl = dpf_ensure_er_decisions_table() | trim %}
{% if target.type == 'duckdb' %}
{% set expected_ddl_raw -%}
create table if not exists {{ target.database }}.{{ dpf_append_only_log_raw_schema() }}.entity_resolution_decisions_log (
    entity_type string,
    source_system_a string,
    source_id_a string,
    source_system_b string,
    source_id_b string,
    decision string,
    matched_entity_key string,
    reviewed_by string,
    reviewed_at timestamp,
    notes string
)
{%- endset %}
{% elif target.type == 'snowflake' %}
{% set expected_ddl_raw -%}
create table if not exists {{ target.database }}.{{ dpf_append_only_log_raw_schema() }}.entity_resolution_decisions_log (
    entity_type string,
    source_system_a string,
    source_id_a string,
    source_system_b string,
    source_id_b string,
    decision string,
    matched_entity_key string,
    reviewed_by string,
    reviewed_at timestamp_ltz,
    notes string
)
{%- endset %}
{% else %}
{% set expected_ddl_raw -%}
select 1
{%- endset %}
{% endif %}
{% set expected_ddl = expected_ddl_raw | trim %}

{% if actual_ddl != expected_ddl %}
select 'er_decisions_log_ddl_changed' as failure_type
{% else %}
select 'er_decisions_log_ddl_changed' as failure_type where false
{% endif %}
