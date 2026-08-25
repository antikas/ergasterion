{#
  Cross-target satellite replay suppression for AutomateDV 0.11.5.

  This file holds two things: the one shared implementation of replay suppression
  (dpf_sat_replay_suppression), and the Snowflake dispatch arm that adopts it
  (snowflake__sat). The DuckDB arm in macros/automate_dv_duckdb.sql adopts the same
  shared implementation, so both targets carry one key, one comparison and one body.

  Replay suppression is the satellite guard that discards a candidate row whose
  (business key, hashdiff, effective time) already exists in the target. A staged
  history replayed in full on every run hands the satellite the versions it already
  stores; without the guard an incremental run appends each of them again.

  The guard leaves AutomateDV's own satellite SQL authoritative: it wraps
  automate_dv.default__sat unchanged and filters the rows that SQL produces.

  The existing-side scan is bounded by the window floor. A candidate row collides only
  with a stored row carrying the same effective time, so stored rows below the lowest
  effective time on the candidate side can never collide. Where the table carries a
  delta window, that lowest candidate effective time is the window floor, and the guard
  reads only the stored rows inside the window.
#}

{#- The window floor the existing-side scan is bounded by, read from the stage relation
    the satellite is built from. The stage is a windowed recompute wherever its table
    declares a staging increment block, so its lowest effective time is the window floor
    there and the whole staged set's lowest effective time everywhere else.

    The bound stands down when the stage carries a null effective time: a candidate null
    collides with a stored null, and a floor comparison discards a null. It also stands
    down when the stage relation has never been built, and when the effective time
    spans more than one column, so the bound only ever narrows a scan it is sound for. -#}
{%- macro dpf_sat_window_floor(src_eff, source_model) -%}
    {%- if execute -%}
        {%- set effective_columns = automate_dv.expand_column_list(columns=[src_eff]) -%}
        {%- if effective_columns | length == 1 -%}
            {%- set column = effective_columns[0] -%}
            {%- set declared = ref(source_model) -%}
            {%- set staged = adapter.get_relation(
                database=declared.database,
                schema=declared.schema,
                identifier=declared.identifier) -%}
            {%- if staged is not none -%}
                {%- set probe = run_query(
                    "select count(*) as staged_rows,"
                    ~ " sum(case when " ~ column ~ " is null then 1 else 0 end) as null_effective"
                    ~ " from " ~ staged) -%}
                {%- if probe is not none and probe.rows | length > 0 -%}
                    {%- set staged_rows = probe.rows[0][0] | int -%}
                    {%- set null_effective = (probe.rows[0][1] | int) if probe.rows[0][1] is not none else 0 -%}
                    {%- if staged_rows > 0 and null_effective == 0 -%}
                        {%- do return("(select min(" ~ column ~ ") from " ~ staged ~ ")") -%}
                    {%- endif -%}
                {%- endif -%}
            {%- endif -%}
        {%- endif -%}
    {%- endif -%}
    {%- do return(none) -%}
{%- endmacro -%}

{%- macro dpf_sat_replay_suppression(src_pk, src_hashdiff, src_payload, src_extra_columns, src_eff, src_ldts, src_source, source_model) -%}
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
        {%- set window_floor = dpf_sat_window_floor(src_eff, source_model) -%}
        SELECT candidate.*
        FROM (
            {{ generated_satellite_sql }}
        ) AS candidate
        WHERE NOT EXISTS (
            SELECT 1
            FROM {{ this }} AS existing
            WHERE
                {%- if window_floor is not none %}
                {{ automate_dv.prefix([effective_columns[0]], 'existing', alias_target='target') }} >=
                    {{ window_floor }}
                    AND
                {%- endif %}
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

{%- macro snowflake__sat(src_pk, src_hashdiff, src_payload, src_extra_columns, src_eff, src_ldts, src_source, source_model) -%}
    {#
      Snowflake reads IS NOT DISTINCT FROM as the null-safe equality DuckDB reads it as,
      so the shared body renders the same key on both targets with no dialect branch.
    #}
    {{- dpf_sat_replay_suppression(
        src_pk=src_pk,
        src_hashdiff=src_hashdiff,
        src_payload=src_payload,
        src_extra_columns=src_extra_columns,
        src_eff=src_eff,
        src_ldts=src_ldts,
        src_source=src_source,
        source_model=source_model
    ) -}}
{%- endmacro %}
