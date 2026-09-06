{#
  Watermark increments, executed in the warehouse: the consumption watermark and the
  delta window every declared table's layers filter on.

  Vocabulary, used here exactly as the translator and the errors use it:

    effective column the one staging output column a table's projection maps to the
                     effective time.
    consumption watermark
                     the point on the effective column up to which every relation fed
                     by a source has absorbed history.
    delta window     the interval [consumption watermark minus lookback, infinity),
                     entered with a >= comparison at the floor.
    replay suppression
                     the guard discarding a candidate row whose (business key,
                     fingerprint, effective time) already exists in the target.
#}

{#- ==============================================================================
    Watermark increments: the consumption watermark, the delta window and its floor.

    A table carrying a declared staging increment block processes one delta window per
    run. The window is the interval [consumption watermark minus lookback, infinity),
    and every layer of that table -- the staging model, its bridges and its stages --
    filters on the one floor these macros resolve.

    The consumption watermark for a source table is the least value, across the
    satellites fed by that table only, of each satellite's maximum effective time, coalesced to the
    initial-load sentinel. The satellites are the last relations a successful run
    advances, so their state is exactly what has been durably absorbed: a run that fails
    mid-way leaves them behind, and the next run's window re-covers the rows it never
    consumed. Crash safety is structural, and no state store, ledger relation or Bronze
    change carries any part of it.

    Two build constraints shape the resolution below:

      - Every satellite is resolved through adapter.get_relation under an `execute`
        guard, on the coordinates the manifest already carries for that model, and read
        through run_query. No relation name is built here.
      - Nothing here takes a ref() to a satellite. That edge would run from a staging
        layer to a satellite built from it, and dbt would refuse the project with a
        dependency cycle.

    The per-satellite MAX carries a record_source predicate bound to that satellite's
    own record-source literal, so a satellite that ever held a sibling source's rows
    could never advance this table's watermark.
    ============================================================================== -#}

{#- The initial-load sentinel: the value a satellite that has absorbed nothing reports,
    and the floor of an unbounded window. It is written date-shaped, so a cast to a date
    column and a cast to a timestamp column both accept it. -#}
{%- macro dpf_window_sentinel() -%}1900-01-01{%- endmacro -%}

{#- One window floor, rendered as a literal normalised to the effective column's own
    native type, so the boundary evaluates at one declared granularity while every
    pruning column stays bare. -#}
{%- macro dpf_window_floor_literal(value, column_type) -%}
    {%- set literal = dpf_window_sentinel() if value is none else (value | string) -%}
    {%- set rendered_type = column_type if column_type else dbt.type_timestamp() -%}
    {{- "cast(" ~ dbt.string_literal(literal) ~ " as " ~ rendered_type ~ ")" -}}
{%- endmacro -%}

{#- The physical relation of a generated model, on the coordinates the manifest carries
    for it. adapter.get_relation reports whether the relation exists; a model that has
    never been built resolves to none. -#}
{%- macro dpf_manifest_relation(model_name) -%}
    {%- if execute -%}
        {%- for node in graph.nodes.values() -%}
            {%- if node.resource_type == 'model' and node.name == model_name -%}
                {%- do return(adapter.get_relation(
                    database=node.database, schema=node.schema, identifier=node.alias)) -%}
            {%- endif -%}
        {%- endfor -%}
    {%- endif -%}
    {%- do return(none) -%}
{%- endmacro -%}

{#- The satellites' effective column type, read from the first satellite that exists. It
    is the type every window floor for this source is normalised to. -#}
{%- macro dpf_watermark_column_type(watermark) -%}
    {%- if execute -%}
        {%- set effective = watermark['effective_column'] -%}
        {%- for entry in watermark['satellites'] -%}
            {%- set relation = dpf_manifest_relation(entry['relation']) -%}
            {%- if relation is not none -%}
                {%- for column in adapter.get_columns_in_relation(relation) -%}
                    {%- if column.name | lower == effective | lower -%}
                        {%- do return(column.data_type) -%}
                    {%- endif -%}
                {%- endfor -%}
            {%- endif -%}
        {%- endfor -%}
    {%- endif -%}
    {%- do return(none) -%}
{%- endmacro -%}

{#- The delta window's floor for one source table: the consumption watermark minus the
    lookback. A satellite that does not exist yet has absorbed nothing, so the whole
    watermark falls to the sentinel and the window is unbounded below. -#}
{%- macro dpf_resolve_window_floor(lookback_minutes, watermark) -%}
    {%- set effective = watermark['effective_column'] -%}
    {%- set column_type = dpf_watermark_column_type(watermark) -%}
    {%- set sentinel = dpf_window_floor_literal(none, column_type) -%}
    {%- set maxima = [] -%}
    {%- for entry in watermark['satellites'] -%}
        {%- set relation = dpf_manifest_relation(entry['relation']) -%}
        {%- if relation is none -%}
            {%- do return(sentinel) -%}
        {%- endif -%}
        {%- do maxima.append(
            "(select coalesce(max(" ~ effective ~ "), " ~ sentinel ~ ") from " ~ relation
            ~ " where record_source = " ~ dbt.string_literal(entry['record_source']) ~ ")") -%}
    {%- endfor -%}
    {%- if maxima | length == 0 -%}
        {%- do return(sentinel) -%}
    {%- endif -%}
    {%- set consumption_watermark = "least(" ~ (maxima | join(", ")) ~ ")" -%}
    {%- set query -%}
        select cast({{ dbt.dateadd('minute', 0 - lookback_minutes, consumption_watermark) }}
            as {{ column_type if column_type else dbt.type_timestamp() }}) as window_floor
    {%- endset -%}
    {%- set result = run_query(query) -%}
    {%- if result is none or result.rows | length == 0 -%}
        {%- do return(sentinel) -%}
    {%- endif -%}
    {%- do return(dpf_window_floor_literal(result.rows[0][0], column_type)) -%}
{%- endmacro -%}

{#- The floor every generated window predicate calls.

    The resolution is cached under the source table it belongs to, so the floor calls
    inside one render share a single resolution: a staging model calls for the floor
    three times, in its window, in its bounded delete predicate and in its run report,
    and resolves it fewer times than that.

    The cache lives for the render that filled it, and dbt renders a model's body and
    its materialization separately, which makes per-model resolution the standing
    fallback. Its cost is bounded: one resolution per windowed render, and one satellite
    lookup per satellite of the source inside it, which the log record below carries as
    its satellite count. Every windowed layer of a source -- its staging model, its
    bridges and its stages -- resolves the same floor by construction, because satellite
    state is static within an invocation until the satellites themselves build, and they
    build after every windowed layer in DAG order.

    Parsing reads no warehouse state, so a parsed predicate carries the sentinel and the
    run-time render carries the resolved floor. -#}
{%- macro dpf_window_floor(source_name, table_name, effective_column, lookback_minutes, watermark) -%}
    {%- if not execute -%}
        {{- dpf_window_floor_literal(none, none) -}}
    {%- else -%}
        {%- set key = 'dpf_window_floor:' ~ source_name ~ '.' ~ table_name -%}
        {%- set cached = load_result(key) -%}
        {%- if cached is not none -%}
            {#- load_result consumes the entry, so it goes straight back for the next
                predicate in this render. -#}
            {%- do store_result(key, cached['response']) -%}
            {{- cached['response'] -}}
        {%- else -%}
            {%- set floor = dpf_resolve_window_floor(lookback_minutes, watermark) -%}
            {%- do store_result(key, floor) -%}
            {%- do log("DPF_WINDOW_FLOOR=" ~ tojson({
                "model": this.identifier if this else none,
                "source": source_name,
                "table": table_name,
                "effective_column": effective_column,
                "lookback_minutes": lookback_minutes,
                "satellites": watermark['satellites'] | length,
                "floor": floor | string}), info=True) -%}
            {{- floor -}}
        {%- endif -%}
    {%- endif -%}
{%- endmacro -%}

{#- What a normal run reports for one declared table: the applied window floor, the
    cumulative row count in the staging relation and the rows currently at or above that
    floor. It does not claim to count rows written by this invocation. -#}
{%- macro dpf_log_window_rows(source_name, table_name, effective_column, lookback_minutes, watermark) -%}
    {%- if execute -%}
        {%- set floor = dpf_window_floor(
            source_name, table_name, effective_column, lookback_minutes, watermark) -%}
        {%- set query -%}
            select
                count(*) as relation_rows_total,
                sum(case when {{ effective_column }} >= {{ floor }} then 1 else 0 end) as rows_in_window
            from {{ this }}
        {%- endset -%}
        {%- set result = run_query(query) -%}
        {%- set row = result.rows[0] if result is not none and result.rows | length > 0 else none -%}
        {%- do log("DPF_WINDOW_ROWS=" ~ tojson({
            "model": this.identifier,
            "source": source_name,
            "table": table_name,
            "floor": floor | string,
            "relation_rows_total": (row[0] | int) if row is not none else 0,
            "relation_rows_in_window": (row[1] | int) if (row is not none and row[1] is not none) else 0}),
            info=True) -%}
    {%- endif -%}
{%- endmacro -%}

{#- The alias dbt gives the destination relation inside an incremental predicate. It is
    the adapter's own choice, so the delete predicate names it through this dispatch and
    carries one target's spelling nowhere. -#}
{%- macro dpf_incremental_target_alias() -%}
    {{- adapter.dispatch('dpf_incremental_target_alias')() -}}
{%- endmacro -%}

{%- macro default__dpf_incremental_target_alias() -%}DBT_INTERNAL_DEST{%- endmacro -%}

{%- macro duckdb__dpf_incremental_target_alias() -%}DBT_INCREMENTAL_TARGET{%- endmacro -%}

{#- ------------------------------------------------------------------------------
    The bounded delete predicate.

    dbt captures a model's configuration while it parses the project, and parsing reads
    no warehouse state, so a floor resolved there is always the sentinel and a predicate
    built from it prunes nothing. The generated configuration therefore states the window
    the delete side is bounded by, and the delete predicate is built from that statement
    at execution, where the satellites are readable.

    The statement travels as one marked string, and the incremental strategy below turns
    every marked predicate into the bounded comparison before it hands the arguments to
    dbt's own delete-and-insert SQL, which stays unchanged.
    ------------------------------------------------------------------------------ -#}
{%- macro dpf_window_delete_marker() -%}DPF_WINDOW_DELETE:{%- endmacro -%}

{#- The statement is rendered through the tojson filter rather than the context function
    of the same name, because parsing carries the filter and not the function. -#}
{%- macro dpf_window_delete_predicate(source_name, table_name, effective_column, operator, lookback_minutes, watermark) -%}
    {{- dpf_window_delete_marker() ~ ({
        "source": source_name,
        "table": table_name,
        "effective_column": effective_column,
        "operator": operator,
        "lookback_minutes": lookback_minutes,
        "watermark": watermark} | tojson) -}}
{%- endmacro -%}

{%- macro dpf_resolved_incremental_predicates(predicates) -%}
    {%- set marker = dpf_window_delete_marker() -%}
    {%- set resolved = [] -%}
    {%- for predicate in predicates or [] -%}
        {%- if predicate is string and predicate.startswith(marker) -%}
            {%- set window = fromjson(predicate[marker | length:]) -%}
            {%- set floor = dpf_window_floor(
                window['source'],
                window['table'],
                window['effective_column'],
                window['lookback_minutes'],
                window['watermark']) -%}
            {%- do resolved.append(
                dpf_incremental_target_alias() ~ "." ~ window['effective_column']
                ~ " " ~ window['operator'] ~ " " ~ floor) -%}
        {%- else -%}
            {%- do resolved.append(predicate) -%}
        {%- endif -%}
    {%- endfor -%}
    {%- do return(resolved) -%}
{%- endmacro -%}

{#- The delete-and-insert strategy for every target. It resolves the marked predicates
    and delegates: dbt's own SQL for the delete and the insert stands unchanged, and a
    predicate this project did not write passes through untouched. -#}
{%- macro default__get_incremental_delete_insert_sql(arg_dict) -%}
    {%- do return(get_delete_insert_merge_sql(
        arg_dict["target_relation"],
        arg_dict["temp_relation"],
        arg_dict["unique_key"],
        arg_dict["dest_columns"],
        dpf_resolved_incremental_predicates(arg_dict["incremental_predicates"]))) -%}
{%- endmacro -%}

{#- ------------------------------------------------------------------------------
    The audit invocation.

    A normal run performs no scan outside the window, so a key whose rows all fall below
    the floor stays invisible to it. This operation is the periodic check that finds
    those keys: it scans the full source on demand and reports the keys sitting wholly
    outside the window, with a sample an operator acts on. Two remedies act on a key the
    report names: a one-run lookback widening, and a bounded backfill over that key.
    ------------------------------------------------------------------------------ -#}
{%- macro dpf_window_audit(payload) -%}
    {%- if execute -%}
        {%- set floor = dpf_window_floor(
            payload['source'],
            payload['table'],
            payload['effective_column'],
            payload['lookback_minutes'],
            payload['watermark']) -%}
        {%- set landing = payload['landing'] -%}
        {%- if landing['kind'] == 'source' -%}
            {%- set relation = source(landing['source_name'], landing['identifier']) -%}
        {%- else -%}
            {%- set relation = ref(landing['model']) -%}
        {%- endif -%}
        {%- set column_type = dpf_watermark_column_type(payload['watermark']) -%}
        {%- set effective_type = column_type if column_type else dbt.type_timestamp() -%}
        {%- set key_columns = payload['landing_key'] -%}
        {%- set keys = key_columns | join(', ') -%}
        {%- set sample_size = payload.get('sample', 20) -%}
        {%- set keyed -%}
            select {{ keys }},
                max(cast({{ payload['landing_effective_column'] }} as {{ effective_type }})) as max_effective
            from {{ relation }}
            group by {{ keys }}
        {%- endset -%}
        {%- set totals = run_query(
            "select count(*) as keys_total,"
            ~ " sum(case when max_effective < " ~ floor ~ " then 1 else 0 end) as keys_outside"
            ~ " from (" ~ keyed ~ ") as audited") -%}
        {%- set row = totals.rows[0] -%}
        {%- set sampled = run_query(
            "select " ~ keys ~ ", max_effective from (" ~ keyed ~ ") as audited"
            ~ " where max_effective < " ~ floor
            ~ " order by max_effective desc limit " ~ sample_size) -%}
        {%- set sample = [] -%}
        {%- for sample_row in sampled.rows -%}
            {%- set entry = {} -%}
            {%- for column in key_columns -%}
                {%- do entry.update({column: sample_row[loop.index0] | string}) -%}
            {%- endfor -%}
            {%- do entry.update({"max_effective": sample_row[key_columns | length] | string}) -%}
            {%- do sample.append(entry) -%}
        {%- endfor -%}
        {%- do log("DPF_WINDOW_AUDIT_RESULT=" ~ tojson({
            "operation": "audit-window",
            "source": payload['source'],
            "table": payload['table'],
            "effective_column": payload['effective_column'],
            "lookback_minutes": payload['lookback_minutes'],
            "floor": floor | string,
            "keys_total": row[0] | int,
            "keys_outside_window": (row[1] | int) if row[1] is not none else 0,
            "sample": sample}), info=True) -%}
    {%- endif -%}
{%- endmacro -%}
