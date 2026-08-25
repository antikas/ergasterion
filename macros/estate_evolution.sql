{#
  Estate evolution: the declared re-baseline and the delta window, executed in the
  warehouse. The file carries two halves. The first is the re-baseline. The second,
  below the divider headed "Watermark increments", is the consumption watermark and the
  delta window every declared table's three layers filter on.

  Vocabulary, used here exactly as the emitter, the CLI and the errors use it:

    hashdiff basis   the exact column set an entity's stored hashdiffs were computed
                     over. AutomateDV sorts the columns alphabetically before hashing,
                     so the set is the whole fact.
    evolution ledger the generated committed file per domain recording every entity's
                     payload roster and hashdiff basis, with a basis version.
    extension        an additive payload change, absorbed online.
    re-baseline      the declared operation that adopts the current payload as the new
                     hashdiff basis and recomputes the stored hashdiffs in place.
    effective column the one staging output column a table's bridge maps to
                     effective_from.
    consumption watermark
                     the point on the effective column up to which every satellite of a
                     source has absorbed history.
    delta window     the interval [consumption watermark minus lookback, infinity),
                     entered with a >= comparison at the floor.
    replay suppression
                     the satellite guard discarding a candidate row whose (business key,
                     hashdiff, effective time) already exists in the target.

  A re-baseline runs in four operator steps over a committed pending basis. The first records a
  pending basis: a row in the pending relation here, and a pending record in the
  evolution ledger. While that row stands, generated stage models fail closed with the
  named error below. The second adds any missing satellite columns, recomputes every
  stored hashdiff from the stored payload under the pending basis, verifies the rewrite
  with a golden-hash parity spot check, writes one audit row per entity, and promotes the
  ledger. The operator then emits and deploys the regenerated models before explicitly
  clearing the pending row.

  The recompute calls automate_dv.hash with the new basis and is_hashdiff=true, so one
  hash construction serves the stage macros and this migration alike. The re-baseline is
  the one governed exception to the vault's insert-only rule and it rewrites the derived
  fingerprint alone: history rows, identities, counts, load datetimes and record sources
  stay exactly as they were.

  The recompute reads the stored payload of the row it writes, so running it twice
  writes the same value: every step here is idempotent, and a re-run after an
  interruption converges.

  The DuckDB arm provides the executable reference implementation. The Snowflake arm
  implements the transaction shape Snowflake needs: the idempotent column additions sit
  outside the explicit transaction because Snowflake DDL autocommits, and the per-entity
  recompute sits inside one explicit BEGIN and COMMIT.
#}

{#- The alias the bare hashdiff expression is rendered under and then stripped from.
    automate_dv.hash renders `<expression> AS <alias>`; an UPDATE needs the expression
    alone, and stripping a known alias keeps automate_dv.hash the single construction. -#}
{%- macro dpf_rebaseline_alias() -%}dpf_rebaseline_hashdiff{%- endmacro -%}

{%- macro dpf_hashdiff_expression(columns) -%}
    {#- The one hashdiff construction: automate_dv.hash over the given basis with
        is_hashdiff=true, rendered bare so it can sit on the right of an UPDATE. -#}
    {%- set alias = dpf_rebaseline_alias() -%}
    {%- set rendered = automate_dv.hash(columns=columns, alias=alias, is_hashdiff=true) -%}
    {%- set bare = rendered.rsplit('AS ' ~ alias, 1)[0] -%}
    {%- if bare == rendered -%}
        {%- do exceptions.raise_compiler_error(
            "estate evolution: the hashdiff expression did not render under the alias "
            ~ alias ~ ", so the bare expression cannot be recovered: " ~ rendered) -%}
    {%- endif -%}
    {{- bare | trim -}}
{%- endmacro -%}

{#- The two warehouse relations this operation owns, both in the target's own schema. -#}
{%- macro dpf_evolution_pending_relation() -%}
    {%- do return(api.Relation.create(
        database=target.database,
        schema=target.schema,
        identifier='dpf_estate_evolution_pending')) -%}
{%- endmacro -%}

{%- macro dpf_evolution_audit_relation() -%}
    {%- do return(api.Relation.create(
        database=target.database,
        schema=target.schema,
        identifier='dpf_estate_evolution_audit')) -%}
{%- endmacro -%}

{%- macro dpf_ensure_evolution_relations() -%}
    {%- set pending = dpf_evolution_pending_relation() -%}
    {%- set audit = dpf_evolution_audit_relation() -%}
    {%- do adapter.create_schema(pending) -%}
    {%- do run_query(
        "create table if not exists " ~ pending ~ " (
             entity " ~ dbt.type_string() ~ ",
             domain_name " ~ dbt.type_string() ~ ",
             basis_version " ~ dbt.type_int() ~ ",
             previous_basis_version " ~ dbt.type_int() ~ ",
             started_at " ~ dbt.type_timestamp() ~ "
         )") -%}
    {%- do run_query(
        "create table if not exists " ~ audit ~ " (
             entity " ~ dbt.type_string() ~ ",
             old_basis_version " ~ dbt.type_int() ~ ",
             new_basis_version " ~ dbt.type_int() ~ ",
             rows_rewritten " ~ dbt.type_int() ~ ",
             migrated_at " ~ dbt.type_timestamp() ~ "
         )") -%}
{%- endmacro -%}

{#- The fail-closed stage gate. A pending basis blocks generated stage execution, and
    the error names the entity, the basis versions and the recovery commands. Operators
    pause scheduled builds for the whole maintenance window; the explicit --clear step
    releases that window after regenerated models are deployed. -#}
{%- macro dpf_assert_no_pending_rebaseline() -%}
    {%- if execute -%}
        {%- set pending = dpf_evolution_pending_relation() -%}
        {%- set existing = adapter.get_relation(
            database=pending.database, schema=pending.schema, identifier=pending.identifier) -%}
        {%- if existing is not none -%}
            {%- set rows = run_query(
                "select entity, domain_name, previous_basis_version, basis_version from "
                ~ pending ~ " order by entity") -%}
            {%- if rows is not none and rows.rows | length > 0 -%}
                {%- set named = [] -%}
                {%- for row in rows.rows -%}
                    {%- do named.append(
                        row[1] ~ "." ~ row[0] ~ " (basis version " ~ row[2] ~ " -> " ~ row[3] ~ ")") -%}
                {%- endfor -%}
                {%- set first = rows.rows[0] -%}
                {%- do exceptions.raise_compiler_error(
                    "estate evolution: pending re-baseline on " ~ (named | join(", "))
                    ~ " blocks generated stage execution -- continue it with"
                    ~ " `ergasterion evolve rebaseline " ~ first[1] ~ " " ~ first[0] ~ " --complete`,"
                    ~ " release it after model deployment with `ergasterion evolve rebaseline "
                    ~ first[1] ~ " " ~ first[0] ~ " --clear`, or abort it before promotion with"
                    ~ " `ergasterion evolve rebaseline " ~ first[1] ~ " " ~ first[0] ~ " --abort`") -%}
            {%- endif -%}
        {%- endif -%}
    {%- endif -%}
{%- endmacro -%}

{#- Every generated stage model calls automate_dv.stage, and automate_dv.stage dispatches,
    so the root project's arm for a target is where the pending gate meets the build. The
    arms below assert the gate and then hand AutomateDV's own staging SQL back unchanged. -#}
{%- macro duckdb__stage(include_source_columns, source_model, hashed_columns, derived_columns, null_columns, ranked_columns) -%}
    {%- do dpf_assert_no_pending_rebaseline() -%}
    {{- automate_dv.default__stage(
        include_source_columns=include_source_columns,
        source_model=source_model,
        hashed_columns=hashed_columns,
        derived_columns=derived_columns,
        null_columns=null_columns,
        ranked_columns=ranked_columns) -}}
{%- endmacro -%}

{%- macro snowflake__stage(include_source_columns, source_model, hashed_columns, derived_columns, null_columns, ranked_columns) -%}
    {%- do dpf_assert_no_pending_rebaseline() -%}
    {{- automate_dv.default__stage(
        include_source_columns=include_source_columns,
        source_model=source_model,
        hashed_columns=hashed_columns,
        derived_columns=derived_columns,
        null_columns=null_columns,
        ranked_columns=ranked_columns) -}}
{%- endmacro -%}

{#- ------------------------------------------------------------------------------
    Phase one: record the pending basis in the warehouse.
    The pending row is written before the ledger's pending record, so a build is
    blocked from the first moment a re-baseline exists anywhere.
    ------------------------------------------------------------------------------ -#}
{%- macro dpf_evolve_rebaseline_begin(payload) -%}
    {%- if execute -%}
        {%- do dpf_ensure_evolution_relations() -%}
        {%- set pending = dpf_evolution_pending_relation() -%}
        {%- do run_query(
            "delete from " ~ pending ~ " where entity = '" ~ payload['entity'] ~ "'") -%}
        {%- do run_query(
            "insert into " ~ pending ~ " (entity, domain_name, basis_version, previous_basis_version, started_at)"
            ~ " select '" ~ payload['entity'] ~ "', '" ~ payload['domain'] ~ "', "
            ~ payload['basis_version'] ~ ", " ~ payload['previous_basis_version'] ~ ", "
            ~ dbt.current_timestamp()) -%}
        {#- dbt releases a run-operation's connection without committing, so every write
            here is committed explicitly. -#}
        {%- do adapter.commit() -%}
        {%- do log("DPF_REBASELINE_RESULT=" ~ tojson({
            "operation": "begin",
            "entity": payload['entity'],
            "domain": payload['domain'],
            "basis_version": payload['basis_version'],
            "previous_basis_version": payload['previous_basis_version']}), info=True) -%}
    {%- endif -%}
{%- endmacro -%}

{#- Clear the pending row. The ledger is the authority, so this runs after the ledger
    promotion or demotion has landed. -#}
{%- macro dpf_evolve_rebaseline_clear(payload) -%}
    {%- if execute -%}
        {%- do dpf_ensure_evolution_relations() -%}
        {%- set pending = dpf_evolution_pending_relation() -%}
        {%- do run_query(
            "delete from " ~ pending ~ " where entity = '" ~ payload['entity'] ~ "'") -%}
        {%- do adapter.commit() -%}
        {%- do log("DPF_REBASELINE_RESULT=" ~ tojson({
            "operation": "clear", "entity": payload['entity']}), info=True) -%}
    {%- endif -%}
{%- endmacro -%}

{#- Read the pending rows back, so the CLI and its tests can see warehouse state. Every
    operation here is called with the same one-argument shape; this one reads nothing
    from it and reports the markers the whole estate carries. -#}
{%- macro dpf_evolve_rebaseline_status(payload=none) -%}
    {%- if execute -%}
        {%- set pending = dpf_evolution_pending_relation() -%}
        {%- set existing = adapter.get_relation(
            database=pending.database, schema=pending.schema, identifier=pending.identifier) -%}
        {%- set entries = [] -%}
        {%- if existing is not none -%}
            {%- set rows = run_query(
                "select entity, domain_name, previous_basis_version, basis_version from "
                ~ pending ~ " order by entity") -%}
            {%- for row in rows.rows -%}
                {%- do entries.append({
                    "entity": row[0],
                    "domain": row[1],
                    "previous_basis_version": row[2],
                    "basis_version": row[3]}) -%}
            {%- endfor -%}
        {%- endif -%}
        {%- do log("DPF_REBASELINE_RESULT=" ~ tojson({
            "operation": "status", "pending": entries}), info=True) -%}
    {%- endif -%}
{%- endmacro -%}

{#- ------------------------------------------------------------------------------
    Phase two: the rewrite, the parity spot check and the audit row.
    ------------------------------------------------------------------------------ -#}
{%- macro dpf_evolve_rebaseline_rewrite(payload) -%}
    {%- if execute -%}
        {%- do dpf_ensure_evolution_relations() -%}
        {%- set outcome = dpf_rebaseline_migrate(payload) -%}
        {%- set audit = dpf_evolution_audit_relation() -%}
        {#- One audit row per entity and new basis version: a re-run replaces its own row,
            so a converging re-run stays one act in the audit. -#}
        {%- do run_query(
            "delete from " ~ audit ~ " where entity = '" ~ payload['entity'] ~ "'"
            ~ " and new_basis_version = " ~ payload['basis_version']) -%}
        {%- do run_query(
            "insert into " ~ audit ~ " (entity, old_basis_version, new_basis_version, rows_rewritten, migrated_at)"
            ~ " select '" ~ payload['entity'] ~ "', " ~ payload['previous_basis_version'] ~ ", "
            ~ payload['basis_version'] ~ ", " ~ outcome['rows_rewritten'] ~ ", "
            ~ dbt.current_timestamp()) -%}
        {%- do adapter.commit() -%}
        {%- do log("DPF_REBASELINE_RESULT=" ~ tojson(outcome), info=True) -%}
    {%- endif -%}
{%- endmacro -%}

{%- macro dpf_rebaseline_migrate(payload) -%}
    {%- do return(adapter.dispatch('dpf_rebaseline_migrate')(payload)) -%}
{%- endmacro -%}

{#- The arm every other target resolves to. A re-baseline rewrites stored history, so a
    target with no arm of its own stops with a named error. -#}
{%- macro default__dpf_rebaseline_migrate(payload) -%}
    {%- do exceptions.raise_compiler_error(
        "estate evolution: the re-baseline migration carries no arm for target type '"
        ~ target.type ~ "'; the executed arm is DuckDB and the authored arm is Snowflake") -%}
{%- endmacro -%}

{#- The satellite relations an entity's re-baseline touches, resolved through ref() so no
    relation name is built by hand, with the matching stage relation beside each one: the
    stage relation is where a missing satellite column takes its physical type from. -#}
{%- macro dpf_rebaseline_targets(payload) -%}
    {%- set targets = [] -%}
    {%- for pair in payload['satellites'] -%}
        {%- set declared = ref(pair['satellite']) -%}
        {%- set satellite = adapter.get_relation(
            database=declared.database, schema=declared.schema, identifier=declared.identifier) -%}
        {%- if satellite is not none -%}
            {%- set staged = ref(pair['stage']) -%}
            {%- do targets.append({
                "name": pair['satellite'],
                "relation": satellite,
                "stage": adapter.get_relation(
                    database=staged.database, schema=staged.schema, identifier=staged.identifier)}) -%}
        {%- endif -%}
    {%- endfor -%}
    {%- do return(targets) -%}
{%- endmacro -%}

{#- The column additions a re-baseline needs: every basis column the satellite does not
    carry yet, typed from the stage relation where that relation carries the column. The
    caller checks presence first, so running the additions twice adds nothing. -#}
{%- macro dpf_rebaseline_column_additions(target, basis) -%}
    {%- set present = [] -%}
    {%- for column in adapter.get_columns_in_relation(target['relation']) -%}
        {%- do present.append(column.name | lower) -%}
    {%- endfor -%}
    {%- set staged_types = {} -%}
    {%- if target['stage'] is not none -%}
        {%- for column in adapter.get_columns_in_relation(target['stage']) -%}
            {%- do staged_types.update({column.name | lower: column.data_type}) -%}
        {%- endfor -%}
    {%- endif -%}
    {%- set additions = [] -%}
    {%- for column in basis -%}
        {%- if column | lower not in present -%}
            {%- do additions.append({
                "column": column,
                "data_type": staged_types.get(column | lower, dbt.type_string())}) -%}
        {%- endif -%}
    {%- endfor -%}
    {%- do return(additions) -%}
{%- endmacro -%}

{#- The bounded golden-hash parity spot check: stored fingerprints against the hash
    automate_dv.hash builds over the stored payload under the new basis. -#}
{%- macro dpf_rebaseline_spot_check(target, hashdiff, expression, sample) -%}
    {%- set query -%}
        select count(*) as mismatches from (
            select {{ hashdiff }} as stored, ({{ expression }}) as recomputed
            from {{ target['relation'] }}
            limit {{ sample }}
        ) as spot
        where spot.stored is distinct from spot.recomputed
    {%- endset -%}
    {%- set result = run_query(query) -%}
    {%- set mismatches = result.rows[0][0] | int -%}
    {%- if mismatches > 0 -%}
        {%- do exceptions.raise_compiler_error(
            "estate evolution: the golden-hash parity spot check found " ~ mismatches
            ~ " row(s) in " ~ target['name'] ~ " whose stored hashdiff differs from the"
            ~ " hash of their stored payload under the new basis") -%}
    {%- endif -%}
{%- endmacro -%}

{%- macro duckdb__dpf_rebaseline_migrate(payload) -%}
    {#- DuckDB carries no explicit transaction here: each statement is idempotent on its
        own, so an interrupted run leaves a partially rewritten entity that the next run
        finishes, and the recompute reads only the stored payload. -#}
    {%- set basis = payload['basis'] -%}
    {%- set hashdiff = payload['hashdiff'] -%}
    {%- set sample = payload.get('spot_check_rows', 1000) -%}
    {%- set expression = dpf_hashdiff_expression(basis) -%}
    {%- set rewritten = {} -%}
    {%- set added = {} -%}
    {%- set total = namespace(rows=0) -%}
    {%- for target in dpf_rebaseline_targets(payload) -%}
        {%- set additions = dpf_rebaseline_column_additions(target, basis) -%}
        {%- for addition in additions -%}
            {%- do run_query(
                "alter table " ~ target['relation'] ~ " add column "
                ~ adapter.quote(addition['column']) ~ " " ~ addition['data_type']) -%}
        {%- endfor -%}
        {%- do added.update({target['name']: additions | map(attribute='column') | list}) -%}
        {%- set counted = run_query(
            "select count(*) from " ~ target['relation'] ~ " where " ~ hashdiff
            ~ " is distinct from (" ~ expression ~ ")") -%}
        {%- set rows = counted.rows[0][0] | int -%}
        {%- if rows > 0 -%}
            {%- do run_query(
                "update " ~ target['relation'] ~ " set " ~ hashdiff ~ " = (" ~ expression ~ ")"
                ~ " where " ~ hashdiff ~ " is distinct from (" ~ expression ~ ")") -%}
        {%- endif -%}
        {%- do dpf_rebaseline_spot_check(target, hashdiff, expression, sample) -%}
        {#- One satellite, one commit: an interrupted rewrite leaves the satellites it
            finished in their new fingerprints and the next run finishes the rest. -#}
        {%- do adapter.commit() -%}
        {%- do rewritten.update({target['name']: rows}) -%}
        {%- set total.rows = total.rows + rows -%}
    {%- endfor -%}
    {%- do return({
        "operation": "rewrite",
        "adapter": "duckdb",
        "entity": payload['entity'],
        "domain": payload['domain'],
        "basis_version": payload['basis_version'],
        "previous_basis_version": payload['previous_basis_version'],
        "rows_rewritten": total.rows,
        "per_satellite": rewritten,
        "columns_added": added}) -%}
{%- endmacro -%}

{%- macro snowflake__dpf_rebaseline_migrate(payload) -%}
    {#- Snowflake DDL autocommits, so the idempotent column additions run outside the
        explicit transaction; the per-entity recompute runs inside one explicit BEGIN and
        COMMIT, so an interrupted rewrite leaves the entity's fingerprints whole. -#}
    {%- set basis = payload['basis'] -%}
    {%- set hashdiff = payload['hashdiff'] -%}
    {%- set sample = payload.get('spot_check_rows', 1000) -%}
    {%- set expression = dpf_hashdiff_expression(basis) -%}
    {%- set targets = dpf_rebaseline_targets(payload) -%}
    {%- set rewritten = {} -%}
    {%- set added = {} -%}
    {%- set total = namespace(rows=0) -%}
    {%- for target in targets -%}
        {%- set additions = dpf_rebaseline_column_additions(target, basis) -%}
        {%- for addition in additions -%}
            {%- do run_query(
                "alter table " ~ target['relation'] ~ " add column "
                ~ adapter.quote(addition['column']) ~ " " ~ addition['data_type']) -%}
        {%- endfor -%}
        {%- do added.update({target['name']: additions | map(attribute='column') | list}) -%}
    {%- endfor -%}
    {%- do run_query("begin") -%}
    {%- for target in targets -%}
        {%- set counted = run_query(
            "select count(*) from " ~ target['relation'] ~ " where " ~ hashdiff
            ~ " is distinct from (" ~ expression ~ ")") -%}
        {%- set rows = counted.rows[0][0] | int -%}
        {%- if rows > 0 -%}
            {%- do run_query(
                "update " ~ target['relation'] ~ " set " ~ hashdiff ~ " = (" ~ expression ~ ")"
                ~ " where " ~ hashdiff ~ " is distinct from (" ~ expression ~ ")") -%}
        {%- endif -%}
        {%- do rewritten.update({target['name']: rows}) -%}
        {%- set total.rows = total.rows + rows -%}
    {%- endfor -%}
    {%- do run_query("commit") -%}
    {%- for target in targets -%}
        {%- do dpf_rebaseline_spot_check(target, hashdiff, expression, sample) -%}
    {%- endfor -%}
    {%- do return({
        "operation": "rewrite",
        "adapter": "snowflake",
        "entity": payload['entity'],
        "domain": payload['domain'],
        "basis_version": payload['basis_version'],
        "previous_basis_version": payload['previous_basis_version'],
        "rows_rewritten": total.rows,
        "per_satellite": rewritten,
        "columns_added": added}) -%}
{%- endmacro -%}

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
