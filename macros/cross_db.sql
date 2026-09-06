{#-
  Cross-database (adapter-dispatch) primitives.

  This is the ONE sanctioned home for dialect-specific SQL. Every BigQuery-only
  construct the factory used to hand-write (safe_cast + int64/float64 types, the
  regexp_* family with raw-string literals, to_hex(md5(...))) is routed through
  these macros so declarations and models stay dialect-free. The dialect lint
  (ergasterion/dialect_lint.py) never scans macros/, precisely because dialect-specific
  text is legitimate here and nowhere else.

  Two adapters are covered: BigQuery, whose form is the default__ implementation,
  and DuckDB, which overrides where it diverges. Of the 21 cross-db macros, 13 need
  DuckDB arms; the other eight (dpf_type, dpf_decimal_type, dpf_safe_divide,
  dpf_date_key, dpf_array, dpf_string_agg, dpf_array_agg_distinct and
  dpf_array_length) are already neutral or valid on DuckDB.
-#}

{#- Typed, dialect-free safe cast. `type_token` is one of:
    int | float | numeric | timestamp | date | string | boolean. Renders the BigQuery
    safe_cast(...) form through dbt's own cross-db safe_cast + type macros; DuckDB
    overrides with its native try_cast. -#}
{% macro dpf_safe_cast(expr, type_token) -%}
    {{ return(adapter.dispatch('dpf_safe_cast', 'ergasterion')(expr, type_token)) }}
{%- endmacro %}

{% macro default__dpf_safe_cast(expr, type_token) -%}
    {{ return(dbt.safe_cast(expr, dpf_type(type_token))) }}
{%- endmacro %}

{% macro duckdb__dpf_safe_cast(expr, type_token) -%}
    try_cast({{ expr }} as {{ dpf_type(type_token) }})
{%- endmacro %}


{#- Normalise a JSON payload held either as text (the usual ODCS representation) or
    as a native semi-structured value (possible from DDL imports). Text must be parsed
    directly: serialising it first would silently produce a JSON string scalar rather
    than the object/array carried in the text. -#}
{% macro dpf_json_cast(expr) -%}
    {{ return(adapter.dispatch('dpf_json_cast', 'ergasterion')(expr)) }}
{%- endmacro %}

{% macro default__dpf_json_cast(expr) -%}
    case
        when json_type(to_json({{ expr }})) = 'string'
            then safe.parse_json(json_value(to_json({{ expr }})))
        else to_json({{ expr }})
    end
{%- endmacro %}

{% macro duckdb__dpf_json_cast(expr) -%}
    case
        when typeof({{ expr }}) = 'VARCHAR'
            then try_cast({{ expr }} as json)
        else to_json({{ expr }})
    end
{%- endmacro %}

{#- Map a dialect-free type token to the adapter's concrete type name. The token set
    is the one ergasterion/framework/adapters.py declares (NEUTRAL_TYPE_TOKENS) and
    every registered adapter's conventions.yml type_mapping resolves. -#}
{% macro dpf_type(type_token) -%}
    {%- if type_token == 'int' -%}
        {{ return(dbt.type_int()) }}
    {%- elif type_token == 'float' -%}
        {{ return(dbt.type_float()) }}
    {%- elif type_token == 'numeric' -%}
        {{ return(dbt.type_numeric()) }}
    {%- elif type_token == 'string' -%}
        {{ return(dbt.type_string()) }}
    {%- elif type_token == 'timestamp' -%}
        {{ return(dbt.type_timestamp()) }}
    {%- elif type_token == 'date' -%}
        {#- DATE is ANSI-standard and identical on BigQuery and DuckDB. -#}
        {{ return('date') }}
    {%- elif type_token == 'boolean' -%}
        {#- BOOLEAN is accepted by both adapters; it is an alias of BOOL on BigQuery. -#}
        {{ return('boolean') }}
    {%- else -%}
        {{ exceptions.raise_compiler_error("dpf_type: unknown type token '" ~ type_token ~ "' (want int|float|numeric|timestamp|date|string|boolean)") }}
    {%- endif -%}
{%- endmacro %}


{#- The concrete type name for a declared decimal, carrying its precision and scale.
    dpf_type('numeric') deliberately drops both (dbt's type_numeric() resolves each
    adapter's own default), so a declaration that states precision and scale renders
    through this macro instead and keeps them. numeric(p, s) is spelled identically on
    BigQuery and DuckDB, so this macro needs no dispatch. -#}
{% macro dpf_decimal_type(precision, scale) -%}
    numeric({{ precision }}, {{ scale }})
{%- endmacro %}


{#- Replace all matches of `pattern` in `subject` with `replacement`.
    The point of the macro is to keep patterns as plain quoted literals (no BigQuery
    r'...' raw strings) and to centralise the call. `pattern` and `replacement` are
    passed as already-quoted SQL string literals. -#}
{% macro dpf_regexp_replace(subject, pattern, replacement="''") -%}
    {{ return(adapter.dispatch('dpf_regexp_replace', 'ergasterion')(subject, pattern, replacement)) }}
{%- endmacro %}

{% macro default__dpf_regexp_replace(subject, pattern, replacement) -%}
    regexp_replace({{ subject }}, {{ pattern }}, {{ replacement }})
{%- endmacro %}

{% macro duckdb__dpf_regexp_replace(subject, pattern, replacement) -%}
    regexp_replace({{ subject }}, {{ pattern }}, {{ replacement }}, 'g')
{%- endmacro %}


{#- Boolean: does `subject` contain a match for `pattern`. -#}
{% macro dpf_regexp_contains(subject, pattern) -%}
    {{ return(adapter.dispatch('dpf_regexp_contains', 'ergasterion')(subject, pattern)) }}
{%- endmacro %}

{% macro default__dpf_regexp_contains(subject, pattern) -%}
    regexp_contains({{ subject }}, {{ pattern }})
{%- endmacro %}

{% macro duckdb__dpf_regexp_contains(subject, pattern) -%}
    regexp_matches({{ subject }}, {{ pattern }})
{%- endmacro %}


{#- Extract the first capturing group of `pattern` from `subject`.
    BigQuery regexp_extract returns capturing-group 1 when the pattern has a group.
    DuckDB's regexp_extract_all preserves the distinction between a nonparticipating
    optional group (NULL) and a participating empty group (''). -#}
{% macro dpf_regexp_extract(subject, pattern) -%}
    {{ return(adapter.dispatch('dpf_regexp_extract', 'ergasterion')(subject, pattern)) }}
{%- endmacro %}

{% macro default__dpf_regexp_extract(subject, pattern) -%}
    regexp_extract({{ subject }}, {{ pattern }})
{%- endmacro %}

{% macro duckdb__dpf_regexp_extract(subject, pattern) -%}
    list_extract(regexp_extract_all({{ subject }}, {{ pattern }}, 1), 1)
{%- endmacro %}


{#- Lower-case hex of the MD5 of `expr`. BigQuery md5() returns BYTES and needs
    to_hex(); DuckDB md5() already returns the lower-case hex varchar. -#}
{% macro dpf_hash_hex(expr) -%}
    {{ return(adapter.dispatch('dpf_hash_hex', 'ergasterion')(expr)) }}
{%- endmacro %}

{% macro default__dpf_hash_hex(expr) -%}
    to_hex(md5({{ expr }}))
{%- endmacro %}

{% macro duckdb__dpf_hash_hex(expr) -%}
    md5({{ expr }})
{%- endmacro %}


{#- Array construction from a list of already-quoted SQL element expressions.
    The [a, b, ...] literal is valid on both adapters. -#}
{% macro dpf_array(elements) -%}
    {{ return(adapter.dispatch('dpf_array', 'ergasterion')(elements)) }}
{%- endmacro %}

{% macro default__dpf_array(elements) -%}
    [{{ elements | join(', ') }}]
{%- endmacro %}


{#- Typed empty array. BigQuery needs an explicit element type (cast([] as array<T>));
    DuckDB spells the same idea as cast([] as T[]). -#}
{% macro dpf_empty_array(type_token='string') -%}
    {{ return(adapter.dispatch('dpf_empty_array', 'ergasterion')(type_token)) }}
{%- endmacro %}

{% macro default__dpf_empty_array(type_token) -%}
    cast([] as array<{{ dpf_type(type_token) }}>)
{%- endmacro %}

{% macro duckdb__dpf_empty_array(type_token) -%}
    cast([] as {{ dpf_type(type_token) }}[])
{%- endmacro %}


{#- Serialise an object literal to a JSON string. `pairs` is a list of [key, expr]
    two-item lists: `key` is the (unquoted) JSON key, `expr` is an already-rendered
    SQL value expression. BigQuery builds a STRUCT and to_json_string()s it; DuckDB
    builds the object with json_object() and renders it as varchar. -#}
{% macro dpf_to_json_object(pairs) -%}
    {{ return(adapter.dispatch('dpf_to_json_object', 'ergasterion')(pairs)) }}
{%- endmacro %}

{% macro default__dpf_to_json_object(pairs) -%}
    to_json_string(struct(
        {%- for key, expr in pairs %}
        {{ expr }} as {{ key }}{{ "," if not loop.last }}
        {%- endfor %}
    ))
{%- endmacro %}

{% macro duckdb__dpf_to_json_object(pairs) -%}
    cast(json_object(
        {%- for key, expr in pairs %}
        '{{ key }}', {{ expr }}{{ "," if not loop.last }}
        {%- endfor %}
    ) as varchar)
{%- endmacro %}


{#- Null-safe division. Pure ANSI across both adapters -- BigQuery's safe_divide(a, b) is
    a / nullif(b, 0). Kept as a macro so the intent (and the divide-by-zero guard) is
    declared once and no hand-authored model reaches for BigQuery safe_divide. -#}
{% macro dpf_safe_divide(numerator, denominator) -%}
    ({{ numerator }}) / nullif({{ denominator }}, 0)
{%- endmacro %}


{#- Ordered, optionally-distinct string aggregation. `delimiter` is an already-quoted
    SQL string literal. string_agg([distinct] expr, delim [order by ...]) is valid on
    both adapters. -#}
{% macro dpf_string_agg(expr, delimiter, order_by=none, distinct=false) -%}
    {{ return(adapter.dispatch('dpf_string_agg', 'ergasterion')(expr, delimiter, order_by, distinct)) }}
{%- endmacro %}

{% macro default__dpf_string_agg(expr, delimiter, order_by, distinct) -%}
    string_agg({{ 'distinct ' if distinct else '' }}{{ expr }}, {{ delimiter }}{{ ' order by ' ~ order_by if order_by else '' }})
{%- endmacro %}


{#- Integer YYYYMMDD date key. Arithmetic on EXTRACT is ANSI and identical on both
    adapters, avoiding BigQuery's format_date(...) + cast-to-int64. -#}
{% macro dpf_date_key(date_expr) -%}
    (extract(year from {{ date_expr }}) * 10000 + extract(month from {{ date_expr }}) * 100 + extract(day from {{ date_expr }}))
{%- endmacro %}


{#- Truncate a DATE to a calendar boundary, returning a DATE on both adapters.
    BigQuery: date_trunc(date, part); DuckDB: date_trunc('part', date). `datepart`
    is one of day|month|quarter|year (unquoted token). -#}
{% macro dpf_date_trunc(datepart, date_expr) -%}
    {{ return(adapter.dispatch('dpf_date_trunc', 'ergasterion')(datepart, date_expr)) }}
{%- endmacro %}

{% macro default__dpf_date_trunc(datepart, date_expr) -%}
    date_trunc({{ date_expr }}, {{ datepart }})
{%- endmacro %}

{% macro duckdb__dpf_date_trunc(datepart, date_expr) -%}
    cast(date_trunc('{{ datepart }}', {{ date_expr }}) as date)
{%- endmacro %}


{#- A contiguous DATE series between two scalar date expressions, rendered as a
    stand-alone relation with a single column `date_day`. BigQuery generates the
    array with generate_date_array + unnest; DuckDB uses generate_series over a day
    interval. `start_expr` / `end_expr` are already-rendered SQL scalar date
    expressions (for example correlated `(select ... )` subqueries). -#}
{% macro dpf_date_series(start_expr, end_expr) -%}
    {{ return(adapter.dispatch('dpf_date_series', 'ergasterion')(start_expr, end_expr)) }}
{%- endmacro %}

{% macro default__dpf_date_series(start_expr, end_expr) -%}
    select date_day
    from unnest(generate_date_array({{ start_expr }}, {{ end_expr }})) as date_day
{%- endmacro %}

{% macro duckdb__dpf_date_series(start_expr, end_expr) -%}
    select cast(date_day as date) as date_day
    from generate_series({{ start_expr }}, {{ end_expr }}, interval 1 day) as _date_gen(date_day)
{%- endmacro %}


{#- Levenshtein edit distance between two strings. Both adapters have a native
    function (BigQuery EDIT_DISTANCE, DuckDB levenshtein) -- the same character
    insert/delete/substitute semantics and argument order, with only the function
    name differing. -#}
{% macro dpf_edit_distance(expr_a, expr_b) -%}
    {{ return(adapter.dispatch('dpf_edit_distance', 'ergasterion')(expr_a, expr_b)) }}
{%- endmacro %}

{% macro default__dpf_edit_distance(expr_a, expr_b) -%}
    edit_distance({{ expr_a }}, {{ expr_b }})
{%- endmacro %}

{% macro duckdb__dpf_edit_distance(expr_a, expr_b) -%}
    levenshtein({{ expr_a }}, {{ expr_b }})
{%- endmacro %}


{#- Whole-day difference between two DATE expressions, as `date_a - date_b`
    (positive when date_a is later). BigQuery date_diff(date1, date2, day) already
    returns date1 - date2; DuckDB subtracts the two dates directly. -#}
{% macro dpf_date_diff_days(date_a, date_b) -%}
    {{ return(adapter.dispatch('dpf_date_diff_days', 'ergasterion')(date_a, date_b)) }}
{%- endmacro %}

{% macro default__dpf_date_diff_days(date_a, date_b) -%}
    date_diff({{ date_a }}, {{ date_b }}, day)
{%- endmacro %}

{% macro duckdb__dpf_date_diff_days(date_a, date_b) -%}
    {{ date_a }} - {{ date_b }}
{%- endmacro %}


{#- Distinct-valued array aggregation, one array per GROUP BY group -- the
    set-union counterpart to dpf_string_agg. Used to build cross-source unions
    from a normalised per-source-row model without collapsing to a single
    survivorship winner. Both adapters support DISTINCT combined with ORDER BY
    inside ARRAY_AGG; ordering by the aggregated expression itself keeps element
    order deterministic across repeated builds. -#}
{% macro dpf_array_agg_distinct(expr) -%}
    {{ return(adapter.dispatch('dpf_array_agg_distinct', 'ergasterion')(expr)) }}
{%- endmacro %}

{% macro default__dpf_array_agg_distinct(expr) -%}
    array_agg(distinct {{ expr }} order by {{ expr }})
{%- endmacro %}


{#- Aggregate one JSON object per GROUP BY group from key/value pairs -- one pair
    contributed per input row -- the map-building counterpart to
    dpf_array_agg_distinct. Used to build per-source maps from a normalised
    per-source-row model, covering every contributing source rather than a single
    winning source's id. BigQuery has no native OBJECT_AGG; JSON_OBJECT's
    two-parallel-array overload (array_agg(key) order by key, array_agg(value)
    order by key) builds the same shape, with both arrays independently ordered by
    the identical deterministic key expression so they stay index-aligned
    pair-for-pair. Both branches render the result as a JSON STRING, matching the
    STRING shape dpf_to_json_object already returns for the static-key case --
    callers read either back with the same string-level (for example LIKE) or
    platform JSON-parse checks. -#}
{% macro dpf_map_agg(key_expr, value_expr) -%}
    {{ return(adapter.dispatch('dpf_map_agg', 'ergasterion')(key_expr, value_expr)) }}
{%- endmacro %}

{% macro default__dpf_map_agg(key_expr, value_expr) -%}
    to_json_string(json_object(
        array_agg({{ key_expr }} order by {{ key_expr }}),
        array_agg({{ value_expr }} order by {{ key_expr }})
    ))
{%- endmacro %}

{% macro duckdb__dpf_map_agg(key_expr, value_expr) -%}
    cast(to_json(map(
        list({{ key_expr }} order by {{ key_expr }}),
        list({{ value_expr }} order by {{ key_expr }})
    )) as varchar)
{%- endmacro %}


{#- Number of elements in an array. array_length() is spelled the same way on both
    adapters, so expected-value tests can assert alias-union cardinality without a
    dialect-specific function leaking into tests/. -#}
{% macro dpf_array_length(expr) -%}
    {{ return(adapter.dispatch('dpf_array_length', 'ergasterion')(expr)) }}
{%- endmacro %}

{% macro default__dpf_array_length(expr) -%}
    array_length({{ expr }})
{%- endmacro %}
