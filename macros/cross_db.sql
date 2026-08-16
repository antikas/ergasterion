{#-
  Cross-database (adapter-dispatch) primitives.

  This is the ONE sanctioned home for dialect-specific SQL. Every BigQuery-only
  construct the factory used to hand-write (safe_cast + int64/float64 types, the
  regexp_* family with raw-string literals, to_hex(md5(...))) is routed through
  these macros so declarations and models stay dialect-free. The dialect lint
  (ergasterion/dialect_lint.py) never scans macros/, precisely because dialect-specific
  text is legitimate here and nowhere else.

  Adapters covered explicitly: BigQuery (the native default), Snowflake, and
  DuckDB. The default__ implementation is the BigQuery form; snowflake__ and
  duckdb__ override where they diverge. Of the 20 cross-db macros, 13 need
  DuckDB arms; the other seven (dpf_type, dpf_safe_divide, dpf_date_key,
  dpf_array, dpf_string_agg, dpf_array_agg_distinct, and dpf_array_length) are
  already neutral or valid on DuckDB.
-#}

{#- Typed, dialect-free safe cast. `type_token` is one of:
    int | float | numeric | date | string. Renders BigQuery safe_cast(...) via dbt's
    own cross-db safe_cast + type macros; Snowflake gets a dedicated override (see
    snowflake__dpf_safe_cast below) because dbt-snowflake's `try_cast` compiles straight
    to a raw Snowflake TRY_CAST(field AS type), and Snowflake's TRY_CAST only accepts a
    VARCHAR source -- `TRY_CAST(a_date_column AS ...)` or `TRY_CAST(a_number_column AS
    ...)` is a SQL compilation error ("TRY_CAST cannot be used with arguments of types
    DATE and VARCHAR..."), even though the identical BigQuery SAFE_CAST call accepts any
    source type. The wrapper stringifies the source before attempting the conversion. -#}
{% macro dpf_safe_cast(expr, type_token) -%}
    {{ return(adapter.dispatch('dpf_safe_cast', 'ergasterion')(expr, type_token)) }}
{%- endmacro %}

{% macro default__dpf_safe_cast(expr, type_token) -%}
    {{ return(dbt.safe_cast(expr, dpf_type(type_token))) }}
{%- endmacro %}

{#- Stringify first via a plain (non-try) CAST -- casting any concrete type (DATE,
    NUMBER, ...) to VARCHAR always succeeds on Snowflake, it never throws -- then
    TRY_CAST that guaranteed-VARCHAR value to the real target type, which is exactly
    the source type TRY_CAST requires. Preserves safe-cast semantics (NULL on a bad
    conversion, e.g. numeric overflow into a narrower precision) while satisfying
    Snowflake's TRY_CAST source-type restriction regardless of the caller's actual
    column type. -#}
{% macro snowflake__dpf_safe_cast(expr, type_token) -%}
    try_cast(to_varchar({{ expr }}) as {{ dpf_type(type_token) }})
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

{% macro snowflake__dpf_json_cast(expr) -%}
    case
        when is_varchar(to_variant({{ expr }}))
            then try_parse_json(to_varchar({{ expr }}))
        else to_variant({{ expr }})
    end
{%- endmacro %}

{% macro duckdb__dpf_json_cast(expr) -%}
    case
        when typeof({{ expr }}) = 'VARCHAR'
            then try_cast({{ expr }} as json)
        else to_json({{ expr }})
    end
{%- endmacro %}

{#- Map a dialect-free type token to the adapter's concrete type name. -#}
{% macro dpf_type(type_token) -%}
    {%- if type_token == 'int' -%}
        {{ return(dbt.type_int()) }}
    {%- elif type_token == 'float' -%}
        {{ return(dbt.type_float()) }}
    {%- elif type_token == 'numeric' -%}
        {{ return(dbt.type_numeric()) }}
    {%- elif type_token == 'string' -%}
        {{ return(dbt.type_string()) }}
    {%- elif type_token == 'date' -%}
        {#- DATE is ANSI-standard and identical on BigQuery, Snowflake, and DuckDB. -#}
        {{ return('date') }}
    {%- elif type_token == 'boolean' -%}
        {#- BOOLEAN is accepted by all three adapters; it is an alias of BOOL on BigQuery. -#}
        {{ return('boolean') }}
    {%- else -%}
        {{ exceptions.raise_compiler_error("dpf_type: unknown type token '" ~ type_token ~ "' (want int|float|numeric|date|string)") }}
    {%- endif -%}
{%- endmacro %}


{#- Replace all matches of `pattern` in `subject` with `replacement`.
    3-arg regexp_replace is identical on BigQuery and Snowflake; the point of the
    macro is to keep patterns as plain quoted literals (no BigQuery r'...' raw
    strings) and to centralise the call. `pattern` and `replacement` are passed as
    already-quoted SQL string literals. -#}
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

{#- IMPORTANT: use regexp_instr(...) > 0, NOT regexp_like. Snowflake regexp_like is
    FULLY ANCHORED (implicit ^...$ whole-string match), whereas BigQuery
    regexp_contains is an UNANCHORED substring match. regexp_instr returns the
    1-based position of the first match (0 = no match), so `> 0` is the correct
    unanchored equivalent. Getting this wrong silently diverges the golden-record
    identity between adapters (e.g. normalise_prefixed_id on 'CPS12345EU'). -#}
{% macro snowflake__dpf_regexp_contains(subject, pattern) -%}
    regexp_instr({{ subject }}, {{ pattern }}) > 0
{%- endmacro %}

{% macro duckdb__dpf_regexp_contains(subject, pattern) -%}
    regexp_matches({{ subject }}, {{ pattern }})
{%- endmacro %}


{#- Extract the first capturing group of `pattern` from `subject`.
    BigQuery regexp_extract returns capturing-group 1 when the pattern has a group;
    Snowflake regexp_substr needs the extract flag ('e') and an explicit group index.
    DuckDB's regexp_extract_all preserves the distinction between a nonparticipating
    optional group (NULL) and a participating empty group (''). -#}
{% macro dpf_regexp_extract(subject, pattern) -%}
    {{ return(adapter.dispatch('dpf_regexp_extract', 'ergasterion')(subject, pattern)) }}
{%- endmacro %}

{% macro default__dpf_regexp_extract(subject, pattern) -%}
    regexp_extract({{ subject }}, {{ pattern }})
{%- endmacro %}

{% macro snowflake__dpf_regexp_extract(subject, pattern) -%}
    regexp_substr({{ subject }}, {{ pattern }}, 1, 1, 'e', 1)
{%- endmacro %}

{% macro duckdb__dpf_regexp_extract(subject, pattern) -%}
    list_extract(regexp_extract_all({{ subject }}, {{ pattern }}, 1), 1)
{%- endmacro %}


{#- Lower-case hex of the MD5 of `expr`. BigQuery md5() returns BYTES and needs
    to_hex(); Snowflake md5() already returns the lower-case hex varchar. -#}
{% macro dpf_hash_hex(expr) -%}
    {{ return(adapter.dispatch('dpf_hash_hex', 'ergasterion')(expr)) }}
{%- endmacro %}

{% macro default__dpf_hash_hex(expr) -%}
    to_hex(md5({{ expr }}))
{%- endmacro %}

{% macro snowflake__dpf_hash_hex(expr) -%}
    md5({{ expr }})
{%- endmacro %}

{% macro duckdb__dpf_hash_hex(expr) -%}
    md5({{ expr }})
{%- endmacro %}


{#- Array construction from a list of already-quoted SQL element expressions.
    BigQuery uses the [a, b, ...] literal; Snowflake uses array_construct(a, b, ...). -#}
{% macro dpf_array(elements) -%}
    {{ return(adapter.dispatch('dpf_array', 'ergasterion')(elements)) }}
{%- endmacro %}

{% macro default__dpf_array(elements) -%}
    [{{ elements | join(', ') }}]
{%- endmacro %}

{% macro snowflake__dpf_array(elements) -%}
    array_construct({{ elements | join(', ') }})
{%- endmacro %}


{#- Typed empty array. BigQuery needs an explicit element type (cast([] as array<T>));
    Snowflake arrays are variant-typed, so array_construct() is an empty array. -#}
{% macro dpf_empty_array(type_token='string') -%}
    {{ return(adapter.dispatch('dpf_empty_array', 'ergasterion')(type_token)) }}
{%- endmacro %}

{% macro default__dpf_empty_array(type_token) -%}
    cast([] as array<{{ dpf_type(type_token) }}>)
{%- endmacro %}

{% macro snowflake__dpf_empty_array(type_token) -%}
    array_construct()
{%- endmacro %}

{% macro duckdb__dpf_empty_array(type_token) -%}
    cast([] as {{ dpf_type(type_token) }}[])
{%- endmacro %}


{#- Serialise an object literal to a JSON string. `pairs` is a list of [key, expr]
    two-item lists: `key` is the (unquoted) JSON key, `expr` is an already-rendered
    SQL value expression. BigQuery builds a STRUCT and to_json_string()s it; Snowflake
    uses object_construct_keep_null(...) so null values are preserved as JSON null,
    matching BigQuery's semantics, then to_json()s the object to a VARCHAR. -#}
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

{% macro snowflake__dpf_to_json_object(pairs) -%}
    to_json(object_construct_keep_null(
        {%- for key, expr in pairs %}
        '{{ key }}', {{ expr }}{{ "," if not loop.last }}
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


{#- Null-safe division. Pure ANSI across all three adapters -- BigQuery's safe_divide(a, b) is
    a / nullif(b, 0). Kept as a macro so the intent (and the divide-by-zero guard) is
    declared once and no hand-authored model reaches for BigQuery safe_divide. -#}
{% macro dpf_safe_divide(numerator, denominator) -%}
    ({{ numerator }}) / nullif({{ denominator }}, 0)
{%- endmacro %}


{#- Ordered, optionally-distinct string aggregation. `delimiter` is an already-quoted
    SQL string literal. BigQuery: string_agg([distinct] expr, delim [order by ...]).
    Snowflake: listagg([distinct] expr, delim) within group (order by ...). -#}
{% macro dpf_string_agg(expr, delimiter, order_by=none, distinct=false) -%}
    {{ return(adapter.dispatch('dpf_string_agg', 'ergasterion')(expr, delimiter, order_by, distinct)) }}
{%- endmacro %}

{% macro default__dpf_string_agg(expr, delimiter, order_by, distinct) -%}
    string_agg({{ 'distinct ' if distinct else '' }}{{ expr }}, {{ delimiter }}{{ ' order by ' ~ order_by if order_by else '' }})
{%- endmacro %}

{% macro snowflake__dpf_string_agg(expr, delimiter, order_by, distinct) -%}
    listagg({{ 'distinct ' if distinct else '' }}{{ expr }}, {{ delimiter }}){{ ' within group (order by ' ~ order_by ~ ')' if order_by else '' }}
{%- endmacro %}


{#- Integer YYYYMMDD date key. Arithmetic on EXTRACT is ANSI and identical on all three
    adapters, avoiding BigQuery's format_date(...) + cast-to-int64. -#}
{% macro dpf_date_key(date_expr) -%}
    (extract(year from {{ date_expr }}) * 10000 + extract(month from {{ date_expr }}) * 100 + extract(day from {{ date_expr }}))
{%- endmacro %}


{#- Truncate a DATE to a calendar boundary, returning a DATE on all three adapters.
    BigQuery: date_trunc(date, part); Snowflake: date_trunc('part', date). `datepart`
    is one of day|month|quarter|year (unquoted token). -#}
{% macro dpf_date_trunc(datepart, date_expr) -%}
    {{ return(adapter.dispatch('dpf_date_trunc', 'ergasterion')(datepart, date_expr)) }}
{%- endmacro %}

{% macro default__dpf_date_trunc(datepart, date_expr) -%}
    date_trunc({{ date_expr }}, {{ datepart }})
{%- endmacro %}

{% macro snowflake__dpf_date_trunc(datepart, date_expr) -%}
    date_trunc('{{ datepart }}', {{ date_expr }})
{%- endmacro %}

{% macro duckdb__dpf_date_trunc(datepart, date_expr) -%}
    cast(date_trunc('{{ datepart }}', {{ date_expr }}) as date)
{%- endmacro %}


{#- A contiguous DATE series between two scalar date expressions, rendered as a
    stand-alone relation with a single column `date_day`. BigQuery generates the
    array with generate_date_array + unnest; Snowflake enumerates rows via a
    generator table and dateadd(), then filters to the closed [start, end] interval.
    `start_expr` / `end_expr` are already-rendered SQL scalar date expressions
    (e.g. correlated `(select ... )` subqueries). -#}
{% macro dpf_date_series(start_expr, end_expr) -%}
    {{ return(adapter.dispatch('dpf_date_series', 'ergasterion')(start_expr, end_expr)) }}
{%- endmacro %}

{% macro default__dpf_date_series(start_expr, end_expr) -%}
    select date_day
    from unnest(generate_date_array({{ start_expr }}, {{ end_expr }})) as date_day
{%- endmacro %}

{% macro snowflake__dpf_date_series(start_expr, end_expr) -%}
    select dateadd(day, seq_num, {{ start_expr }}) as date_day
    from (
        select row_number() over (order by null) - 1 as seq_num
        from table(generator(rowcount => 100000))
    ) as _date_gen
    where dateadd(day, seq_num, {{ start_expr }}) <= {{ end_expr }}
{%- endmacro %}

{% macro duckdb__dpf_date_series(start_expr, end_expr) -%}
    select cast(date_day as date) as date_day
    from generate_series({{ start_expr }}, {{ end_expr }}, interval 1 day) as _date_gen(date_day)
{%- endmacro %}


{#- Levenshtein edit distance between two strings. All three adapters have a native
    function (BigQuery EDIT_DISTANCE, Snowflake EDITDISTANCE, DuckDB levenshtein) --
    the same character insert/delete/substitute semantics and argument order, with
    only the function name differing. -#}
{% macro dpf_edit_distance(expr_a, expr_b) -%}
    {{ return(adapter.dispatch('dpf_edit_distance', 'ergasterion')(expr_a, expr_b)) }}
{%- endmacro %}

{% macro default__dpf_edit_distance(expr_a, expr_b) -%}
    edit_distance({{ expr_a }}, {{ expr_b }})
{%- endmacro %}

{% macro snowflake__dpf_edit_distance(expr_a, expr_b) -%}
    editdistance({{ expr_a }}, {{ expr_b }})
{%- endmacro %}

{% macro duckdb__dpf_edit_distance(expr_a, expr_b) -%}
    levenshtein({{ expr_a }}, {{ expr_b }})
{%- endmacro %}


{#- Whole-day difference between two DATE expressions, as `date_a - date_b`
    (positive when date_a is later). BigQuery date_diff(date1, date2, day) already
    returns date1 - date2; Snowflake datediff(day, expr1, expr2) returns
    expr2 - expr1, so the two arguments are swapped in the override to preserve
    the same date_a-minus-date_b sign convention on all three adapters. -#}
{% macro dpf_date_diff_days(date_a, date_b) -%}
    {{ return(adapter.dispatch('dpf_date_diff_days', 'ergasterion')(date_a, date_b)) }}
{%- endmacro %}

{% macro default__dpf_date_diff_days(date_a, date_b) -%}
    date_diff({{ date_a }}, {{ date_b }}, day)
{%- endmacro %}

{% macro snowflake__dpf_date_diff_days(date_a, date_b) -%}
    datediff(day, {{ date_b }}, {{ date_a }})
{%- endmacro %}

{% macro duckdb__dpf_date_diff_days(date_a, date_b) -%}
    {{ date_a }} - {{ date_b }}
{%- endmacro %}


{#- Distinct-valued array aggregation, one array per GROUP BY group -- the
    set-union counterpart to dpf_string_agg. Used to build cross-source unions
    from a normalised per-source-row
    model without collapsing to a single survivorship winner. All three adapters
    support DISTINCT combined with ORDER BY inside ARRAY_AGG; ordering by the
    aggregated expression itself keeps element order deterministic across
    repeated builds (BigQuery: array_agg(distinct expr order by expr); Snowflake:
    array_agg(distinct expr) within group (order by expr) -- Snowflake attaches
    the ORDER BY as a separate WITHIN GROUP clause rather than inside the
    argument list). -#}
{% macro dpf_array_agg_distinct(expr) -%}
    {{ return(adapter.dispatch('dpf_array_agg_distinct', 'ergasterion')(expr)) }}
{%- endmacro %}

{% macro default__dpf_array_agg_distinct(expr) -%}
    array_agg(distinct {{ expr }} order by {{ expr }})
{%- endmacro %}

{% macro snowflake__dpf_array_agg_distinct(expr) -%}
    array_agg(distinct {{ expr }}) within group (order by {{ expr }})
{%- endmacro %}


{#- Aggregate one JSON object per GROUP BY group from key/value pairs -- one pair
    contributed per input row -- the map-building counterpart to
    dpf_array_agg_distinct. Used to build per-source maps from a normalised per-source-row model,
    covering every contributing source rather than a single winning source's id.
    BigQuery has no native OBJECT_AGG; JSON_OBJECT's two-parallel-array overload
    (array_agg(key) order by key, array_agg(value) order by key) builds the same
    shape, with both arrays independently ordered by the identical deterministic
    key expression so they stay index-aligned pair-for-pair. Snowflake OBJECT_AGG
    is a native key/value aggregate that needs no such pairing trick. All three
    branches render the result as a JSON STRING (to_json_string(...) /
    to_json(...) / cast(to_json(...) as varchar)), matching the STRING shape
    dpf_to_json_object already returns
    for the static-key case -- callers read either back with the same
    string-level (e.g. LIKE) or platform JSON-parse checks. -#}
{% macro dpf_map_agg(key_expr, value_expr) -%}
    {{ return(adapter.dispatch('dpf_map_agg', 'ergasterion')(key_expr, value_expr)) }}
{%- endmacro %}

{% macro default__dpf_map_agg(key_expr, value_expr) -%}
    to_json_string(json_object(
        array_agg({{ key_expr }} order by {{ key_expr }}),
        array_agg({{ value_expr }} order by {{ key_expr }})
    ))
{%- endmacro %}

{% macro snowflake__dpf_map_agg(key_expr, value_expr) -%}
    {#- OBJECT_AGG requires a VARIANT value (VARCHAR value -> 001044/42P13 at
        runtime, parse-green: live CI finding 2026-07-10) -- wrap explicitly. -#}
    to_json(object_agg({{ key_expr }}, to_variant({{ value_expr }})))
{%- endmacro %}

{% macro duckdb__dpf_map_agg(key_expr, value_expr) -%}
    cast(to_json(map(
        list({{ key_expr }} order by {{ key_expr }}),
        list({{ value_expr }} order by {{ key_expr }})
    )) as varchar)
{%- endmacro %}


{#- Number of elements in an array. BigQuery array_length(); Snowflake
    array_size() -- same semantics, different name, so
    expected-value tests can assert alias-union cardinality without a
    dialect-specific function leaking into tests/. -#}
{% macro dpf_array_length(expr) -%}
    {{ return(adapter.dispatch('dpf_array_length', 'ergasterion')(expr)) }}
{%- endmacro %}

{% macro default__dpf_array_length(expr) -%}
    array_length({{ expr }})
{%- endmacro %}

{% macro snowflake__dpf_array_length(expr) -%}
    array_size({{ expr }})
{%- endmacro %}
