{#-
  Business-vault survivorship uses SQL accepted by Snowflake, BigQuery, and DuckDB:
    * per-source "latest row" uses QUALIFY row_number() rather than BigQuery's
      `select * except (...)`;
    * the most_recent strategy picks the winning candidate via a `mr_<attribute>`
      CTE -- a union-all of per-source candidates (value/effective_from/load_datetime/
      source_name/priority_rank) reduced to one row per hub key with QUALIFY
      row_number() -- and the winner is brought into the final row via a plain LEFT
      JOIN on the hub key. A scalar correlated subquery in the SELECT list (the
      previous approach) is rejected by Snowflake when the enclosing model is a VIEW
      (002031: "Unsupported subquery type cannot be evaluated inside VIEW object"); a
      JOIN + window function has no such restriction and runs on all three adapters.
  Determinism: for each attribute the winner is chosen by the SOURCE's business
  effective date (effective_from) DESC, with source_priority order (priority_rank
  ASC) as the deterministic tie-break -- NEVER by load_datetime. load_datetime is
  populated at the staging layer with the bare ANSI `current_timestamp` expression
  (build wall-clock), so
  ordering by it made the winner a thread-scheduling/build-order race rather than a
  data fact; it is retained only as audit metadata exposed as <attribute>__load_datetime.
  The per-source "latest row" CTE likewise picks the business-latest snapshot per
  source (effective_from DESC), not the build-latest. Grain stays one row per hub key
  -- mr_<attribute> is deduplicated to at most one row per hub key before the join.
-#}
{% macro survivorship_golden_record(entity, hub_model, hub_pk, hub_nk, source_satellites, attribute_rules) -%}
{%- set most_recent_count = attribute_rules.values() | selectattr('strategy', 'equalto', 'most_recent') | list | length %}
with hub as (
    select
        {{ hub_pk }},
        {{ hub_nk }},
        load_datetime as hub_load_datetime,
        record_source as hub_record_source
    from {{ ref(hub_model) }}
),

{%- for source in source_satellites %}
current_{{ source.name | lower }} as (
    select sat.*
    from {{ ref(source.model) }} as sat
    qualify row_number() over (
        partition by sat.{{ hub_pk }}, sat.record_source
        order by sat.effective_from desc, sat.{{ 'source_record_id' if 'source_record_id' in attribute_rules else hub_pk.replace('_hk', '_hashdiff') }} asc
    ) = 1
){% if not loop.last or most_recent_count > 0 %},{% endif %}
{%- endfor %}

{%- for attribute, rule in attribute_rules.items() if rule.strategy == 'most_recent' %}
mr_{{ attribute }} as (
    select
        {{ hub_pk }},
        value,
        source_name,
        load_datetime
    from (
        {%- for source_name in rule.source_priority %}
        select
            current_{{ source_name | lower }}.{{ hub_pk }} as {{ hub_pk }},
            current_{{ source_name | lower }}.{{ attribute }} as value,
            current_{{ source_name | lower }}.effective_from as effective_from,
            current_{{ source_name | lower }}.load_datetime as load_datetime,
            '{{ source_name }}' as source_name,
            {{ loop.index }} as priority_rank
        from current_{{ source_name | lower }}
        {% if not loop.last %}union all{% endif %}
        {%- endfor %}
    ) as candidates
    where value is not null
    qualify row_number() over (
        partition by {{ hub_pk }}
        order by effective_from desc, priority_rank asc
    ) = 1
){% if not loop.last %},{% endif %}
{%- endfor %}

select
    hub.{{ hub_pk }},
    hub.{{ hub_nk }},
    hub.hub_load_datetime,
    hub.hub_record_source,

{%- for attribute, rule in attribute_rules.items() %}
    {%- set source_priority = rule.source_priority %}
    {%- set strategy = rule.strategy %}
    {%- if strategy == 'first_non_null' %}
    {%- if source_priority | length == 1 %}
    {#- Single-source (degenerate) survivorship: one source contributes this entity,
        so there is no survivorship contest -- the golden value IS that source's value.
        Emit the bare column, NOT coalesce(x): a single-argument coalesce is a
        Snowflake compile error (000938 "not enough arguments for function COALESCE").
        The bare-column form runs on all three adapters. Multi-source entities take the
        else branch below, whose rendered SQL is
        byte-identical to the pre-guard output. -#}
    current_{{ source_priority[0] | lower }}.{{ attribute }} as {{ attribute }},
    {%- else %}
    coalesce(
        {%- for source_name in source_priority %}
        current_{{ source_name | lower }}.{{ attribute }}{% if not loop.last %}, {% endif %}
        {%- endfor %}
    ) as {{ attribute }},
    {%- endif %}
    case
        {%- for source_name in source_priority %}
        when current_{{ source_name | lower }}.{{ attribute }} is not null then '{{ source_name }}'
        {%- endfor %}
    end as {{ attribute }}__source,
    case
        {%- for source_name in source_priority %}
        when current_{{ source_name | lower }}.{{ attribute }} is not null then current_{{ source_name | lower }}.load_datetime
        {%- endfor %}
    end as {{ attribute }}__load_datetime
    {%- elif strategy == 'most_recent' %}
    mr_{{ attribute }}.value as {{ attribute }},
    mr_{{ attribute }}.source_name as {{ attribute }}__source,
    mr_{{ attribute }}.load_datetime as {{ attribute }}__load_datetime
    {%- else %}
    {{ exceptions.raise_compiler_error("Unsupported survivorship strategy '" ~ strategy ~ "' for " ~ entity ~ "." ~ attribute) }}
    {%- endif %}{% if not loop.last %},{% endif %}

{%- endfor %}

from hub
{%- for source in source_satellites %}
left join current_{{ source.name | lower }}
    on current_{{ source.name | lower }}.{{ hub_pk }} = hub.{{ hub_pk }}
{%- endfor %}
{%- for attribute, rule in attribute_rules.items() if rule.strategy == 'most_recent' %}
left join mr_{{ attribute }}
    on mr_{{ attribute }}.{{ hub_pk }} = hub.{{ hub_pk }}
{%- endfor %}
{%- endmacro %}
