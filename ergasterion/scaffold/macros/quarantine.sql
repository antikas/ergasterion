{#-
  The Data Validation pattern's quarantine and threshold primitives.

  Architecture section 4 gives Data Validation three emitted artefacts and
  one semantic: tests, a quarantine relation, an abort condition, and never
  a silent pass-through. The generated relations carry the first two: a
  checked relation holding every input row with one boolean violation
  column per declared rule, and a quarantine relation holding one row per
  violated rule per failing row. The generic tests below carry the third.

  Every test returns rows only when the condition it names is broken, which
  is dbt's own generic-test contract. Each is called from a generated
  schema file with its arguments nested under `arguments`, and each names
  the rule it stands for in its returned rows, so a stored failure says
  which declared rule produced it.

  A bound and a pattern arrive already rendered as SQL literals: the engine
  knows the declared value's type and renders it once, so this file never
  guesses whether a bound is a number, a date or a string.
-#}

{#- The rate of violating rows for one rule, against the declared ceiling.
    Returns one row -- naming the rule, the violating row count and the
    total -- when the rate exceeds `max_rate`, and nothing when it does
    not. A ceiling of 0 makes any violating row an abort. An empty
    relation never breaches: a rate over no rows is not a fact. -#}
{% test dpf_error_threshold(model, violation_column, rule, max_rate) -%}
select
    rule_name,
    violating_rows,
    total_rows
from (
    select
        '{{ rule }}' as rule_name,
        sum(case when {{ violation_column }} then 1 else 0 end) as violating_rows,
        count(*) as total_rows
    from {{ model }}
) as summary
where total_rows > 0
  and (violating_rows * 1.0) / total_rows > {{ max_rate }}
{%- endtest %}


{#- The share of non-null values in `column_name`, against the declared
    minimum. Returns one row naming the rule when the share falls below
    it. -#}
{% test dpf_completeness(model, column_name, rule, threshold) -%}
select
    rule_name,
    present_rows,
    total_rows
from (
    select
        '{{ rule }}' as rule_name,
        sum(case when {{ column_name }} is null then 0 else 1 end) as present_rows,
        count(*) as total_rows
    from {{ model }}
) as summary
where total_rows > 0
  and (present_rows * 1.0) / total_rows < {{ threshold }}
{%- endtest %}


{#- Every row whose `column_name` falls outside the declared bounds. Both
    bounds arrive as rendered SQL literals; `none` means the side is not
    bounded. A null value is not out of range, it is absent, and a
    completeness or not-null rule is what states that. -#}
{% test dpf_range(model, column_name, rule, min_value=none, max_value=none) -%}
select
    '{{ rule }}' as rule_name,
    {{ column_name }} as offending_value
from {{ model }}
where {{ column_name }} is not null
  and (
    {%- if min_value is not none %}
        {{ column_name }} < {{ min_value }}
    {%- endif %}
    {%- if min_value is not none and max_value is not none %}
        or
    {%- endif %}
    {%- if max_value is not none %}
        {{ column_name }} > {{ max_value }}
    {%- endif %}
  )
{%- endtest %}


{#- Every row whose `column_name` does not match the declared pattern. The
    pattern arrives as a rendered SQL literal and the match itself goes
    through the cross-database dispatch macro, because the two declared
    adapters spell a regular-expression match differently. -#}
{% test dpf_matches_regex(model, column_name, rule, pattern) -%}
select
    '{{ rule }}' as rule_name,
    {{ column_name }} as offending_value
from {{ model }}
where {{ column_name }} is not null
  and not {{ dpf_regexp_contains(column_name, pattern) }}
{%- endtest %}
