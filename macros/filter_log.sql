{#-
  The Data Filtering pattern's audit primitive.

  Architecture section 4 requires every exclusion of a Data Filtering occurrence
  to be auditable by predicate and count. The generated filter-log relation
  carries one row per declared predicate, naming the predicate and the number of
  rows it excluded from the product's pre-filter relation.

  The count is the one piece of SQL every predicate row shares, so it lives here
  once rather than being re-spelled per predicate in the emitted model. It is
  written with a plain CASE inside SUM, which both declared adapters accept:
  neither BigQuery's COUNTIF nor DuckDB's aggregate FILTER clause is portable,
  and this macro is the reason no generated model reaches for either.

  `predicate` is a boolean SQL expression, or the name of a boolean column the
  calling model already evaluated the predicate into. A row counts as excluded
  when the predicate is false OR unknown: a predicate that evaluates to NULL
  keeps no row, so a count that ignored NULL would under-report the rows the
  filter actually dropped.
-#}

{% macro dpf_excluded_count(predicate) -%}
    sum(case when not ({{ predicate }}) or ({{ predicate }}) is null then 1 else 0 end)
{%- endmacro %}
