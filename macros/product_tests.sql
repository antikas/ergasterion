{#-
  The three generated tests that are not Data Validation rules.

  Architecture section 4 gives Data Aggregation a declared grain and Data
  Contracts a compliance check ("Product not published if it violates its
  contract"); architecture section 6 gives the dimensional shape's type 2
  dimensions a half-open effective range. All three are rendered by the dbt
  translator into the product's generated schema file, with their arguments
  nested under `arguments`, and all three are ordinary dbt generic tests:
  they return rows only when the thing they name is broken.

  A compliance failure blocks publication because dbt runs a model's tests
  before anything that reads it: the current pointer and the SLA record are
  models over the product's relation, so a product whose emitted schema
  disagrees with its contract never reaches either.
-#}

{#- Every break in a slowly changing dimension's history. `key` is the
    dimension key, `effective_from` and `effective_to` the two columns its
    half-open range publishes. A version's range must end exactly where the
    next version's begins, only the version in force now may be open, and
    no range may end at or before it starts. Each break comes back as the
    key, the range that broke and the range that follows it. -#}
{% test dpf_effective_range_contiguity(model, key, effective_from, effective_to) -%}
{%- set partition = key | join(', ') -%}
with ordered as (
    select
        {{ partition }},
        {{ effective_from }},
        {{ effective_to }},
        lead({{ effective_from }}) over (
            partition by {{ partition }} order by {{ effective_from }}
        ) as next_effective_from
    from {{ model }}
)

select
    {{ partition }},
    {{ effective_from }},
    {{ effective_to }},
    next_effective_from
from ordered
where (next_effective_from is null and {{ effective_to }} is not null)
   or (next_effective_from is not null and {{ effective_to }} is null)
   or (next_effective_from is not null and {{ effective_to }} <> next_effective_from)
   or ({{ effective_to }} is not null and {{ effective_to }} <= {{ effective_from }})
{%- endtest %}


{#- Every grain value carrying more than one row. `columns` is the declared
    grain, already checked as plain SQL identifiers by the engine. -#}
{% test dpf_unique_grain(model, columns) -%}
{%- set grain = columns | join(', ') -%}
select
    {{ grain }},
    count(*) as grain_row_count
from {{ model }}
group by {{ grain }}
having count(*) > 1
{%- endtest %}


{#- Every disagreement between the relation dbt built and the schema the
    product's contract publishes: a contract column the relation does not
    carry, a relation column the contract does not declare, a column order
    the contract does not state, and a null in a column the contract
    declares required.

    The column comparison reads the built relation through the adapter, so
    it sees what was actually created rather than what the model text
    intended. The required-ness comparison is a query over the relation
    itself. Types are not compared here: the emitted casts already go
    through each adapter's own physical type mapping, and the parse gate
    resolves every one of them per adapter, so a type claim made by
    comparing an adapter's reported type name against another vocabulary
    would assert less than those two already do. -#}
{% test dpf_contract_compliance(model, columns, required) -%}
{%- set relation_columns = adapter.get_columns_in_relation(model) -%}
{%- set actual = [] -%}
{%- for relation_column in relation_columns -%}
    {%- do actual.append(relation_column.name | lower) -%}
{%- endfor -%}
{%- set expected = [] -%}
{%- for name in columns -%}
    {%- do expected.append(name | lower) -%}
{%- endfor -%}
{%- set findings = [] -%}
{%- for name in expected -%}
    {%- if name not in actual -%}
        {%- do findings.append(['missing_column', name]) -%}
    {%- endif -%}
{%- endfor -%}
{%- for name in actual -%}
    {%- if name not in expected -%}
        {%- do findings.append(['unexpected_column', name]) -%}
    {%- endif -%}
{%- endfor -%}
{%- if findings | length == 0 and actual != expected -%}
    {%- do findings.append(['column_order', expected | join(',')]) -%}
{%- endif -%}
{%- set present_required = [] -%}
{%- for name in required -%}
    {%- if (name | lower) in actual -%}
        {%- do present_required.append(name) -%}
    {%- endif -%}
{%- endfor -%}
with schema_findings as (
{%- if findings | length > 0 %}
{%- for finding in findings %}
    select
        '{{ finding[0] }}' as disagreement,
        '{{ finding[1] }}' as detail
{%- if not loop.last %}
    union all
{%- endif %}
{%- endfor %}
{%- else %}
    select
        cast(null as {{ dpf_type('string') }}) as disagreement,
        cast(null as {{ dpf_type('string') }}) as detail
    where 1 = 0
{%- endif %}
),

required_findings as (
{%- if present_required | length > 0 %}
{%- for name in present_required %}
    select
        'null_in_required_column' as disagreement,
        '{{ name }}' as detail
    from {{ model }}
    where {{ name }} is null
{%- if not loop.last %}
    union all
{%- endif %}
{%- endfor %}
{%- else %}
    select
        cast(null as {{ dpf_type('string') }}) as disagreement,
        cast(null as {{ dpf_type('string') }}) as detail
    where 1 = 0
{%- endif %}
)

select disagreement, detail from schema_findings
union all
select disagreement, detail from required_findings
{%- endtest %}


{#- Every key of one source of a consolidating product that the product's
    own relation does not carry (architecture section 8: a consolidated
    product proves its coverage against the upstream contracts it
    consolidates).

    `source` is the source's own model, spliced in as a ref by the emitted
    schema file; `keys` are the columns the coverage is proved by, as the
    product's relation names them -- the declared merge keys, or, for a
    union, the columns of the one schema the union publishes -- and
    `source_columns` the same columns as that source names them, in the
    same order, so a source read under its own names is still compared
    against the right ones. Every distinct key of the source must appear
    in the product's relation; each that does not comes back as a row, so
    a source silently dropped along the way turns the test red.

    The comparison is null-safe, and it has to be. A union publishes one
    schema, so its coverage is proved on every column of it, and a lane
    that never states one of those columns brings it as null on every row
    it delivers. Plain equality is unknown when either side is null, so an
    anti-join on equality would report every one of that lane's rows as
    missing while all of them are there. Two nulls in the same column of
    the same row are the same value for this proof, and a row of the source
    is covered when the product's relation carries a row matching it column
    for column, nulls included.

    That does not soften what the proof catches: a row the product dropped
    matches nothing on any column, so it still comes back. -#}
{% test dpf_reconcile_coverage(model, source, keys, source_columns) -%}
with source_keys as (
    select distinct
        {% for key in keys %}{{ source_columns[loop.index0] }} as {{ key }}{% if not loop.last %},
        {% endif %}{% endfor %}
    from {{ source }}
),

published_keys as (
    select distinct
        {{ keys | join(',
        ') }}
    from {{ model }}
)

select
    source_keys.*
from source_keys
where not exists (
    select 1
    from published_keys
    where {% for key in keys %}(source_keys.{{ key }} = published_keys.{{ key }}
       or (source_keys.{{ key }} is null and published_keys.{{ key }} is null)){% if not loop.last %}
      and {% endif %}{% endfor %}
)
{%- endtest %}
