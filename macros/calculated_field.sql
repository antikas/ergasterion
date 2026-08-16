{% macro calculated_field(spec) -%}
{%- set name = spec.get('name') -%}
{%- set base_model = spec.get('base_model') -%}
{%- set base_alias = spec.get('base_alias', 'base') -%}
{%- set grain = spec.get('grain', []) -%}
{%- set inputs = spec.get('inputs', {}) -%}
{%- set outputs = spec.get('outputs', []) -%}
{%- set joins = spec.get('joins', []) -%}
{%- set pass_through = spec.get('pass_through', []) -%}
{%- set where = spec.get('where') -%}

{%- if not name -%}
    {{ exceptions.raise_compiler_error("Calculated field spec is missing required key 'name'.") }}
{%- endif -%}
{%- if not base_model -%}
    {{ exceptions.raise_compiler_error("Calculated field spec '" ~ name ~ "' is missing required key 'base_model'.") }}
{%- endif -%}
{%- if grain | length == 0 -%}
    {{ exceptions.raise_compiler_error("Calculated field spec '" ~ name ~ "' must declare a grain.") }}
{%- endif -%}
{%- if inputs | length == 0 -%}
    {{ exceptions.raise_compiler_error("Calculated field spec '" ~ name ~ "' must declare named inputs.") }}
{%- endif -%}
{%- if outputs | length == 0 -%}
    {{ exceptions.raise_compiler_error("Calculated field spec '" ~ name ~ "' must declare at least one output.") }}
{%- endif -%}

{%- set input_select_items = [] -%}
{%- set final_select_items = [] -%}

{%- for field in grain -%}
    {%- if field is string -%}
        {%- set field_name = field -%}
        {%- set field_expr = base_alias ~ '.' ~ field -%}
    {%- else -%}
        {%- set field_name = field.get('name') -%}
        {%- set field_expr = field.get('expr', base_alias ~ '.' ~ field_name) -%}
    {%- endif -%}
    {%- do input_select_items.append(field_expr ~ ' as ' ~ field_name) -%}
    {%- do final_select_items.append(field_name) -%}
{%- endfor -%}

{%- for input_name, input_expr in inputs.items() -%}
    {%- do input_select_items.append(input_expr ~ ' as ' ~ input_name) -%}
{%- endfor -%}

{%- for field in pass_through -%}
    {%- if field is string -%}
        {%- do final_select_items.append(field) -%}
    {%- else -%}
        {%- set field_name = field.get('name') -%}
        {%- set field_expr = field.get('expr') -%}
        {%- if not field_name or not field_expr -%}
            {{ exceptions.raise_compiler_error("Calculated field spec '" ~ name ~ "' has a pass_through item without name and expr.") }}
        {%- endif -%}
        {%- do input_select_items.append(field_expr ~ ' as ' ~ field_name) -%}
        {%- do final_select_items.append(field_name) -%}
    {%- endif -%}
{%- endfor -%}

{%- for output in outputs -%}
    {%- set output_name = output.get('name') -%}
    {%- set expression = output.get('expression') -%}
    {%- set output_type = output.get('output_type') -%}
    {%- if not output_name or not expression or not output_type -%}
        {{ exceptions.raise_compiler_error("Calculated field spec '" ~ name ~ "' has an output without name, expression, and output_type.") }}
    {%- endif -%}
    {#- output_type is a dialect-free token (int|float|numeric|date|string|boolean);
        dpf_type maps it to the target adapter's concrete type name. -#}
    {%- do final_select_items.append('cast((' ~ expression ~ ') as ' ~ dpf_type(output_type) ~ ') as ' ~ output_name) -%}
{%- endfor -%}

with input_frame as (
    select
        {{ input_select_items | join(',\n        ') }}
    from {{ ref(base_model) }} as {{ base_alias }}
    {%- for join in joins %}
    {{ join.get('type', 'left') }} join {{ ref(join.get('model')) }} as {{ join.get('alias', join.get('model')) }}
        on {{ join.get('on') | join('\n        and ') }}
    {%- endfor %}
    {%- if where %}
    where {{ where }}
    {%- endif %}
)

select
    {{ final_select_items | join(',\n    ') }}
from input_frame
{%- endmacro %}
