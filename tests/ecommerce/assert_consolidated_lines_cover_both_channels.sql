-- What this proves: the consolidating union of the two order line lanes
-- carries every line each channel delivered, and carries each of them once.
--
-- The union's own coverage rule is generated from the declaration and runs
-- beside this test; what that rule proves is that each source's rows are
-- present. This proves the other direction and the split: the consolidated
-- relation carries no line neither channel delivered, and the number of
-- lines attributed to each channel is the number that channel delivered.
-- A union that read one lane twice would satisfy coverage and fail here.
--
-- Singular test: it passes when it returns no rows.
with consolidated as (
    select
        order_line_source_system,
        order_line_record_id
    from {{ ref('ecommerce__order_line') }}
),

observed as (
    select
        order_line_source_system,
        count(*) as lines_carried,
        count(distinct order_line_record_id) as distinct_records
    from consolidated
    group by order_line_source_system
),

expected (order_line_source_system, lines_carried) as (
    values
        ('cartivo', 3),
        ('mercaro', 4)
),

per_channel as (
    select
        coalesce(observed.order_line_source_system, expected.order_line_source_system) as source_system,
        'the consolidated lines of this channel are not the lines it delivered' as disagreement
    from observed
    full join expected
        on observed.order_line_source_system = expected.order_line_source_system
    where observed.order_line_source_system is null
       or expected.order_line_source_system is null
       or observed.lines_carried <> expected.lines_carried
       or observed.distinct_records <> observed.lines_carried
),

delivered as (
    select
        cast(null as varchar) as source_system,
        'the consolidated relation does not carry the delivered line count' as disagreement
    from (
        select
            (select count(*) from consolidated) as consolidated_lines,
            (select count(*) from {{ ref('ecommerce__cartivo_order_line_extract') }})
              + (select count(*) from {{ ref('ecommerce__mercaro_order_line_extract') }}) as delivered_lines
    ) as counted
    where consolidated_lines <> delivered_lines
)

select source_system, disagreement from per_channel
union all
select source_system, disagreement from delivered
