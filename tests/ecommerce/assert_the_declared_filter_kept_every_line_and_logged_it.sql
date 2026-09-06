-- What this proves: the declared filter on the served star is a live control
-- and not a comment. Its predicate is recorded by name with the number of
-- rows it excluded, and that number agrees with the rows the fact carries.
--
-- The served fact reports on the lines of fulfilled orders. Every order the
-- two channels delivered is fulfilled, so the correct answer today is that
-- the predicate excluded nothing and every delivered line reached the fact.
-- That is worth asserting rather than assuming: an exclusion the estate
-- never intended would appear here as a non-zero count, and a filter that
-- had stopped being applied at all would appear as a missing log row.
--
-- Singular test: it passes when it returns no rows.
with logged as (
    select
        predicate_name,
        excluded_count
    from {{ ref('ecommerce__order_star__filter_log') }}
),

not_logged as (
    select
        'the declared predicate is not recorded in the filter log' as disagreement
    from logged
    having count(*) <> 1
        or count(*) filter (where predicate_name = 'fulfilled_lines_only') <> 1
),

unexpected_exclusion as (
    select
        'the filter excluded rows the estate did not expect it to' as disagreement
    from logged
    where excluded_count <> 0
),

lines_lost as (
    select
        'the fact does not carry every line the filter kept' as disagreement
    from (
        select
            (select count(*) from {{ ref('ecommerce__order_line') }}) as consolidated_lines,
            (select count(*) from {{ ref('ecommerce__order_star__fact_order_line') }}) as fact_lines,
            (select coalesce(sum(excluded_count), 0) from logged) as excluded
    ) as counted
    where fact_lines + excluded <> consolidated_lines
)

select disagreement from not_logged
union all
select disagreement from unexpected_exclusion
union all
select disagreement from lines_lost
