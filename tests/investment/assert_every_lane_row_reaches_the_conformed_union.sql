-- What this proves: the consolidating union loses no row and invents none.
--
-- Each entity's conformed product is a union of its source lanes. Every row of
-- every lane has to be a row of the union, under the source system that lane
-- states, and the union has to hold nothing else.
--
-- The counts are read from the lanes rather than stated here. A declared
-- reconcile rule cannot express this coverage: it matches a base row to a
-- published row on every column with an equality, and every lane states a column
-- some other system does not, so a null column would report every row missing.
--
-- Singular test: it passes when it returns no rows.
with expected (entity_name, source_system, lane_rows) as (
    values
        ('fund', 'chrono', (select count(*) from {{ ref('investment__chrono_fund') }})),
        ('fund', 'meridex', (select count(*) from {{ ref('investment__meridex_fund') }})),
        ('fund', 'portiq', (select count(*) from {{ ref('investment__portiq_fund') }})),
        ('fund', 'vantora', (select count(*) from {{ ref('investment__vantora_fund') }})),
        ('gp', 'chrono', (select count(*) from {{ ref('investment__chrono_gp') }})),
        ('gp', 'meridex', (select count(*) from {{ ref('investment__meridex_gp') }})),
        ('gp', 'portiq', (select count(*) from {{ ref('investment__portiq_gp') }})),
        ('gp', 'vantora', (select count(*) from {{ ref('investment__vantora_gp') }})),
        ('portfolio_company', 'chrono', (select count(*) from {{ ref('investment__chrono_portfolio_company') }})),
        ('portfolio_company', 'meridex', (select count(*) from {{ ref('investment__meridex_portfolio_company') }})),
        ('portfolio_company', 'portiq', (select count(*) from {{ ref('investment__portiq_portfolio_company') }})),
        ('portfolio_company', 'vantora', (select count(*) from {{ ref('investment__vantora_portfolio_company') }})),
        ('legal_vehicle', 'meridex', (select count(*) from {{ ref('investment__meridex_legal_vehicle') }})),
        ('legal_vehicle', 'vantora', (select count(*) from {{ ref('investment__vantora_legal_vehicle') }}))
),

published (entity_name, source_system, union_rows) as (
    select 'fund', fund_source_system, count(*) from {{ ref('investment__fund_conformed') }} group by 2
    union all
    select 'gp', gp_source_system, count(*) from {{ ref('investment__gp_conformed') }} group by 2
    union all
    select 'portfolio_company', company_source_system, count(*) from {{ ref('investment__portfolio_company_conformed') }} group by 2
    union all
    select 'legal_vehicle', vehicle_source_system, count(*) from {{ ref('investment__legal_vehicle_conformed') }} group by 2
)

select
    'the union does not carry the rows its lane brought' as disagreement,
    coalesce(expected.entity_name, published.entity_name) || ' from '
        || coalesce(expected.source_system, published.source_system) as subject
from expected
full outer join published
    on expected.entity_name = published.entity_name
   and expected.source_system = published.source_system
where expected.lane_rows is distinct from published.union_rows
