select
    'fund' as entity_type,
    source_system,
    source_id,
    source_record_id,
    resolution_tier,
    confidence,
    pending_probabilistic
from {{ ref('res_fund') }}
where golden_fund_key is null

union all

select
    'gp' as entity_type,
    source_system,
    source_id,
    cast(null as string) as source_record_id,
    resolution_tier,
    confidence,
    pending_probabilistic
from {{ ref('res_gp') }}
where golden_gp_key is null

union all

select
    'portfolio_company' as entity_type,
    source_system,
    source_id,
    cast(null as string) as source_record_id,
    resolution_tier,
    confidence,
    pending_probabilistic
from {{ ref('res_portfolio_company') }}
where golden_portfolio_company_key is null

