with vantora_funds as (
    select * from {{ ref('stg_vantora_funds') }}
)

select
    source_system,
    source_record_id,
    source_fund_id,
    {{ normalise_prefixed_id('source_fund_id', 'CPS') }} as canonical_vantora_fund_id,
    fund_name,
    {{ normalise_name('fund_name') }} as normalised_fund_name,
    fund_family_name,
    vehicle_type,
    source_gp_id,
    gp_name,
    lei,
    shared_external_id,
    asset_class,
    strategy,
    vintage_year,
    committed_capital_usd,
    currency,
    domicile,
    fund_status,
    as_of_date
from vantora_funds

