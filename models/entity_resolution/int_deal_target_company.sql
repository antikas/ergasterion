-- Deal target-company lookup. A READ-ONLY, deduped map from a portfolio
-- company's declared shared external id to its resolved golden_portfolio_company_key,
-- sourced from res_portfolio_company. The deal->target-company bridge
-- (link_deal_target_company) joins this so a deal's target_entity_id resolves to an
-- EXISTING golden company key WITHOUT ORIGO ever contributing a row into
-- res_portfolio_company (which would trip that entity's resolution grain / uniqueness
-- and the company-facing tests). One row per external id, deterministic (lowest key).
select
    company_external_id,
    golden_portfolio_company_key
from (
    select
        shared_external_id as company_external_id,
        golden_portfolio_company_key
    from {{ ref('res_portfolio_company') }}
    where golden_portfolio_company_key is not null
      and shared_external_id is not null
    qualify row_number() over (
        partition by shared_external_id
        order by golden_portfolio_company_key
    ) = 1
) as company_by_external_id
