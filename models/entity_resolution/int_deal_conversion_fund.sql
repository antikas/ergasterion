-- Deal-conversion fund lookup. A READ-ONLY, deduped map from a fund's
-- declared shared external id to its resolved golden_fund_key, sourced from res_fund.
-- The deal->fund conversion bridge (link_deal_fund_conversion) joins this so a closed
-- deal's converted_record_id resolves to an EXISTING well-resolved fund's golden key
-- WITHOUT ORIGO ever contributing a row into res_fund itself -- feeding ORIGO into
-- res_fund would trip the fund precision/recall overlap-manifest tests (a resolved
-- record absent from the fund manifest is a coverage gap by design). One row per
-- external id: a fund's res_fund rows all share one golden_fund_key, but the QUALIFY
-- keeps the join single-valued regardless, deterministically (lowest golden key).
select
    fund_external_id,
    golden_fund_key
from (
    select
        shared_external_id as fund_external_id,
        golden_fund_key
    from {{ ref('res_fund') }}
    where golden_fund_key is not null
      and shared_external_id is not null
    qualify row_number() over (
        partition by shared_external_id
        order by golden_fund_key
    ) = 1
) as fund_by_external_id
