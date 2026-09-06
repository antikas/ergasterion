-- What this proves: the declared survivorship strategy decides, not the order
-- rows happen to arrive in.
--
-- Four systems state the same buyout fund. One of them states its commitment in
-- thousands rather than in dollars, and it states it as at 30 September; the
-- system that states it in dollars reports as at 31 December. The declared
-- strategy for that column is most_recent, so the later report survives and the
-- figure the estate publishes is the dollar one.
--
-- The second arm proves the strategy and not the accident: the record that
-- carries the wrong figure is still in the vault's own observation satellite, so
-- the disagreement is kept rather than deleted.
--
-- Singular test: it passes when it returns no rows.
with surviving as (
    select
        'the surviving commitment is not the later reported one' as disagreement,
        cast(fund_committed_capital_usd as varchar) as subject
    from {{ ref('investment__fund_vault__golden_fund') }}
    where fund_external_identity = 'OPENIM-FUND-NORTHSTAR-IV'
      and fund_committed_capital_usd <> 65000000.00
),

kept as (
    select
        'the record that states the commitment in thousands was not kept' as disagreement,
        'vantora:CEP-FUND-004' as subject
    where not exists (
        select 1
        from {{ ref('investment__fund_conformed') }}
        where fund_identity_key = 'vantora:CEP-FUND-004'
          and source_committed_capital_usd = 65000.00
    )
)

select disagreement, subject from surviving
union all
select disagreement, subject from kept
