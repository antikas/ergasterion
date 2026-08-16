-- Property-graph binding over the emitted investment estate: a CREATE PROPERTY GRAPH DDL
-- mapping the physical golden-record / hub node tables and link edge tables generated for
-- this domain from the declarations/domains SSOT. Regenerated, never hand-edited.
--
-- Scope: this binding realises LINK-BACKED relationships only -- those backed by a physical
-- link table. Column-level relationships (a relationship carried by a payload column of a
-- node table) are type-graph knowledge with no physical link table and are not part of this
-- estate binding.
--
-- Engine support: assumes an engine providing SQL/PGQ-style CREATE PROPERTY GRAPH DDL over
-- the relational estate. This artefact is a declarative binding map; it is not executed here
-- and asserts no ISO GQL conformance. Node identity is the Data Vault hash key together with
-- its business natural key; link foreign keys reference the hash-key component.

CREATE PROPERTY GRAPH investment_estate
  NODE TABLES (
    bv_deal_golden_record
      KEY (deal_hk, golden_deal_key)
      LABEL deal,
    bv_fund_golden_record
      KEY (fund_hk, golden_fund_key)
      LABEL fund,
    bv_gp_golden_record
      KEY (gp_hk, golden_gp_key)
      LABEL gp,
    bv_legal_vehicle_golden_record
      KEY (legal_vehicle_hk, golden_legal_vehicle_key)
      LABEL legal_vehicle,
    bv_portfolio_company_golden_record
      KEY (portfolio_company_hk, golden_portfolio_company_key)
      LABEL portfolio_company
  )
  EDGE TABLES (
    link_deal_fund_conversion
      KEY (deal_fund_conversion_lhk)
      SOURCE KEY (deal_hk) REFERENCES bv_deal_golden_record (deal_hk, golden_deal_key)
      DESTINATION KEY (fund_hk) REFERENCES bv_fund_golden_record (fund_hk, golden_fund_key)
      LABEL CONVERTED_TO_FUND,
    link_deal_target_company
      KEY (deal_target_company_lhk)
      SOURCE KEY (deal_hk) REFERENCES bv_deal_golden_record (deal_hk, golden_deal_key)
      DESTINATION KEY (portfolio_company_hk) REFERENCES bv_portfolio_company_golden_record (portfolio_company_hk, golden_portfolio_company_key)
      LABEL TARGETS_COMPANY,
    link_fund_gp
      KEY (fund_gp_lhk)
      SOURCE KEY (fund_hk) REFERENCES bv_fund_golden_record (fund_hk, golden_fund_key)
      DESTINATION KEY (gp_hk) REFERENCES bv_gp_golden_record (gp_hk, golden_gp_key)
      LABEL MANAGED_BY,
    link_fund_portfolio_company
      KEY (fund_portfolio_company_lhk)
      SOURCE KEY (fund_hk) REFERENCES bv_fund_golden_record (fund_hk, golden_fund_key)
      DESTINATION KEY (portfolio_company_hk) REFERENCES bv_portfolio_company_golden_record (portfolio_company_hk, golden_portfolio_company_key)
      LABEL HOLDS_POSITION_IN,
    link_gp_succession
      KEY (gp_succession_lhk)
      SOURCE KEY (predecessor_gp_hk) REFERENCES bv_gp_golden_record (gp_hk, golden_gp_key)
      DESTINATION KEY (successor_gp_hk) REFERENCES bv_gp_golden_record (gp_hk, golden_gp_key)
      LABEL SUCCEEDED_BY,
    link_investment_vehicle
      KEY (investment_vehicle_lhk)
      SOURCE KEY (fund_hk) REFERENCES bv_fund_golden_record (fund_hk, golden_fund_key)
      DESTINATION KEY (legal_vehicle_hk) REFERENCES bv_legal_vehicle_golden_record (legal_vehicle_hk, golden_legal_vehicle_key)
      LABEL INVESTS_THROUGH
  );
