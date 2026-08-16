-- Property-graph binding over the emitted ecommerce estate: a CREATE PROPERTY GRAPH DDL
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

CREATE PROPERTY GRAPH ecommerce_estate
  NODE TABLES (
    bv_customer_golden_record
      KEY (customer_hk, golden_customer_key)
      LABEL customer,
    bv_order_golden_record
      KEY (order_hk, golden_order_key)
      LABEL order,
    bv_product_golden_record
      KEY (product_hk, golden_product_key)
      LABEL product
  )
  EDGE TABLES (
    link_order_customer
      KEY (order_customer_lhk)
      SOURCE KEY (order_hk) REFERENCES bv_order_golden_record (order_hk, golden_order_key)
      DESTINATION KEY (customer_hk) REFERENCES bv_customer_golden_record (customer_hk, golden_customer_key)
      LABEL PLACED_BY,
    link_order_line_product
      KEY (order_line_lhk)
      SOURCE KEY (order_hk) REFERENCES bv_order_golden_record (order_hk, golden_order_key)
      DESTINATION KEY (product_hk) REFERENCES bv_product_golden_record (product_hk, golden_product_key)
      LABEL INCLUDES
  );
