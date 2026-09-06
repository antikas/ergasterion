-- What this proves: the canonical interfaces speak the reference model's
-- vocabulary, not one source system's spelling.
--
-- The five source declarations record, per source and per entity, which
-- delivered column carries which reference-model attribute. The canonical
-- products publish those attribute names. This test names the ones a consumer
-- of each entity relies on and reads them back out of the built adapter's own
-- catalogue, so a rename anywhere between the delivered extract and the
-- interface fails here.
--
-- Singular test: it passes when it returns no rows.
with expected (relation_name, column_name) as (
    values
        ('{{ ref('investment__fund_canonical__fund').identifier }}', 'external_ids'),
        ('{{ ref('investment__fund_canonical__fund').identifier }}', 'lei'),
        ('{{ ref('investment__fund_canonical__fund').identifier }}', 'fund_name'),
        ('{{ ref('investment__fund_canonical__fund').identifier }}', 'gp_id'),
        ('{{ ref('investment__fund_canonical__fund').identifier }}', 'asset_class'),
        ('{{ ref('investment__fund_canonical__fund').identifier }}', 'strategy'),
        ('{{ ref('investment__fund_canonical__fund').identifier }}', 'vintage_year'),
        ('{{ ref('investment__fund_canonical__fund').identifier }}', 'committed_capital_usd'),
        ('{{ ref('investment__fund_canonical__fund').identifier }}', 'currency'),
        ('{{ ref('investment__fund_canonical__fund').identifier }}', 'domicile'),
        ('{{ ref('investment__fund_canonical__fund').identifier }}', 'fund_status'),
        ('{{ ref('investment__gp_canonical__gp').identifier }}', 'gp_name'),
        ('{{ ref('investment__gp_canonical__gp').identifier }}', 'domicile'),
        ('{{ ref('investment__gp_canonical__gp').identifier }}', 'relationship_start_date'),
        ('{{ ref('investment__portfolio_company_canonical__portfolio_company').identifier }}', 'company_name'),
        ('{{ ref('investment__portfolio_company_canonical__portfolio_company').identifier }}', 'sector'),
        ('{{ ref('investment__portfolio_company_canonical__portfolio_company').identifier }}', 'sub_sector'),
        ('{{ ref('investment__portfolio_company_canonical__portfolio_company').identifier }}', 'country'),
        ('{{ ref('investment__portfolio_company_canonical__portfolio_company').identifier }}', 'status'),
        ('{{ ref('investment__legal_vehicle_canonical__legal_vehicle').identifier }}', 'vehicle_id'),
        ('{{ ref('investment__legal_vehicle_canonical__legal_vehicle').identifier }}', 'vehicle_name'),
        ('{{ ref('investment__legal_vehicle_canonical__legal_vehicle').identifier }}', 'vehicle_type'),
        ('{{ ref('investment__legal_vehicle_canonical__legal_vehicle').identifier }}', 'jurisdiction'),
        ('{{ ref('investment__legal_vehicle_canonical__legal_vehicle').identifier }}', 'incorporation_date'),
        ('{{ ref('investment__legal_vehicle_canonical__legal_vehicle').identifier }}', 'investment_id'),
        ('{{ ref('investment__deal_canonical__deal').identifier }}', 'deal_id'),
        ('{{ ref('investment__deal_canonical__deal').identifier }}', 'deal_name'),
        ('{{ ref('investment__deal_canonical__deal').identifier }}', 'target_entity_id'),
        ('{{ ref('investment__deal_canonical__deal').identifier }}', 'strategy'),
        ('{{ ref('investment__deal_canonical__deal').identifier }}', 'originating_team'),
        ('{{ ref('investment__deal_canonical__deal').identifier }}', 'source_channel'),
        ('{{ ref('investment__deal_canonical__deal').identifier }}', 'structure'),
        ('{{ ref('investment__deal_canonical__deal').identifier }}', 'converted_record_type'),
        ('{{ ref('investment__deal_canonical__deal').identifier }}', 'converted_record_id')
),

built as (
    select table_name, column_name
    from information_schema.columns
)

select
    'the canonical interface does not publish the reference model name' as disagreement,
    expected.relation_name || '.' || expected.column_name as subject
from expected
left join built
    on built.table_name = expected.relation_name
   and built.column_name = expected.column_name
where built.column_name is null
