-- Expected-value cross-source external-id-map test.
--
-- canonical_fund.external_ids must be a PER-SOURCE map keyed by record source
-- covering every contributing source for Apex Growth II -- vantora, meridex, portiq,
-- and chrono -- not a single winning source's id. Checked via a plain LIKE
-- substring match against the rendered JSON-object string (ANSI-standard, no
-- dialect-specific JSON-parse function needed): a JSON object's top-level key is
-- a stable `"key"` substring of each supported renderer's serialisation.
-- BigQuery uses TO_JSON_STRING over JSON_OBJECT with aggregated key/value arrays;
-- Snowflake uses TO_JSON over OBJECT_AGG with values converted to VARIANT. DuckDB
-- takes a different shape: TO_JSON converts a MAP built from key/value lists before
-- the result is cast to VARCHAR.
--
-- Singular test: PASSES when it returns zero rows. It returns a row if any of
-- the 4 contributing sources is missing from external_ids (i.e. the fix
-- regressed to the survivorship winner's single id).
select
    fund_id,
    lei,
    external_ids
from {{ ref('canonical_fund') }}
where lei = '549300APEXGROWTH20001'
  and not (
        external_ids like '%"vantora"%'
    and external_ids like '%"meridex"%'
    and external_ids like '%"portiq"%'
    and external_ids like '%"chrono"%'
  )
