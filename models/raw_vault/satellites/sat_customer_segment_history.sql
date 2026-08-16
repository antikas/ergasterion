{{ config(materialized='table', schema='raw_vault') }}

-- Customer segment (loyalty-tier) history satellite for the e-commerce
-- domain's instance of the reusable half-open-range SCD2 pattern
-- (docs/architecture/scd2-classification.md) -- alongside DEPT/SECTOR/DEAL_STAGE,
-- not folded into that shared classification_type/classification_value registry:
-- segment (bronze/silver/gold) is a domain-2 loyalty tier, not an investment
-- classification, so this satellite carries the segment code as a plain column
-- rather than adding a new classification_type row to investment-domain reference
-- seeds. Domain vocabularies are not shared across the two example
-- domains).
--
-- Hand-authored, NOT emitted (same footing as sat_deal_stage_history.sql -- a
-- hand-authored addition alongside the emitted satellites in this folder, never an
-- edit to one of them). Source: seeds/customer_segment_history_seed.csv, staged by
-- this model (customer_external_id, segment, effective_from,
-- record_source, load_datetime -- see dbt_project.yml's seed config comment).
--
-- customer_external_id in the seed (e.g. CUST-AVA) is the SAME human-readable ground
-- truth label customer_er_overlap_manifest.csv uses -- it is not a column any raw
-- source or canonical model carries. Resolving it to golden_customer_key requires
-- the identical two-hop join the ER precision test uses: manifest
-- (source_system, source_record_id) -> res_customer (source_system, source_id) ->
-- golden_customer_key. A customer_external_id maps to exactly one golden_customer_key
-- by construction (assert_res_customer_er_precision_manifest.sql's vacuity-gap arm
-- already proves this for every duplicated true_customer_external_id; singletons are
-- trivially one-to-one).
--
-- SAME-DAY DETERMINISM (precedent, applied defensively): the seed's
-- load_datetime column is a real per-row business timestamp here (it varies with
-- effective_from, e.g. "2025-05-01T00:00:00"), not the build-wall-clock constant
-- other satellites use that name for -- so it doubles as the intra-day ordering term.
-- Exposed here as segment_recorded_at (renamed on read, mirroring stage_recorded_at's
-- role) so the SCD2 window in dim_customer_segment.sql orders on
-- (effective_from, segment_recorded_at, customer_segment_history_key) rather than
-- falling back to the surrogate key alone on a same-day tie -- exactly the
-- fix, applied here even though the currently seeded history has no actual
-- same-day transition for any customer (each seeded change lands on a distinct
-- effective_from): a future same-day retag (two segment rows seeded for one customer
-- on one date) must resolve deterministically, not by hash, and the ordering is
-- already in place rather than deferred to when it first bites (the residual this
-- repo discloses for dim_investment_classification's own un-fixed sibling case --
-- see scd2-classification.md, "Sibling check" -- is exactly what this satellite
-- avoids by applying the fix up front).

with seed_history as (
    select
        customer_external_id,
        segment,
        {{ dpf_safe_cast('effective_from', 'date') }} as effective_from,
        record_source,
        {{ dpf_safe_cast('load_datetime', 'string') }} as segment_recorded_at
    from {{ ref('customer_segment_history_seed') }}
),

customer_external_id_map as (
    -- The same manifest -> res_customer two-hop the ER precision test uses,
    -- reused here, not forked, to resolve the seed's human-readable customer label to
    -- the actual golden_customer_key.
    select distinct
        manifest.true_customer_external_id as customer_external_id,
        resolved.golden_customer_key
    from {{ ref('customer_er_overlap_manifest') }} as manifest
    inner join {{ ref('res_customer') }} as resolved
        on resolved.source_system = manifest.source_system
        and resolved.source_record_id = manifest.source_record_id
    where resolved.golden_customer_key is not null
)

select
    {{ dbt_utils.generate_surrogate_key([
        'customer_external_id_map.golden_customer_key',
        'seed_history.segment',
        dpf_safe_cast('seed_history.effective_from', 'string')
    ]) }} as customer_segment_history_key,
    customer_external_id_map.golden_customer_key,
    seed_history.customer_external_id,
    seed_history.segment,
    seed_history.effective_from,
    seed_history.record_source,
    seed_history.segment_recorded_at
from seed_history
inner join customer_external_id_map
    on customer_external_id_map.customer_external_id = seed_history.customer_external_id
