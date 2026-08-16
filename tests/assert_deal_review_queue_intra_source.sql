-- Intra-source deal review-queue test.
--
-- Correctness claim: the deal tier-2 path is wired and live. The seeded middle-band
-- pair (D-009 "Riverstone Logistics" / D-010 "Riverstone Logistix") carries no deal
-- external id, so res_deal's tier-1 misses it and both records come out
-- pending_probabilistic. They differ in strategy (buyout vs growth -> sector_score 0)
-- but share a sourced_date (date_score 1) and near-identical normalised names
-- (edit distance 2 over 19 chars -> string_score ~= 0.895), so the composite lands
-- ~= 0.70 -- inside the 0.65-0.85 review band -- and surfaces UNMERGED in review_queue
-- for a human steward rather than being auto-merged. This is intra-source dedup routed
-- through the EXISTING scoring-config + review-queue machinery (the deal branch in
-- res_pending_probabilistic + the entity-type-gated intra-source arm in
-- int_entity_resolution_candidate_pairs). Fund matching is a separate cross-source
-- process.
--
-- Singular test: PASSES when at least one deal pair is in the review queue. Returns a
-- violation row when no deal reaches review (a regression in the tier-2 deal wiring or
-- a seed change that removed the middle-band pair).
select 'deal_review_queue_is_empty' as violation
where not exists (
    select 1 from {{ ref('review_queue') }}
    where entity_type = 'deal'
)
