-- PIT-consumer test: pit_investment_classification vs dim_investment_classification (fold).
--
-- pit_investment_classification had no consumer -- nothing in the project selected
-- from it. This test gives it one: its whole reason to exist is to point, per
-- entity and classification type, at the CURRENT classification history row. That
-- claim is exactly what dim_investment_classification's `is_current = true` rows
-- also encode (via the same half-open SCD2 ranges, `effective_to` coalesced to
-- 9999-12-31 for the open-ended row). If the two ever disagree, one of the two
-- current-state paths is wrong.
--
-- dim_investment_classification carries one extra row per classification type that
-- pit_investment_classification does not and should not: the reserved `UNKNOWN`
-- member (`fund_id = 'UNKNOWN'`), synthesised directly from classification_type /
-- classification_value rather than sourced from real entity history. That row is
-- excluded from the comparison below on both sides of the fence.
--
-- Three violations are reported (any one fails the test):
--   1. missing_from_dim   -- pit points at an (entity, classification type) that has
--                            no current row in the dimension;
--   2. missing_from_pit   -- the dimension has a current row for a real entity that
--                            pit does not point at;
--   3. value_mismatch     -- both point at the same (entity, classification type)
--                            but disagree on the current classification value or its
--                            effective_from.
--
-- Singular test: PASSES when it returns zero rows.
with pit as (
    select
        entity_key as fund_id,
        classification_type_code,
        current_classification_value_code as classification_value_code,
        current_effective_from as effective_from
    from {{ ref('pit_investment_classification') }}
),

dim_current as (
    select
        fund_id,
        classification_type_code,
        classification_value_code,
        effective_from
    from {{ ref('dim_investment_classification') }}
    where is_current
      and fund_id != 'UNKNOWN'
),

missing_from_dim as (
    select
        pit.fund_id,
        pit.classification_type_code,
        'missing_from_dim' as violation
    from pit
    left join dim_current
        on dim_current.fund_id = pit.fund_id
        and dim_current.classification_type_code = pit.classification_type_code
    where dim_current.fund_id is null
),

missing_from_pit as (
    select
        dim_current.fund_id,
        dim_current.classification_type_code,
        'missing_from_pit' as violation
    from dim_current
    left join pit
        on pit.fund_id = dim_current.fund_id
        and pit.classification_type_code = dim_current.classification_type_code
    where pit.fund_id is null
),

value_mismatch as (
    select
        pit.fund_id,
        pit.classification_type_code,
        'value_mismatch' as violation
    from pit
    inner join dim_current
        on dim_current.fund_id = pit.fund_id
        and dim_current.classification_type_code = pit.classification_type_code
    where pit.classification_value_code is distinct from dim_current.classification_value_code
       or pit.effective_from is distinct from dim_current.effective_from
)

select fund_id, classification_type_code, violation from missing_from_dim
union all
select fund_id, classification_type_code, violation from missing_from_pit
union all
select fund_id, classification_type_code, violation from value_mismatch
