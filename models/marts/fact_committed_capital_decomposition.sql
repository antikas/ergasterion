{{ config(materialized='table', schema='marts') }}

-- Discrepancy decomposition. When two sources assert different values for the
-- SAME resolved fact, do not stop at the survivorship winner -- split the
-- difference into named, explained components so an analyst sees WHY the sources
-- differ, not only WHICH one won. The golden record (bv_fund_golden_record)
-- picks the winner; this model sits beside it and accounts for the gap the
-- winner-pick hides.
--
-- Deterministic spine: pure SQL over the raw-vault satellites and the
-- business-vault golden record. No model, no inference, no LLM in the value
-- path. Every component is a fixed arithmetic rule; anything the data at hand
-- cannot support is 0 and flows honestly into the unexplained residual, with a
-- dimension flag so the analyst knows which explanation is PLAUSIBLE even when
-- its magnitude is not computable here.
--
-- Honest scope for the data at hand (see each component below):
--   * unit_scale  -- COMPUTED. Derivable from the two scalar values alone: a
--       ratio that is (within a tight tolerance) an exact power of 1000 is a
--       millions/thousands-vs-units reporting-scale mismatch, and rescaling one
--       side to the other's units closes the entire gap. Deterministic rule,
--       near-zero false-positive risk, no external reference table.
--   * fx_currency -- 0 + flag. The disputed attribute is already USD-normalised
--       (committed_capital_USD); an FX-choice component would need a rate table
--       keyed by native currency and as-of date, which the vault does not carry.
--       We can DETECT a native-currency mismatch and flag it; we cannot compute
--       its magnitude, so it stays in the residual.
--   * as_of_timing -- 0 + flag. Attributing a magnitude to timing needs the same
--       source's value at the other source's as-of date (a same-source reprice);
--       cross-source we cannot separate timing from genuine disagreement. We
--       expose the as-of gap in days and flag it; the magnitude stays in residual.
--   * scope_universe / methodology -- 0. The vault carries no per-source scope or
--       valuation-methodology metadata for committed capital, so neither is
--       computable or flaggable. They remain named zero-valued columns so the
--       decomposition has a stable, explicit shape.
--
-- Scope and keying: the example seeds disagree on committed_capital_usd. The
-- structural tests are therefore keyed to structure, not numeric pins: the six components sum to
-- total_delta (identity), and the grain is one row per disputed fact per ordered
-- source pair (surrogate-key uniqueness). Whether unit_scale actually fires on
-- any given row is data-dependent and deliberately NOT asserted.
--
-- Grain: one row per (fund_hk, disputed_attribute, source_a, source_b) with
-- source_a < source_b (ordered pair, so each unordered source disagreement
-- appears once) and value_a <> value_b (only genuine disagreements produce a
-- row). disputed_attribute is the literal committed_capital_usd because that is the
-- numeric fund fact this model decomposes.
--
-- Source list mirrors bv_fund_golden_record's source_satellites. This model is
-- hand-authored rather than emitted: the decomposition is a single entity-level
-- fan-in over one disputed fact, so emission-per-source adds nothing (there is
-- no per-source file to generate) -- extending to a fifth source is the one-line
-- edit below, the same edit the declaration-driven golden record would take.

{%- set sources = [
    {'name': 'VANTORA', 'model': 'sat_fund_vantora'},
    {'name': 'MERIDEX',  'model': 'sat_fund_meridex'},
    {'name': 'PORTIQ', 'model': 'sat_fund_portiq'},
    {'name': 'CHRONO', 'model': 'sat_fund_chrono'}
] %}

-- Residual is flagged as material when it exceeds this fraction of the total
-- delta. Relative (not absolute) so it is invariant to the re-seed's value
-- scale. Overridable per-run via --vars.
{%- set residual_tolerance_fraction = var('committed_capital_residual_tolerance', 0.01) %}
-- Tolerance for the power-of-1000 scale-ratio match (0.5% of the target factor).
{%- set scale_match_tolerance = 0.005 %}

with golden as (
    select
        fund_hk,
        golden_fund_key,
        committed_capital_usd as golden_committed_capital_usd,
        committed_capital_usd__source as golden_source
    from {{ ref('bv_fund_golden_record') }}
),

{%- for source in sources %}
current_{{ source.name | lower }} as (
    select
        fund_hk,
        committed_capital_usd,
        currency,
        as_of_date,
        load_datetime
    from {{ ref(source.model) }}
    qualify row_number() over (
        partition by fund_hk
        order by effective_from desc
    ) = 1
),
{%- endfor %}

-- Tall union: one row per (source, fund) carrying that source's asserted value
-- and the metadata the dimension flags read. This IS the per-source discrepancy
-- surface -- the golden record collapses it to a single winner, so it is built
-- here rather than re-taken from the survivorship-collapsed row.
source_values as (
    {%- for source in sources %}
    select
        '{{ source.name }}' as source_name,
        fund_hk,
        committed_capital_usd as value,
        currency,
        as_of_date,
        load_datetime
    from current_{{ source.name | lower }}
    where committed_capital_usd is not null
    {% if not loop.last %}union all{% endif %}
    {%- endfor %}
),

-- Ordered source pairs that genuinely disagree. source_a < source_b keeps each
-- unordered disagreement to one row; value_a <> value_b drops the agreements.
pairs as (
    select
        a.fund_hk,
        a.source_name as source_a,
        b.source_name as source_b,
        a.value as value_a,
        b.value as value_b,
        a.currency as currency_a,
        b.currency as currency_b,
        a.as_of_date as as_of_a,
        b.as_of_date as as_of_b,
        a.load_datetime as load_datetime_a,
        b.load_datetime as load_datetime_b
    from source_values as a
    inner join source_values as b
        on a.fund_hk = b.fund_hk
        and a.source_name < b.source_name
    where a.value <> b.value
),

decomposed as (
    select
        pairs.*,
        (value_a - value_b) as total_delta,
        -- Power-of-1000 scale ratio between the two magnitudes (>= 1 by
        -- construction). NULL when the smaller magnitude is 0 (no scale
        -- interpretation possible).
        case
            when least(abs(value_a), abs(value_b)) <> 0
                then greatest(abs(value_a), abs(value_b))
                     / nullif(least(abs(value_a), abs(value_b)), 0)
        end as scale_ratio
    from pairs
),

flagged as (
    select
        decomposed.*,
        -- The exact power of 1000 the ratio lands on (within tolerance), else NULL.
        case
            when scale_ratio is not null
                and abs(scale_ratio - 1000) <= 1000 * {{ scale_match_tolerance }}
                then 1000
            when scale_ratio is not null
                and abs(scale_ratio - 1000000) <= 1000000 * {{ scale_match_tolerance }}
                then 1000000
        end as exact_scale_power,
        {{ dpf_date_diff_days('as_of_a', 'as_of_b') }} as as_of_gap_days,
        case when currency_a <> currency_b then true else false end as native_currency_mismatch
    from decomposed
),

-- At the tolerance margin, the
-- unit_scale component explains only the portion an EXACT rescale closes,
-- scaled_b - value_b, never the whole delta. A ratio near-but-not-exactly a power
-- of 1000 (e.g. 1,000,500x) leaves value_a - scaled_b genuinely unexplained, and
-- that remainder must surface in the residual, not be silently absorbed.
attributed as (
    select
        flagged.*,
        case when exact_scale_power is not null then true else false end as is_scale_mismatch,
        case
            when exact_scale_power is null then cast(0 as {{ dbt.type_numeric() }})
            when abs(value_a) >= abs(value_b)
                then (value_b * exact_scale_power) - value_b
            else (value_b / exact_scale_power) - value_b
        end as component_unit_scale
    from flagged
)

select
    {{ dbt_utils.generate_surrogate_key(['attributed.fund_hk', 'disputed_attribute', 'source_a', 'source_b']) }} as committed_capital_decomposition_key,
    attributed.fund_hk,
    golden.golden_fund_key,
    disputed_attribute,
    source_a,
    source_b,
    value_a,
    value_b,
    currency_a,
    currency_b,
    as_of_a,
    as_of_b,
    golden.golden_committed_capital_usd,
    golden.golden_source,
    total_delta,

    -- Explained components. Each must be a number so the identity holds exactly;
    -- the ones the data cannot support are 0 and carry a companion flag.
    component_unit_scale,
    cast(0 as {{ dbt.type_numeric() }}) as component_fx_currency,
    cast(0 as {{ dbt.type_numeric() }}) as component_as_of_timing,
    cast(0 as {{ dbt.type_numeric() }}) as component_scope_universe,
    cast(0 as {{ dbt.type_numeric() }}) as component_methodology,

    -- Residual = delta minus everything explained. Only unit_scale can be
    -- non-zero, so residual = total_delta - component_unit_scale; written as the
    -- full subtraction to keep every declared component explicit.
    -- At the scale-match tolerance margin this residual is genuinely non-zero
    -- (see the attributed CTE) rather than forced to 0.
    (
        total_delta
        - component_unit_scale
        - 0  -- component_fx_currency
        - 0  -- component_as_of_timing
        - 0  -- component_scope_universe
        - 0  -- component_methodology
    ) as unexplained_residual,

    -- Dimension flags: which explanations are PLAUSIBLE for the residual even
    -- where their magnitude is not computable from the vault.
    is_scale_mismatch,
    native_currency_mismatch,
    as_of_gap_days,
    case when as_of_gap_days is not null and as_of_gap_days <> 0 then true else false end as as_of_dimension_present,

    -- Residual materiality, relative to the delta so it is scale-invariant.
    abs(total_delta - component_unit_scale) as residual_abs,
    case
        when abs(total_delta) <> 0
            then abs(total_delta - component_unit_scale) / nullif(abs(total_delta), 0)
    end as residual_fraction_of_delta,
    case
        when abs(total_delta - component_unit_scale)
             > abs(total_delta) * {{ residual_tolerance_fraction }}
            then true
        else false
    end as residual_over_tolerance
from attributed
cross join (select 'committed_capital_usd' as disputed_attribute) as attr
left join golden
    on golden.fund_hk = attributed.fund_hk
