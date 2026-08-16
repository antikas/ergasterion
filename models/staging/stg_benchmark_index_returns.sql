-- Hand-authored staging model. Not part of the ergasterion/emit.py
-- declarations pipeline: benchmark index levels are a single reference/config
-- data source with no multi-source entity resolution to do, so this follows
-- the same "reference seed, cast, feed straight to a mart" shape already used
-- by seeds/hurdle_config.csv -> models/calculated_fields/cf_fund_hurdle.sql,
-- rather than standing up a full raw_vault hub/link/satellite chain for it.
{{ config(materialized='table', schema='staging') }}

with source as (
    select * from {{ ref('benchmark_index_returns') }}
)

select
    cast(benchmark_id as string) as benchmark_id,
    cast(benchmark_name as string) as benchmark_name,
    {{ dpf_safe_cast('index_date', 'date') }} as index_date,
    {{ dpf_safe_cast('index_value', 'numeric') }} as index_value,
    cast(currency as string) as currency,
    cast(source_system as string) as source_system
from source
