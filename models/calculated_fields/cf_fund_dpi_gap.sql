{{ config(materialized='table', schema='calculated_fields') }}

{% set dpi_gap_spec = {
    'name': 'fund_dpi_gap_to_one',
    'base_model': 'fact_fund_performance',
    'base_alias': 'performance',
    'grain': [
        'fund_performance_key',
        'fund_key',
        'fund_id',
        'performance_as_of_date'
    ],
    'inputs': {
        'dpi': 'performance.dpi',
        'target_dpi': 'cast(1 as numeric)'
    },
    'pass_through': [
        'dpi',
        'target_dpi'
    ],
    'outputs': [
        {
            'name': 'dpi_gap_to_one',
            'expression': 'case when dpi is null then null else target_dpi - dpi end',
            'output_type': 'numeric'
        },
        {
            'name': 'dpi_target_reached',
            'expression': 'case when dpi is null then null else dpi >= target_dpi end',
            'output_type': 'boolean'
        }
    ]
} %}

{{ calculated_field(dpi_gap_spec) }}
