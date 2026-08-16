{{ config(materialized='table', schema='calculated_fields') }}

{% set hurdle_spec = {
    'name': 'fund_hurdle',
    'base_model': 'fact_fund_performance',
    'base_alias': 'performance',
    'grain': [
        'fund_performance_key',
        'fund_key',
        'fund_id',
        'shared_external_id',
        'performance_as_of_date'
    ],
    'joins': [
        {
            'model': 'dim_fund',
            'alias': 'fund',
            'type': 'left',
            'on': [
                'fund.fund_key = performance.fund_key'
            ]
        },
        {
            'model': 'hurdle_config',
            'alias': 'fund_hurdle_config',
            'type': 'left',
            'on': [
                "fund_hurdle_config.config_scope = 'fund'",
                'fund_hurdle_config.fund_id = performance.shared_external_id'
            ]
        },
        {
            'model': 'hurdle_config',
            'alias': 'strategy_hurdle_config',
            'type': 'left',
            'on': [
                "strategy_hurdle_config.config_scope = 'strategy'",
                'strategy_hurdle_config.strategy = fund.strategy'
            ]
        }
    ],
    'inputs': {
        'tvpi': 'performance.tvpi',
        'dpi': 'performance.dpi',
        'rvpi': 'performance.rvpi',
        'moic': 'performance.moic',
        'irr': 'performance.irr',
        'hurdle_type': 'coalesce(fund_hurdle_config.hurdle_type, strategy_hurdle_config.hurdle_type)',
        'hurdle_return_metric': 'coalesce(fund_hurdle_config.return_metric, strategy_hurdle_config.return_metric)',
        'hurdle_rate': dpf_safe_cast('coalesce(fund_hurdle_config.hurdle_rate, strategy_hurdle_config.hurdle_rate)', 'numeric'),
        'hurdle_config_scope': "case when fund_hurdle_config.hurdle_type is not null then 'fund' when strategy_hurdle_config.hurdle_type is not null then 'strategy' else 'unconfigured' end",
        'hurdle_config_source': 'coalesce(fund_hurdle_config.config_source, strategy_hurdle_config.config_source)',
        'hurdle_return_value': "case lower(coalesce(fund_hurdle_config.return_metric, strategy_hurdle_config.return_metric)) when 'tvpi' then performance.tvpi when 'dpi' then performance.dpi when 'rvpi' then performance.rvpi when 'moic' then performance.moic when 'irr' then " ~ dpf_safe_cast('performance.irr', 'numeric') ~ " end"
    },
    'pass_through': [
        'tvpi',
        'dpi',
        'rvpi',
        'moic',
        'irr',
        'hurdle_type',
        'hurdle_return_metric',
        'hurdle_rate',
        'hurdle_config_scope',
        'hurdle_config_source',
        'hurdle_return_value'
    ],
    'outputs': [
        {
            'name': 'hurdle_cleared',
            'expression': 'case when hurdle_rate is null or hurdle_return_value is null then null else hurdle_return_value >= hurdle_rate end',
            'output_type': 'boolean'
        },
        {
            'name': 'excess_over_hurdle',
            'expression': 'case when hurdle_rate is null or hurdle_return_value is null then null else hurdle_return_value - hurdle_rate end',
            'output_type': 'numeric'
        }
    ]
} %}

{{ calculated_field(hurdle_spec) }}
