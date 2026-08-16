-- Append-only decision-log durability test. A total row-count assertion would reject
-- valid new decisions and could miss a truncate-then-refixture cycle, so this test has
-- two independent arms:
--
--   (a) Fixture presence: the three fixture decisions this estate seeds
--       (estate-side, seeds/deal_decision_log_fixtures.csv, merged into the log by the
--       generic dpf_merge_seed_into_append_only_log post-hook) must exist with their
--       exact decision values, scoped by decision_id. This catches missing or corrupted
--       fixtures without constraining the number of real decisions.
--
--   (b) Structural ownership: using dbt's compile-time manifest, assert that no
--       model, seed, or snapshot node in this
--       project materializes to a relation named `deal_decision_log` or
--       `entity_resolution_decisions_log` (case-insensitive match on the node's
--       alias or name). If anyone adds a model,
--       seed, or snapshot that materializes to either log's relation name, this test
--       fails at compile time. This structural guarantee keeps dbt from owning either
--       relation's lifecycle. The two logs
--       stay `source`-declared only (models/entity_resolution/_entity_resolution.yml,
--       models/deal_approvals/_deal_approvals.yml), created and fixture-seeded solely
--       by the on-run-start hooks in dbt_project.yml.
--
-- The test passes when it returns zero rows.
--
-- The fixture-presence arm reads the append-only log, whose rows
-- are merged in by the post-hook on the deal_decision_log_fixtures seed. This depends_on
-- edge makes that seed (and its post-hook) run before this test, so arm (a) sees the
-- fixtures on a fresh build.
-- depends_on: {{ ref('deal_decision_log_fixtures') }}

{% set forbidden_relation_names = ['deal_decision_log', 'entity_resolution_decisions_log'] -%}
{%- set offending_node_ids = [] -%}
{%- for node in graph.nodes.values() -%}
    {%- if node.resource_type in ('model', 'seed', 'snapshot') -%}
        {%- set node_relation_name = (node.get('alias') or node.get('name') or '') | lower -%}
        {%- if node_relation_name in forbidden_relation_names -%}
            {%- do offending_node_ids.append(node.unique_id ~ ' -> ' ~ node_relation_name) -%}
        {%- endif -%}
    {%- endif -%}
{%- endfor -%}

with fixture_expected as (
    select 'DEC-ORIGO-EXT-DUP-B-01' as decision_id, 'defer' as expected_decision
    union all
    select 'DEC-ORIGO-EXT-DUP-B-02' as decision_id, 'approve_with_conditions' as expected_decision
    union all
    select 'DEC-ORIGO-EXT-001-01' as decision_id, 'defer' as expected_decision
),

fixture_actual as (
    select * from {{ source('deal_decision_raw', 'deal_decision_log') }}
),

fixture_check as (
    select
        fixture_expected.decision_id,
        fixture_expected.expected_decision,
        fixture_actual.decision as actual_decision
    from fixture_expected
    left join fixture_actual
        on fixture_actual.decision_id = fixture_expected.decision_id
)

select
    'fixture_missing_or_wrong_decision' as failure_type,
    fixture_check.decision_id || ': expected ' || fixture_check.expected_decision
        || ', got ' || coalesce(fixture_check.actual_decision, 'MISSING') as failure_detail
from fixture_check
where fixture_check.actual_decision is null
   or fixture_check.actual_decision != fixture_check.expected_decision

{% if offending_node_ids | length > 0 %}
union all
select
    'structural_unmanaged_log_relation_materialized' as failure_type,
    {{ "'" ~ (offending_node_ids | join(' | ')) ~ "'" }} as failure_detail
{% endif %}
