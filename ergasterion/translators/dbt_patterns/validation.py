"""The Data Validation pattern, rendered for dbt.

Architecture section 4 gives Data Validation rules by type, an optional
stage, a threshold and an on-failure policy, and requires it to emit tests,
a quarantine relation and an abort condition, never a silent pass-through.
The three land on dbt like this.

The occurrence cuts the product's chain (``segment``). Everything rendered
so far becomes a checked relation carrying every row reaching the
occurrence plus one boolean column per declared rule, true exactly when
that row breaks that rule. A violation that cannot be decided counts as a
violation: the column is coalesced to true, so an unknown never passes as
a pass.

Beside it sits the quarantine relation: one row per failing row per broken
rule, naming the rule. It is a table, so the evidence of a run survives the
run.

The declared ``on_failure`` policy decides the rest, and there is no
default: a policy nobody declared is not one the engine picks.

  * ``quarantine`` diverts. The product's chain resumes from the checked
    relation keeping only rows that broke no rule, so the published
    relation carries none of them, and each rule additionally gets a
    threshold test at error severity: over the declared ``error_threshold``
    the run aborts naming the rule, under it the rows stay quarantined and
    the run continues. An ``error_threshold`` is required, because
    quarantine-and-continue without a ceiling is a silent pass-through of
    any volume of bad data.
  * ``abort`` stops. Every rule's test runs at error severity on the
    checked relation, so a single failing row fails the build and nothing
    downstream of it publishes. Rows are not diverted: a product that
    aborts on a rule has nothing to divert them into.
  * ``warn`` records. Every rule's test runs at warn severity, the rows
    reach the published relation, and the quarantine relation is the
    evidence that they did.

A rule that reconciles against the upstream contracts a consolidating
product combines states no check on a column of this relation at all: it
is the coverage proof architecture section 8 asks of a consolidated
product. It renders as one generated test per source over the relation the
product publishes, asserting that every key of that source's read is still
there -- the declared merge keys, or, for a union, the columns of the one
schema it publishes.

Every other rule renders as a generated dbt test on the checked relation,
in dbt's nested ``arguments`` form: dbt's own ``not_null``, ``unique`` and
``accepted_values`` where they say exactly what the rule says, and the
generic tests in ``macros/quarantine.sql`` for the completeness share, the
declared bounds and the declared pattern.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from ergasterion.framework.contract import COMBINE_METHOD_MERGE, OpeningComposition
from ergasterion.framework.models import RelationField
from ergasterion.translators.dbt_patterns.schema_doc import (
    SEVERITY_ERROR,
    SEVERITY_WARN,
    GeneratedTest,
)
from ergasterion.translators.dbt_patterns.segment import Companion, Segment
from ergasterion.translators.dbt_patterns.sql import (
    RenderingError,
    suffixed_model_name,
    identifier,
    ref,
    select_projection,
    sql_string_literal,
    value_literal,
)
from ergasterion.translators.dbt_patterns.steps import Cte, cte_name

PATTERN = "data_validation"

POLICY_QUARANTINE = "quarantine"
POLICY_ABORT = "abort"
POLICY_WARN = "warn"
POLICIES: tuple[str, ...] = (POLICY_QUARANTINE, POLICY_ABORT, POLICY_WARN)

CHECKED_SUFFIX = "checked"
QUARANTINE_SUFFIX = "quarantine"

VIOLATION_PREFIX = "violation_"
RULE_NAME_COLUMN = "rule_name"
QUARANTINE_INPUT_CTE = "quarantined"

VIEW_CONFIG = "{{ config(materialized='view') }}"
TABLE_CONFIG = "{{ config(materialized='table') }}"

# The generic tests macros/quarantine.sql carries, beside the three dbt
# ships that say exactly what a declared rule says.
TEST_COMPLETENESS = "dpf_completeness"
TEST_RANGE = "dpf_range"
TEST_MATCHES_REGEX = "dpf_matches_regex"
TEST_ERROR_THRESHOLD = "dpf_error_threshold"

# The coverage proof a consolidating product states about the contracts it
# consolidates (architecture section 8) and the one metric it is stated in.
# A rule declaring another metric fails closed: a metric nobody renders
# would be a guarantee nothing proves.
RECONCILE_KEY = "reconcile"
RECONCILE_METRIC = "row_count"
TEST_RECONCILE_COVERAGE = "dpf_reconcile_coverage"


@dataclass(frozen=True)
class CompiledRule:
    """One declared check as the rendering needs it: the identity it is
    named by, the boolean expression that is true for a row breaking it,
    the column that expression is computed into, and the dbt test that
    states the same thing independently."""

    rule_id: str
    field: str
    column: str
    violation: str
    test: str
    arguments: dict[str, Any]


def stage_suffix(step: dict, base: str) -> str:
    """The suffix a validation occurrence's relations carry. A repeated
    pattern declares a distinct stage per occurrence (validation before and
    after transformation), and layer-1 validation rejects two occurrences
    sharing one, so the stage alone always tells them apart."""

    stage = step.get("stage")
    return base if stage is None else f"{base}_{stage}"


def _policy(step: dict, *, product: str, occurrence: str) -> str:
    policy = step.get("on_failure")
    if policy not in POLICIES:
        raise RenderingError(
            product=product,
            occurrence=occurrence,
            rule="undeclared_on_failure_policy",
            detail=(
                f"on_failure {policy!r} is not declared; name one of "
                f"{', '.join(POLICIES)}, because a validation occurrence declaring no policy "
                "would pass its failing rows through in silence"
            ),
        )
    return str(policy)


def _threshold(step: dict, *, product: str, occurrence: str) -> float:
    threshold = step.get("error_threshold")
    if not isinstance(threshold, (int, float)) or isinstance(threshold, bool):
        raise RenderingError(
            product=product,
            occurrence=occurrence,
            rule="undeclared_error_threshold",
            detail=(
                f"on_failure {POLICY_QUARANTINE!r} needs the error_threshold the run aborts "
                f"above, and this occurrence declares {threshold!r}"
            ),
        )
    return float(threshold)


def _checks(rule: dict, *, column: str, product: str, tag: str) -> list[CompiledRule]:
    """Every check one declared rule entry states, in a fixed order, so two
    emissions of the same declaration place the same violation columns."""

    checks: list[CompiledRule] = []
    if rule.get("not_null") is True:
        checks.append(
            CompiledRule(
                rule_id=f"{column}_not_null",
                field=column,
                column="",
                violation=f"{column} is null",
                test="not_null",
                arguments={},
            )
        )
    if "completeness" in rule:
        checks.append(
            CompiledRule(
                rule_id=f"{column}_completeness",
                field=column,
                column="",
                violation=f"{column} is null",
                test=TEST_COMPLETENESS,
                arguments={"rule": f"{column}_completeness", "threshold": rule["completeness"]},
            )
        )
    if rule.get("unique") is True:
        checks.append(
            CompiledRule(
                rule_id=f"{column}_unique",
                field=column,
                column="",
                violation=f"{column} is not null and count(*) over (partition by {column}) > 1",
                test="unique",
                arguments={},
            )
        )
    if "allowed_values" in rule:
        literals = ", ".join(
            value_literal(value, product=product, occurrence=tag)
            for value in rule["allowed_values"]
        )
        checks.append(
            CompiledRule(
                rule_id=f"{column}_allowed_values",
                field=column,
                column="",
                violation=f"{column} is not null and {column} not in ({literals})",
                test="accepted_values",
                arguments={"values": list(rule["allowed_values"])},
            )
        )
    if "min" in rule or "max" in rule:
        bounds: list[str] = []
        arguments: dict[str, Any] = {"rule": f"{column}_range"}
        if "min" in rule:
            literal = value_literal(rule["min"], product=product, occurrence=tag)
            bounds.append(f"{column} < {literal}")
            arguments["min_value"] = literal
        if "max" in rule:
            literal = value_literal(rule["max"], product=product, occurrence=tag)
            bounds.append(f"{column} > {literal}")
            arguments["max_value"] = literal
        joined = " or ".join(bounds)
        checks.append(
            CompiledRule(
                rule_id=f"{column}_range",
                field=column,
                column="",
                violation=f"{column} is not null and ({joined})",
                test=TEST_RANGE,
                arguments=arguments,
            )
        )
    if "regex" in rule:
        pattern = sql_string_literal(str(rule["regex"]))
        call = '{{ dpf_regexp_contains("' + column + '", "' + pattern + '") }}'
        checks.append(
            CompiledRule(
                rule_id=f"{column}_regex",
                field=column,
                column="",
                violation=f"{column} is not null and not {call}",
                test=TEST_MATCHES_REGEX,
                arguments={"rule": f"{column}_regex", "pattern": pattern},
            )
        )
    return checks


def compile_rules(
    step: dict, *, product: str, occurrence: str, fields: Sequence[RelationField]
) -> tuple[CompiledRule, ...]:
    """Every declared rule as a compiled check, in declaration order. A
    rule that names no column the composition carries here, and a rule
    declaring no check at all, both fail closed: either would state a
    guarantee that silently checks nothing."""

    visible = {entry.name for entry in fields}
    compiled: list[CompiledRule] = []
    for rule_index, rule in enumerate(step.get("rules") or []):
        tag = f"{occurrence}.rules[{rule_index}]"
        if RECONCILE_KEY in rule:
            # A reconciliation constrains no column of this relation: it is
            # a statement about the upstream contracts the product
            # consolidates, rendered by ``reconcile_tests`` below.
            continue
        name = rule.get("field")
        if not isinstance(name, str) or name not in visible:
            raise RenderingError(
                product=product,
                occurrence=tag,
                rule="unresolved_rule_field",
                detail=(
                    f"rule field {name!r} is not a column the composition carries here: "
                    f"{sorted(visible)}; a rule constraining no column of this relation is not "
                    "one this route renders"
                ),
            )
        column = identifier(name, product=product, occurrence=tag)
        checks = _checks(rule, column=column, product=product, tag=tag)
        if not checks:
            raise RenderingError(
                product=product,
                occurrence=tag,
                rule="empty_validation_rule",
                detail=(
                    f"the rule on field {name!r} declares no check, so it would state a "
                    "guarantee nothing tests"
                ),
            )
        compiled.extend(checks)

    return tuple(
        CompiledRule(
            rule_id=rule.rule_id,
            field=rule.field,
            column=f"{VIOLATION_PREFIX}{position:02d}",
            violation=rule.violation,
            test=rule.test,
            arguments=rule.arguments,
        )
        for position, rule in enumerate(compiled)
    )


def _quarantine_body(rules: Sequence[CompiledRule], columns: Sequence[str]) -> str:
    """The quarantine relation's body: one select per compiled rule,
    naming the rule beside every row that broke it."""

    blocks: list[str] = []
    for rule in rules:
        projection = select_projection(
            (sql_string_literal(rule.rule_id) + f" as {RULE_NAME_COLUMN}", *columns),
            QUARANTINE_INPUT_CTE,
            where=rule.column,
        )
        blocks.append(projection)
    return "\n\n    union all\n\n".join(blocks)


def reconcile_tests(
    step: dict,
    *,
    product: str,
    occurrence: str,
    published_model: str | None,
    published_columns: Sequence[str],
    composition: OpeningComposition,
    source_models: Mapping[str, str],
) -> tuple[GeneratedTest, ...]:
    """The coverage a consolidating product proves against the contracts it
    consolidates (architecture section 8: "a consolidated product proves
    its coverage with a Data Validation occurrence whose rules reference
    its upstream contracts").

    One generated test per contract the rule names: every key of that
    source's read must appear in the relation the product publishes. The
    keys are the ones the combination is made on -- the declared merge
    keys, or, for a union, the columns of the one schema it publishes --
    so the proof is stated in the same terms the composition was.

    Fails closed naming the product, the source and the key when the rule
    sits on a product that combines nothing, when it names a contract the
    product does not read, when the relation the product publishes does
    not carry a key the proof needs, or when it declares a metric this
    rendering does not prove."""

    rules = [
        (index, rule)
        for index, rule in enumerate(step.get("rules") or [])
        if RECONCILE_KEY in (rule or {})
    ]
    if not rules:
        return ()

    tests: list[GeneratedTest] = []
    for rule_index, rule in rules:
        tag = f"{occurrence}.rules[{rule_index}]"
        reconciliation = rule[RECONCILE_KEY] or {}
        metric = reconciliation.get("metric")
        if metric != RECONCILE_METRIC:
            raise RenderingError(
                product=product,
                occurrence=tag,
                rule="unrenderable_reconciliation_metric",
                detail=(
                    f"metric {metric!r} is not one this route proves; it proves "
                    f"{RECONCILE_METRIC!r}, as every key of each source's read appearing in "
                    "the relation this product publishes"
                ),
            )
        if composition.method is None:
            raise RenderingError(
                product=product,
                occurrence=tag,
                rule="reconciliation_without_a_combination",
                detail=(
                    "a reconciliation proves that what two or more sources brought is still "
                    "here, and this product combines none: it reads "
                    f"{len(composition.sources)} source(s)"
                ),
            )
        if published_model is None:
            raise RenderingError(
                product=product,
                occurrence=tag,
                rule="reconciliation_over_a_shape_of_several_relations",
                detail=(
                    "this product's shape publishes more than one relation, and a "
                    "reconciliation is stated over the one relation the composition publishes"
                ),
            )
        keys = (
            composition.keys
            if composition.method == COMBINE_METHOD_MERGE
            else tuple(entry.name for entry in composition.fields)
        )
        carried = set(published_columns)
        by_producer = {
            source.contract.split("@", 1)[0]: source for source in composition.sources
        }
        for contract in reconciliation.get("contracts") or []:
            producer = str(contract).split("@", 1)[0]
            source = by_producer.get(producer)
            if source is None:
                raise RenderingError(
                    product=product,
                    occurrence=tag,
                    rule="reconciliation_source_not_read",
                    detail=(
                        f"contract {str(contract)!r} is not one this product's sources read: "
                        f"{sorted(by_producer)}"
                    ),
                )
            brought = {column.name: column.source_name for column in source.columns}
            for key in keys:
                if key not in brought:
                    raise RenderingError(
                        product=product,
                        occurrence=tag,
                        rule="unreconcilable_key",
                        detail=(
                            f"source {str(contract)!r} does not bring key {key!r} the "
                            "combination is made on"
                        ),
                    )
                if key not in carried:
                    raise RenderingError(
                        product=product,
                        occurrence=tag,
                        rule="unreconcilable_key",
                        detail=(
                            f"source {str(contract)!r} is proved by key {key!r}, which the "
                            f"relation this product publishes does not carry: "
                            f"{sorted(carried)}"
                        ),
                    )
            source_model = source_models[source.key]
            tests.append(
                GeneratedTest(
                    model=published_model,
                    test=TEST_RECONCILE_COVERAGE,
                    name=f"{TEST_RECONCILE_COVERAGE}_{published_model}_{source_model}",
                    column=None,
                    arguments={
                        "source": f"ref('{source_model}')",
                        "keys": [
                            identifier(key, product=product, occurrence=tag) for key in keys
                        ],
                        # The same columns as this source names them, in
                        # the same order: a union source is read under its
                        # own names and conformed as it is stacked, so the
                        # proof compares the two namings, not one of them
                        # twice.
                        "source_columns": [
                            identifier(brought[key], product=product, occurrence=tag)
                            for key in keys
                        ],
                    },
                    severity=SEVERITY_ERROR,
                    store_failures=True,
                )
            )
    return tuple(tests)


def render_segment(
    *,
    index: int,
    step: dict,
    product: str,
    domain: str,
    name: str,
    fields: Sequence[RelationField],
    previous: str,
    published_model: str | None,
    published_columns: Sequence[str],
    composition: OpeningComposition,
    source_models: Mapping[str, str],
) -> Segment:
    """The checked relation, the quarantine relation, the generated tests
    and the chain the product resumes with, for one validation occurrence.

    A rule that constrains a column of this relation is compiled into the
    checked relation and its own test; a rule that reconciles against the
    contracts a consolidating product combines is rendered as a coverage
    test over the relation the product publishes
    (``published_model``)."""

    occurrence = f"steps[{index}]:{PATTERN}"
    policy = _policy(step, product=product, occurrence=occurrence)
    rules = compile_rules(step, product=product, occurrence=occurrence, fields=fields)
    coverage = reconcile_tests(
        step,
        product=product,
        occurrence=occurrence,
        published_model=published_model,
        published_columns=published_columns,
        composition=composition,
        source_models=source_models,
    )
    suffix = stage_suffix(step, CHECKED_SUFFIX)
    checked_model = suffixed_model_name(domain, name, suffix)

    field_columns = tuple(
        identifier(entry.name, product=product, occurrence=occurrence) for entry in fields
    )
    violation_columns = tuple(rule.column for rule in rules)
    # A violation that cannot be decided is a violation: an unknown must
    # never leave a row on the published side of a quarantine.
    checked_columns = field_columns + tuple(
        f"coalesce({rule.violation}, true) as {rule.column}" for rule in rules
    )

    quarantine = Companion(
        suffix=stage_suffix(step, QUARANTINE_SUFFIX),
        model_config=TABLE_CONFIG,
        ctes=(
            Cte(
                name=QUARANTINE_INPUT_CTE,
                body=select_projection(field_columns + violation_columns, ref(checked_model)),
            ),
            Cte(name="violations", body=_quarantine_body(rules, field_columns)),
        ),
        columns=(RULE_NAME_COLUMN,) + field_columns,
        final_cte="violations",
        purpose="one row per failing row per broken rule, naming the rule",
        description=(
            "One row per failing row per broken validation rule, naming the rule it broke."
        ),
    )

    severity = SEVERITY_ERROR if policy == POLICY_ABORT else SEVERITY_WARN
    tests: list[GeneratedTest] = [*coverage] + [
        GeneratedTest(
            model=checked_model,
            test=rule.test,
            name=f"dpf_{suffix}_{rule.rule_id}",
            column=rule.field,
            arguments=dict(rule.arguments),
            severity=severity,
        )
        for rule in rules
    ]
    if policy == POLICY_QUARANTINE:
        threshold = _threshold(step, product=product, occurrence=occurrence)
        tests.extend(
            GeneratedTest(
                model=checked_model,
                test=TEST_ERROR_THRESHOLD,
                name=f"dpf_{suffix}_threshold_{rule.rule_id}",
                column=None,
                arguments={
                    "violation_column": rule.column,
                    "rule": rule.rule_id,
                    "max_rate": threshold,
                },
                severity=SEVERITY_ERROR,
            )
            for rule in rules
        )
        resume_columns = field_columns + violation_columns
        kept = " and ".join(f"not {column}" for column in violation_columns)
        resume_ctes: tuple[Cte, ...] = (
            Cte(
                name=cte_name(index, PATTERN),
                body=select_projection(field_columns, f"{suffix}_input", where=kept),
            ),
        )
    else:
        resume_columns = field_columns
        resume_ctes = ()

    return Segment(
        suffix=suffix,
        model_config=VIEW_CONFIG,
        ctes=(),
        columns=checked_columns,
        final_cte=previous,
        purpose=(
            "every row reaching a data_validation occurrence, with one column per declared "
            "rule that is true when the row breaks it"
        ),
        description=(
            "Every row reaching this product's validation, with the violation each declared "
            "rule computed for it."
        ),
        resume_columns=resume_columns,
        resume_ctes=resume_ctes,
        companions=(quarantine,),
        tests=tuple(tests),
        test_columns=tuple(dict.fromkeys(rule.field for rule in rules)),
    )


__all__ = [
    "CHECKED_SUFFIX",
    "RECONCILE_KEY",
    "RECONCILE_METRIC",
    "TEST_RECONCILE_COVERAGE",
    "POLICIES",
    "POLICY_ABORT",
    "POLICY_QUARANTINE",
    "POLICY_WARN",
    "QUARANTINE_SUFFIX",
    "CompiledRule",
    "compile_rules",
    "reconcile_tests",
    "render_segment",
    "stage_suffix",
]
