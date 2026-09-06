"""The Data Curation pattern, rendered for dbt.

Architecture section 4 gives Data Curation an entity, a resolution
strategy, keys, survivorship and merge rules, and requires it to emit one
surviving record per entity plus resolution evidence, with explicit
precedence and whatever is unresolved left visible. The occurrence cuts
the product's chain (``segment``) into three artefacts.

**The resolution relation.** Everything rendered so far, plus the
resolution itself: each input record keyed by the column that identifies
it, one edge per declared deterministic key the record carries, and the
cluster each record belongs to. Clusters come from bounded minimum-label
propagation over the record-to-key-value graph, unrolled to the declared
number of rounds. Two records join one cluster when they share any declared
key value, transitively, which is what a ranked key list means: a record
carrying only the second key still merges with records carrying the first,
as long as some chain of shared values connects them. The unroll is
written out round by round rather than as a recursive query because both
declared adapters forbid an aggregate in a recursive term, and each round's
label pull is an aggregate. The declared round count is therefore part of
the declaration, not a constant here: it must cover the widest cluster the
estate expects.

**The pending-key relation.** The resolution evidence, as a table. Under a
deterministic strategy it lists every record no declared key merged with
anything, so what is unresolved stays visible. Under a probabilistic
strategy it also carries the candidate pairs: the pending records blocked
on the declared columns, scored by the declared named rule, and classified
into the declared review band. A pair reaching the review band is evidence
for a decision, not a decision: only a declared key merges records into an
entity, so a probabilistic match enters the estate as a key someone
declared after reviewing it, never as an inferred merge behind the
contract.

**The surviving record.** The product's chain resumes from the resolution
relation with one row per resolved entity. Every column carries the value
the declared survivorship strategy picks: ``first_non_null`` takes the
first non-null value in declared source order; ``most_recent`` takes the
value of the most recent record, in declared source order on a tie. Both
break a remaining tie on the record key, so a rebuild over unchanged input
always picks the same row. Every column visible at the occurrence must
declare a strategy: collapsing several records into one without saying
which value survives is exactly the inference architecture section 13
forbids.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from ergasterion.framework.models import RelationField
from ergasterion.translators.dbt_patterns.segment import Companion, Segment
from ergasterion.translators.dbt_patterns.sql import (
    RenderingError,
    suffixed_model_name,
    identifier,
    jinja_literal,
    ref,
    select_projection,
    sql_string_literal,
)
from ergasterion.translators.dbt_patterns.steps import Cte

PATTERN = "data_curation"

STRATEGY_DETERMINISTIC = "deterministic"
STRATEGY_PROBABILISTIC = "probabilistic"

SURVIVORSHIP_FIRST_NON_NULL = "first_non_null"
SURVIVORSHIP_MOST_RECENT = "most_recent"
SURVIVORSHIP_STRATEGIES: tuple[str, ...] = (
    SURVIVORSHIP_FIRST_NON_NULL,
    SURVIVORSHIP_MOST_RECENT,
)

RESOLUTION_SUFFIX = "resolution"
PENDING_SUFFIX = "pending_keys"

RECORD_KEY_COLUMN = "resolution_record_key"
ENTITY_KEY_COLUMN = "resolution_entity_key"
ROW_COUNT_COLUMN = "resolution_row_count"
KEY_NAME_COLUMN = "resolution_key_name"
TIER_COLUMN = "resolution_tier"
PENDING_COLUMN = "resolution_pending"
RESOLUTION_COLUMNS: tuple[str, ...] = (
    RECORD_KEY_COLUMN,
    ENTITY_KEY_COLUMN,
    ROW_COUNT_COLUMN,
    KEY_NAME_COLUMN,
    TIER_COLUMN,
    PENDING_COLUMN,
)

TIER_DETERMINISTIC = "deterministic"
TIER_UNRESOLVED = "unresolved"

BAND_ACCEPTED = "accepted"
BAND_REVIEW = "review"
BAND_REJECTED = "rejected"
BAND_UNSCORED = "unscored"
BAND_UNRESOLVED = "unresolved"

PAIR_LEFT = "left_side"
PAIR_RIGHT = "right_side"

VIEW_CONFIG = "{{ config(materialized='view') }}"
TABLE_CONFIG = "{{ config(materialized='table') }}"

STRING_TYPE_CALL = "{{ dpf_type('string') }}"
NUMERIC_TYPE_CALL = "{{ dpf_type('numeric') }}"

RECORDS_CTE = "curation_records"
EDGES_CTE = "curation_edges"
COMPONENTS_CTE = "curation_components"
STATS_CTE = "curation_component_stats"
KEYS_CTE = "curation_component_keys"
RESOLVED_CTE = "curation_resolved"
ENTITIES_CTE = "curation_entities"
SURVIVOR_CTE = "curation_survivor"
PENDING_RECORDS_CTE = "pending_records"
CANDIDATE_PAIRS_CTE = "candidate_pairs"
BANDED_CTE = "banded_pairs"


@dataclass(frozen=True)
class ScoringPlan:
    """One probabilistic scoring configuration, resolved: the dbt macro the
    named rule renders as, the columns compared across a candidate pair,
    the columns pairs are blocked on, and the declared band edges."""

    macro: str
    compare: tuple[str, ...]
    block_on: tuple[str, ...]
    lower: float
    upper: float


def _string_column(
    name: object, *, fields: Sequence[RelationField], product: str, occurrence: str, role: str
) -> str:
    """One declared column, checked as visible here and as a string. The
    resolution compares, orders and coalesces these values as text, so a
    column of another type would need a cast whose target differs per
    adapter and whose collation nobody declared."""

    by_name = {entry.name: entry for entry in fields}
    entry = by_name.get(name)
    if entry is None:
        raise RenderingError(
            product=product,
            occurrence=occurrence,
            rule="unresolved_curation_column",
            detail=(
                f"{role} {name!r} is not in the schema visible at this occurrence: "
                f"{sorted(by_name)}"
            ),
        )
    if entry.type != "string":
        raise RenderingError(
            product=product,
            occurrence=occurrence,
            rule="unrenderable_curation_column",
            detail=(
                f"{role} {name!r} is declared as {entry.type!r}; the resolution compares and "
                "orders these values as text, so it must be a string"
            ),
        )
    return identifier(name, product=product, occurrence=occurrence)


def _visible_column(
    name: object, *, fields: Sequence[RelationField], product: str, occurrence: str, role: str
) -> str:
    visible = {entry.name for entry in fields}
    if name not in visible:
        raise RenderingError(
            product=product,
            occurrence=occurrence,
            rule="unresolved_curation_column",
            detail=(
                f"{role} {name!r} is not in the schema visible at this occurrence: "
                f"{sorted(visible)}"
            ),
        )
    return identifier(name, product=product, occurrence=occurrence)


def _scoring_plan(
    step: dict,
    *,
    product: str,
    occurrence: str,
    fields: Sequence[RelationField],
    rule_signatures: Mapping[str, Any],
) -> ScoringPlan | None:
    resolution = step["resolution"]
    strategy = resolution["strategy"]
    scoring = resolution.get("scoring")
    if strategy == STRATEGY_DETERMINISTIC:
        if scoring is not None:
            raise RenderingError(
                product=product,
                occurrence=occurrence,
                rule="scoring_without_probabilistic_strategy",
                detail=(
                    "a deterministic resolution declares scoring, which nothing renders; "
                    f"declare strategy {STRATEGY_PROBABILISTIC!r} or drop the scoring block"
                ),
            )
        return None
    if scoring is None:
        raise RenderingError(
            product=product,
            occurrence=occurrence,
            rule="probabilistic_strategy_without_scoring",
            detail=(
                "a probabilistic resolution needs the named rule that scores one candidate "
                "pair, the columns compared across it, the columns pairs are blocked on and "
                "the review band"
            ),
        )
    compare = tuple(
        _visible_column(
            name, fields=fields, product=product, occurrence=occurrence, role="scoring compare column"
        )
        for name in scoring["compare"]
    )
    block_on = tuple(
        _visible_column(
            name, fields=fields, product=product, occurrence=occurrence, role="scoring block column"
        )
        for name in scoring["block_on"]
    )
    band = scoring["review_band"]
    lower, upper = float(band["lower"]), float(band["upper"])
    if lower > upper:
        raise RenderingError(
            product=product,
            occurrence=occurrence,
            rule="inverted_review_band",
            detail=f"the review band runs from {lower} up to {upper}, which is no band at all",
        )
    # The emission route resolved every rule a declaration references
    # before rendering began, so this is an index, not a lookup with a
    # fallback: a rule no catalogue carries failed closed there.
    rule_name = scoring["rule"]
    signature = rule_signatures[rule_name]
    expected = 2 * len(compare)
    if len(signature.inputs) != expected:
        raise RenderingError(
            product=product,
            occurrence=occurrence,
            rule="scoring_rule_arity",
            detail=(
                f"named rule {rule_name!r} declares {len(signature.inputs)} input(s); scoring "
                f"one candidate pair over {len(compare)} compared column(s) binds {expected}, "
                "the left side then the right side of each column in declared order"
            ),
        )
    return ScoringPlan(
        macro=signature.macro, compare=compare, block_on=block_on, lower=lower, upper=upper
    )


def _survivorship(
    step: dict, *, product: str, occurrence: str, fields: Sequence[RelationField]
) -> tuple[tuple[str, str], ...]:
    declared = step.get("survivorship") or {}
    ordered: list[tuple[str, str]] = []
    for entry in fields:
        strategy = declared.get(entry.name)
        if strategy is None:
            raise RenderingError(
                product=product,
                occurrence=occurrence,
                rule="undeclared_survivorship",
                detail=(
                    f"column {entry.name!r} reaches this occurrence with no survivorship "
                    "strategy; collapsing records into one entity without saying which value "
                    "survives would decide it by accident"
                ),
            )
        if strategy not in SURVIVORSHIP_STRATEGIES:
            raise RenderingError(
                product=product,
                occurrence=occurrence,
                rule="unknown_survivorship_strategy",
                detail=(
                    f"survivorship strategy {strategy!r} for column {entry.name!r} is not one "
                    f"this translator renders: {', '.join(SURVIVORSHIP_STRATEGIES)}"
                ),
            )
        ordered.append(
            (identifier(entry.name, product=product, occurrence=occurrence), str(strategy))
        )
    unknown = sorted(set(declared) - {entry.name for entry in fields})
    if unknown:
        raise RenderingError(
            product=product,
            occurrence=occurrence,
            rule="unresolved_survivorship_column",
            detail=(
                f"survivorship names {', '.join(unknown)}, which the composition does not "
                "carry here"
            ),
        )
    return tuple(ordered)


def _edges_body(keys: Sequence[str]) -> str:
    blocks = []
    for rank, key in enumerate(keys, start=1):
        blocks.append(
            "\n".join(
                [
                    "    select",
                    f"        {RECORD_KEY_COLUMN},",
                    f"        {rank} as key_rank,",
                    f"        {sql_string_literal(key)} as key_name,",
                    f"        {sql_string_literal(key + ':')} || {key} as key_value",
                    f"    from {RECORDS_CTE}",
                    f"    where {key} is not null",
                ]
            )
        )
    return "\n\n    union all\n\n".join(blocks)


def _label_rounds(iterations: int) -> list[Cte]:
    """The unrolled minimum-label propagation: one value round and one
    label round per declared iteration, starting from every record
    labelled with its own key."""

    rounds: list[Cte] = [
        Cte(
            name="curation_labels_0",
            body="\n".join(
                [
                    "    select distinct",
                    f"        {RECORD_KEY_COLUMN},",
                    f"        {RECORD_KEY_COLUMN} as label",
                    f"    from {EDGES_CTE}",
                ]
            ),
        )
    ]
    for round_index in range(1, iterations + 1):
        rounds.append(
            Cte(
                name=f"curation_values_{round_index}",
                body="\n".join(
                    [
                        "    select",
                        "        edges.key_value,",
                        "        min(labelled.label) as value_label",
                        f"    from {EDGES_CTE} as edges",
                        f"    inner join curation_labels_{round_index - 1} as labelled",
                        f"        on labelled.{RECORD_KEY_COLUMN} = edges.{RECORD_KEY_COLUMN}",
                        "    group by edges.key_value",
                    ]
                ),
            )
        )
        rounds.append(
            Cte(
                name=f"curation_labels_{round_index}",
                body="\n".join(
                    [
                        "    select",
                        f"        edges.{RECORD_KEY_COLUMN},",
                        "        min(matched.value_label) as label",
                        f"    from {EDGES_CTE} as edges",
                        f"    inner join curation_values_{round_index} as matched",
                        "        on matched.key_value = edges.key_value",
                        f"    group by edges.{RECORD_KEY_COLUMN}",
                    ]
                ),
            )
        )
    return rounds


def _resolved_body(field_columns: Sequence[str], keys: Sequence[str]) -> str:
    key_name_branches = "\n".join(
        f"            when keys.best_key_rank = {rank} then {sql_string_literal(key)}"
        for rank, key in enumerate(keys, start=1)
    )
    columns = [f"records.{column}" for column in field_columns]
    columns.append(f"records.{RECORD_KEY_COLUMN}")
    columns.append(
        f"coalesce(components.component_id, records.{RECORD_KEY_COLUMN}) as {ENTITY_KEY_COLUMN}"
    )
    columns.append(f"coalesce(stats.component_row_count, 1) as {ROW_COUNT_COLUMN}")
    columns.append(
        "\n".join(["case", key_name_branches, f"        end as {KEY_NAME_COLUMN}"])
    )
    columns.append(
        "\n".join(
            [
                "case",
                f"            when coalesce(stats.component_row_count, 1) > 1 "
                f"then {sql_string_literal(TIER_DETERMINISTIC)}",
                f"            else {sql_string_literal(TIER_UNRESOLVED)}",
                f"        end as {TIER_COLUMN}",
            ]
        )
    )
    columns.append(f"coalesce(stats.component_row_count, 1) = 1 as {PENDING_COLUMN}")
    lines = ["    select"]
    for position, column in enumerate(columns):
        comma = "," if position < len(columns) - 1 else ""
        lines.append(f"        {column}{comma}")
    lines.extend(
        [
            f"    from {RECORDS_CTE} as records",
            f"    left join {COMPONENTS_CTE} as components",
            f"        on components.{RECORD_KEY_COLUMN} = records.{RECORD_KEY_COLUMN}",
            f"    left join {STATS_CTE} as stats",
            "        on stats.component_id = components.component_id",
            f"    left join {KEYS_CTE} as keys",
            "        on keys.component_id = components.component_id",
        ]
    )
    return "\n".join(lines)


def _pending_companion(*, resolution_model: str, scoring: ScoringPlan | None) -> Companion:
    if scoring is None:
        ctes = (
            Cte(
                name=PENDING_RECORDS_CTE,
                body=select_projection(
                    (RECORD_KEY_COLUMN,), ref(resolution_model), where=PENDING_COLUMN
                ),
            ),
            Cte(
                name=BANDED_CTE,
                body=select_projection(
                    (
                        f"{RECORD_KEY_COLUMN} as record_key_a",
                        f"cast(null as {STRING_TYPE_CALL}) as record_key_b",
                        f"cast(null as {NUMERIC_TYPE_CALL}) as match_score",
                        f"{sql_string_literal(BAND_UNRESOLVED)} as review_band",
                    ),
                    PENDING_RECORDS_CTE,
                ),
            ),
        )
    else:
        read_columns = (RECORD_KEY_COLUMN,) + tuple(
            dict.fromkeys(scoring.compare + scoring.block_on)
        )
        arguments = ", ".join(
            jinja_literal(f"{side}.{column}")
            for column in scoring.compare
            for side in (PAIR_LEFT, PAIR_RIGHT)
        )
        call = "{{ " + f"{scoring.macro}({arguments})" + " }}"
        join_conditions = [
            f"{PAIR_LEFT}.{column} = {PAIR_RIGHT}.{column}" for column in scoring.block_on
        ]
        join_conditions.append(
            f"{PAIR_LEFT}.{RECORD_KEY_COLUMN} < {PAIR_RIGHT}.{RECORD_KEY_COLUMN}"
        )
        pair_body = "\n".join(
            [
                "    select",
                f"        {PAIR_LEFT}.{RECORD_KEY_COLUMN} as record_key_a,",
                f"        {PAIR_RIGHT}.{RECORD_KEY_COLUMN} as record_key_b,",
                f"        {call} as match_score",
                f"    from {PENDING_RECORDS_CTE} as {PAIR_LEFT}",
                f"    inner join {PENDING_RECORDS_CTE} as {PAIR_RIGHT}",
                "        on " + "\n       and ".join(join_conditions),
            ]
        )
        band_case = "\n".join(
            [
                "case",
                f"            when match_score is null then {sql_string_literal(BAND_UNSCORED)}",
                f"            when match_score >= {scoring.upper!r} "
                f"then {sql_string_literal(BAND_ACCEPTED)}",
                f"            when match_score >= {scoring.lower!r} "
                f"then {sql_string_literal(BAND_REVIEW)}",
                f"            else {sql_string_literal(BAND_REJECTED)}",
                "        end as review_band",
            ]
        )
        ctes = (
            Cte(
                name=PENDING_RECORDS_CTE,
                body=select_projection(read_columns, ref(resolution_model), where=PENDING_COLUMN),
            ),
            Cte(name=CANDIDATE_PAIRS_CTE, body=pair_body),
            Cte(
                name=BANDED_CTE,
                body=select_projection(
                    ("record_key_a", "record_key_b", "match_score", band_case),
                    CANDIDATE_PAIRS_CTE,
                ),
            ),
        )
    return Companion(
        suffix=PENDING_SUFFIX,
        model_config=TABLE_CONFIG,
        ctes=ctes,
        columns=("record_key_a", "record_key_b", "match_score", "review_band"),
        final_cte=BANDED_CTE,
        purpose=(
            "the resolution evidence: every record no declared key merged, with the scored "
            "candidate pairs and the band each fell in"
        ),
        description=(
            "The resolution evidence for this product: unmerged records, their candidate "
            "pairs, the match score and the review band each pair fell in."
        ),
    )


def _priority_expression(source_column: str, priority: Sequence[str]) -> str:
    branches = " ".join(
        f"when {source_column} = {sql_string_literal(source)} then {rank}"
        for rank, source in enumerate(priority, start=1)
    )
    return f"case {branches} else {len(priority) + 1} end"


def _survivorship_ctes(
    *,
    survivorship: Sequence[tuple[str, str]],
    input_cte: str,
    priority: str,
    recency_column: str,
) -> tuple[tuple[Cte, ...], list[str], list[str]]:
    ctes: list[Cte] = []
    joins: list[str] = []
    projected: list[str] = []
    for column, strategy in survivorship:
        order_terms = [f"case when {column} is null then 1 else 0 end"]
        if strategy == SURVIVORSHIP_MOST_RECENT:
            order_terms.append(f"{recency_column} desc")
        order_terms.append(priority)
        order_terms.append(RECORD_KEY_COLUMN)
        cte_name = f"survivorship_{column}"
        ranked = "\n".join(
            [
                "        select",
                f"            {ENTITY_KEY_COLUMN},",
                f"            {column} as attribute_value,",
                "            row_number() over (",
                f"                partition by {ENTITY_KEY_COLUMN}",
                "                order by " + ", ".join(order_terms),
                "            ) as pick_rank",
                f"        from {input_cte}",
            ]
        )
        ctes.append(
            Cte(
                name=cte_name,
                body="\n".join(
                    [
                        "    select",
                        f"        {ENTITY_KEY_COLUMN},",
                        "        attribute_value",
                        "    from (",
                        ranked,
                        f"    ) as ranked_{column}",
                        "    where pick_rank = 1",
                    ]
                ),
            )
        )
        joins.append(f"    left join {cte_name}")
        joins.append(
            f"        on {cte_name}.{ENTITY_KEY_COLUMN} = entities.{ENTITY_KEY_COLUMN}"
        )
        projected.append(f"{cte_name}.attribute_value as {column}")
    return tuple(ctes), joins, projected


def render_segment(
    *,
    index: int,
    step: dict,
    product: str,
    domain: str,
    name: str,
    fields: Sequence[RelationField],
    previous: str,
    rule_signatures: Mapping[str, Any],
) -> Segment:
    """The resolution relation, the pending-key relation and the
    survivorship chain the surviving record resumes with, for one curation
    occurrence."""

    occurrence = f"steps[{index}]:{PATTERN}"
    resolution = step["resolution"]
    field_columns = tuple(
        identifier(entry.name, product=product, occurrence=occurrence) for entry in fields
    )
    clash = sorted(set(field_columns) & set(RESOLUTION_COLUMNS))
    if clash:
        raise RenderingError(
            product=product,
            occurrence=occurrence,
            rule="reserved_resolution_column",
            detail=(
                f"the composition carries {', '.join(clash)} here, which the resolution "
                "relation writes itself; rename the column in the composition"
            ),
        )

    record_key = resolution.get("record_key")
    if record_key is None:
        raise RenderingError(
            product=product,
            occurrence=occurrence,
            rule="undeclared_record_key",
            detail=(
                "the resolution needs the column that identifies one input record; cluster "
                "labels propagate along it and every survivorship tie is broken by it"
            ),
        )
    record_key_column = _string_column(
        record_key, fields=fields, product=product, occurrence=occurrence, role="record_key"
    )
    iterations = resolution.get("merge_iterations")
    if not isinstance(iterations, int) or isinstance(iterations, bool) or iterations < 1:
        raise RenderingError(
            product=product,
            occurrence=occurrence,
            rule="undeclared_merge_iterations",
            detail=(
                f"merge_iterations is {iterations!r}; the cluster unroll runs a declared number "
                "of rounds, and a round count nobody declared would silently decide how wide a "
                "cluster this estate can resolve"
            ),
        )
    keys = tuple(
        _string_column(
            key, fields=fields, product=product, occurrence=occurrence, role="resolution key"
        )
        for key in resolution["keys"]
    )
    scoring = _scoring_plan(
        step,
        product=product,
        occurrence=occurrence,
        fields=fields,
        rule_signatures=rule_signatures,
    )
    survivorship = _survivorship(
        step, product=product, occurrence=occurrence, fields=fields
    )
    precedence = step.get("precedence")
    if precedence is None:
        raise RenderingError(
            product=product,
            occurrence=occurrence,
            rule="undeclared_precedence",
            detail=(
                "survivorship needs the column naming the contributing source, the column "
                "recency is ordered by, and the declared source order; architecture section 4 "
                "makes that precedence explicit, never inferred"
            ),
        )
    source_column = _string_column(
        precedence["source_column"],
        fields=fields,
        product=product,
        occurrence=occurrence,
        role="precedence source_column",
    )
    recency_column = _visible_column(
        precedence["recency_column"],
        fields=fields,
        product=product,
        occurrence=occurrence,
        role="precedence recency_column",
    )
    priority = _priority_expression(source_column, list(precedence["source_priority"]))

    resolution_model = suffixed_model_name(domain, name, RESOLUTION_SUFFIX)
    input_cte = f"{RESOLUTION_SUFFIX}_input"

    ctes: list[Cte] = [
        Cte(
            name=RECORDS_CTE,
            body=select_projection(
                field_columns + (f"{record_key_column} as {RECORD_KEY_COLUMN}",), previous
            ),
        ),
        Cte(name=EDGES_CTE, body=_edges_body(keys)),
    ]
    ctes.extend(_label_rounds(iterations))
    ctes.append(
        Cte(
            name=COMPONENTS_CTE,
            body=select_projection(
                (RECORD_KEY_COLUMN, "label as component_id"), f"curation_labels_{iterations}"
            ),
        )
    )
    ctes.append(
        Cte(
            name=STATS_CTE,
            body="\n".join(
                [
                    "    select",
                    "        component_id,",
                    "        count(*) as component_row_count",
                    f"    from {COMPONENTS_CTE}",
                    "    group by component_id",
                ]
            ),
        )
    )
    ctes.append(
        Cte(
            name=KEYS_CTE,
            body="\n".join(
                [
                    "    select",
                    "        components.component_id,",
                    "        min(edges.key_rank) as best_key_rank",
                    f"    from {COMPONENTS_CTE} as components",
                    f"    inner join {EDGES_CTE} as edges",
                    f"        on edges.{RECORD_KEY_COLUMN} = components.{RECORD_KEY_COLUMN}",
                    "    group by components.component_id",
                ]
            ),
        )
    )
    ctes.append(Cte(name=RESOLVED_CTE, body=_resolved_body(field_columns, keys)))

    survivorship_ctes, joins, projected = _survivorship_ctes(
        survivorship=survivorship,
        input_cte=input_cte,
        priority=priority,
        recency_column=recency_column,
    )
    entities = Cte(
        name=ENTITIES_CTE,
        body="\n".join(
            ["    select distinct", f"        {ENTITY_KEY_COLUMN}", f"    from {input_cte}"]
        ),
    )
    survivor_lines = ["    select"]
    for position, column in enumerate(projected):
        comma = "," if position < len(projected) - 1 else ""
        survivor_lines.append(f"        {column}{comma}")
    survivor_lines.append(f"    from {ENTITIES_CTE} as entities")
    survivor_lines.extend(joins)
    survivor = Cte(name=SURVIVOR_CTE, body="\n".join(survivor_lines))

    return Segment(
        suffix=RESOLUTION_SUFFIX,
        model_config=VIEW_CONFIG,
        ctes=tuple(ctes),
        columns=field_columns + RESOLUTION_COLUMNS,
        final_cte=RESOLVED_CTE,
        purpose=(
            "every input record with the entity its declared keys resolved it into, the "
            "cluster size, the best-ranked key and whether it is still pending"
        ),
        description=(
            "Every input record with the entity its declared keys resolved it into, and the "
            "evidence of how."
        ),
        resume_columns=field_columns + (RECORD_KEY_COLUMN, ENTITY_KEY_COLUMN),
        resume_ctes=survivorship_ctes + (entities, survivor),
        companions=(
            _pending_companion(resolution_model=resolution_model, scoring=scoring),
        ),
    )


__all__ = [
    "PENDING_SUFFIX",
    "RESOLUTION_COLUMNS",
    "RESOLUTION_SUFFIX",
    "STRATEGY_DETERMINISTIC",
    "STRATEGY_PROBABILISTIC",
    "SURVIVORSHIP_STRATEGIES",
    "ScoringPlan",
    "render_segment",
]
