"""The transformation patterns the dbt translator renders as common table
expressions inside a product's model.

Each renderer receives the occurrence's own configuration, the name of the
common table expression the previous occurrence left, and the field set the
contract propagation says is visible AFTER this occurrence
(``ergasterion.framework.contract.propagate_relation_fields``). Each returns
the common table expressions it needs, in order, the last of which carries
exactly those fields. Selecting exactly the propagated field set is what
keeps the emitted relation and the published contract from ever disagreeing:
the renderer never decides which columns exist, it renders the ones the
contract already derived.

Batch Transfer reads the upstream contract's relation and transforms
nothing. Schema Transform renames, casts and drops. Calculated Fields
renders an inline expression verbatim (architecture section 3.2: "The text
is rendered verbatim into the generated artefacts") and a named rule as a
call to the dispatch macro that implements it. Data Enrichment joins the
referenced product's relation on the declared keys under the declared
no-match policy, as an as-of read where the lookup declares a temporal
binding. Data Aggregation groups at the declared grain and renders the
declared aggregate expressions over it.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from ergasterion.framework.contract import (
    COMBINE_JOIN_INNER,
    COMBINE_JOIN_OUTER,
    COMBINE_METHOD_MERGE,
    COMBINE_METHOD_UNION,
    ConformedColumn,
    OpeningComposition,
    SourceComposition,
    declared_source_relation,
    source_relation_key,
)
from ergasterion.framework.models import RelationField
from ergasterion.translators.dbt_patterns.sql import (
    RenderingError,
    cast_expression,
    identifier,
    quoted_physical_identifier,
    ref,
    select_projection,
)

# The no-match policies a Data Enrichment lookup may declare. A lookup that
# declares neither keeps every row and leaves the looked-up fields null; a
# lookup declaring `drop` keeps only rows that matched.
NO_MATCH_NULL = None
NO_MATCH_DROP = "drop"

BASE_ALIAS = "base"


@dataclass(frozen=True)
class Cte:
    """One named common table expression of a generated model."""

    name: str
    body: str


def cte_name(index: int, pattern: str) -> str:
    return f"step_{index:02d}_{pattern}"


# The occurrence every failure of the opening read names: the block that
# declares what a composition reads.
OPENING_OCCURRENCE = "sources"

# The alias each source of a merge is read under. A merge names the same
# column on two sides, so every column it projects is qualified.
MERGE_ALIAS = "merged"

# The SQL each declared merge join renders as. The declaration says which
# rows the merge keeps; this is the one place that answer becomes SQL, and
# a composition carrying anything else fails closed rather than defaulting
# to whichever keyword is written first here.
JOIN_KEYWORDS: dict[str, str] = {
    COMBINE_JOIN_INNER: "inner join",
    COMBINE_JOIN_OUTER: "full join",
}


def _merge_alias(index: int) -> str:
    return f"{MERGE_ALIAS}_{index:02d}"


def _column_expression(
    column: ConformedColumn, *, product: str, alias: str | None
) -> str:
    """One source column as it is read: the column itself, qualified by the
    source's alias where the read carries one, and cast to the neutral type
    its conformance mapping declares where it carries one.

    Where the producer stores the column under a name of its own, the read
    is of that stored name, quoted by the running adapter. The consumer
    still carries the column under the logical name its own composition
    knows it by (``_projected_column``), so a declaration downstream of a
    renamed column never spells the stored name."""

    if column.physical_source_name is not None:
        expression = quoted_physical_identifier(
            column.physical_source_name, product=product, occurrence=OPENING_OCCURRENCE
        )
    else:
        expression = identifier(column.source_name, product=product, occurrence=OPENING_OCCURRENCE)
    if alias is not None:
        expression = f"{alias}.{expression}"
    if column.cast_type is not None:
        expression = cast_expression(
            expression, column.cast_type, product=product, occurrence=OPENING_OCCURRENCE
        )
    return expression


def _projected_column(column: ConformedColumn, *, product: str, alias: str | None) -> str:
    """One source column as a select-list entry, named as the relation the
    composition opens with names it."""

    expression = _column_expression(column, product=product, alias=alias)
    name = identifier(column.name, product=product, occurrence=OPENING_OCCURRENCE)
    return name if expression == name else f"{expression} as {name}"


def _key_expression(
    source: SourceComposition, key: str, *, product: str, alias: str | None
) -> str:
    """The expression one source of a merge carries a declared key under.
    The contract pipe has already refused a source that does not carry
    one, so the lookup always resolves."""

    column = next(entry for entry in source.columns if entry.name == key)
    return _column_expression(column, product=product, alias=alias)


def _union_body(
    composition: OpeningComposition, *, models: Mapping[str, str], product: str
) -> str:
    """Every source's rows under the one schema the union publishes. Each
    source is read as its own conformed projection, so the union's arms
    always carry the same columns in the same order."""

    return "\n    union all\n".join(
        select_projection(
            [_projected_column(column, product=product, alias=None) for column in source.columns],
            ref(models[source.key]),
        )
        for source in composition.sources
    )


def _merge_body(
    composition: OpeningComposition, *, models: Mapping[str, str], product: str
) -> str:
    """Every source's columns side by side on the declared keys, keeping
    the rows the declared join says: an ``outer`` merge keeps every row of
    every source, an ``inner`` merge keeps the rows every source carries
    the key of. Each key is coalesced across the sources, so a row present
    in only one of them still carries it (and under an inner join every
    side carries the same value, so the coalesce is the same expression
    read once). Every other column comes from the one source that brings
    it (the contract pipe refuses two).

    The join is the only thing that differs between the two, because the
    contract pipe already decided what the published schema is: the same
    resolution that chose this keyword marked the fields an unmatched side
    can leave empty as optional."""

    join = JOIN_KEYWORDS.get(composition.join)
    if join is None:
        raise RenderingError(
            product=product,
            occurrence=OPENING_OCCURRENCE,
            rule="unrenderable_merge_join",
            detail=(
                f"a merge keeps the rows its declared join says, one of "
                f"{sorted(JOIN_KEYWORDS)}; this composition carries {composition.join!r}"
            ),
        )

    aliases = [_merge_alias(index) for index in range(len(composition.sources))]
    columns: list[str] = []
    for key in composition.keys:
        carried = [
            _key_expression(source, key, product=product, alias=alias)
            for source, alias in zip(composition.sources, aliases)
        ]
        columns.append(
            "coalesce({expressions}) as {name}".format(
                expressions=", ".join(carried),
                name=identifier(key, product=product, occurrence=OPENING_OCCURRENCE),
            )
        )
    for source, alias in zip(composition.sources, aliases):
        columns.extend(
            _projected_column(column, product=product, alias=alias)
            for column in source.columns
            if column.name not in composition.keys
        )

    lines = [f"{ref(models[composition.sources[0].key])} as {aliases[0]}"]
    for index, source in enumerate(composition.sources[1:], start=1):
        conditions = []
        for key in composition.keys:
            prior = [
                _key_expression(
                    composition.sources[position], key, product=product, alias=aliases[position]
                )
                for position in range(index)
            ]
            left = prior[0] if len(prior) == 1 else "coalesce({0})".format(", ".join(prior))
            right = _key_expression(source, key, product=product, alias=aliases[index])
            conditions.append(f"{left} = {right}")
        lines.append(f"    {join} {ref(models[source.key])} as {aliases[index]}")
        lines.append("        on " + " and ".join(conditions))
    return select_projection(columns, "\n".join(lines))


def render_opening(
    composition: OpeningComposition, *, name: str, models: Mapping[str, str], product: str
) -> Cte:
    """The relation a product's composition opens with: one source read as
    it is, or two or more combined the way the declaration says
    (architecture section 8's consolidating generation). ``models`` is the
    model each source's read resolves to, keyed the way the contract pipe
    resolved it (``ergasterion.framework.contract.source_relation_key``).

    The composition itself is resolved once, by the contract pipe, so this
    renders what the published contract was derived from and never decides
    for itself which columns the opening carries."""

    if composition.method == COMBINE_METHOD_UNION:
        body = _union_body(composition, models=models, product=product)
    elif composition.method == COMBINE_METHOD_MERGE:
        body = _merge_body(composition, models=models, product=product)
    elif composition.method is None and len(composition.sources) == 1:
        source = composition.sources[0]
        body = select_projection(
            [_projected_column(column, product=product, alias=None) for column in source.columns],
            ref(models[source.key]),
        )
    else:
        raise RenderingError(
            product=product,
            occurrence=OPENING_OCCURRENCE,
            rule="unrenderable_composition",
            detail=(
                f"{len(composition.sources)} source(s) combined as {composition.method!r}; this "
                "translator renders one source read as it is, a union of their rows or a merge "
                "of their columns"
            ),
        )
    return Cte(name=name, body=body)


def render_batch_transfer(
    *, index: int, step: dict, previous: str, fields: Sequence[RelationField], product: str
) -> tuple[Cte, ...]:
    """A read of the published product the upstream contract names, with no
    transformation of its own (architecture section 4, "No transformation
    during transfer")."""

    columns = [identifier(field.name, product=product) for field in fields]
    return (Cte(name=cte_name(index, "batch_transfer"), body=select_projection(columns, previous)),)


def render_schema_transform(
    *, index: int, step: dict, previous: str, fields: Sequence[RelationField], product: str
) -> tuple[Cte, ...]:
    """Renames, casts and drops. A mapping entry naming ``to`` renders the
    source column cast to the declared neutral type under the target name;
    an entry declaring ``drop`` renders nothing, so the column is simply
    absent from the projection."""

    occurrence = f"steps[{index}]:schema_transform"
    produced: dict[str, str] = {}
    for mapping in step.get("mapping") or []:
        target = mapping.get("to")
        if target is None:
            continue
        source_column = identifier(mapping["from"], product=product, occurrence=occurrence)
        produced[target] = "{expression} as {name}".format(
            expression=cast_expression(
                source_column, mapping["type"], product=product, occurrence=occurrence
            ),
            name=identifier(target, product=product, occurrence=occurrence),
        )
    columns = [
        produced.get(field.name, identifier(field.name, product=product, occurrence=occurrence))
        for field in fields
    ]
    return (Cte(name=cte_name(index, "schema_transform"), body=select_projection(columns, previous)),)


def render_calculated_fields(
    *,
    index: int,
    step: dict,
    previous: str,
    fields: Sequence[RelationField],
    product: str,
    rule_calls: Mapping[str, str],
) -> tuple[Cte, ...]:
    """Derived columns. An inline expression is rendered verbatim and a
    named rule as the call ``rule_calls`` carries for it, which the
    translator resolved from the estate's rule catalogue and the
    implementation registry. Either way the result is cast to the neutral
    type the field declares.

    The cast is what keeps the relation and its contract agreeing on more
    than column names: an expression's own result type is the adapter's
    business (a boolean comparison, a macro's string, an integer division),
    the declaration states what the column publishes as, and the emitted
    relation carries the declared one through the same
    ``ergasterion.framework.adapters`` type mapping the contract is read
    against."""

    occurrence = f"steps[{index}]:calculated_fields"
    produced: dict[str, str] = {}
    for field_index, entry in enumerate(step.get("fields") or []):
        name = identifier(entry["name"], product=product, occurrence=occurrence)
        expression = entry.get("expression")
        if expression is None:
            expression = rule_calls[f"{occurrence}.fields[{field_index}]"]
        produced[entry["name"]] = "{expression} as {name}".format(
            expression=cast_expression(
                f"({expression})", entry["type"], product=product, occurrence=occurrence
            ),
            name=name,
        )
    columns = [
        produced.get(field.name, identifier(field.name, product=product, occurrence=occurrence))
        for field in fields
    ]
    return (Cte(name=cte_name(index, "calculated_fields"), body=select_projection(columns, previous)),)


def _join_condition(
    *, lookup: dict, base: str, reference: str, product: str, occurrence: str
) -> list[str]:
    conditions = []
    for base_column, reference_column in sorted((lookup.get("on") or {}).items()):
        conditions.append(
            f"{base}.{identifier(base_column, product=product, occurrence=occurrence)} = "
            f"{reference}.{identifier(reference_column, product=product, occurrence=occurrence)}"
        )
    return conditions


def _join_keyword(lookup: dict, *, product: str, occurrence: str) -> str:
    policy = lookup.get("no_match", NO_MATCH_NULL)
    if policy == NO_MATCH_NULL:
        return "left join"
    if policy == NO_MATCH_DROP:
        return "inner join"
    raise RenderingError(
        product=product,
        occurrence=occurrence,
        rule="unknown_no_match_policy",
        detail=(
            f"no_match {policy!r} is not a declared policy; use null to keep the row with the "
            f"looked-up fields empty, or {NO_MATCH_DROP!r} to keep only rows that matched"
        ),
    )


def render_data_enrichment(
    *,
    index: int,
    step: dict,
    previous: str,
    fields: Sequence[RelationField],
    product: str,
    lookup_models: Mapping[str, str],
) -> tuple[Cte, ...]:
    """Joins each declared lookup's reference relation on the declared
    keys: the one relation of the referenced product the lookup reads
    (its ``relation`` key, resolved by the contract pipe). ``on`` maps this
    product's column to the reference relation's column.

    A lookup declaring ``temporal`` is an as-of read: for each distinct
    combination of join keys and event time in the enriched relation, the
    latest reference row effective at or before that event time. It is
    resolved through two helper common table expressions rather than a
    lateral join, because a lateral join is not portable across the
    declared adapters. Ties on the effectivity column are broken by the
    looked-up fields themselves, so a repeated build always picks the same
    row."""

    occurrence = f"steps[{index}]:data_enrichment"
    lookups = step.get("lookups") or []
    helpers: list[Cte] = []
    joins: list[str] = []
    looked_up: dict[str, str] = {}
    base_cte = cte_name(index, "data_enrichment")

    for lookup_index, lookup in enumerate(lookups):
        alias = f"lookup_{lookup_index}"
        # The one relation this lookup reads, under the same key the
        # contract pipe resolved it with, so the joined relation is the
        # relation the propagated schema took the looked-up fields from.
        reference_model = lookup_models[
            source_relation_key(lookup.get("contract"), declared_source_relation(lookup))
        ]
        lookup_fields = [
            identifier(name, product=product, occurrence=occurrence) for name in lookup["fields"]
        ]
        keyword = _join_keyword(lookup, product=product, occurrence=occurrence)
        temporal = lookup.get("temporal")

        if temporal is None:
            joins.append(f"    {keyword} {ref(reference_model)} as {alias}")
            joins.append(
                "        on "
                + "\n       and ".join(
                    _join_condition(
                        lookup=lookup,
                        base=BASE_ALIAS,
                        reference=alias,
                        product=product,
                        occurrence=occurrence,
                    )
                )
            )
        else:
            as_of = identifier(temporal["as_of"], product=product, occurrence=occurrence)
            effective = identifier(temporal["effective"], product=product, occurrence=occurrence)
            base_keys = [
                identifier(column, product=product, occurrence=occurrence)
                for column in sorted((lookup.get("on") or {}))
            ]
            keys_cte = f"{base_cte}_asof_{lookup_index}_keys"
            ranked_cte = f"{base_cte}_asof_{lookup_index}_ranked"
            match_cte = f"{base_cte}_asof_{lookup_index}"

            helpers.append(
                Cte(
                    name=keys_cte,
                    body=select_projection(
                        ["distinct " + base_keys[0]] + base_keys[1:] + [as_of], previous
                    ),
                )
            )
            order_by = ", ".join(
                [f"reference.{effective} desc"] + [f"reference.{name}" for name in lookup_fields]
            )
            partition_by = ", ".join([f"keys.{name}" for name in base_keys] + [f"keys.{as_of}"])
            ranked_columns = (
                [f"keys.{name}" for name in base_keys]
                + [f"keys.{as_of}"]
                + [f"reference.{name}" for name in lookup_fields]
                + [
                    "row_number() over (\n"
                    f"            partition by {partition_by}\n"
                    f"            order by {order_by}\n"
                    "        ) as match_rank"
                ]
            )
            ranked_body = "\n".join(
                [
                    "    select",
                    *[
                        f"        {column}," if position < len(ranked_columns) - 1 else f"        {column}"
                        for position, column in enumerate(ranked_columns)
                    ],
                    f"    from {keys_cte} as keys",
                    f"    {keyword} {ref(reference_model)} as reference",
                    "        on "
                    + "\n       and ".join(
                        _join_condition(
                            lookup=lookup,
                            base="keys",
                            reference="reference",
                            product=product,
                            occurrence=occurrence,
                        )
                        + [f"reference.{effective} <= keys.{as_of}"]
                    ),
                ]
            )
            helpers.append(Cte(name=ranked_cte, body=ranked_body))
            helpers.append(
                Cte(
                    name=match_cte,
                    body=select_projection(
                        base_keys + [as_of] + lookup_fields, ranked_cte, where="match_rank = 1"
                    ),
                )
            )
            joins.append(f"    {keyword} {match_cte} as {alias}")
            joins.append(
                "        on "
                + "\n       and ".join(
                    [f"{BASE_ALIAS}.{name} = {alias}.{name}" for name in base_keys]
                    + [f"{BASE_ALIAS}.{as_of} = {alias}.{as_of}"]
                )
            )

        for name in lookup["fields"]:
            looked_up[name] = f"{alias}.{identifier(name, product=product, occurrence=occurrence)}"

    columns = [
        looked_up.get(
            field.name,
            f"{BASE_ALIAS}.{identifier(field.name, product=product, occurrence=occurrence)}",
        )
        for field in fields
    ]
    body = "\n".join(
        [
            select_projection(columns, f"{previous} as {BASE_ALIAS}"),
            *joins,
        ]
    )
    return (*helpers, Cte(name=base_cte, body=body))


# The late-arrival policies a Data Aggregation occurrence may declare.
# ``recompute_period`` recomputes every period present in the input and
# republishes it whole, keyed by the grain, so a row arriving late for an
# earlier period rewrites that period rather than being appended beside it.
LATE_ARRIVAL_RECOMPUTE_PERIOD = "recompute_period"
LATE_ARRIVAL_POLICIES: tuple[str, ...] = (LATE_ARRIVAL_RECOMPUTE_PERIOD,)


def render_data_aggregation(
    *, index: int, step: dict, previous: str, fields: Sequence[RelationField], product: str
) -> tuple[Cte, ...]:
    """The declared grain and groups with the declared aggregate
    expressions over them (architecture section 4, Data Aggregation:
    "Versioned, deterministic recomputation").

    The projection is the field set the contract propagation leaves after
    this occurrence, which is the grain and group columns followed by the
    aggregates, so the emitted relation is at the declared grain by
    construction. Each aggregate is cast to the neutral type it declares:
    an aggregate replaces the field set it groups, so nothing else in the
    composition can carry its type, and the sum or count an adapter returns
    is the adapter's own width rather than the one the contract publishes.

    How that relation reaches the published one is the declared
    late-arrival policy's business, and it is enforced against the
    product's publication where both are visible."""

    occurrence = f"steps[{index}]:data_aggregation"
    grouped = [
        identifier(name, product=product, occurrence=occurrence)
        for name in list(step.get("grain") or []) + list(step.get("groups") or [])
    ]
    expressions = {
        aggregate["name"]: (aggregate["expression"], aggregate["type"])
        for aggregate in step.get("aggregates") or []
    }
    columns: list[str] = []
    for field in fields:
        name = identifier(field.name, product=product, occurrence=occurrence)
        if field.name in expressions:
            expression, declared_type = expressions[field.name]
            columns.append(
                "{expression} as {name}".format(
                    expression=cast_expression(
                        f"({expression})", declared_type, product=product, occurrence=occurrence
                    ),
                    name=name,
                )
            )
        else:
            columns.append(name)
    body = "\n".join(
        [select_projection(columns, previous), "    group by", *[
            f"        {name}," if position < len(grouped) - 1 else f"        {name}"
            for position, name in enumerate(grouped)
        ]]
    )
    return (Cte(name=cte_name(index, "data_aggregation"), body=body),)


# Every pattern this module renders as a common table expression, and the
# renderer that does it. ergasterion.translators.dbt_patterns checks this
# registry against the dbt translator's registered capabilities at import,
# so a capability can never be declared with no renderer behind it.
CTE_RENDERERS: dict[str, Any] = {
    "batch_transfer": render_batch_transfer,
    "schema_transform": render_schema_transform,
    "calculated_fields": render_calculated_fields,
    "data_enrichment": render_data_enrichment,
    "data_aggregation": render_data_aggregation,
}
