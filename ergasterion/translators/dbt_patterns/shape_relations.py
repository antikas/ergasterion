"""Rendering the relations a shape renders, for dbt (architecture sections
6 and 10).

A shape says what it renders and from which columns of the composition's
own relation (``ergasterion.framework.shapes.ShapeRelation``). This module
is the dbt half of that: one arm per derivation, each turning one
``ShapeRelation`` into the common table expressions and the final
projection its model is built from. The product's composition is rendered
once, into a base relation, and every arm reads that -- except an arm whose
relation names siblings (``ShapeRelation.reads``), which reads those
relations instead.

  * a projection selects the declared columns as they stand -- a canonical
    entity, and a fact at its declared grain;
  * a type 1 dimension keeps one row per key, the version the declared
    change column ranks last;
  * a type 2 dimension keeps one row per version of the key, with a
    half-open effective range. A version starts where an attribute differs
    from the one before it, the range runs to the instant the next version
    starts, and the version in force now has no end;
  * an identity store and an association store keep one row per business
    key and per combination of them, each with a hashed identity key;
  * a version store and a snapshot store keep a payload's versions against
    that identity, fingerprinted over a frozen column set;
  * a surviving record and a point-in-time relation read those stores
    rather than the composition: one row per entity carrying the value each
    declared strategy picks, and one row per entity and snapshot instant
    carrying the version of each store in force then.

The three dimension and projection arms recompute the whole relation from
the columns the composition leaves. Neither merges into a relation that
already exists, so neither needs an adapter's own merge mechanics, and the
two declared adapters compile the same text. The three store arms do
accumulate across runs, and what that costs them is one macro call each:
whether the run is the first one, and what the store already holds, is a
fact about the run rather than about the declaration.

Every arm's final projection casts each column to the neutral type the
contract declares for it, through the same dispatch macro the rest of the
route casts with, so the built relation carries the declared type on
whichever adapter compiled it.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping, Sequence

from ergasterion.framework.shapes import (
    DERIVATION_ASSOCIATION_STORE,
    DERIVATION_DIMENSION_TYPE_1,
    DERIVATION_DIMENSION_TYPE_2,
    DERIVATION_IDENTITY_STORE,
    DERIVATION_POINT_IN_TIME,
    DERIVATION_PROJECTION,
    DERIVATION_SNAPSHOT_STORE,
    DERIVATION_SURVIVING_RECORD,
    DERIVATION_VERSION_STORE,
    EFFECTIVE_FROM,
    ShapeRelation,
)
from ergasterion.shapes import data_vault as vault
from ergasterion.translators.dbt_patterns.sql import (
    PUBLISH_TIMESTAMP_CALL,
    RenderingError,
    declared_type_projection,
    identifier,
    jinja_literal,
    macro_call,
    ref,
    select_projection,
    sql_string_literal,
)
from ergasterion.translators.dbt_patterns.steps import Cte


@dataclass(frozen=True)
class RelationContext:
    """What every arm below renders one relation against: the private base
    relation the composition was rendered into, the product's own published
    name, the model each of the shape's relations is rendered as (a
    relation derived from its siblings reads them by model name), the
    product's declared shape section, and the record version each store
    the shape keeps is written under, from the estate's ledger for that
    shape."""

    base_model: str
    product: str
    models: Mapping[str, str] = field(default_factory=dict)
    shape_config: Mapping[str, Any] = field(default_factory=dict)
    store_versions: Mapping[str, int] = field(default_factory=dict)

# The columns the dimension arms compute for themselves. They exist only
# inside a model's own common table expressions and never reach a
# published relation. A declared column of the same name would make the
# generated SQL ambiguous, so it fails closed rather than being renamed.
VERSION_RANK = "dpf_version_rank"
VERSION_START = "dpf_version_start"
VERSION_NUMBER = "dpf_version_number"
# The two a surviving record computes while ranking one attribute's
# candidate values across the stores it survives them from.
CANDIDATE_VALUE = "dpf_value"
CANDIDATE_PRIORITY = "dpf_source_priority"
WORKING_COLUMNS: tuple[str, ...] = (
    VERSION_RANK,
    VERSION_START,
    VERSION_NUMBER,
    CANDIDATE_VALUE,
    CANDIDATE_PRIORITY,
)

# The macros a vault store places. Each is a call into macros/ rather than
# inline SQL because each needs what only the running build knows: the
# adapter's own hash construction, and whether this run is the first one
# or is adding to a store that already carries rows.
HASH_MACRO = "dpf_vault_hash"
UNSTORED_KEY_MACRO = "dpf_vault_unstored_key"
NEW_VERSIONS_MACRO = "dpf_vault_new_versions"


def _occurrence(relation: ShapeRelation) -> str:
    return f"target.shape_config:{relation.suffix}"


def _names(columns: Sequence[str], *, product: str, occurrence: str) -> list[str]:
    return [identifier(column, product=product, occurrence=occurrence) for column in columns]


def _check_working_columns(relation: ShapeRelation, *, product: str, occurrence: str) -> None:
    clashing = [column for column in relation.source_columns if column in WORKING_COLUMNS]
    if clashing:
        raise RenderingError(
            product=product,
            occurrence=occurrence,
            rule="working_column_clash",
            detail=(
                f"the composition carries {', '.join(clashing)}, which this rendering computes "
                "for itself while deriving the dimension"
            ),
        )


def _window(key: Sequence[str], order: str, *, descending: bool = False) -> str:
    direction = " desc" if descending else ""
    return f"partition by {', '.join(key)} order by {order}{direction}"


def _source_cte(
    relation: ShapeRelation,
    *,
    base_model: str,
    product: str,
    occurrence: str,
    distinct: bool = False,
) -> Cte:
    return Cte(
        name=f"{relation.suffix}_source",
        body=select_projection(
            _names(relation.source_columns, product=product, occurrence=occurrence),
            ref(base_model),
            distinct=distinct,
        ),
    )


def _projection(
    relation: ShapeRelation, context: RelationContext
) -> tuple[tuple[Cte, ...], list[str], str]:
    base_model, product = context.base_model, context.product
    occurrence = _occurrence(relation)
    source = _source_cte(relation, base_model=base_model, product=product, occurrence=occurrence)
    columns = declared_type_projection(
        relation.schema.fields, product=product, occurrence=occurrence
    )
    return (source,), columns, source.name


def _type_1_dimension(
    relation: ShapeRelation, context: RelationContext
) -> tuple[tuple[Cte, ...], list[str], str]:
    base_model, product = context.base_model, context.product
    occurrence = _occurrence(relation)
    _check_working_columns(relation, product=product, occurrence=occurrence)
    key = _names(relation.key, product=product, occurrence=occurrence)
    change = identifier(relation.change_column, product=product, occurrence=occurrence)
    published = _names(
        [field.name for field in relation.schema.fields], product=product, occurrence=occurrence
    )

    source = _source_cte(relation, base_model=base_model, product=product, occurrence=occurrence)
    ranked = Cte(
        name=f"{relation.suffix}_ranked",
        body=select_projection(
            [
                *published,
                f"row_number() over ({_window(key, change, descending=True)}) as {VERSION_RANK}",
            ],
            source.name,
        ),
    )
    current = Cte(
        name=f"{relation.suffix}_current",
        body=select_projection(published, ranked.name, where=f"{VERSION_RANK} = 1"),
    )
    return (
        (source, ranked, current),
        declared_type_projection(relation.schema.fields, product=product, occurrence=occurrence),
        current.name,
    )


def _type_2_dimension(
    relation: ShapeRelation, context: RelationContext
) -> tuple[tuple[Cte, ...], list[str], str]:
    base_model, product = context.base_model, context.product
    occurrence = _occurrence(relation)
    _check_working_columns(relation, product=product, occurrence=occurrence)
    effective_from, effective_to = relation.range_columns or ("", "")
    if not effective_from or not effective_to:
        raise RenderingError(
            product=product,
            occurrence=occurrence,
            rule="missing_effective_range",
            detail="a type 2 dimension declares the two columns its effective range publishes",
        )
    key = _names(relation.key, product=product, occurrence=occurrence)
    change = identifier(relation.change_column, product=product, occurrence=occurrence)
    attributes = [
        identifier(field.name, product=product, occurrence=occurrence)
        for field in relation.schema.fields
        if field.name not in relation.key and field.name not in relation.range_columns
    ]
    window = _window(key, change)

    source = _source_cte(
        relation, base_model=base_model, product=product, occurrence=occurrence, distinct=True
    )
    changed = "\n".join(
        f"            when {attribute} is distinct from lag({attribute}) over ({window}) then 1"
        for attribute in attributes
    )
    marked = Cte(
        name=f"{relation.suffix}_marked",
        body=select_projection(
            [
                *key,
                *attributes,
                change,
                (
                    "case"
                    f"\n            when row_number() over ({window}) = 1 then 1"
                    f"\n{changed}"
                    "\n            else 0"
                    f"\n        end as {VERSION_START}"
                ),
            ],
            source.name,
        ),
    )
    numbered = Cte(
        name=f"{relation.suffix}_numbered",
        body=select_projection(
            [
                *key,
                *attributes,
                change,
                (
                    f"sum({VERSION_START}) over ({window} rows between unbounded preceding and "
                    f"current row) as {VERSION_NUMBER}"
                ),
            ],
            marked.name,
        ),
    )
    collapsed = Cte(
        name=f"{relation.suffix}_collapsed",
        body=select_projection(
            [*key, *attributes, VERSION_NUMBER, f"min({change}) as {effective_from}"],
            numbered.name,
            group_by=[*key, *attributes, VERSION_NUMBER],
        ),
    )
    ranged = Cte(
        name=f"{relation.suffix}_ranged",
        body=select_projection(
            [
                *key,
                *attributes,
                effective_from,
                (
                    f"lead({effective_from}) over ({_window(key, effective_from)}) "
                    f"as {effective_to}"
                ),
            ],
            collapsed.name,
        ),
    )
    return (
        (source, marked, numbered, collapsed, ranged),
        declared_type_projection(relation.schema.fields, product=product, occurrence=occurrence),
        ranged.name,
    )


# --------------------------------------------------------------------- vault stores
#
# The five arms below render the ``data_vault`` shape's relations
# (``ergasterion.shapes.data_vault``). Three of them are insert-only
# stores: what a run derives is added to what earlier runs stored, and a
# row already stored is never written twice. That is the one thing a
# generated model cannot decide for itself -- whether this run is the
# first one, and what the store already holds -- so each store's
# suppression is a call into ``macros/data_vault.sql`` and everything else
# is generated SQL like every other arm here.


def _vault_plan(context: RelationContext) -> vault.VaultPlan:
    """The product's declared vault, resolved from its shape section. The
    shape's own ``relations`` resolves the same plan from the same words,
    so what the contract publishes and what these arms render can never be
    derived two ways."""

    return vault.resolve_plan(product=context.product, shape_config=context.shape_config)


def _hash_call(columns: Sequence[str]) -> str:
    """The dispatch call that hashes ``columns`` into one identity key or
    change fingerprint. The order is the hash input, so two relations
    naming the same columns in the same order always produce the same
    key."""

    return macro_call(HASH_MACRO, "[" + ", ".join(jinja_literal(name) for name in columns) + "]")


def _present(columns: Sequence[str]) -> str:
    return " and ".join(f"{name} is not null" for name in columns)


def _metadata_columns(product: str) -> list[str]:
    return [
        f"{PUBLISH_TIMESTAMP_CALL} as {vault.LOAD_DATETIME_COLUMN}",
        f"{sql_string_literal(product)} as {vault.RECORD_SOURCE_COLUMN}",
    ]


def _published(relation: ShapeRelation) -> list[str]:
    return [entry.name for entry in relation.schema.fields]


def _join_select(
    columns: Sequence[str], source: str, joins: Sequence[tuple[str, str]]
) -> str:
    """A select of ``columns`` from ``source`` left joined to each
    (relation, condition) in ``joins``, in the one layout every arm here
    that reads more than one relation uses."""

    lines = ["    select"]
    for position, column in enumerate(columns):
        comma = "," if position < len(columns) - 1 else ""
        lines.append(f"        {column}{comma}")
    lines.append(f"    from {source}")
    for name, condition in joins:
        lines.append(f"    left join {name}")
        lines.append(f"        on {condition}")
    return "\n".join(lines)


def _identity_store(
    relation: ShapeRelation, context: RelationContext
) -> tuple[tuple[Cte, ...], list[str], str]:
    """One hub: the store of an entity's identities. One row per business
    key the composition carries in full, its identity key hashed from that
    key, and the instant and source of the run that first stored it. A key
    already stored is not written again, so the instant stays the one it
    was first seen at."""

    plan = _vault_plan(context)
    hub = next(entry for entry in plan.hubs if entry.suffix == relation.suffix)
    return _key_store(
        relation,
        context,
        key_column=hub.key_column,
        business_key=hub.business_key,
        identities=((hub.key_column, hub.business_key),),
    )


def _association_store(
    relation: ShapeRelation, context: RelationContext
) -> tuple[tuple[Cte, ...], list[str], str]:
    """One link: the store of an association between hubs. One row per
    combination of business keys the composition carries in full, the
    association's own identity key hashed from all of them and each hub's
    identity key hashed from its own, so the link and the hubs agree on
    every key by construction."""

    plan = _vault_plan(context)
    link = next(entry for entry in plan.links if entry.suffix == relation.suffix)
    return _key_store(
        relation,
        context,
        key_column=link.key_column,
        business_key=link.business_key,
        identities=(
            (link.key_column, link.business_key),
            *((hub.key_column, hub.business_key) for hub in link.hubs),
        ),
    )


def _key_store(
    relation: ShapeRelation,
    context: RelationContext,
    *,
    key_column: str,
    business_key: Sequence[str],
    identities: Sequence[tuple[str, Sequence[str]]],
) -> tuple[tuple[Cte, ...], list[str], str]:
    """What a hub and a link have in common: distinct business keys, an
    identity key hashed from each declared group, and only the keys the
    store does not already carry."""

    product = context.product
    occurrence = _occurrence(relation)
    _check_working_columns(relation, product=product, occurrence=occurrence)
    keys = _names(business_key, product=product, occurrence=occurrence)

    source = Cte(
        name=f"{relation.suffix}_source",
        body=select_projection(keys, ref(context.base_model), where=_present(keys), distinct=True),
    )
    keyed = Cte(
        name=f"{relation.suffix}_keyed",
        body=select_projection(
            [
                *(
                    "{call} as {name}".format(
                        call=_hash_call(_names(columns, product=product, occurrence=occurrence)),
                        name=identifier(name, product=product, occurrence=occurrence),
                    )
                    for name, columns in identities
                ),
                *keys,
                *_metadata_columns(product),
            ],
            source.name,
        ),
    )
    unstored = Cte(
        name=f"{relation.suffix}_unstored",
        body=select_projection(
            _published(relation),
            keyed.name,
            where=macro_call(
                UNSTORED_KEY_MACRO, jinja_literal(keyed.name), jinja_literal(key_column)
            ),
        ),
    )
    return (
        (source, keyed, unstored),
        declared_type_projection(relation.schema.fields, product=product, occurrence=occurrence),
        unstored.name,
    )


def _payload_store(
    relation: ShapeRelation, context: RelationContext, *, kind: str
) -> tuple[tuple[Cte, ...], list[str], str]:
    """One satellite: the insert-only store of a payload's versions against
    its hub or link.

    Every delivered row is fingerprinted over the satellite's hashdiff
    basis, stamped with the basis version that fingerprint was computed
    under, and offered to the store. Which of them the store keeps is the
    satellite's declared kind: a ``current`` satellite keeps a payload
    differing from the version in force for that key, and a ``history``
    satellite keeps every snapshot at an instant it has not stored yet.
    Both are held to the store's own watermark, so nothing lands behind
    what the store already carries under the same basis version."""

    product = context.product
    occurrence = _occurrence(relation)
    _check_working_columns(relation, product=product, occurrence=occurrence)
    plan = _vault_plan(context)
    satellite = plan.satellite(relation.suffix)
    basis_version = context.store_versions.get(satellite.name)
    if basis_version is None:
        raise RenderingError(
            product=product,
            occurrence=occurrence,
            rule="unrecorded_hashdiff_basis",
            detail=(
                f"the estate's evolution ledger records no hashdiff basis version for satellite "
                f"{satellite.name!r}; a fingerprint means nothing without the basis it was "
                "computed under"
            ),
        )

    parent_key = _names(satellite.parent_business_key, product=product, occurrence=occurrence)
    payload = _names(satellite.payload, product=product, occurrence=occurrence)
    basis = _names(satellite.basis, product=product, occurrence=occurrence)
    change = identifier(satellite.change_column, product=product, occurrence=occurrence)
    key_column = identifier(satellite.parent_key_column, product=product, occurrence=occurrence)

    source = Cte(
        name=f"{relation.suffix}_source",
        body=select_projection(
            [*parent_key, *payload, change],
            ref(context.base_model),
            where=_present(parent_key),
            distinct=True,
        ),
    )
    fingerprinted = Cte(
        name=f"{relation.suffix}_fingerprinted",
        body=select_projection(
            [
                f"{_hash_call(parent_key)} as {key_column}",
                f"{_hash_call(basis)} as {vault.FINGERPRINT_COLUMN}",
                f"{basis_version} as {vault.BASIS_VERSION_COLUMN}",
                *payload,
                f"{change} as {EFFECTIVE_FROM}",
                *_metadata_columns(product),
            ],
            source.name,
        ),
    )
    unstored = Cte(
        name=f"{relation.suffix}_unstored",
        body=select_projection(
            _published(relation),
            fingerprinted.name,
            where=macro_call(
                NEW_VERSIONS_MACRO,
                jinja_literal(fingerprinted.name),
                jinja_literal(key_column),
                jinja_literal(vault.FINGERPRINT_COLUMN),
                jinja_literal(EFFECTIVE_FROM),
                jinja_literal(vault.BASIS_VERSION_COLUMN),
                jinja_literal(kind),
            ),
        ),
    )
    return (
        (source, fingerprinted, unstored),
        declared_type_projection(relation.schema.fields, product=product, occurrence=occurrence),
        unstored.name,
    )


def _version_store(
    relation: ShapeRelation, context: RelationContext
) -> tuple[tuple[Cte, ...], list[str], str]:
    return _payload_store(relation, context, kind=vault.KIND_CURRENT)


def _snapshot_store(
    relation: ShapeRelation, context: RelationContext
) -> tuple[tuple[Cte, ...], list[str], str]:
    return _payload_store(relation, context, kind=vault.KIND_HISTORY)


def _sibling_model(relation: ShapeRelation, context: RelationContext, suffix: str) -> str:
    """The model one of this relation's siblings is rendered as. A sibling
    the emission did not plan fails closed rather than rendering a read of
    a relation nothing writes."""

    model = context.models.get(suffix)
    if model is None:
        raise RenderingError(
            product=context.product,
            occurrence=_occurrence(relation),
            rule="unresolved_sibling_relation",
            detail=(
                f"this relation is derived from {suffix!r}, which is not a relation this "
                f"product's shape renders: {sorted(context.models)}"
            ),
        )
    return model


def _current_version_ctes(
    relation: ShapeRelation,
    context: RelationContext,
    satellite: vault.Satellite,
    columns: Sequence[str],
) -> tuple[tuple[Cte, Cte], str]:
    """The version of one satellite in force per key: every stored version
    ranked by the basis version first and the instant it came into force
    second, latest of each, and only the top rank kept.

    The basis version ranks first because a re-baseline leaves the entity's
    history under the old basis in place and stores its current state under
    the new one: reading the latest instant alone would read whichever of
    the two happened to be later, which is a fact about the re-baseline
    rather than about the entity."""

    product = context.product
    occurrence = _occurrence(relation)
    key_column = identifier(satellite.parent_key_column, product=product, occurrence=occurrence)
    projected = _names(columns, product=product, occurrence=occurrence)
    prefix = f"{relation.suffix}_{satellite.name}"
    ranked = Cte(
        name=f"{prefix}_ranked",
        body=select_projection(
            [
                key_column,
                *projected,
                EFFECTIVE_FROM,
                (
                    f"row_number() over (partition by {key_column} order by "
                    f"{vault.BASIS_VERSION_COLUMN} desc, {EFFECTIVE_FROM} desc) as "
                    f"{VERSION_RANK}"
                ),
            ],
            ref(_sibling_model(relation, context, satellite.suffix)),
        ),
    )
    current = Cte(
        name=f"{prefix}_current",
        body=select_projection(
            [key_column, *projected, EFFECTIVE_FROM], ranked.name, where=f"{VERSION_RANK} = 1"
        ),
    )
    return (ranked, current), current.name


def _surviving_record(
    relation: ShapeRelation, context: RelationContext
) -> tuple[tuple[Cte, ...], list[str], str]:
    """One business-vault surviving record: one row per entity in the hub,
    carrying for each declared attribute the value the declared strategy
    picks across the versions in force in the declared satellites.

    Both strategies rank the same candidate set -- every non-null value the
    declared satellites carry in force, each tagged with the instant it
    came into force and the position of the satellite it came from -- and
    differ only in which of the two the ranking reads first.
    ``most_recent`` reads the instant first and settles a tie on the
    declared satellite order; ``first_non_null`` reads the declared order
    first and settles a tie on the instant. One ranking, so an attribute
    can never be survived two ways, and a single-satellite rule needs no
    special case."""

    product = context.product
    occurrence = _occurrence(relation)
    _check_working_columns(relation, product=product, occurrence=occurrence)
    plan = _vault_plan(context)
    record = next(entry for entry in plan.surviving if entry.suffix == relation.suffix)
    hub = record.hub
    key_column = identifier(hub.key_column, product=product, occurrence=occurrence)
    business_key = _names(hub.business_key, product=product, occurrence=occurrence)

    hub_cte = Cte(
        name=f"{relation.suffix}_hub",
        body=select_projection(
            [key_column, *business_key, vault.LOAD_DATETIME_COLUMN, vault.RECORD_SOURCE_COLUMN],
            ref(_sibling_model(relation, context, hub.suffix)),
        ),
    )

    ctes: list[Cte] = [hub_cte]
    current_by_satellite: dict[str, str] = {}
    for satellite in record.satellites:
        carried = [entry.name for entry in record.attributes if entry.name in satellite.payload]
        rendered, current = _current_version_ctes(relation, context, satellite, carried)
        ctes.extend(rendered)
        current_by_satellite[satellite.name] = current

    joins: list[tuple[str, str]] = []
    attribute_columns: list[str] = []
    for attribute in record.attributes:
        column = identifier(attribute.name, product=product, occurrence=occurrence)
        arms = [
            select_projection(
                [
                    key_column,
                    f"{column} as {CANDIDATE_VALUE}",
                    EFFECTIVE_FROM,
                    f"{position} as {CANDIDATE_PRIORITY}",
                ],
                current_by_satellite[satellite.name],
                where=f"{column} is not null",
            )
            for position, satellite in enumerate(record.satellites, start=1)
            if attribute.name in satellite.payload
        ]
        candidates = Cte(
            name=f"{relation.suffix}_{attribute.name}_candidates",
            body="\n    union all\n".join(arms),
        )
        order = (
            f"{EFFECTIVE_FROM} desc, {CANDIDATE_PRIORITY} asc"
            if attribute.strategy == vault.STRATEGY_MOST_RECENT
            else f"{CANDIDATE_PRIORITY} asc, {EFFECTIVE_FROM} desc"
        )
        ranked = Cte(
            name=f"{relation.suffix}_{attribute.name}_ranked",
            body=select_projection(
                [
                    key_column,
                    CANDIDATE_VALUE,
                    (
                        f"row_number() over (partition by {key_column} order by {order}) "
                        f"as {VERSION_RANK}"
                    ),
                ],
                candidates.name,
            ),
        )
        surviving = Cte(
            name=f"{relation.suffix}_{attribute.name}",
            body=select_projection(
                [key_column, CANDIDATE_VALUE], ranked.name, where=f"{VERSION_RANK} = 1"
            ),
        )
        ctes.extend((candidates, ranked, surviving))
        joins.append(
            (surviving.name, f"{surviving.name}.{key_column} = {hub_cte.name}.{key_column}")
        )
        attribute_columns.append(f"{surviving.name}.{CANDIDATE_VALUE} as {column}")

    joined = Cte(
        name=f"{relation.suffix}_joined",
        body=_join_select(
            [
                f"{hub_cte.name}.{key_column} as {key_column}",
                *(f"{hub_cte.name}.{column} as {column}" for column in business_key),
                *attribute_columns,
                f"{hub_cte.name}.{vault.LOAD_DATETIME_COLUMN} as {vault.LOAD_DATETIME_COLUMN}",
                f"{hub_cte.name}.{vault.RECORD_SOURCE_COLUMN} as {vault.RECORD_SOURCE_COLUMN}",
            ],
            hub_cte.name,
            joins,
        ),
    )
    ctes.append(joined)
    return (
        tuple(ctes),
        declared_type_projection(relation.schema.fields, product=product, occurrence=occurrence),
        joined.name,
    )


def _point_in_time(
    relation: ShapeRelation, context: RelationContext
) -> tuple[tuple[Cte, ...], list[str], str]:
    """One point-in-time relation: one row per entity and snapshot instant,
    carrying for each declared satellite the instant of the version in
    force at that snapshot.

    The snapshot spine is the distinct instants the declared satellites
    themselves carry, so every row points at something the vault stores and
    the relation is never a window somebody typed. A satellite carrying no
    version at or before a snapshot points nowhere for it, which is a fact
    about the vault rather than a gap in the relation.

    Each satellite is read at the highest basis version that entity carries
    and no other. A re-baseline stores the entity's current state again
    under the next basis version without removing what the previous one
    detected, so a relation reading both would carry one snapshot per basis
    at the same instant and point at a version the estate has superseded."""

    product = context.product
    occurrence = _occurrence(relation)
    _check_working_columns(relation, product=product, occurrence=occurrence)
    plan = _vault_plan(context)
    pit = next(entry for entry in plan.point_in_time if entry.suffix == relation.suffix)
    key_column = identifier(pit.hub.key_column, product=product, occurrence=occurrence)

    ctes: list[Cte] = []
    scoped_by_satellite: dict[str, str] = {}
    for satellite in pit.satellites:
        prefix = f"{relation.suffix}_{satellite.name}"
        ranked = Cte(
            name=f"{prefix}_ranked",
            body=select_projection(
                [
                    key_column,
                    EFFECTIVE_FROM,
                    (
                        f"dense_rank() over (partition by {key_column} order by "
                        f"{vault.BASIS_VERSION_COLUMN} desc) as {VERSION_RANK}"
                    ),
                ],
                ref(_sibling_model(relation, context, satellite.suffix)),
            ),
        )
        scoped = Cte(
            name=f"{prefix}_scoped",
            body=select_projection(
                [key_column, EFFECTIVE_FROM], ranked.name, where=f"{VERSION_RANK} = 1"
            ),
        )
        ctes.extend((ranked, scoped))
        scoped_by_satellite[satellite.name] = scoped.name

    spine = Cte(
        name=f"{relation.suffix}_spine",
        body="\n    union distinct\n".join(
            select_projection(
                [key_column, f"{EFFECTIVE_FROM} as {vault.AS_OF_COLUMN}"],
                scoped_by_satellite[satellite.name],
                distinct=True,
            )
            for satellite in pit.satellites
        ),
    )
    ctes.append(spine)

    joins: list[tuple[str, str]] = []
    pointer_columns: list[str] = []
    for satellite in pit.satellites:
        pointer = vault.pointer_column(satellite.name)
        stored = scoped_by_satellite[satellite.name]
        alias = f"{relation.suffix}_{satellite.name}_stored"
        pointer_cte = Cte(
            name=f"{relation.suffix}_{satellite.name}",
            body="\n".join(
                [
                    "    select",
                    f"        {spine.name}.{key_column} as {key_column},",
                    f"        {spine.name}.{vault.AS_OF_COLUMN} as {vault.AS_OF_COLUMN},",
                    f"        max({alias}.{EFFECTIVE_FROM}) as {pointer}",
                    f"    from {spine.name}",
                    f"    left join {stored} as {alias}",
                    f"        on {alias}.{key_column} = {spine.name}.{key_column}",
                    f"        and {alias}.{EFFECTIVE_FROM} <= "
                    f"{spine.name}.{vault.AS_OF_COLUMN}",
                    f"    group by {spine.name}.{key_column}, {spine.name}.{vault.AS_OF_COLUMN}",
                ]
            ),
        )
        ctes.append(pointer_cte)
        joins.append(
            (
                pointer_cte.name,
                f"{pointer_cte.name}.{key_column} = {spine.name}.{key_column} "
                f"and {pointer_cte.name}.{vault.AS_OF_COLUMN} = "
                f"{spine.name}.{vault.AS_OF_COLUMN}",
            )
        )
        pointer_columns.append(f"{pointer_cte.name}.{pointer} as {pointer}")

    joined = Cte(
        name=f"{relation.suffix}_joined",
        body=_join_select(
            [
                f"{spine.name}.{key_column} as {key_column}",
                f"{spine.name}.{vault.AS_OF_COLUMN} as {vault.AS_OF_COLUMN}",
                *pointer_columns,
            ],
            spine.name,
            joins,
        ),
    )
    ctes.append(joined)
    return (
        tuple(ctes),
        declared_type_projection(relation.schema.fields, product=product, occurrence=occurrence),
        joined.name,
    )


_ARMS = {
    DERIVATION_PROJECTION: _projection,
    DERIVATION_DIMENSION_TYPE_1: _type_1_dimension,
    DERIVATION_DIMENSION_TYPE_2: _type_2_dimension,
    DERIVATION_IDENTITY_STORE: _identity_store,
    DERIVATION_ASSOCIATION_STORE: _association_store,
    DERIVATION_VERSION_STORE: _version_store,
    DERIVATION_SNAPSHOT_STORE: _snapshot_store,
    DERIVATION_SURVIVING_RECORD: _surviving_record,
    DERIVATION_POINT_IN_TIME: _point_in_time,
}


def render_relation(
    relation: ShapeRelation, context: RelationContext
) -> tuple[tuple[Cte, ...], list[str], str]:
    """One shape relation as the common table expressions, final columns
    and final expression name its dbt model is built from. A derivation
    this translator has no arm for fails closed naming the product, the
    relation and the derivation, rather than rendering a guess."""

    arm = _ARMS.get(relation.derivation)
    if arm is None:
        raise RenderingError(
            product=context.product,
            occurrence=_occurrence(relation),
            rule="unrenderable_derivation",
            detail=(
                f"derivation {relation.derivation!r} is not one this translator renders: "
                f"{', '.join(sorted(_ARMS))}"
            ),
        )
    return arm(relation, context)


__all__ = [
    "CANDIDATE_PRIORITY",
    "CANDIDATE_VALUE",
    "HASH_MACRO",
    "NEW_VERSIONS_MACRO",
    "UNSTORED_KEY_MACRO",
    "VERSION_NUMBER",
    "VERSION_RANK",
    "VERSION_START",
    "WORKING_COLUMNS",
    "RelationContext",
    "render_relation",
]
