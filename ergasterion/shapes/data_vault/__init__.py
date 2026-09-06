"""The ``data_vault`` shape: hubs, links, satellites, a business-vault
surviving record and a point-in-time relation over a curated product
(architecture sections 6 and 14 check 3).

The target block declares the modelling; the composition declares how the
rows are produced. The shape adds one constraint the composition's profile
cannot express: the product must carry a Data Curation occurrence
(architecture section 6, the ``data_vault`` row: "Requires a Data Curation
occurrence"). A vault stores an entity's identity and the versions of its
payload, and neither means anything over records nothing resolved into an
entity, so a product without that occurrence fails closed naming the
product, the shape and the composition.

Five relation kinds render, and every one of them is published: a
consumer names the one it reads on its source (owner ruling R4).

  * a **hub** per declared business key: the store of one entity's
    identities. One row per business key, an identity key hashed from it,
    and the instant and source of the run that first stored it. It is an
    insert-only store: a key already stored keeps the instant it was first
    seen, whatever later runs deliver;
  * a **link** per declared association: the store of one association
    between two or more hubs. One row per combination of the hubs'
    identity keys, an identity key hashed from them, insert-only in the
    same way;
  * a **satellite** per declared payload: the insert-only store of that
    payload's versions against its hub or link. Each row carries the
    parent's identity key, a change fingerprint over the satellite's
    hashdiff basis, the basis version that fingerprint was computed under,
    the payload itself, the instant the version came into force, and the
    run that stored it. Two kinds:

      - ``current`` stores a version only where the payload differs from
        the version in force for that key, which is what an ordinary
        satellite does: it records changes;
      - ``history`` stores every delivered snapshot at an instant it has
        not stored yet, unchanged payloads included, which is what a
        history satellite does: it records observations.

  * a **business-vault surviving record** per declared hub: one row per
    entity carrying, for each declared attribute, the value the declared
    strategy picks across the declared satellites' versions in force;
  * a **point-in-time relation** per declared hub: one row per entity and
    snapshot instant, carrying for each declared satellite the instant of
    the version in force at that snapshot. The snapshot spine is the
    distinct instants the satellites themselves carry, so the relation is
    derived from the vault and is never a window somebody typed.

The hashdiff basis is the exact column set a satellite's fingerprints were
computed over, and it is frozen once the estate has stored versions under
it. ``ergasterion.shapes.data_vault.evolution`` owns the record of it and
the grading of a declared change against it; ``ledger`` below is the hook
the emission route reads it through.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from ergasterion.framework.models import RelationField, RelationSchema
from ergasterion.framework.shapes import (
    DERIVATION_ASSOCIATION_STORE,
    DERIVATION_IDENTITY_STORE,
    DERIVATION_POINT_IN_TIME,
    DERIVATION_SNAPSHOT_STORE,
    DERIVATION_SURVIVING_RECORD,
    DERIVATION_VERSION_STORE,
    EFFECTIVE_FROM,
    MATERIALISATION_INCREMENTAL,
    MATERIALISATION_TABLE,
    TIME_TYPES,
    ShapeConstraintError,
    ShapeDefinition,
    ShapeLedger,
    ShapeRelation,
    column_field,
    register_shape,
)
from ergasterion.shapes.data_vault import evolution as evolution_mod

SHAPE_NAME = "data_vault"

# The occurrence a vault product's own composition must carry.
CURATION_PATTERN = "data_curation"

# The prefix each relation kind takes under the product's namespace. A
# declared name is unique within its kind, and the prefix keeps two kinds
# from ever claiming one relation name.
HUB_PREFIX = "hub"
LINK_PREFIX = "link"
SATELLITE_PREFIX = "sat"
SURVIVING_PREFIX = "golden"
POINT_IN_TIME_PREFIX = "pit"

# The two ways a satellite decides a delivered payload is a new version.
KIND_CURRENT = "current"
KIND_HISTORY = "history"
SATELLITE_KINDS: tuple[str, ...] = (KIND_CURRENT, KIND_HISTORY)

# What each satellite kind is rendered as. One derivation per kind, so a
# translator that renders only one of them fails closed on the other
# rather than storing observations where the declaration asked for
# changes.
SATELLITE_DERIVATIONS: dict[str, str] = {
    KIND_CURRENT: DERIVATION_VERSION_STORE,
    KIND_HISTORY: DERIVATION_SNAPSHOT_STORE,
}

# The two strategies a surviving attribute may declare. The same two words
# the Data Curation pattern's survivorship uses, and the same meaning:
# ``most_recent`` takes the value in force at the latest instant,
# ``first_non_null`` the first non-null value in declared satellite order.
STRATEGY_MOST_RECENT = "most_recent"
STRATEGY_FIRST_NON_NULL = "first_non_null"
SURVIVING_STRATEGIES: tuple[str, ...] = (STRATEGY_MOST_RECENT, STRATEGY_FIRST_NON_NULL)

# The columns the shape generates for itself. Each is computed rather than
# carried, so a composition already carrying one of these names would make
# the generated SQL ambiguous and fails closed instead.
IDENTITY_SUFFIX = "_hk"
ASSOCIATION_SUFFIX = "_lhk"
FINGERPRINT_COLUMN = "hashdiff"
BASIS_VERSION_COLUMN = "hashdiff_basis_version"
LOAD_DATETIME_COLUMN = "load_datetime"
RECORD_SOURCE_COLUMN = "record_source"
AS_OF_COLUMN = "as_of"

# What a reader of a satellite has to know about the basis version beside
# it, published on the column itself so the contract carries the rule
# rather than leaving a consumer to infer it from two rows that differ in
# nothing else.
BASIS_VERSION_DESCRIPTION = (
    "The hashdiff basis version this version's fingerprint was computed under. A "
    "re-baseline stores one version per entity under the next version and leaves every "
    "version stored under the previous one in place, so a reader that wants the "
    "entity's current state scopes to the highest basis version it carries."
)

GENERATED_COLUMNS: tuple[str, ...] = (
    FINGERPRINT_COLUMN,
    BASIS_VERSION_COLUMN,
    LOAD_DATETIME_COLUMN,
    RECORD_SOURCE_COLUMN,
    AS_OF_COLUMN,
    EFFECTIVE_FROM,
)

# The neutral types the columns this shape generates publish as.
IDENTITY_TYPE = "string"
FINGERPRINT_TYPE = "string"
BASIS_VERSION_TYPE = "integer"
LOAD_DATETIME_TYPE = "timestamp"
RECORD_SOURCE_TYPE = "string"


def _name_schema() -> dict:
    return {"type": "string", "minLength": 1}


def _column_list(minimum: int = 1) -> dict:
    return {
        "type": "array",
        "minItems": minimum,
        "items": {"type": "string", "minLength": 1},
    }


DATA_VAULT_SHAPE_CONFIG_SCHEMA: dict = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "title": (
        "data_vault shape_config -- the hubs, links, satellites, business-vault rules "
        "and point-in-time relations this product's vault renders"
    ),
    "type": "object",
    "additionalProperties": False,
    "required": ["hubs", "satellites"],
    "properties": {
        "hubs": {
            "type": "array",
            "minItems": 1,
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": ["name", "business_key"],
                "properties": {
                    "name": _name_schema(),
                    "business_key": _column_list(),
                    "description": _name_schema(),
                },
            },
        },
        "links": {
            "type": "array",
            "minItems": 1,
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": ["name", "hubs"],
                "properties": {
                    "name": _name_schema(),
                    "hubs": _column_list(minimum=2),
                    "description": _name_schema(),
                },
            },
        },
        "satellites": {
            "type": "array",
            "minItems": 1,
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": ["name", "kind", "parent", "payload", "change_column"],
                "properties": {
                    "name": _name_schema(),
                    "kind": {"type": "string", "enum": list(SATELLITE_KINDS)},
                    "parent": _name_schema(),
                    "payload": _column_list(),
                    "change_column": _name_schema(),
                    "hashdiff_exclude": _column_list(minimum=0),
                    "description": _name_schema(),
                },
            },
        },
        "business_vault": {
            "type": "array",
            "minItems": 1,
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": ["name", "hub", "satellites", "attributes"],
                "properties": {
                    "name": _name_schema(),
                    "hub": _name_schema(),
                    "satellites": _column_list(),
                    "attributes": {
                        "type": "array",
                        "minItems": 1,
                        "items": {
                            "type": "object",
                            "additionalProperties": False,
                            "required": ["name", "strategy"],
                            "properties": {
                                "name": _name_schema(),
                                "strategy": {
                                    "type": "string",
                                    "enum": list(SURVIVING_STRATEGIES),
                                },
                            },
                        },
                    },
                    "description": _name_schema(),
                },
            },
        },
        "point_in_time": {
            "type": "array",
            "minItems": 1,
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": ["name", "hub", "satellites"],
                "properties": {
                    "name": _name_schema(),
                    "hub": _name_schema(),
                    "satellites": _column_list(),
                    "description": _name_schema(),
                },
            },
        },
    },
}


def hub_suffix(name: str) -> str:
    """The relation and model suffix one declared hub takes under its
    product's namespace. One owner of the name, so the contract, the graph
    and every rendered artefact take the same one."""

    return f"{HUB_PREFIX}_{name}"


def link_suffix(name: str) -> str:
    """The suffix one declared link takes under its product's namespace."""

    return f"{LINK_PREFIX}_{name}"


def satellite_suffix(name: str) -> str:
    """The suffix one declared satellite takes under its product's
    namespace."""

    return f"{SATELLITE_PREFIX}_{name}"


def surviving_suffix(name: str) -> str:
    """The suffix one declared business-vault surviving record takes under
    its product's namespace."""

    return f"{SURVIVING_PREFIX}_{name}"


def point_in_time_suffix(name: str) -> str:
    """The suffix one declared point-in-time relation takes under its
    product's namespace."""

    return f"{POINT_IN_TIME_PREFIX}_{name}"


def identity_column(name: str) -> str:
    """The identity key column one declared hub publishes."""

    return f"{name}{IDENTITY_SUFFIX}"


def association_column(name: str) -> str:
    """The identity key column one declared link publishes."""

    return f"{name}{ASSOCIATION_SUFFIX}"


def pointer_column(satellite: str) -> str:
    """The column a point-in-time relation publishes for one satellite:
    the instant of the version of that satellite in force at the
    snapshot."""

    return f"{satellite}_{EFFECTIVE_FROM}"


@dataclass(frozen=True)
class Hub:
    """One declared hub, resolved: the relation suffix it renders under,
    the identity key column it publishes, and the ordered business-key
    columns that key is hashed from."""

    name: str
    suffix: str
    key_column: str
    business_key: tuple[str, ...]


@dataclass(frozen=True)
class Link:
    """One declared link, resolved: its own identity key column, the hubs
    it associates in declared order, and the ordered business-key columns
    the association key is hashed from (every hub's, in that order)."""

    name: str
    suffix: str
    key_column: str
    hubs: tuple[Hub, ...]

    @property
    def business_key(self) -> tuple[str, ...]:
        return tuple(column for hub in self.hubs for column in hub.business_key)


@dataclass(frozen=True)
class Satellite:
    """One declared satellite, resolved: the parent whose identity key it
    hangs off, the payload it stores, the frozen column set its change
    fingerprint is computed over, the column that orders its versions, and
    the kind that decides when a delivered payload is a new version."""

    name: str
    suffix: str
    kind: str
    parent_key_column: str
    parent_business_key: tuple[str, ...]
    payload: tuple[str, ...]
    basis: tuple[str, ...]
    exclusions: tuple[str, ...]
    change_column: str


@dataclass(frozen=True)
class SurvivingAttribute:
    """One attribute of a business-vault surviving record: the column and
    the strategy that picks its surviving value."""

    name: str
    strategy: str


@dataclass(frozen=True)
class SurvivingRecord:
    """One declared business-vault rule, resolved: the hub it publishes one
    row per entity of, the satellites its values are survived across in
    declared order, and the attributes it survives."""

    name: str
    suffix: str
    hub: Hub
    satellites: tuple[Satellite, ...]
    attributes: tuple[SurvivingAttribute, ...]


@dataclass(frozen=True)
class PointInTime:
    """One declared point-in-time relation, resolved: the hub it is keyed
    by, the satellites it points into, and the neutral type its snapshot
    instant publishes as."""

    name: str
    suffix: str
    hub: Hub
    satellites: tuple[Satellite, ...]


@dataclass(frozen=True)
class VaultPlan:
    """One product's declared vault, resolved against the field set its
    composition leaves. Every check the shape makes has already run, so a
    reader of this plan renders what it names without re-deciding
    anything."""

    product: str
    hubs: tuple[Hub, ...]
    links: tuple[Link, ...]
    satellites: tuple[Satellite, ...]
    surviving: tuple[SurvivingRecord, ...]
    point_in_time: tuple[PointInTime, ...]

    def satellite(self, suffix: str) -> Satellite:
        for entry in self.satellites:
            if entry.suffix == suffix:
                return entry
        raise KeyError(suffix)


def _fail(product: str, occurrence: str, detail: str) -> ShapeConstraintError:
    return ShapeConstraintError(
        product=product, shape=SHAPE_NAME, occurrence=occurrence, detail=detail
    )


def _unique(names: Sequence[str], *, product: str, occurrence: str, kind: str) -> None:
    seen: set[str] = set()
    for entry in names:
        if entry in seen:
            raise _fail(
                product,
                occurrence,
                f"{kind} {entry!r} is declared twice; one declared name, one relation",
            )
        seen.add(entry)


def _check_not_generated(
    columns: Sequence[str], *, product: str, occurrence: str, role: str
) -> None:
    clashing = [column for column in columns if column in GENERATED_COLUMNS]
    if clashing:
        raise _fail(
            product,
            occurrence,
            (
                f"{role} names {', '.join(clashing)}, which this shape generates for the "
                f"relations it renders; a composition column of that name would make the "
                "generated SQL ambiguous"
            ),
        )


def _fields_for(
    columns: Sequence[str],
    *,
    fields: Sequence[RelationField],
    product: str,
    occurrence: str,
    required: bool = False,
) -> tuple[RelationField, ...]:
    return tuple(
        RelationField(name=entry.name, type=entry.type, required=entry.required or required)
        for entry in (
            column_field(fields, column, product=product, shape=SHAPE_NAME, occurrence=occurrence)
            for column in columns
        )
    )


def _metadata_fields() -> tuple[RelationField, ...]:
    return (
        RelationField(name=LOAD_DATETIME_COLUMN, type=LOAD_DATETIME_TYPE, required=True),
        RelationField(name=RECORD_SOURCE_COLUMN, type=RECORD_SOURCE_TYPE, required=True),
    )


def resolve_plan(*, product: str, shape_config: Mapping[str, Any]) -> VaultPlan:
    """This product's declared vault, resolved from the declaration alone.
    Every cross-reference inside the target block -- a link naming a hub, a
    satellite naming its parent, a business-vault rule or a point-in-time
    relation naming its satellites -- is resolved here and fails closed
    naming the product, the shape and the position in the declaration.

    What the composition must carry for the plan to render is
    ``check_composition`` below. Both the shape's own ``relations`` and the
    translator that renders them read this one answer, so which columns
    feed which key can never be derived two ways."""

    hub_entries = list(shape_config.get("hubs") or [])
    _unique(
        [str(entry["name"]) for entry in hub_entries],
        product=product,
        occurrence="target.shape_config.hubs",
        kind="hub",
    )
    hubs: dict[str, Hub] = {}
    for index, entry in enumerate(hub_entries):
        occurrence = f"target.shape_config.hubs[{index}]"
        business_key = tuple(str(column) for column in entry["business_key"])
        _check_not_generated(
            business_key, product=product, occurrence=occurrence, role="the business key"
        )
        _unique(
            business_key,
            product=product,
            occurrence=occurrence,
            kind="business-key column",
        )
        name = str(entry["name"])
        hubs[name] = Hub(
            name=name,
            suffix=hub_suffix(name),
            key_column=identity_column(name),
            business_key=business_key,
        )

    link_entries = list(shape_config.get("links") or [])
    _unique(
        [str(entry["name"]) for entry in link_entries],
        product=product,
        occurrence="target.shape_config.links",
        kind="link",
    )
    links: dict[str, Link] = {}
    for index, entry in enumerate(link_entries):
        occurrence = f"target.shape_config.links[{index}]"
        referenced = [str(name) for name in entry["hubs"]]
        _unique(referenced, product=product, occurrence=occurrence, kind="hub reference")
        resolved_hubs = []
        for hub_name in referenced:
            hub = hubs.get(hub_name)
            if hub is None:
                raise _fail(
                    product,
                    occurrence,
                    f"hub {hub_name!r} is not one this product declares: {sorted(hubs)}",
                )
            resolved_hubs.append(hub)
        name = str(entry["name"])
        links[name] = Link(
            name=name,
            suffix=link_suffix(name),
            key_column=association_column(name),
            hubs=tuple(resolved_hubs),
        )

    parents: dict[str, tuple[str, tuple[str, ...]]] = {
        **{name: (hub.key_column, hub.business_key) for name, hub in hubs.items()},
        **{name: (link.key_column, link.business_key) for name, link in links.items()},
    }

    satellite_entries = list(shape_config.get("satellites") or [])
    _unique(
        [str(entry["name"]) for entry in satellite_entries],
        product=product,
        occurrence="target.shape_config.satellites",
        kind="satellite",
    )
    satellites: dict[str, Satellite] = {}
    for index, entry in enumerate(satellite_entries):
        occurrence = f"target.shape_config.satellites[{index}]"
        parent_name = str(entry["parent"])
        parent = parents.get(parent_name)
        if parent is None:
            raise _fail(
                product,
                occurrence,
                (
                    f"parent {parent_name!r} is neither a hub nor a link this product declares: "
                    f"{sorted(parents)}"
                ),
            )
        payload = tuple(str(column) for column in entry["payload"])
        _unique(payload, product=product, occurrence=occurrence, kind="payload column")
        _check_not_generated(payload, product=product, occurrence=occurrence, role="the payload")
        change_column = str(entry["change_column"])
        if change_column in payload:
            raise _fail(
                product,
                occurrence,
                (
                    f"column {change_column!r} is both the payload and the column ordering its "
                    "versions; a satellite's change column says when a version came into force, "
                    "never what it holds"
                ),
            )
        exclusions = tuple(str(column) for column in entry.get("hashdiff_exclude") or [])
        outside = [column for column in exclusions if column not in payload]
        if outside:
            raise _fail(
                product,
                f"{occurrence}.hashdiff_exclude",
                (
                    f"column(s) {', '.join(outside)} are excluded from change detection and are "
                    "not in this satellite's payload; only a stored column can be left out of "
                    "the fingerprint over it"
                ),
            )
        basis = tuple(column for column in payload if column not in exclusions)
        if not basis:
            raise _fail(
                product,
                f"{occurrence}.hashdiff_exclude",
                (
                    "every payload column is excluded from change detection, which leaves no "
                    "column to fingerprint; a satellite that can never detect a change stores "
                    "one version and calls every later one identical"
                ),
            )
        name = str(entry["name"])
        satellites[name] = Satellite(
            name=name,
            suffix=satellite_suffix(name),
            kind=str(entry["kind"]),
            parent_key_column=parent[0],
            parent_business_key=parent[1],
            payload=payload,
            basis=basis,
            exclusions=exclusions,
            change_column=change_column,
        )

    def _satellites_of(
        hub: Hub, declared: Sequence[str], *, occurrence: str
    ) -> tuple[Satellite, ...]:
        resolved: list[Satellite] = []
        _unique(
            [str(entry) for entry in declared],
            product=product,
            occurrence=occurrence,
            kind="satellite reference",
        )
        for satellite_name in declared:
            satellite = satellites.get(str(satellite_name))
            if satellite is None:
                raise _fail(
                    product,
                    occurrence,
                    (
                        f"satellite {str(satellite_name)!r} is not one this product declares: "
                        f"{sorted(satellites)}"
                    ),
                )
            if satellite.parent_key_column != hub.key_column:
                raise _fail(
                    product,
                    occurrence,
                    (
                        f"satellite {satellite.name!r} hangs off {satellite.parent_key_column!r} "
                        f"and this relation is keyed by {hub.key_column!r}; a relation over a hub "
                        "reads only the satellites of that hub"
                    ),
                )
            resolved.append(satellite)
        return tuple(resolved)

    surviving_entries = list(shape_config.get("business_vault") or [])
    _unique(
        [str(entry["name"]) for entry in surviving_entries],
        product=product,
        occurrence="target.shape_config.business_vault",
        kind="business-vault rule",
    )
    surviving: list[SurvivingRecord] = []
    for index, entry in enumerate(surviving_entries):
        occurrence = f"target.shape_config.business_vault[{index}]"
        hub = hubs.get(str(entry["hub"]))
        if hub is None:
            raise _fail(
                product,
                occurrence,
                f"hub {str(entry['hub'])!r} is not one this product declares: {sorted(hubs)}",
            )
        rule_satellites = _satellites_of(hub, entry["satellites"], occurrence=occurrence)
        attributes = tuple(
            SurvivingAttribute(name=str(item["name"]), strategy=str(item["strategy"]))
            for item in entry["attributes"]
        )
        _unique(
            [item.name for item in attributes],
            product=product,
            occurrence=occurrence,
            kind="surviving attribute",
        )
        stored = {column for satellite in rule_satellites for column in satellite.payload}
        missing = [item.name for item in attributes if item.name not in stored]
        if missing:
            raise _fail(
                product,
                occurrence,
                (
                    f"attribute(s) {', '.join(missing)} are survived from satellites that store "
                    f"{sorted(stored)}; a surviving value comes from a stored one"
                ),
            )
        name = str(entry["name"])
        surviving.append(
            SurvivingRecord(
                name=name,
                suffix=surviving_suffix(name),
                hub=hub,
                satellites=rule_satellites,
                attributes=attributes,
            )
        )

    pit_entries = list(shape_config.get("point_in_time") or [])
    _unique(
        [str(entry["name"]) for entry in pit_entries],
        product=product,
        occurrence="target.shape_config.point_in_time",
        kind="point-in-time relation",
    )
    point_in_time: list[PointInTime] = []
    for index, entry in enumerate(pit_entries):
        occurrence = f"target.shape_config.point_in_time[{index}]"
        hub = hubs.get(str(entry["hub"]))
        if hub is None:
            raise _fail(
                product,
                occurrence,
                f"hub {str(entry['hub'])!r} is not one this product declares: {sorted(hubs)}",
            )
        pit_satellites = _satellites_of(hub, entry["satellites"], occurrence=occurrence)
        name = str(entry["name"])
        point_in_time.append(
            PointInTime(
                name=name,
                suffix=point_in_time_suffix(name),
                hub=hub,
                satellites=pit_satellites,
            )
        )

    return VaultPlan(
        product=product,
        hubs=tuple(hubs.values()),
        links=tuple(links.values()),
        satellites=tuple(satellites.values()),
        surviving=tuple(surviving),
        point_in_time=tuple(point_in_time),
    )


def check_composition(
    plan: VaultPlan, *, product: str, fields: Sequence[RelationField]
) -> dict[str, RelationField]:
    """Every column a resolved plan reads, checked against the field set
    the composition leaves, and the change column of each satellite
    returned with the neutral type the composition declares for it.

    A vault reads only columns the composition carries: a business key, a
    payload column and a change column each fail closed here naming the
    product, the shape and the position. A change column that is not an
    instant fails too, and so does a point-in-time relation whose
    satellites order their versions by two different instant types, since
    one snapshot column cannot publish as both."""

    for hub in plan.hubs:
        _fields_for(
            hub.business_key,
            fields=fields,
            product=product,
            occurrence=f"target.shape_config.hubs:{hub.name}",
        )
    change_fields: dict[str, RelationField] = {}
    for satellite in plan.satellites:
        occurrence = f"target.shape_config.satellites:{satellite.name}"
        _fields_for(satellite.payload, fields=fields, product=product, occurrence=occurrence)
        change_field = column_field(
            fields,
            satellite.change_column,
            product=product,
            shape=SHAPE_NAME,
            occurrence=f"{occurrence}.change_column",
        )
        if change_field.type not in TIME_TYPES:
            raise _fail(
                product,
                f"{occurrence}.change_column",
                (
                    f"column {satellite.change_column!r} is declared {change_field.type!r}; a "
                    f"version comes into force at an instant, one of {sorted(TIME_TYPES)}"
                ),
            )
        change_fields[satellite.name] = change_field
    for pit in plan.point_in_time:
        types = {change_fields[entry.name].type for entry in pit.satellites}
        if len(types) != 1:
            raise _fail(
                product,
                f"target.shape_config.point_in_time:{pit.name}",
                (
                    "the satellites this relation reads order their versions by "
                    f"{sorted(str(entry) for entry in types)}; one snapshot instant cannot be "
                    "two types at once"
                ),
            )
    return change_fields


def payload_types(
    relations: Sequence[ShapeRelation], *, shape_config: Mapping[str, Any]
) -> dict[str, dict[str, Any]]:
    """The neutral type each satellite's stored columns publish as, read off
    the relations the shape renders rather than off the composition.

    Two callers read it: the emission route, grading the declaration against
    the estate's record, and the re-baseline operation, staging the declared
    record. Both take the type from the relation that publishes the column,
    so a record and a contract can never disagree about what a stored column
    is."""

    by_suffix = {entry.suffix: entry for entry in relations}
    types: dict[str, dict[str, Any]] = {}
    for entry in shape_config.get("satellites") or []:
        name = str(entry.get("name"))
        relation = by_suffix.get(satellite_suffix(name))
        if relation is None:
            continue
        published = {field.name: field.type for field in relation.schema.fields}
        types[name] = {
            str(column): published[str(column)]
            for column in entry.get("payload") or []
            if str(column) in published
        }
    return types


class DataVaultShape(ShapeDefinition):
    """Hubs, links, satellites, business-vault surviving records and
    point-in-time relations over a curated product."""

    def relation_names(
        self, *, domain: str, name: str, shape_config: Mapping[str, Any] | None = None
    ) -> tuple[str, ...]:
        # Answered from the declared names alone: this is asked before the
        # composition's field set is known, so it resolves no column.
        config = shape_config or {}
        product = f"{domain}.{name}"
        suffixes = [
            *(hub_suffix(str(entry["name"])) for entry in config.get("hubs") or []),
            *(link_suffix(str(entry["name"])) for entry in config.get("links") or []),
            *(satellite_suffix(str(entry["name"])) for entry in config.get("satellites") or []),
            *(surviving_suffix(str(entry["name"])) for entry in config.get("business_vault") or []),
            *(
                point_in_time_suffix(str(entry["name"]))
                for entry in config.get("point_in_time") or []
            ),
        ]
        return tuple(f"{product}__{suffix}" for suffix in suffixes)

    def relations(
        self,
        *,
        domain: str,
        name: str,
        shape_config: Mapping[str, Any],
        fields: Sequence[RelationField],
    ) -> tuple[ShapeRelation, ...]:
        product = f"{domain}.{name}"
        plan = resolve_plan(product=product, shape_config=shape_config)
        change_fields = check_composition(plan, product=product, fields=fields)
        relations: list[ShapeRelation] = []

        for hub in plan.hubs:
            occurrence = f"target.shape_config.hubs:{hub.name}"
            published = (
                RelationField(name=hub.key_column, type=IDENTITY_TYPE, required=True),
                *_fields_for(
                    hub.business_key,
                    fields=fields,
                    product=product,
                    occurrence=occurrence,
                    required=True,
                ),
                *_metadata_fields(),
            )
            relations.append(
                ShapeRelation(
                    suffix=hub.suffix,
                    schema=RelationSchema(name=f"{product}__{hub.suffix}", fields=published),
                    derivation=DERIVATION_IDENTITY_STORE,
                    source_columns=hub.business_key,
                    key=(hub.key_column,),
                    materialisation=MATERIALISATION_INCREMENTAL,
                )
            )

        for link in plan.links:
            published = (
                RelationField(name=link.key_column, type=IDENTITY_TYPE, required=True),
                *(
                    RelationField(name=hub.key_column, type=IDENTITY_TYPE, required=True)
                    for hub in link.hubs
                ),
                *_metadata_fields(),
            )
            relations.append(
                ShapeRelation(
                    suffix=link.suffix,
                    schema=RelationSchema(name=f"{product}__{link.suffix}", fields=published),
                    derivation=DERIVATION_ASSOCIATION_STORE,
                    source_columns=link.business_key,
                    key=(link.key_column,),
                    materialisation=MATERIALISATION_INCREMENTAL,
                )
            )

        for satellite in plan.satellites:
            occurrence = f"target.shape_config.satellites:{satellite.name}"
            published = (
                RelationField(name=satellite.parent_key_column, type=IDENTITY_TYPE, required=True),
                RelationField(name=FINGERPRINT_COLUMN, type=FINGERPRINT_TYPE, required=True),
                RelationField(
                    name=BASIS_VERSION_COLUMN,
                    type=BASIS_VERSION_TYPE,
                    required=True,
                    description=BASIS_VERSION_DESCRIPTION,
                ),
                *_fields_for(
                    satellite.payload, fields=fields, product=product, occurrence=occurrence
                ),
                RelationField(
                    name=EFFECTIVE_FROM,
                    type=change_fields[satellite.name].type,
                    required=True,
                ),
                *_metadata_fields(),
            )
            relations.append(
                ShapeRelation(
                    suffix=satellite.suffix,
                    schema=RelationSchema(
                        name=f"{product}__{satellite.suffix}", fields=published
                    ),
                    derivation=SATELLITE_DERIVATIONS[satellite.kind],
                    source_columns=(
                        satellite.parent_business_key
                        + satellite.payload
                        + (satellite.change_column,)
                    ),
                    key=(satellite.parent_key_column, EFFECTIVE_FROM, BASIS_VERSION_COLUMN),
                    materialisation=MATERIALISATION_INCREMENTAL,
                    change_column=satellite.change_column,
                )
            )

        for record in plan.surviving:
            occurrence = f"target.shape_config.business_vault:{record.name}"
            published = (
                RelationField(name=record.hub.key_column, type=IDENTITY_TYPE, required=True),
                *_fields_for(
                    record.hub.business_key,
                    fields=fields,
                    product=product,
                    occurrence=occurrence,
                    required=True,
                ),
                *_fields_for(
                    tuple(entry.name for entry in record.attributes),
                    fields=fields,
                    product=product,
                    occurrence=occurrence,
                ),
                *_metadata_fields(),
            )
            relations.append(
                ShapeRelation(
                    suffix=record.suffix,
                    schema=RelationSchema(name=f"{product}__{record.suffix}", fields=published),
                    derivation=DERIVATION_SURVIVING_RECORD,
                    source_columns=(
                        record.hub.business_key
                        + tuple(entry.name for entry in record.attributes)
                    ),
                    key=(record.hub.key_column,),
                    materialisation=MATERIALISATION_TABLE,
                    reads=(record.hub.suffix, *(entry.suffix for entry in record.satellites)),
                )
            )

        for pit in plan.point_in_time:
            published = (
                RelationField(name=pit.hub.key_column, type=IDENTITY_TYPE, required=True),
                RelationField(
                    name=AS_OF_COLUMN,
                    type=change_fields[pit.satellites[0].name].type,
                    required=True,
                ),
                *(
                    RelationField(
                        name=pointer_column(entry.name),
                        type=change_fields[entry.name].type,
                        required=False,
                    )
                    for entry in pit.satellites
                ),
            )
            relations.append(
                ShapeRelation(
                    suffix=pit.suffix,
                    schema=RelationSchema(name=f"{product}__{pit.suffix}", fields=published),
                    derivation=DERIVATION_POINT_IN_TIME,
                    source_columns=pit.hub.business_key,
                    key=(pit.hub.key_column, AS_OF_COLUMN),
                    materialisation=MATERIALISATION_TABLE,
                    reads=tuple(entry.suffix for entry in pit.satellites),
                )
            )

        return tuple(relations)

    def ledger_relative_path(self, *, domain: str, name: str) -> str | None:
        return evolution_mod.ledger_relative_path(domain=domain, name=name)

    def ledger(
        self,
        *,
        product: str,
        shape_config: Mapping[str, Any],
        relations: Sequence[ShapeRelation],
        recorded: Mapping[str, Any] | None,
    ) -> ShapeLedger | None:
        domain, _, name = product.partition(".")
        declared = evolution_mod.declared_records(
            shape_config.get("satellites") or [],
            types=payload_types(relations, shape_config=shape_config),
        )
        document, notices = evolution_mod.grade(
            product=product, declared=declared, recorded=recorded
        )
        return ShapeLedger(
            relative_path=evolution_mod.ledger_relative_path(domain=domain, name=name),
            document=document,
            notices=notices,
            store_versions=evolution_mod.basis_versions(document),
        )

    def check_estate(
        self,
        *,
        product: str,
        shape_config: Mapping[str, Any],
        document: Mapping[str, Any],
        upstream: Mapping[str, Mapping[str, Any]],
    ) -> None:
        patterns = [(step or {}).get("pattern") for step in (document.get("steps") or [])]
        if CURATION_PATTERN not in patterns:
            raise _fail(
                product,
                "steps",
                (
                    f"the composition declares no {CURATION_PATTERN!r} occurrence; a vault stores "
                    "an entity's identity and the versions of its payload, and neither is "
                    "meaningful over records nothing resolved into an entity"
                ),
            )


DATA_VAULT_SHAPE = register_shape(
    DataVaultShape(name=SHAPE_NAME, shape_config_schema=DATA_VAULT_SHAPE_CONFIG_SCHEMA)
)

__all__ = [
    "AS_OF_COLUMN",
    "ASSOCIATION_SUFFIX",
    "BASIS_VERSION_COLUMN",
    "BASIS_VERSION_TYPE",
    "CURATION_PATTERN",
    "DATA_VAULT_SHAPE",
    "DATA_VAULT_SHAPE_CONFIG_SCHEMA",
    "DataVaultShape",
    "FINGERPRINT_COLUMN",
    "GENERATED_COLUMNS",
    "Hub",
    "IDENTITY_SUFFIX",
    "KIND_CURRENT",
    "KIND_HISTORY",
    "LOAD_DATETIME_COLUMN",
    "Link",
    "PointInTime",
    "RECORD_SOURCE_COLUMN",
    "SATELLITE_KINDS",
    "SHAPE_NAME",
    "STRATEGY_FIRST_NON_NULL",
    "STRATEGY_MOST_RECENT",
    "SURVIVING_STRATEGIES",
    "Satellite",
    "SurvivingAttribute",
    "SurvivingRecord",
    "VaultPlan",
    "BASIS_VERSION_DESCRIPTION",
    "association_column",
    "check_composition",
    "hub_suffix",
    "identity_column",
    "link_suffix",
    "payload_types",
    "point_in_time_suffix",
    "pointer_column",
    "resolve_plan",
    "satellite_suffix",
    "surviving_suffix",
]
