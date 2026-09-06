"""The ``ods`` shape: an operational data store's normalised relational
form (architecture section 6): entity relations with declared keys and
effectivity, one cross-reference relation and one audit tail relation, for
every entity the target block declares.

The target block declares one or more entities. Each names the columns
that identify it (``key``), the columns describing it (``attributes``),
the column its history is ordered by (``change_column``), and the two
columns the raw composition carries for where a row came from
(``source_system_column``, ``source_key_column``) and what happened to it
(``operation_column``). Ownership is declared, never inferred (owner
ruling R1): this shape does not resolve an entity key itself, and it adds
no constraint of its own on the composition (architecture section 6: an
``ods`` product's target block needs entities and keys, and adds nothing
to composition). It reads whichever entity key the composition's own
occurrences leave, exactly as every other shape reads the columns the
composition leaves.

Three relations render per declared entity, all of them tables -- an
operational data store computes, it is never an interface boundary:

  * the entity relation carries one row per version of the entity's
    attributes, with a half-open effective range: ``effective_from`` is
    the change instant the version came into force, ``effective_to`` is
    the instant the next version came into force, and the version in
    force now carries none. A version starts where an attribute differs
    from the one before it, which is exactly the derivation the
    dimensional shape's type 2 dimension already renders, so this shape
    asks the dbt translator for the same arm
    (``DERIVATION_DIMENSION_TYPE_2``) rather than a second one that would
    compute the same thing twice;
  * the cross-reference relation carries one row per distinct source
    system and source key, resolved to the entity key its most recent row
    named -- the same derivation a type 1 dimension renders
    (``DERIVATION_DIMENSION_TYPE_1``): "the current version of one key",
    read here as "the current resolution of one source key";
  * the audit tail carries one row per row the composition delivers, with
    no filtering and no collapsing: a straight projection
    (``DERIVATION_PROJECTION``) of the entity key, the source system and
    key, the change timestamp and the operation.

No new rendering arm is needed: the three relations this shape declares
are three existing derivations, applied to different column roles.
"""

from __future__ import annotations

from typing import Any, Mapping, Sequence

from ergasterion.framework.models import RelationField, RelationSchema
from ergasterion.framework.shapes import (
    DERIVATION_DIMENSION_TYPE_1,
    DERIVATION_DIMENSION_TYPE_2,
    DERIVATION_PROJECTION,
    EFFECTIVE_FROM,
    EFFECTIVE_TO,
    MATERIALISATION_TABLE,
    TIME_TYPES,
    ShapeConstraintError,
    ShapeDefinition,
    ShapeRelation,
    column_field,
    register_shape,
)

SHAPE_NAME = "ods"

# EFFECTIVE_FROM and EFFECTIVE_TO -- the two columns an entity relation
# publishes beside its key and attributes, half-open: a row covers
# [effective_from, effective_to), and the row in force now carries no
# effective_to -- are declared once in ergasterion.framework.shapes,
# because the dimensional shape's type 2 dimension publishes the very
# same range, and imported here re-exported so a caller of this module
# reads them from it.

# TIME_TYPES -- the neutral types a change column may declare -- is
# declared in ergasterion.framework.shapes for the same reason as the two
# range columns above: the data_vault shape asks the same question of its
# own declared change column, so the answer is owned once and imported
# here rather than restated.

XREF_SUFFIX = "xref"
AUDIT_SUFFIX = "audit"

ODS_SHAPE_CONFIG_SCHEMA: dict = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "title": (
        "ods shape_config -- one or more entities, each with its key, its "
        "effectivity and its cross-reference columns"
    ),
    "type": "object",
    "additionalProperties": False,
    "required": ["entities"],
    "properties": {
        "entities": {
            "type": "array",
            "minItems": 1,
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": [
                    "name",
                    "key",
                    "attributes",
                    "change_column",
                    "source_system_column",
                    "source_key_column",
                    "operation_column",
                ],
                "properties": {
                    "name": {"type": "string", "minLength": 1},
                    "key": {
                        "type": "array",
                        "minItems": 1,
                        "items": {"type": "string", "minLength": 1},
                    },
                    "attributes": {
                        "type": "array",
                        "minItems": 1,
                        "items": {"type": "string", "minLength": 1},
                    },
                    "change_column": {"type": "string", "minLength": 1},
                    "source_system_column": {"type": "string", "minLength": 1},
                    "source_key_column": {"type": "string", "minLength": 1},
                    "operation_column": {"type": "string", "minLength": 1},
                    "description": {"type": "string", "minLength": 1},
                },
            },
        }
    },
}


def xref_suffix(name: str) -> str:
    """The relation and model suffix one entity's cross-reference relation
    takes under its product's namespace. One owner of the name, so the
    contract, the graph and every rendered artefact take the same one."""

    return f"{name}_{XREF_SUFFIX}"


def audit_suffix(name: str) -> str:
    """The relation and model suffix one entity's audit tail takes under
    its product's namespace."""

    return f"{name}_{AUDIT_SUFFIX}"


class OdsShape(ShapeDefinition):
    """One entity relation, one cross-reference relation and one audit
    tail per declared entity."""

    def _entities(self, shape_config: Mapping[str, Any]) -> list[Mapping[str, Any]]:
        return list(shape_config.get("entities") or [])

    def relation_names(
        self, *, domain: str, name: str, shape_config: Mapping[str, Any] | None = None
    ) -> tuple[str, ...]:
        config = shape_config or {}
        product = f"{domain}.{name}"
        names: list[str] = []
        for entity in self._entities(config):
            entity_name = str(entity["name"])
            names.append(f"{product}__{entity_name}")
            names.append(f"{product}__{xref_suffix(entity_name)}")
            names.append(f"{product}__{audit_suffix(entity_name)}")
        return tuple(names)

    def _roles(
        self, entity: Mapping[str, Any], *, product: str, occurrence: str
    ) -> dict[str, tuple[str, ...]]:
        """Every column role one declared entity names, checked disjoint
        from every other role: an entity's key identifies it, its
        attributes describe it, its change column orders its history, and
        its source system, source key and operation columns are the raw
        composition's own bookkeeping. Six roles, one column each, never
        shared."""

        key = tuple(str(column) for column in entity["key"])
        attributes = tuple(str(column) for column in entity["attributes"])
        roles: dict[str, tuple[str, ...]] = {
            "key": key,
            "attributes": attributes,
            "change_column": (str(entity["change_column"]),),
            "source_system_column": (str(entity["source_system_column"]),),
            "source_key_column": (str(entity["source_key_column"]),),
            "operation_column": (str(entity["operation_column"]),),
        }
        seen: dict[str, str] = {}
        for role_name, columns in roles.items():
            for column in columns:
                clash = seen.get(column)
                if clash is not None:
                    raise ShapeConstraintError(
                        product=product,
                        shape=self.name,
                        occurrence=occurrence,
                        detail=(
                            f"column {column!r} is declared as both {clash!r} and {role_name!r}; "
                            "an entity's key, attributes, change column, source system, source "
                            "key and operation are six distinct roles, one column each"
                        ),
                    )
                seen[column] = role_name
        reserved = [column for column in key + attributes if column in (EFFECTIVE_FROM, EFFECTIVE_TO)]
        if reserved:
            raise ShapeConstraintError(
                product=product,
                shape=self.name,
                occurrence=occurrence,
                detail=(
                    f"entity already carries {', '.join(reserved)}, which the entity relation "
                    "publishes as its own effective range"
                ),
            )
        return roles

    def _fields_for(
        self,
        columns: Sequence[str],
        *,
        key: Sequence[str],
        product: str,
        occurrence: str,
        fields: Sequence[RelationField],
    ) -> tuple[RelationField, ...]:
        """The composition's own fields for ``columns``, each carrying its
        original required-ness except a column also named in ``key``,
        which a generated relation always requires."""

        return tuple(
            RelationField(
                name=field.name, type=field.type, required=field.required or field.name in key
            )
            for field in (
                column_field(fields, column, product=product, shape=self.name, occurrence=occurrence)
                for column in columns
            )
        )

    def _entity_relation(
        self,
        entity: Mapping[str, Any],
        *,
        product: str,
        occurrence: str,
        fields: Sequence[RelationField],
        roles: Mapping[str, tuple[str, ...]],
    ) -> ShapeRelation:
        name = str(entity["name"])
        key = roles["key"]
        attributes = roles["attributes"]
        change_column = roles["change_column"][0]
        change_field = column_field(
            fields,
            change_column,
            product=product,
            shape=self.name,
            occurrence=f"{occurrence}.change_column",
        )
        if change_field.type not in TIME_TYPES:
            raise ShapeConstraintError(
                product=product,
                shape=self.name,
                occurrence=f"{occurrence}.change_column",
                detail=(
                    f"column {change_column!r} is declared {change_field.type!r}; an entity's "
                    f"effectivity is ordered by an instant, one of {sorted(TIME_TYPES)}"
                ),
            )
        published = self._fields_for(
            key + attributes, key=key, product=product, occurrence=occurrence, fields=fields
        )
        range_fields = (
            RelationField(name=EFFECTIVE_FROM, type=change_field.type, required=True),
            RelationField(name=EFFECTIVE_TO, type=change_field.type, required=False),
        )
        return ShapeRelation(
            suffix=name,
            schema=RelationSchema(name=f"{product}__{name}", fields=published + range_fields),
            derivation=DERIVATION_DIMENSION_TYPE_2,
            source_columns=key + attributes + (change_column,),
            key=key,
            materialisation=MATERIALISATION_TABLE,
            change_column=change_column,
            range_columns=(EFFECTIVE_FROM, EFFECTIVE_TO),
        )

    def _xref_relation(
        self,
        entity: Mapping[str, Any],
        *,
        product: str,
        occurrence: str,
        fields: Sequence[RelationField],
        roles: Mapping[str, tuple[str, ...]],
    ) -> ShapeRelation:
        name = str(entity["name"])
        source_system_column = roles["source_system_column"][0]
        source_key_column = roles["source_key_column"][0]
        change_column = roles["change_column"][0]
        xref_key = (source_system_column, source_key_column)
        columns = xref_key + roles["key"]
        published = self._fields_for(
            columns, key=xref_key, product=product, occurrence=occurrence, fields=fields
        )
        suffix = xref_suffix(name)
        return ShapeRelation(
            suffix=suffix,
            schema=RelationSchema(name=f"{product}__{suffix}", fields=published),
            derivation=DERIVATION_DIMENSION_TYPE_1,
            source_columns=columns + (change_column,),
            key=xref_key,
            materialisation=MATERIALISATION_TABLE,
            change_column=change_column,
        )

    def _audit_relation(
        self,
        entity: Mapping[str, Any],
        *,
        product: str,
        occurrence: str,
        fields: Sequence[RelationField],
        roles: Mapping[str, tuple[str, ...]],
    ) -> ShapeRelation:
        name = str(entity["name"])
        source_system_column = roles["source_system_column"][0]
        source_key_column = roles["source_key_column"][0]
        change_column = roles["change_column"][0]
        operation_column = roles["operation_column"][0]
        audit_key = (source_system_column, source_key_column, change_column)
        columns = roles["key"] + audit_key + (operation_column,)
        published = self._fields_for(
            columns, key=audit_key, product=product, occurrence=occurrence, fields=fields
        )
        suffix = audit_suffix(name)
        return ShapeRelation(
            suffix=suffix,
            schema=RelationSchema(name=f"{product}__{suffix}", fields=published),
            derivation=DERIVATION_PROJECTION,
            source_columns=columns,
            key=audit_key,
            materialisation=MATERIALISATION_TABLE,
        )

    def relations(
        self,
        *,
        domain: str,
        name: str,
        shape_config: Mapping[str, Any],
        fields: Sequence[RelationField],
    ) -> tuple[ShapeRelation, ...]:
        product = f"{domain}.{name}"
        relations: list[ShapeRelation] = []
        seen_names: set[str] = set()
        for index, entity in enumerate(self._entities(shape_config)):
            occurrence = f"target.shape_config.entities[{index}]"
            entity_name = str(entity["name"])
            if entity_name in seen_names:
                raise ShapeConstraintError(
                    product=product,
                    shape=self.name,
                    occurrence=occurrence,
                    detail=f"entity {entity_name!r} is declared twice; one entity, three relations",
                )
            seen_names.add(entity_name)
            roles = self._roles(entity, product=product, occurrence=occurrence)
            relations.append(
                self._entity_relation(
                    entity, product=product, occurrence=occurrence, fields=fields, roles=roles
                )
            )
            relations.append(
                self._xref_relation(
                    entity, product=product, occurrence=occurrence, fields=fields, roles=roles
                )
            )
            relations.append(
                self._audit_relation(
                    entity, product=product, occurrence=occurrence, fields=fields, roles=roles
                )
            )
        return tuple(relations)


ODS_SHAPE = register_shape(OdsShape(name=SHAPE_NAME, shape_config_schema=ODS_SHAPE_CONFIG_SCHEMA))

__all__ = [
    "AUDIT_SUFFIX",
    "EFFECTIVE_FROM",
    "EFFECTIVE_TO",
    "ODS_SHAPE",
    "ODS_SHAPE_CONFIG_SCHEMA",
    "OdsShape",
    "SHAPE_NAME",
    "TIME_TYPES",
    "XREF_SUFFIX",
    "audit_suffix",
    "xref_suffix",
]
