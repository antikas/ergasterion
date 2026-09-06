"""The ``canonical`` shape: one relation per canonical entity over a
curated product, at an interface boundary (architecture section 6).

The product's composition does the reading, checking and publishing; the
shape imposes the modelling: for each declared entity, one relation
carrying the columns that entity is made of, keyed by the columns that
identify it. Every relation is a view, because an interface boundary is
where a consumer reads a shape of the data rather than where the data is
computed. The estate declares which model paths may carry a view
(``declarations/targets/interfaces.yml``); the emission route holds this
shape to that declaration.

An entity may also declare which entity of an external reference model it
is the estate's reading of, and which of that model's attributes each of
its own columns carries. The shape holds that block to what it can prove
without a checkout: it is well formed, and every column it maps is one the
entity publishes. ``ergasterion.validate_canonical`` proves the other half
against a checkout of the model itself.

The shape adds one constraint the composition's profile cannot express:
the product it reads must be a curated one. A canonical entity is the
agreed form of an entity that something already resolved -- a projection
over unresolved records would publish an interface the estate never
curated -- so a product whose upstream carries no Data Curation occurrence
fails closed naming the product, the shape and the upstream.
"""

from __future__ import annotations

from typing import Any, Mapping, Sequence

from ergasterion.framework.models import RelationField, RelationSchema
from ergasterion.framework.shapes import (
    DERIVATION_PROJECTION,
    MATERIALISATION_VIEW,
    ShapeConstraintError,
    ShapeDefinition,
    ShapeRelation,
    column_field,
    register_shape,
)

SHAPE_NAME = "canonical"

# The occurrence a canonical product's upstream must carry.
CURATION_PATTERN = "data_curation"

# The model path this shape's relations sit at, under the translator's own
# product root. An estate declares it as an interface boundary in
# declarations/targets/interfaces.yml, and emission fails closed when it
# does not.
INTERFACE_PATH = "canonical"

# An entity's optional statement of which entity of an external reference
# model it is the estate's reading of, and which of that model's attributes
# each of its own columns carries. The entity is named by its identifier in
# that model, never by a file path: where the model is checked out is an
# operator's fact and a declaration carrying a path would fail the
# neutrality gate (architecture section 13).
#
# What this shape proves about the block needs no checkout: the block is
# well formed and every column it maps is one the entity publishes. That
# the reference model carries the attribute is proved against a checkout by
# ``ergasterion.validate_canonical``.
REFERENCE_KEY = "reference"
REFERENCE_ENTITY_KEY = "entity"
REFERENCE_ATTRIBUTES_KEY = "attributes"

CANONICAL_SHAPE_CONFIG_SCHEMA: dict = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "title": "canonical shape_config -- the entity list, each entity optionally mapped onto a reference model, and nothing else",
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
                "required": ["name", "key", "columns"],
                "properties": {
                    "name": {"type": "string", "minLength": 1},
                    "key": {
                        "type": "array",
                        "minItems": 1,
                        "items": {"type": "string", "minLength": 1},
                    },
                    "columns": {
                        "type": "array",
                        "minItems": 1,
                        "items": {"type": "string", "minLength": 1},
                    },
                    "description": {"type": "string", "minLength": 1},
                    REFERENCE_KEY: {
                        "type": "object",
                        "additionalProperties": False,
                        "required": [REFERENCE_ENTITY_KEY, REFERENCE_ATTRIBUTES_KEY],
                        "properties": {
                            REFERENCE_ENTITY_KEY: {"type": "string", "minLength": 1},
                            REFERENCE_ATTRIBUTES_KEY: {
                                "type": "object",
                                "minProperties": 1,
                                "additionalProperties": {"type": "string", "minLength": 1},
                            },
                        },
                    },
                },
            },
        }
    },
}


class CanonicalShape(ShapeDefinition):
    """One relation per declared entity, each a view over the curated
    product the composition reads."""

    def _entities(self, shape_config: Mapping[str, Any]) -> list[Mapping[str, Any]]:
        return list(shape_config.get("entities") or [])

    def relation_names(
        self, *, domain: str, name: str, shape_config: Mapping[str, Any] | None = None
    ) -> tuple[str, ...]:
        return tuple(
            f"{domain}.{name}__{entity['name']}"
            for entity in self._entities(shape_config or {})
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
        seen: set[str] = set()
        for index, entity in enumerate(self._entities(shape_config)):
            entity_name = str(entity["name"])
            occurrence = f"target.shape_config.entities[{index}]"
            if entity_name in seen:
                raise ShapeConstraintError(
                    product=product,
                    shape=self.name,
                    occurrence=occurrence,
                    detail=f"entity {entity_name!r} is declared twice; one entity, one relation",
                )
            seen.add(entity_name)
            columns = [str(column) for column in entity["columns"]]
            key = tuple(str(column) for column in entity["key"])
            missing = [column for column in key if column not in columns]
            if missing:
                raise ShapeConstraintError(
                    product=product,
                    shape=self.name,
                    occurrence=occurrence,
                    detail=(
                        f"entity {entity_name!r} is keyed on {', '.join(missing)}, which its "
                        "own columns do not carry; an entity is identified by columns it "
                        "publishes"
                    ),
                )
            reference = entity.get(REFERENCE_KEY) or {}
            unpublished = [
                column
                for column in sorted(reference.get(REFERENCE_ATTRIBUTES_KEY) or {})
                if column not in columns
            ]
            if unpublished:
                raise ShapeConstraintError(
                    product=product,
                    shape=self.name,
                    occurrence=occurrence,
                    detail=(
                        f"entity {entity_name!r} maps {', '.join(unpublished)} onto the "
                        "reference model, and its own columns do not carry them; an entity "
                        "maps only columns it publishes"
                    ),
                )
            entity_fields = tuple(
                RelationField(
                    name=field.name,
                    type=field.type,
                    required=field.required or field.name in key,
                )
                for field in (
                    column_field(
                        fields, column, product=product, shape=self.name, occurrence=occurrence
                    )
                    for column in columns
                )
            )
            relations.append(
                ShapeRelation(
                    suffix=entity_name,
                    schema=RelationSchema(
                        name=f"{product}__{entity_name}", fields=entity_fields
                    ),
                    derivation=DERIVATION_PROJECTION,
                    source_columns=tuple(columns),
                    key=key,
                    materialisation=MATERIALISATION_VIEW,
                )
            )
        return tuple(relations)

    def check_estate(
        self,
        *,
        product: str,
        shape_config: Mapping[str, Any],
        document: Mapping[str, Any],
        upstream: Mapping[str, Mapping[str, Any]],
    ) -> None:
        sources = list(document.get("sources") or [])
        if len(sources) != 1:
            raise ShapeConstraintError(
                product=product,
                shape=self.name,
                occurrence="sources",
                detail=(
                    f"{len(sources)} sources are declared; a canonical product is the interface "
                    "over one curated product"
                ),
            )
        published = str(sources[0].get("contract")).split("@", 1)[0]
        document_upstream = upstream.get(published)
        if document_upstream is None:
            raise ShapeConstraintError(
                product=product,
                shape=self.name,
                occurrence="sources[0]",
                detail=(
                    f"upstream {published!r} is not a product this estate declares, so it "
                    f"carries no {CURATION_PATTERN!r} occurrence to curate the entities"
                ),
            )
        patterns = [
            (step or {}).get("pattern") for step in (document_upstream.get("steps") or [])
        ]
        if CURATION_PATTERN not in patterns:
            raise ShapeConstraintError(
                product=product,
                shape=self.name,
                occurrence="sources[0]",
                detail=(
                    f"upstream {published!r} declares no {CURATION_PATTERN!r} occurrence; a "
                    "canonical entity is the agreed form of an entity something already "
                    "resolved"
                ),
            )


CANONICAL_SHAPE = register_shape(
    CanonicalShape(
        name=SHAPE_NAME,
        shape_config_schema=CANONICAL_SHAPE_CONFIG_SCHEMA,
        interface_path=INTERFACE_PATH,
    )
)

__all__ = [
    "CANONICAL_SHAPE",
    "CANONICAL_SHAPE_CONFIG_SCHEMA",
    "CURATION_PATTERN",
    "INTERFACE_PATH",
    "REFERENCE_ATTRIBUTES_KEY",
    "REFERENCE_ENTITY_KEY",
    "REFERENCE_KEY",
    "CanonicalShape",
    "SHAPE_NAME",
]
