"""The ``dimensional`` shape: facts at a declared grain, dimensions as
type 1 and type 2, and the semantic layer over them (architecture section
6, owner ruling R6).

The target block declares the dimensions and the facts. A dimension names
the key that identifies it, the attributes it carries and the column its
versions are ordered by. A type 1 dimension publishes one row per key, the
version the change column ranks last. A type 2 dimension publishes one row
per version of the key, with a half-open effective range: ``effective_from``
is the change instant the version came into force, ``effective_to`` is the
instant the next version came into force, and the version in force now has
none. A fact names its grain, the dimensions it is analysed by and the
measures it carries; the relation it publishes is the grain, the key of
every dimension it names, its time dimension and its measures.

A dimension two facts both name is rendered once: the dimension list, not
the fact, owns the relation, so two facts sharing a conformed dimension
read the same one.

Derivation is a recomputation from the columns the composition leaves.
Nothing here merges into a relation that already exists, so no arm of this
shape needs an adapter's own merge mechanics; a dimension type that did
would be a stop, not a special case.

The semantic layer -- the measures each fact carries, the metrics over
them, and the window the time spine those metrics aggregate over covers --
is declared here and rendered by the translator into its own semantic
model (MetricFlow YAML in the dbt translator). The shape checks that every
measure is a column the composition carries and that every metric names a
measure some fact declares; what the artefact looks like is the
translator's.
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

SHAPE_NAME = "dimensional"

# EFFECTIVE_FROM and EFFECTIVE_TO -- the two columns a type 2 dimension
# publishes beside its key and attributes, half-open: a row covers
# [effective_from, effective_to), and the row in force now carries no
# effective_to -- are declared once in ergasterion.framework.shapes,
# because the ods shape's own entity history publishes the very same
# range, and imported here re-exported so existing callers of this
# module keep reading them from it.

TYPE_1 = 1
TYPE_2 = 2

# TIME_TYPES -- the neutral types a column ordering a history may
# declare -- is declared in ergasterion.framework.shapes: the ods and
# data_vault shapes ask the same question of their own declared change
# columns, so the answer is owned once and imported here rather than
# restated.

AGGREGATIONS: tuple[str, ...] = ("sum", "average", "min", "max", "count")

# The shape of a declared time-spine bound: an ISO calendar date, checked
# by the shape's own schema so a malformed bound never reaches a rendering.
DATE_PATTERN = r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$"

METRIC_TYPES: tuple[str, ...] = ("simple",)

DIMENSIONAL_SHAPE_CONFIG_SCHEMA: dict = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "title": "dimensional shape_config -- dimensions, facts, measures and metrics",
    "type": "object",
    "additionalProperties": False,
    "required": ["dimensions", "facts", "time_spine"],
    "properties": {
        "dimensions": {
            "type": "array",
            "minItems": 1,
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": ["name", "type", "key", "attributes", "change_column"],
                "properties": {
                    "name": {"type": "string", "minLength": 1},
                    "type": {"enum": [TYPE_1, TYPE_2]},
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
                    "description": {"type": "string", "minLength": 1},
                },
            },
        },
        "facts": {
            "type": "array",
            "minItems": 1,
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": ["name", "grain", "dimensions", "time_dimension", "measures"],
                "properties": {
                    "name": {"type": "string", "minLength": 1},
                    "grain": {
                        "type": "array",
                        "minItems": 1,
                        "items": {"type": "string", "minLength": 1},
                    },
                    "dimensions": {
                        "type": "array",
                        "minItems": 1,
                        "items": {"type": "string", "minLength": 1},
                    },
                    "time_dimension": {"type": "string", "minLength": 1},
                    "measures": {
                        "type": "array",
                        "minItems": 1,
                        "items": {
                            "type": "object",
                            "additionalProperties": False,
                            "required": ["name", "aggregation"],
                            "properties": {
                                "name": {"type": "string", "minLength": 1},
                                "aggregation": {"enum": list(AGGREGATIONS)},
                                "description": {"type": "string", "minLength": 1},
                            },
                        },
                    },
                    "description": {"type": "string", "minLength": 1},
                },
            },
        },
        "time_spine": {
            "type": "object",
            "additionalProperties": False,
            "required": ["from", "to"],
            "description": (
                "The declared window the semantic layer's time spine covers, as two ISO "
                "dates. A semantic layer needs a dense date relation to aggregate over, "
                "and how far it runs is estate policy, never something the engine infers "
                "from the rows it happens to see."
            ),
            "properties": {
                "from": {"type": "string", "pattern": DATE_PATTERN},
                "to": {"type": "string", "pattern": DATE_PATTERN},
            },
        },
        "metrics": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": ["name", "type", "measure"],
                "properties": {
                    "name": {"type": "string", "minLength": 1},
                    "type": {"enum": list(METRIC_TYPES)},
                    "measure": {"type": "string", "minLength": 1},
                    "label": {"type": "string", "minLength": 1},
                    "description": {"type": "string", "minLength": 1},
                },
            },
        },
    },
}


def dimension_suffix(name: str) -> str:
    """The relation and model suffix one dimension takes under its
    product's namespace. One owner of the name, so the contract, the graph
    and every rendered artefact take the same one."""

    return f"dim_{name}"


def fact_suffix(name: str) -> str:
    """The relation and model suffix one fact takes under its product's
    namespace."""

    return f"fact_{name}"


def _ordered(columns: Sequence[str]) -> tuple[str, ...]:
    """``columns`` with repeats dropped, first occurrence winning. Two facts
    naming the same dimension, or a grain column that is also a dimension
    key, must not project the same column twice."""

    seen: list[str] = []
    for column in columns:
        if column not in seen:
            seen.append(column)
    return tuple(seen)


class DimensionalShape(ShapeDefinition):
    """One relation per declared dimension and one per declared fact."""

    def _dimensions(self, shape_config: Mapping[str, Any]) -> list[Mapping[str, Any]]:
        return list(shape_config.get("dimensions") or [])

    def _facts(self, shape_config: Mapping[str, Any]) -> list[Mapping[str, Any]]:
        return list(shape_config.get("facts") or [])

    def relation_names(
        self, *, domain: str, name: str, shape_config: Mapping[str, Any] | None = None
    ) -> tuple[str, ...]:
        config = shape_config or {}
        product = f"{domain}.{name}"
        return tuple(
            f"{product}__{dimension_suffix(str(dimension['name']))}"
            for dimension in self._dimensions(config)
        ) + tuple(
            f"{product}__{fact_suffix(str(fact['name']))}" for fact in self._facts(config)
        )

    def _time_field(
        self, fields: Sequence[RelationField], column: str, *, product: str, occurrence: str
    ) -> RelationField:
        field = column_field(
            fields, column, product=product, shape=self.name, occurrence=occurrence
        )
        if field.type not in TIME_TYPES:
            raise ShapeConstraintError(
                product=product,
                shape=self.name,
                occurrence=occurrence,
                detail=(
                    f"column {column!r} is declared {field.type!r}; a version order and a time "
                    f"dimension are instants, one of {sorted(TIME_TYPES)}"
                ),
            )
        return field

    def _dimension_relation(
        self,
        dimension: Mapping[str, Any],
        *,
        product: str,
        occurrence: str,
        fields: Sequence[RelationField],
    ) -> ShapeRelation:
        name = str(dimension["name"])
        key = tuple(str(column) for column in dimension["key"])
        if len(key) != 1:
            raise ShapeConstraintError(
                product=product,
                shape=self.name,
                occurrence=occurrence,
                detail=(
                    f"dimension {name!r} is keyed on {len(key)} columns; a conformed dimension "
                    "is joined and analysed through one key column, so its key is one column"
                ),
            )
        change_column = str(dimension["change_column"])
        attributes = tuple(str(column) for column in dimension["attributes"])
        overlapping = [
            column for column in attributes if column in key or column == change_column
        ]
        if overlapping:
            raise ShapeConstraintError(
                product=product,
                shape=self.name,
                occurrence=occurrence,
                detail=(
                    f"dimension {name!r} carries {', '.join(overlapping)} as an attribute and as "
                    "its key or its change column; the key identifies the row, the change column "
                    "orders its versions, and an attribute describes it, so one column is only "
                    "ever one of the three and the relation never projects it twice"
                ),
            )
        change_field = self._time_field(
            fields, change_column, product=product, occurrence=f"{occurrence}.change_column"
        )
        key_fields = tuple(
            RelationField(name=field.name, type=field.type, required=True)
            for field in (
                column_field(
                    fields, column, product=product, shape=self.name, occurrence=occurrence
                )
                for column in key
            )
        )
        attribute_fields = tuple(
            column_field(fields, column, product=product, shape=self.name, occurrence=occurrence)
            for column in attributes
        )
        declared_type = dimension["type"]
        if declared_type == TYPE_1:
            return ShapeRelation(
                suffix=dimension_suffix(name),
                schema=RelationSchema(
                    name=f"{product}__{dimension_suffix(name)}",
                    fields=key_fields + attribute_fields,
                ),
                derivation=DERIVATION_DIMENSION_TYPE_1,
                source_columns=key + attributes + (change_column,),
                key=key,
                materialisation=MATERIALISATION_TABLE,
                change_column=change_column,
            )
        clashing = [
            column for column in key + attributes if column in (EFFECTIVE_FROM, EFFECTIVE_TO)
        ]
        if clashing:
            raise ShapeConstraintError(
                product=product,
                shape=self.name,
                occurrence=occurrence,
                detail=(
                    f"dimension {name!r} already carries {', '.join(clashing)}, which a type 2 "
                    "dimension publishes as its own effective range"
                ),
            )
        range_fields = (
            RelationField(name=EFFECTIVE_FROM, type=change_field.type, required=True),
            RelationField(name=EFFECTIVE_TO, type=change_field.type, required=False),
        )
        return ShapeRelation(
            suffix=dimension_suffix(name),
            schema=RelationSchema(
                name=f"{product}__{dimension_suffix(name)}",
                fields=key_fields + attribute_fields + range_fields,
            ),
            derivation=DERIVATION_DIMENSION_TYPE_2,
            source_columns=key + attributes + (change_column,),
            key=key,
            materialisation=MATERIALISATION_TABLE,
            change_column=change_column,
            range_columns=(EFFECTIVE_FROM, EFFECTIVE_TO),
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
        keys_by_dimension: dict[str, tuple[str, ...]] = {}
        for index, dimension in enumerate(self._dimensions(shape_config)):
            occurrence = f"target.shape_config.dimensions[{index}]"
            dimension_name = str(dimension["name"])
            if dimension_name in keys_by_dimension:
                raise ShapeConstraintError(
                    product=product,
                    shape=self.name,
                    occurrence=occurrence,
                    detail=(
                        f"dimension {dimension_name!r} is declared twice; a conformed dimension "
                        "is declared once and named by every fact that shares it"
                    ),
                )
            relation = self._dimension_relation(
                dimension, product=product, occurrence=occurrence, fields=fields
            )
            keys_by_dimension[dimension_name] = tuple(
                str(column) for column in dimension["key"]
            )
            relations.append(relation)

        measures: dict[str, str] = {}
        seen_facts: set[str] = set()
        for index, fact in enumerate(self._facts(shape_config)):
            occurrence = f"target.shape_config.facts[{index}]"
            fact_name = str(fact["name"])
            if fact_name in seen_facts:
                raise ShapeConstraintError(
                    product=product,
                    shape=self.name,
                    occurrence=occurrence,
                    detail=f"fact {fact_name!r} is declared twice; one fact, one relation",
                )
            seen_facts.add(fact_name)
            grain = tuple(str(column) for column in fact["grain"])
            named_dimensions = tuple(str(entry) for entry in fact["dimensions"])
            unknown = [
                entry for entry in named_dimensions if entry not in keys_by_dimension
            ]
            if unknown:
                raise ShapeConstraintError(
                    product=product,
                    shape=self.name,
                    occurrence=occurrence,
                    detail=(
                        f"fact {fact_name!r} is analysed by {', '.join(unknown)}, which this "
                        f"target declares no dimension for: {sorted(keys_by_dimension)}"
                    ),
                )
            self._time_field(
                fields,
                str(fact["time_dimension"]),
                product=product,
                occurrence=f"{occurrence}.time_dimension",
            )
            measure_columns: list[str] = []
            for measure_index, measure in enumerate(fact["measures"]):
                measure_name = str(measure["name"])
                if measure_name in measures:
                    raise ShapeConstraintError(
                        product=product,
                        shape=self.name,
                        occurrence=f"{occurrence}.measures[{measure_index}]",
                        detail=(
                            f"measure {measure_name!r} is already declared by fact "
                            f"{measures[measure_name]!r}; a measure is named once across the "
                            "semantic layer"
                        ),
                    )
                measures[measure_name] = fact_name
                measure_columns.append(measure_name)
            columns = _ordered(
                list(grain)
                + [key for entry in named_dimensions for key in keys_by_dimension[entry]]
                + [str(fact["time_dimension"])]
                + measure_columns
            )
            fact_fields = tuple(
                RelationField(
                    name=field.name,
                    type=field.type,
                    required=field.required or field.name in grain,
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
                    suffix=fact_suffix(fact_name),
                    schema=RelationSchema(
                        name=f"{product}__{fact_suffix(fact_name)}", fields=fact_fields
                    ),
                    derivation=DERIVATION_PROJECTION,
                    source_columns=columns,
                    key=grain,
                    materialisation=MATERIALISATION_TABLE,
                )
            )

        for index, metric in enumerate(shape_config.get("metrics") or []):
            measure = str(metric["measure"])
            if measure not in measures:
                raise ShapeConstraintError(
                    product=product,
                    shape=self.name,
                    occurrence=f"target.shape_config.metrics[{index}]",
                    detail=(
                        f"metric {metric['name']!r} is computed from measure {measure!r}, which "
                        f"no fact declares: {sorted(measures)}"
                    ),
                )
        return tuple(relations)


DIMENSIONAL_SHAPE = register_shape(
    DimensionalShape(name=SHAPE_NAME, shape_config_schema=DIMENSIONAL_SHAPE_CONFIG_SCHEMA)
)

__all__ = [
    "AGGREGATIONS",
    "DIMENSIONAL_SHAPE",
    "DIMENSIONAL_SHAPE_CONFIG_SCHEMA",
    "DimensionalShape",
    "EFFECTIVE_FROM",
    "EFFECTIVE_TO",
    "SHAPE_NAME",
    "TYPE_1",
    "TYPE_2",
    "dimension_suffix",
    "fact_suffix",
]
