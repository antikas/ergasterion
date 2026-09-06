"""The semantic layer of the ``dimensional`` shape, rendered for dbt
(architecture section 6, owner ruling R6).

The shape's target block declares the measures each fact carries and the
metrics over them. dbt's semantic layer reads that as MetricFlow YAML: one
semantic model per relation, and one metric per declared metric.

  * a fact's semantic model carries its measures, the time dimension they
    are aggregated over, and one foreign entity per dimension it is
    analysed by, bound to that dimension's key column;
  * a dimension's semantic model carries its attributes as categorical
    dimensions and its key as the entity the facts join to. A type 2
    dimension's key is a natural key, and its effective range is declared
    as the dimension's validity window, so a join through it is
    point-in-time rather than to whichever version happens to be current.

Beside them stands the time spine: dbt's semantic layer aggregates a
measure over a dense relation of dates, and refuses to parse a project
that declares metrics without one. A dbt project carries one spine per
granularity, so the spine is one shared relation of the estate rather
than one per product: it is rendered once, over the window that covers
every declared window in the estate, through the dispatch macro that
carries the two adapters' different date-series mechanics, and described
once in its own document. Every product's semantic layer aggregates over
it.

A product's own document is a publication artefact beside the models:
nothing in the generated SQL reads it, and it names only relations that
product publishes. The shared spine is described in the estate document
beside the spine relation, where its own configuration lives.
"""

from __future__ import annotations

from typing import Any, Mapping, Sequence

from ergasterion.framework.graph import ESTATE_SCOPE
from ergasterion.framework.shapes import ShapeRelation
from ergasterion.shapes.dimensional import (
    EFFECTIVE_FROM,
    EFFECTIVE_TO,
    TYPE_2,
    dimension_suffix,
    fact_suffix,
)
from ergasterion.translators.dbt_patterns.sql import (
    RenderingError,
    jinja_literal,
    sql_string_literal,
)
from ergasterion.translators.dbt_patterns.steps import Cte

ENTITY_PRIMARY = "primary"
ENTITY_FOREIGN = "foreign"
ENTITY_NATURAL = "natural"

# The shared time spine relation: the namespace it sits in, its own name,
# the one column it carries, and the dispatch macro that renders a dense
# date series on whichever adapter compiles it.
#
# The namespace is the estate's rather than a product's, because a dbt
# project carries one spine per granularity and this one relation serves
# every semantic layer the estate renders. It is not a product: nothing
# publishes it and no contract lists it. The word for that owner is the
# graph's (``ESTATE_SCOPE``), read from there rather than repeated, so the
# namespace a relation sits in and the owner it registers under can never
# drift apart. An estate that declares a product domain of this name and
# publishes a relation under it collides, and the graph refuses the
# registration rather than letting two writers claim one relation.
TIME_SPINE_NAMESPACE = ESTATE_SCOPE
TIME_SPINE_SUFFIX = "time_spine"
TIME_SPINE_RELATION = f"{TIME_SPINE_NAMESPACE}.{TIME_SPINE_SUFFIX}"
TIME_SPINE_MODEL = f"{TIME_SPINE_NAMESPACE}__{TIME_SPINE_SUFFIX}"
TIME_SPINE_COLUMN = "date_day"
DATE_SERIES_MACRO = "dpf_date_series"
DATE_TYPE_CALL = "{{ dpf_type('date') }}"

# A semantic model is named for the relation it exposes, but dbt refuses a
# semantic name carrying a double underscore, which every generated model
# name carries between domain, product and relation. The semantic name is
# therefore the same three parts joined singly: one derivation, so a
# semantic model and the model it reads can never drift apart.
NAME_SEPARATOR = "_"


def semantic_name(model: str) -> str:
    """The semantic model name for one generated dbt model."""

    return NAME_SEPARATOR.join(part for part in model.split("__") if part)


# Every declared instant is exposed at day granularity: the engine
# declares a date or a timestamp, and MetricFlow needs the smallest
# granularity a query may group by.
TIME_GRANULARITY = "day"


def _model_reference(model: str) -> str:
    return f"ref('{model}')"


def _dimension_model(
    dimension: Mapping[str, Any], *, relation: ShapeRelation, model: str
) -> dict[str, Any]:
    name = str(dimension["name"])
    key = str(dimension["key"][0])
    is_type_2 = dimension["type"] == TYPE_2
    dimensions: list[dict[str, Any]] = [
        {"name": str(attribute), "type": "categorical"}
        for attribute in dimension["attributes"]
    ]
    document: dict[str, Any] = {
        "name": semantic_name(model),
        "description": f"The {name} dimension published as {relation.schema.name}.",
        "model": _model_reference(model),
        "entities": [
            {
                "name": name,
                "type": ENTITY_NATURAL if is_type_2 else ENTITY_PRIMARY,
                "expr": key,
            }
        ],
    }
    if is_type_2:
        # A type 2 dimension has one row per version of the key, so the key
        # is a natural entity rather than a primary one. dbt still needs a
        # primary entity to hang the dimensions from, and the row's own
        # identity is the version, so that is what it is named for.
        document["primary_entity"] = f"{name}_version"
        document["defaults"] = {"agg_time_dimension": EFFECTIVE_FROM}
        dimensions = [
            {
                "name": EFFECTIVE_FROM,
                "type": "time",
                "type_params": {
                    "time_granularity": TIME_GRANULARITY,
                    "validity_params": {"is_start": True},
                },
            },
            {
                "name": EFFECTIVE_TO,
                "type": "time",
                "type_params": {
                    "time_granularity": TIME_GRANULARITY,
                    "validity_params": {"is_end": True},
                },
            },
            *dimensions,
        ]
    document["dimensions"] = dimensions
    return document


def _fact_model(
    fact: Mapping[str, Any],
    *,
    relation: ShapeRelation,
    model: str,
    keys_by_dimension: Mapping[str, str],
) -> dict[str, Any]:
    name = str(fact["name"])
    time_dimension = str(fact["time_dimension"])
    return {
        "name": semantic_name(model),
        "description": f"The {name} fact published as {relation.schema.name}.",
        "model": _model_reference(model),
        "primary_entity": name,
        "defaults": {"agg_time_dimension": time_dimension},
        "entities": [
            {
                "name": str(entry),
                "type": ENTITY_FOREIGN,
                "expr": keys_by_dimension[str(entry)],
            }
            for entry in fact["dimensions"]
        ],
        "dimensions": [
            {
                "name": time_dimension,
                "type": "time",
                "type_params": {"time_granularity": TIME_GRANULARITY},
            }
        ],
        "measures": [
            {
                "name": str(measure["name"]),
                "agg": str(measure["aggregation"]),
                "description": str(
                    measure.get(
                        "description", f"{measure['aggregation']} of {measure['name']}."
                    )
                ),
            }
            for measure in fact["measures"]
        ],
    }


def declared_window(shape_config: Mapping[str, Any], *, product: str) -> tuple[str, str]:
    """The window one product declares its semantic layer's metrics are
    aggregated over, as two ISO dates. Fails closed naming the product when
    the shape declares no window."""

    window = shape_config.get("time_spine") or {}
    bounds = (window.get("from"), window.get("to"))
    if not all(isinstance(bound, str) and bound for bound in bounds):
        raise RenderingError(
            product=product,
            occurrence="target.shape_config.time_spine",
            rule="undeclared_time_spine_window",
            detail="a semantic layer's time spine covers a declared window, from one date to another",
        )
    return str(bounds[0]), str(bounds[1])


def covering_window(windows: Sequence[tuple[str, str]]) -> tuple[str, str]:
    """The one window the shared spine is rendered over: the earliest start
    and the latest end of every window the estate's products declared. The
    bounds are ISO dates, which sort as text exactly as they sort as dates,
    so the widest pair is a plain minimum and maximum.

    One spine covering every declared window is what lets an estate render
    more than one semantic layer: each product still declares the window its
    own metrics are aggregated over, and none of them needs a spine of its
    own."""

    if not windows:
        raise ValueError("the shared time spine is rendered only for a declared window")
    return min(start for start, _end in windows), max(end for _start, end in windows)


def time_spine_model(window: tuple[str, str]) -> tuple[tuple[Cte, ...], list[str], str]:
    """The shared time spine relation as the common table expressions,
    columns and final expression its model is built from: a dense series of
    dates over the covering window, cast to the neutral date type.

    The window's two bounds reach the dispatch macro as SQL expressions,
    which is how the macro takes them. Their cast names the date type
    directly rather than through the type dispatch: a dispatch call cannot
    be nested inside another call's argument, and this is the one type both
    declared adapters spell identically."""

    arguments = ", ".join(
        jinja_literal(f"cast({sql_string_literal(str(bound))} as date)") for bound in window
    )
    series = Cte(
        name=TIME_SPINE_SUFFIX,
        body="    {{ " + f"{DATE_SERIES_MACRO}({arguments})" + " }}",
    )
    columns = [f"cast({TIME_SPINE_COLUMN} as {DATE_TYPE_CALL}) as {TIME_SPINE_COLUMN}"]
    return (series,), columns, series.name


def time_spine_document(products: Sequence[str]) -> dict[str, Any]:
    """The document that describes the shared spine to dbt, written once
    beside the spine relation. ``products`` are the products whose semantic
    layers aggregate over it, named so a reader of the generated tree can
    see what the relation is there for."""

    return {
        "version": 2,
        "models": [
            {
                "name": TIME_SPINE_MODEL,
                "description": (
                    "The time spine every semantic layer of this estate aggregates its "
                    f"metrics over: {', '.join(products)}."
                ),
                "time_spine": {"standard_granularity_column": TIME_SPINE_COLUMN},
                "columns": [{"name": TIME_SPINE_COLUMN, "granularity": TIME_GRANULARITY}],
            }
        ],
    }


def build_document(
    *,
    product: str,
    shape_config: Mapping[str, Any],
    relations: Mapping[str, ShapeRelation],
    models: Mapping[str, str],
) -> dict[str, Any]:
    """The MetricFlow document for one dimensional product.
    ``relations`` and ``models`` are keyed by relation suffix, so a
    semantic model can only ever name a relation this product publishes.

    The document names no spine: the estate renders one shared spine and
    describes it once beside the spine relation (``time_spine_document``),
    so a second dimensional product adds a semantic layer and nothing
    else."""

    semantic_models: list[dict[str, Any]] = []
    keys_by_dimension: dict[str, str] = {}
    for dimension in shape_config.get("dimensions") or []:
        name = str(dimension["name"])
        suffix = dimension_suffix(name)
        keys_by_dimension[name] = str(dimension["key"][0])
        semantic_models.append(
            _dimension_model(dimension, relation=relations[suffix], model=models[suffix])
        )
    for fact in shape_config.get("facts") or []:
        suffix = fact_suffix(str(fact["name"]))
        semantic_models.append(
            _fact_model(
                fact,
                relation=relations[suffix],
                model=models[suffix],
                keys_by_dimension=keys_by_dimension,
            )
        )

    metrics: list[dict[str, Any]] = []
    for metric in shape_config.get("metrics") or []:
        name = str(metric["name"])
        metrics.append(
            {
                "name": name,
                "label": str(metric.get("label", name)),
                "description": str(
                    metric.get("description", f"{metric['type']} metric over {metric['measure']}.")
                ),
                "type": str(metric["type"]),
                "type_params": {"measure": str(metric["measure"])},
            }
        )

    if not semantic_models:
        raise RenderingError(
            product=product,
            occurrence="target.shape_config",
            rule="empty_semantic_layer",
            detail="a dimensional product declares at least one relation to expose",
        )
    document: dict[str, Any] = {
        "version": 2,
        "semantic_models": semantic_models,
    }
    if metrics:
        document["metrics"] = metrics
    return document


__all__ = [
    "DATE_SERIES_MACRO",
    "semantic_name",
    "TIME_GRANULARITY",
    "TIME_SPINE_COLUMN",
    "TIME_SPINE_MODEL",
    "TIME_SPINE_NAMESPACE",
    "TIME_SPINE_RELATION",
    "TIME_SPINE_SUFFIX",
    "build_document",
    "covering_window",
    "declared_window",
    "time_spine_document",
    "time_spine_model",
]
