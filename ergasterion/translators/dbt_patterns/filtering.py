"""The Data Filtering pattern, rendered for dbt.

Architecture section 4 requires every exclusion of a Data Filtering
occurrence to be auditable by predicate and count. A count needs something
to count against, so the occurrence cuts the product's chain
(``segment``): everything rendered so far becomes the pre-filter relation,
the filter log counts each declared predicate's exclusions against it, and
the product's chain resumes from it with the predicates applied.

The count itself is the one piece of SQL every predicate row shares, so it
stays in ``macros/filter_log.sql`` rather than being re-spelled per
predicate in the emitted model.
"""

from __future__ import annotations

from typing import Sequence

from ergasterion.framework.models import RelationField
from ergasterion.translators.dbt_patterns.segment import Companion, Segment
from ergasterion.translators.dbt_patterns.sql import (
    suffixed_model_name,
    identifier,
    ref,
    select_projection,
    sql_string_literal,
)
from ergasterion.translators.dbt_patterns.steps import Cte, cte_name

PATTERN = "data_filtering"

PREFILTER_SUFFIX = "prefilter"
FILTER_LOG_SUFFIX = "filter_log"

EVALUATED_CTE = "evaluated"
COUNTED_CTE = "counted"

VIEW_CONFIG = "{{ config(materialized='view') }}"
TABLE_CONFIG = "{{ config(materialized='table') }}"

PREDICATE_NAME_COLUMN = "predicate_name"
EXCLUDED_COUNT_COLUMN = "excluded_count"


def _excluded_count_call(column: str) -> str:
    return "{{ dpf_excluded_count('" + column + "') }}"


def _predicate_column(position: int) -> str:
    return f"predicate_{position:02d}"


def render_segment(
    *,
    index: int,
    step: dict,
    product: str,
    domain: str,
    name: str,
    fields: Sequence[RelationField],
    previous: str,
) -> Segment:
    """The pre-filter relation, the filter log and the filtered chain the
    product resumes with, for one filtering occurrence."""

    occurrence = f"steps[{index}]:{PATTERN}"
    field_columns = tuple(
        identifier(entry.name, product=product, occurrence=occurrence) for entry in fields
    )
    prefilter_model = suffixed_model_name(domain, name, PREFILTER_SUFFIX)
    predicates = step.get("predicates") or []

    evaluated = Cte(
        name=EVALUATED_CTE,
        body=select_projection(
            tuple(
                f"({predicate['expression']}) as {_predicate_column(position)}"
                for position, predicate in enumerate(predicates)
            ),
            ref(prefilter_model),
        ),
    )
    counted = Cte(
        name=COUNTED_CTE,
        body="\n\n    union all\n\n".join(
            select_projection(
                (
                    f"{sql_string_literal(predicate['name'])} as {PREDICATE_NAME_COLUMN}",
                    f"{_excluded_count_call(_predicate_column(position))} as "
                    f"{EXCLUDED_COUNT_COLUMN}",
                ),
                EVALUATED_CTE,
            )
            for position, predicate in enumerate(predicates)
        ),
    )
    where = "\n      and ".join(f"({predicate['expression']})" for predicate in predicates)

    return Segment(
        suffix=PREFILTER_SUFFIX,
        model_config=VIEW_CONFIG,
        ctes=(),
        columns=field_columns,
        final_cte=previous,
        purpose="the relation a data_filtering occurrence audits its exclusions against",
        description="The relation this product filters, kept so every exclusion stays countable.",
        resume_columns=field_columns,
        resume_ctes=(
            Cte(
                name=cte_name(index, PATTERN),
                body=select_projection(
                    field_columns, f"{PREFILTER_SUFFIX}_input", where=where
                ),
            ),
        ),
        companions=(
            Companion(
                suffix=FILTER_LOG_SUFFIX,
                model_config=TABLE_CONFIG,
                ctes=(evaluated, counted),
                columns=(PREDICATE_NAME_COLUMN, EXCLUDED_COUNT_COLUMN),
                final_cte=COUNTED_CTE,
                purpose="one row per declared predicate with the rows it excluded",
                description="One row per declared predicate, with the number of rows it excluded.",
            ),
        ),
    )


__all__ = ["FILTER_LOG_SUFFIX", "PREFILTER_SUFFIX", "render_segment"]
