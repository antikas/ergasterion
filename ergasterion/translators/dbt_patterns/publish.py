"""The Data Publish pattern, rendered for dbt.

Architecture section 4 gives Data Publish three emitted artefacts and one
semantic: atomic publication, a current pointer, an SLA record, and all or
nothing. The three map onto dbt like this:

  * publication is the product model's own materialisation. A declared
    ``atomic`` mode is dbt's table materialisation, which builds the new
    relation and swaps it in as one step; a declared ``incremental`` mode
    replaces only the rows the run produced, keyed by the declared
    ``unique_key``, and likewise lands whole or not at all. The declaration
    never names the mechanic: the two declared adapters accept different
    strategy tokens and neither accepts the other's, so the strategy is
    adapter data (``ergasterion.framework.adapters``), the generated model
    carries every declared adapter's own token, and the dispatch macro in
    ``macros/publish.sql`` picks the running one;
  * the current pointer is a view over the published relation, so a
    consumer reads one stable name whichever way the relation was
    materialised;
  * the SLA record carries the publication instant and the row count,
    both derived from the published relation itself.

The pointer and the record are translator-private relations under the
product's namespace (architecture section 10): they are not part of the
product's contract, and the translator registers them as auxiliary
lineage.
"""

from __future__ import annotations

from typing import Any, Mapping, Sequence

from ergasterion.translators.dbt_patterns.sql import RenderingError, identifier, jinja_literal

PUBLICATION_MODE_ATOMIC = "atomic"
PUBLICATION_MODE_INCREMENTAL = "incremental"

# The dispatch macro a generated incremental model resolves its strategy
# through at compile time.
INCREMENTAL_STRATEGY_MACRO = "dpf_incremental_strategy"

CURRENT_SUFFIX = "current"
SLA_SUFFIX = "sla"


def publication_mode(step: dict, *, product: str) -> str:
    """The declared publication mode. A ``data_publish`` occurrence that
    names none fails closed: how a product is published is a declared fact
    about it, never a default the translator picks."""

    mode = step.get("publication_mode")
    if mode not in (PUBLICATION_MODE_ATOMIC, PUBLICATION_MODE_INCREMENTAL):
        raise RenderingError(
            product=product,
            occurrence="data_publish",
            rule="undeclared_publication_mode",
            detail=(
                f"publication_mode {mode!r} is not declared; name "
                f"{PUBLICATION_MODE_ATOMIC!r} or {PUBLICATION_MODE_INCREMENTAL!r}"
            ),
        )
    return str(mode)


def _strategy_mapping(strategies: Mapping[str, str], *, product: str) -> str:
    if not strategies:
        raise RenderingError(
            product=product,
            occurrence="data_publish",
            rule="undeclared_incremental_strategy",
            detail=(
                "an incremental publication needs the strategy each declared adapter "
                "republishes a keyed relation with, and no declared adapter carries one"
            ),
        )
    body = ", ".join(
        f"{jinja_literal(adapter)}: {jinja_literal(strategies[adapter])}"
        for adapter in sorted(strategies)
    )
    return "{" + body + "}"


def model_config(step: dict, *, product: str, strategies: Mapping[str, str]) -> str:
    """The product model's dbt configuration: the materialisation the
    declared publication mode calls for. ``strategies`` maps each declared
    adapter to the incremental strategy its own conventions name; an
    incremental publication with no declared adapter to take one from fails
    closed rather than emitting a configuration that cannot compile."""

    mode = publication_mode(step, product=product)
    if mode == PUBLICATION_MODE_ATOMIC:
        return "{{ config(materialized='table') }}"
    return incremental_config(
        step.get("unique_key") or [],
        product=product,
        strategies=strategies,
        occurrence="data_publish",
    )


def incremental_config(
    unique_key: Sequence[Any],
    *,
    product: str,
    strategies: Mapping[str, str],
    occurrence: str,
) -> str:
    """The dbt configuration of a relation that is added to rather than
    rebuilt: the declared key its rows are replaced by, and the strategy
    each declared adapter replaces them with, resolved at compile time by
    the dispatch macro.

    Two callers, one configuration: a product publishing incrementally
    (above) and a relation a shape declares as an insert-only store
    (``ergasterion.framework.shapes.MATERIALISATION_INCREMENTAL``). Both
    land whole or not at all, and neither may carry an adapter's own
    strategy token in a declaration."""

    keys = [identifier(key, product=product, occurrence=occurrence) for key in unique_key]
    rendered_keys = ", ".join(jinja_literal(key) for key in keys)
    rendered_strategies = _strategy_mapping(strategies, product=product)
    lines = [
        "{{ config(",
        "    materialized='incremental',",
        f"    incremental_strategy={INCREMENTAL_STRATEGY_MACRO}({rendered_strategies}),",
        f"    unique_key=[{rendered_keys}]",
        ") }}",
    ]
    return "\n".join(lines)


def publication_facts(
    step: dict, *, product: str, strategies: Mapping[str, str]
) -> dict[str, Any]:
    """What the runtime manifest records about this product's publication:
    the declared mode and key, and, for an incremental publication, the
    strategy each declared adapter republishes with -- the same per-adapter
    fact the generated model carries, so a runtime reads one truth."""

    mode = publication_mode(step, product=product)
    if mode != PUBLICATION_MODE_INCREMENTAL:
        strategy: Any = "swap"
    else:
        _strategy_mapping(strategies, product=product)
        strategy = {adapter: strategies[adapter] for adapter in sorted(strategies)}
    return {
        "publication_mode": mode,
        "unique_key": list(step.get("unique_key") or []),
        "strategy": strategy,
    }
