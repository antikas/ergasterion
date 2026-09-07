"""The Checkpoint and Retries pattern, rendered for dbt.

Architecture section 4 makes Checkpoint and Retries a wrapper around the
composition whose semantic is that every step is idempotent. dbt has no
wrapper object to emit, so the pattern renders as two things:

  * the run boundary and the retry policy, written into a runtime manifest
    beside the product's models. The manifest is what a runtime reads to
    know what one run of this product covers and how a failed run is
    retried;
  * idempotent materialisation, which is the model configuration Data
    Publish already renders. A table swap rebuilds the whole relation from
    the same inputs, and an incremental publication replaces exactly the
    rows its declared key identifies, so a second run over unchanged
    inputs leaves the published relation as the first run left it.

Every fact in the manifest is derived from the declaration and from what
the translator actually rendered. There is no default retry policy and no
default granularity: the ``checkpointing`` block declares both, and a
product whose block is missing either fails closed rather than publishing
a manifest that states a policy nobody declared.
"""

from __future__ import annotations

from typing import Any, Sequence

from ergasterion.translators.dbt_patterns.sql import RenderingError

RUNTIME_MANIFEST_SCHEMA = "ergasterion.product-runtime/v1"

REQUIRED_CHECKPOINT_KEYS: tuple[str, ...] = ("granularity", "max_retries", "backoff")


def runtime_manifest(
    *,
    product: str,
    translator: str,
    checkpointing: dict,
    occurrences: Sequence[str],
    publication: dict[str, Any],
    materialisation: str,
    published_relations: Sequence[str],
    auxiliary_relations: Sequence[str],
    stored: Sequence[dict[str, Any]],
    aggregation: Sequence[dict[str, Any]] = (),
) -> dict[str, Any]:
    """The runtime manifest for one product: its run boundary, its retry
    policy, how its relation is materialised and published, the grain and
    late-arrival policy of every aggregation it owns, and every relation one
    run produces.

    ``aggregation`` is a list because a composition may aggregate more than
    once; a product that aggregates not at all carries an empty one, which
    is a fact about the composition rather than an absent field.

    ``stored`` carries one entry per published relation whose declaration
    states a name of its own: the relation, the ``physical_relation`` schema
    and name it is stored under, and the ``physical_name`` beside the
    logical ``name`` of every column that is stored under one. A product
    that renames nothing carries none, so a runtime reading the manifest
    reads a rename only where one was declared."""

    missing = [key for key in REQUIRED_CHECKPOINT_KEYS if checkpointing.get(key) is None]
    if missing:
        raise RenderingError(
            product=product,
            occurrence="checkpointing",
            rule="incomplete_checkpoint_policy",
            detail=f"the checkpointing block declares no {', '.join(missing)}",
        )
    if not occurrences:
        raise RenderingError(
            product=product,
            occurrence="checkpointing",
            rule="empty_run_boundary",
            detail=(
                "the run boundary encloses no occurrence, so the manifest would state a retry "
                "policy over nothing"
            ),
        )
    return {
        "schema": RUNTIME_MANIFEST_SCHEMA,
        "product": product,
        "translator": translator,
        "run_boundary": {
            "granularity": checkpointing["granularity"],
            "checkpoint": bool(checkpointing.get("checkpoint", False)),
            "occurrences": list(occurrences),
        },
        "retry_policy": {
            "max_retries": checkpointing["max_retries"],
            "backoff": checkpointing["backoff"],
        },
        "materialisation": {"intent": materialisation, **publication},
        "aggregation": [dict(entry) for entry in aggregation],
        "relations": {
            "published": list(published_relations),
            "auxiliary": list(auxiliary_relations),
        },
        **({"stored": [dict(entry) for entry in stored]} if stored else {}),
    }
