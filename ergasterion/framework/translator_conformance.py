"""The translator conformance seam.

A data-driven harness that proves ``TranslationRouter`` fails closed on the
five ways a translator can misbehave: missing ownership, duplicate ownership,
reordered ownership, a stale plan digest, and a rejected handoff schema. It
also carries a positive vector proving a well-formed translator set routes
cleanly. Vectors live in ``tests/fixtures/translator_conformance.json`` as
plain data (no code): each names a set of translator declarations
(ownership/observation/order/digest/rejected handoffs) and the router error
code, if any, routing that set against a plan must raise.

``FakeTranslator`` is a minimal, dependency-free implementation of the
``RoutableTranslator`` protocol built purely from vector data. This module
never imports ``ergasterion.translators``. Real local-ingestion and dbt
translators run through the same ``check_translator_conformance`` entry point
this module exposes, passing their own ``Translator`` instances, which satisfy
``RoutableTranslator`` structurally with no import required in either
direction.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path

from ergasterion.framework.models import Edge, ExecutionPlan, TranslationResult
from ergasterion.framework.routing import (
    RoutableTranslator,
    RoutingError,
    RoutingResult,
    TranslationRouter,
)


@dataclass(frozen=True)
class FakeTranslator:
    """A ``RoutableTranslator`` built entirely from conformance-vector data.
    Used by the fixture-driven vectors below; also useful directly in a unit
    test that needs a translator double without a real target backend."""

    _target_name: str
    _owned: frozenset[str]
    _observed: frozenset[str] = field(default_factory=frozenset)
    _order: tuple[str, ...] = ()
    _digest: str | None = None
    _rejected_handoffs: frozenset[tuple[str, str]] = field(default_factory=frozenset)

    @property
    def target_name(self) -> str:
        return self._target_name

    def owned_occurrences(self) -> frozenset[str]:
        return self._owned

    def observed_occurrences(self) -> frozenset[str]:
        return self._observed

    def execution_order(self) -> tuple[str, ...]:
        return self._order

    def plan_digest(self) -> str | None:
        return self._digest

    def accepts_handoff(self, edge: Edge) -> bool:
        return (edge.source, edge.target) not in self._rejected_handoffs

    def translate(self, plan: ExecutionPlan) -> TranslationResult:
        return TranslationResult(
            artefacts={},
            metadata={"target_name": self._target_name},
            warnings=(),
        )


def _translator_from_json(spec: dict) -> FakeTranslator:
    return FakeTranslator(
        _target_name=spec["target_name"],
        _owned=frozenset(spec["owned_occurrences"]),
        _observed=frozenset(spec.get("observed_occurrences", [])),
        _order=tuple(spec["execution_order"]),
        _digest=spec.get("plan_digest"),
        _rejected_handoffs=frozenset(tuple(pair) for pair in spec.get("rejected_handoffs", [])),
    )


@dataclass(frozen=True)
class ConformanceVector:
    vector_id: str
    description: str
    expected_error_code: str | None
    translators: tuple[FakeTranslator, ...]


def load_vectors(path: Path) -> tuple[ConformanceVector, ...]:
    with open(path, encoding="utf-8") as fh:
        document = json.load(fh)
    vectors = []
    for raw in document["vectors"]:
        translators = tuple(_translator_from_json(t) for t in raw["translators"])
        vectors.append(
            ConformanceVector(
                vector_id=raw["id"],
                description=raw["description"],
                expected_error_code=raw.get("expected_error_code"),
                translators=translators,
            )
        )
    return tuple(vectors)


@dataclass(frozen=True)
class VectorOutcome:
    vector_id: str
    passed: bool
    detail: str


def check_translator_conformance(plan: ExecutionPlan, translators) -> RoutingResult:
    """The stable public seam entry point: route ``translators`` (any sequence
    of ``RoutableTranslator``-shaped objects, real or fake) against ``plan``.
    Raises the matching ``RoutingError`` subclass on the first violation found;
    returns the composed ``RoutingResult`` when every check passes."""

    return TranslationRouter(plan, translators).route()


def run_vector(plan: ExecutionPlan, vector: ConformanceVector) -> VectorOutcome:
    got_code: str | None
    try:
        check_translator_conformance(plan, vector.translators)
        got_code = None
    except RoutingError as exc:
        got_code = exc.code

    if got_code == vector.expected_error_code:
        return VectorOutcome(vector.vector_id, True, f"router outcome matched: {got_code!r}")
    return VectorOutcome(
        vector.vector_id,
        False,
        f"expected error code {vector.expected_error_code!r}, router produced {got_code!r}",
    )


def run_all(plan: ExecutionPlan, vectors: tuple[ConformanceVector, ...]) -> tuple[VectorOutcome, ...]:
    return tuple(run_vector(plan, vector) for vector in vectors)
