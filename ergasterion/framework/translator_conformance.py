"""The translator conformance seam.

A data-driven harness that proves ``TranslationRouter`` fails closed on the
five ways a translator can misbehave under occurrence-ownership routing:
missing ownership, duplicate ownership, reordered ownership, a stale plan
digest, and a rejected handoff schema. It also carries a positive vector
proving a well-formed translator set routes cleanly, plus a family of
table-driven vectors proving the two-axis capability router (architecture
sections 9, 11; owner ruling R1): a clean two-translator, two-adapter route, a
label with no translator-table entry for a pattern, and a named translator
missing the capability for a declared adapter. Vectors live in
``tests/fixtures/translator_conformance.json`` as plain data (no code): each
names a set of translator declarations (ownership/observation/order/digest/
rejected handoffs, and, for the table-driven vectors, capabilities) plus,
where table-driven, the ``label``, ``translator_table`` and ``adapters`` to
route by, and the router error code, if any, routing that set must raise.

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
from typing import Sequence

from ergasterion.framework.models import Capability, Edge, ExecutionPlan, TranslationResult
from ergasterion.framework.routing import (
    RoutableTranslator,
    RoutingError,
    RoutingResult,
    TranslationRouter,
    TranslatorTable,
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
    _capabilities: frozenset[Capability] = field(default_factory=frozenset)

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

    def capabilities(self) -> frozenset[Capability]:
        return self._capabilities

    def translate(self, plan: ExecutionPlan) -> TranslationResult:
        return TranslationResult(
            artefacts={},
            metadata={"target_name": self._target_name},
            warnings=(),
        )


def _translator_from_json(spec: dict) -> FakeTranslator:
    return FakeTranslator(
        _target_name=spec["target_name"],
        _owned=frozenset(spec.get("owned_occurrences", [])),
        _observed=frozenset(spec.get("observed_occurrences", [])),
        _order=tuple(spec.get("execution_order", [])),
        _digest=spec.get("plan_digest"),
        _rejected_handoffs=frozenset(tuple(pair) for pair in spec.get("rejected_handoffs", [])),
        _capabilities=frozenset(
            Capability(*entry) for entry in spec.get("capabilities", [])
        ),
    )


@dataclass(frozen=True)
class ConformanceVector:
    vector_id: str
    description: str
    expected_error_code: str | None
    translators: tuple[FakeTranslator, ...]
    label: str | None = None
    translator_table: TranslatorTable | None = None
    adapters: tuple[str, ...] | None = None


def load_vectors(path: Path) -> tuple[ConformanceVector, ...]:
    with open(path, encoding="utf-8") as fh:
        document = json.load(fh)
    vectors = []
    for raw in document["vectors"]:
        translators = tuple(_translator_from_json(t) for t in raw["translators"])
        adapters = raw.get("adapters")
        vectors.append(
            ConformanceVector(
                vector_id=raw["id"],
                description=raw["description"],
                expected_error_code=raw.get("expected_error_code"),
                translators=translators,
                label=raw.get("label"),
                translator_table=raw.get("translator_table"),
                adapters=tuple(adapters) if adapters is not None else None,
            )
        )
    return tuple(vectors)


@dataclass(frozen=True)
class VectorOutcome:
    vector_id: str
    passed: bool
    detail: str


def check_translator_conformance(
    plan: ExecutionPlan,
    translators,
    *,
    label: str | None = None,
    translator_table: TranslatorTable | None = None,
    adapters: Sequence[str] | None = None,
) -> RoutingResult:
    """The stable public seam entry point: route ``translators`` (any sequence
    of ``RoutableTranslator``-shaped objects, real or fake) against ``plan``.
    Raises the matching ``RoutingError`` subclass on the first violation found;
    returns the composed ``RoutingResult`` when every check passes.

    ``label``, ``translator_table`` and ``adapters``, given together, route by
    the estate's translator table (``TranslationRouter.route()``'s table-
    driven mode) instead of occurrence ownership; a vector that passes none
    of them keeps its original occurrence-ownership outcome."""

    return TranslationRouter(plan, translators).route(
        label=label, translator_table=translator_table, adapters=adapters
    )


def run_vector(plan: ExecutionPlan, vector: ConformanceVector) -> VectorOutcome:
    got_code: str | None
    try:
        check_translator_conformance(
            plan,
            vector.translators,
            label=vector.label,
            translator_table=vector.translator_table,
            adapters=vector.adapters,
        )
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
