"""The deterministic translation router.

Assigns every execution-owner-required occurrence in a resolved
``ExecutionPlan`` to exactly one translator, validates that ownership respects
the plan's dependency edges and wrapper enclosure, checks that every translator
was built against the plan it is routing (digest), checks that every owned or
observed occurrence accepts the handoff schema on its incoming edges, and
composes each owning translator's ``translate()`` result without reordering the
plan.

This module never imports ``ergasterion.translators``. It declares the narrow
structural shape it needs as ``RoutableTranslator``, a ``typing.Protocol``. The concrete
``Translator`` ABC in ``ergasterion/translators/base.py`` satisfies this shape
without either module importing the other.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol, Sequence, runtime_checkable

from ergasterion.framework.models import (
    Edge,
    ExecutionPlan,
    FrameworkError,
    TranslationResult,
    compute_plan_digest,
)


@runtime_checkable
class RoutableTranslator(Protocol):
    """The structural shape ``TranslationRouter`` needs from a translator. Any
    object exposing these members routes, whether or not it subclasses
    ``ergasterion.translators.base.Translator``."""

    @property
    def target_name(self) -> str: ...

    def owned_occurrences(self) -> frozenset[str]:
        """Occurrence IDs this translator claims as sole execution owner."""
        ...

    def observed_occurrences(self) -> frozenset[str]:
        """Occurrence IDs this translator observes or projects without claiming
        execution ownership."""
        ...

    def execution_order(self) -> tuple[str, ...]:
        """The exact order this translator processes its owned occurrences in.
        Must be a permutation of ``owned_occurrences()``."""
        ...

    def plan_digest(self) -> str | None:
        """The plan digest this translator was built/validated against, or
        ``None`` when it pins none (skips the staleness check)."""
        ...

    def accepts_handoff(self, edge: Edge) -> bool:
        """Whether this translator accepts the handoff schema on an incoming
        edge to an occurrence it owns or observes."""
        ...

    def translate(self, plan: ExecutionPlan) -> TranslationResult: ...


# --------------------------------------------------------------------------- errors


class RoutingError(FrameworkError):
    """Base for every router failure. Always loud, always carries a stable
    ``.code``."""

    code = "routing_error"


class MissingExecutionOwnerError(RoutingError):
    code = "missing_execution_owner"

    def __init__(self, occurrence_id: str) -> None:
        self.occurrence_id = occurrence_id
        super().__init__(f"occurrence {occurrence_id!r} requires an execution owner and has none")


class DuplicateExecutionOwnerError(RoutingError):
    code = "duplicate_execution_owner"

    def __init__(self, occurrence_id: str, owner_names: tuple[str, ...]) -> None:
        self.occurrence_id = occurrence_id
        self.owner_names = owner_names
        super().__init__(f"occurrence {occurrence_id!r} has {len(owner_names)} execution owners: {owner_names}")


class ReorderedOwnershipError(RoutingError):
    code = "reordered_ownership"

    def __init__(self, translator_name: str, detail: str) -> None:
        self.translator_name = translator_name
        super().__init__(f"translator {translator_name!r} reorders ownership: {detail}")


class DigestMismatchError(RoutingError):
    code = "digest_mismatch"

    def __init__(self, translator_name: str, expected: str, got: str) -> None:
        self.translator_name = translator_name
        self.expected = expected
        self.got = got
        super().__init__(
            f"translator {translator_name!r} was built against plan digest {got!r}, "
            f"the routed plan digests {expected!r}"
        )


class BadHandoffError(RoutingError):
    code = "bad_handoff"

    def __init__(self, translator_name: str, edge: Edge) -> None:
        self.translator_name = translator_name
        self.edge = edge
        super().__init__(
            f"translator {translator_name!r} rejects handoff schema "
            f"{edge.handoff_schema_id.value!r} on edge {edge.source!r} -> {edge.target!r}"
        )


class UndeclaredAttachmentError(RoutingError):
    code = "undeclared_attachment"

    def __init__(self, translator_name: str, detail: str) -> None:
        self.translator_name = translator_name
        super().__init__(f"translator {translator_name!r} declares an undeclared attachment: {detail}")


class DuplicateTargetNameError(RoutingError):
    code = "duplicate_target_name"

    def __init__(self, target_name: str, count: int) -> None:
        self.target_name = target_name
        self.count = count
        super().__init__(f"target_name {target_name!r} is declared by {count} translators; target names must be unique")


# --------------------------------------------------------------------------- result


@dataclass(frozen=True)
class RouteAssignment:
    occurrence_id: str
    translator_name: str


@dataclass(frozen=True)
class RoutingResult:
    plan: ExecutionPlan
    assignments: tuple[RouteAssignment, ...]
    translations: dict[str, TranslationResult]


# --------------------------------------------------------------------------- router


class TranslationRouter:
    """Routes a resolved ``ExecutionPlan`` to a fixed set of translators."""

    def __init__(self, plan: ExecutionPlan, translators: Sequence[RoutableTranslator]) -> None:
        self._plan = plan
        self._translators = tuple(translators)

    def route(self) -> RoutingResult:
        plan = self._plan
        known_ids = {o.occurrence_id for o in plan.occurrences}

        self._check_target_names_unique()
        self._check_attachments(known_ids)
        self._check_ownership_shape()
        owner_of = self._check_coverage()
        self._check_digests()
        self._check_handoffs()

        assignments = tuple(
            RouteAssignment(o.occurrence_id, owner_of[o.occurrence_id].target_name)
            for o in plan.occurrences
            if o.occurrence_id in owner_of
        )
        # A translator that owns and observes nothing is a legitimate,
        # unattached translator (for example one declared but not yet wired
        # to any occurrence). It is excluded from RoutingResult.translations:
        # translate() is never called on it, and the router raises no error
        # for it. tests/python/test_framework_core.py::
        # test_router_skips_unattached_translator_without_error asserts this
        # behaviour directly.
        translations: dict[str, TranslationResult] = {}
        for translator in self._translators:
            if translator.owned_occurrences() or translator.observed_occurrences():
                translations[translator.target_name] = translator.translate(plan)

        return RoutingResult(plan=plan, assignments=assignments, translations=translations)

    # ----------------------------------------------------------------- checks

    def _check_target_names_unique(self) -> None:
        counts: dict[str, int] = {}
        for translator in self._translators:
            counts[translator.target_name] = counts.get(translator.target_name, 0) + 1
        for target_name, count in counts.items():
            if count > 1:
                raise DuplicateTargetNameError(target_name, count)

    def _check_attachments(self, known_ids: set[str]) -> None:
        for translator in self._translators:
            owned = translator.owned_occurrences()
            observed = translator.observed_occurrences()
            unknown = (owned | observed) - known_ids
            if unknown:
                raise UndeclaredAttachmentError(
                    translator.target_name, f"references unknown occurrence(s) {sorted(unknown)}"
                )
            overlap = owned & observed
            if overlap:
                raise UndeclaredAttachmentError(
                    translator.target_name,
                    f"declares both ownership and observation of {sorted(overlap)}",
                )

    def _check_ownership_shape(self) -> None:
        plan = self._plan
        for translator in self._translators:
            owned = translator.owned_occurrences()
            order = translator.execution_order()
            if set(order) != owned or len(order) != len(owned):
                raise ReorderedOwnershipError(
                    translator.target_name,
                    f"execution_order {list(order)} is not a permutation of owned_occurrences {sorted(owned)}",
                )
            position = {occurrence_id: index for index, occurrence_id in enumerate(order)}
            for edge in plan.edges:
                if edge.source in owned and edge.target in owned:
                    if position[edge.source] >= position[edge.target]:
                        raise ReorderedOwnershipError(
                            translator.target_name,
                            f"{edge.source!r} must precede {edge.target!r} in execution_order",
                        )
            if plan.wrapper_id in owned:
                wrapper_position = position[plan.wrapper_id]
                for member in plan.wrapper_members:
                    if member in owned and position[member] <= wrapper_position:
                        raise ReorderedOwnershipError(
                            translator.target_name,
                            f"wrapper {plan.wrapper_id!r} must precede its member {member!r} in execution_order",
                        )

    def _check_coverage(self) -> dict[str, RoutableTranslator]:
        plan = self._plan
        owner_of: dict[str, RoutableTranslator] = {}
        owners_by_occurrence: dict[str, list[RoutableTranslator]] = {o.occurrence_id: [] for o in plan.occurrences}
        for translator in self._translators:
            for occurrence_id in translator.owned_occurrences():
                owners_by_occurrence[occurrence_id].append(translator)

        for occurrence in plan.occurrences:
            owners = owners_by_occurrence[occurrence.occurrence_id]
            if len(owners) > 1:
                raise DuplicateExecutionOwnerError(
                    occurrence.occurrence_id, tuple(t.target_name for t in owners)
                )
            if len(owners) == 1:
                owner_of[occurrence.occurrence_id] = owners[0]
                continue
            if occurrence.execution_owner_required:
                raise MissingExecutionOwnerError(occurrence.occurrence_id)
        return owner_of

    def _check_digests(self) -> None:
        expected = compute_plan_digest(self._plan)
        for translator in self._translators:
            got = translator.plan_digest()
            if got is not None and got != expected:
                raise DigestMismatchError(translator.target_name, expected, got)

    def _check_handoffs(self) -> None:
        for translator in self._translators:
            relevant = translator.owned_occurrences() | translator.observed_occurrences()
            for edge in self._plan.edges:
                if edge.target in relevant and not translator.accepts_handoff(edge):
                    raise BadHandoffError(translator.target_name, edge)
