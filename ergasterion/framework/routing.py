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

A second, additive resolution mode lives here beside the occurrence-ownership
checks above: table-driven routing (architecture sections 3.4, 9, 11; owner
ruling R1, plan decision D29). ``TranslationRouter.route()`` takes three new,
all-or-nothing keyword arguments -- ``label``, ``translator_table`` and
``adapters`` -- and, when given, resolves every occurrence by looking up the
estate's translator table with the product's layer label and the occurrence's
pattern, then requiring the named translator to carry the two-axis capability
(pattern-or-shape, translator, adapter) for every declared adapter. Called
with none of the three, the router keeps its original occurrence-ownership
behaviour exactly as before: the table-driven mode is a pure addition, not a
replacement.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping, Protocol, Sequence, runtime_checkable

from ergasterion.framework.models import (
    Capability,
    Edge,
    ExecutionPlan,
    FrameworkError,
    TranslationResult,
    compute_plan_digest,
)

# label -> pattern-or-shape token -> translator target_name. Estate data
# (``estate.yml``'s ``translators:`` block), never engine logic: the router
# looks a label up as a key into this table and never branches on what the
# label means (architecture section 13).
TranslatorTable = Mapping[str, Mapping[str, str]]


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

    def capabilities(self) -> frozenset[Capability]:
        """The two-axis capability set this translator registers: every
        (pattern-or-shape, translator, adapter) triple it can render. Table-
        driven routing reads this; occurrence-ownership routing does not."""
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


# --------------------------------------------------------------------- table-driven routing errors


class TableRoutingArgumentError(RoutingError):
    """Raised when ``route()`` is called with some but not all of ``label``,
    ``translator_table`` and ``adapters``. The three are all-or-nothing: a
    caller either routes by table (all three given) or by occurrence
    ownership (none given); a partial combination is a caller programming
    error, not a routable state."""

    code = "table_routing_argument_error"

    def __init__(self) -> None:
        super().__init__(
            "route() needs 'label', 'translator_table' and 'adapters' together, or none of them"
        )


class MissingTranslatorTableEntryError(RoutingError):
    """Raised when the estate's translator table carries no entry for
    ``pattern_or_shape`` under ``label``: the label admits this pattern in
    its profile, but no owner is declared for it (architecture check 12).
    Fails closed naming the label, the pattern (or shape) and the estate's
    full declared adapter set, since no owner was even found to check
    per-adapter capability against."""

    code = "missing_translator_table_entry"

    def __init__(self, label: str, pattern_or_shape: str, adapters: tuple[str, ...]) -> None:
        self.label = label
        self.pattern_or_shape = pattern_or_shape
        self.adapters = adapters
        super().__init__(
            f"label {label!r} has no translator-table entry for pattern/shape {pattern_or_shape!r} "
            f"(declared adapters: {', '.join(adapters)})"
        )


class UnregisteredTableTranslatorError(RoutingError):
    """Raised when the translator table names a translator, for a label and
    pattern/shape, that was never passed to this router. A table entry is
    only ever an owner claim over a translator the caller actually
    instantiated and supplied."""

    code = "unregistered_table_translator"

    def __init__(self, label: str, pattern_or_shape: str, translator_name: str) -> None:
        self.label = label
        self.pattern_or_shape = pattern_or_shape
        self.translator_name = translator_name
        super().__init__(
            f"label {label!r} names translator {translator_name!r} for pattern/shape "
            f"{pattern_or_shape!r}, but no such translator was supplied to the router"
        )


class MissingCapabilityError(RoutingError):
    """Raised when the translator table names a real, supplied translator for
    a label and pattern/shape, but that translator never registered the
    (pattern-or-shape, translator, adapter) capability for one of the
    estate's declared adapters. Fails closed naming the label, the pattern
    (or shape), the translator and the specific adapter missing the
    capability (architecture check 12)."""

    code = "missing_capability"

    def __init__(self, label: str, pattern_or_shape: str, translator_name: str, adapter: str) -> None:
        self.label = label
        self.pattern_or_shape = pattern_or_shape
        self.translator_name = translator_name
        self.adapter = adapter
        super().__init__(
            f"translator {translator_name!r} has no registered capability for pattern/shape "
            f"{pattern_or_shape!r} under label {label!r} adapter {adapter!r}"
        )


class ForeignCapabilityError(RoutingError):
    """Raised when a translator's own ``capabilities()`` carries an entry
    whose ``translator`` field names a different translator. A translator
    may only ever claim its own ``target_name`` as a capability's owner;
    claiming another translator's identity is a coherence bug the router
    rejects rather than silently trusting."""

    code = "foreign_capability"

    def __init__(self, translator_name: str, claimed_translator: str) -> None:
        self.translator_name = translator_name
        self.claimed_translator = claimed_translator
        super().__init__(
            f"translator {translator_name!r} declares a capability naming translator "
            f"{claimed_translator!r}; a translator may only declare its own capabilities"
        )


# --------------------------------------------------------------------------- result


@dataclass(frozen=True)
class RouteAssignment:
    """One occurrence's resolved owner. ``adapter`` is populated only by
    table-driven routing (one ``RouteAssignment`` row per occurrence per
    declared adapter, since the two-axis capability model records translator
    AND adapter together); occurrence-ownership routing leaves it ``None``,
    since that mode never resolves against a specific adapter."""

    occurrence_id: str
    translator_name: str
    adapter: str | None = None


@dataclass(frozen=True)
class RoutingResult:
    """``assignments`` ordering: occurrence-ownership routing orders strictly
    by ``occurrence_id`` (the plan's own canonical order), one row per
    occurrence. Table-driven routing orders by ``occurrence_id`` first, then,
    within one occurrence, by the caller's ``adapters`` sequence in the exact
    order given -- never re-sorted -- so a caller that cares about a
    particular adapter's row position controls it through the order it
    passes ``adapters``."""

    plan: ExecutionPlan
    assignments: tuple[RouteAssignment, ...]
    translations: dict[str, TranslationResult]


# --------------------------------------------------------------------- table-driven resolution


def resolve_table_owners(
    *,
    occurrences: Sequence[tuple[str, str]],
    label: str,
    translator_table: TranslatorTable,
    adapters: Sequence[str],
    translators: Sequence[RoutableTranslator],
) -> tuple[RouteAssignment, ...]:
    """Resolve every occurrence to exactly one owning translator through
    the estate's translator table (architecture sections 9, 11; owner
    ruling R1; check 12), and return one ``RouteAssignment`` per
    occurrence per adapter, in the order ``occurrences`` and ``adapters``
    were given.

    ``occurrences`` is a sequence of ``(occurrence_id, pattern_or_shape)``
    pairs. The token is a pattern's registry value or a registered shape's
    name: the table names an owner for both, and this function treats them
    identically, exactly as the table does.

    Fails closed with ``MissingTranslatorTableEntryError`` when the label
    declares no owner for a token, ``UnregisteredTableTranslatorError``
    when it names a translator the caller did not supply,
    ``ForeignCapabilityError`` when a supplied translator claims a
    capability under another translator's name, and
    ``MissingCapabilityError`` naming label, token, translator and the
    exact adapter when the named owner never registered the capability.

    Resolution never calls ``translate()``: it answers only who owns what.
    ``TranslationRouter._route_by_table`` calls it for a resolved
    ``ExecutionPlan``'s occurrences and then renders; a caller routing a
    product declaration's own occurrences calls it directly, so there is
    one implementation of the lookup and one vocabulary of routing
    failures."""

    by_target_name = {t.target_name: t for t in translators}
    label_table = translator_table.get(label, {})
    adapters_tuple = tuple(adapters)
    checked_coherent: set[str] = set()
    assignments: list[RouteAssignment] = []

    for occurrence_id, token in occurrences:
        translator_name = label_table.get(token)
        if translator_name is None:
            raise MissingTranslatorTableEntryError(label, token, adapters_tuple)

        translator = by_target_name.get(translator_name)
        if translator is None:
            raise UnregisteredTableTranslatorError(label, token, translator_name)

        capabilities = translator.capabilities()
        if translator_name not in checked_coherent:
            for capability in capabilities:
                if capability.translator != translator_name:
                    raise ForeignCapabilityError(translator_name, capability.translator)
            checked_coherent.add(translator_name)

        for adapter in adapters_tuple:
            if Capability(token, translator_name, adapter) not in capabilities:
                raise MissingCapabilityError(label, token, translator_name, adapter)
            assignments.append(RouteAssignment(occurrence_id, translator_name, adapter))

    return tuple(assignments)


# --------------------------------------------------------------------------- router


class TranslationRouter:
    """Routes a resolved ``ExecutionPlan`` to a fixed set of translators."""

    def __init__(self, plan: ExecutionPlan, translators: Sequence[RoutableTranslator]) -> None:
        self._plan = plan
        self._translators = tuple(translators)

    def route(
        self,
        *,
        label: str | None = None,
        translator_table: TranslatorTable | None = None,
        adapters: Sequence[str] | None = None,
    ) -> RoutingResult:
        """Route the plan this router was constructed with.

        Called with none of ``label``, ``translator_table`` and ``adapters``,
        this is exactly the original occurrence-ownership router: unchanged
        behaviour, unchanged errors.

        Called with all three, this resolves every occurrence through the
        estate's translator table instead (architecture sections 9, 11;
        owner ruling R1): for each occurrence, the table is looked up by
        ``label`` and the occurrence's pattern; the named translator must be
        one of ``self._translators`` and must carry the (pattern-or-shape,
        translator, adapter) capability for every entry in ``adapters``.
        Mixing (some but not all three given) is a caller error.
        """

        given = (label is not None, translator_table is not None, adapters is not None)
        if any(given) and not all(given):
            raise TableRoutingArgumentError()

        self._check_target_names_unique()
        if all(given):
            assert label is not None and translator_table is not None and adapters is not None
            return self._route_by_table(label, translator_table, tuple(adapters))
        return self._route_by_ownership()

    def _route_by_ownership(self) -> RoutingResult:
        plan = self._plan
        known_ids = {o.occurrence_id for o in plan.occurrences}

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

    def _route_by_table(
        self, label: str, translator_table: TranslatorTable, adapters: tuple[str, ...]
    ) -> RoutingResult:
        plan = self._plan
        by_target_name = {t.target_name: t for t in self._translators}
        assignments = resolve_table_owners(
            occurrences=tuple(
                (occurrence.occurrence_id, occurrence.pattern_id.value) for occurrence in plan.occurrences
            ),
            label=label,
            translator_table=translator_table,
            adapters=adapters,
            translators=self._translators,
        )
        touched_names: list[str] = []
        for assignment in assignments:
            if assignment.translator_name not in touched_names:
                touched_names.append(assignment.translator_name)

        translations = {name: by_target_name[name].translate(plan) for name in touched_names}
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
