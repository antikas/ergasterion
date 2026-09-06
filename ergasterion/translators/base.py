"""The abstract base class for target-specific translators.

A translator converts a resolved, platform-neutral ``ExecutionPlan`` into
target-specific artefacts such as SQL models, tests, sources, and runtime
manifests. The dependency flows one way: this module imports the framework's
typed graph and translation-result IR; the framework never imports this module.

Translators declare occurrence-level ownership through ``owned_occurrences()``,
``observed_occurrences()`` and ``execution_order()``. The router
(``ergasterion/framework/routing.py``) needs those exact identities to prove
one execution owner per occurrence and to detect a translator that silently
reorders its own dependency-respecting execution. ``plan_digest()`` and
``accepts_handoff()`` give the translator conformance
seam (``ergasterion/framework/translator_conformance.py``) a way to fail
closed on a stale build or an incompatible handoff schema.

``auxiliary_relations()``, ``validate()``, ``deploy()``, ``detect_drift()``
and ``conventions()`` are optional capabilities with default
implementations.
"""

from __future__ import annotations

from abc import ABC, abstractmethod

from ergasterion.framework.graph import AuxiliaryRelation
from ergasterion.framework.models import (
    Capability,
    ConventionsDocument,
    DriftReport,
    Edge,
    ExecutionPlan,
    TranslationResult,
    TranslatorValidationResult,
)

# Re-exported for translator implementations that want one import site.
__all__ = [
    "AuxiliaryRelation",
    "Capability",
    "ConventionsDocument",
    "DriftReport",
    "TranslationResult",
    "TranslatorValidationResult",
    "Translator",
]


class Translator(ABC):
    """Converts a resolved ``ExecutionPlan`` into target-specific artefacts.

    One ``Translator`` subclass exists per target platform: a local-ingestion
    translator, a dbt translator, and any platform such as Composer, Airflow
    or Databricks each implement this same interface.
    """

    @property
    @abstractmethod
    def target_name(self) -> str:
        """Human-readable target platform name (for example ``'dbt'``)."""
        ...

    @abstractmethod
    def owned_occurrences(self) -> frozenset[str]:
        """Occurrence IDs this translator claims as sole execution owner. The
        router rejects a plan occurrence with zero or with more than one
        owner."""
        ...

    def observed_occurrences(self) -> frozenset[str]:
        """Occurrence IDs this translator observes or projects without
        claiming execution ownership (for example a dbt translator emitting a
        static contract test for an occurrence a local-ingestion translator
        owns). Default: none."""
        return frozenset()

    def capabilities(self) -> frozenset[Capability]:
        """The two-axis capability set this translator registers with the
        router (architecture section 3.4): every (pattern-or-shape,
        translator, adapter) triple this translator can render. The estate's
        translator table (``ergasterion.framework.routing``) names, per
        layer label, the translator for each pattern and shape; the router
        then requires that named translator to carry this capability for
        every adapter the estate declares, failing closed on a gap. Every
        entry's ``translator`` field must equal this translator's own
        ``target_name`` -- the router rejects a translator that declares a
        foreign one. Default: no capabilities (an occurrence-ownership-only
        translator that predates the two-axis model, or one not yet wired
        into any estate's translator table)."""
        return frozenset()

    @abstractmethod
    def execution_order(self) -> tuple[str, ...]:
        """The exact order this translator processes its owned occurrences
        in. Must be a permutation of ``owned_occurrences()`` that respects
        every plan edge and the wrapper's enclosure; the router rejects a
        translator that reorders either."""
        ...

    def auxiliary_relations(self) -> tuple[AuxiliaryRelation, ...]:
        """The private relations this translator renders under a product's
        namespace for a named rule or a staged computation (architecture
        section 10). They are excluded from every product contract and
        registered in the estate graph as auxiliary lineage; the graph
        collects them through ``ergasterion.framework.graph.
        collect_auxiliary_relations``. Default: none, for a translator
        whose rendering needs no private relation."""
        return ()

    def plan_digest(self) -> str | None:
        """The plan digest this translator was built and validated against.
        Default ``None``: no digest pinned, so the router skips the staleness
        check. A translator built against one resolved plan should echo
        ``ergasterion.framework.models.compute_plan_digest(plan)`` here so a
        later, differently-shaped plan cannot silently route through it."""
        return None

    def accepts_handoff(self, edge: Edge) -> bool:
        """Whether this translator accepts the handoff schema on an incoming
        edge to an occurrence it owns or observes. Default: accepts every
        closed handoff schema; a translator narrows this only when it
        genuinely cannot consume a given schema."""
        return True

    @abstractmethod
    def validate_compatibility(self, plan: ExecutionPlan) -> list[str]:
        """Return the list of issues preventing this plan from running on the
        target platform. An empty list means fully compatible."""
        ...

    @abstractmethod
    def translate(self, plan: ExecutionPlan) -> TranslationResult:
        """Convert the execution plan into target-specific artefacts."""
        ...

    def validate(self, plan: ExecutionPlan, result: TranslationResult) -> TranslatorValidationResult:
        """Run contract and property-based tests on generated artefacts.
        Default implementation returns pass; translators should override."""
        return TranslatorValidationResult(passed=True)

    def deploy(self, result: TranslationResult, environment: str) -> None:
        """Optional: deploy artefacts to a target environment. Not every
        translator implements deployment."""
        raise NotImplementedError(f"deployment is not implemented for the {self.target_name!r} translator")

    def detect_drift(self, plan: ExecutionPlan, deployed_artefacts: dict[str, str]) -> DriftReport:
        """Compare deployed artefacts against what the current plan would
        generate. Default implementation does character-level comparison;
        translators should override with platform-aware semantic
        comparison."""
        current = self.translate(plan)
        drifted = tuple(
            filename
            for filename, content in deployed_artefacts.items()
            if filename in current.artefacts and current.artefacts[filename] != content
        )
        return DriftReport(has_drift=len(drifted) > 0, drifted_artefacts=drifted)

    def conventions(self) -> ConventionsDocument:
        """Platform-specific conventions read by a translation agent before
        generating code. Default implementation returns an empty document."""
        return ConventionsDocument(target=self.target_name)
