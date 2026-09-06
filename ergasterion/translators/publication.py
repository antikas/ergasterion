"""The publication translator (architecture section 10).

Renders Data Contracts, Schema Publish, Metadata Capture and Lineage
Capture into ODCS contracts, one ODPS descriptor per product, the contract
compliance check, and the estate graph. The contract artefacts come from
the product contract ``ergasterion.framework.contract`` builds out of a
validated declaration and its shape; the graph artefacts come from the
product graph ``ergasterion.framework.graph`` resolves out of the same
declarations. Both are supplied to this translator already resolved: it
serialises, it never re-derives.

It registers the two-axis capability for those four patterns, under every
declared adapter, for the router's table-driven mode
(``ergasterion.framework.routing.TranslationRouter.route(label=,
translator_table=, adapters=)``). The estate's translator table names this
translator for all four patterns under every label, including landing
(architecture section 10: "at every profile including landing").

This translator never claims occurrence ownership in the occurrence-
ownership routing sense (``owned_occurrences()`` is always empty): every
product it serves is resolved through the table-driven router, which reads
only ``capabilities()`` and ``translate()``.
"""

from __future__ import annotations

from ergasterion.framework.adapters import ADAPTER_NAMES
from ergasterion.framework.contract import (
    ProductContract,
    build_compliance_check,
    build_odcs_document,
    build_odps_document,
    dump_json,
    dump_yaml,
    odcs_id,
)
from ergasterion.framework.graph import ProductGraph, graph_artefacts
from ergasterion.framework.models import Capability, ExecutionPlan, PatternId, TranslationResult
from ergasterion.translators.base import Translator

TARGET_NAME = "publication"
TRANSLATOR_VERSION = "1.0.0"

PUBLICATION_PATTERNS: tuple[PatternId, ...] = (
    PatternId.DATA_CONTRACTS,
    PatternId.SCHEMA_PUBLISH,
    PatternId.METADATA_CAPTURE,
    PatternId.LINEAGE_CAPTURE,
)

# Where the estate-graph artefacts sit inside this translator's artefact
# set. The estate emitter writes the same file names under its own graphs
# directory; both take them from ergasterion.framework.graph.
GRAPH_ARTEFACT_PREFIX = "graph"


class PublicationTranslator(Translator):
    """Renders every supplied product's contract into ODCS, ODPS and the
    contract compliance check, and the supplied product graph into its
    node, edge, lineage, validation and description artefacts. Constructed
    with the finished ``ProductContract`` set and, for Lineage Capture, the
    finished ``ProductGraph`` it serves; ``translate()`` is a pure function
    of those, never of the routed ``ExecutionPlan`` (the plan's occurrences
    carry no per-product declaration content -- see the module docstring of
    ``ergasterion.framework.contract``)."""

    def __init__(
        self,
        *,
        contracts: tuple[ProductContract, ...] = (),
        graph: ProductGraph | None = None,
        plan_digest: str | None = None,
    ) -> None:
        self._contracts = contracts
        self._graph = graph
        self._plan_digest = plan_digest

    @property
    def target_name(self) -> str:
        return TARGET_NAME

    def owned_occurrences(self) -> frozenset[str]:
        return frozenset()

    def observed_occurrences(self) -> frozenset[str]:
        return frozenset()

    def capabilities(self) -> frozenset[Capability]:
        return frozenset(
            Capability(pattern.value, self.target_name, adapter)
            for pattern in PUBLICATION_PATTERNS
            for adapter in ADAPTER_NAMES
        )

    def execution_order(self) -> tuple[str, ...]:
        return ()

    def plan_digest(self) -> str | None:
        return self._plan_digest

    def validate_compatibility(self, plan: ExecutionPlan) -> list[str]:
        return []

    def translate(self, plan: ExecutionPlan) -> TranslationResult:
        artefacts: dict[str, str] = {}
        for contract in self._contracts:
            domain = contract.identity.domain
            name = contract.identity.name
            base = f"{domain}/{name}"
            for relation in contract.relations:
                relation_short_name = relation.name.rsplit(".", 1)[-1]
                artefacts[f"{base}/{relation_short_name}.odcs.yml"] = dump_yaml(
                    build_odcs_document(contract, relation)
                )
            artefacts[f"{base}/{name}.odps.yml"] = dump_yaml(build_odps_document(contract))
            artefacts[f"{base}/contract-compliance.json"] = dump_json(build_compliance_check(contract))
        graph_metadata: dict[str, object] = {}
        if self._graph is not None:
            for file_name, text in graph_artefacts(self._graph).items():
                artefacts[f"{GRAPH_ARTEFACT_PREFIX}/{file_name}"] = text
            graph_metadata = {
                "order": list(self._graph.order()),
                "generations": self._graph.generations(),
                "checkpoint_relations": list(self._graph.checkpoint_relations()),
                "auxiliary_relations": [
                    {"product": entry.product, "relation": entry.relation, "translator": entry.translator}
                    for entry in self._graph.auxiliary
                ],
            }
        return TranslationResult(
            artefacts=artefacts,
            metadata={
                "target_name": self.target_name,
                "products": [
                    {
                        "domain": c.identity.domain,
                        "name": c.identity.name,
                        "relations": [odcs_id(c, r) for r in c.relations],
                    }
                    for c in self._contracts
                ],
                "graph": graph_metadata,
            },
        )
