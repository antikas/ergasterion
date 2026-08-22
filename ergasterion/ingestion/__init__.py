"""Closed Pydantic projections of the frozen Bronze portable IDL, delivery/state half:
delivery input, raw-receipt, reprocessing/remediation, migrations/state, validation/
disposition, lifecycle/publication/projection intent and confirmation, attestation,
backup and evidence records -- plus the deterministic runtime built on top of them:
the nine port protocols (``ports``), the ``IngestionRuntime`` state machine
(``runtime``), and the packaged adapter-conformance seam and in-memory reference
implementation (``conformance``).

This package depends on ``ergasterion.framework`` (``bronze_contract`` for vocabulary
and the contract declaration, ``runtime_binding`` for runtime binding and deployment);
neither of those imports it. See ``ergasterion.ingestion.records`` for the full record
family, the port declarations, and the schema-bundle/equivalence-report generators.
"""

from ergasterion.ingestion.conformance import (
    build_memory_ports,
    exercise_all_operations,
    load_vectors,
    run_adapter_conformance,
    run_all,
)
from ergasterion.ingestion.ports import PORT_PROTOCOLS, PortSet
from ergasterion.ingestion.records import (
    ALL_ENUM_MODELS,
    ALL_RECORD_MODELS,
    ALL_UNION_MODELS,
    PORT_OPERATION_ORDER,
    PORTS,
    generate_equivalence_report,
    generate_schema_bundle,
    load_idl,
)
from ergasterion.ingestion.runtime import (
    PORT_FIELD_ORDER,
    Admission,
    AppliedProjection,
    Clock,
    IngestionRuntime,
    PortError,
    admit,
    scheduled_occurrences,
)

__all__ = [
    "ALL_ENUM_MODELS",
    "ALL_RECORD_MODELS",
    "ALL_UNION_MODELS",
    "PORT_OPERATION_ORDER",
    "PORTS",
    "generate_equivalence_report",
    "generate_schema_bundle",
    "load_idl",
    "PortSet",
    "PORT_PROTOCOLS",
    "PORT_FIELD_ORDER",
    "Admission",
    "AppliedProjection",
    "IngestionRuntime",
    "PortError",
    "Clock",
    "admit",
    "scheduled_occurrences",
    "build_memory_ports",
    "exercise_all_operations",
    "load_vectors",
    "run_adapter_conformance",
    "run_all",
]
