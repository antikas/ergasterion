"""Ergasterion's platform-neutral pattern registry, composition graph and router.

This package classifies the canonical fifteen patterns, resolves a named
profile (only ``landing`` has an execution graph in this release) to its
exact, normative, immutable, digest-bearing execution graph, and routes that
graph to a fixed set of translators. It imports no dbt, DuckDB, SQLite or
orchestrator package. It never imports ``ergasterion.translators``: the
dependency flows one way.
"""

from ergasterion.framework.models import (
    ConventionsDocument,
    DriftReport,
    Edge,
    EdgeRole,
    ExecutionPlan,
    FrameworkError,
    HandoffSchemaId,
    InvalidProfileDefinitionError,
    Occurrence,
    PatternDisposition,
    PatternId,
    Role,
    TranslationResult,
    TranslatorValidationResult,
    UnknownProfileError,
    UnsupportedProfileError,
    ValidationFinding,
    ValidationSeverity,
    compute_plan_digest,
)
from ergasterion.framework.patterns import (
    PATTERN_DISPLAY_NAMES,
    PROFILE_NAMES,
    Profile,
    load_profile,
    parse_profile_document,
)
from ergasterion.framework.resolver import resolve
from ergasterion.framework.routing import (
    BadHandoffError,
    DigestMismatchError,
    DuplicateExecutionOwnerError,
    DuplicateTargetNameError,
    MissingExecutionOwnerError,
    ReorderedOwnershipError,
    RoutableTranslator,
    RouteAssignment,
    RoutingError,
    RoutingResult,
    TranslationRouter,
    UndeclaredAttachmentError,
)

__all__ = [
    "BadHandoffError",
    "ConventionsDocument",
    "DigestMismatchError",
    "DriftReport",
    "DuplicateExecutionOwnerError",
    "DuplicateTargetNameError",
    "Edge",
    "EdgeRole",
    "ExecutionPlan",
    "FrameworkError",
    "HandoffSchemaId",
    "InvalidProfileDefinitionError",
    "MissingExecutionOwnerError",
    "Occurrence",
    "PATTERN_DISPLAY_NAMES",
    "PROFILE_NAMES",
    "PatternDisposition",
    "PatternId",
    "Profile",
    "ReorderedOwnershipError",
    "Role",
    "RoutableTranslator",
    "RouteAssignment",
    "RoutingError",
    "RoutingResult",
    "TranslationResult",
    "TranslationRouter",
    "TranslatorValidationResult",
    "UnknownProfileError",
    "UnsupportedProfileError",
    "UndeclaredAttachmentError",
    "ValidationFinding",
    "ValidationSeverity",
    "compute_plan_digest",
    "load_profile",
    "parse_profile_document",
    "resolve",
]
