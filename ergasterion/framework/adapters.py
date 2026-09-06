"""The adapter package: per-platform conventions as data (architecture
sections 10, 11; owner rulings R1, R10; plan decisions D29, D35).

An adapter is the platform axis (architecture section 2): where artefacts
run. This module owns exactly the conventions ground truth 5 (plan
2026-09-03-ergasterion-engine) assigns the adapter package: dialect deny
lists, physical type mapping, and identifier rules, each declared once per
adapter under ``ergasterion/adapters/<adapter>/conventions.yml`` and read
only through this module. It never carries structural budgets or interface
boundaries -- those stay estate policy in
``declarations/targets/<adapter>.yml`` (``ergasterion.structure_gate``).

``ergasterion/adapters/<adapter>/conventions.yml`` is ENGINE data, not estate
data: it ships inside the wheel (``pyproject.toml``'s ``adapters/*/*.yml``
package-data glob, D28) and resolves package-relative, exactly like
``ergasterion.framework.patterns.PROFILES_DIR``. This module never imports
``EstateContext``: an adapter's conventions are the same regardless of which
estate is pointed at the engine.

Adding a platform is registering a new ``ergasterion/adapters/<name>/
conventions.yml`` directory; this module never changes for it (architecture
section 3.4: "Adding a platform is an adapter. Neither touches the engine.").
``ADAPTER_NAMES`` is therefore discovered from what is actually shipped, never
a hard-coded closed set.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

import yaml

from ergasterion.framework.models import FrameworkError

ADAPTERS_DIR = Path(__file__).resolve().parent.parent / "adapters"

# The two kinds of platform architecture section 10 names: the reference
# adapter executes the whole estate locally and is the engine's executable
# truth; a deployment adapter is everything else.
ADAPTER_KINDS: tuple[str, ...] = ("reference", "deployment")

REQUIRED_CONVENTIONS_KEYS: tuple[str, ...] = (
    "adapter",
    "kind",
    "dialect",
    "incremental_strategy",
    "deny_rules",
    "type_mapping",
    "identifier_rules",
)

# The dpf_type() token vocabulary macros/cross_db.sql's cross-db macros cast
# through (int, float, numeric, string, date, boolean) -- distinct from, and
# lower-level than, the product-declaration neutral type system architecture
# section 3.3 and ergasterion.framework.declaration validate (string,
# integer, boolean, date, timestamp, decimal-as-object). This module lifts
# the former, the physical-type-mapping table dpf_type() itself dispatches:
# every adapter's type_mapping must resolve every one of these tokens, so a
# cross-db macro call never has an adapter with nothing to render it as.
NEUTRAL_TYPE_TOKENS: frozenset[str] = frozenset(
    {"int", "float", "numeric", "string", "timestamp", "date", "boolean"}
)


class UnknownAdapterError(FrameworkError):
    """Raised when a name is not a registered adapter: no
    ``ergasterion/adapters/<name>/conventions.yml`` ships for it."""

    code = "unknown_adapter"

    def __init__(self, adapter_name: object) -> None:
        self.adapter_name = adapter_name
        super().__init__(f"unknown adapter: {adapter_name!r}")


@dataclass(frozen=True)
class DenyRuleSpec:
    """One dialect deny-list entry, as adapter package data. ``pattern`` is
    compiled once at load time; ``ergasterion.dialect_lint`` builds its own
    ``DenyRule`` records from these rather than carrying deny-list data of
    its own."""

    token: str
    pattern: re.Pattern[str]
    message: str


@dataclass(frozen=True)
class AdapterConventions:
    """One adapter's conventions, loaded from its
    ``conventions.yml``: the SQL dialect its artefacts are parsed in, the
    incremental strategy it republishes a keyed relation with, the dialect
    deny-list, the physical type mapping for every neutral type token, and the
    identifier policy (P7: case, quoting, length).

    ``incremental_strategy`` is adapter data because the declared strategies
    genuinely differ: DuckDB republishes with delete-and-insert, BigQuery with
    a merge, and neither accepts the other's token. A declaration says only
    that a product publishes incrementally and by which key; which mechanic
    realises that is the platform's."""

    adapter: str
    kind: str
    dialect: str
    incremental_strategy: str
    deny_rules: tuple[DenyRuleSpec, ...]
    type_mapping: dict[str, str]
    identifier_rules: dict[str, object]


def discover_adapters() -> tuple[str, ...]:
    """Every adapter this build ships conventions for, sorted. Discovered
    from the shipped directories under ``ergasterion/adapters/`` -- never a
    hard-coded set -- so registering a new platform is adding its directory,
    not changing this module."""

    if not ADAPTERS_DIR.is_dir():
        return ()
    return tuple(
        sorted(
            child.name
            for child in ADAPTERS_DIR.iterdir()
            if child.is_dir() and (child / "conventions.yml").is_file()
        )
    )


ADAPTER_NAMES: tuple[str, ...] = discover_adapters()


_CONVENTIONS_CACHE: dict[str, AdapterConventions] = {}


def load_adapter_conventions(adapter_name: str) -> AdapterConventions:
    """Load and validate ``ergasterion/adapters/<adapter_name>/
    conventions.yml``. Fails closed with ``UnknownAdapterError`` when no such
    directory ships, or plain ``FrameworkError`` when the file is present but
    malformed: a missing required key, an adapter/filename mismatch, a kind
    outside ``ADAPTER_KINDS``, a deny rule missing a field, an invalid regex,
    or a type mapping that leaves a neutral type token unresolved."""

    if adapter_name in _CONVENTIONS_CACHE:
        return _CONVENTIONS_CACHE[adapter_name]

    path = ADAPTERS_DIR / adapter_name / "conventions.yml"
    if not path.is_file():
        raise UnknownAdapterError(adapter_name)

    document = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(document, dict):
        raise FrameworkError(f"{path}: expected a YAML mapping")

    missing_keys = [key for key in REQUIRED_CONVENTIONS_KEYS if key not in document]
    if missing_keys:
        raise FrameworkError(f"{path}: missing required key(s): {', '.join(missing_keys)}")

    declared_adapter = document["adapter"]
    if declared_adapter != adapter_name:
        raise FrameworkError(
            f"{path}: adapter {declared_adapter!r} must match its directory name {adapter_name!r}"
        )

    kind = document["kind"]
    if kind not in ADAPTER_KINDS:
        raise FrameworkError(f"{path}: kind must be one of {ADAPTER_KINDS}, got {kind!r}")

    dialect = document["dialect"]
    if not isinstance(dialect, str) or not dialect:
        raise FrameworkError(f"{path}: 'dialect' must be a non-empty SQL dialect name, got {dialect!r}")

    incremental_strategy = document["incremental_strategy"]
    if not isinstance(incremental_strategy, str) or not incremental_strategy:
        raise FrameworkError(
            f"{path}: 'incremental_strategy' must be a non-empty strategy name this adapter "
            f"accepts, got {incremental_strategy!r}"
        )

    raw_deny_rules = document["deny_rules"]
    if not isinstance(raw_deny_rules, list) or not raw_deny_rules:
        raise FrameworkError(
            f"{path}: 'deny_rules' must be a non-empty list -- every registered adapter "
            "declares at least one portability rule"
        )
    deny_rules: list[DenyRuleSpec] = []
    for entry in raw_deny_rules:
        if (
            not isinstance(entry, dict)
            or not isinstance(entry.get("token"), str)
            or not isinstance(entry.get("pattern"), str)
            or not isinstance(entry.get("message"), str)
        ):
            raise FrameworkError(f"{path}: each deny_rules entry needs a token, pattern and message")
        try:
            compiled = re.compile(entry["pattern"], re.IGNORECASE)
        except re.error as exc:
            raise FrameworkError(f"{path}: deny rule {entry['token']!r} has an invalid pattern: {exc}") from exc
        deny_rules.append(DenyRuleSpec(token=entry["token"], pattern=compiled, message=entry["message"]))

    type_mapping = document["type_mapping"]
    if not isinstance(type_mapping, dict) or not all(
        isinstance(k, str) and isinstance(v, str) for k, v in type_mapping.items()
    ):
        raise FrameworkError(f"{path}: 'type_mapping' must be a mapping of neutral type token to physical type")
    missing_types = sorted(NEUTRAL_TYPE_TOKENS - set(type_mapping))
    if missing_types:
        raise FrameworkError(f"{path}: type_mapping is missing neutral type token(s): {', '.join(missing_types)}")

    identifier_rules = document["identifier_rules"]
    if not isinstance(identifier_rules, dict):
        raise FrameworkError(f"{path}: 'identifier_rules' must be a mapping")

    conventions = AdapterConventions(
        adapter=adapter_name,
        kind=kind,
        dialect=dialect,
        incremental_strategy=incremental_strategy,
        deny_rules=tuple(deny_rules),
        type_mapping=dict(type_mapping),
        identifier_rules=dict(identifier_rules),
    )
    _CONVENTIONS_CACHE[adapter_name] = conventions
    return conventions
