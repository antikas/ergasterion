"""Product declaration validation, layer 1 (architecture sections 3.1-3.4, 9).

This module loads a product declaration (a plain dict parsed from YAML),
validates it structurally against ``ergasterion/schemas/product-declaration-
v1.schema.json`` and each step's own pattern configuration schema under
``ergasterion/schemas/patterns/``, then enforces the engine's semantic rules:
label-to-profile resolution through estate policy, composition versus the
resolved profile, ordering, the neutral type system, the neutrality gate for
inline expressions, source-as-contract shape, and duplicate published names
across the estate's whole ``declarations/products/`` tree.

Design note on where a rule lives. JSON Schema (the product-declaration and
per-pattern files) proves a declaration has the right SHAPE: types,
required-ness, no stray top-level or per-block key. Every rule the declaration
contract names by identity -- forbidden pattern, missing mandatory pattern,
out-of-order occurrence, wrong-shape section, unknown pattern key, non-neutral
type, duplicate published name, source-not-a-contract, a consolidation
product with fewer than two sources, and the label/profile resolution family
-- is instead enforced explicitly in this module's Python, each raising a
``DeclarationError`` or ``LabelResolutionError`` that names the product, the
occurrence, and a stable rule slug. This gives every acceptance case a
precise, testable identity independent of JSON Schema's own error wording,
while still using JSON Schema for defence in depth against any other
malformed input.

``checkpoint_retries`` is the one pattern that never appears in ``steps``: it
is the enclosing wrapper (architecture section 4, "Wrapper around the
composition"), configured once through the product's top-level
``checkpointing`` block (architecture section 3.1's worked example). Every
other one of the fifteen patterns is an ordinary step.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator

import jsonschema
import re
import yaml

from ergasterion.estate import EstateContext
from ergasterion.framework.models import (
    FrameworkError,
    InvalidProfileDefinitionError,
    PatternDisposition,
    PatternId,
)
from ergasterion.framework.neutrality import (
    MODE_NAMED_ONLY,
    MODE_SQL,
    NeutralityViolationError,
    find_markers,
    validate_expression,
)
from ergasterion.framework.patterns import PROFILE_NAMES, Profile, load_profile, parse_profile_document
from ergasterion.framework.shapes import UnknownShapeError, get_shape
from ergasterion.framework import rules as rules_mod

SCHEMAS_DIR = Path(__file__).resolve().parent.parent / "schemas"
PATTERN_SCHEMAS_DIR = SCHEMAS_DIR / "patterns"
PRODUCT_SCHEMA_PATH = SCHEMAS_DIR / "product-declaration-v1.schema.json"

# The patterns that appear as ordinary occurrences in `steps:`. checkpoint_retries
# is excluded: see the module docstring.
STEP_PATTERNS: frozenset[PatternId] = frozenset(p for p in PatternId if p is not PatternId.CHECKPOINT_RETRIES)

# The five fixed neutral scalar types (architecture section 3.3): "decimal"
# is never a bare scalar token, it always carries precision and scale, so it
# is expressed as an object, like a structured type, never as a plain
# string -- the full neutral type system is therefore five scalar names plus
# decimal-as-object plus estate-enabled structured-type objects.
NEUTRAL_SCALAR_TYPES: frozenset[str] = frozenset({"string", "integer", "boolean", "date", "timestamp"})

# The two source kinds a ``sources`` entry may declare (architecture section
# 12's fixture-backed source relations). ``contract`` is the default and the
# ordinary case: the contract is published by a product in this estate.
# ``fixture`` binds the contract to a fixture relation for local proof and
# carries the fields that relation delivers, because nothing in the estate
# publishes it.
SOURCE_KIND_CONTRACT = "contract"
SOURCE_KIND_FIXTURE = "fixture"

# The key a ``sources`` entry carries to name which of a producing
# product's published relations it reads (architecture section 7). A
# producer publishing exactly one relation needs no key; a producer
# publishing several is read through one, and the contract pipe
# (``ergasterion.framework.contract``) resolves it.
SOURCE_RELATION_KEY = "relation"

# How a product opening its composition from two or more sources says they
# combine (architecture sections 6 and 8, the consolidating generation:
# "two or more non-landing product sources, with a union or merge
# composition and declared schema conformance"). The block is a sibling of
# ``sources`` because the combination is a fact about the set of them, not
# about any one; ``union`` stacks their rows under one conformed schema and
# ``merge`` puts their columns side by side on the keys it declares.
COMBINE_KEY = "combine"
COMBINE_METHOD_UNION = "union"
COMBINE_METHOD_MERGE = "merge"
COMBINE_METHODS: tuple[str, ...] = (COMBINE_METHOD_MERGE, COMBINE_METHOD_UNION)

# Which rows a merge keeps. ``inner`` keeps the rows every source carries
# the key of, so every column it publishes is as required as the source it
# came from. ``outer`` keeps every row of every source, so a column can
# only be as required as the row it sits on, and the contract publishes the
# columns inherited from a side that can be unmatched as optional.
#
# There is no default. A merge that kept the wrong rows would either lose
# rows silently or publish nulls in a column something downstream was told
# was always present, and the engine never picks between the two for a
# declaration (architecture section 13).
COMBINE_JOIN_KEY = "join"
COMBINE_JOIN_INNER = "inner"
COMBINE_JOIN_OUTER = "outer"
COMBINE_JOINS: tuple[str, ...] = (COMBINE_JOIN_INNER, COMBINE_JOIN_OUTER)

# The key one source of a combination carries to rename and cast its own
# fields onto the shape the combination needs. Its entries take the same
# ``from``/``to``/``type`` form a schema_transform mapping does, because it
# is the same act: the difference is only that this one happens as the
# source is read, so every source of the combination arrives conformed.
SOURCE_CONFORM_KEY = "conform"

# The publication mode whose rows are replaced by a declared key rather than
# swapped whole (architecture section 4, Data Publish).
PUBLICATION_MODE_INCREMENTAL = "incremental"

# A contract reference is "<domain-or-namespace-segment>(.<segment>)+@<major>":
# at least two dot-separated segments, then an integer major version. A bare
# relation/table name (no dot, no "@") never matches -- this is exactly the
# "a source naming a relation rather than a contract" rejection.
_CONTRACT_REFERENCE = re.compile(r"^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)+@[0-9]+$")


# --------------------------------------------------------------------------- errors


class DeclarationError(FrameworkError):
    """One product declaration validation failure. Every raise names the
    product, a stable rule slug (for example ``"forbidden_pattern"``), and,
    where one applies, the occurrence (a step index/pattern, a JSON-pointer-
    style path, or a section name) -- so every acceptance case has one
    precise, testable identity rather than free-text prose alone."""

    code = "declaration_error"

    def __init__(self, *, product: str, rule: str, detail: str, occurrence: str | None = None) -> None:
        self.product = product
        self.rule = rule
        self.occurrence = occurrence
        self.detail = detail
        occurrence_text = occurrence if occurrence is not None else "n/a"
        super().__init__(f"product {product!r}: rule {rule!r} occurrence {occurrence_text!r}: {detail}")


class LabelResolutionError(FrameworkError):
    """Raised by ``resolve_profile_for_label`` for the label/profile
    resolution family: an unknown label, an unknown profile, a profile the
    label does not admit, or a label admitting more than one profile with
    none named. Always names the product, the label and the profile
    (``None`` when not given)."""

    code = "label_resolution_error"

    def __init__(self, *, product: str, rule: str, label: object, profile: object, detail: str) -> None:
        self.product = product
        self.rule = rule
        self.label = label
        self.profile = profile
        super().__init__(f"product {product!r}: rule {rule!r} label {label!r} profile {profile!r}: {detail}")


# --------------------------------------------------------------------------- estate policy


@dataclass(frozen=True)
class EstatePolicy:
    """The estate.yml-declared policy this validation layer reads.
    ``expression_mode`` is ``"sql"`` or ``"named_only"`` (architecture
    section 3.2); ``structured_types`` are the structured type names this
    estate enables beyond the fixed neutral scalars (section 3.3);
    ``labels`` maps each layer label a product may declare to the tuple of
    profile names it admits (section 11, owner ruling R1); ``profiles``
    carries every profile this estate may name, the five reference profiles
    plus any the estate declares of its own (section 11, "the profiles it
    uses, either the reference profiles or its own"), each already parsed
    and validated against the pattern registry."""

    expression_mode: str
    structured_types: frozenset[str]
    labels: dict[str, tuple[str, ...]]
    profiles: dict[str, Profile]


def load_estate_policy(estate_file: Path) -> EstatePolicy:
    """Load and validate the declaration-policy portion of ``estate.yml``.
    Fails closed with plain ``FrameworkError`` on a missing file or a
    malformed policy: these are estate misconfigurations, not a single
    product's fault, so they are not ``DeclarationError``."""

    if not estate_file.is_file():
        raise FrameworkError(f"estate file is missing: {estate_file}")
    document = yaml.safe_load(estate_file.read_text(encoding="utf-8")) or {}
    estate = document.get("estate")
    if not isinstance(estate, dict):
        raise FrameworkError(f"{estate_file}: missing its top-level 'estate:' block")

    expression_mode = estate.get("expression_mode", MODE_SQL)
    if expression_mode not in (MODE_SQL, MODE_NAMED_ONLY):
        raise FrameworkError(
            f"{estate_file}: unknown expression_mode {expression_mode!r}; "
            f"must be {MODE_SQL!r} or {MODE_NAMED_ONLY!r}"
        )

    raw_structured_types = estate.get("structured_types") or []
    if not isinstance(raw_structured_types, list) or not all(
        isinstance(entry, str) for entry in raw_structured_types
    ):
        # A bare scalar (for example a string written where a one-item list
        # was meant) must fail closed here: frozenset() on a string would
        # silently iterate its CHARACTERS rather than reject the mistake.
        raise FrameworkError(
            f"{estate_file}: 'structured_types' must be a list of type names, got {raw_structured_types!r}"
        )
    structured_types = frozenset(raw_structured_types)

    profiles = _load_profiles(estate_file, estate)

    raw_labels = estate.get("labels") or {}
    if not isinstance(raw_labels, dict):
        raise FrameworkError(f"{estate_file}: 'labels' must be a mapping of label name to {{profiles: [...]}}")
    labels: dict[str, tuple[str, ...]] = {}
    for label_name, label_document in raw_labels.items():
        if not isinstance(label_document, dict) or "profiles" not in label_document:
            raise FrameworkError(f"{estate_file}: label {label_name!r} must carry a 'profiles' list")
        admitted = tuple(label_document["profiles"])
        if not admitted:
            raise FrameworkError(f"{estate_file}: label {label_name!r} names no profiles")
        for profile_name in admitted:
            if profile_name not in profiles:
                raise FrameworkError(
                    f"{estate_file}: label {label_name!r} names an unknown profile {profile_name!r}; "
                    f"this estate carries {sorted(profiles)!r}"
                )
            if PatternId.DATA_CONTRACTS not in profiles[profile_name].mandatory:
                # Architecture section 5's one deliberate strengthening of the
                # catalogue: every product publishes a contract, because the
                # contract is the only pipe. A profile that leaves it out would
                # admit a product with no pipe at all.
                raise FrameworkError(
                    f"{estate_file}: label {label_name!r} admits profile {profile_name!r}, which does not "
                    f"make {PatternId.DATA_CONTRACTS.value!r} mandatory; every profile must, because the "
                    "contract is the only pipe"
                )
        labels[label_name] = admitted

    # An estate profile no label admits is configuration with no reader: it
    # constrains nothing, and a product naming it fails on its label instead,
    # which reports the wrong fault.
    admitted_names = {name for names in labels.values() for name in names}
    orphaned = sorted(set(profiles) - set(PROFILE_NAMES) - admitted_names)
    if orphaned:
        raise FrameworkError(
            f"{estate_file}: estate profile {orphaned[0]!r} is declared but no label admits it"
        )

    return EstatePolicy(
        expression_mode=expression_mode,
        structured_types=structured_types,
        labels=labels,
        profiles=profiles,
    )


def _load_profiles(estate_file: Path, estate: dict) -> dict[str, Profile]:
    """Every profile this estate may name: the five reference profiles the
    engine ships as data under ``ergasterion/profiles/``, plus each profile
    the estate declares of its own under ``estate.profiles`` (architecture
    section 11). An estate-declared profile is parsed by the same
    ``parse_profile_document`` the reference ones are, so it is held to the
    same pattern registry: a composition naming a pattern outside the closed
    fifteen fails closed here, naming the profile and the offending pattern.
    An estate profile may not reuse a reference profile's name -- that name
    already resolves to a composition the engine ships, and two compositions
    under one name have no single meaning."""

    profiles: dict[str, Profile] = {name: load_profile(name) for name in PROFILE_NAMES}

    declared = estate.get("profiles") or {}
    if not isinstance(declared, dict):
        raise FrameworkError(
            f"{estate_file}: 'profiles' must be a mapping of profile name to its composition document"
        )
    for profile_name, document in declared.items():
        if not isinstance(profile_name, str) or not profile_name:
            raise FrameworkError(f"{estate_file}: profiles: every profile name must be a non-empty string")
        if profile_name in PROFILE_NAMES:
            raise FrameworkError(
                f"{estate_file}: estate profile {profile_name!r} reuses a reference profile name; "
                "declare an estate profile under a name of its own"
            )
        if not isinstance(document, dict):
            raise FrameworkError(
                f"{estate_file}: estate profile {profile_name!r} must carry a composition document"
            )
        try:
            profiles[profile_name] = parse_profile_document(profile_name, document)
        except InvalidProfileDefinitionError as exc:
            raise FrameworkError(f"{estate_file}: estate profile {profile_name!r}: {exc}") from exc
    return profiles


def resolve_profile_for_label(policy: EstatePolicy, *, product: str, label: object, profile: object) -> str:
    """Resolve a product's ``layer`` label and optional ``profile`` to
    exactly one profile name this estate carries, through ``policy.labels``. Fails
    closed with ``LabelResolutionError`` naming ``product``, the rule slug
    (``unknown_label``, ``profile_required``, ``unknown_profile`` or
    ``profile_not_admitted``), ``label`` and ``profile``."""

    if not isinstance(label, str) or label not in policy.labels:
        raise LabelResolutionError(
            product=product,
            rule="unknown_label",
            label=label,
            profile=profile,
            detail=f"label {label!r} is not declared in estate.yml's labels",
        )
    admitted = policy.labels[label]
    if profile is None:
        if len(admitted) == 1:
            return admitted[0]
        raise LabelResolutionError(
            product=product,
            rule="profile_required",
            label=label,
            profile=profile,
            detail=f"label {label!r} admits more than one profile {admitted!r}; the product must name 'profile'",
        )
    if not isinstance(profile, str) or profile not in policy.profiles:
        raise LabelResolutionError(
            product=product,
            rule="unknown_profile",
            label=label,
            profile=profile,
            detail=f"profile {profile!r} is not one of the profiles this estate carries {sorted(policy.profiles)!r}",
        )
    if profile not in admitted:
        raise LabelResolutionError(
            product=product,
            rule="profile_not_admitted",
            label=label,
            profile=profile,
            detail=f"label {label!r} does not admit profile {profile!r}; admitted: {admitted!r}",
        )
    return profile


# --------------------------------------------------------------------------- schema loading


def _load_json_schema(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


_PRODUCT_SCHEMA_CACHE: dict | None = None
_PATTERN_SCHEMA_CACHE: dict[str, dict] = {}


def _product_schema() -> dict:
    global _PRODUCT_SCHEMA_CACHE
    if _PRODUCT_SCHEMA_CACHE is None:
        _PRODUCT_SCHEMA_CACHE = _load_json_schema(PRODUCT_SCHEMA_PATH)
    return _PRODUCT_SCHEMA_CACHE


def _pattern_schema(pattern_id: PatternId) -> dict:
    if pattern_id.value not in _PATTERN_SCHEMA_CACHE:
        path = PATTERN_SCHEMAS_DIR / f"{pattern_id.value}.schema.json"
        if not path.is_file():
            raise FrameworkError(f"no configuration schema shipped for pattern {pattern_id.value!r}: {path}")
        _PATTERN_SCHEMA_CACHE[pattern_id.value] = _load_json_schema(path)
    return _PATTERN_SCHEMA_CACHE[pattern_id.value]


def _first_schema_error_message(schema: dict, instance: Any) -> str | None:
    validator = jsonschema.Draft202012Validator(schema)
    errors = sorted(validator.iter_errors(instance), key=lambda e: list(e.path))
    if not errors:
        return None
    first = errors[0]
    path = "/".join(str(p) for p in first.path) or "<root>"
    return f"{path}: {first.message}"


# --------------------------------------------------------------------------- neutral types


def _validate_type(type_value: Any, *, product: str, occurrence: str, policy: EstatePolicy) -> None:
    """Validate one neutral-type usage (architecture section 3.3): a fixed
    scalar name, a decimal object carrying integer precision and scale, or a
    structured-type object naming a type the estate enables. Fails closed
    with ``non_neutral_type`` for anything else, or ``undeclared_structured_
    type`` for a structured type name the estate has not listed."""

    if isinstance(type_value, str):
        if type_value in NEUTRAL_SCALAR_TYPES:
            return
        raise DeclarationError(
            product=product,
            rule="non_neutral_type",
            occurrence=occurrence,
            detail=(
                f"{type_value!r} is not a neutral type; use one of {sorted(NEUTRAL_SCALAR_TYPES)!r}, "
                "a decimal object with precision/scale, or an estate-enabled structured type object"
            ),
        )
    if isinstance(type_value, dict):
        name = type_value.get("name")
        if name == "decimal":
            precision = type_value.get("precision")
            scale = type_value.get("scale")
            if not isinstance(precision, int) or isinstance(precision, bool) or precision < 1:
                raise DeclarationError(
                    product=product, rule="non_neutral_type", occurrence=occurrence,
                    detail="a decimal type requires a positive integer 'precision'",
                )
            if not isinstance(scale, int) or isinstance(scale, bool) or scale < 0:
                raise DeclarationError(
                    product=product, rule="non_neutral_type", occurrence=occurrence,
                    detail="a decimal type requires a non-negative integer 'scale'",
                )
            return
        if not isinstance(name, str) or not name or name in NEUTRAL_SCALAR_TYPES:
            raise DeclarationError(
                product=product,
                rule="non_neutral_type",
                occurrence=occurrence,
                detail=f"structured type object names {name!r}, which is not a valid structured-type name",
            )
        if name not in policy.structured_types:
            raise DeclarationError(
                product=product,
                rule="undeclared_structured_type",
                occurrence=occurrence,
                detail=f"structured type {name!r} is not declared in estate.yml's structured_types",
            )
        return
    raise DeclarationError(
        product=product, rule="non_neutral_type", occurrence=occurrence,
        detail=f"type must be a string or an object, got {type_value!r}",
    )


def _walk_key(node: Any, key: str, path: str) -> Iterator[tuple[str, Any]]:
    """Recursively yield ``(path, value)`` for every occurrence of ``key`` in
    a nested dict/list structure. Used for ``type`` (neutral-type checking)
    and, scoped to the ``named_only`` "no inline expression at all" rule,
    ``expression``: both are leaf values that only ever appear nested inside
    a step's own configuration, at whatever depth that pattern's schema
    places them, so one generic walker covers every pattern without
    hard-coding a path per pattern."""

    if isinstance(node, dict):
        if key in node:
            yield path, node[key]
        for child_key, value in node.items():
            child_path = f"{path}.{child_key}" if path else child_key
            yield from _walk_key(value, key, child_path)
    elif isinstance(node, list):
        for index, item in enumerate(node):
            yield from _walk_key(item, key, f"{path}[{index}]")


def _walk_strings(node: Any, path: str) -> Iterator[tuple[str, str]]:
    """Recursively yield ``(path, value)`` for every STRING LEAF anywhere in
    a nested dict/list structure -- the whole document, not only ``steps``
    and not only a value keyed ``expression``. This is what the neutrality
    gate walks (architecture section 3.4, check 7: "a declaration containing
    engine syntax fails validation" is a whole-document guarantee, not one
    scoped to any particular key or section: a Jinja tag hiding in a
    product's owner, a source's scope, or a contract's freshness string is
    exactly as forbidden as one inside a calculated field)."""

    if isinstance(node, str):
        yield path, node
    elif isinstance(node, dict):
        for child_key, value in node.items():
            child_path = f"{path}.{child_key}" if path else child_key
            yield from _walk_strings(value, child_path)
    elif isinstance(node, list):
        for index, item in enumerate(node):
            yield from _walk_strings(item, f"{path}[{index}]")


# --------------------------------------------------------------------------- validated result


@dataclass(frozen=True)
class ValidatedProduct:
    """The result of one product declaration passing layer-1 validation."""

    name: str
    domain: str
    layer: str
    profile: str
    shape: str


# --------------------------------------------------------------------------- validation


def _is_contract_reference(value: object) -> bool:
    return isinstance(value, str) and bool(_CONTRACT_REFERENCE.match(value))


def declared_combination(document: Any) -> dict | None:
    """The ``combine`` block one product declares, or ``None`` where it
    declares none. The one reader of that key, so the contract pipe and
    this validation never disagree about where the combination is stated."""

    block = document.get(COMBINE_KEY) if isinstance(document, dict) else None
    return block if isinstance(block, dict) else None


def conformance_mapping(source: Any) -> tuple[dict, ...]:
    """The conformance entries one source declares, in declaration order,
    or an empty tuple where it declares none."""

    block = source.get(SOURCE_CONFORM_KEY) if isinstance(source, dict) else None
    entries = block.get("mapping") if isinstance(block, dict) else None
    return tuple(entry for entry in entries or () if isinstance(entry, dict))


def validate_declaration(document: dict, *, policy: EstatePolicy) -> ValidatedProduct:
    """Validate one parsed product declaration against layer 1: schema shape,
    label/profile resolution, sources, composition versus profile, ordering,
    neutral types, the neutrality gate, and the target's shape section.
    Returns a ``ValidatedProduct`` on success; raises ``DeclarationError`` or
    ``LabelResolutionError`` on the first failure found, naming the product,
    the rule and the occurrence."""

    product_block = document.get("product") if isinstance(document, dict) else None
    product_name = product_block.get("name") if isinstance(product_block, dict) else None
    if not isinstance(product_name, str) or not product_name:
        raise DeclarationError(
            product="<unknown>",
            rule="malformed_product_block",
            detail="the 'product' block must carry a non-empty string 'name'",
        )

    # 1. Structural envelope: required top-level keys, product/source/target shape,
    #    and no stray top-level or per-block key (this is where a per-product
    #    'mode'/'expression_mode' key is rejected: neither is ever a legal
    #    property of the 'product' block or the document root).
    envelope_error = _first_schema_error_message(_product_schema(), document)
    if envelope_error is not None:
        raise DeclarationError(
            product=product_name, rule="schema_violation", occurrence=envelope_error.split(":", 1)[0],
            detail=envelope_error,
        )

    # 1a. The neutrality gate over the WHOLE document: every string leaf,
    #     both estate expression modes, all five technology markers. Not
    #     scoped to 'steps' or to a key named 'expression' -- a marker in
    #     product.owner, a source's scope, or target.contract.freshness is
    #     exactly as forbidden as one inside a calculated field.
    for str_path, str_value in _walk_strings(document, ""):
        markers = find_markers(str_value)
        if markers:
            raise DeclarationError(
                product=product_name,
                rule="neutrality_violation",
                occurrence=str_path,
                detail=f"technology syntax found: {', '.join(markers)}: {str_value!r}",
            )

    # 1b. named_only: no inline expression at all, scoped to keys literally
    #     named 'expression' -- the only place a declaration ever carries
    #     inline SQL. Markers are already fully covered by 1a in both
    #     modes; this is the mode-specific "no inline SQL at all" half.
    if policy.expression_mode == MODE_NAMED_ONLY:
        for expr_path, expr_value in _walk_key(document, "expression", ""):
            if not isinstance(expr_value, str):
                continue
            try:
                validate_expression(expr_value, mode=policy.expression_mode, context=f"{product_name}:{expr_path}")
            except NeutralityViolationError as exc:
                raise DeclarationError(
                    product=product_name, rule="neutrality_violation", occurrence=expr_path, detail=str(exc),
                ) from exc

    # 2. Label/profile resolution (architecture section 11, owner ruling R1).
    layer = product_block.get("layer")
    requested_profile = product_block.get("profile")
    profile_name = resolve_profile_for_label(policy, product=product_name, label=layer, profile=requested_profile)
    # Every profile the estate may name is already parsed on the policy, the
    # reference ones and the estate's own alike: this validation reads one
    # registry, never a second loader for the estate's half.
    profile = policy.profiles[profile_name]

    # 3. Sources: contract-reference shape, the source-kind pairing, the
    #    neutral types a fixture binding declares, and the consolidation
    #    minimum.
    sources = document.get("sources") or []
    for index, source in enumerate(sources):
        contract = source.get("contract")
        if not _is_contract_reference(contract):
            raise DeclarationError(
                product=product_name,
                rule="source_not_a_contract",
                occurrence=f"sources[{index}]",
                detail=f"source names {contract!r}, which is not a '<domain>.<name>@<major>' contract reference",
            )
        kind = source.get("kind", SOURCE_KIND_CONTRACT)
        binding = source.get("fixture")
        if (kind == SOURCE_KIND_FIXTURE) != (binding is not None):
            raise DeclarationError(
                product=product_name,
                rule="fixture_binding_mismatch",
                occurrence=f"sources[{index}]",
                detail=(
                    f"source kind {kind!r} and the presence of a 'fixture' block must agree: "
                    f"kind {SOURCE_KIND_FIXTURE!r} needs a fixture block, and a fixture block "
                    f"needs kind {SOURCE_KIND_FIXTURE!r}"
                ),
            )
        if binding is not None and source.get(SOURCE_RELATION_KEY) is not None:
            raise DeclarationError(
                product=product_name,
                rule="fixture_source_names_a_relation",
                occurrence=f"sources[{index}]",
                detail=(
                    f"a fixture-bound source already names the relation it reads in its fixture "
                    f"block, so it cannot also carry a {SOURCE_RELATION_KEY!r} key: that key names "
                    "which relation of a producing product's contract a source reads"
                ),
            )
        for field_index, field in enumerate(((binding or {}).get("fields") or [])):
            _validate_type(
                field.get("type"),
                product=product_name,
                occurrence=f"sources[{index}].fixture.fields[{field_index}].type",
                policy=policy,
            )
        for entry_index, entry in enumerate(conformance_mapping(source)):
            _validate_type(
                entry.get("type"),
                product=product_name,
                occurrence=f"sources[{index}].{SOURCE_CONFORM_KEY}.mapping[{entry_index}].type",
                policy=policy,
            )

    # 3a. Two sources that resolve to one relation. Every side of the pipe
    #     keys a read by the producer's published name and the relation it
    #     names, so a second source resolving to the same relation is not a
    #     second read at all: its rows would be stacked on themselves by a
    #     union, and its columns would collide with themselves in a merge.
    seen_reads: dict[str, int] = {}
    for index, source in enumerate(sources):
        published = str(source.get("contract")).split("@", 1)[0]
        relation = source.get(SOURCE_RELATION_KEY)
        read = published if not isinstance(relation, str) else f"{published}#{relation}"
        if read in seen_reads:
            raise DeclarationError(
                product=product_name,
                rule="duplicate_source",
                occurrence=f"sources[{index}]",
                detail=(
                    f"source {str(source.get('contract'))!r} reads the same relation "
                    f"{read!r} as sources[{seen_reads[read]}]; one relation is read once, and a "
                    "second read of it would stack or merge it with itself"
                ),
            )
        seen_reads[read] = index

    # 3b. How two or more sources combine (architecture section 8's
    #     consolidating generation). The engine never picks between a row
    #     union and a column merge, so a product declaring several sources
    #     declares which it means, and a product declaring one never
    #     declares a combination there is nothing to combine for.
    combination = declared_combination(document)
    if len(sources) >= 2 and combination is None:
        raise DeclarationError(
            product=product_name,
            rule="undeclared_composition",
            occurrence=COMBINE_KEY,
            detail=(
                f"{len(sources)} sources are declared and the product does not say how they "
                f"combine; declare a {COMBINE_KEY!r} block whose 'method' is "
                f"{COMBINE_METHOD_UNION!r} or {COMBINE_METHOD_MERGE!r}"
            ),
        )
    if combination is not None:
        if len(sources) < 2:
            raise DeclarationError(
                product=product_name,
                rule="combine_without_two_sources",
                occurrence=COMBINE_KEY,
                detail=(
                    f"a {COMBINE_KEY!r} block says how two or more sources combine, and this "
                    f"product declares {len(sources)}"
                ),
            )
        method = combination.get("method")
        if method not in COMBINE_METHODS:
            raise DeclarationError(
                product=product_name,
                rule="unknown_combine_method",
                occurrence=f"{COMBINE_KEY}.method",
                detail=f"method {method!r} is not one of {list(COMBINE_METHODS)!r}",
            )
        keys = combination.get("keys")
        if method == COMBINE_METHOD_MERGE and not keys:
            raise DeclarationError(
                product=product_name,
                rule="merge_without_keys",
                occurrence=f"{COMBINE_KEY}.keys",
                detail=(
                    f"a {COMBINE_METHOD_MERGE!r} combination puts its sources' columns side by "
                    "side on declared keys, and this product declares none"
                ),
            )
        join = combination.get(COMBINE_JOIN_KEY)
        if method == COMBINE_METHOD_MERGE and join not in COMBINE_JOINS:
            raise DeclarationError(
                product=product_name,
                rule="merge_without_a_declared_join",
                occurrence=f"{COMBINE_KEY}.{COMBINE_JOIN_KEY}",
                detail=(
                    f"a {COMBINE_METHOD_MERGE!r} combination declares which rows it keeps: "
                    f"{COMBINE_JOIN_INNER!r} keeps the rows every source carries the key of and "
                    f"publishes their columns as required as the source they came from, "
                    f"{COMBINE_JOIN_OUTER!r} keeps every row of every source and publishes the "
                    f"columns inherited from a side that can be unmatched as optional; got "
                    f"{join!r}"
                ),
            )
        if method == COMBINE_METHOD_UNION and join is not None:
            raise DeclarationError(
                product=product_name,
                rule="union_with_a_join",
                occurrence=f"{COMBINE_KEY}.{COMBINE_JOIN_KEY}",
                detail=(
                    f"a {COMBINE_METHOD_UNION!r} combination stacks rows under one conformed "
                    "schema and joins nothing, so it keeps every row of every source and takes "
                    "no join"
                ),
            )
        if method == COMBINE_METHOD_UNION and keys:
            raise DeclarationError(
                product=product_name,
                rule="union_with_keys",
                occurrence=f"{COMBINE_KEY}.keys",
                detail=(
                    f"a {COMBINE_METHOD_UNION!r} combination stacks rows under one conformed "
                    "schema and joins nothing, so it takes no keys"
                ),
            )
    for index, source in enumerate(sources):
        if source.get(SOURCE_CONFORM_KEY) is not None and combination is None:
            raise DeclarationError(
                product=product_name,
                rule="conform_without_a_combination",
                occurrence=f"sources[{index}].{SOURCE_CONFORM_KEY}",
                detail=(
                    f"a {SOURCE_CONFORM_KEY!r} mapping conforms one source to the others it is "
                    "combined with; a product reading one source renames and casts in a "
                    "schema_transform occurrence"
                ),
            )

    if profile_name == "consolidation" and len(sources) < 2:
        raise DeclarationError(
            product=product_name,
            rule="consolidation_needs_two_sources",
            occurrence="sources",
            detail=f"a consolidation product must declare at least two product sources; found {len(sources)}",
        )

    # 4. Steps: pattern membership, forbidden disposition, per-pattern config
    #    schema, ordering and neutral types. Architecture section 2: "a
    #    product may use a pattern more than once, for example validation
    #    before and after transformation"; section 5: "the stages a
    #    repeated pattern may take". The ordering constraint therefore
    #    applies to a pattern's FIRST occurrence only -- 4a below governs
    #    every repeat.
    steps = document.get("steps") or []
    order_index = {pattern_id: idx for idx, pattern_id in enumerate(profile.ordering)}
    last_order_index = -1
    last_pattern_value = None
    occurrences_by_pattern: dict[PatternId, list[tuple[int, object]]] = {}

    for index, step in enumerate(steps):
        raw_pattern = step.get("pattern") if isinstance(step, dict) else None
        try:
            pattern_id = PatternId(raw_pattern)
        except ValueError:
            raise DeclarationError(
                product=product_name,
                rule="unknown_pattern_key",
                occurrence=f"steps[{index}]",
                detail=f"step names an unrecognised pattern {raw_pattern!r}",
            ) from None
        if pattern_id not in STEP_PATTERNS:
            raise DeclarationError(
                product=product_name,
                rule="unknown_pattern_key",
                occurrence=f"steps[{index}]",
                detail=(
                    f"{pattern_id.value!r} is not a step pattern; checkpoint_retries is configured "
                    "through the top-level 'checkpointing' block, never as a step"
                ),
            )

        occurrence_tag = f"steps[{index}]:{pattern_id.value}"
        disposition = profile.disposition_of(pattern_id)
        if disposition is PatternDisposition.FORBIDDEN:
            raise DeclarationError(
                product=product_name,
                rule="forbidden_pattern",
                occurrence=occurrence_tag,
                detail=f"pattern {pattern_id.value!r} is forbidden under profile {profile_name!r}",
            )

        step_error = _first_schema_error_message(_pattern_schema(pattern_id), step)
        if step_error is not None:
            raise DeclarationError(
                product=product_name, rule="unknown_pattern_key", occurrence=occurrence_tag, detail=step_error,
            )

        is_first_occurrence = pattern_id not in occurrences_by_pattern
        if pattern_id in order_index and is_first_occurrence:
            this_index = order_index[pattern_id]
            if this_index < last_order_index:
                raise DeclarationError(
                    product=product_name,
                    rule="out_of_order_occurrence",
                    occurrence=occurrence_tag,
                    detail=(
                        f"{pattern_id.value!r} occurs after {last_pattern_value!r} but profile "
                        f"{profile_name!r} orders it earlier"
                    ),
                )
            last_order_index = this_index
            last_pattern_value = pattern_id.value

        for type_path, type_value in _walk_key(step, "type", occurrence_tag):
            _validate_type(type_value, product=product_name, occurrence=type_path, policy=policy)

        if pattern_id is PatternId.DATA_PUBLISH:
            incremental = step.get("publication_mode") == PUBLICATION_MODE_INCREMENTAL
            keyed = step.get("unique_key") is not None
            if incremental != keyed:
                raise DeclarationError(
                    product=product_name,
                    rule="publication_key_mismatch",
                    occurrence=occurrence_tag,
                    detail=(
                        f"publication_mode {PUBLICATION_MODE_INCREMENTAL!r} and 'unique_key' must "
                        "agree: an incremental publication needs the key its rows are replaced by, "
                        "and a key means nothing to a whole-relation swap"
                    ),
                )

        occurrences_by_pattern.setdefault(pattern_id, []).append((index, step.get("stage")))

    # 4a. A repeated pattern is legal only when its own configuration schema
    #     declares a 'stage' (data_validation is the reference case: stage
    #     pre and stage post, architecture section 3.1's worked example),
    #     and every occurrence's stage value must be distinct -- two
    #     occurrences sharing a stage (including two both omitting it) fail
    #     closed, and so does a repeat of a pattern whose schema carries no
    #     'stage' at all to distinguish the repeats.
    for pattern_id, occurrences in occurrences_by_pattern.items():
        if len(occurrences) <= 1:
            continue
        schema_has_stage = "stage" in _pattern_schema(pattern_id).get("properties", {})
        if not schema_has_stage:
            second_index, _ = occurrences[1]
            raise DeclarationError(
                product=product_name,
                rule="repeated_pattern_without_stage",
                occurrence=f"steps[{second_index}]:{pattern_id.value}",
                detail=(
                    f"pattern {pattern_id.value!r} occurs {len(occurrences)} times, but its "
                    "configuration schema declares no 'stage' to distinguish repeats"
                ),
            )
        stages_seen: dict[object, int] = {}
        for occurrence_index, stage_value in occurrences:
            if stage_value in stages_seen:
                raise DeclarationError(
                    product=product_name,
                    rule="duplicate_stage",
                    occurrence=f"steps[{occurrence_index}]:{pattern_id.value}",
                    detail=(
                        f"pattern {pattern_id.value!r} repeats stage {stage_value!r} "
                        f"(first at steps[{stages_seen[stage_value]}])"
                    ),
                )
            stages_seen[stage_value] = occurrence_index

    seen_patterns: set[PatternId] = set(occurrences_by_pattern)
    missing_mandatory = sorted(
        p.value for p in profile.mandatory if p is not PatternId.CHECKPOINT_RETRIES and p not in seen_patterns
    )
    if missing_mandatory:
        raise DeclarationError(
            product=product_name,
            rule="missing_mandatory_pattern",
            occurrence=missing_mandatory[0],
            detail=f"profile {profile_name!r} requires pattern(s) {missing_mandatory!r}, none present in steps",
        )

    # 5. checkpoint_retries: the top-level 'checkpointing' block, not a step.
    if PatternId.CHECKPOINT_RETRIES in profile.mandatory:
        checkpointing = document.get("checkpointing")
        if not isinstance(checkpointing, dict):
            raise DeclarationError(
                product=product_name,
                rule="missing_mandatory_pattern",
                occurrence="checkpoint_retries",
                detail=f"profile {profile_name!r} requires checkpoint_retries; the 'checkpointing' block is missing",
            )
        checkpoint_error = _first_schema_error_message(_pattern_schema(PatternId.CHECKPOINT_RETRIES), checkpointing)
        if checkpoint_error is not None:
            raise DeclarationError(
                product=product_name, rule="unknown_pattern_key", occurrence="checkpointing", detail=checkpoint_error,
            )

    # 6. Target: registered shape and its shape_config section.
    target = document["target"]
    shape_name = target.get("shape")
    try:
        shape = get_shape(shape_name)
    except UnknownShapeError as exc:
        raise DeclarationError(
            product=product_name, rule="unknown_shape", occurrence="target.shape", detail=str(exc),
        ) from exc

    shape_config = target.get("shape_config") or {}
    shape_error = _first_schema_error_message(shape.shape_config_schema, shape_config)
    if shape_error is not None:
        raise DeclarationError(
            product=product_name,
            rule="wrong_shape_section",
            occurrence=f"target.shape_config.{shape_error.split(':', 1)[0]}",
            detail=shape_error,
        )

    return ValidatedProduct(
        name=product_name,
        domain=product_block["domain"],
        layer=layer,
        profile=profile_name,
        shape=shape_name,
    )


def validate_estate_products(products_dir: Path, *, policy: EstatePolicy) -> list[str]:
    """Validate every product declaration under ``products_dir`` (searched
    recursively for ``*.yml``/``*.yaml``, in sorted order for determinism),
    cross-checking that no two declare the same published name
    (``domain.name``) and that no fixture-bound source shadows a contract
    this estate itself publishes. Returns the sorted list of validated
    product names. A missing ``products_dir`` is not an error: a fresh
    estate with no product declarations yet is valid (no migrations, D12
    withdrawn).

    The fixture cross-check is the estate-level half of the source-kind
    rule ``validate_declaration`` enforces per document. A fixture binding
    declares the fields its relation delivers; if a product in the same
    estate publishes that contract, the binding would be a second,
    hand-maintained copy of that product's resolved schema, free to drift
    from it. The fixture hook is for a contract this estate does not
    produce, so the pair fails closed naming both."""

    if not products_dir.is_dir():
        return []
    paths = sorted(set(products_dir.rglob("*.yml")) | set(products_dir.rglob("*.yaml")))
    published_names: dict[str, Path] = {}
    fixture_bound: dict[str, tuple[str, Path]] = {}
    validated: list[str] = []
    for path in paths:
        document = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        result = validate_declaration(document, policy=policy)
        published_name = f"{result.domain}.{result.name}"
        if published_name in published_names:
            raise DeclarationError(
                product=result.name,
                rule="duplicate_published_name",
                occurrence=published_name,
                detail=(
                    f"published name {published_name!r} is claimed by both "
                    f"{published_names[published_name]} and {path}"
                ),
            )
        published_names[published_name] = path
        for index, source in enumerate(document.get("sources") or []):
            if source.get("kind") != SOURCE_KIND_FIXTURE:
                continue
            fixture_bound.setdefault(
                str(source["contract"]).split("@", 1)[0], (result.name, path)
            )
        validated.append(result.name)

    for target, (consumer, consumer_path) in sorted(fixture_bound.items()):
        producer_path = published_names.get(target)
        if producer_path is not None:
            raise DeclarationError(
                product=consumer,
                rule="fixture_shadows_declared_product",
                occurrence=target,
                detail=(
                    f"{consumer_path} binds contract {target!r} to a fixture relation, but "
                    f"{producer_path} declares the product that publishes it; the fixture hook "
                    "is for a contract this estate does not produce"
                ),
            )
    return sorted(validated)


# --------------------------------------------------------------------------- CLI


def main() -> int:
    """Layer 1 (schema, composition, ordering, neutral types, the
    neutrality gate) then layer 2 (inline expression parsing and column
    resolution, the aggregation rule, named-rule reference resolution and
    the completeness gate -- ergasterion.framework.rules) over the
    estate's product declarations. Layer 2 runs only once layer 1 has
    validated every product: a plan is not complete enough to resolve
    expressions or rule references against until its shape, composition
    and neutrality are already proven. Architecture section 9's
    ``validate`` step is both layers together; the two are kept
    distinguishable in the failure output (``FAIL (layer 1)`` /
    ``FAIL (layer 2)``) so a caller can tell a structural declaration
    defect from an expression/rule-resolution defect without parsing
    the error's own class."""

    parser = argparse.ArgumentParser(
        description="Validate every product declaration under the estate's declarations/products/ tree."
    )
    parser.add_argument(
        "--estate-root",
        type=Path,
        default=None,
        help="Root of the estate to validate (resolved from the environment or working directory when omitted).",
    )
    args = parser.parse_args()
    ctx = EstateContext.resolve(estate_root=args.estate_root)
    products_dir = ctx.declarations_dir / "products"

    try:
        policy = load_estate_policy(ctx.estate_file)
        names = validate_estate_products(products_dir, policy=policy)
    except FrameworkError as error:
        print(f"validate FAIL (layer 1): {error}")
        return 1

    try:
        catalogue = rules_mod.load_rule_catalogue(estate_dir=ctx.root / "rules")
        rules_mod.validate_estate_layer2(products_dir, catalogue=catalogue)
    except FrameworkError as error:
        print(f"validate FAIL (layer 2): {error}")
        return 1

    print(f"validate OK: {len(names)} product declaration(s) valid under {products_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
