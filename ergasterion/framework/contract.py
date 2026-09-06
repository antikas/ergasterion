"""The product contract: architecture section 7, "Contracts: the only pipe".

Every product publishes one contract, generated from its validated
declaration and its shape (architecture sections 4, 6, 7; section 13's ban
on inferring ownership from names). This module builds that contract as a
plain, engine-neutral record -- identity, schema, freshness, quality
guarantees, versioning policy, lineage, access and support -- and projects
it into the serialisations the publication translator renders: an ODCS v3.1
document per relation, one ODPS descriptor per product, and the contract
compliance check as an emitted artefact description. Nothing here is ever
hand-written: every function is a pure projection of a validated declaration
document (``ergasterion.framework.declaration.validate_declaration`` has
already run) plus the estate's declared ownership policy.

Schema derivation propagates real typed fields through the whole
composition, and the product's shape then says which relations that field
set publishes (``ergasterion.framework.shapes``: the ``declared`` shape
publishes the composition's own relation, another shape may publish
several) -- a contract never publishes a schema the engine did
not derive. A landing product's relation schema is its source table's
typed fields from the landing (Landing Product) contract the ingestion side
already builds (``relation_schema_from_landing_projection``). A non-landing
product starts from the schemas of the contracts its ``sources`` name,
resolved from the estate's other product declarations and landing
contracts at the pinned major, then applies each occurrence in composition
order (``derive_relation_fields``): ``batch_transfer`` passes fields
through unchanged; ``schema_transform`` renames, casts and drops per its
mapping; ``calculated_fields`` adds each field with its declared type;
``data_enrichment`` adds the looked-up fields with the types the
referenced contract carries for them; ``data_aggregation`` replaces the
field set with the grain/group fields (types carried over from the
current set) plus each aggregate, which must declare its own type or the
occurrence fails closed; ``data_curation`` passes fields through and adds
nothing undeclared; ``data_filtering``, ``data_validation``,
``data_contracts``, ``lineage_capture``, ``metadata_capture``,
``schema_publish``, ``data_publish`` and ``checkpoint_retries`` change
nothing. Where a field's type cannot be derived -- an unresolved source, an
enrichment field the referenced contract does not carry, or an aggregate
with no declared type -- resolution fails closed naming the product, the
occurrence and the field; a contract is never emitted with an empty or
partial schema silently. ``resolve_relation_registry`` resolves every
product in an estate this way, in dependency order, and checks every
declared ``sources[].expect.fields`` entry against the resolved producer's
schema as it goes (``check_source_expectation``).

Which relation a consumer reads. A product's contract lists every relation
its shape renders, and a source names the one it reads through its
``relation`` key (architecture section 7). Both reads of an upstream
contract carry it: a ``sources`` entry and a ``data_enrichment`` lookup.
The key is the relation as the producer's shape names it, without the
product prefix, because the read already names the product.
``resolve_consumer_reads`` is the one place that answers the question, for
the schema propagation here, for the estate graph and for the translator
that renders the read: a read of a producer publishing exactly one
relation needs no key and resolves to it; a read of a producer publishing
several and naming none fails closed with
``AmbiguousSourceRelationError`` naming consumer, read, producer and every
name available; a key naming a relation the producer does not publish
fails closed with ``UnknownSourceRelationError`` naming the same, and a
key repeating the product prefix is told the form expected. Nothing
anywhere takes the first relation of several.

How two or more sources combine. A product opening its composition from
one source carries that source's relation as it is. A product declaring
two or more says how they combine, in its ``combine`` block: ``union``
puts their rows under one conformed schema, ``merge`` puts their columns
side by side on declared keys, keeping the rows its declared ``join``
says. There is no default for either: a product declaring several sources
and no combination fails closed, because merging what was meant to be
stacked (or the reverse) is exactly the silent mis-read this pipe exists
to prevent, and a merge that named no join would either drop rows or
publish nulls in a column a consumer was told is always present.

A source of a combination may carry a ``conform``
mapping (``from``, ``to``, ``type``) that renames and casts its own fields
onto the shape the combination needs, and a source that still cannot
conform -- a union source missing a field of the conformed schema, or
carrying one the conformed schema does not, or declaring a different type
for one; a merge source missing a declared key, or bringing a non-key
field another source already brings -- fails closed naming the product,
the source and the field. ``resolve_opening_composition`` is the one
implementation: the schema propagation below reads it for the field set
the composition opens with, and the dbt translator reads the same result
for the relation it renders, so the rendered read and the published
contract can never disagree about how the sources were combined.

A field is marked ``required`` (not nullable) when a ``data_validation``
occurrence anywhere in the composition declares ``not_null: true`` or
``completeness: 1.0`` for that field name, or when it was already required
in the schema it passed through from. The one place a field loses that
mark is an ``outer`` merge: it keeps every row of every source, so a
non-key field inherited from a side that can be unmatched is published as
optional, and the contract and the compliance check agree about it by
construction rather than by hope.
"""

from __future__ import annotations

import difflib
import json
from dataclasses import dataclass, replace
from enum import Enum
from pathlib import Path
from typing import Any, Iterable, Mapping

import yaml

from ergasterion.framework.declaration import (
    COMBINE_JOIN_INNER,
    COMBINE_JOIN_KEY,
    COMBINE_JOIN_OUTER,
    COMBINE_JOINS,
    COMBINE_KEY,
    COMBINE_METHOD_MERGE,
    COMBINE_METHOD_UNION,
    COMBINE_METHODS,
    SOURCE_CONFORM_KEY,
    SOURCE_RELATION_KEY,
    ValidatedProduct,
    conformance_mapping,
    declared_combination,
)
from ergasterion.framework.generated import YAML_HEADER
from ergasterion.framework.models import FrameworkError, RelationField, RelationSchema
from ergasterion.framework.shapes import ShapeRelation, UnknownShapeError, get_shape

# The one marker every generated file carries, in its YAML comment form. Defined
# once in ergasterion/framework/generated.py and imported here: a second marker
# text would make a generated contract read as hand-authored to every reader that
# looks for the first one, and would name a module path that has already moved.
GENERATED_HEADER = YAML_HEADER + "\n"

# What one resolved read of an upstream contract was declared as: a
# ``sources`` entry the composition opens from, or a ``data_enrichment``
# lookup it joins to.
READ_SOURCE = "source"
READ_LOOKUP = "lookup"


def dump_yaml(document: dict[str, Any]) -> str:
    """The one YAML rendering every product-contract artefact (the
    contract's own JSON projection when written to disk, ODCS, ODPS) uses,
    so the publication translator and the emit_contracts.py/emit_odps.py
    disk writers stay byte-identical by construction."""

    body = yaml.safe_dump(document, sort_keys=False, default_flow_style=False, allow_unicode=True, width=100)
    return GENERATED_HEADER + body


def dump_json(document: dict[str, Any]) -> str:
    """Deterministic, sorted-key JSON rendering for the compliance-check
    artefact (no YAML header convention: it is a plain descriptive
    document, not a served contract)."""

    return json.dumps(document, sort_keys=True, indent=2) + "\n"


def write_generated_files(
    files: dict[Path, str], *, directory: Path, prune_patterns: Iterable[str]
) -> list[Path]:
    """Write ``files`` to disk, changed-only, and prune whatever under
    ``directory`` matches one of ``prune_patterns`` but is no longer among
    ``files`` (a renamed or removed product). The one write/prune
    implementation both ``ergasterion.emit_contracts`` and
    ``ergasterion.emit_odps`` call for their own product-declaration-driven
    artefacts, so the two never carry near-identical copies."""

    changed: list[Path] = []
    live = set(files)
    if directory.exists():
        for pattern in prune_patterns:
            for existing in directory.rglob(pattern):
                if existing not in live:
                    changed.append(existing)
                    existing.unlink()
    for path in sorted(files):
        content = files[path]
        current = path.read_text(encoding="utf-8") if path.exists() else None
        if current == content:
            continue
        changed.append(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content.encode("utf-8"))
    return changed


def check_generated_files(
    files: dict[Path, str], *, directory: Path, root: Path, prune_patterns: Iterable[str]
) -> list[str]:
    """Report every on-disk divergence from ``files`` (missing, drifted,
    orphaned under ``directory``) without writing. The read-only
    counterpart of ``write_generated_files``, shared the same way."""

    problems: list[str] = []
    for path in sorted(files):
        rel = path.relative_to(root).as_posix()
        if not path.exists():
            problems.append(f"MISSING (never generated on disk): {rel}")
            continue
        current = path.read_text(encoding="utf-8")
        if current != files[path]:
            diff = "\n".join(
                difflib.unified_diff(
                    current.splitlines(), files[path].splitlines(),
                    fromfile=f"{rel} (on disk)", tofile=f"{rel} (regenerated)", lineterm="",
                )
            )
            problems.append(f"DRIFT (hand-edited or stale): {rel}\n{diff}")
    if directory.exists():
        live = set(files)
        for pattern in prune_patterns:
            for existing in sorted(directory.rglob(pattern)):
                if existing not in live:
                    problems.append(f"ORPHAN (on disk, no longer generated): {existing.relative_to(root).as_posix()}")
    return problems

# --------------------------------------------------------------------------- estate ownership


@dataclass(frozen=True)
class EstateOwnership:
    """``estate.yml``'s ``namespace``, ``support`` and ``team``: the three
    declarations behind a generated contract's identity, ownership and
    support elements (architecture section 7; section 13). None of the
    three carries an engine default."""

    namespace: str
    support: str
    team: str


class EstateOwnershipError(FrameworkError):
    """Raised when ``estate.yml`` is missing its ``estate:`` block, or one
    of ``namespace``, ``support`` or ``team`` is absent or not a non-empty
    string. Names every missing key found, not just the first, so a caller
    corrects the estate once rather than one key at a time."""

    code = "estate_ownership_error"

    def __init__(self, *, estate_file: Path, missing: tuple[str, ...]) -> None:
        self.estate_file = estate_file
        self.missing = missing
        super().__init__(
            f"{estate_file}: estate.yml has no engine default for {', '.join(missing)}; "
            "declare each as a non-empty string under the top-level 'estate:' block"
        )


def load_estate_ownership(estate_file: Path) -> EstateOwnership:
    """Load and require ``estate.yml``'s ``namespace``, ``support`` and
    ``team`` together. Fails closed with ``EstateOwnershipError`` naming
    every one of the three that is missing or not a non-empty string."""

    if not estate_file.is_file():
        raise EstateOwnershipError(estate_file=estate_file, missing=("namespace", "support", "team"))
    document = yaml.safe_load(estate_file.read_text(encoding="utf-8")) or {}
    estate = document.get("estate")
    if not isinstance(estate, dict):
        raise EstateOwnershipError(estate_file=estate_file, missing=("namespace", "support", "team"))

    missing = tuple(
        key for key in ("namespace", "support", "team")
        if not isinstance(estate.get(key), str) or not estate.get(key)
    )
    if missing:
        raise EstateOwnershipError(estate_file=estate_file, missing=missing)

    return EstateOwnership(namespace=estate["namespace"], support=estate["support"], team=estate["team"])


# --------------------------------------------------------------------------- contract model


@dataclass(frozen=True)
class ProductIdentity:
    """A product contract's identity element: the estate namespace, the
    product's domain and name, and the major version a consumer pins to
    (``name@major``, architecture section 7)."""

    namespace: str
    domain: str
    name: str
    major: int


@dataclass(frozen=True)
class QualityGuarantee:
    """One quality guarantee the contract advertises: an ODCS quality
    dimension and a human-readable description of what is enforced."""

    dimension: str
    description: str


@dataclass(frozen=True)
class ProductContract:
    """The generated product contract (architecture section 7): identity,
    schema (every relation the shape renders), freshness, quality
    guarantees, versioning policy, lineage, access and support. Built once
    by ``build_product_contract``; never constructed by hand elsewhere."""

    identity: ProductIdentity
    relations: tuple[RelationSchema, ...]
    freshness: str | None
    quality: tuple[QualityGuarantee, ...]
    versioning_policy: str
    lineage: tuple[str, ...]
    access: dict[str, Any] | None
    support: str
    team: str
    description: str | None = None


CONTRACT_SCHEMA = "ergasterion.product-contract/v1"

DEFAULT_VERSIONING_POLICY = (
    "additive nullable fields are minor and non-breaking; removals, renames, type changes "
    "and new required fields are major and breaking"
)


class InvalidVersionError(FrameworkError):
    """A product's declared ``product.version`` has no parseable leading
    integer major component. Names the product and the offending version
    text."""

    code = "invalid_version"

    def __init__(self, *, product: str, version: object) -> None:
        self.product = product
        self.version = version
        super().__init__(f"product {product!r}: version {version!r} has no parseable leading major component")


class ContractGenerationError(FrameworkError):
    """Raised when a product's contract cannot be generated from its
    declaration and shape: today, only when the shape is not one this
    module knows how to render relations for. Unreachable for a document
    that already passed ``ergasterion.framework.declaration.validate_
    declaration`` against the current shape registry (only ``declared`` is
    registered), kept as a named failure for the day a new shape registers
    without a matching branch here."""

    code = "contract_generation_error"

    def __init__(self, *, product: str, shape: str) -> None:
        self.product = product
        self.shape = shape
        super().__init__(f"product {product!r}: no relation rendering for shape {shape!r}")


# --------------------------------------------------------------------------- schema derivation


class MissingLandingSchemaError(FrameworkError):
    """A landing product has neither of the two origins its schema may
    come from: a typed schema supplied for it (its Landing Product
    Contract's projection, keyed by the product's own published name) or a
    source bound to the relation it lands, carrying the fields that
    relation delivers (owner ruling R5). A landing product's columns are
    never derived from its own steps, so an absent origin fails closed
    rather than publishing an empty relation."""

    code = "missing_landing_schema"

    def __init__(self, *, product: str) -> None:
        self.product = product
        super().__init__(
            f"product {product!r}: no landing schema supplied (its Landing Product Contract's "
            "typed projection, keyed by this product's own published name) and its 'sources' "
            "binds no relation whose declared fields could supply one"
        )


class LandingSchemaOriginError(FrameworkError):
    """A landing product's schema has more than one origin: a supplied
    landing contract and a bound relation, or two bound relations. Names
    the product and what it declares. One relation is landed, so one
    origin states its columns; two would be free to drift apart."""

    code = "landing_schema_origin"

    def __init__(self, *, product: str, detail: str) -> None:
        self.product = product
        self.detail = detail
        super().__init__(
            f"product {product!r}: a landing product takes its schema from exactly one origin, "
            f"and this one declares {detail}"
        )


class UnresolvedSourceError(FrameworkError):
    """A product names a source or enrichment contract that resolves to
    neither an already-resolved product nor a supplied landing schema, or
    the registry as a whole makes no progress across a full pass (a
    cycle). Names the product and the unresolved reference(s)."""

    code = "unresolved_source"

    def __init__(self, *, product: str, source: object) -> None:
        self.product = product
        self.source = source
        super().__init__(f"product {product!r}: source {source!r} does not resolve to any known contract")


class AmbiguousSourceRelationError(FrameworkError):
    """A consumer reads the contract of a product whose shape publishes
    more than one relation, without saying which one it reads. Names the
    consumer, the read (a source or an enrichment lookup), the producer,
    every relation the producer publishes and the names one may be chosen
    by."""

    code = "ambiguous_source_relation"

    def __init__(
        self,
        *,
        consumer: str,
        producer: str,
        relations: tuple[str, ...],
        choices: tuple[str, ...] = (),
        read: str = "source",
    ) -> None:
        self.consumer = consumer
        self.producer = producer
        self.relations = relations
        self.choices = choices
        self.read = read
        super().__init__(
            f"product {consumer!r}: {read} {producer!r} publishes {len(relations)} relations "
            f"({', '.join(relations)}); a consumer declares which relation it reads, with a "
            f"{SOURCE_RELATION_KEY!r} key on the {read} naming one of: {', '.join(choices)}"
        )


class UnknownSourceRelationError(FrameworkError):
    """A consumer's read names a relation the producer's contract does not
    publish under that name. Names the consumer, the read, the producer,
    the relation asked for and the names available. A key that carries the
    producer's own product prefix is named as the form error it is: the key
    is the relation as the producer's shape names it, and the read already
    names the product."""

    code = "unknown_source_relation"

    def __init__(
        self,
        *,
        consumer: str,
        producer: str,
        relation: str,
        choices: tuple[str, ...],
        read: str = "source",
        prefixed: bool = False,
    ) -> None:
        self.consumer = consumer
        self.producer = producer
        self.relation = relation
        self.choices = choices
        self.read = read
        self.prefixed = prefixed
        available = ", ".join(choices) if choices else "none"
        detail = (
            (
                f"; the {SOURCE_RELATION_KEY!r} key names the relation as {producer!r} names it, "
                f"without the {producer!r} prefix"
            )
            if prefixed
            else ""
        )
        super().__init__(
            f"product {consumer!r}: {read} {producer!r} publishes no relation named "
            f"{relation!r}{detail}; it publishes: {available}"
        )


class UnresolvedFieldError(FrameworkError):
    """A field's type cannot be derived while propagating the schema
    through one occurrence: a ``data_enrichment`` lookup field the
    referenced contract does not carry, a ``data_aggregation`` grain or
    group field the current schema does not carry, or an aggregate with no
    declared type. Names the product, the occurrence and the field -- the
    brief's stop condition made explicit rather than an empty or partial
    schema published silently."""

    code = "unresolved_field"

    def __init__(self, *, product: str, occurrence: str, field: str) -> None:
        self.product = product
        self.occurrence = occurrence
        self.field = field
        super().__init__(
            f"product {product!r}: occurrence {occurrence!r}: field {field!r} has no derivable type"
        )


class SourceExpectationError(FrameworkError):
    """A product's declared ``sources[].expect.fields`` names a field the
    resolved producer's current schema does not carry (architecture
    section 3.1: "what this product relies on; validated at emit").
    Names the consumer, the producer and the first missing field."""

    code = "source_expectation_error"

    def __init__(self, *, consumer: str, producer: str, field: str) -> None:
        self.consumer = consumer
        self.producer = producer
        self.field = field
        super().__init__(
            f"consumer {consumer!r} expects field {field!r} from producer {producer!r}, "
            "which its resolved schema does not carry"
        )


class UndeclaredCompositionError(FrameworkError):
    """A product opens its composition from two or more sources and does
    not say how they combine. Names the product, the number of sources and
    the key expected. There is no default: a row union and a column merge
    are different relations, and the engine never picks one."""

    code = "undeclared_composition"

    def __init__(self, *, product: str, sources: int) -> None:
        self.product = product
        self.sources = sources
        super().__init__(
            f"product {product!r}: {sources} sources are declared and the product does not say "
            f"how they combine; declare a {COMBINE_KEY!r} block whose 'method' is "
            f"{COMBINE_METHOD_UNION!r} (their rows under one conformed schema) or "
            f"{COMBINE_METHOD_MERGE!r} (their columns on declared keys)"
        )


class UndeclaredMergeJoinError(FrameworkError):
    """A merge does not say which rows it keeps. Names the product and the
    vocabulary. There is no default: an outer merge publishes empty columns
    where an inner merge drops rows, and the engine never picks one."""

    code = "undeclared_merge_join"

    def __init__(self, *, product: str, join: object) -> None:
        self.product = product
        self.join = join
        super().__init__(
            f"product {product!r}: a {COMBINE_METHOD_MERGE!r} combination declares which rows "
            f"it keeps, one of {list(COMBINE_JOINS)!r}, in {COMBINE_KEY!r}.{COMBINE_JOIN_KEY!r}; "
            f"got {join!r}"
        )


class UnconformableSourceError(FrameworkError):
    """One source of a combination cannot be conformed to the shape the
    combination needs: its mapping names a field it does not carry or
    renames one twice, or a union source misses, adds or retypes a field
    of the schema the union publishes. Names the product, the source
    contract and the field, with what the source would have to do about
    it."""

    code = "unconformable_source"

    def __init__(self, *, product: str, source: str, field: str, detail: str) -> None:
        self.product = product
        self.source = source
        self.field = field
        self.detail = detail
        super().__init__(
            f"product {product!r}: source {source!r} cannot conform at field {field!r}: {detail}"
        )


class UnmergeableSourceError(FrameworkError):
    """One source of a merge cannot be merged with the sources beside it:
    it does not carry a declared key, it declares a different type for
    one, or it brings a non-key field another source already brings.
    Names the product, the source contract and the field."""

    code = "unmergeable_source"

    def __init__(self, *, product: str, source: str, field: str, detail: str) -> None:
        self.product = product
        self.source = source
        self.field = field
        self.detail = detail
        super().__init__(
            f"product {product!r}: source {source!r} cannot merge at field {field!r}: {detail}"
        )


def check_source_expectation(
    *, consumer: str, producer: str, expected_fields: Iterable[str], schema: RelationSchema
) -> None:
    """Fail closed with ``SourceExpectationError`` naming the first
    expected field ``schema`` (the producer's resolved current schema)
    does not carry."""

    present = {field.name for field in schema.fields}
    missing = sorted(set(expected_fields) - present)
    if missing:
        raise SourceExpectationError(consumer=consumer, producer=producer, field=missing[0])


_LOGICAL_TYPE_TOKENS = {
    "utf8_string": "string",
    "boolean": "boolean",
    "date": "date",
    "int64": "integer",
    "utc_instant": "timestamp",
}


def _neutral_type_from_logical_type(logical_type: Any) -> Any:
    """Project a Landing Product Contract field's ``LogicalType``
    (``ergasterion.framework.landing_contract``) onto this engine's neutral
    type system (architecture section 3.3). Fails closed with
    ``UnmappedNeutralTypeError`` on a logical type with no neutral
    counterpart (for example ``binary``)."""

    if isinstance(logical_type, str):
        try:
            return _LOGICAL_TYPE_TOKENS[logical_type]
        except KeyError:
            raise UnmappedNeutralTypeError(type_value=logical_type) from None
    kind = getattr(logical_type, "kind", None)
    if kind == "decimal":
        return {"name": "decimal", "precision": logical_type.precision, "scale": logical_type.scale}
    if kind == "local_datetime":
        return "timestamp"
    raise UnmappedNeutralTypeError(type_value=logical_type)


def relation_schema_from_landing_projection(*, domain: str, name: str, projection: Any) -> RelationSchema:
    """A landing product's relation schema: the ingestion side's own typed
    physical schema (a Landing Product Contract's ``projection``, each
    field carrying ``name``, ``logical_type`` and ``nullable``), projected
    onto this engine's neutral types. The single source of truth for a
    landing product's published columns; never re-derived from the
    product declaration's own steps."""

    fields = tuple(
        RelationField(
            name=field.name,
            type=_neutral_type_from_logical_type(field.logical_type),
            required=not field.nullable,
        )
        for field in projection
    )
    return RelationSchema(name=f"{domain}.{name}", fields=fields)


def shape_relations_for(
    document: dict, validated: ValidatedProduct, *, fields: tuple[RelationField, ...]
) -> tuple[ShapeRelation, ...]:
    """Every relation a product publishes, taken from its shape
    (``ergasterion.framework.shapes``, the one owner of what a shape
    renders), given the field set its composition leaves. Fails closed with
    ``ContractGenerationError`` naming product and shape for a shape the
    registry does not carry or one that renders no relation at all, and
    with ``ShapeConstraintError`` for a shape section the composition
    cannot carry."""

    try:
        shape = get_shape(validated.shape)
    except UnknownShapeError:
        raise ContractGenerationError(product=validated.name, shape=validated.shape) from None
    relations = shape.relations(
        domain=validated.domain,
        name=validated.name,
        shape_config=(document.get("target") or {}).get("shape_config") or {},
        fields=fields,
    )
    if not relations:
        raise ContractGenerationError(product=validated.name, shape=validated.shape)
    return relations


def _source_published_name(contract_ref: object) -> object:
    return contract_ref.split("@", 1)[0] if isinstance(contract_ref, str) else contract_ref


def declared_source_relation(source: Mapping[str, Any]) -> str | None:
    """The relation a declared read says it reads (architecture section 7),
    or ``None`` where it says nothing. Both reads of an upstream contract
    carry the key: a ``sources`` entry and a ``data_enrichment`` lookup.

    The value is the relation as the producer's own shape names it, without
    the producer's product prefix: a source of ``sales.order_star`` reading
    its order-line fact declares ``relation: fact_order_line``. The read
    already names the product."""

    value = source.get(SOURCE_RELATION_KEY)
    return value if isinstance(value, str) else None


def relation_choice(producer: str, relation_name: str) -> str | None:
    """The name a consumer declares to read ``relation_name`` from
    ``producer``: the relation as the producer's shape names it, without
    the product prefix. ``None`` for a product's own composition relation,
    which carries no name of its own and is therefore never chosen by name.

    This is the one place the published-name convention is read back, so a
    declaration never has to spell out how a shape composes a relation
    name."""

    prefix = f"{producer}__"
    return relation_name[len(prefix):] if relation_name.startswith(prefix) else None


def relation_choices(producer: str, available: tuple[str, ...]) -> tuple[str, ...]:
    """Every name a consumer may put in a ``relation`` key for
    ``producer``, in the order the producer publishes them."""

    return tuple(
        choice for choice in (relation_choice(producer, name) for name in available) if choice
    )


def source_relation_key(contract_ref: object, relation: str | None = None) -> str:
    """The key one declared read of an upstream contract resolves under.

    It is a lookup key, not a relation name: the producer's published name,
    with the relation the read names appended where it names one, so two
    reads of one producer under different relations never collide. Every
    side of the pipe calls this -- the schema propagation below, the
    emission route that says which model a read is rendered from, and the
    translator that renders it -- so the schema a rendered read projects can
    never be a different relation's from the one the contract derived."""

    published = str(_source_published_name(contract_ref))
    return published if relation is None else f"{published}#{relation}"


def _required_field_names(document: dict) -> set[str]:
    required: set[str] = set()
    for step in document.get("steps") or []:
        if (step or {}).get("pattern") != "data_validation":
            continue
        for rule in step.get("rules") or []:
            name = rule.get("field")
            if name is None:
                continue
            if rule.get("not_null") is True or rule.get("completeness") == 1.0:
                required.add(name)
    return required


@dataclass(frozen=True)
class OccurrenceFields:
    """The field set visible AFTER one occurrence of one composition, with
    that occurrence's own identity. ``propagate_relation_fields`` returns
    one of these per step so a translator can render each occurrence
    against the exact columns reaching it, reading the same propagation
    the published contract is derived from rather than re-deriving its
    own."""

    index: int
    pattern: str
    occurrence: str
    fields: tuple[RelationField, ...]


@dataclass(frozen=True)
class ConformedColumn:
    """One column one source contributes to the relation a composition
    opens with: the name the source carries it under, the name the opening
    relation publishes it under, and the neutral type a conformance
    mapping casts it to (``None`` where the column is carried as it
    arrives)."""

    source_name: str
    name: str
    cast_type: Any | None


@dataclass(frozen=True)
class SourceComposition:
    """One source's place in the relation a composition opens with: the
    key its read resolved under (``source_relation_key``, the key a
    translator looks the model up by), the contract it names, and the
    columns it contributes."""

    key: str
    contract: str
    columns: tuple[ConformedColumn, ...]


@dataclass(frozen=True)
class OpeningComposition:
    """The relation a product's composition opens with, and how it was
    made: the combination method (``None`` for a single source), the merge
    keys, the merge's declared join (``None`` for anything but a merge),
    the typed field set, and each source's contribution in declaration
    order."""

    method: str | None
    keys: tuple[str, ...]
    fields: tuple[RelationField, ...]
    sources: tuple[SourceComposition, ...]
    join: str | None = None


def _conform_source(
    source: Mapping[str, Any],
    schema: RelationSchema,
    *,
    consumer: str,
    contract: str,
) -> tuple[tuple[RelationField, ...], tuple[ConformedColumn, ...]]:
    """One source's fields and columns after its own conformance mapping.
    A mapping entry renames its ``from`` column to ``to`` and casts it to
    the declared type; every column the mapping does not name is carried
    as it arrives. A mapping naming a column the source's relation does
    not carry fails closed naming product, source and field."""

    renames: dict[str, Mapping[str, Any]] = {}
    for entry in conformance_mapping(source):
        name = str(entry["from"])
        if name in renames:
            raise UnconformableSourceError(
                product=consumer,
                source=contract,
                field=name,
                detail=f"the {SOURCE_CONFORM_KEY!r} mapping renames it twice",
            )
        renames[name] = entry
    present = {field.name for field in schema.fields}
    for name in sorted(renames):
        if name not in present:
            raise UnconformableSourceError(
                product=consumer,
                source=contract,
                field=name,
                detail=(
                    f"the {SOURCE_CONFORM_KEY!r} mapping renames it, and the source's relation "
                    f"carries: {', '.join(sorted(present)) or 'nothing'}"
                ),
            )
    fields: list[RelationField] = []
    columns: list[ConformedColumn] = []
    for field in schema.fields:
        entry = renames.get(field.name)
        if entry is None:
            fields.append(field)
            columns.append(ConformedColumn(source_name=field.name, name=field.name, cast_type=None))
            continue
        target = str(entry["to"])
        fields.append(RelationField(name=target, type=entry["type"]))
        columns.append(
            ConformedColumn(source_name=field.name, name=target, cast_type=entry["type"])
        )
    seen: set[str] = set()
    for field in fields:
        if field.name in seen:
            raise UnconformableSourceError(
                product=consumer,
                source=contract,
                field=field.name,
                detail=(
                    f"two of this source's columns reach the composition under that name; a "
                    f"{SOURCE_CONFORM_KEY!r} mapping renames a column onto a name the source "
                    "already carries"
                ),
            )
        seen.add(field.name)
    return tuple(fields), tuple(columns)


def _union_fields(
    conformed: list[tuple[str, tuple[RelationField, ...]]], *, consumer: str
) -> tuple[RelationField, ...]:
    """The one schema a union publishes: the first source's conformed
    field set, which every other source must match name for name and type
    for type. A source missing one of its fields, carrying one it does not
    have, or declaring a different type for one fails closed naming
    product, source and field."""

    _first_contract, opening = conformed[0]
    expected = {field.name: field for field in opening}
    for contract, fields in conformed[1:]:
        available = {field.name: field for field in fields}
        for name, field in expected.items():
            candidate = available.get(name)
            if candidate is None:
                raise UnconformableSourceError(
                    product=consumer,
                    source=contract,
                    field=name,
                    detail=(
                        "the union's schema carries it and this source does not; map one of its "
                        f"own fields onto it with a {SOURCE_CONFORM_KEY!r} entry"
                    ),
                )
            if candidate.type != field.type:
                raise UnconformableSourceError(
                    product=consumer,
                    source=contract,
                    field=name,
                    detail=(
                        f"this source declares type {candidate.type!r} and the union's schema "
                        f"declares {field.type!r}; cast it with a {SOURCE_CONFORM_KEY!r} entry"
                    ),
                )
        for name in available:
            if name not in expected:
                raise UnconformableSourceError(
                    product=consumer,
                    source=contract,
                    field=name,
                    detail=(
                        "this source carries it and the union's schema does not; a union "
                        "publishes one schema, so map it onto a field the schema carries"
                    ),
                )
    return opening


def _merged_fields(
    conformed: list[tuple[str, tuple[RelationField, ...]]],
    *,
    consumer: str,
    keys: tuple[str, ...],
    join: str,
) -> tuple[RelationField, ...]:
    """The one schema a merge publishes: the declared keys, then every
    non-key field each source brings, in declaration order. A source
    missing a key, declaring a different type for one, or bringing a
    non-key field another source already brings fails closed naming
    product, source and field.

    The declared join says how required each of those non-key fields can
    be. An ``outer`` merge keeps every row of every source, so a row can
    reach the published relation with nothing at all from one side, and
    every field inherited from a side that can be unmatched is published
    as optional. An ``inner`` merge keeps only the rows every source
    carries the key of, so each field stays exactly as required as the
    source it came from. The keys themselves are coalesced across the
    sources, so they are as required as the first source declares them
    either way."""

    ordered: list[RelationField] = []
    for key in keys:
        first_contract, first_fields = conformed[0]
        carried = {field.name: field for field in first_fields}.get(key)
        if carried is None:
            raise UnmergeableSourceError(
                product=consumer,
                source=first_contract,
                field=key,
                detail="the merge declares it as a key and this source does not carry it",
            )
        ordered.append(carried)
        for contract, fields in conformed[1:]:
            candidate = {field.name: field for field in fields}.get(key)
            if candidate is None:
                raise UnmergeableSourceError(
                    product=consumer,
                    source=contract,
                    field=key,
                    detail="the merge declares it as a key and this source does not carry it",
                )
            if candidate.type != carried.type:
                raise UnmergeableSourceError(
                    product=consumer,
                    source=contract,
                    field=key,
                    detail=(
                        f"this source declares key type {candidate.type!r} and "
                        f"{first_contract!r} declares {carried.type!r}"
                    ),
                )
    brought: dict[str, str] = {}
    for contract, fields in conformed:
        for field in fields:
            if field.name in keys:
                continue
            if field.name in brought:
                raise UnmergeableSourceError(
                    product=consumer,
                    source=contract,
                    field=field.name,
                    detail=(
                        f"source {brought[field.name]!r} already brings it; a merge never picks "
                        f"one of two, so rename it with a {SOURCE_CONFORM_KEY!r} entry or declare "
                        "it as a merge key"
                    ),
                )
            brought[field.name] = contract
            ordered.append(
                replace(field, required=False)
                if join == COMBINE_JOIN_OUTER and field.required
                else field
            )
    return tuple(ordered)


def resolve_opening_composition(
    document: dict, *, consumer: str, source_schemas: Mapping[str, RelationSchema]
) -> OpeningComposition:
    """The relation one product's composition opens with, resolved from
    the schemas its sources publish and the combination it declares.

    This is the one implementation of how sources combine.
    ``propagate_relation_fields`` reads the field set off it, and the
    translator renders the read off the same result, so the relation
    rendered and the contract published can never disagree.

    Fails closed with ``UnresolvedSourceError`` on a source whose schema
    is not supplied, ``UndeclaredCompositionError`` on two or more sources
    with no declared combination (``ergasterion.framework.declaration``
    rejects that before anything is generated; this is the same rule at
    the point the schemas are actually combined, so nothing here ever
    guesses), ``UnconformableSourceError`` on a union source that cannot
    conform and ``UnmergeableSourceError`` on a merge source that cannot
    merge."""

    sources = document.get("sources") or []
    resolved: list[tuple[str, tuple[RelationField, ...]]] = []
    compositions: list[SourceComposition] = []
    for source in sources:
        contract_ref = source.get("contract")
        key = source_relation_key(contract_ref, declared_source_relation(source))
        schema = source_schemas.get(key)
        if schema is None:
            raise UnresolvedSourceError(product=consumer, source=contract_ref)
        expected = (source.get("expect") or {}).get("fields") or []
        if expected:
            check_source_expectation(
                consumer=consumer,
                producer=str(contract_ref),
                expected_fields=expected,
                schema=schema,
            )
        fields, columns = _conform_source(
            source, schema, consumer=consumer, contract=str(contract_ref)
        )
        resolved.append((str(contract_ref), fields))
        compositions.append(
            SourceComposition(key=key, contract=str(contract_ref), columns=columns)
        )

    if not sources:
        return OpeningComposition(method=None, keys=(), fields=(), sources=())
    if len(sources) == 1:
        return OpeningComposition(
            method=None, keys=(), fields=resolved[0][1], sources=tuple(compositions)
        )

    combination = declared_combination(document)
    if combination is None:
        raise UndeclaredCompositionError(product=consumer, sources=len(sources))
    method = combination.get("method")
    keys = tuple(str(key) for key in (combination.get("keys") or ()))
    if method == COMBINE_METHOD_UNION:
        fields = _union_fields(resolved, consumer=consumer)
        # A union stacks rows, and rows are matched by position, so every
        # source contributes its columns in the order the schema publishes
        # them rather than in the order its own relation happens to carry
        # them. Every source carries exactly the schema's names by now
        # (``_union_fields`` refuses anything else), so the ordering is
        # total.
        position = {field.name: index for index, field in enumerate(fields)}
        compositions = [
            replace(
                source,
                columns=tuple(sorted(source.columns, key=lambda column: position[column.name])),
            )
            for source in compositions
        ]
    elif method == COMBINE_METHOD_MERGE:
        join = combination.get(COMBINE_JOIN_KEY)
        if join not in COMBINE_JOINS:
            # ``ergasterion.framework.declaration`` refuses this before
            # anything is generated. This is the same rule where the schemas
            # are actually merged, reachable by a caller that resolves a
            # composition it did not validate, so nothing here picks a join
            # for a declaration that named none.
            raise UndeclaredMergeJoinError(product=consumer, join=join)
        fields = _merged_fields(resolved, consumer=consumer, keys=keys, join=str(join))
        return OpeningComposition(
            method=str(method),
            keys=keys,
            fields=fields,
            sources=tuple(compositions),
            join=str(join),
        )
    else:
        raise UndeclaredCompositionError(product=consumer, sources=len(sources))
    return OpeningComposition(
        method=str(method), keys=keys, fields=fields, sources=tuple(compositions)
    )


def propagate_relation_fields(
    document: dict, validated: ValidatedProduct, *, source_schemas: Mapping[str, RelationSchema]
) -> tuple[tuple[RelationField, ...], tuple[OccurrenceFields, ...]]:
    """Propagate typed fields through one non-landing product's
    composition and return both the field set the ``sources`` block opens
    with and the field set after every step, in step order (see the module
    docstring for the per-pattern rules).

    This is the one implementation of that propagation.
    ``derive_relation_fields`` takes the last entry for the published
    contract; a translator takes the whole sequence, so the schema a
    rendered occurrence reads can never disagree with the schema the
    contract publishes."""

    consumer = f"{validated.domain}.{validated.name}"
    per_occurrence: list[OccurrenceFields] = []

    composition = resolve_opening_composition(
        document, consumer=consumer, source_schemas=source_schemas
    )
    fields: dict[str, RelationField] = {field.name: field for field in composition.fields}
    opening = composition.fields

    for index, step in enumerate(document.get("steps") or []):
        pattern = (step or {}).get("pattern")
        occurrence_tag = f"steps[{index}]:{pattern}"

        if pattern == "schema_transform":
            for mapping in step.get("mapping") or []:
                from_name = mapping.get("from")
                to_name = mapping.get("to")
                if from_name is not None:
                    fields.pop(from_name, None)
                if to_name:
                    fields[to_name] = RelationField(name=to_name, type=mapping.get("type"))

        elif pattern == "calculated_fields":
            for field_entry in step.get("fields") or []:
                name = field_entry.get("name")
                if name:
                    fields[name] = RelationField(name=name, type=field_entry.get("type"))

        elif pattern == "data_enrichment":
            for lookup in step.get("lookups") or []:
                lookup_ref = lookup.get("contract")
                lookup_name = source_relation_key(lookup_ref, declared_source_relation(lookup))
                lookup_schema = source_schemas.get(lookup_name)
                if lookup_schema is None:
                    raise UnresolvedSourceError(product=consumer, source=lookup_ref)
                lookup_fields = {field.name: field for field in lookup_schema.fields}
                for field_name in lookup.get("fields") or []:
                    referenced = lookup_fields.get(field_name)
                    if referenced is None:
                        raise UnresolvedFieldError(product=consumer, occurrence=occurrence_tag, field=field_name)
                    fields[field_name] = RelationField(name=field_name, type=referenced.type)

        elif pattern == "data_aggregation":
            group_names = list(step.get("grain") or []) + list(step.get("groups") or [])
            aggregated: dict[str, RelationField] = {}
            for name in group_names:
                existing = fields.get(name)
                if existing is None:
                    raise UnresolvedFieldError(product=consumer, occurrence=occurrence_tag, field=name)
                aggregated[name] = existing
            for aggregate in step.get("aggregates") or []:
                name = aggregate.get("name")
                declared_type = aggregate.get("type")
                if not name or declared_type is None:
                    raise UnresolvedFieldError(
                        product=consumer, occurrence=occurrence_tag, field=name or "<unnamed>"
                    )
                aggregated[name] = RelationField(name=name, type=declared_type)
            fields = aggregated

        # batch_transfer, data_curation ("passes fields through and adds
        # nothing undeclared"), data_filtering, data_validation,
        # data_contracts, lineage_capture, metadata_capture, schema_publish,
        # data_publish and checkpoint_retries change nothing.

        per_occurrence.append(
            OccurrenceFields(
                index=index,
                pattern=str(pattern),
                occurrence=occurrence_tag,
                fields=tuple(fields.values()),
            )
        )

    return opening, tuple(per_occurrence)


def derive_relation_fields(
    document: dict, validated: ValidatedProduct, *, source_schemas: Mapping[str, RelationSchema]
) -> tuple[RelationField, ...]:
    """The typed field set a non-landing product's composition leaves, with
    every field the composition's ``data_validation`` rules require marked
    not-null. ``source_schemas`` maps every published name this product's
    ``sources`` or ``data_enrichment`` lookups may reference to its
    already-resolved ``RelationSchema``. Fails closed with
    ``UnresolvedSourceError`` on a source or lookup contract
    ``source_schemas`` does not carry, or ``UnresolvedFieldError`` naming
    the product, the occurrence and the field when a field's type cannot
    be derived."""

    opening, per_occurrence = propagate_relation_fields(
        document, validated, source_schemas=source_schemas
    )
    final = per_occurrence[-1].fields if per_occurrence else opening
    required = _required_field_names(document)
    return tuple(
        RelationField(name=field.name, type=field.type, required=field.required or field.name in required)
        for field in final
    )


def derive_shape_relations(
    document: dict, validated: ValidatedProduct, *, source_schemas: Mapping[str, RelationSchema]
) -> tuple[ShapeRelation, ...]:
    """Every relation a non-landing product publishes: its shape applied to
    the field set its composition leaves."""

    return shape_relations_for(
        document,
        validated,
        fields=derive_relation_fields(document, validated, source_schemas=source_schemas),
    )


def derive_relation_schema(
    document: dict, validated: ValidatedProduct, *, source_schemas: Mapping[str, RelationSchema]
) -> RelationSchema:
    """The one relation schema a non-landing product publishes. Fails
    closed with ``ContractGenerationError`` naming product and shape for a
    shape that renders more than one: a caller asking for one relation has
    to be told when there are several, rather than being handed the
    first."""

    relations = derive_shape_relations(document, validated, source_schemas=source_schemas)
    if len(relations) != 1:
        raise ContractGenerationError(product=validated.name, shape=validated.shape)
    return relations[0].schema


def landing_schema_for(
    document: dict, *, product: str, landing_schemas: Mapping[str, RelationSchema]
) -> RelationSchema:
    """The relation schema one landing product publishes, from the one
    origin it declares (owner ruling R5).

    Two origins exist and exactly one must be present. The ingestion side's
    own typed projection is supplied for the product's published name; a
    landing product that lands a bound relation instead -- a seed or a
    fixture relation, the way reference data enters an estate -- declares
    that relation as a source and carries the fields it delivers. Fails
    closed with ``MissingLandingSchemaError`` naming product and sources
    when neither is present, and with ``LandingSchemaOriginError`` when
    both are, or when two relations are bound."""

    supplied = landing_schemas.get(product)
    bindings = [
        source
        for source in document.get("sources") or []
        if isinstance(source, Mapping) and source.get("fixture") is not None
    ]
    if len(bindings) > 1:
        raise LandingSchemaOriginError(
            product=product,
            detail=(
                f"{len(bindings)} bound relations: "
                + ", ".join(repr(str(source.get("contract"))) for source in bindings)
            ),
        )
    if supplied is not None and bindings:
        raise LandingSchemaOriginError(
            product=product,
            detail=(
                "both a supplied landing schema and the bound relation "
                f"{str(bindings[0].get('contract'))!r}"
            ),
        )
    if supplied is not None:
        return supplied
    if not bindings:
        raise MissingLandingSchemaError(product=product)
    binding = bindings[0]["fixture"]
    return RelationSchema(
        name=product,
        fields=tuple(
            RelationField(name=field["name"], type=field["type"])
            for field in binding["fields"]
        ),
    )


def fixture_relation_schemas(
    entries: Mapping[str, tuple[dict, ValidatedProduct]]
) -> dict[str, RelationSchema]:
    """Every contract an estate's products bind to a fixture relation
    (architecture section 12's fixture-backed source relations), keyed by
    the published name the contract reference resolves to, with the field
    list the binding declares.

    These stand in the same place as a landing product's ingestion-derived
    schema: they are the schemas a composition starts from that no product
    in the estate derives. ``ergasterion.framework.declaration.
    validate_estate_products`` has already refused any binding that
    shadows a contract the estate publishes, so a name here can never
    collide with a resolved product."""

    schemas: dict[str, RelationSchema] = {}
    for _published_name, (document, _validated) in sorted(entries.items()):
        for source in document.get("sources") or []:
            binding = source.get("fixture")
            if binding is None:
                continue
            target = _source_published_name(source.get("contract"))
            schemas[str(target)] = RelationSchema(
                name=str(target),
                fields=tuple(
                    RelationField(name=field["name"], type=field["type"])
                    for field in binding["fields"]
                ),
            )
    return schemas


@dataclass(frozen=True)
class ResolvedRead:
    """One consumer's read of one upstream contract, resolved to the single
    relation it reads.

    ``key`` is what the schema propagation and the translator look the read
    up under (``source_relation_key``); ``relation`` is the resolved
    relation's published name; ``suffix`` is the shape suffix that relation
    is rendered under, and ``None`` where the relation is the producer's
    own composition relation or an opening schema no product derives."""

    kind: str
    contract: str
    producer: str
    key: str
    relation: str
    suffix: str | None
    schema: RelationSchema


def resolve_relation_name(
    available: tuple[str, ...],
    *,
    consumer: str,
    producer: str,
    relation: str | None,
    read: str = READ_SOURCE,
) -> str:
    """The published name of the one relation a consumer reads from
    ``producer``, given every relation ``producer`` publishes and the
    relation the consumer's read declares (``None`` where it declares
    none). ``read`` is the word for what declared it, a source or a lookup.

    The declared key is the relation as the producer's shape names it,
    without the product prefix (``relation_choice``). Fails closed with
    ``AmbiguousSourceRelationError`` when a producer publishes several and
    the read names none, and with ``UnknownSourceRelationError`` when it
    names one the producer does not publish -- including a key that repeats
    the product prefix, which is named as the form error it is. There is no
    default and no first-of-several: architecture section 7 has the
    consumer declare which relation it reads, and a guess here is the
    silent mis-read the contract pipe exists to prevent."""

    choices = relation_choices(producer, available)
    if relation is None:
        if len(available) != 1:
            raise AmbiguousSourceRelationError(
                consumer=consumer,
                producer=producer,
                relations=available,
                choices=choices,
                read=read,
            )
        return available[0]
    for name in available:
        if relation_choice(producer, name) == relation:
            return name
    raise UnknownSourceRelationError(
        consumer=consumer,
        producer=producer,
        relation=relation,
        choices=choices,
        read=read,
        prefixed=relation == producer or relation.startswith(f"{producer}__"),
    )


def resolve_source_relation(
    relations: tuple[ShapeRelation, ...],
    *,
    consumer: str,
    producer: str,
    relation: str | None,
    read: str = READ_SOURCE,
) -> ShapeRelation:
    """The one relation of ``producer``'s shape a consumer reads, resolved
    by name through ``resolve_relation_name``."""

    chosen = resolve_relation_name(
        tuple(entry.schema.name for entry in relations),
        consumer=consumer,
        producer=producer,
        relation=relation,
        read=read,
    )
    return next(entry for entry in relations if entry.schema.name == chosen)


def _resolve_read(
    *,
    kind: str,
    consumer: str,
    contract_ref: object,
    relation: str | None,
    resolved: Mapping[str, tuple[ShapeRelation, ...]],
    opening_schemas: Mapping[str, RelationSchema],
) -> ResolvedRead:
    """One read of one upstream contract, resolved to the relation it
    reads. A producer this estate derives is resolved through its shape's
    relations; a schema the estate only opens with (a landing product's
    ingestion schema or a fixture-bound source's declared fields) publishes
    exactly one relation, under the producer's own published name."""

    producer = str(_source_published_name(contract_ref))
    relations = resolved.get(producer)
    if relations is None:
        schema = opening_schemas.get(producer)
        if schema is None:
            raise UnresolvedSourceError(product=consumer, source=contract_ref)
        name = resolve_relation_name(
            (producer,), consumer=consumer, producer=producer, relation=relation, read=kind
        )
        return ResolvedRead(
            kind=kind,
            contract=str(contract_ref),
            producer=producer,
            key=source_relation_key(contract_ref, relation),
            relation=name,
            suffix=None,
            schema=schema,
        )
    chosen = resolve_source_relation(
        relations, consumer=consumer, producer=producer, relation=relation, read=kind
    )
    return ResolvedRead(
        kind=kind,
        contract=str(contract_ref),
        producer=producer,
        key=source_relation_key(contract_ref, relation),
        relation=chosen.schema.name,
        suffix=chosen.suffix,
        schema=chosen.schema,
    )


def resolve_consumer_reads(
    document: dict,
    *,
    consumer: str,
    resolved: Mapping[str, tuple[ShapeRelation, ...]],
    opening_schemas: Mapping[str, RelationSchema],
) -> tuple[ResolvedRead, ...]:
    """Every upstream relation one consumer reads: its ``sources`` in
    declaration order, then its ``data_enrichment`` lookups in composition
    order.

    This is the one resolution path. The relation registry below builds a
    consumer's propagation schemas from it, the emission route asks it
    which model each read is rendered from, and the estate graph asks it
    which relation an edge carries, so no two of them can resolve one
    source to two different relations."""

    reads: list[ResolvedRead] = []
    for source in document.get("sources") or []:
        reads.append(
            _resolve_read(
                kind=READ_SOURCE,
                consumer=consumer,
                contract_ref=source.get("contract"),
                relation=declared_source_relation(source),
                resolved=resolved,
                opening_schemas=opening_schemas,
            )
        )
    for step in document.get("steps") or []:
        if (step or {}).get("pattern") != "data_enrichment":
            continue
        for lookup in step.get("lookups") or []:
            reads.append(
                _resolve_read(
                    kind=READ_LOOKUP,
                    consumer=consumer,
                    contract_ref=lookup.get("contract"),
                    relation=declared_source_relation(lookup),
                    resolved=resolved,
                    opening_schemas=opening_schemas,
                )
            )
    return tuple(reads)


def consumer_source_schemas(
    document: dict,
    *,
    consumer: str,
    resolved: Mapping[str, tuple[ShapeRelation, ...]],
    opening_schemas: Mapping[str, RelationSchema],
) -> dict[str, RelationSchema]:
    """The schemas one consumer's composition propagates from, keyed the
    way ``propagate_relation_fields`` looks a read up
    (``source_relation_key``): every schema the estate opens with, and the
    one relation each declared read resolves to."""

    schemas = dict(opening_schemas)
    for read in resolve_consumer_reads(
        document, consumer=consumer, resolved=resolved, opening_schemas=opening_schemas
    ):
        schemas[read.key] = read.schema
    return schemas


def resolve_relation_registry(
    entries: Mapping[str, tuple[dict, ValidatedProduct]], *, landing_schemas: Mapping[str, RelationSchema]
) -> dict[str, tuple[ShapeRelation, ...]]:
    """Resolve every product's relations across one estate's worth of
    already layer-1-validated declarations (``entries``: published name to
    ``(document, validated)``), in dependency order. A landing product
    (``validated.profile == "landing"``) takes its field set directly from
    ``landing_schemas``, keyed by its own published name
    (``relation_schema_from_landing_projection``); every other product is
    derived (``derive_shape_relations``) once every contract its
    ``sources`` and ``data_enrichment`` lookups name is available, from
    either ``landing_schemas`` or an already-resolved entry. Each product's
    shape then says which relations that field set publishes. Fails closed
    with ``ContractGenerationError`` on an unsupported shape,
    ``MissingLandingSchemaError`` on a landing product with no supplied
    schema, ``AmbiguousSourceRelationError`` on a source of a producer
    publishing more than one relation and naming none of them,
    ``UnknownSourceRelationError`` on a source naming a relation its
    producer's contract does not list, and ``UnresolvedSourceError`` naming
    every product a full pass makes no progress on (an unresolvable or
    cyclic reference)."""

    resolved: dict[str, tuple[ShapeRelation, ...]] = {}
    remaining = dict(entries)
    while remaining:
        progressed = False
        for published_name in sorted(remaining):
            document, validated = remaining[published_name]

            if validated.profile == "landing":
                schema = landing_schema_for(
                    document, product=published_name, landing_schemas=landing_schemas
                )
                resolved[published_name] = shape_relations_for(
                    document, validated, fields=schema.fields
                )
                del remaining[published_name]
                progressed = True
                continue

            needed = {
                _source_published_name(source.get("contract")) for source in document.get("sources") or []
            }
            needed |= {
                _source_published_name(lookup.get("contract"))
                for step in document.get("steps") or []
                if (step or {}).get("pattern") == "data_enrichment"
                for lookup in step.get("lookups") or []
            }
            available = set(resolved) | set(landing_schemas)
            if not needed <= available:
                continue

            source_schemas = consumer_source_schemas(
                document,
                consumer=published_name,
                resolved=resolved,
                opening_schemas=landing_schemas,
            )
            resolved[published_name] = derive_shape_relations(
                document, validated, source_schemas=source_schemas
            )
            del remaining[published_name]
            progressed = True

        if not progressed:
            unresolved = sorted(remaining)
            raise UnresolvedSourceError(product=", ".join(unresolved), source="<cycle or missing source/lookup>")

    return resolved


# --------------------------------------------------------------------------- quality, versioning, lineage


def build_quality_guarantees(document: dict) -> tuple[QualityGuarantee, ...]:
    """Every quality guarantee the composition's ``data_validation``
    occurrences declare, across every stage, in step order, deduplicated by
    (dimension, description). A reconciliation rule contributes a
    consistency guarantee naming the metric and the upstream contracts it
    reconciles to, so a declared coverage proof is never silently absent
    from the contract."""

    seen: dict[tuple[str, str], None] = {}
    for step in document.get("steps") or []:
        if (step or {}).get("pattern") != "data_validation":
            continue
        for rule in step.get("rules") or []:
            field = rule.get("field")
            entries: list[tuple[str, str]] = []
            if rule.get("not_null") is True:
                entries.append(("completeness", f"{field} must not be null"))
            if "completeness" in rule:
                entries.append(
                    ("completeness", f"{field} completeness must be at least {rule['completeness']}")
                )
            if rule.get("unique") is True:
                entries.append(("uniqueness", f"{field} must be unique"))
            if "min" in rule or "max" in rule:
                bounds = {k: rule[k] for k in ("min", "max") if k in rule}
                entries.append(("conformity", f"{field} must be within range {bounds}"))
            if "allowed_values" in rule:
                entries.append(("conformity", f"{field} must be one of {rule['allowed_values']}"))
            if "regex" in rule:
                entries.append(("conformity", f"{field} must match pattern {rule['regex']!r}"))
            reconcile = rule.get("reconcile")
            if reconcile is not None:
                entries.append(
                    (
                        "consistency",
                        f"{reconcile['metric']} reconciles to {list(reconcile['contracts'])}",
                    )
                )
            for entry in entries:
                seen.setdefault(entry, None)

    return tuple(QualityGuarantee(dimension=dim, description=desc) for dim, desc in seen)


def build_versioning_policy(document: dict) -> str:
    """The product's declared ``schema_publish.versioning_policy`` when
    present, else the engine's own fixed schema-evolution doctrine
    (architecture section 7)."""

    for step in document.get("steps") or []:
        if (step or {}).get("pattern") == "schema_publish":
            declared = step.get("versioning_policy")
            if declared:
                return declared
    return DEFAULT_VERSIONING_POLICY


def build_lineage(document: dict) -> tuple[str, ...]:
    """The upstream contract references this product consumes, in
    declaration order."""

    return tuple(source["contract"] for source in document.get("sources") or [] if source.get("contract"))


def _metadata_capture_step(document: dict) -> dict:
    for step in document.get("steps") or []:
        if (step or {}).get("pattern") == "metadata_capture":
            return step
    return {}


def _major_version(version: object, *, product: str) -> int:
    text = str(version)
    head = text.split(".", 1)[0]
    try:
        return int(head)
    except ValueError:
        raise InvalidVersionError(product=product, version=version) from None


# --------------------------------------------------------------------------- build


def build_product_contract(
    document: dict,
    validated: ValidatedProduct,
    *,
    ownership: EstateOwnership,
    relations: tuple[RelationSchema, ...],
) -> ProductContract:
    """Build the product contract for one already layer-1-validated
    declaration document. Never hand-written: every element is a
    deterministic projection of ``document`` (plus its shape, through
    ``validated``), the estate's declared ownership policy, and
    ``relations`` -- every relation this product's shape renders, already
    resolved (``resolve_relation_registry``); this function never derives a
    schema itself, so a contract can never publish one the engine did not
    actually propagate through the estate. The contract lists every one of
    them (architecture section 7; owner ruling R4)."""

    product_block = document.get("product") or {}
    identity = ProductIdentity(
        namespace=ownership.namespace,
        domain=validated.domain,
        name=validated.name,
        major=_major_version(product_block.get("version"), product=validated.name),
    )
    target = document.get("target") or {}
    contract_block = target.get("contract") or {}
    metadata = _metadata_capture_step(document)

    return ProductContract(
        identity=identity,
        relations=tuple(relations),
        freshness=contract_block.get("freshness"),
        quality=build_quality_guarantees(document),
        versioning_policy=build_versioning_policy(document),
        lineage=build_lineage(document),
        access=contract_block.get("access"),
        support=ownership.support,
        team=ownership.team,
        description=metadata.get("description"),
    )


# --------------------------------------------------------------------------- JSON projection


def _field_document(field: RelationField) -> dict[str, Any]:
    return {"name": field.name, "type": field.type, "required": field.required}


def _relation_document(relation: RelationSchema) -> dict[str, Any]:
    return {"name": relation.name, "fields": [_field_document(f) for f in relation.fields]}


def contract_document(contract: ProductContract) -> dict[str, Any]:
    """The contract's own JSON projection, matching
    ``ergasterion/schemas/product-contract-v1.schema.json``."""

    document: dict[str, Any] = {
        "schema": CONTRACT_SCHEMA,
        "identity": {
            "namespace": contract.identity.namespace,
            "domain": contract.identity.domain,
            "name": contract.identity.name,
            "major": contract.identity.major,
        },
        "relations": [_relation_document(r) for r in contract.relations],
        "versioningPolicy": contract.versioning_policy,
        "support": contract.support,
        "team": contract.team,
    }
    if contract.freshness is not None:
        document["freshness"] = contract.freshness
    if contract.quality:
        document["quality"] = [{"dimension": q.dimension, "description": q.description} for q in contract.quality]
    if contract.lineage:
        document["lineage"] = list(contract.lineage)
    if contract.access is not None:
        document["access"] = contract.access
    if contract.description is not None:
        document["description"] = contract.description
    return document


# --------------------------------------------------------------------------- ODCS / ODPS / compliance-check projection

_ODCS_LOGICAL_TYPES = {
    "string": "string",
    "integer": "integer",
    "boolean": "boolean",
    "date": "date",
    "timestamp": "timestamp",
}


class UnmappedNeutralTypeError(FrameworkError):
    """A field's neutral type has no ODCS logicalType mapping. Unreachable
    for a document that already passed layer-1 validation (the neutral
    type system there is closed to the five scalars, decimal-as-object and
    the estate's own enabled structured types); kept as a named, fail-
    closed failure rather than a silent default for a caller supplying
    unvalidated data directly to this module."""

    code = "unmapped_neutral_type"

    def __init__(self, *, type_value: object) -> None:
        self.type_value = type_value
        super().__init__(f"no ODCS logicalType mapping for neutral type {type_value!r}")


def _odcs_logical_type(type_value: Any) -> str:
    if isinstance(type_value, str):
        try:
            return _ODCS_LOGICAL_TYPES[type_value]
        except KeyError:
            raise UnmappedNeutralTypeError(type_value=type_value) from None
    if isinstance(type_value, dict):
        name = type_value.get("name")
        if name == "decimal":
            return "number"
        if name == "array":
            return "array"
    raise UnmappedNeutralTypeError(type_value=type_value)


def odcs_id(contract: ProductContract, relation: RelationSchema) -> str:
    return f"dpf:{contract.identity.domain}:{relation.name.rsplit('.', 1)[-1]}"


def odps_id(contract: ProductContract) -> str:
    return f"dpf:{contract.identity.domain}:{contract.identity.name}"


def _support_document(contract: ProductContract) -> list[dict[str, Any]]:
    """The declared support channel, and nothing else: no URL is invented
    for it (ODCS does not require one for a ``SupportItem``)."""

    return [{"channel": contract.support}]


def _team_document(contract: ProductContract) -> dict[str, Any]:
    """The declared team name, and nothing else: no description text is
    invented for it."""

    return {"name": contract.team}


def build_odcs_document(contract: ProductContract, relation: RelationSchema) -> dict[str, Any]:
    """One ODCS v3.1 document for one relation the contract's shape
    renders (architecture sections 4, 7)."""

    relation_short_name = relation.name.rsplit(".", 1)[-1]
    properties: list[dict[str, Any]] = []
    for field in relation.fields:
        prop: dict[str, Any] = {"name": field.name, "logicalType": _odcs_logical_type(field.type)}
        if field.required:
            prop["required"] = True
        if field.description is not None:
            prop["description"] = field.description
        properties.append(prop)

    schema_object: dict[str, Any] = {
        "name": relation_short_name,
        "physicalName": relation_short_name,
        "logicalType": "object",
        "physicalType": "table",
        "properties": properties,
    }
    if contract.quality:
        schema_object["quality"] = [
            {
                "type": "custom",
                "engine": "ergasterion",
                "implementation": guarantee.description,
                "dimension": guarantee.dimension,
                "description": guarantee.description,
            }
            for guarantee in contract.quality
        ]

    document: dict[str, Any] = {
        "apiVersion": "v3.1.0",
        "kind": "DataContract",
        "id": odcs_id(contract, relation),
        "version": f"{contract.identity.major}.0.0",
        "status": "active",
        "name": relation_short_name,
        "domain": contract.identity.domain,
        "schema": [schema_object],
        "support": _support_document(contract),
        "team": _team_document(contract),
    }
    if contract.description is not None:
        document["description"] = {"purpose": contract.description}
    custom_properties: list[dict[str, Any]] = []
    if contract.lineage:
        custom_properties.append({"property": "dpf.lineage", "value": list(contract.lineage)})
    if contract.access is not None:
        custom_properties.append({"property": "dpf.access", "value": contract.access})
    if custom_properties:
        document["customProperties"] = custom_properties
    return document


def build_odps_document(contract: ProductContract) -> dict[str, Any]:
    """One ODPS (Bitol) v1.0.0 descriptor for the whole product, one output
    port per relation the shape renders (architecture section 7,
    Metadata Capture at product level). Carries no ``support`` entry: the
    ODPS schema requires a ``url`` on every support channel, and none is
    declared -- omitting the optional key is honest, inventing one is
    not. ``team`` carries the declared name only."""

    version = f"{contract.identity.major}.0.0"
    document: dict[str, Any] = {
        "apiVersion": "v1.0.0",
        "kind": "DataProduct",
        "id": odps_id(contract),
        "name": f"{contract.identity.domain}.{contract.identity.name}",
        "version": version,
        "status": "active",
        "domain": contract.identity.domain,
        "description": {
            "purpose": contract.description or f"{contract.identity.domain}.{contract.identity.name} product.",
        },
        "outputPorts": [
            {
                "name": relation.name.rsplit(".", 1)[-1],
                "version": version,
                "contractId": odcs_id(contract, relation),
            }
            for relation in contract.relations
        ],
        "team": _team_document(contract),
    }
    return document


def build_compliance_check(contract: ProductContract) -> dict[str, Any]:
    """The contract compliance check, as an emitted artefact description
    (architecture section 4: Data Contracts "emits ... its compliance
    check"). Describes what the check verifies; rendering it into an
    executable dbt test is a later item's translator work."""

    return {
        "schema": "ergasterion.contract-compliance/v1",
        "product": f"{contract.identity.domain}.{contract.identity.name}",
        "relations": [relation.name for relation in contract.relations],
        "description": (
            "Every column of each listed relation matches this contract's published schema "
            "(name, type and required-ness); the product is not published if it violates its "
            "own contract."
        ),
    }


# --------------------------------------------------------------------------- schema evolution / consumer compatibility


class ChangeClass(str, Enum):
    """Architecture section 7's schema-evolution classes: additive is
    minor and non-breaking; removals, renames, type changes and new
    required fields are major and breaking."""

    NONE = "none"
    MINOR = "minor"
    MAJOR = "major"


def classify_relation_change(prior: RelationSchema, candidate: RelationSchema) -> tuple[ChangeClass, tuple[str, ...]]:
    """Classify the change from ``prior`` to ``candidate`` (same relation,
    two points in time). Returns the change class and, for a ``MAJOR``
    classification, every field name responsible for it: a removed field, a
    newly added required field, a retyped field, or a field that became
    required."""

    prior_fields = {f.name: f for f in prior.fields}
    candidate_fields = {f.name: f for f in candidate.fields}

    removed = sorted(set(prior_fields) - set(candidate_fields))
    added = sorted(set(candidate_fields) - set(prior_fields))
    added_required = [name for name in added if candidate_fields[name].required]
    retyped = sorted(
        name for name in set(prior_fields) & set(candidate_fields)
        if prior_fields[name].type != candidate_fields[name].type
    )
    newly_required = sorted(
        name for name in set(prior_fields) & set(candidate_fields)
        if candidate_fields[name].required and not prior_fields[name].required
    )

    offending = tuple(removed + added_required + retyped + newly_required)
    if offending:
        return ChangeClass.MAJOR, offending
    if added:
        return ChangeClass.MINOR, ()
    return ChangeClass.NONE, ()


class ConsumerCompatibilityError(FrameworkError):
    """A candidate relation schema breaks a consumer pinned to the
    producer's current major version. Names the consumer, the producer and
    the first offending field; when the change removed a field and added
    at least one other (the shape of a rename), the message also names
    every added field, so a rename reads with both its old and new name
    rather than only the field that disappeared."""

    code = "consumer_compatibility_error"

    def __init__(
        self,
        *,
        consumer: str,
        producer: str,
        field: str,
        change_class: ChangeClass,
        removed: tuple[str, ...] = (),
        added: tuple[str, ...] = (),
    ) -> None:
        self.consumer = consumer
        self.producer = producer
        self.field = field
        self.change_class = change_class
        self.removed = tuple(removed)
        self.added = tuple(added)
        message = (
            f"consumer {consumer!r} of producer {producer!r} is broken by a {change_class.value} "
            f"change to field {field!r}"
        )
        if self.removed and self.added:
            message += f"; removed field(s) {list(self.removed)!r}, new field(s) {list(self.added)!r}"
        super().__init__(message)


def check_consumer_compatibility(
    *, consumer: str, producer: str, prior: RelationSchema, candidate: RelationSchema
) -> None:
    """Fail closed with ``ConsumerCompatibilityError`` when ``candidate``
    is a major (breaking) change from ``prior`` for the same relation. A
    consumer pinned to the producer's major version is unaffected by a
    minor (additive) change and passes silently (architecture section 7:
    "A consumer pinned to a major version is unaffected by minor changes
    and blocked by major ones until it re-declares")."""

    if prior.name != candidate.name:
        raise ConsumerCompatibilityError(
            consumer=consumer, producer=producer, field=candidate.name, change_class=ChangeClass.MAJOR
        )
    change_class, offending = classify_relation_change(prior, candidate)
    if change_class is ChangeClass.MAJOR:
        removed = tuple(sorted(set(f.name for f in prior.fields) - set(f.name for f in candidate.fields)))
        added = tuple(sorted(set(f.name for f in candidate.fields) - set(f.name for f in prior.fields)))
        raise ConsumerCompatibilityError(
            consumer=consumer, producer=producer, field=offending[0], change_class=change_class,
            removed=removed, added=added,
        )


# --------------------------------------------------------------------------- manifest-driven cross-check


class ManifestDisagreementError(FrameworkError):
    """The manifest-driven ODCS route (``ergasterion.emit_contracts.
    build_contract``, reading the compiled dbt manifest) disagrees with the
    declaration-driven contract for the same published table. Once a table
    is declared under ``declarations/products/``, the declaration is the
    single source of truth for its schema; the manifest-driven route
    survives only to prove the two agree."""

    code = "manifest_disagreement"

    def __init__(self, *, table: str, missing: tuple[str, ...], extra: tuple[str, ...]) -> None:
        self.table = table
        self.missing = missing
        self.extra = extra
        super().__init__(
            f"table {table!r}: manifest-driven and declaration-driven schemas disagree "
            f"(missing from manifest: {list(missing)}; extra in manifest: {list(extra)})"
        )


def cross_check_manifest_agreement(
    *, table: str, manifest_fields: frozenset[str], declaration_relation: RelationSchema
) -> None:
    """Fail closed when the manifest-driven contract's column set for
    ``table`` disagrees with the declaration-driven relation's own field
    set. A no-op whenever a served table has no declaration-driven
    counterpart (today's committed estate: none do)."""

    declared_fields = frozenset(f.name for f in declaration_relation.fields)
    if manifest_fields != declared_fields:
        raise ManifestDisagreementError(
            table=table,
            missing=tuple(sorted(declared_fields - manifest_fields)),
            extra=tuple(sorted(manifest_fields - declared_fields)),
        )
