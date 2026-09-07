"""The product graph of an estate (architecture sections 4, 8, 10 and 12).

Nodes are products. Edges are contract dependencies. The graph is acyclic,
ordered topologically, and emitted as an artefact suite: node and edge
tables, product-level and field-level lineage, the validation occurrences
the compositions declare, and one description document tying them
together. Every fact in every artefact is derived from the product
declarations under an estate's ``declarations/products/`` tree; nothing is
inferred from a table name, a file path, a model tree or any modelling
vocabulary a particular shape happens to use.

Ordering and generations. The order is Kahn's algorithm over the contract
edges, taking every currently unblocked product in published-name order
before moving on, so one estate always produces one order. A product's
generation is depth, defined structurally from the declaration's own
``sources`` block (architecture section 8):

  * ``landing``: the product resolves to the landing profile. It consumes
    no upstream contract; it is where the estate's data enters.
  * ``first``: every source contract is published by a landing product.
  * ``later``: at least one source contract is published by a non-landing
    product.
  * ``consolidating``: two or more source contracts are published by
    non-landing products.

A ``data_enrichment`` lookup is a contract dependency and therefore an
edge, so the referenced product is always built first, but it is a
reference read rather than a source edge and never changes a generation
label. A non-landing product that declares no source at all has no
generation and fails closed rather than being labelled by default.

Fail-closed rules. A source or lookup contract no product in the estate
publishes fails closed naming the consumer and the reference. A cycle
fails closed naming every product in it. A reconciliation rule naming a
contract the product does not consume fails closed naming the product, the
occurrence and the reference. An auxiliary relation registered for an
unknown product, or one colliding with a published relation name, fails
closed naming both. The finished graph is checked for coverage before any
artefact is serialised, so an artefact never publishes a partial fact.

Lineage. Product-level lineage is the edge set: product to product through
the contract each edge names, and relation-level within it: each source or
lookup edge names, in full, the one relation of the producer's contract the
consumer reads (architecture section 7), resolved from that read's own
``relation`` key. A read of a producer publishing several relations and
naming none fails closed, and so does one naming a relation the producer
does not publish.

Field-level lineage is derived from the four declaration mechanisms that
create or carry a field:

  * ``schema_transform``: the mapping's ``from`` field becomes its ``to``
    field;
  * ``calculated_fields``: an inline expression's own resolved column
    references (the parser reports them) become the calculated field, and
    a named-rule field records the rule and version it is computed by;
    an expression that reads no column records the expression text itself,
    so a constant field is never absent from the lineage;
  * ``data_enrichment``: each looked-up field is carried in from the
    contract the lookup names;
  * ``data_aggregation``: grain and group fields are carried through, and
    each aggregate's expression columns become the aggregate, or the
    expression text when it reads no column.

Every field a composition declares as an output is checked against that
lineage before any artefact is written, so a declared field can never go
missing from the emitted set.

Verification hooks (architecture section 12). A product whose
``checkpointing`` block declares ``checkpoint: true`` forces table
materialisation intent and every relation it publishes is registered in
the emitted graph. A product's published relation names come from its
shape, applied to its declared domain and name, so they are stable across
re-emission by construction.
Physical layout beyond the checkpoint intent is estate and adapter policy,
not a product declaration concern, so a product without the flag carries
the ``undeclared`` intent rather than a guessed one.

Auxiliary relations. A translator may render private relations for a named
rule or a staged computation under the product's namespace. They are
excluded from the product's contract and registered here as auxiliary
lineage (architecture section 10). ``AuxiliaryRelationRegistry`` is the
registration API a translator calls;
``collect_auxiliary_relations`` gathers the registrations of every
translator an estate names.
"""

from __future__ import annotations

import csv
import io
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping, Protocol, Sequence

import yaml

from ergasterion.framework.contract import (
    READ_LOOKUP,
    READ_SOURCE,
    declared_source_relation,
    dump_json,
    resolve_relation_name,
)
from ergasterion.framework.declaration import (
    addressable_relations,
    physical_declaration,
    SOURCE_KIND_FIXTURE as DECLARED_FIXTURE_SOURCE_KIND,
    SOURCE_CONFORM_KEY,
    EstatePolicy,
    ValidatedProduct,
    conformance_mapping,
    declared_combination,
    validate_declaration,
    validate_estate_products,
)
from ergasterion.framework.expressions import parse_expression
from ergasterion.framework.models import FrameworkError, PatternId
from ergasterion.framework.shapes import get_shape

GRAPH_SCHEMA = "ergasterion.product-graph/v1"

# The owner an auxiliary relation carries when it belongs to the estate
# rather than to one product: a translator-private relation several
# products read and none of them owns, for example the one time spine a
# dbt project carries per granularity. A published name is always
# ``<domain>.<name>``, so a value with no dot in it can never collide with
# one, and the relation itself is still refused if a product publishes it.
ESTATE_SCOPE = "estate"

# Generation labels (architecture section 8). Depth, never a type.
GENERATION_LANDING = "landing"
GENERATION_FIRST = "first"
GENERATION_LATER = "later"
GENERATION_CONSOLIDATING = "consolidating"
GENERATIONS: tuple[str, ...] = (
    GENERATION_LANDING,
    GENERATION_FIRST,
    GENERATION_LATER,
    GENERATION_CONSOLIDATING,
)

# Materialisation intent. Only the checkpoint flag declares one today
# (architecture section 12); everything else about physical layout is
# adapter convention and estate policy, so it is named as undeclared here
# rather than guessed.
MATERIALISATION_TABLE = "table"
MATERIALISATION_UNDECLARED = "undeclared"

# Edge kinds.
EDGE_SOURCE = "source"
EDGE_LOOKUP = "lookup"
EDGE_AUXILIARY = "auxiliary"

# Field-lineage source kinds.
SOURCE_KIND_FIELD = "field"
SOURCE_KIND_CONTRACT = "contract"
SOURCE_KIND_RULE = "rule"
# An inline expression that reads no column at all -- a constant, or an
# aggregate over the whole relation -- still produced its field, so its
# lineage record carries the expression itself as the source.
SOURCE_KIND_EXPRESSION = "expression"

# Validation occurrence kinds.
VALIDATION_FIELD = "field"
VALIDATION_RECONCILIATION = "reconciliation"

LANDING_PROFILE = "landing"


# --------------------------------------------------------------------------- errors


class ProductGraphError(FrameworkError):
    """Base for every product-graph resolution failure."""

    code = "product_graph_error"


class MissingUpstreamProductError(ProductGraphError):
    """A product consumes a contract no product in the estate publishes.
    Names the consumer, the exact contract reference as declared, and the
    published name it resolves to."""

    code = "missing_upstream_product"

    def __init__(self, *, consumer: str, contract: str, upstream: str) -> None:
        self.consumer = consumer
        self.contract = contract
        self.upstream = upstream
        super().__init__(
            f"product {consumer!r} consumes contract {contract!r}, but no product in this estate "
            f"publishes {upstream!r}"
        )


class ProductGraphCycleError(ProductGraphError):
    """The contract edges form a cycle. Names every product in it."""

    code = "product_graph_cycle"

    def __init__(self, products: tuple[str, ...]) -> None:
        self.products = products
        super().__init__(
            "the product graph is not acyclic: no build order exists for "
            f"{', '.join(products)}"
        )


class UnclassifiedGenerationError(ProductGraphError):
    """A non-landing product declares no source contract, so it has no
    generation. Names the product and its profile."""

    code = "unclassified_generation"

    def __init__(self, *, product: str, profile: str) -> None:
        self.product = product
        self.profile = profile
        super().__init__(
            f"product {product!r} resolves to profile {profile!r} and declares no source contract, "
            "so its generation cannot be derived"
        )


class ReconciliationReferenceError(ProductGraphError):
    """A reconciliation rule references a contract the product does not
    consume. Names the product, the occurrence and the reference."""

    code = "reconciliation_reference"

    def __init__(self, *, product: str, occurrence: str, contract: str, declared: tuple[str, ...]) -> None:
        self.product = product
        self.occurrence = occurrence
        self.contract = contract
        self.declared = declared
        super().__init__(
            f"product {product!r} occurrence {occurrence!r}: reconciliation rule references contract "
            f"{contract!r}, which this product does not consume; declared sources: {list(declared)!r}"
        )


class AuxiliaryRelationError(ProductGraphError):
    """An auxiliary relation registration cannot be placed in the graph:
    the product is not in the estate, or the relation name collides with a
    published relation."""

    code = "auxiliary_relation"

    def __init__(self, *, product: str, relation: str, detail: str) -> None:
        self.product = product
        self.relation = relation
        self.detail = detail
        super().__init__(
            f"auxiliary relation {relation!r} registered for product {product!r}: {detail}"
        )


class ProductGraphCoverageError(ProductGraphError):
    """A finished graph or its description does not cover what the
    declarations state. Names exactly what is missing."""

    code = "product_graph_coverage"

    def __init__(self, detail: str) -> None:
        self.detail = detail
        super().__init__(f"product graph coverage failed: {detail}")


# --------------------------------------------------------------------------- typed graph


@dataclass(frozen=True)
class ProductNode:
    """One product in the estate graph."""

    published_name: str
    name: str
    domain: str
    layer: str
    profile: str
    shape: str
    generation: str
    relations: tuple[str, ...]
    materialisation: str
    checkpoint: bool


@dataclass(frozen=True)
class ProductEdge:
    """One contract dependency between two products, or one translator
    -private auxiliary relation attached to its product.

    ``relation`` is relation-level: on a source or lookup edge it is the
    one relation of the producer's contract the consumer reads, resolved
    from that read's own ``relation`` key through the contract pipe's
    single resolver (architecture section 7), not the set of everything the
    producer publishes. It is the relation's full published name: an
    artefact names a relation in full, while a declaration names it as its
    producer does."""

    source: str
    target: str
    kind: str
    contract: str
    relation: str


@dataclass(frozen=True)
class FieldLineageEdge:
    """One field-to-field lineage fact derived from one occurrence."""

    product: str
    target_field: str
    source_kind: str
    source_product: str
    source: str
    occurrence: str
    transform: str


@dataclass(frozen=True)
class StoredField:
    """One published column whose declaration states the name it is stored
    under, beside the logical name the field lineage carries it by. The
    lineage rows name a column by its logical name everywhere, because that
    is the name the composition and every consumer compose with; this is
    where a reader resolves that logical name to the name the built relation
    actually carries."""

    product: str
    relation: str
    field: str
    physical_name: str


@dataclass(frozen=True)
class StoredRelation:
    """One published relation whose declaration states the schema and table
    it is stored under."""

    product: str
    relation: str
    physical_schema: str | None
    physical_name: str | None


@dataclass(frozen=True)
class DeclaredField:
    """One field a composition declares as an output of one occurrence.
    Read alongside the field lineage: coverage requires every declared
    field to carry at least one lineage record, so an occurrence can never
    contribute a field the emitted lineage does not explain."""

    product: str
    field: str
    occurrence: str


@dataclass(frozen=True)
class ValidationOccurrence:
    """One rule of one ``data_validation`` occurrence, rendered as the
    graph's own validation record. A reconciliation rule carries the
    upstream contracts it reconciles to (architecture section 8)."""

    product: str
    occurrence: str
    stage: str
    kind: str
    subject: str
    references: tuple[str, ...] = ()


@dataclass(frozen=True)
class AuxiliaryRelation:
    """One translator-private relation rendered under a product's
    namespace, excluded from the product's contract and registered as
    auxiliary lineage (architecture section 10)."""

    product: str
    relation: str
    translator: str
    purpose: str


@dataclass(frozen=True)
class ProductGraph:
    """One estate's resolved product graph. ``nodes`` are in topological
    order: that order is the build order and the order every artefact
    publishes."""

    nodes: tuple[ProductNode, ...]
    edges: tuple[ProductEdge, ...]
    field_lineage: tuple[FieldLineageEdge, ...]
    validations: tuple[ValidationOccurrence, ...]
    declared_fields: tuple[DeclaredField, ...] = ()
    auxiliary: tuple[AuxiliaryRelation, ...] = ()
    stored_relations: tuple[StoredRelation, ...] = ()
    stored_fields: tuple[StoredField, ...] = ()

    def order(self) -> tuple[str, ...]:
        """Every product's published name in topological order."""

        return tuple(node.published_name for node in self.nodes)

    def generations(self) -> dict[str, list[str]]:
        """Each generation label mapped to its member products, in
        topological order. Every declared label is present, empty or
        not, so a reader never has to guess whether a missing key means
        an empty generation or an unsupported one."""

        buckets: dict[str, list[str]] = {label: [] for label in GENERATIONS}
        for node in self.nodes:
            buckets[node.generation].append(node.published_name)
        return buckets

    def checkpoint_relations(self) -> tuple[str, ...]:
        """Every relation a checkpoint flag registered, in topological
        order."""

        return tuple(
            relation for node in self.nodes if node.checkpoint for relation in node.relations
        )


# --------------------------------------------------------------------------- auxiliary registration


class AuxiliaryRelationProvider(Protocol):
    """What ``collect_auxiliary_relations`` needs of a translator: the
    private relations it renders under a product's namespace."""

    def auxiliary_relations(self) -> tuple[AuxiliaryRelation, ...]:
        ...


class AuxiliaryRelationRegistry:
    """The registration API a translator uses to declare a private
    auxiliary relation. Registrations are keyed by (product, relation):
    registering the same relation twice for the same product is only
    accepted when both registrations agree in every field, so two
    translators cannot silently claim one relation."""

    def __init__(self) -> None:
        self._entries: dict[tuple[str, str], AuxiliaryRelation] = {}

    def register(self, *, product: str, relation: str, translator: str, purpose: str) -> AuxiliaryRelation:
        """Register one auxiliary relation and return it. Every field must
        be a non-empty string."""

        for label, value in (
            ("product", product),
            ("relation", relation),
            ("translator", translator),
            ("purpose", purpose),
        ):
            if not isinstance(value, str) or not value:
                raise AuxiliaryRelationError(
                    product=str(product),
                    relation=str(relation),
                    detail=f"{label} must be a non-empty string, got {value!r}",
                )
        entry = AuxiliaryRelation(product=product, relation=relation, translator=translator, purpose=purpose)
        existing = self._entries.get((product, relation))
        if existing is not None and existing != entry:
            raise AuxiliaryRelationError(
                product=product,
                relation=relation,
                detail=(
                    f"already registered by translator {existing.translator!r} for purpose "
                    f"{existing.purpose!r}; {translator!r} cannot claim it as {purpose!r}"
                ),
            )
        self._entries[(product, relation)] = entry
        return entry

    def relations(self) -> tuple[AuxiliaryRelation, ...]:
        """Every registration, ordered by product then relation."""

        return tuple(self._entries[key] for key in sorted(self._entries))

    def __len__(self) -> int:
        return len(self._entries)


def collect_auxiliary_relations(
    providers: Iterable[AuxiliaryRelationProvider],
) -> AuxiliaryRelationRegistry:
    """Ask every supplied translator for the private relations it renders
    and return one registry carrying all of them. A translator that
    renders none contributes nothing; the registry is still returned, so
    the caller never branches on whether any translator answered."""

    registry = AuxiliaryRelationRegistry()
    for provider in providers:
        for entry in provider.auxiliary_relations():
            registry.register(
                product=entry.product,
                relation=entry.relation,
                translator=entry.translator,
                purpose=entry.purpose,
            )
    return registry


# --------------------------------------------------------------------------- declaration reading


def published_name(validated: ValidatedProduct) -> str:
    """A product's published name: its declared domain and name. The one
    identity the graph, the contract and every consumer reference use."""

    return f"{validated.domain}.{validated.name}"


def published_relation_names(validated: ValidatedProduct, document: dict) -> tuple[str, ...]:
    """Every relation a product publishes, derived from its declaration
    through its shape (``ergasterion.framework.shapes``, the one owner of
    what a shape renders). Stable across re-emission by construction: the
    names are a projection of the declared shape, its target section, the
    domain and the name, and of nothing else (architecture section 12's
    stable relation names). A product naming a shape the registry does not
    carry fails closed with ``UnknownShapeError`` rather than publishing a
    guessed relation."""

    return get_shape(validated.shape).relation_names(
        domain=validated.domain,
        name=validated.name,
        shape_config=(document.get("target") or {}).get("shape_config") or {},
    )


def _contract_target(reference: object) -> str:
    """The published name a ``<domain>.<name>@<major>`` contract reference
    resolves to."""

    return str(reference).split("@", 1)[0]


def load_product_entries(
    products_dir: Path, *, policy: EstatePolicy
) -> dict[str, tuple[dict, ValidatedProduct]]:
    """Load and validate every product declaration under ``products_dir``
    (recursively, sorted for determinism), keyed by published name.

    Estate-level validation, including the rule that no two declarations
    claim one published name, has exactly one owner
    (``ergasterion.framework.declaration.validate_estate_products``); this
    loader calls it and then reads the documents it needs, rather than
    carrying a second copy of that rule. A missing ``products_dir`` yields
    no products: an estate with none declared yet is valid."""

    validate_estate_products(products_dir, policy=policy)
    if not products_dir.is_dir():
        return {}
    paths = sorted(set(products_dir.rglob("*.yml")) | set(products_dir.rglob("*.yaml")))
    entries: dict[str, tuple[dict, ValidatedProduct]] = {}
    for path in paths:
        document = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        validated = validate_declaration(document, policy=policy)
        entries[published_name(validated)] = (document, validated)
    return entries


def _source_entries(document: dict) -> tuple[tuple[str, str | None, bool], ...]:
    """Every source's contract reference with the relation it declares it
    reads (``None`` where it declares none) and whether it is fixture-bound
    (architecture section 12). A fixture-bound source names a contract no
    product in this estate publishes, so it is an entry point into the
    graph rather than an edge within it: it carries no producer node to
    draw an edge from, and for generation it stands exactly where a
    landing product's contract stands."""

    return tuple(
        (
            str(source["contract"]),
            declared_source_relation(source),
            source.get("kind") == DECLARED_FIXTURE_SOURCE_KIND,
        )
        for source in document.get("sources") or []
        if isinstance(source, dict) and source.get("contract")
    )


def _source_references(document: dict) -> tuple[str, ...]:
    """Every source contract reference published by a product in this
    estate: the fixture-bound ones are entry points, not edges."""

    return tuple(
        reference
        for reference, _relation, is_fixture in _source_entries(document)
        if not is_fixture
    )


def _declared_source_relations(document: dict) -> tuple[tuple[str, str | None], ...]:
    """Every source edge's contract reference with the relation the source
    declares it reads, in declaration order."""

    return tuple(
        (reference, relation)
        for reference, relation, is_fixture in _source_entries(document)
        if not is_fixture
    )


def _lookup_references(document: dict) -> tuple[tuple[str, str, str | None], ...]:
    """Every ``data_enrichment`` lookup's contract reference with the
    occurrence tag that declared it and the relation it declares it reads
    (``None`` where it declares none)."""

    found: list[tuple[str, str, str | None]] = []
    for index, step in enumerate(document.get("steps") or []):
        if (step or {}).get("pattern") != PatternId.DATA_ENRICHMENT.value:
            continue
        occurrence = f"steps[{index}]:{PatternId.DATA_ENRICHMENT.value}"
        for lookup in step.get("lookups") or []:
            reference = (lookup or {}).get("contract")
            if reference:
                found.append((occurrence, str(reference), declared_source_relation(lookup)))
    return tuple(found)


def _checkpoint_declared(document: dict) -> bool:
    checkpointing = document.get("checkpointing")
    return isinstance(checkpointing, dict) and checkpointing.get("checkpoint") is True


# --------------------------------------------------------------------------- build


def _edges_for(
    entries: Mapping[str, tuple[dict, ValidatedProduct]],
    relations: Mapping[str, tuple[str, ...]],
) -> tuple[ProductEdge, ...]:
    edges: list[ProductEdge] = []
    for consumer in sorted(entries):
        document, _validated = entries[consumer]
        for reference, declared in _declared_source_relations(document):
            upstream = _contract_target(reference)
            if upstream not in entries:
                raise MissingUpstreamProductError(consumer=consumer, contract=reference, upstream=upstream)
            edges.append(
                ProductEdge(
                    source=upstream,
                    target=consumer,
                    kind=EDGE_SOURCE,
                    contract=reference,
                    relation=resolve_relation_name(
                        relations[upstream],
                        consumer=consumer,
                        producer=upstream,
                        relation=declared,
                        read=READ_SOURCE,
                    ),
                )
            )
        for _occurrence, reference, declared in _lookup_references(document):
            upstream = _contract_target(reference)
            if upstream not in entries:
                raise MissingUpstreamProductError(consumer=consumer, contract=reference, upstream=upstream)
            edges.append(
                ProductEdge(
                    source=upstream,
                    target=consumer,
                    kind=EDGE_LOOKUP,
                    contract=reference,
                    relation=resolve_relation_name(
                        relations[upstream],
                        consumer=consumer,
                        producer=upstream,
                        relation=declared,
                        read=READ_LOOKUP,
                    ),
                )
            )
    return tuple(sorted(edges, key=lambda e: (e.source, e.target, e.kind, e.contract)))


def _topological_order(products: Iterable[str], edges: Iterable[ProductEdge]) -> tuple[str, ...]:
    remaining = set(products)
    blockers: dict[str, set[str]] = {name: set() for name in remaining}
    for edge in edges:
        # A product that consumes itself blocks itself, which is exactly the
        # one-product cycle: it is reported by the same rule as any longer one.
        blockers[edge.target].add(edge.source)
    order: list[str] = []
    placed: set[str] = set()
    while remaining:
        ready = sorted(name for name in remaining if blockers[name] <= placed)
        if not ready:
            raise ProductGraphCycleError(tuple(sorted(remaining)))
        order.extend(ready)
        placed.update(ready)
        remaining.difference_update(ready)
    return tuple(order)


def _generation_of(
    published: str,
    document: dict,
    validated: ValidatedProduct,
    entries: Mapping[str, tuple[dict, ValidatedProduct]],
) -> str:
    if validated.profile == LANDING_PROFILE:
        return GENERATION_LANDING
    entries_declared = _source_entries(document)
    if not entries_declared:
        raise UnclassifiedGenerationError(product=published, profile=validated.profile)
    # A fixture-bound source stands where a landing product's contract
    # stands: it is where data enters the estate, so it never lifts a
    # product out of the first generation.
    non_landing = [
        _contract_target(reference)
        for reference, _relation, is_fixture in entries_declared
        if not is_fixture and entries[_contract_target(reference)][1].profile != LANDING_PROFILE
    ]
    if len(non_landing) >= 2:
        return GENERATION_CONSOLIDATING
    if len(non_landing) == 1:
        return GENERATION_LATER
    return GENERATION_FIRST


def _stored_names_for(
    published: str, document: dict, relation_names: Sequence[str]
) -> tuple[list[StoredRelation], list[StoredField]]:
    """The names one product's declaration states its relations and columns
    are stored under. A declaration that states none produces nothing, so
    the graph of an estate that renames nothing is exactly the graph it was.
    ``ergasterion.framework.declaration.physical_declaration`` is the one
    reader of the block; this places each entry on the relation it
    addresses."""

    declared = physical_declaration(document, product=published)
    if not declared:
        return [], []
    addressable = addressable_relations(published, relation_names)
    relations: list[StoredRelation] = []
    fields: list[StoredField] = []
    for key, entry in declared.items():
        relation = addressable.get(key)
        if relation is None:
            # The rules have already refused a key the shape does not
            # publish; reaching one here would mean the graph was built from
            # declarations nothing validated.
            raise ProductGraphCoverageError(
                f"product {published!r} states a stored name for relation {key!r}, which its "
                f"shape does not publish: {sorted(addressable)!r}"
            )
        if entry.name is not None or entry.schema is not None:
            relations.append(
                StoredRelation(
                    product=published,
                    relation=relation,
                    physical_schema=entry.schema,
                    physical_name=entry.name,
                )
            )
        for logical, physical in entry.fields:
            fields.append(
                StoredField(
                    product=published,
                    relation=relation,
                    field=logical,
                    physical_name=str(physical),
                )
            )
    return relations, fields


def _composition_fields_for(
    published: str, document: dict
) -> tuple[tuple[FieldLineageEdge, ...], tuple[DeclaredField, ...]]:
    """Every field-lineage record one product's composition states, with
    the list of fields that composition declares as outputs. The two are
    returned together so coverage can require one to explain the other."""

    lineage: list[FieldLineageEdge] = []
    declared: list[DeclaredField] = []

    def record(
        *, target: str, source_kind: str, source_product: str, source: str, occurrence: str, transform: str
    ) -> None:
        lineage.append(
            FieldLineageEdge(
                product=published,
                target_field=target,
                source_kind=source_kind,
                source_product=source_product,
                source=source,
                occurrence=occurrence,
                transform=transform,
            )
        )

    def from_expression(*, target: str, text: str, occurrence: str, transform: str) -> None:
        """An inline expression's lineage: one record per column the parser
        resolved, or, when it resolves none (a constant, or an aggregate
        over the whole relation), one record carrying the expression
        itself. A declared field always leaves at least one record."""

        parsed = parse_expression(text, product=published, occurrence=occurrence)
        if parsed.columns:
            for column in parsed.columns:
                record(
                    target=target,
                    source_kind=SOURCE_KIND_FIELD,
                    source_product=published,
                    source=column,
                    occurrence=occurrence,
                    transform=transform,
                )
            return
        record(
            target=target,
            source_kind=SOURCE_KIND_EXPRESSION,
            source_product=published,
            source=parsed.text,
            occurrence=occurrence,
            transform=transform,
        )

    # The conformance a source of a combination declares is a rename at the
    # read itself: the field arrives from the upstream contract under one
    # name and enters this product's relation under another, so it leaves
    # the same lineage a rename inside the composition would.
    combination = declared_combination(document)
    for index, source in enumerate(document.get("sources") or []):
        if not isinstance(source, dict):
            continue
        is_fixture = source.get("kind") == DECLARED_FIXTURE_SOURCE_KIND
        upstream = _contract_target(source.get("contract"))
        for entry in conformance_mapping(source):
            record(
                target=str(entry["to"]),
                source_kind=SOURCE_KIND_FIELD if is_fixture else SOURCE_KIND_CONTRACT,
                source_product=published if is_fixture else upstream,
                source=str(entry["from"]),
                occurrence=f"sources[{index}].{SOURCE_CONFORM_KEY}",
                transform=str((combination or {}).get("method") or SOURCE_CONFORM_KEY),
            )

    for index, step in enumerate(document.get("steps") or []):
        pattern = (step or {}).get("pattern")
        occurrence = f"steps[{index}]:{pattern}"

        if pattern == PatternId.SCHEMA_TRANSFORM.value:
            for mapping in step.get("mapping") or []:
                if mapping.get("to") is None:
                    # A drop names no target field, so it declares no output
                    # and contributes no lineage: the dropped column simply
                    # stops being carried from this occurrence onward.
                    continue
                target = str(mapping["to"])
                declared.append(DeclaredField(product=published, field=target, occurrence=occurrence))
                record(
                    target=target,
                    source_kind=SOURCE_KIND_FIELD,
                    source_product=published,
                    source=str(mapping["from"]),
                    occurrence=occurrence,
                    transform=PatternId.SCHEMA_TRANSFORM.value,
                )

        elif pattern == PatternId.CALCULATED_FIELDS.value:
            for field_entry in step.get("fields") or []:
                name = str(field_entry["name"])
                declared.append(DeclaredField(product=published, field=name, occurrence=occurrence))
                expression = field_entry.get("expression")
                if expression is not None:
                    from_expression(
                        target=name,
                        text=str(expression),
                        occurrence=occurrence,
                        transform=PatternId.CALCULATED_FIELDS.value,
                    )
                    continue
                rule = str(field_entry["rule"])
                version = field_entry.get("rule_version")
                record(
                    target=name,
                    source_kind=SOURCE_KIND_RULE,
                    source_product=published,
                    source=rule if version is None else f"{rule}@{version}",
                    occurrence=occurrence,
                    transform=PatternId.CALCULATED_FIELDS.value,
                )

        elif pattern == PatternId.DATA_ENRICHMENT.value:
            for lookup in step.get("lookups") or []:
                upstream = _contract_target(lookup["contract"])
                for field_name in lookup.get("fields") or []:
                    target = str(field_name)
                    declared.append(DeclaredField(product=published, field=target, occurrence=occurrence))
                    record(
                        target=target,
                        source_kind=SOURCE_KIND_CONTRACT,
                        source_product=upstream,
                        source=target,
                        occurrence=occurrence,
                        transform=PatternId.DATA_ENRICHMENT.value,
                    )

        elif pattern == PatternId.DATA_AGGREGATION.value:
            for carried in list(step.get("grain") or []) + list(step.get("groups") or []):
                target = str(carried)
                declared.append(DeclaredField(product=published, field=target, occurrence=occurrence))
                record(
                    target=target,
                    source_kind=SOURCE_KIND_FIELD,
                    source_product=published,
                    source=target,
                    occurrence=occurrence,
                    transform=PatternId.DATA_AGGREGATION.value,
                )
            for aggregate in step.get("aggregates") or []:
                target = str(aggregate["name"])
                declared.append(DeclaredField(product=published, field=target, occurrence=occurrence))
                from_expression(
                    target=target,
                    text=str(aggregate["expression"]),
                    occurrence=occurrence,
                    transform=PatternId.DATA_AGGREGATION.value,
                )

    return tuple(lineage), tuple(declared)


def _validations_for(published: str, document: dict) -> tuple[ValidationOccurrence, ...]:
    declared_sources = _source_references(document)
    records: list[ValidationOccurrence] = []
    for index, step in enumerate(document.get("steps") or []):
        if (step or {}).get("pattern") != PatternId.DATA_VALIDATION.value:
            continue
        occurrence = f"steps[{index}]:{PatternId.DATA_VALIDATION.value}"
        stage = str(step.get("stage") or "")
        for rule in step.get("rules") or []:
            reconcile = (rule or {}).get("reconcile")
            if reconcile is None:
                records.append(
                    ValidationOccurrence(
                        product=published,
                        occurrence=occurrence,
                        stage=stage,
                        kind=VALIDATION_FIELD,
                        subject=str(rule["field"]),
                    )
                )
                continue
            references = tuple(str(reference) for reference in reconcile.get("contracts") or [])
            for reference in references:
                if reference not in declared_sources:
                    raise ReconciliationReferenceError(
                        product=published,
                        occurrence=occurrence,
                        contract=reference,
                        declared=declared_sources,
                    )
            records.append(
                ValidationOccurrence(
                    product=published,
                    occurrence=occurrence,
                    stage=stage,
                    kind=VALIDATION_RECONCILIATION,
                    subject=str(reconcile["metric"]),
                    references=references,
                )
            )
    return tuple(records)


def build_product_graph(
    entries: Mapping[str, tuple[dict, ValidatedProduct]],
    *,
    auxiliary: AuxiliaryRelationRegistry | None = None,
) -> ProductGraph:
    """Resolve one estate's product graph from its validated product
    declarations, keyed by published name. Fails closed on a source or
    lookup contract no product publishes, on a cycle, on a non-landing
    product with no source, on a reconciliation rule naming a contract the
    product does not consume, and on an auxiliary relation that cannot be
    placed. The returned graph has already passed
    ``assert_product_graph_coverage``."""

    for key, (_document, validated) in entries.items():
        derived = published_name(validated)
        if key != derived:
            raise ProductGraphCoverageError(
                f"entry key {key!r} does not match the declaration's published name {derived!r}"
            )

    relations = {
        name: published_relation_names(validated, document)
        for name, (document, validated) in entries.items()
    }
    edges = list(_edges_for(entries, relations))
    order = _topological_order(entries.keys(), edges)

    nodes: list[ProductNode] = []
    field_lineage: list[FieldLineageEdge] = []
    declared_fields: list[DeclaredField] = []
    validations: list[ValidationOccurrence] = []
    stored_relations: list[StoredRelation] = []
    stored_fields: list[StoredField] = []
    for name in order:
        document, validated = entries[name]
        checkpoint = _checkpoint_declared(document)
        nodes.append(
            ProductNode(
                published_name=name,
                name=validated.name,
                domain=validated.domain,
                layer=validated.layer,
                profile=validated.profile,
                shape=validated.shape,
                generation=_generation_of(name, document, validated, entries),
                relations=relations[name],
                materialisation=MATERIALISATION_TABLE if checkpoint else MATERIALISATION_UNDECLARED,
                checkpoint=checkpoint,
            )
        )
        product_lineage, product_declared = _composition_fields_for(name, document)
        field_lineage.extend(product_lineage)
        declared_fields.extend(product_declared)
        validations.extend(_validations_for(name, document))
        product_relations, product_fields = _stored_names_for(name, document, relations[name])
        stored_relations.extend(product_relations)
        stored_fields.extend(product_fields)

    registrations = auxiliary.relations() if auxiliary is not None else ()
    published_relations = {relation for names in relations.values() for relation in names}
    for entry in registrations:
        if entry.product != ESTATE_SCOPE and entry.product not in entries:
            raise AuxiliaryRelationError(
                product=entry.product,
                relation=entry.relation,
                detail="no product with that published name is declared in this estate",
            )
        if entry.relation in published_relations:
            raise AuxiliaryRelationError(
                product=entry.product,
                relation=entry.relation,
                detail="the name is already a published relation, and an auxiliary relation is private",
            )
        if entry.product == ESTATE_SCOPE:
            # An estate-scoped relation belongs to no product, so there is no
            # node to hang an edge from. It is carried in this graph's
            # auxiliary set, which the emission route resolves with the SQL
            # translator's own registrations and fails closed on. The emitted
            # graph artefacts are written by a route that asks only the
            # publication translator, so they list no auxiliary relation at
            # all, this one included.
            continue
        edges.append(
            ProductEdge(
                source=entry.product,
                target=entry.product,
                kind=EDGE_AUXILIARY,
                contract=entry.purpose,
                relation=entry.relation,
            )
        )

    graph = ProductGraph(
        nodes=tuple(nodes),
        edges=tuple(sorted(edges, key=lambda e: (e.source, e.target, e.kind, e.contract, e.relation))),
        field_lineage=tuple(field_lineage),
        validations=tuple(validations),
        declared_fields=tuple(declared_fields),
        auxiliary=registrations,
        stored_relations=tuple(stored_relations),
        stored_fields=tuple(stored_fields),
    )
    assert_product_graph_coverage(graph)
    return graph


# --------------------------------------------------------------------------- coverage


def assert_product_graph_coverage(graph: ProductGraph) -> None:
    """Fail closed when a finished graph would publish a partial fact:
    a duplicated product, a node with no relation or no known generation or
    materialisation intent, a checkpointed node that does not force a
    table, an edge whose endpoint is not a node or that carries no
    relation, and a lineage or validation record attached to a product the
    graph does not carry, or one missing a field name, and any field the
    composition declares as an output that the emitted lineage does not
    explain. Placing an auxiliary relation has its own owner
    (``build_product_graph`` raises ``AuxiliaryRelationError`` naming the
    registration), so it is not re-checked here."""

    names = [node.published_name for node in graph.nodes]
    node_ids = set(names)
    if len(names) != len(node_ids):
        duplicated = sorted({name for name in names if names.count(name) > 1})
        raise ProductGraphCoverageError(f"duplicate product node(s): {duplicated}")

    for node in graph.nodes:
        if not node.relations or not all(node.relations):
            raise ProductGraphCoverageError(f"product {node.published_name!r} publishes no relation name")
        if node.generation not in GENERATIONS:
            raise ProductGraphCoverageError(
                f"product {node.published_name!r} carries generation {node.generation!r}, "
                f"which is not one of {list(GENERATIONS)}"
            )
        if node.materialisation not in (MATERIALISATION_TABLE, MATERIALISATION_UNDECLARED):
            raise ProductGraphCoverageError(
                f"product {node.published_name!r} carries materialisation intent {node.materialisation!r}"
            )
        if node.checkpoint and node.materialisation != MATERIALISATION_TABLE:
            raise ProductGraphCoverageError(
                f"product {node.published_name!r} declares a checkpoint but does not force table materialisation"
            )

    for edge in graph.edges:
        for endpoint in (edge.source, edge.target):
            if endpoint not in node_ids:
                raise ProductGraphCoverageError(
                    f"edge {edge.source!r} -> {edge.target!r} names {endpoint!r}, which is not a product node"
                )
        if not edge.relation:
            raise ProductGraphCoverageError(f"edge {edge.source!r} -> {edge.target!r} carries no relation")

    for lineage in graph.field_lineage:
        if lineage.product not in node_ids:
            raise ProductGraphCoverageError(
                f"field lineage names product {lineage.product!r}, which is not a product node"
            )
        if lineage.source_kind == SOURCE_KIND_CONTRACT and lineage.source_product not in node_ids:
            raise ProductGraphCoverageError(
                f"field lineage into {lineage.product!r} names upstream {lineage.source_product!r}, "
                "which is not a product node"
            )
        if not lineage.target_field or not lineage.source:
            raise ProductGraphCoverageError(
                f"field lineage for product {lineage.product!r} at {lineage.occurrence!r} is incomplete"
            )

    explained: set[tuple[str, str]] = {
        (lineage.product, lineage.target_field) for lineage in graph.field_lineage
    }
    for entry in graph.declared_fields:
        if entry.product not in node_ids:
            raise ProductGraphCoverageError(
                f"declared field {entry.field!r} names product {entry.product!r}, which is not a product node"
            )
        if (entry.product, entry.field) not in explained:
            raise ProductGraphCoverageError(
                f"product {entry.product!r} declares field {entry.field!r} at {entry.occurrence!r}, "
                "and the emitted field lineage carries no record explaining it"
            )

    for validation in graph.validations:
        if validation.product not in node_ids:
            raise ProductGraphCoverageError(
                f"validation occurrence names product {validation.product!r}, which is not a product node"
            )


# --------------------------------------------------------------------------- serialisation


NODE_HEADERS: list[str] = [
    "id",
    "name",
    "domain",
    "layer",
    "profile",
    "shape",
    "generation",
    "relations",
    "materialisation",
    "checkpoint",
]
EDGE_HEADERS: list[str] = ["edge_id", "src", "dst", "kind", "contract", "relation"]
FIELD_LINEAGE_HEADERS: list[str] = [
    "edge_id",
    "product",
    "target_field",
    "source_kind",
    "source_product",
    "source",
    "occurrence",
    "transform",
]
VALIDATION_HEADERS: list[str] = ["id", "product", "occurrence", "stage", "kind", "subject", "references"]

NODES_ARTEFACT = "product-nodes.csv"
EDGES_ARTEFACT = "product-edges.csv"
FIELD_LINEAGE_ARTEFACT = "product-field-lineage.csv"
VALIDATIONS_ARTEFACT = "product-validations.csv"
DESCRIPTION_ARTEFACT = "product-graph.json"

ARTEFACT_NAMES: tuple[str, ...] = (
    NODES_ARTEFACT,
    EDGES_ARTEFACT,
    FIELD_LINEAGE_ARTEFACT,
    VALIDATIONS_ARTEFACT,
    DESCRIPTION_ARTEFACT,
)


def csv_document(headers: list[str], rows: list[list[str]]) -> str:
    """The one CSV rendering every emitted graph table uses: a header row,
    minimal quoting and a newline terminator on every platform."""

    buffer = io.StringIO()
    writer = csv.writer(buffer, lineterminator="\n", quoting=csv.QUOTE_MINIMAL)
    writer.writerow(headers)
    for row in rows:
        writer.writerow(row)
    return buffer.getvalue()


def row_id(prefix: str, index: int) -> str:
    """A stable, zero-padded row identifier: the same shape every emitted
    graph table uses for its own rows."""

    return f"{prefix}{index:05d}"


def serialise_nodes_csv(graph: ProductGraph) -> str:
    rows = [
        [
            node.published_name,
            node.name,
            node.domain,
            node.layer,
            node.profile,
            node.shape,
            node.generation,
            ";".join(node.relations),
            node.materialisation,
            "true" if node.checkpoint else "false",
        ]
        for node in graph.nodes
    ]
    return csv_document(NODE_HEADERS, rows)


def serialise_edges_csv(graph: ProductGraph) -> str:
    rows = [
        [row_id("e", index), edge.source, edge.target, edge.kind, edge.contract, edge.relation]
        for index, edge in enumerate(graph.edges, start=1)
    ]
    return csv_document(EDGE_HEADERS, rows)


def serialise_field_lineage_csv(graph: ProductGraph) -> str:
    rows = [
        [
            row_id("f", index),
            lineage.product,
            lineage.target_field,
            lineage.source_kind,
            lineage.source_product,
            lineage.source,
            lineage.occurrence,
            lineage.transform,
        ]
        for index, lineage in enumerate(graph.field_lineage, start=1)
    ]
    return csv_document(FIELD_LINEAGE_HEADERS, rows)


def serialise_validations_csv(graph: ProductGraph) -> str:
    rows = [
        [
            row_id("v", index),
            validation.product,
            validation.occurrence,
            validation.stage,
            validation.kind,
            validation.subject,
            ";".join(validation.references),
        ]
        for index, validation in enumerate(graph.validations, start=1)
    ]
    return csv_document(VALIDATION_HEADERS, rows)


def build_graph_description(graph: ProductGraph) -> dict[str, Any]:
    """The description document: the build order, the generation
    membership, every published relation, the relations a checkpoint
    registered, the auxiliary relations, and the header lists the CSV
    tables were written with."""

    return {
        "schema": GRAPH_SCHEMA,
        "csv": {
            "node_headers": NODE_HEADERS,
            "edge_headers": EDGE_HEADERS,
            "field_lineage_headers": FIELD_LINEAGE_HEADERS,
            "validation_headers": VALIDATION_HEADERS,
        },
        "order": list(graph.order()),
        "generations": graph.generations(),
        "relations": {node.published_name: list(node.relations) for node in graph.nodes},
        "checkpoint_relations": list(graph.checkpoint_relations()),
        "auxiliary_relations": [
            {
                "product": entry.product,
                "relation": entry.relation,
                "translator": entry.translator,
                "purpose": entry.purpose,
            }
            for entry in graph.auxiliary
        ],
        # The stored names, and only where a declaration states one. The
        # field-lineage rows name every column by the logical name the
        # composition carries it under; these two lists are where a reader
        # resolves a logical name to the schema, table and column the built
        # relation actually carries. Both are absent from the description of
        # an estate that states none.
        **(
            {
                "stored_relations": [
                    {
                        "product": entry.product,
                        "relation": entry.relation,
                        "physical_schema": entry.physical_schema,
                        "physical_name": entry.physical_name,
                    }
                    for entry in graph.stored_relations
                ]
            }
            if graph.stored_relations
            else {}
        ),
        **(
            {
                "stored_fields": [
                    {
                        "product": entry.product,
                        "relation": entry.relation,
                        "field": entry.field,
                        "physical_name": entry.physical_name,
                    }
                    for entry in graph.stored_fields
                ]
            }
            if graph.stored_fields
            else {}
        ),
        "counts": {
            "products": len(graph.nodes),
            "edges": len(graph.edges),
            "field_lineage": len(graph.field_lineage),
            "declared_fields": len(graph.declared_fields),
            "validations": len(graph.validations),
        },
    }


def assert_description_coverage(description: Mapping[str, Any], graph: ProductGraph) -> None:
    """Fail closed when the description would publish less than the graph
    holds: a missing product in the order, a generation bucket that does
    not partition the products, a missing relation name, or a count that
    disagrees with the graph."""

    expected = {node.published_name for node in graph.nodes}
    order = list(description.get("order") or [])
    if set(order) != expected or len(order) != len(expected):
        raise ProductGraphCoverageError(
            f"description order {order} does not cover every product {sorted(expected)}"
        )

    generations = description.get("generations") or {}
    if set(generations) != set(GENERATIONS):
        raise ProductGraphCoverageError(
            f"description generations name {sorted(generations)}, expected {list(GENERATIONS)}"
        )
    members = [name for label in GENERATIONS for name in generations.get(label, [])]
    if sorted(members) != sorted(expected):
        raise ProductGraphCoverageError(
            f"description generations cover {sorted(members)}, expected {sorted(expected)}"
        )

    relations = description.get("relations") or {}
    if set(relations) != expected:
        raise ProductGraphCoverageError(
            f"description relations cover {sorted(relations)}, expected {sorted(expected)}"
        )
    for name, relation in relations.items():
        if not relation:
            raise ProductGraphCoverageError(f"description publishes no relation name for {name!r}")

    counts = description.get("counts") or {}
    actual = {
        "products": len(graph.nodes),
        "edges": len(graph.edges),
        "field_lineage": len(graph.field_lineage),
        "declared_fields": len(graph.declared_fields),
        "validations": len(graph.validations),
    }
    if counts != actual:
        raise ProductGraphCoverageError(f"description counts {counts} disagree with the graph {actual}")


def serialise_graph_description(graph: ProductGraph) -> str:
    description = build_graph_description(graph)
    assert_description_coverage(description, graph)
    return dump_json(description)


def graph_artefacts(graph: ProductGraph) -> dict[str, str]:
    """Every product-graph artefact as {file name: text}. The one
    rendering both the estate emitter and the publication translator
    call, so the two can never disagree by a byte."""

    assert_product_graph_coverage(graph)
    return {
        NODES_ARTEFACT: serialise_nodes_csv(graph),
        EDGES_ARTEFACT: serialise_edges_csv(graph),
        FIELD_LINEAGE_ARTEFACT: serialise_field_lineage_csv(graph),
        VALIDATIONS_ARTEFACT: serialise_validations_csv(graph),
        DESCRIPTION_ARTEFACT: serialise_graph_description(graph),
    }
