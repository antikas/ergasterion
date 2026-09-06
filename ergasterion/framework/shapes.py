"""The shape registry: the target's modelling form as a plug-in (architecture
section 6).

A shape is a registered rendering of a product's target. It carries its own
declaration schema for the ``target.shape_config`` block, the relations it
renders with the fields each one publishes, and any constraint it adds
beyond the composition's profile. This module holds the registry and that
vocabulary. How a relation becomes an artefact is a translator concern and
lives in ``ergasterion.translators``, never here: a shape says what it
renders and from which columns, a translator says in what technology
(architecture section 10, "a shape's declaration schema and constraints are
engine-level, its rendering is per translator").

``declared`` is registered here, in the engine, because it is the shape a
product takes when it imposes no modelling of its own: the relations
exactly as the composition produces them. Every other shape is a plug-in
package under ``ergasterion/shapes/<name>/`` that calls ``register_shape``
when it is loaded, and ``_load_plugins`` loads that package the first time
anything asks the registry a question. A product naming a shape no plug-in
registers fails closed with ``UnknownShapeError``: this is not a special
case in validation, it is simply a name the registry does not carry.

A shape that renders more than one relation says so through
``relations()``. Everything that needs to know what a product publishes --
the contract (``ergasterion.framework.contract``), the estate graph
(``ergasterion.framework.graph``) and the translator that renders the
artefacts -- reads that one answer, so a relation can never be published
under a name one of them derived privately.

Most shapes render a function of the declaration and nothing else. A shape
whose rendering depends on what earlier runs already stored says so through
``ledger_relative_path`` and ``ledger``: the emission route reads the record
the estate carries, hands it to the shape to grade against the current
declaration, and writes the graded record back with every other generated
file (``ShapeLedger``).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping, Sequence

from ergasterion.framework.models import FrameworkError, RelationField, RelationSchema

# How a translator derives one of a shape's relations. The set is closed:
# a translator renders one arm per derivation, and a derivation nothing
# renders fails closed at emission rather than producing a guessed relation.
DERIVATION_COMPOSITION = "composition"
DERIVATION_PROJECTION = "projection"
DERIVATION_DIMENSION_TYPE_1 = "dimension_type_1"
DERIVATION_DIMENSION_TYPE_2 = "dimension_type_2"
# The six derivations a vault modelling adds: the store of one entity's
# identities, the store of one association between them, the two
# insert-only stores of one payload -- its changes and its observations --
# the surviving value of each declared attribute over those stores, and
# the pointer relation that reads them all as they stood at one instant.
DERIVATION_IDENTITY_STORE = "identity_store"
DERIVATION_ASSOCIATION_STORE = "association_store"
DERIVATION_VERSION_STORE = "version_store"
DERIVATION_SNAPSHOT_STORE = "snapshot_store"
DERIVATION_SURVIVING_RECORD = "surviving_record"
DERIVATION_POINT_IN_TIME = "point_in_time"

# What a shape's relation materialises as, where the shape decides it. A
# relation carrying no materialisation is materialised the way its
# product's Data Publish occurrence declares.
MATERIALISATION_VIEW = "view"
MATERIALISATION_TABLE = "table"
# An insert-only store: the relation accumulates across runs rather than
# being rebuilt from its input, so what a run derives is added to what is
# already there and a row already stored is never written twice.
MATERIALISATION_INCREMENTAL = "incremental"

# The two columns a half-open effective range publishes, wherever a
# shape's relation carries one: a type 2 dimension (the dimensional
# shape) and an entity's own history (the ods shape) are two different
# shapes rendering the very same range, so the names are declared once,
# here, rather than by each shape that needs them. ``effective_from`` is
# the change instant a version came into force; ``effective_to`` is the
# instant the next version came into force, or absent for the version in
# force now.
EFFECTIVE_FROM = "effective_from"
EFFECTIVE_TO = "effective_to"

# The neutral types a column ordering a history may declare. Both are
# instants; nothing else orders one. Declared here rather than by each
# shape that needs one, for the same reason as the two range columns
# above: an entity's history (the ods shape) and a payload's versions (the
# data_vault shape) are two shapes asking the same question of a declared
# column.
TIME_TYPES: frozenset[str] = frozenset({"date", "timestamp"})


class UnknownShapeError(FrameworkError):
    """Raised when a product names a shape this registry does not carry."""

    code = "unknown_shape"

    def __init__(self, shape_name: object) -> None:
        self.shape_name = shape_name
        super().__init__(f"unknown shape: {shape_name!r}")


class DuplicateShapeError(FrameworkError):
    """Raised when two definitions claim the same shape name. One name, one
    definition: a second registration is a collision, never an override."""

    code = "duplicate_shape"

    def __init__(self, shape_name: str) -> None:
        self.shape_name = shape_name
        super().__init__(f"shape {shape_name!r} is already registered")


class ShapeConstraintError(FrameworkError):
    """A product's declaration does not satisfy a constraint its shape
    adds. Always names the product, the shape and where in the declaration
    the constraint bit."""

    code = "shape_constraint"

    def __init__(self, *, product: str, shape: str, detail: str, occurrence: str | None = None) -> None:
        self.product = product
        self.shape = shape
        self.occurrence = occurrence
        self.detail = detail
        where = f" at {occurrence!r}" if occurrence else ""
        super().__init__(f"product {product!r}: shape {shape!r}{where}: {detail}")


@dataclass(frozen=True)
class ShapeRelation:
    """One relation a shape renders.

    ``schema`` is the relation as the contract publishes it. ``derivation``
    says how a translator produces it, ``source_columns`` which columns of
    the composition's own relation it reads, and ``key`` the columns a
    generated test holds it unique on -- an entity key, a fact's declared
    grain, or a dimension key with the start of its effective range.
    ``materialisation`` is ``None`` where the product's Data Publish
    occurrence decides it. ``change_column`` and ``range_columns`` carry
    the declared version column and the pair of effective-range columns a
    slowly changing dimension needs; both are ``None`` everywhere else.

    ``reads`` names the sibling suffixes a relation is derived from, and
    is empty everywhere else: a relation naming one is rendered over those
    relations rather than over the composition's own. Its
    ``source_columns`` stay the composition columns it ultimately depends
    on, so the check that the composition produces what the shape reads
    still covers it.
    """

    suffix: str | None
    schema: RelationSchema
    derivation: str
    source_columns: tuple[str, ...] = ()
    key: tuple[str, ...] = ()
    materialisation: str | None = None
    change_column: str | None = None
    range_columns: tuple[str, str] | None = None
    reads: tuple[str, ...] = ()


def column_field(
    fields: Sequence[RelationField], name: object, *, product: str, shape: str, occurrence: str
) -> RelationField:
    """The composition's field called ``name``, or a closed failure naming
    product, shape, occurrence and column. Every shape resolves a declared
    column through this, so a shape can never publish a column the
    composition does not carry, and never guess its type."""

    for entry in fields:
        if entry.name == name:
            return entry
    raise ShapeConstraintError(
        product=product,
        shape=shape,
        occurrence=occurrence,
        detail=(
            f"column {name!r} is not one the composition carries: "
            f"{sorted(entry.name for entry in fields)}"
        ),
    )


@dataclass(frozen=True)
class ShapeLedger:
    """The durable record one shape keeps between emissions.

    Most shapes keep none: what they render is a function of the
    declaration alone, so a second emission of the same declaration is the
    same artefacts and nothing has to be remembered. A shape whose
    rendering depends on what earlier runs already stored -- an insert-only
    store whose change detection is computed over a frozen column set --
    needs the estate to carry that column set from one emission to the
    next, and this is it: one document per product, at ``relative_path``
    inside the estate, written by the emission route with every other
    generated file, plus the notices the grading produced.
    """

    relative_path: str
    document: dict
    notices: tuple[str, ...] = ()
    # The version of the record each store the shape keeps is currently
    # written under. A translator carries it into the generated relation,
    # so a stored row always says which record it was written under.
    store_versions: Mapping[str, int] = field(default_factory=dict)


@dataclass(frozen=True)
class ShapeDefinition:
    """One registered shape: its name, the JSON Schema its
    ``target.shape_config`` block must satisfy, and what it renders.

    ``declared`` renders the relations exactly as the composition produces
    them (architecture section 6), which is one relation carrying the
    product's own published name, and needs nothing in the target block.
    A shape that renders more than one relation, or that adds a constraint
    on the estate around it, subclasses this and overrides the three hooks
    below; the engine needs no other change.

    ``interface_path`` is the model path a shape's relations sit at when
    the shape is an interface boundary rather than computation: the estate
    declares which model paths may carry a view
    (``declarations/targets/interfaces.yml``), and the emission route holds
    a shape declaring one to that declaration. A shape that is computation
    declares none, and its relations sit with every other product's.
    """

    name: str
    shape_config_schema: dict
    interface_path: str | None = None

    def relation_names(
        self, *, domain: str, name: str, shape_config: Mapping[str, Any] | None = None
    ) -> tuple[str, ...]:
        """Every relation name this shape publishes for a product, derived
        from the product's declared domain, name and shape section and from
        nothing else (architecture section 12's stable relation names)."""

        return (f"{domain}.{name}",)

    def relations(
        self,
        *,
        domain: str,
        name: str,
        shape_config: Mapping[str, Any],
        fields: Sequence[RelationField],
    ) -> tuple[ShapeRelation, ...]:
        """Every relation this shape renders, with the fields each one
        publishes, given the field set the composition leaves. Fails closed
        with ``ShapeConstraintError`` on anything the declaration states
        that the composition cannot carry."""

        return (
            ShapeRelation(
                suffix=None,
                schema=RelationSchema(name=f"{domain}.{name}", fields=tuple(fields)),
                derivation=DERIVATION_COMPOSITION,
            ),
        )

    def ledger_relative_path(self, *, domain: str, name: str) -> str | None:
        """Where this shape's durable record for one product sits inside
        the estate, or ``None`` for a shape that keeps none. The emission
        route reads the file at this path before asking for the record, so
        the path is answered without it."""

        return None

    def ledger(
        self,
        *,
        product: str,
        shape_config: Mapping[str, Any],
        relations: Sequence[ShapeRelation],
        recorded: Mapping[str, Any] | None,
    ) -> ShapeLedger | None:
        """This shape's durable record for one product, graded against
        ``recorded`` -- what the estate carries from the last emission, or
        ``None`` on the first one. ``relations`` is what this shape renders
        for the product, so a record can describe a published column
        without deriving its type a second way. Fails closed on a declared
        change the record cannot absorb, naming the product, the shape,
        what changed and the remedy. A shape keeping no record returns
        ``None``."""

        return None

    def check_estate(
        self,
        *,
        product: str,
        shape_config: Mapping[str, Any],
        document: Mapping[str, Any],
        upstream: Mapping[str, Mapping[str, Any]],
    ) -> None:
        """Fail closed on a constraint this shape adds that only the estate
        can answer -- what the products upstream of this one are, and how
        this one publishes. ``upstream`` maps each consumed contract's
        published name to that product's declaration document, and carries
        no entry for a source no product in the estate publishes."""

        return None


# The ``declared`` shape's own target-block section. Architecture section 3.1:
# "A shape may add a section to the target block, and only there", so this is
# the one legal home for a whole select body -- the alternative to letting the
# composition's occurrences produce the relation step by step (section 3.2's
# inline-SQL route: "a whole select body is written as SQL in the
# declaration"). The body is optional: a product that declares none renders as
# its composition, which is the shape's ordinary reading.
DECLARED_SHAPE_CONFIG_SCHEMA: dict = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "title": "declared shape_config -- an optional whole select body, and nothing else",
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "select": {
            "type": "string",
            "minLength": 1,
            "description": (
                "One whole SQL SELECT statement producing this product's relation. Its "
                "output columns must be exactly the relation schema the composition "
                "publishes, or emission fails closed."
            ),
        }
    },
}

_SHAPE_REGISTRY: dict[str, ShapeDefinition] = {
    "declared": ShapeDefinition(name="declared", shape_config_schema=DECLARED_SHAPE_CONFIG_SCHEMA),
}

_PLUGINS_LOADED = False


def register_shape(definition: ShapeDefinition) -> ShapeDefinition:
    """Register one shape. Called by each plug-in package under
    ``ergasterion/shapes/`` as it is loaded; a name already registered
    fails closed rather than being replaced."""

    if definition.name in _SHAPE_REGISTRY:
        raise DuplicateShapeError(definition.name)
    _SHAPE_REGISTRY[definition.name] = definition
    return definition


def _load_plugins() -> None:
    """Load the shape plug-in packages once. Imported here rather than at
    module import so a plug-in can import this module for the vocabulary it
    registers with."""

    global _PLUGINS_LOADED
    if _PLUGINS_LOADED:
        return
    _PLUGINS_LOADED = True
    from ergasterion import shapes as _plugins  # noqa: F401


def registered_shape_names() -> tuple[str, ...]:
    """Every registered shape name, sorted. The one place to ask what the
    registry carries: reading a module-level dictionary would answer before
    the plug-ins are loaded."""

    _load_plugins()
    return tuple(sorted(_SHAPE_REGISTRY))


def get_shape(name: object) -> ShapeDefinition:
    """Look up a registered shape by name. Fails closed with
    ``UnknownShapeError`` when ``name`` is not a string or is not
    registered -- there is no default shape and no fuzzy match."""

    _load_plugins()
    if not isinstance(name, str) or name not in _SHAPE_REGISTRY:
        raise UnknownShapeError(name)
    return _SHAPE_REGISTRY[name]
