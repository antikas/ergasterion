"""Rendering the transformation patterns and the registered shapes into a
dbt project (architecture sections 4, 6 and 10).

Under the ``declared`` shape one product renders as one model carrying the
relation its contract publishes, plus the translator-private relations its
occurrences call for.
Most patterns render as a common table expression inside that model
(``steps``). Three own evidence about the rows reaching them, and evidence
has to be a relation something else can read, so each cuts the model's
chain in two (``segment``): Data Filtering leaves a pre-filter relation and
a filter log (``filtering``), Data Validation leaves a checked relation and
a quarantine relation (``validation``), and Data Curation leaves a
resolution relation and a pending-key relation (``curation``). Data Publish
adds the current pointer and the SLA record (``publish``). Every private
relation sits under the product's own namespace and is registered as
auxiliary lineage, never as part of the contract (architecture section 10).

The chain projects, at each occurrence, exactly the field set the contract
propagation leaves after it
(``ergasterion.framework.contract.propagate_relation_fields``), so the
emitted relation and the published contract are the same set of columns by
construction rather than by agreement.

Beside the models the route writes one schema document per product
(``schema_doc``): the contract metadata, the published columns with the
named rule each computed one carries, every private relation, and every
generated test. The generated tests are the Data Validation rules, the
threshold that aborts a run above it, the declared aggregation grain, and
the contract compliance check that keeps a product whose emitted schema
disagrees with its contract from publishing at all.

A product may instead declare a whole select body in its shape's target
section (architecture section 3.2's inline-SQL route). The engine binds the
composition's input relation and renders the declared body over it, so the
declaration still names no table (architecture section 13). Its output
columns must be exactly the relation the contract publishes, or emission
fails closed.

A shape that imposes a modelling of its own publishes relations rendered
over the composition rather than the composition's own relation
(``ergasterion.framework.shapes``). The chain is then rendered once into a
private base relation, and every relation the shape declares is rendered
over that (``shape_relations``), with its own key or grain test, its own
contract compliance check and its own publication relations. A relation the
shape derives from its siblings reads them instead of the base. A shape
carrying a semantic layer renders that too (``semantic``). The time spine
those semantic layers aggregate over is not a product's: a dbt project
carries one per granularity, so it is rendered once for the estate
(``render_time_spine``) over the window covering every window its products
declared, and registered as an auxiliary relation of the estate.

A shape's relation is materialised the way the shape declares: a view at an
interface boundary, a table for computation rebuilt whole, or an insert-only
store that is added to rather than rebuilt, keyed by the relation's own key
through the same configuration a product publishing incrementally takes
(``publish``).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Mapping, Sequence

from ergasterion.framework.contract import (
    OccurrenceFields,
    OpeningComposition,
    dump_json,
)
from ergasterion.framework.expressions import parse_select_body
from ergasterion.framework.graph import ESTATE_SCOPE, AuxiliaryRelation
from ergasterion.framework.models import RelationField, RelationSchema
from ergasterion.framework.shapes import (
    DERIVATION_COMPOSITION,
    MATERIALISATION_INCREMENTAL,
    MATERIALISATION_VIEW,
    ShapeRelation,
    get_shape,
)
from ergasterion.shapes.canonical import SHAPE_NAME as CANONICAL_SHAPE
from ergasterion.shapes.data_vault import SHAPE_NAME as DATA_VAULT_SHAPE
from ergasterion.shapes.dimensional import SHAPE_NAME as DIMENSIONAL_SHAPE
from ergasterion.shapes.ods import SHAPE_NAME as ODS_SHAPE
from ergasterion.translators.dbt_patterns import curation as curation_mod
from ergasterion.translators.dbt_patterns import filtering as filtering_mod
from ergasterion.translators.dbt_patterns import publish as publish_mod
from ergasterion.translators.dbt_patterns import semantic as semantic_mod
from ergasterion.translators.dbt_patterns import shape_relations as shape_relations_mod
from ergasterion.translators.dbt_patterns import validation as validation_mod
from ergasterion.translators.dbt_patterns.checkpoint import runtime_manifest
from ergasterion.translators.dbt_patterns.schema_doc import (
    SEVERITY_ERROR,
    GeneratedTest,
    PublishedModel,
    build_schema_document,
    dump_schema_document,
)
from ergasterion.translators.dbt_patterns.segment import Segment
from ergasterion.translators.dbt_patterns.sql import (
    PUBLISH_TIMESTAMP_CALL,
    RenderingError,
    declared_type_projection,
    identifier,
    jinja_literal,
    model_name,
    normalise_generated_text,
    ref,
    select_projection,
    suffixed_model_name,
    suffixed_relation_name,
)
from ergasterion.translators.dbt_patterns.steps import (
    CTE_RENDERERS,
    LATE_ARRIVAL_POLICIES,
    Cte,
    render_opening,
)

TRANSLATOR_NAME = "dbt"

DECLARED_SHAPE = "declared"

# Every shape this translator renders. It renders a shape by rendering
# each relation the shape declares (``shape_relations``), so the set is
# the shapes whose derivations it has an arm for, and the estate's
# translator table may name dbt for any of them.
PRODUCT_SHAPES: tuple[str, ...] = (
    DECLARED_SHAPE,
    CANONICAL_SHAPE,
    DATA_VAULT_SHAPE,
    DIMENSIONAL_SHAPE,
    ODS_SHAPE,
)

# The patterns this package renders without a common table expression of
# their own: Data Publish configures the model's materialisation and emits
# the pointer and the SLA record, and Checkpoint and Retries writes the run
# boundary and the retry policy into the runtime manifest.
NON_CTE_PATTERNS: tuple[str, ...] = ("data_publish", "checkpoint_retries")

PATTERN_DATA_FILTERING = filtering_mod.PATTERN
PATTERN_DATA_VALIDATION = validation_mod.PATTERN
PATTERN_DATA_CURATION = curation_mod.PATTERN
PATTERN_DATA_AGGREGATION = "data_aggregation"
PATTERN_DATA_ENRICHMENT = "data_enrichment"
PATTERN_DATA_PUBLISH = "data_publish"
PATTERN_CHECKPOINT_RETRIES = "checkpoint_retries"
PATTERN_CALCULATED_FIELDS = "calculated_fields"

# The three patterns that cut the chain, and the renderer that does it.
SEGMENT_RENDERERS: dict[str, Callable[..., Segment]] = {
    PATTERN_DATA_FILTERING: filtering_mod.render_segment,
    PATTERN_DATA_VALIDATION: validation_mod.render_segment,
    PATTERN_DATA_CURATION: curation_mod.render_segment,
}

# Every pattern the dbt translator registers a product capability for. The
# dbt translator reads this tuple, so a registered capability always has a
# renderer behind it.
PRODUCT_PATTERNS: tuple[str, ...] = tuple(
    sorted(set(CTE_RENDERERS) | set(SEGMENT_RENDERERS) | set(NON_CTE_PATTERNS))
)

MODELS_ROOT = "models/products"
MANIFESTS_ROOT = "manifests/products"

# Where the estate's own shared relations sit: a directory of the model
# tree that belongs to no product, holding the one time spine every
# semantic layer aggregates over.
TIME_SPINE_DIRECTORY = f"{MODELS_ROOT}/{semantic_mod.TIME_SPINE_NAMESPACE}"

SOURCE_CTE = "source_00"
BODY_CTE = "declared_body"

VIEW_CONFIG = "{{ config(materialized='view') }}"
TABLE_CONFIG = "{{ config(materialized='table') }}"

# The macro calls the generated auxiliary relations place. Each is a call
# into macros/ rather than inline SQL, because each is either genuinely
# dialect-divergent (the publication instant, imported from ``sql`` where
# every renderer reads it) or a shape every row shares.
TIMESTAMP_TYPE_CALL = "{{ dpf_type('timestamp') }}"
COUNT_TYPE_CALL = "{{ dpf_type('int') }}"

# The three generated tests macros/product_tests.sql carries.
TEST_UNIQUE_GRAIN = "dpf_unique_grain"
TEST_CONTRACT_COMPLIANCE = "dpf_contract_compliance"
TEST_RANGE_CONTIGUITY = "dpf_effective_range_contiguity"

# The semantic model a shape declares one for is rendered beside the
# product's schema document, under this suffix. One entry per shape that
# carries a semantic layer, so a shape without one emits no empty
# artefact and a new one registers its builder here.
SEMANTIC_SUFFIX = "__semantic.yml"
SEMANTIC_BUILDERS: dict[str, Callable[..., dict[str, Any]]] = {
    DIMENSIONAL_SHAPE: semantic_mod.build_document,
}


@dataclass(frozen=True)
class RuleSignature:
    """One named rule as a generated call needs it: the dbt macro that
    implements it on every declared adapter, its declared input names, and
    the catalogue version the emitted metadata records."""

    macro: str
    inputs: tuple[str, ...]
    version: int


@dataclass(frozen=True)
class ProductPlan:
    """Everything the dbt translator needs to render one product, already
    resolved by the emission route: the declaration, every relation the
    product's shape publishes, the propagated field set at every
    occurrence, which occurrences the estate's translator table gave to
    dbt, the model each consumed contract is read from (sources and
    enrichment lookups alike, keyed by
    ``ergasterion.framework.contract.source_relation_key``, so a read
    naming one relation of a producer that publishes several is rendered
    over that relation's model), and the rule signatures the composition
    references.

    ``composition`` is how the sources open the relation: the same
    resolution the published contract was derived from, carrying each
    source's conformed columns, so the rendered read and the contract can
    never disagree. ``shape_owner`` is the translator the estate's table
    names for this product's shape, which is the translator that renders
    its relations; the route hands this translator only the products it
    owns."""

    published_name: str
    domain: str
    name: str
    layer: str
    profile: str
    shape: str
    shape_owner: str
    document: dict
    relations: tuple[ShapeRelation, ...]
    composition: OpeningComposition
    opening_fields: tuple[RelationField, ...]
    occurrence_fields: tuple[OccurrenceFields, ...]
    owned_steps: frozenset[int]
    checkpoint_owned: bool
    source_models: Mapping[str, str]
    lookup_models: Mapping[str, str]
    rule_signatures: Mapping[str, RuleSignature]
    incremental_strategies: Mapping[str, str]
    materialisation: str
    # The record version each store the product's shape keeps is written
    # under, read off the estate's ledger for that shape
    # (``ergasterion.framework.shapes.ShapeDefinition.ledger``). Empty for
    # a shape that keeps none.
    store_versions: Mapping[str, int] = field(default_factory=dict)


@dataclass(frozen=True)
class ProductArtefacts:
    """One product's rendered output: artefacts keyed by their path inside
    the estate, and the private relations the translator registered while
    rendering them."""

    artefacts: dict[str, str] = field(default_factory=dict)
    auxiliary: tuple[AuxiliaryRelation, ...] = ()


class UnownedPatternError(RenderingError):
    """A product needs a pattern the estate's translator table did not give
    to dbt, and dbt cannot render the product without it."""

    code = "unowned_pattern"

    def __init__(self, *, product: str, pattern: str) -> None:
        super().__init__(
            product=product,
            occurrence=pattern,
            rule="unowned_pattern",
            detail=(
                f"dbt renders this product's shape, so the estate must also give it "
                f"{pattern!r}: how a relation is published, and the run boundary it is retried "
                "within, cannot belong to a different translator from the relation itself"
            ),
        )


class RelationColumnsError(RenderingError):
    """The rendered relation does not carry every column the published
    contract declares."""

    code = "relation_columns"

    def __init__(self, *, product: str, occurrence: str, missing: tuple[str, ...]) -> None:
        self.missing = missing
        super().__init__(
            product=product,
            occurrence=occurrence,
            rule="relation_columns",
            detail=(
                f"the rendered relation does not produce {', '.join(missing)}, which the "
                "published contract declares"
            ),
        )


def _step(plan: ProductPlan, index: int) -> dict:
    return (plan.document.get("steps") or [])[index]


def _steps_of(plan: ProductPlan, pattern: str) -> list[int]:
    return [
        index
        for index, step in enumerate(plan.document.get("steps") or [])
        if (step or {}).get("pattern") == pattern
    ]


def _owned_steps_of(plan: ProductPlan, pattern: str) -> list[int]:
    return [index for index in _steps_of(plan, pattern) if index in plan.owned_steps]


def _fields_before(plan: ProductPlan, index: int) -> tuple[RelationField, ...]:
    if index == 0:
        return plan.opening_fields
    return plan.occurrence_fields[index - 1].fields


def _rule_calls(plan: ProductPlan, index: int) -> dict[str, str]:
    """Every named-rule call one ``calculated_fields`` occurrence renders,
    keyed by the field's occurrence tag. A rule's declared inputs bind by
    name to the columns visible where the field is computed; a column the
    composition does not carry there fails closed rather than rendering a
    call to something that is not in scope."""

    occurrence = f"steps[{index}]:{PATTERN_CALCULATED_FIELDS}"
    visible = {entry.name for entry in _fields_before(plan, index)}
    calls: dict[str, str] = {}
    for field_index, entry in enumerate(_step(plan, index).get("fields") or []):
        rule_name = entry.get("rule")
        if rule_name is not None:
            tag = f"{occurrence}.fields[{field_index}]"
            signature = plan.rule_signatures[rule_name]
            missing = [name for name in signature.inputs if name not in visible]
            if missing:
                raise RenderingError(
                    product=plan.published_name,
                    occurrence=tag,
                    rule="unbound_rule_input",
                    detail=(
                        f"named rule {rule_name!r} declares input(s) {', '.join(missing)}, which "
                        f"the columns visible here do not carry: {sorted(visible)}"
                    ),
                )
            # Each argument is a Jinja string literal carrying the column
            # name. A dbt macro receives its arguments as Jinja values and
            # interpolates them into SQL, so a bare identifier here would
            # be an undefined Jinja variable that renders as nothing.
            arguments = ", ".join(
                jinja_literal(identifier(name, product=plan.published_name, occurrence=tag))
                for name in signature.inputs
            )
            calls[tag] = "{{ " + f"{signature.macro}({arguments})" + " }}"
        visible.add(entry["name"])
    return calls


def _render_cte(plan: ProductPlan, index: int, previous: str) -> tuple[Cte, ...]:
    """The common table expressions one non-cutting occurrence renders."""

    step = _step(plan, index)
    pattern = step["pattern"]
    keywords: dict[str, Any] = {
        "index": index,
        "step": step,
        "previous": previous,
        "fields": plan.occurrence_fields[index].fields,
        "product": plan.published_name,
    }
    if pattern == PATTERN_CALCULATED_FIELDS:
        keywords["rule_calls"] = _rule_calls(plan, index)
    elif pattern == PATTERN_DATA_ENRICHMENT:
        keywords["lookup_models"] = plan.lookup_models
    return CTE_RENDERERS[pattern](**keywords)


def _render_segment(plan: ProductPlan, index: int, previous: str) -> Segment:
    """The cut one evidence-owning occurrence makes in the chain."""

    step = _step(plan, index)
    pattern = step["pattern"]
    keywords: dict[str, Any] = {
        "index": index,
        "step": step,
        "product": plan.published_name,
        "domain": plan.domain,
        "name": plan.name,
        "fields": _fields_before(plan, index),
        "previous": previous,
    }
    if pattern == PATTERN_DATA_CURATION:
        keywords["rule_signatures"] = plan.rule_signatures
    elif pattern == PATTERN_DATA_VALIDATION:
        # A validation occurrence may carry the coverage a consolidating
        # product proves against the contracts it combines, which is a
        # statement about the relation the product publishes and about
        # each source's read, not about the rows reaching this occurrence.
        composition_relation = plan.relations[0].schema if _renders_composition(plan) else None
        keywords["published_model"] = (
            model_name(plan.domain, plan.name) if composition_relation is not None else None
        )
        keywords["published_columns"] = (
            tuple(entry.name for entry in composition_relation.fields)
            if composition_relation is not None
            else ()
        )
        keywords["composition"] = plan.composition
        keywords["source_models"] = plan.source_models
    return SEGMENT_RENDERERS[pattern](**keywords)


def _source_cte(plan: ProductPlan, *, name: str) -> Cte:
    """The relation this product's composition opens with, rendered from
    the composition the contract pipe resolved: one source read as it is,
    or two or more combined as the declaration says (architecture section
    8). Each source is read from the model of the relation it resolved to,
    looked up under the same key it was resolved under, so a rendered read
    is always the relation the published contract propagated from."""

    return render_opening(
        plan.composition,
        name=name,
        models=plan.source_models,
        product=plan.published_name,
    )


def _select_body(plan: ProductPlan) -> str | None:
    config = (plan.document.get("target") or {}).get("shape_config") or {}
    body = config.get("select")
    return body if isinstance(body, str) else None


def _check_produced_columns(
    plan: ProductPlan, produced: Sequence[str], *, required: Sequence[str], occurrence: str
) -> None:
    """Fail closed unless the rendering produced every column ``required``
    names: the columns the published contract declares, or the columns the
    shape reads to render its own relations."""

    available = set(produced)
    missing = tuple(name for name in required if name not in available)
    if missing:
        raise RelationColumnsError(
            product=plan.published_name, occurrence=occurrence, missing=missing
        )


def _column_metadata(plan: ProductPlan, fields: Sequence[RelationField]) -> list[dict[str, Any]]:
    """One entry per published column, carrying the named rule and the
    declared rule version wherever a calculated field is computed by one
    (architecture section 4, Calculated Fields: "version captured").

    Only a composition-rendered product reaches this with a rule: a
    declared select body cannot call a dispatch macro, so
    ``_check_body_can_carry_the_composition`` refuses that pairing before
    any metadata is written."""

    rules: dict[str, dict[str, Any]] = {}
    for index in _steps_of(plan, PATTERN_CALCULATED_FIELDS):
        for entry in _step(plan, index).get("fields") or []:
            rule_name = entry.get("rule")
            if rule_name is None:
                continue
            signature = plan.rule_signatures[rule_name]
            rules[entry["name"]] = {
                "rule": rule_name,
                "rule_version": entry.get("rule_version", signature.version),
            }
    return [{"name": entry.name, "rule": rules.get(entry.name)} for entry in fields]


def _check_body_can_carry_the_composition(plan: ProductPlan) -> None:
    """Refuse a declared select body whose composition carries an
    occurrence the body cannot honour.

    A select body states the whole relation in one statement, so anything
    the engine would otherwise render around it is not rendered at all.
    Two kinds of occurrence would therefore lose their semantics silently,
    and architecture section 4 never weakens a pattern to fit a rendering:

      * every occurrence that owes evidence about the rows reaching it --
        Data Filtering its exclusion counts, Data Validation its quarantine
        relation, Data Curation its resolution evidence -- needs a relation
        of its own to count, quarantine or resolve against, and a body
        renders none;
      * a Calculated Fields occurrence computed by a named rule owes the
        emitted SQL a call to that rule's dispatch macro. A body is
        declaration-neutral SQL and can call no macro, so the rule's
        version would be recorded as metadata against a column nothing
        computed by it.

    A Data Aggregation occurrence is refused for the same reason as the
    first group: its declared grain is what the late-arrival policy
    republishes whole, and a body the engine does not read carries no grain
    the engine can hold it to.
    """

    for pattern in (*sorted(SEGMENT_RENDERERS), PATTERN_DATA_AGGREGATION):
        owned = _owned_steps_of(plan, pattern)
        if owned:
            raise RenderingError(
                product=plan.published_name,
                occurrence=f"steps[{owned[0]}]:{pattern}",
                rule="body_cannot_carry_occurrence",
                detail=(
                    f"the product declares a whole select body and a {pattern!r} occurrence; a "
                    "body renders neither the relation that occurrence needs of its own nor the "
                    "configuration the engine would hold it to, so state the occurrence in the "
                    "composition or drop the body"
                ),
            )
    for index in _steps_of(plan, PATTERN_CALCULATED_FIELDS):
        for field_index, entry in enumerate(_step(plan, index).get("fields") or []):
            rule_name = entry.get("rule")
            if rule_name is None:
                continue
            raise RenderingError(
                product=plan.published_name,
                occurrence=f"steps[{index}]:{PATTERN_CALCULATED_FIELDS}.fields[{field_index}]",
                rule="body_cannot_carry_occurrence",
                detail=(
                    f"the product declares a whole select body and computes {entry['name']!r} by "
                    f"named rule {rule_name!r}; a body calls no dispatch macro, so the rule would "
                    "be recorded against a column nothing computed by it"
                ),
            )


def _aggregation_facts(plan: ProductPlan, publish_step: dict) -> list[dict[str, Any]]:
    """The declared grain and late-arrival policy of every aggregation
    occurrence dbt owns, checked against how the product publishes.

    ``recompute_period`` recomputes every period present in the input and
    republishes it whole. That only lands as declared when the publication
    replaces rows by the grain itself: an atomic rebuild would be a
    different guarantee, and a key that is not the grain would leave the
    recomputed period beside the published one instead of replacing it. A
    policy this translator does not render, and a publication that cannot
    carry the declared one, both fail closed rather than emitting a model
    whose behaviour nobody declared."""

    facts: list[dict[str, Any]] = []
    for index in _owned_steps_of(plan, PATTERN_DATA_AGGREGATION):
        step = _step(plan, index)
        occurrence = f"steps[{index}]:{PATTERN_DATA_AGGREGATION}"
        policy = step.get("late_arrival_policy")
        if policy not in LATE_ARRIVAL_POLICIES:
            raise RenderingError(
                product=plan.published_name,
                occurrence=occurrence,
                rule="unrenderable_late_arrival_policy",
                detail=(
                    f"late_arrival_policy {policy!r} is not one this translator renders: "
                    f"{', '.join(LATE_ARRIVAL_POLICIES)}"
                ),
            )
        grain = [
            identifier(name, product=plan.published_name, occurrence=occurrence)
            for name in step.get("grain") or []
        ]
        mode = publish_mod.publication_mode(publish_step, product=plan.published_name)
        keys = list(publish_step.get("unique_key") or [])
        if mode != publish_mod.PUBLICATION_MODE_INCREMENTAL or sorted(keys) != sorted(grain):
            raise RenderingError(
                product=plan.published_name,
                occurrence=occurrence,
                rule="late_arrival_policy_needs_the_grain_key",
                detail=(
                    f"late_arrival_policy {policy!r} recomputes a whole period and republishes "
                    f"it, so the product must publish incrementally keyed by the grain "
                    f"{grain}; it publishes {mode!r} keyed by {keys}"
                ),
            )
        facts.append({"grain": grain, "late_arrival_policy": policy})
    return facts


def _compliance_test(model: str, relation: RelationSchema) -> GeneratedTest:
    """The Data Contracts pattern's compliance check as a dbt test on one
    published model (architecture section 4: "Product not published if it
    violates its contract"). dbt runs a model's tests before anything that
    reads it, and the current pointer and the SLA record read this model,
    so a relation disagreeing with its contract never publishes. Failures
    are stored, so the disagreement itself is readable rather than only
    counted. One check per published relation: a product whose shape
    renders several publishes none of them on a disagreement in any."""

    return GeneratedTest(
        model=model,
        test=TEST_CONTRACT_COMPLIANCE,
        name=f"dpf_contract_compliance_{model}",
        column=None,
        arguments={
            "columns": [entry.name for entry in relation.fields],
            "required": [entry.name for entry in relation.fields if entry.required],
        },
        severity=SEVERITY_ERROR,
        store_failures=True,
    )


def _model_artefact(
    templates: Any,
    *,
    sql_header: str,
    model_config: str,
    ctes: Sequence[Cte],
    columns: Sequence[str],
    final_cte: str,
) -> str:
    return normalise_generated_text(
        templates.get_template("product/model.sql.j2").render(
            generated_header=sql_header,
            model_config=model_config,
            ctes=list(ctes),
            columns=list(columns),
            final_cte=final_cte,
        )
    )


BASE_SUFFIX = "base"


def _render_chain(
    plan: ProductPlan,
    *,
    templates: Any,
    sql_header: str,
    directory: str,
    artefacts: dict[str, str],
    register: Callable[..., None],
    tests: list[GeneratedTest],
) -> tuple[list[Cte], str, list[str]]:
    """The composition itself, rendered once: every occurrence dbt owns, in
    order, as common table expressions, with each evidence-owning
    occurrence cutting the chain into an auxiliary model of its own.
    Returns the common table expressions the chain ends with, the name of
    the last one, and the columns it produces. Whatever publishes those
    columns -- the product's own relation, or the relations its shape
    renders over it -- reads this one rendering."""

    ordered_steps = [
        index
        for index in sorted(plan.owned_steps)
        if _step(plan, index)["pattern"] in CTE_RENDERERS
        or _step(plan, index)["pattern"] in SEGMENT_RENDERERS
    ]
    ctes: list[Cte] = [_source_cte(plan, name=SOURCE_CTE)]
    previous = SOURCE_CTE
    for index in ordered_steps:
        pattern = _step(plan, index)["pattern"]
        if pattern not in SEGMENT_RENDERERS:
            rendered = _render_cte(plan, index, previous)
            ctes.extend(rendered)
            previous = rendered[-1].name
            continue

        segment = _render_segment(plan, index, previous)
        segment_model = suffixed_model_name(plan.domain, plan.name, segment.suffix)
        artefacts[f"{directory}/{segment_model}.sql"] = _model_artefact(
            templates,
            sql_header=sql_header,
            model_config=segment.model_config,
            ctes=[*ctes, *segment.ctes],
            columns=segment.columns,
            final_cte=segment.final_cte,
        )
        register(
            segment.suffix,
            purpose=segment.purpose,
            description=segment.description,
            columns=segment.test_columns,
        )
        for companion in segment.companions:
            companion_model = suffixed_model_name(plan.domain, plan.name, companion.suffix)
            artefacts[f"{directory}/{companion_model}.sql"] = _model_artefact(
                templates,
                sql_header=sql_header,
                model_config=companion.model_config,
                ctes=companion.ctes,
                columns=companion.columns,
                final_cte=companion.final_cte,
            )
            register(
                companion.suffix,
                purpose=companion.purpose,
                description=companion.description,
            )
        tests.extend(segment.tests)
        resume_cte = f"{segment.suffix}_input"
        ctes = [
            Cte(
                name=resume_cte,
                body=select_projection(segment.resume_columns, ref(segment_model)),
            ),
            *segment.resume_ctes,
        ]
        previous = segment.resume_ctes[-1].name if segment.resume_ctes else resume_cte

    produced = (
        [entry.name for entry in plan.occurrence_fields[ordered_steps[-1]].fields]
        if ordered_steps
        else [entry.name for entry in plan.opening_fields]
    )
    return ctes, previous, produced


def _publication_relations(
    plan: ProductPlan,
    *,
    templates: Any,
    sql_header: str,
    directory: str,
    artefacts: dict[str, str],
    register: Callable[..., None],
    model: str,
    prefix: str,
    pointer: bool = True,
) -> None:
    """The Data Publish pattern's private relations over one published
    model: the current pointer a consumer reads it through, and the SLA
    record of the last run. A product whose shape publishes several
    relations gets them per relation, because publication is a fact about
    a relation rather than about a product's file.

    ``pointer`` is false for a relation the shape already materialises as
    a view. The pointer exists so a consumer reads one stable name
    whichever way the relation was materialised; a view at an interface
    boundary is already that name, and a second view over it would only
    deepen the view chain the estate budgets."""

    relations = (
        (
            publish_mod.CURRENT_SUFFIX,
            "product/current_pointer.sql.j2",
            VIEW_CONFIG,
            "the current pointer a consumer reads the published relation through",
            "The current pointer onto this product's published relation.",
        ),
    ) if pointer else ()
    for suffix, template, configuration, purpose, description in (
        *relations,
        (
            publish_mod.SLA_SUFFIX,
            "product/sla_record.sql.j2",
            TABLE_CONFIG,
            "the publication instant and row count of the last run",
            "The publication instant and published row count of the last run.",
        ),
    ):
        suffixed = f"{prefix}{suffix}"
        publication_model = suffixed_model_name(plan.domain, plan.name, suffixed)
        artefacts[f"{directory}/{publication_model}.sql"] = normalise_generated_text(
            templates.get_template(template).render(
                generated_header=sql_header,
                model_config=configuration,
                source=ref(model),
                publish_timestamp=PUBLISH_TIMESTAMP_CALL,
                timestamp_type=TIMESTAMP_TYPE_CALL,
                count_type=COUNT_TYPE_CALL,
            )
        )
        register(suffixed, purpose=purpose, description=description)


def _relation_config(plan: ProductPlan, relation: ShapeRelation) -> str:
    """The dbt configuration of one relation a shape renders: the
    materialisation the shape declared for it.

    A view is an interface boundary the estate declared, a table is
    computation rebuilt whole, and an insert-only store is added to rather
    than rebuilt, keyed by the relation's own key through the same
    configuration a product publishing incrementally takes
    (``publish.incremental_config``). A shape declaring no materialisation
    for a relation it renders over the composition takes the table."""

    if relation.materialisation == MATERIALISATION_VIEW:
        return VIEW_CONFIG
    if relation.materialisation == MATERIALISATION_INCREMENTAL:
        return publish_mod.incremental_config(
            relation.key,
            product=plan.published_name,
            strategies=plan.incremental_strategies,
            occurrence=f"target.shape_config:{relation.suffix}",
        )
    return TABLE_CONFIG


def _renders_composition(plan: ProductPlan) -> bool:
    """Whether this product's shape publishes the composition's own
    relation, rather than relations rendered over it."""

    return len(plan.relations) == 1 and plan.relations[0].derivation == DERIVATION_COMPOSITION


def _unique_key_test(model: str, columns: Sequence[str]) -> GeneratedTest:
    """The generated test that holds one model to a key: a declared
    aggregation grain, a canonical entity's key, a fact's grain, or a
    dimension key with the start of its effective range."""

    return GeneratedTest(
        model=model,
        test=TEST_UNIQUE_GRAIN,
        name=f"dpf_unique_grain_{model}",
        column=None,
        arguments={"columns": list(columns)},
        severity=SEVERITY_ERROR,
    )


def _contiguity_test(model: str, relation: ShapeRelation) -> GeneratedTest:
    """The generated test that holds a slowly changing dimension to a
    contiguous history: each version's range ends exactly where the next
    one starts, only the version in force now is open, and no range ends
    before it starts."""

    effective_from, effective_to = relation.range_columns or ("", "")
    return GeneratedTest(
        model=model,
        test=TEST_RANGE_CONTIGUITY,
        name=f"dpf_effective_range_contiguity_{model}",
        column=None,
        arguments={
            "key": list(relation.key),
            "effective_from": effective_from,
            "effective_to": effective_to,
        },
        severity=SEVERITY_ERROR,
        store_failures=True,
    )


def render_product(
    plan: ProductPlan, *, templates: Any, sql_header: str, yaml_header: str
) -> ProductArtefacts:
    """Render one product's dbt artefacts and the private relations they
    introduce. ``templates`` is a Jinja environment loaded from
    ``ergasterion/templates``; the two headers are the frozen generated
    markers every artefact this engine writes carries."""

    publish_indices = _steps_of(plan, PATTERN_DATA_PUBLISH)
    if not publish_indices or publish_indices[0] not in plan.owned_steps:
        raise UnownedPatternError(product=plan.published_name, pattern=PATTERN_DATA_PUBLISH)
    if not plan.checkpoint_owned:
        raise UnownedPatternError(product=plan.published_name, pattern=PATTERN_CHECKPOINT_RETRIES)
    publish_step = _step(plan, publish_indices[0])

    artefacts: dict[str, str] = {}
    auxiliary: list[AuxiliaryRelation] = []
    described: list[dict[str, Any]] = []
    tests: list[GeneratedTest] = []
    published: list[PublishedModel] = []
    product_model = model_name(plan.domain, plan.name)
    interface = get_shape(plan.shape).interface_path
    directory = (
        f"{MODELS_ROOT}/{interface}/{plan.domain}"
        if interface
        else f"{MODELS_ROOT}/{plan.domain}"
    )
    model_config = publish_mod.model_config(
        publish_step, product=plan.published_name, strategies=plan.incremental_strategies
    )
    aggregation_facts = _aggregation_facts(plan, publish_step)

    def register(
        suffix: str,
        *,
        purpose: str,
        description: str,
        columns: Sequence[str] = (),
        describe: bool = True,
    ) -> None:
        """Register one translator-private relation as auxiliary lineage
        and, unless another generated document already describes it,
        describe it in the product's schema document. Two descriptions of
        one model would give dbt two patches for it."""

        auxiliary.append(
            AuxiliaryRelation(
                product=plan.published_name,
                relation=suffixed_relation_name(plan.domain, plan.name, suffix),
                translator=TRANSLATOR_NAME,
                purpose=purpose,
            )
        )
        if not describe:
            return
        described.append(
            {
                "name": suffixed_model_name(plan.domain, plan.name, suffix),
                "description": description,
                "columns": list(columns),
            }
        )

    if _renders_composition(plan):
        relation = plan.relations[0].schema
        body = _select_body(plan)
        if body is not None:
            _check_body_can_carry_the_composition(plan)
            parsed = parse_select_body(
                body,
                input_schema=[entry.name for entry in plan.opening_fields],
                product=plan.published_name,
                occurrence="target.shape_config.select",
            )
            if parsed.has_from:
                raise RenderingError(
                    product=plan.published_name,
                    occurrence="target.shape_config.select",
                    rule="select_body_names_a_relation",
                    detail=(
                        "the declared select body names its own FROM source; the engine binds "
                        "the composition's input relation, so a declaration never names one"
                    ),
                )
            _check_produced_columns(
                plan,
                parsed.output_columns,
                required=[entry.name for entry in relation.fields],
                occurrence="target.shape_config.select",
            )
            artefacts[f"{directory}/{product_model}.sql"] = normalise_generated_text(
                templates.get_template("product/select_body.sql.j2").render(
                    generated_header=sql_header,
                    model_config=model_config,
                    source_cte=_source_cte(plan, name=SOURCE_CTE),
                    select_body=body.strip(),
                    source_cte_name=SOURCE_CTE,
                    body_cte_name=BODY_CTE,
                    columns=declared_type_projection(
                        relation.fields,
                        product=plan.published_name,
                        occurrence="target.shape_config.select",
                    ),
                )
            )
        else:
            ctes, previous, produced = _render_chain(
                plan,
                templates=templates,
                sql_header=sql_header,
                directory=directory,
                artefacts=artefacts,
                register=register,
                tests=tests,
            )
            _check_produced_columns(
                plan,
                produced,
                required=[entry.name for entry in relation.fields],
                occurrence="steps",
            )
            artefacts[f"{directory}/{product_model}.sql"] = _model_artefact(
                templates,
                sql_header=sql_header,
                model_config=model_config,
                ctes=ctes,
                columns=[
                    identifier(entry.name, product=plan.published_name, occurrence="target")
                    for entry in relation.fields
                ],
                final_cte=previous,
            )

        for fact in aggregation_facts:
            tests.append(_unique_key_test(product_model, fact["grain"]))
        tests.append(_compliance_test(product_model, relation))
        _publication_relations(
            plan,
            templates=templates,
            sql_header=sql_header,
            directory=directory,
            artefacts=artefacts,
            register=register,
            model=product_model,
            prefix="",
        )
        published.append(
            PublishedModel(
                model=product_model,
                relation=relation.name,
                columns=tuple(_column_metadata(plan, relation.fields)),
            )
        )
    else:
        mode = publish_mod.publication_mode(publish_step, product=plan.published_name)
        if mode != publish_mod.PUBLICATION_MODE_ATOMIC:
            raise RenderingError(
                product=plan.published_name,
                occurrence=f"steps[{publish_indices[0]}]:{PATTERN_DATA_PUBLISH}",
                rule="shape_publishes_its_relations_whole",
                detail=(
                    f"this product's shape renders {len(plan.relations)} relations, each derived "
                    f"from the whole composition; declare publication_mode "
                    f"{publish_mod.PUBLICATION_MODE_ATOMIC!r} rather than {mode!r}, which "
                    "replaces rows by a key no derived relation carries"
                ),
            )
        ctes, previous, produced = _render_chain(
            plan,
            templates=templates,
            sql_header=sql_header,
            directory=directory,
            artefacts=artefacts,
            register=register,
            tests=tests,
        )
        _check_produced_columns(
            plan,
            produced,
            required=sorted(
                {column for entry in plan.relations for column in entry.source_columns}
            ),
            occurrence="steps",
        )
        base_model = suffixed_model_name(plan.domain, plan.name, BASE_SUFFIX)
        artefacts[f"{directory}/{base_model}.sql"] = _model_artefact(
            templates,
            sql_header=sql_header,
            model_config=TABLE_CONFIG,
            ctes=ctes,
            columns=[
                identifier(name, product=plan.published_name, occurrence="steps")
                for name in produced
            ],
            final_cte=previous,
        )
        register(
            BASE_SUFFIX,
            purpose="the composition's relation, which every relation this shape renders reads",
            description=(
                "The composition's own relation, read by every relation this product publishes."
            ),
        )

        # Every relation's model name is resolved before any of them is
        # rendered, because a relation a shape derives from its siblings
        # reads them by model name and the loop below would otherwise only
        # know the ones it had already reached.
        models_by_suffix: dict[str, str] = {
            str(entry.suffix): suffixed_model_name(plan.domain, plan.name, str(entry.suffix))
            for entry in plan.relations
        }
        context = shape_relations_mod.RelationContext(
            base_model=base_model,
            product=plan.published_name,
            models=models_by_suffix,
            shape_config=(plan.document.get("target") or {}).get("shape_config") or {},
            store_versions=plan.store_versions,
        )
        relations_by_suffix: dict[str, ShapeRelation] = {}
        for entry in plan.relations:
            suffix = str(entry.suffix)
            model = models_by_suffix[suffix]
            relation_ctes, columns, final_cte = shape_relations_mod.render_relation(entry, context)
            artefacts[f"{directory}/{model}.sql"] = _model_artefact(
                templates,
                sql_header=sql_header,
                model_config=_relation_config(plan, entry),
                ctes=relation_ctes,
                columns=columns,
                final_cte=final_cte,
            )
            key_columns = list(entry.key) + (
                [entry.range_columns[0]] if entry.range_columns else []
            )
            tests.append(_unique_key_test(model, key_columns))
            if entry.range_columns:
                tests.append(_contiguity_test(model, entry))
            tests.append(_compliance_test(model, entry.schema))
            _publication_relations(
                plan,
                templates=templates,
                sql_header=sql_header,
                directory=directory,
                artefacts=artefacts,
                register=register,
                model=model,
                prefix=f"{suffix}_",
                pointer=entry.materialisation != MATERIALISATION_VIEW,
            )
            published.append(
                PublishedModel(
                    model=model,
                    relation=entry.schema.name,
                    columns=tuple(_column_metadata(plan, entry.schema.fields)),
                )
            )
            relations_by_suffix[suffix] = entry

        builder = SEMANTIC_BUILDERS.get(plan.shape)
        if builder is not None:
            shape_config = (plan.document.get("target") or {}).get("shape_config") or {}
            # The window is read here, where the product declares it, so an
            # undeclared one fails closed naming this product rather than
            # the estate. The spine itself is the estate's, rendered once
            # by ``render_time_spine`` over every window read this way.
            semantic_mod.declared_window(shape_config, product=plan.published_name)
            artefacts[f"{directory}/{product_model}{SEMANTIC_SUFFIX}"] = normalise_generated_text(
                dump_schema_document(
                    builder(
                        product=plan.published_name,
                        shape_config=shape_config,
                        relations=relations_by_suffix,
                        models=models_by_suffix,
                    ),
                    generated_header=yaml_header,
                )
            )

    artefacts[f"{directory}/{product_model}.yml"] = normalise_generated_text(
        dump_schema_document(
            build_schema_document(
                product=plan.published_name,
                profile=plan.profile,
                shape=plan.shape,
                publication_mode=publish_mod.publication_mode(
                    publish_step, product=plan.published_name
                ),
                published=published,
                auxiliary_models=sorted(described, key=lambda item: item["name"]),
                tests=tests,
            ),
            generated_header=yaml_header,
        )
    )

    manifest = runtime_manifest(
        product=plan.published_name,
        translator=TRANSLATOR_NAME,
        checkpointing=plan.document.get("checkpointing") or {},
        occurrences=[plan.occurrence_fields[index].occurrence for index in sorted(plan.owned_steps)],
        publication=publish_mod.publication_facts(
            publish_step, product=plan.published_name, strategies=plan.incremental_strategies
        ),
        materialisation=plan.materialisation,
        published_relations=[entry.schema.name for entry in plan.relations],
        auxiliary_relations=sorted(entry.relation for entry in auxiliary),
        aggregation=aggregation_facts,
    )
    artefacts[f"{MANIFESTS_ROOT}/{plan.domain}/{plan.name}.json"] = dump_json(manifest)
    return ProductArtefacts(artefacts=artefacts, auxiliary=tuple(auxiliary))


def render_time_spine(
    plans: Sequence[ProductPlan], *, templates: Any, sql_header: str, yaml_header: str
) -> ProductArtefacts:
    """The estate's one shared time spine, rendered once for every product
    whose shape carries a semantic layer.

    A dbt project carries one spine per granularity, so a spine per product
    would let a second semantic layer either be refused at parse or pick a
    winner silently. This renders one relation instead, over the window that
    covers every window the estate's products declared, describes it once in
    its own document, and registers it as an auxiliary relation of the
    estate: it is translator-private, no contract lists it, and it sits
    under no product's namespace because it belongs to none of them
    (architecture section 10, owner ruling R6).

    An estate whose products declare no semantic layer renders nothing
    here."""

    owners = [plan for plan in plans if plan.shape in SEMANTIC_BUILDERS]
    if not owners:
        return ProductArtefacts()
    window = semantic_mod.covering_window(
        [
            semantic_mod.declared_window(
                (plan.document.get("target") or {}).get("shape_config") or {},
                product=plan.published_name,
            )
            for plan in owners
        ]
    )
    ctes, columns, final_cte = semantic_mod.time_spine_model(window)
    model = semantic_mod.TIME_SPINE_MODEL
    artefacts = {
        f"{TIME_SPINE_DIRECTORY}/{model}.sql": _model_artefact(
            templates,
            sql_header=sql_header,
            model_config=TABLE_CONFIG,
            ctes=ctes,
            columns=columns,
            final_cte=final_cte,
        ),
        f"{TIME_SPINE_DIRECTORY}/{model}.yml": normalise_generated_text(
            dump_schema_document(
                semantic_mod.time_spine_document(
                    sorted(plan.published_name for plan in owners)
                ),
                generated_header=yaml_header,
            )
        ),
    }
    return ProductArtefacts(
        artefacts=artefacts,
        auxiliary=(
            AuxiliaryRelation(
                product=ESTATE_SCOPE,
                relation=semantic_mod.TIME_SPINE_RELATION,
                translator=TRANSLATOR_NAME,
                purpose="the dense date relation every semantic layer of this estate aggregates over",
            ),
        ),
    )


__all__ = [
    "DECLARED_SHAPE",
    "PRODUCT_SHAPES",
    "MANIFESTS_ROOT",
    "MODELS_ROOT",
    "PRODUCT_PATTERNS",
    "ProductArtefacts",
    "ProductPlan",
    "RelationColumnsError",
    "RuleSignature",
    "TIME_SPINE_DIRECTORY",
    "UnownedPatternError",
    "render_product",
    "render_time_spine",
]
