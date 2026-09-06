"""The product emission route (architecture sections 4, 6, 9, 10, 12).

``ergasterion emit-products`` is the command that turns an estate's product
declarations into its complete generated estate. It runs architecture section
9's pipeline end to end for the dbt translator:

    validate   layer 1 (schema, composition, ordering, neutral types, the
               neutrality gate, the source-kind pairing) and layer 2
               (inline expression parse and column resolution, named-rule
               resolution, the completeness gate);
    plan       every product's relation schema, propagated through its
               composition and split into the relations its shape
               publishes, the constraints a shape adds that only the estate
               can answer, the durable record a shape keeps between
               emissions graded against what the estate carries, the
               interface boundary a shape rendering views is held to, and
               the estate's product graph;
    route      every occurrence, the checkpoint wrapper and the product's
               shape, through the estate's translator table, failing closed
               on a missing entry, an unsupplied translator or a missing
               capability, naming label, pattern and adapter; the
               translator the table names for a product's shape is the one
               that renders its relations, so this route hands the dbt
               translator only the products whose shape is dbt's and emits
               nothing for the rest, whose own translator renders them;
    emit       one model per product plus the private relations its
               occurrences call for, deterministic and byte-stable, each
               carrying the generated marker;
    gate       the parse gate and the dialect deny-list, both once per
               declared adapter and both over the generated text before
               anything is written, so check mode enforces exactly what
               write mode does, then the estate's structural budgets once
               per declared adapter over the product tree on disk, each
               offense naming the artefact, the rule and the adapter.

After the model route passes, the command invokes the existing contract, ODPS
descriptor and product-graph emitters with the same estate and check mode.

This is the estate's only model-emission route. It reads product
declarations under ``declarations/products/`` and writes ``models/products/``
and ``manifests/products/``; nothing else writes into either tree.

Where a product's relation schema starts. A product's composition is
propagated from the schemas of the contracts its ``sources`` name. Two
kinds of source open a composition without another product deriving them:
a landing product's schema, which the ingestion side's own product
contracts carry, and a fixture-bound source, whose declaration carries the
fields its fixture relation delivers (architecture section 12). Both are
supplied to the relation registry the same way. A landing product that
lands a bound relation instead of arriving with a supplied schema takes
its own columns from the fields that binding declares (owner ruling R5),
which the registry resolves.

Where two or more sources open one composition, the declaration says how
they combine and the contract pipe resolves it once
(``ergasterion.framework.contract.resolve_opening_composition``); the plan
carries that resolution, so the relation rendered here is the one the
published contract was derived from.
"""

from __future__ import annotations

import argparse
import dataclasses
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping, Sequence

import yaml

from ergasterion import emit_contracts, emit_graph, emit_odps
from ergasterion.dialect_lint import lint_artefacts
from ergasterion.estate import EstateContext
from ergasterion.framework import contract as contract_mod
from ergasterion.framework import declaration as declaration_mod
from ergasterion.framework import graph as graph_mod
from ergasterion.framework import rules as rules_mod
from ergasterion.framework.adapters import load_adapter_conventions
from ergasterion.framework.estate_config import EstateConfiguration, load_estate_configuration
from ergasterion.framework.models import FrameworkError
from ergasterion.framework.routing import RoutableTranslator, resolve_table_owners
from ergasterion.framework.shapes import ShapeLedger, get_shape
from ergasterion.structure_gate import (
    Offense,
    check_structure,
    load_structure_declarations,
    targets_dir,
)
from ergasterion.translators.dbt import DbtProductTranslator
from ergasterion.translators.dbt_patterns import (
    MANIFESTS_ROOT,
    MODELS_ROOT,
    ProductPlan,
    RuleSignature,
    parse_gate,
)
from ergasterion.translators.dbt_patterns.sql import (
    model_name,
    suffixed_model_name,
    suffixed_relation_name,
)
from ergasterion.translators.local_ingestion import LocalIngestionTranslator
from ergasterion.translators.publication import PublicationTranslator

_DEFAULT_CTX = EstateContext.default()

TRANSLATOR_NAME = "dbt"

# The occurrence identity the checkpoint wrapper is routed under. It is not
# a step (architecture section 4: "Wrapper around the composition"), so it
# is named for the block that configures it.
CHECKPOINT_OCCURRENCE = "checkpointing"
SHAPE_OCCURRENCE = "target.shape"

MODEL_PRUNE_PATTERNS = ("*.sql", "*.yml")
MANIFEST_PRUNE_PATTERNS = ("*.json",)

# A shape ledger is never pruned; see ``_ledger_files``.
LEDGER_PRUNE_PATTERNS: tuple[str, ...] = ()


class ProductEmissionError(FrameworkError):
    """One failure of the product emission route that belongs to the route
    itself rather than to validation, routing or rendering."""

    code = "product_emission_error"


@dataclass(frozen=True)
class EmissionResult:
    """What one emission produced: the artefacts keyed by their path in the
    estate, one summary line per product, the adapters the estate declares,
    and the models the route rendered as translator-private relations (which
    the structural stage reads)."""

    files: dict[Path, str]
    summaries: list[str]
    adapters: tuple[str, ...]
    private_models: frozenset[str]
    notices: tuple[str, ...] = ()


def estate_configuration(ctx: EstateContext) -> EstateConfiguration:
    """This estate's whole configuration, loaded and cross-checked once
    against the translators this route carries (architecture section 11).
    Every stage below reads the labels, profiles, adapters and translator
    table off the result, so the route has one estate reading, not one per
    block."""

    return load_estate_configuration(
        ctx.estate_file,
        translator_names=[translator.target_name for translator in _estate_translators()],
    )


def _estate_translators() -> tuple[RoutableTranslator, ...]:
    """Every translator this command carries, so the estate's table can
    name any of them. Only the dbt product translator renders here; the
    other two are supplied because ownership resolution must be able to
    confirm that the translator the estate names for a pattern really
    carries the capability, whether or not this command is the one that
    renders it."""

    return (DbtProductTranslator(), PublicationTranslator(), LocalIngestionTranslator())


def _landing_schemas(
    ctx: EstateContext, entries: Mapping[str, tuple[dict, declaration_mod.ValidatedProduct]]
) -> dict[str, contract_mod.RelationSchema]:
    """Every relation schema a composition may open with that no product in
    this estate derives: the fixture-bound sources' declared fields, plus
    the ingestion side's own landing schemas when the estate carries
    source-delivery declarations at all. An estate with none -- a product-only
    estate proving itself on fixture relations -- contributes nothing from
    that side, which is a fact about the estate, not a fallback."""

    schemas = dict(contract_mod.fixture_relation_schemas(entries))
    if ctx.declarations_dir.is_dir() and any(ctx.declarations_dir.glob("*.yml")):
        schemas.update(emit_contracts.landing_schemas_from_typed_declarations(ctx))
    return schemas


def _rule_signatures(
    document: dict,
    *,
    catalogue: rules_mod.RuleCatalogue,
    adapters: tuple[str, ...],
) -> dict[str, RuleSignature]:
    """Every named rule one product references -- a ``calculated_fields``
    field computed by one, or the rule a ``data_curation`` occurrence scores
    its candidate pairs with -- resolved to the dbt macro that implements it
    on every declared adapter, its declared input names and its catalogue
    version."""

    implementations = {**rules_mod.REFERENCE_IMPLEMENTATIONS, **catalogue.implementations}
    signatures: dict[str, RuleSignature] = {}

    def add(name: object) -> None:
        if not isinstance(name, str) or name in signatures:
            return
        rule = catalogue.resolve(name)
        signatures[name] = RuleSignature(
            macro=rules_mod.dbt_implementation_macro(
                name, implementations=implementations, adapters=adapters
            ),
            inputs=tuple(rule_input.name for rule_input in rule.inputs),
            version=rule.version,
        )

    for step in document.get("steps") or []:
        pattern = (step or {}).get("pattern")
        if pattern == "calculated_fields":
            for entry in step.get("fields") or []:
                add(entry.get("rule"))
        elif pattern == "data_curation":
            add(((step.get("resolution") or {}).get("scoring") or {}).get("rule"))
    return signatures


def resolve_estate_relations(
    ctx: EstateContext,
    *,
    products_dir: Path | None = None,
    configuration: EstateConfiguration | None = None,
) -> tuple[dict, dict, dict]:
    """Every product declaration in the estate, the schemas a composition
    may open with, and the relations each product's shape renders --
    resolved from the declarations alone, before any shape's durable record
    is read.

    Two callers read it. The planning pass below goes on to grade those
    records and route the occurrences, and it is the routing that holds a
    landing product's fixture binding to the relation its owner reads
    (``_check_landing_binding``): which relation that is depends on the
    translator the estate's table gives the shape, which is not known here. The re-baseline operation
    (``ergasterion.shapes.data_vault.rebaseline``) stops here: the declared
    change that prompts a re-baseline is exactly what the grading fails
    closed on, so the operation that resolves it cannot go through the
    grading."""

    directory = products_dir if products_dir is not None else ctx.declarations_dir / "products"
    configuration = configuration if configuration is not None else estate_configuration(ctx)
    entries = graph_mod.load_product_entries(directory, policy=configuration.policy)
    opening_schemas = _landing_schemas(ctx, entries)
    # The constraints a shape adds are read off the declarations around it,
    # so they are checked before any schema is resolved: a product whose
    # shape refuses the arrangement it is declared in is told that, rather
    # than being told whatever resolving an arrangement its shape forbids
    # ran into first.
    _check_shape_constraints(entries)
    _check_interface_boundaries(ctx, entries)
    resolved = contract_mod.resolve_relation_registry(entries, landing_schemas=opening_schemas)
    return entries, opening_schemas, resolved


def build_product_plans(
    ctx: EstateContext | None = None,
    *,
    products_dir: Path | None = None,
    configuration: EstateConfiguration | None = None,
) -> tuple[tuple[ProductPlan, ...], dict, graph_mod.ProductGraph]:
    """Validate, plan and route every product declaration in the estate.
    Returns the rendering plans in the graph's build order, the validated
    declarations they were built from, and the graph itself.

    ``configuration`` is this estate's already-loaded configuration. A caller
    that has read the estate hands it in so the route reads the estate once;
    a caller that has not (a test planning one fixture estate) omits it and
    this reads it."""

    plans, entries, graph, _ledgers = plan_estate(
        ctx, products_dir=products_dir, configuration=configuration
    )
    return plans, entries, graph


def plan_estate(
    ctx: EstateContext | None = None,
    *,
    products_dir: Path | None = None,
    configuration: EstateConfiguration | None = None,
) -> tuple[tuple[ProductPlan, ...], dict, graph_mod.ProductGraph, dict[str, ShapeLedger]]:
    """The whole planning pass, once: the rendering plans, the declarations,
    the graph, and the durable record each shape keeps. ``generate`` reads
    all four, so one emission grades every ledger exactly once."""

    ctx = ctx or _DEFAULT_CTX
    directory = products_dir if products_dir is not None else ctx.declarations_dir / "products"
    configuration = configuration if configuration is not None else estate_configuration(ctx)
    adapters = configuration.adapter_names()
    table = configuration.translators

    catalogue = rules_mod.load_rule_catalogue(estate_dir=ctx.root / "rules")
    rules_mod.validate_estate_layer2(directory, catalogue=catalogue, adapters=adapters)

    entries, opening_schemas, resolved = resolve_estate_relations(
        ctx, products_dir=directory, configuration=configuration
    )
    ledgers = shape_ledgers(ctx, entries, resolved)
    graph = graph_mod.build_product_graph(entries)
    materialisation = {node.published_name: node.materialisation for node in graph.nodes}

    strategies = {
        adapter: load_adapter_conventions(adapter).incremental_strategy for adapter in adapters
    }
    translators = _estate_translators()
    plans: list[ProductPlan] = []
    for published_name in graph.order():
        document, validated = entries[published_name]
        steps = document.get("steps") or []
        occurrences = [(f"steps[{index}]", step["pattern"]) for index, step in enumerate(steps)]
        occurrences.append((CHECKPOINT_OCCURRENCE, "checkpoint_retries"))
        occurrences.append((SHAPE_OCCURRENCE, validated.shape))
        assignments = resolve_table_owners(
            occurrences=occurrences,
            label=validated.layer,
            translator_table=table,
            adapters=adapters,
            translators=translators,
        )
        owned = {
            assignment.occurrence_id
            for assignment in assignments
            if assignment.translator_name == TRANSLATOR_NAME
        }
        # Who renders this product's relations: the translator the estate's
        # table names for its shape (architecture section 9, "each occurrence
        # to exactly one translator declaring (pattern or shape, translator,
        # adapter)"). A product whose shape belongs to another translator is
        # not rendered here at all, so an occurrence of it routed to dbt
        # would be an occurrence nothing renders: the table says two
        # translators own one relation, and that fails closed rather than
        # silently dropping the occurrence.
        shape_owner = next(
            assignment.translator_name
            for assignment in assignments
            if assignment.occurrence_id == SHAPE_OCCURRENCE
        )
        if shape_owner != TRANSLATOR_NAME and owned:
            raise ProductEmissionError(
                f"product {published_name!r}: label {validated.layer!r} gives shape "
                f"{validated.shape!r} to translator {shape_owner!r} and occurrence(s) "
                f"{sorted(owned)!r} to {TRANSLATOR_NAME!r}; the translator that renders a "
                "product's relations renders every occurrence of it"
            )
        if validated.profile == graph_mod.LANDING_PROFILE:
            _check_landing_binding(
                document,
                validated,
                published_name=published_name,
                shape_owner=shape_owner,
            )
        # Which relation each read resolves to has exactly one owner
        # (``ergasterion.framework.contract.resolve_consumer_reads``), so
        # the schema this propagation starts from, the model the rendered
        # read is built over and the relation the estate graph's edge names
        # are always the same relation.
        reads = contract_mod.resolve_consumer_reads(
            document,
            consumer=published_name,
            resolved=resolved,
            opening_schemas=opening_schemas,
        )
        source_schemas = {
            **opening_schemas,
            **{read.key: read.schema for read in reads},
        }
        opening, per_occurrence = contract_mod.propagate_relation_fields(
            document, validated, source_schemas=source_schemas
        )
        # How the sources open that propagation, for the translator that
        # renders the read. It is the same resolution the propagation just
        # used (``propagate_relation_fields`` opens with this function), so
        # the plan carries the relation the published contract was derived
        # from rather than a second reading of the declaration.
        composition = contract_mod.resolve_opening_composition(
            document, consumer=published_name, source_schemas=source_schemas
        )
        plans.append(
            ProductPlan(
                published_name=published_name,
                domain=validated.domain,
                name=validated.name,
                layer=validated.layer,
                profile=validated.profile,
                shape=validated.shape,
                shape_owner=shape_owner,
                document=document,
                relations=resolved[published_name],
                composition=composition,
                opening_fields=opening,
                occurrence_fields=per_occurrence,
                owned_steps=frozenset(
                    index for index in range(len(steps)) if f"steps[{index}]" in owned
                ),
                checkpoint_owned=CHECKPOINT_OCCURRENCE in owned,
                source_models=_source_models(document, entries, reads=reads),
                lookup_models=_lookup_models(document, entries, reads=reads),
                rule_signatures=_rule_signatures(document, catalogue=catalogue, adapters=adapters),
                incremental_strategies=strategies,
                materialisation=materialisation[published_name],
                store_versions=(
                    ledgers[published_name].store_versions
                    if published_name in ledgers
                    else {}
                ),
            )
        )
    return tuple(plans), entries, graph, ledgers


def shape_ledgers(
    ctx: EstateContext,
    entries: Mapping[str, tuple[dict, declaration_mod.ValidatedProduct]],
    resolved: Mapping[str, tuple[contract_mod.ShapeRelation, ...]],
) -> dict[str, ShapeLedger]:
    """Every durable record the estate's shapes keep, graded against what
    the estate carries from the last emission
    (``ergasterion.framework.shapes.ShapeDefinition.ledger``).

    Most shapes keep none. A shape whose rendering depends on what earlier
    runs already stored reads its record here, before anything is
    rendered, so a declared change the record cannot absorb stops the whole
    emission rather than half of it. The graded record goes out with every
    other generated file, so ``--check`` reports a hand-edited or stale one
    exactly as it reports a drifted model."""

    ledgers: dict[str, ShapeLedger] = {}
    for published_name, (document, validated) in sorted(entries.items()):
        shape = get_shape(validated.shape)
        relative = shape.ledger_relative_path(domain=validated.domain, name=validated.name)
        if relative is None:
            continue
        path = ctx.root / relative
        recorded = (
            yaml.safe_load(path.read_text(encoding="utf-8")) if path.is_file() else None
        )
        ledger = shape.ledger(
            product=published_name,
            shape_config=(document.get("target") or {}).get("shape_config") or {},
            relations=resolved.get(published_name, ()),
            recorded=recorded,
        )
        if ledger is not None:
            ledgers[published_name] = ledger
    return ledgers


def _check_landing_binding(
    document: dict,
    validated: declaration_mod.ValidatedProduct,
    *,
    published_name: str,
    shape_owner: str,
) -> None:
    """The relation a landing product binds, against the translator the
    estate's table gives that product's shape to (owner ruling R5,
    architecture section 9).

    A landing product publishes one relation, and its binding names the
    relation it lands. Which of the two the binding must name follows from
    who renders the product:

      * another translator renders it -- the ingestion runtime lands the
        relation outside the dbt project -- and every consumer reads it as
        the model of the product's own relation, so the bound relation is
        that same one under the name this route reads it by. A binding
        naming a second relation would leave the consumers reading one
        nothing lands;
      * this translator renders it, as a model over the relation it lands,
        so the bound relation is the one that model reads and must be a
        different relation from the model itself. A binding naming the
        product's own relation would render a model reading itself, which
        dbt only reports as a cycle at build time.

    Either way the failure names the product, the source and both names. A
    fixture binding on a non-landing product is a different thing -- a
    contract this estate does not publish at all -- and is not governed by
    this rule."""

    model = model_name(validated.domain, validated.name)
    renders_here = shape_owner == TRANSLATOR_NAME
    for index, source in enumerate(document.get("sources") or []):
        binding = (source or {}).get("fixture")
        if binding is None:
            continue
        declared = str(binding.get("relation"))
        contract_reference = str((source or {}).get("contract"))
        if renders_here and declared == model:
            raise ProductEmissionError(
                f"product {published_name!r}: sources[{index}] {contract_reference!r} binds "
                f"relation {declared!r}, and label {validated.layer!r} gives this product's "
                f"shape to {shape_owner!r}, which renders it as the model {model!r} over the "
                "relation it lands; the bound relation is what that model reads, so it names "
                "the landed relation rather than the model itself"
            )
        if not renders_here and declared != model:
            raise ProductEmissionError(
                f"product {published_name!r}: sources[{index}] {contract_reference!r} binds "
                f"relation {declared!r}, and label {validated.layer!r} gives this product's "
                f"shape to {shape_owner!r}, which lands the relation outside this route; every "
                f"consumer therefore reads it as {model!r}, so the binding names that one"
            )


def _check_shape_constraints(
    entries: Mapping[str, tuple[dict, declaration_mod.ValidatedProduct]]
) -> None:
    """Every constraint a shape adds that only the estate can answer.
    Layer 1 validates one declaration at a time and cannot see the
    products around it; a shape that requires something of its upstream --
    the canonical shape requires a curated one -- is checked here, once
    every declaration in the estate is known, and fails closed naming the
    product, the shape and the upstream."""

    for published_name, (document, validated) in sorted(entries.items()):
        sources = {
            str(source["contract"]).split("@", 1)[0] for source in document.get("sources") or []
        }
        get_shape(validated.shape).check_estate(
            product=published_name,
            shape_config=(document.get("target") or {}).get("shape_config") or {},
            document=document,
            upstream={
                name: upstream_document
                for name, (upstream_document, _validated) in entries.items()
                if name in sources
            },
        )


def _check_interface_boundaries(
    ctx: EstateContext, entries: Mapping[str, tuple[dict, declaration_mod.ValidatedProduct]]
) -> None:
    """A shape that renders an interface renders views, and a view lives
    only where the estate declares an interface boundary
    (``declarations/targets/interfaces.yml``). The estate declares it; this
    route enforces it, and a product whose shape names a path the estate
    has not declared fails closed naming product, shape and path rather
    than emitting a view the structural gate would later reject."""

    interfaces = {
        published_name: (validated.shape, get_shape(validated.shape).interface_path)
        for published_name, (_document, validated) in sorted(entries.items())
        if get_shape(validated.shape).interface_path is not None
    }
    if not interfaces:
        return
    _declarations, view_layers = load_structure_declarations(ctx)
    for published_name, (shape, interface_path) in interfaces.items():
        path = f"{MODELS_ROOT}/{interface_path}"
        if path not in view_layers:
            raise ProductEmissionError(
                f"product {published_name!r}: shape {shape!r} renders its relations as views at "
                f"{path!r}, which this estate does not declare as an interface boundary "
                f"(declared: {', '.join(view_layers) or 'none'})"
            )


def _source_models(
    document: dict,
    entries: Mapping[str, tuple[dict, declaration_mod.ValidatedProduct]],
    *,
    reads: tuple[contract_mod.ResolvedRead, ...],
) -> dict[str, str]:
    """The relation each source is read from, keyed the way the translator
    looks it up (``ergasterion.framework.contract.source_relation_key``): a
    fixture-bound source reads the fixture relation the declaration names,
    and every other source reads the model of the one relation it resolved
    to -- the product's own model where the producer publishes a single
    composition relation, and the suffixed model of the named relation
    where its shape renders several."""

    resolved_reads = {
        read.key: read for read in reads if read.kind == contract_mod.READ_SOURCE
    }
    models: dict[str, str] = {}
    for source in document.get("sources") or []:
        key = contract_mod.source_relation_key(
            source.get("contract"), contract_mod.declared_source_relation(source)
        )
        binding = source.get("fixture")
        if binding is not None:
            models[key] = binding["relation"]
            continue
        read = resolved_reads[key]
        _document, validated = entries[read.producer]
        models[key] = (
            model_name(validated.domain, validated.name)
            if read.suffix is None
            else suffixed_model_name(validated.domain, validated.name, read.suffix)
        )
    return models


def _lookup_models(
    document: dict,
    entries: Mapping[str, tuple[dict, declaration_mod.ValidatedProduct]],
    *,
    reads: tuple[contract_mod.ResolvedRead, ...],
) -> dict[str, str]:
    """The model each ``data_enrichment`` lookup reads, keyed the way the
    translator looks it up
    (``ergasterion.framework.contract.source_relation_key``): the product's
    own model where the producer publishes a single composition relation,
    and the suffixed model of the named relation where its shape renders
    several. A lookup always names a product this estate publishes -- the
    graph fails closed otherwise -- so there is no fixture form here."""

    models: dict[str, str] = {}
    for read in reads:
        if read.kind != contract_mod.READ_LOOKUP:
            continue
        _document, validated = entries[read.producer]
        models[read.key] = (
            model_name(validated.domain, validated.name)
            if read.suffix is None
            else suffixed_model_name(validated.domain, validated.name, read.suffix)
        )
    return models


def private_model_names(
    plans: Sequence[ProductPlan], translator: DbtProductTranslator
) -> frozenset[str]:
    """Every model this route rendered as a translator-private relation.

    The translator registers each private relation under the product's own
    namespace (architecture section 10), so the set is read back off those
    registrations: the suffix is what the registered relation carries beyond
    the product's own relation prefix, and the model name is rebuilt from it
    through the translator's own naming function. Nothing here matches a path
    or a name ending, and a registration that does not sit under the product
    it names fails closed rather than being skipped.

    A relation registered for the estate rather than a product sits under no
    product namespace, so its model name comes straight off the two parts of
    the relation name, the same derivation the standalone gate makes from the
    manifests."""

    by_product = {plan.published_name: plan for plan in plans}
    names: set[str] = set()
    for entry in translator.auxiliary_relations():
        if entry.product == graph_mod.ESTATE_SCOPE:
            namespace, _, relation = entry.relation.partition(".")
            if not namespace or not relation:
                raise ProductEmissionError(
                    f"estate relation {entry.relation!r} is not a qualified "
                    "'<namespace>.<relation>' name"
                )
            names.add(f"{namespace}__{relation}")
            continue
        plan = by_product.get(entry.product)
        if plan is None:
            raise ProductEmissionError(
                f"private relation {entry.relation!r} is registered for product "
                f"{entry.product!r}, which this emission did not plan"
            )
        prefix = suffixed_relation_name(plan.domain, plan.name, "")
        if not entry.relation.startswith(prefix):
            raise ProductEmissionError(
                f"private relation {entry.relation!r} does not sit under the namespace "
                f"{prefix!r} of the product {entry.product!r} that registered it"
            )
        names.add(suffixed_model_name(plan.domain, plan.name, entry.relation[len(prefix):]))
    return frozenset(names)


def structural_gate(
    ctx: EstateContext, *, adapters: Sequence[str], private_models: frozenset[str]
) -> list[Offense]:
    """The estate's structural budgets over the product tree, once per
    declared adapter (architecture section 9's gate stage).

    The scan is scoped to the tree this route owns, so the budgets bind what
    it wrote and nothing else. Every adapter the estate declares must declare
    its budgets under ``declarations/targets/``; one that does not fails
    closed naming the estate and the adapter, because a gate that silently
    skipped an adapter would report a pass it never ran. Translator-private
    relations are exempt from the interface-boundary rule alone (they are not
    interfaces) and bound by every other budget, the view-chain depth
    included."""

    gate_ctx = dataclasses.replace(ctx, models_dir=ctx.root / MODELS_ROOT)
    directory = targets_dir(gate_ctx)
    missing = [adapter for adapter in adapters if not (directory / f"{adapter}.yml").is_file()]
    if missing:
        raise ProductEmissionError(
            f"{ctx.root}: adapter(s) {missing!r} are declared in estate.yml with no structural "
            f"budget declaration under {directory}"
        )
    return check_structure(gate_ctx, non_interface_views=private_models)


def report_structure(offenses: Sequence[Offense], *, adapters: Sequence[str]) -> None:
    """Print the structural stage's verdict, one line per offense naming the
    adapter, the rule and the artefact."""

    for offense in offenses:
        print(
            f"  [{offense.adapter}] {offense.budget}: {offense.artefact} -- {offense.message}"
        )
    print(
        f"structural budgets: {len(offenses)} offense(s) over {len(adapters)} declared "
        f"adapter(s) ({', '.join(adapters)})"
    )


def generate(
    ctx: EstateContext | None = None, *, products_dir: Path | None = None
) -> EmissionResult:
    """Every artefact the product route emits, keyed by its path in the
    estate, with one summary line per product naming its label, profile,
    shape, the adapters it was gated for and how many artefacts it emitted
    (owner ruling R6, plan decision D32). Runs the parse gate and the
    dialect deny-list once per declared adapter over the generated text
    before returning it, so nothing that fails a gate is ever written or
    reported as clean."""

    ctx = ctx or _DEFAULT_CTX
    configuration = estate_configuration(ctx)
    declared_adapters = configuration.adapters
    plans, entries, _graph, ledgers = plan_estate(
        ctx, products_dir=products_dir, configuration=configuration
    )
    # The route hands the dbt translator only the products the estate's
    # table routes to it. A product whose shape belongs to another
    # translator is emitted by that translator's own route; its contract
    # still resolves here, so its consumers read it, and the summary below
    # still names it with its owner.
    rendered = tuple(plan for plan in plans if plan.shape_owner == TRANSLATOR_NAME)
    translator = DbtProductTranslator(products=rendered)
    result = translator.translate()

    # Re-resolving the graph with the translator's own registrations is what
    # places every private relation: it fails closed on one attached to a
    # product this estate does not declare, and on one whose name is already
    # a published relation (architecture section 10).
    graph_mod.build_product_graph(
        entries, auxiliary=graph_mod.collect_auxiliary_relations([translator])
    )

    # The gate stage runs once per declared adapter, reference and deployment
    # alike (architecture section 9): every artefact parses for the adapter,
    # then passes its deny list. Each adapter's deny list is its own
    # portability rule set, so a construct only one platform understands is a
    # defect wherever it is generated, and every failure names the artefact,
    # the rule and the adapter.
    parse_gate.assert_parses(result.artefacts, adapters=declared_adapters.names())
    for adapter_name in declared_adapters.names():
        offenses = lint_artefacts(result.artefacts, adapter_name)
        if offenses:
            detail = "; ".join(
                f"{offense.path.as_posix()}:{offense.line_no} [{offense.token}] {offense.message}"
                for offense in offenses
            )
            raise ProductEmissionError(
                f"dialect gate failed for adapter {adapter_name!r}: {detail}"
            )

    files = {ctx.root / name: text for name, text in result.artefacts.items()}
    for ledger in ledgers.values():
        files[ctx.root / ledger.relative_path] = contract_mod.dump_yaml(dict(ledger.document))
    artefact_counts = {
        str(summary["product"]): int(summary["artefacts"])
        for summary in result.metadata["products"]
    }
    summaries = [
        "emitted {product}: label={label} profile={profile} shape={shape} owner={owner} "
        "adapters={adapters} artefacts={artefacts}".format(
            product=plan.published_name,
            label=plan.layer,
            profile=plan.profile,
            shape=plan.shape,
            owner=plan.shape_owner,
            adapters=",".join(declared_adapters.names()),
            artefacts=artefact_counts.get(plan.published_name, 0),
        )
        for plan in plans
    ]
    return EmissionResult(
        files=files,
        summaries=summaries,
        adapters=declared_adapters.names(),
        private_models=private_model_names(rendered, translator),
        notices=tuple(
            notice for ledger in ledgers.values() for notice in ledger.notices
        ),
    )


def write_files(files: dict[Path, str], *, ctx: EstateContext | None = None) -> list[Path]:
    ctx = ctx or _DEFAULT_CTX
    changed = contract_mod.write_generated_files(
        {path: text for path, text in files.items() if _under(path, ctx, MODELS_ROOT)},
        directory=ctx.root / MODELS_ROOT,
        prune_patterns=MODEL_PRUNE_PATTERNS,
    )
    changed.extend(
        contract_mod.write_generated_files(
            {path: text for path, text in files.items() if _under(path, ctx, MANIFESTS_ROOT)},
            directory=ctx.root / MANIFESTS_ROOT,
            prune_patterns=MANIFEST_PRUNE_PATTERNS,
        )
    )
    changed.extend(
        contract_mod.write_generated_files(
            _ledger_files(files, ctx),
            directory=ctx.root,
            prune_patterns=LEDGER_PRUNE_PATTERNS,
        )
    )
    return changed


def check_files(files: dict[Path, str], *, ctx: EstateContext | None = None) -> list[str]:
    ctx = ctx or _DEFAULT_CTX
    problems = contract_mod.check_generated_files(
        {path: text for path, text in files.items() if _under(path, ctx, MODELS_ROOT)},
        directory=ctx.root / MODELS_ROOT,
        root=ctx.root,
        prune_patterns=MODEL_PRUNE_PATTERNS,
    )
    problems.extend(
        contract_mod.check_generated_files(
            {path: text for path, text in files.items() if _under(path, ctx, MANIFESTS_ROOT)},
            directory=ctx.root / MANIFESTS_ROOT,
            root=ctx.root,
            prune_patterns=MANIFEST_PRUNE_PATTERNS,
        )
    )
    problems.extend(
        contract_mod.check_generated_files(
            _ledger_files(files, ctx),
            directory=ctx.root,
            root=ctx.root,
            prune_patterns=LEDGER_PRUNE_PATTERNS,
        )
    )
    return problems


def _ledger_files(files: dict[Path, str], ctx: EstateContext) -> dict[Path, str]:
    """Whatever one emission produced outside the two trees this route
    owns: the durable record each of the estate's shapes keeps, at the path
    that shape declared for it
    (``ergasterion.framework.shapes.ShapeDefinition.ledger_relative_path``).
    Written and checked like every other generated file, and never pruned:
    the route writes the records of the shapes this estate declares and has
    no business removing a file it did not write."""

    return {
        path: text
        for path, text in files.items()
        if not _under(path, ctx, MODELS_ROOT) and not _under(path, ctx, MANIFESTS_ROOT)
    }


def _under(path: Path, ctx: EstateContext, root: str) -> bool:
    return path.is_relative_to(ctx.root / root)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Emit the dbt project for every product declared under the estate's "
            "declarations/products/ tree."
        )
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Report drift against what is on disk without writing anything.",
    )
    parser.add_argument(
        "--estate-root",
        type=Path,
        default=None,
        help="Root of the estate to emit against (resolved from the environment or cwd when omitted).",
    )
    parser.add_argument(
        "--binding",
        type=Path,
        default=None,
        help="RuntimeBinding YAML file or directory for production Landing contract projections.",
    )
    parser.add_argument(
        "--environment",
        default=None,
        help="Mandatory matching assertion against RuntimeBinding.environment.",
    )
    args = parser.parse_args(argv)
    ctx = EstateContext.resolve(estate_root=args.estate_root)

    try:
        emission = generate(ctx)
    except (FrameworkError, ValueError) as error:
        sys.stderr.write(f"FAIL: {error}\n")
        return 1

    for notice in emission.notices:
        print(f"notice: {notice}")
    for line in emission.summaries:
        print(line)

    files = emission.files
    problems: list[str] = []
    if args.check:
        problems = check_files(files, ctx=ctx)
        for problem in problems:
            print(problem)
        print(f"checked {len(files)} generated file(s); {len(problems)} problem(s)")
    else:
        changed = write_files(files, ctx=ctx)
        print(f"generated {len(changed)} of {len(files)} file(s)")
        for path in sorted(changed):
            print(path.relative_to(ctx.root).as_posix())

    # The structural budgets bind what is on disk, so they run after the write
    # (and, in check mode, over the tree already there): a budget is a property
    # of the estate's model tree, not of one product's rendered text.
    try:
        offenses = structural_gate(
            ctx, adapters=emission.adapters, private_models=emission.private_models
        )
    except (FrameworkError, ValueError) as error:
        sys.stderr.write(f"FAIL: {error}\n")
        return 1
    report_structure(offenses, adapters=emission.adapters)
    if problems or offenses:
        return 1

    downstream_args = ["--estate-root", str(ctx.root)]
    if args.check:
        downstream_args.insert(0, "--check")
    if args.binding is not None:
        downstream_args.extend(["--binding", str(args.binding)])
    if args.environment is not None:
        downstream_args.extend(["--environment", args.environment])

    for emitter in (emit_contracts, emit_odps):
        code = emitter.main(downstream_args)
        if code:
            return code

    graph_args = ["--estate-root", str(ctx.root)]
    if args.check:
        graph_args.insert(0, "--check")
    return emit_graph.main(graph_args)


if __name__ == "__main__":
    raise SystemExit(main())
