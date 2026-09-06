"""The named-rule catalogue and the completeness gate (architecture
sections 3.2, 3.4, 9; owner ruling R3, plan decisions D22, D26, D29).

Two equal routes state a business rule in a declaration (architecture
section 3.2): inline SQL (``ergasterion/framework/expressions.py``) and
named rules (this module). A named rule has one neutral signature -- its
name, its input types, its output type, its version -- declared once in a
rule catalogue and validated against
``ergasterion/schemas/named-rule-catalogue-v1.schema.json``. Its
implementations are code, held in translator and adapter packages, never
in a declaration; this module carries the reference set's implementation
registry (``REFERENCE_IMPLEMENTATIONS``), seeded from the existing
``macros/cross_db.sql`` dispatch macro family.

Two catalogue sources, merged with no shadowing:

  * the engine reference catalogue, ``ergasterion/rules/reference/*.yml``,
    shipped as package data and loaded as the estate's defaults;
  * an optional estate catalogue, ``rules/*.yml`` at the estate root, which
    may ADD rules the reference catalogue does not carry.

A rule name declared more than once -- within either source, or across
both -- fails closed with ``RuleCollisionError``: an estate can extend the
reference catalogue, never redefine a piece of it.

The completeness gate (``check_completeness``) is the other half of
architecture section 3.4's split: every rule a validated product actually
references must have an implementation for every declared (translator,
adapter) pair, or emission fails closed naming the rule and the pair
(check 9). It is a pure function of its inputs -- the referenced rule
names, an implementation registry, and the declared translators and
adapters -- so it needs no live estate translator table to be exercised
(that table is ``estate.yml``'s translator-table concern for
ROUTING; the (translator, adapter) pairs this gate checks against are
already fixed by owner ruling R10 for this estate: the dbt translator,
DuckDB and BigQuery). A later item wires the estate's own declared pairs
through the same ``translators=``/``adapters=`` parameters.

``validate_product_expressions_and_rules`` and ``validate_estate_layer2``
are the layer-2 validate function later items call (architecture section
9's ``validate`` step, beyond layer 1's schema/composition/neutrality
checks in ``ergasterion/framework/declaration.py``, which this module does
not import and does not modify). They assume layer 1 has already run: a
document reaching them is schema-valid and has already passed the
neutrality gate, so under ``named_only`` estate policy it simply carries no
inline expression left to parse.

The schema visible at an occurrence, as tracked here, is an additive
approximation: it starts from the union of every source's declared
``expect.fields`` and grows as ``schema_transform`` (its ``mapping[].to``
names), ``calculated_fields`` (each field's own name, in step order, so a
later field may reference an earlier one), ``data_enrichment`` (its
``lookups[].fields`` names) and ``data_aggregation`` (each aggregate's own
name) add columns. It does not model ``schema_transform``'s drop semantics
or ``data_curation``'s survivorship narrowing: a later item may replace it
with the full plan-level schema propagation architecture section 9
describes."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Mapping

import jsonschema
import yaml

from ergasterion.framework import expressions as expr_mod
from ergasterion.framework.expression_port import ExpressionParserPort
from ergasterion.framework.models import FrameworkError, PatternId

SCHEMAS_DIR = Path(__file__).resolve().parent.parent / "schemas"
RULE_CATALOGUE_SCHEMA_PATH = SCHEMAS_DIR / "named-rule-catalogue-v1.schema.json"
REFERENCE_RULES_DIR = Path(__file__).resolve().parent.parent / "rules" / "reference"

# The (translator, adapter) universe fixed by owner ruling R10 for this
# estate: dbt is the SQL translator; DuckDB (reference) and BigQuery
# (deployment, final target) are the declared adapters. They are codified in
# estate.yml's translator table for ROUTING; these are the same pairs, used
# here as the completeness gate's default so this module needs no estate
# configuration to be exercised end to end.
DBT_TRANSLATOR = "dbt"
REFERENCE_ADAPTERS: tuple[str, ...] = ("duckdb", "bigquery")


# --------------------------------------------------------------------------- errors


class RuleCatalogueError(FrameworkError):
    """A rule catalogue document does not validate against the named-rule
    catalogue schema, or is otherwise malformed. An estate misconfiguration,
    not a single product's fault."""

    code = "rule_catalogue_error"


class RuleCollisionError(FrameworkError):
    """A named rule is declared more than once: within one catalogue
    source, or across the reference and estate catalogues. Architecture
    section 3.2's "no shadowing" -- an estate catalogue may ADD rules, it
    may never redefine one."""

    code = "rule_collision"

    def __init__(self, *, name: str, first_source: Path, second_source: Path) -> None:
        self.name = name
        self.first_source = first_source
        self.second_source = second_source
        super().__init__(
            f"named rule {name!r} is declared more than once, with no shadowing: "
            f"first in {first_source}, again in {second_source}"
        )


class UnknownRuleError(FrameworkError):
    """A declaration references a named rule no catalogue source carries."""

    code = "unknown_rule"

    def __init__(self, name: object) -> None:
        self.name = name
        super().__init__(f"unknown named rule: {name!r}")


class RuleVersionMismatchError(FrameworkError):
    """A declaration's ``rule_version`` does not match the version the
    catalogue actually carries for that rule name. Names the product, the
    occurrence, the rule and both versions, mirroring
    ``ergasterion.framework.expressions.ExpressionError``'s identity: a
    declaration pins a version so a later catalogue bump cannot silently
    change generated behaviour underneath it, and a mismatch fails closed
    rather than resolving against whichever version the catalogue happens
    to carry."""

    code = "rule_version_mismatch"

    def __init__(self, *, product: str, occurrence: str, rule: str, declared_version: int, catalogue_version: int) -> None:
        self.product = product
        self.occurrence = occurrence
        self.rule = rule
        self.declared_version = declared_version
        self.catalogue_version = catalogue_version
        super().__init__(
            f"product {product!r}: rule {rule!r} occurrence {occurrence!r}: "
            f"declared rule_version {declared_version!r} does not match the catalogue's version {catalogue_version!r}"
        )


# --------------------------------------------------------------------------- catalogue


@dataclass(frozen=True)
class RuleInput:
    """One named rule input's neutral signature slot: a name and a neutral
    type (a scalar type name, or an object such as ``{"name": "decimal",
    ...}`` or an estate structured type -- this module does not itself
    enforce the neutral type system's shape; that is
    ``ergasterion.framework.declaration._validate_type``'s job when a
    catalogue-defined type is used as a product's own field type)."""

    name: str
    type: Any


@dataclass(frozen=True)
class Rule:
    """One named rule's neutral signature (architecture section 3.2): its
    name, version, input types and output type, and the human-readable
    description its catalogue entry carries. Implementations are held
    separately, in an implementation registry such as
    ``REFERENCE_IMPLEMENTATIONS``, never on this dataclass: architecture
    section 3.2, "implementations are code ... never in a declaration",
    and never in the catalogue document either."""

    name: str
    version: int
    inputs: tuple[RuleInput, ...]
    output_type: Any
    description: str = ""


@dataclass(frozen=True)
class RuleCatalogue:
    """The merged, collision-free set of named rules an estate resolves
    references against: the engine reference catalogue plus, optionally, an
    estate catalogue that added to it.

    ``implementations`` carries any per-rule ``implementations:`` a
    catalogue document declared inline (see the schema's ``implementations``
    property): a rule the ENGINE reference catalogue does not ship still
    needs a real (translator, adapter) completeness answer, and
    architecture section 3.2's "implementations are code" is honoured by
    treating this as a lightweight manifest naming a dispatch macro, never
    business logic itself. Empty for a catalogue that declares no inline
    implementations -- the common case, since most estate-declared rules
    beyond the engine's own resolve purely through
    ``ergasterion.framework.rules.REFERENCE_IMPLEMENTATIONS``."""

    rules: Mapping[str, Rule]
    implementations: Mapping[str, tuple["RuleImplementation", ...]] = field(default_factory=dict)

    def __contains__(self, name: object) -> bool:
        return name in self.rules

    def resolve(self, name: str) -> Rule:
        """The rule named ``name``, or ``UnknownRuleError`` when no
        catalogue source carries it."""

        try:
            return self.rules[name]
        except KeyError:
            raise UnknownRuleError(name) from None


_CATALOGUE_SCHEMA_CACHE: dict | None = None


def _catalogue_schema() -> dict:
    global _CATALOGUE_SCHEMA_CACHE
    if _CATALOGUE_SCHEMA_CACHE is None:
        _CATALOGUE_SCHEMA_CACHE = json.loads(RULE_CATALOGUE_SCHEMA_PATH.read_text(encoding="utf-8"))
    return _CATALOGUE_SCHEMA_CACHE


def _load_rule_document(path: Path) -> tuple[list[Rule], dict[str, tuple["RuleImplementation", ...]]]:
    """Load and schema-validate one catalogue document, returning its
    rules and any inline ``implementations:`` each rule entry declared
    (see the schema's optional ``implementations`` property; usually
    empty). Fails closed with ``RuleCatalogueError`` on a schema
    violation, or ``RuleCollisionError`` on a name repeated within this
    one file."""

    document = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    validator = jsonschema.Draft202012Validator(_catalogue_schema())
    schema_errors = sorted(validator.iter_errors(document), key=lambda e: list(e.path))
    if schema_errors:
        first = schema_errors[0]
        location = "/".join(str(part) for part in first.path) or "<root>"
        raise RuleCatalogueError(f"{path}: {location}: {first.message}")

    rules: list[Rule] = []
    implementations: dict[str, tuple["RuleImplementation", ...]] = {}
    seen_in_file: set[str] = set()
    for entry in document.get("rules", []):
        name = entry["name"]
        if name in seen_in_file:
            raise RuleCollisionError(name=name, first_source=path, second_source=path)
        seen_in_file.add(name)
        inputs = tuple(RuleInput(name=i["name"], type=i["type"]) for i in entry.get("inputs", []))
        rules.append(
            Rule(
                name=name,
                version=entry["version"],
                inputs=inputs,
                output_type=entry["output"]["type"],
                description=entry.get("description", ""),
            )
        )
        raw_implementations = entry.get("implementations") or []
        if raw_implementations:
            implementations[name] = tuple(
                RuleImplementation(
                    translator=impl["translator"],
                    adapter=impl.get("adapter"),
                    macro=impl["macro"],
                )
                for impl in raw_implementations
            )
    return rules, implementations


def _load_rules_from_dir(
    directory: Path,
) -> tuple[dict[str, tuple[Rule, Path]], dict[str, tuple["RuleImplementation", ...]]]:
    """Every rule name to ``(Rule, defining file)``, and every rule name to
    its inline implementations (if any), across every ``*.yml``/``*.yaml``
    under ``directory``, sorted for determinism. A missing directory
    yields nothing (an estate need not carry its own catalogue). Fails
    closed with ``RuleCollisionError`` on a name repeated across two files
    within this one directory."""

    found: dict[str, tuple[Rule, Path]] = {}
    implementations: dict[str, tuple["RuleImplementation", ...]] = {}
    if not directory.is_dir():
        return found, implementations
    paths = sorted(set(directory.rglob("*.yml")) | set(directory.rglob("*.yaml")))
    for path in paths:
        rules, file_implementations = _load_rule_document(path)
        for rule in rules:
            if rule.name in found:
                raise RuleCollisionError(name=rule.name, first_source=found[rule.name][1], second_source=path)
            found[rule.name] = (rule, path)
        implementations.update(file_implementations)
    return found, implementations


def load_rule_catalogue(*, reference_dir: Path = REFERENCE_RULES_DIR, estate_dir: Path | None = None) -> RuleCatalogue:
    """Load the engine reference catalogue (ships as defaults) merged with
    an optional estate catalogue that may ADD rules. A rule name appearing
    in both -- or repeated within either -- fails closed with
    ``RuleCollisionError``: no shadowing (architecture section 3.2)."""

    reference_rules, combined_implementations = _load_rules_from_dir(reference_dir)
    combined: dict[str, Rule] = {name: rule for name, (rule, _path) in reference_rules.items()}
    sources: dict[str, Path] = {name: path for name, (rule, path) in reference_rules.items()}

    if estate_dir is not None:
        estate_rules, estate_implementations = _load_rules_from_dir(estate_dir)
        for name, (rule, path) in estate_rules.items():
            if name in combined:
                raise RuleCollisionError(name=name, first_source=sources[name], second_source=path)
            combined[name] = rule
            sources[name] = path
        combined_implementations.update(estate_implementations)

    return RuleCatalogue(rules=combined, implementations=combined_implementations)


# --------------------------------------------------------------------------- implementations


@dataclass(frozen=True)
class RuleImplementation:
    """One (rule, translator) implementation record. ``adapter`` is the one
    declared adapter it covers, or ``None`` when the implementation is
    neutral across every declared adapter (the macro carries no adapter
    divergence). ``macro`` names the dbt dispatch macro that renders it
    (architecture section 3.2: "for the dbt translator an implementation
    names a dispatch macro")."""

    translator: str
    adapter: str | None
    macro: str


def _dispatched(name: str) -> tuple[RuleImplementation, ...]:
    """One implementation record per declared adapter, all naming the same
    dispatch macro: dbt's own ``adapter.dispatch`` resolves the per-adapter
    body inside ``macros/cross_db.sql`` (``default__`` for BigQuery, an
    explicit ``duckdb__`` override for DuckDB) -- the macro NAME the
    translator calls is identical, only the rendered SQL body differs. This
    catalogue still tracks one record per adapter, so removing an
    override's coverage is a real, per-pair completeness gap rather than a
    change invisible to the gate."""

    return tuple(RuleImplementation(translator=DBT_TRANSLATOR, adapter=adapter, macro=name) for adapter in REFERENCE_ADAPTERS)


def _neutral(name: str) -> tuple[RuleImplementation, ...]:
    """One implementation record valid for every declared adapter: the
    macro either carries no ``adapter.dispatch`` call at all, or dispatches
    with no ``duckdb__`` override -- dbt's dispatch falls back to
    ``default__`` on DuckDB, so the one macro body already serves every
    declared adapter (architecture section 3.4: "declare one implementation
    neutral across adapters where the macro has no adapter divergence")."""

    return (RuleImplementation(translator=DBT_TRANSLATOR, adapter=None, macro=name),)


# Seeded from macros/cross_db.sql (architecture section 3.2's precedent):
# thirteen macros with a real duckdb__ override get one implementation
# record per declared adapter; the other seven, which carry no adapter
# divergence across DuckDB and BigQuery, get one neutral record each. Every
# name here has a matching signature in
# ergasterion/rules/reference/cross_db.yml.
REFERENCE_IMPLEMENTATIONS: dict[str, tuple[RuleImplementation, ...]] = {
    "dpf_safe_cast": _dispatched("dpf_safe_cast"),
    "dpf_json_cast": _dispatched("dpf_json_cast"),
    "dpf_regexp_replace": _dispatched("dpf_regexp_replace"),
    "dpf_regexp_contains": _dispatched("dpf_regexp_contains"),
    "dpf_regexp_extract": _dispatched("dpf_regexp_extract"),
    "dpf_hash_hex": _dispatched("dpf_hash_hex"),
    "dpf_empty_array": _dispatched("dpf_empty_array"),
    "dpf_to_json_object": _dispatched("dpf_to_json_object"),
    "dpf_date_trunc": _dispatched("dpf_date_trunc"),
    "dpf_date_series": _dispatched("dpf_date_series"),
    "dpf_edit_distance": _dispatched("dpf_edit_distance"),
    "dpf_date_diff_days": _dispatched("dpf_date_diff_days"),
    "dpf_map_agg": _dispatched("dpf_map_agg"),
    "dpf_type": _neutral("dpf_type"),
    "dpf_safe_divide": _neutral("dpf_safe_divide"),
    "dpf_date_key": _neutral("dpf_date_key"),
    "dpf_array": _neutral("dpf_array"),
    "dpf_string_agg": _neutral("dpf_string_agg"),
    "dpf_array_agg_distinct": _neutral("dpf_array_agg_distinct"),
    "dpf_array_length": _neutral("dpf_array_length"),
}


# --------------------------------------------------------------------------- completeness gate


@dataclass(frozen=True)
class CompletenessGap:
    """One missing (rule, translator, adapter) implementation."""

    rule: str
    translator: str
    adapter: str


class CompletenessError(FrameworkError):
    """The completeness gate found at least one referenced rule with no
    implementation for a declared (translator, adapter) pair (architecture
    section 3.4, check 9). Names every gap found, sorted for determinism."""

    code = "completeness_error"

    def __init__(self, gaps: tuple[CompletenessGap, ...]) -> None:
        self.gaps = gaps
        pairs = ", ".join(f"{gap.rule!r} x ({gap.translator!r}, {gap.adapter!r})" for gap in gaps)
        super().__init__(f"named-rule completeness gate failed for: {pairs}")


def check_completeness(
    *,
    referenced_rules: Iterable[str],
    implementations: Mapping[str, tuple[RuleImplementation, ...]] = REFERENCE_IMPLEMENTATIONS,
    translators: Iterable[str] = (DBT_TRANSLATOR,),
    adapters: Iterable[str] = REFERENCE_ADAPTERS,
) -> None:
    """Fail closed with ``CompletenessError`` naming every (rule,
    translator, adapter) gap: a rule in ``referenced_rules`` with no entry
    in ``implementations`` covering that translator for that adapter
    (``adapter=None`` in an implementation record covers every adapter). A
    rule the catalogue carries but no product ever references is not
    checked: architecture section 3.4, check 9 ties the gate to what a
    validated product actually resolves."""

    translators_tuple = tuple(translators)
    adapters_tuple = tuple(adapters)
    gaps: list[CompletenessGap] = []
    for rule_name in sorted(set(referenced_rules)):
        rule_implementations = implementations.get(rule_name, ())
        for translator in translators_tuple:
            for adapter in adapters_tuple:
                covered = any(
                    impl.translator == translator and impl.adapter in (None, adapter)
                    for impl in rule_implementations
                )
                if not covered:
                    gaps.append(CompletenessGap(rule=rule_name, translator=translator, adapter=adapter))
    if gaps:
        raise CompletenessError(tuple(gaps))


class RuleMacroDivergenceError(FrameworkError):
    """One named rule resolves to more than one dbt macro across the
    declared adapters. A dispatch macro's per-adapter bodies differ; its
    NAME does not, because that name is what a generated model calls
    (architecture section 3.2, "the difference goes through a named rule
    with a dispatch implementation per adapter"). Two names would mean two
    generated texts, one per adapter, which is exactly the estate fork
    architecture section 13 forbids."""

    code = "rule_macro_divergence"

    def __init__(self, *, rule: str, macros: tuple[str, ...]) -> None:
        self.rule = rule
        self.macros = macros
        super().__init__(
            f"named rule {rule!r} resolves to more than one dbt macro across the declared "
            f"adapters: {', '.join(macros)}; one rule renders as one call"
        )


def dbt_implementation_macro(
    rule_name: str,
    *,
    implementations: Mapping[str, tuple[RuleImplementation, ...]] = REFERENCE_IMPLEMENTATIONS,
    adapters: Iterable[str] = REFERENCE_ADAPTERS,
) -> str:
    """The one dbt macro that implements ``rule_name`` on every adapter in
    ``adapters``. Runs the completeness gate for that rule first, so a gap
    fails closed with the same ``CompletenessError`` the estate-wide gate
    raises, then fails closed with ``RuleMacroDivergenceError`` when the
    covering records do not agree on one macro name."""

    check_completeness(
        referenced_rules=[rule_name],
        implementations=implementations,
        translators=(DBT_TRANSLATOR,),
        adapters=adapters,
    )
    macros = sorted(
        {
            implementation.macro
            for implementation in implementations.get(rule_name, ())
            if implementation.translator == DBT_TRANSLATOR
        }
    )
    if len(macros) != 1:
        raise RuleMacroDivergenceError(rule=rule_name, macros=tuple(macros))
    return macros[0]


# --------------------------------------------------------------------------- layer 2: products


def validate_product_expressions_and_rules(
    document: Mapping[str, Any],
    *,
    product_name: str,
    catalogue: RuleCatalogue,
    parser: ExpressionParserPort | None = None,
    dialect: str = expr_mod.REFERENCE_DIALECT,
) -> frozenset[str]:
    """Walk one already layer-1-valid product declaration's steps for
    ``calculated_fields``, ``data_filtering`` and ``data_aggregation``
    occurrences (the three patterns whose configuration ever carries an
    inline ``expression``), and for the ``data_curation`` occurrences whose
    probabilistic resolution names the rule that scores a candidate pair.
    For each inline expression, parse it and
    resolve its columns against the schema visible at that occurrence (see
    the module docstring); a ``data_aggregation`` aggregate expression also
    gets the aggregation rule, against that occurrence's ``grain`` union
    ``groups``. For each ``rule`` reference, resolve it against
    ``catalogue``, failing closed with ``UnknownRuleError`` on a name no
    catalogue source carries. When the field also declares ``rule_version``,
    it must match the resolved rule's own catalogue version, or resolution
    fails closed with ``RuleVersionMismatchError`` naming both versions.

    Returns the set of rule names this product referenced, for the
    completeness gate."""

    visible_schema: set[str] = set()
    for source in document.get("sources") or []:
        expect = (source or {}).get("expect") or {}
        visible_schema.update(expect.get("fields") or [])

    referenced_rules: set[str] = set()

    for index, step in enumerate(document.get("steps") or []):
        pattern = (step or {}).get("pattern")
        occurrence_tag = f"steps[{index}]:{pattern}"

        if pattern == PatternId.SCHEMA_TRANSFORM.value:
            for mapping in step.get("mapping") or []:
                to_name = mapping.get("to")
                if to_name:
                    visible_schema.add(to_name)

        elif pattern == PatternId.CALCULATED_FIELDS.value:
            for field_index, field in enumerate(step.get("fields") or []):
                field_tag = f"{occurrence_tag}.fields[{field_index}]"
                expression = field.get("expression")
                rule_name = field.get("rule")
                if expression is not None:
                    expr_mod.parse_and_validate_expression(
                        expression,
                        schema=visible_schema,
                        product=product_name,
                        occurrence=field_tag,
                        dialect=dialect,
                        parser=parser,
                    )
                elif rule_name is not None:
                    resolved_rule = catalogue.resolve(rule_name)
                    declared_version = field.get("rule_version")
                    if declared_version is not None and declared_version != resolved_rule.version:
                        raise RuleVersionMismatchError(
                            product=product_name,
                            occurrence=field_tag,
                            rule=rule_name,
                            declared_version=declared_version,
                            catalogue_version=resolved_rule.version,
                        )
                    referenced_rules.add(rule_name)
                name = field.get("name")
                if name:
                    visible_schema.add(name)

        elif pattern == PatternId.DATA_FILTERING.value:
            for pred_index, predicate in enumerate(step.get("predicates") or []):
                pred_tag = f"{occurrence_tag}.predicates[{pred_index}]"
                expr_mod.parse_and_validate_expression(
                    predicate["expression"],
                    schema=visible_schema,
                    product=product_name,
                    occurrence=pred_tag,
                    dialect=dialect,
                    parser=parser,
                )

        elif pattern == PatternId.DATA_AGGREGATION.value:
            group_columns = set(step.get("grain") or []) | set(step.get("groups") or [])
            for agg_index, aggregate in enumerate(step.get("aggregates") or []):
                agg_tag = f"{occurrence_tag}.aggregates[{agg_index}]"
                expr_mod.parse_and_validate_expression(
                    aggregate["expression"],
                    schema=visible_schema,
                    product=product_name,
                    occurrence=agg_tag,
                    dialect=dialect,
                    group_columns=group_columns,
                    parser=parser,
                )
                name = aggregate.get("name")
                if name:
                    visible_schema.add(name)

        elif pattern == PatternId.DATA_CURATION.value:
            scoring = (step.get("resolution") or {}).get("scoring")
            if scoring is not None:
                scoring_tag = f"{occurrence_tag}.resolution.scoring"
                rule_name = scoring.get("rule")
                resolved_rule = catalogue.resolve(rule_name)
                declared_version = scoring.get("rule_version")
                if declared_version is not None and declared_version != resolved_rule.version:
                    raise RuleVersionMismatchError(
                        product=product_name,
                        occurrence=scoring_tag,
                        rule=rule_name,
                        declared_version=declared_version,
                        catalogue_version=resolved_rule.version,
                    )
                referenced_rules.add(rule_name)

        elif pattern == PatternId.DATA_ENRICHMENT.value:
            for lookup in step.get("lookups") or []:
                for field_name in lookup.get("fields") or []:
                    visible_schema.add(field_name)

    return frozenset(referenced_rules)


def validate_estate_layer2(
    products_dir: Path,
    *,
    catalogue: RuleCatalogue,
    parser: ExpressionParserPort | None = None,
    translators: Iterable[str] = (DBT_TRANSLATOR,),
    adapters: Iterable[str] = REFERENCE_ADAPTERS,
) -> tuple[str, ...]:
    """Layer 2 over every product declaration under ``products_dir``
    (searched recursively for ``*.yml``/``*.yaml``, sorted for
    determinism): expression parsing, column resolution, the aggregation
    rule, and named-rule reference resolution for each, then the
    completeness gate once over every rule any of them referenced. A
    missing ``products_dir`` validates as empty, the same convention as
    ``ergasterion.framework.declaration.validate_estate_products``.

    This function assumes every document under ``products_dir`` has already
    passed layer 1 (``ergasterion.framework.declaration.validate_declaration``):
    it does not re-check schema shape, composition, or the neutrality gate,
    and it does not import ``ergasterion.framework.declaration`` -- a caller
    composes the two layers in order. Returns the sorted product names
    validated."""

    if not products_dir.is_dir():
        return ()

    paths = sorted(set(products_dir.rglob("*.yml")) | set(products_dir.rglob("*.yaml")))
    all_referenced: set[str] = set()
    validated: list[str] = []
    for path in paths:
        document = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        product_block = document.get("product") or {}
        product_name = product_block.get("name", "<unknown>")
        referenced = validate_product_expressions_and_rules(
            document, product_name=product_name, catalogue=catalogue, parser=parser
        )
        all_referenced |= referenced
        validated.append(product_name)

    combined_implementations = {**REFERENCE_IMPLEMENTATIONS, **catalogue.implementations}
    check_completeness(
        referenced_rules=all_referenced,
        implementations=combined_implementations,
        translators=translators,
        adapters=adapters,
    )
    return tuple(sorted(validated))
