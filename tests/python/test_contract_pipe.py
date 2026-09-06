"""Self-tests for the contract pipe (architecture sections 4, 7,
10): ``ergasterion/framework/contract.py``, ``ergasterion/translators/
publication.py``, and the product-declaration-driven generation
``ergasterion/emit_contracts.py`` and ``ergasterion/emit_odps.py`` add.

Same plain assert-and-report convention as the rest of this repo (no
pytest in the .venv): each ``test_*`` raises ``AssertionError`` on failure,
``main()`` runs them all and reports PASS/FAIL.

Covers:
  - estate ownership: ``load_estate_ownership`` fails closed naming every
    missing key (all three absent, and exactly one absent among two
    present), and reads the real committed ``estate.yml`` clean;
  - schema propagation: a landing product's schema comes only from its
    supplied Bronze projection, never from its own steps; a non-landing
    product's schema is derived by propagating typed fields through its
    composition (source union, schema_transform rename/cast/drop,
    calculated_fields, data_enrichment from the referenced contract's own
    types, data_aggregation replacing the field set), failing closed
    naming product/occurrence/field wherever a type cannot be derived, and
    checking every declared ``expect.fields`` entry against the resolved
    producer;
  - the full five-fixture estate resolves to the exact typed schema each
    product should publish (byte-level proof the propagation is real, not
    an empty or partial guess);
  - the product contract's own JSON projection validates against
    ``product-contract-v1.schema.json``;
  - the ODCS and ODPS documents built from a contract validate against
    their vendored schemas, publish no fabricated support URL or team
    description, and the compliance check carries every relation;
  - the publication translator's capabilities (the four publication-shaped
    patterns, every declared adapter) and its ``translate()`` output
    matching what ``emit_contracts.py``/``emit_odps.py`` generate directly
    (the estate graph it also renders is covered by
    tests/python/test_product_graph.py);
  - table-driven routing of the three publication patterns for a landing
    fixture product and a non-landing fixture product, through the REAL
    ``estate.yml`` translator table, plus the router's fail-closed
    behaviour when an adapter capability is missing;
  - schema-change classification and consumer compatibility: an additive
    minor change passes, a removed field, a renamed field (naming BOTH the
    removed and the new field), a type change and a new required field
    each fail closed naming consumer, producer and field;
  - the manifest-driven cross-check: agreement is silent, disagreement
    fails closed naming the table and the exact field difference;
  - which relation a consumer reads (architecture section 7): a read of a
    producer publishing one relation needs no key, a source or an
    enrichment lookup naming one of several (as the producer's own shape
    names it, without the product prefix) resolves to that relation's
    schema and nothing else, and naming none of several, naming one the
    producer does not publish, or repeating the product prefix fails
    closed naming consumer, read, producer and relation;
  - the graph's and the contract's derivations of what a product publishes
    agree, relation for relation, on every fixture estate;
  - two product declarations claiming the same published name fail closed
    naming both files;
  - every fixture product under ``tests/fixtures/products/valid/`` emits
    one contract, one ODCS document per relation and one ODPS descriptor,
    byte-stable across two generations, with the exact typed schema the
    composition propagates.

Usage:
    python tests/python/test_contract_pipe.py
"""

from __future__ import annotations

import json
import tempfile
import traceback
from pathlib import Path

import yaml

if __package__ in (None, ""):
    import os as _os
    import sys as _sys

    _sys.path.insert(0, _os.path.dirname(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))))

from ergasterion import emit_contracts as ec
from ergasterion import emit_odps as eo
from ergasterion.estate import EstateContext, load_estate_adapters, load_translator_table
from ergasterion.framework import declaration as declaration_mod
from ergasterion.framework.contract import (
    AmbiguousSourceRelationError,
    ChangeClass,
    ConsumerCompatibilityError,
    ContractGenerationError,
    EstateOwnership,
    EstateOwnershipError,
    InvalidVersionError,
    LandingSchemaOriginError,
    ManifestDisagreementError,
    MissingLandingSchemaError,
    RelationField,
    RelationSchema,
    SourceExpectationError,
    UndeclaredCompositionError,
    UndeclaredMergeJoinError,
    UnknownSourceRelationError,
    UnmappedNeutralTypeError,
    UnresolvedFieldError,
    UnresolvedSourceError,
    build_compliance_check,
    build_lineage,
    build_odcs_document,
    build_odps_document,
    build_product_contract,
    build_quality_guarantees,
    build_versioning_policy,
    check_consumer_compatibility,
    check_source_expectation,
    classify_relation_change,
    consumer_source_schemas,
    contract_document,
    cross_check_manifest_agreement,
    derive_relation_schema,
    fixture_relation_schemas,
    landing_schema_for,
    load_estate_ownership,
    relation_schema_from_landing_projection,
    READ_LOOKUP,
    READ_SOURCE,
    propagate_relation_fields,
    relation_choices,
    resolve_consumer_reads,
    resolve_opening_composition,
    resolve_relation_name,
    resolve_relation_registry,
    resolve_source_relation,
    source_relation_key,
)
from ergasterion.framework.models import ExecutionPlan, Occurrence, PatternId, Role
from ergasterion.framework.routing import MissingCapabilityError, TranslationRouter
from ergasterion.framework.graph import published_name, published_relation_names
from ergasterion.framework.shapes import ShapeRelation
from ergasterion.translators.publication import PUBLICATION_PATTERNS, PublicationTranslator

FIXTURES_DIR = Path(__file__).resolve().parent.parent / "fixtures" / "products" / "valid"
GRAPH_SIX_ROOT = Path(__file__).resolve().parent.parent / "fixtures" / "estates" / "graph_six"
TWO_SHAPES_ROOT = Path(__file__).resolve().parent.parent / "fixtures" / "estates" / "two_shapes"
GRAPH_SIX_PRODUCTS = GRAPH_SIX_ROOT / "declarations" / "products"


# --------------------------------------------------------------------------- fixtures / helpers


def _load_fixture(name: str) -> dict:
    return yaml.safe_load((FIXTURES_DIR / name).read_text(encoding="utf-8")) or {}


def _validate(document: dict) -> declaration_mod.ValidatedProduct:
    ctx = EstateContext.default()
    policy = declaration_mod.load_estate_policy(ctx.estate_file)
    return declaration_mod.validate_declaration(document, policy=policy)


_OWNERSHIP = EstateOwnership(namespace="io.antikas.ergasterion", support="platform-support", team="data-product-factory")

# The typed physical schemas an ingestion-side Bronze Product Contract would
# publish for every source this fixture estate's non-landing products name:
# "ecommerce.crm_customer_landing" is a real fixture product declaration
# (landing.yml); "ecommerce.customer_segment" and "ecommerce.storefront_
# customer" are referenced only as external contracts (an enrichment lookup
# and a consolidation source), never declared as a product themselves --
# exactly the shape a real committed Bronze source would take. Column names
# deliberately differ from "customer"'s own published names
# (cust_nm/crtd_dt/tag_list) so schema_transform's rename is genuinely
# exercised, not a same-name passthrough.
LANDING_SCHEMAS: dict[str, RelationSchema] = {
    "ecommerce.crm_customer_landing": RelationSchema(
        "ecommerce.crm_customer_landing",
        (
            RelationField("customer_id", "string", True),
            RelationField("email", "string", False),
            RelationField("status_code", "string", False),
            RelationField("cust_nm", "string", False),
            RelationField("crtd_dt", "string", False),
            RelationField("tag_list", "string", False),
        ),
    ),
    "ecommerce.customer_segment": RelationSchema(
        "ecommerce.customer_segment",
        (
            RelationField("segment_code", "string", False),
            RelationField("segment_name", "string", False),
        ),
    ),
    "ecommerce.storefront_customer": RelationSchema(
        "ecommerce.storefront_customer",
        (
            RelationField("customer_id", "string", True),
            RelationField("email", "string", False),
            RelationField("channel", "string", False),
        ),
    ),
}


# The typed physical schemas the ingestion side publishes for the two
# landing products of the six-product fixture estate under
# tests/fixtures/estates/graph_six/. That estate is the one that exercises
# data_aggregation end to end, so it is where the aggregate's declared type
# has to hold up all the way to a generated contract.
GRAPH_SIX_LANDING_SCHEMAS: dict[str, RelationSchema] = {
    "retail.orders_landing": RelationSchema(
        "retail.orders_landing",
        (
            RelationField("order_id", "string", True),
            RelationField("status_code", "string", False),
            RelationField("ordr_dt", "string", False),
            RelationField("cust_id", "string", False),
            RelationField("ordr_ttl", "string", False),
            RelationField("tag_list", "string", False),
            RelationField("sku", "string", False),
        ),
    ),
    "retail.catalogue_landing": RelationSchema(
        "retail.catalogue_landing",
        (
            RelationField("sku", "string", True),
            RelationField("category_name", "string", False),
        ),
    ),
}


def _fixture_entries() -> dict[str, tuple[dict, declaration_mod.ValidatedProduct]]:
    ctx = EstateContext.default()
    policy = declaration_mod.load_estate_policy(ctx.estate_file)
    entries: dict[str, tuple[dict, declaration_mod.ValidatedProduct]] = {}
    for path in sorted(FIXTURES_DIR.glob("*.yml")):
        document = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        validated = declaration_mod.validate_declaration(document, policy=policy)
        entries[f"{validated.domain}.{validated.name}"] = (document, validated)
    return entries


def _fixture_registry() -> dict[str, RelationSchema]:
    # The registry answers with every relation each product's shape
    # publishes; a caller propagating from one reads the single relation a
    # declared-shape product publishes.
    return {
        name: relations[0].schema
        for name, relations in resolve_relation_registry(
            _fixture_entries(), landing_schemas=LANDING_SCHEMAS
        ).items()
    }


# --------------------------------------------------------------------------- estate ownership


def test_estate_ownership_fails_closed_when_all_three_absent() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "estate.yml"
        path.write_text(yaml.safe_dump({"estate": {}}), encoding="utf-8")
        try:
            load_estate_ownership(path)
        except EstateOwnershipError as exc:
            assert set(exc.missing) == {"namespace", "support", "team"}, exc.missing
        else:
            raise AssertionError("expected EstateOwnershipError when all three are absent")


def test_estate_ownership_names_exactly_the_one_missing_key() -> None:
    # Checklist: a gate covering a whole document must be proven on a field
    # OUTSIDE the obvious one -- here, namespace and support are both
    # present and valid; only team is missing.
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "estate.yml"
        path.write_text(
            yaml.safe_dump({"estate": {"namespace": "io.antikas.fixture", "support": "desk"}}), encoding="utf-8"
        )
        try:
            load_estate_ownership(path)
        except EstateOwnershipError as exc:
            assert exc.missing == ("team",), exc.missing
        else:
            raise AssertionError("expected EstateOwnershipError naming only 'team'")


def test_estate_ownership_rejects_an_empty_string() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "estate.yml"
        path.write_text(
            yaml.safe_dump({"estate": {"namespace": "io.antikas.fixture", "support": "", "team": "data"}}),
            encoding="utf-8",
        )
        try:
            load_estate_ownership(path)
        except EstateOwnershipError as exc:
            assert exc.missing == ("support",), exc.missing
        else:
            raise AssertionError("an empty string must fail exactly like an absent key")


def test_real_estate_yml_carries_namespace_support_and_team() -> None:
    ownership = load_estate_ownership(EstateContext.default().estate_file)
    assert ownership.namespace == "io.antikas.ergasterion"
    assert ownership.support and ownership.team


def test_estate_ownership_fails_closed_when_the_file_is_missing() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        try:
            load_estate_ownership(Path(tmp) / "no-such-estate.yml")
        except EstateOwnershipError as exc:
            assert set(exc.missing) == {"namespace", "support", "team"}
        else:
            raise AssertionError("expected EstateOwnershipError for a missing estate file")


# --------------------------------------------------------------------------- landing schema from a Bronze projection


class _FakeProjectionField:
    def __init__(self, name: str, logical_type, nullable: bool) -> None:
        self.name = name
        self.logical_type = logical_type
        self.nullable = nullable


class _FakeDecimalType:
    kind = "decimal"

    def __init__(self, precision: int, scale: int) -> None:
        self.precision = precision
        self.scale = scale


def test_relation_schema_from_landing_projection_maps_every_scalar_kind() -> None:
    projection = [
        _FakeProjectionField("customer_id", "utf8_string", False),
        _FakeProjectionField("is_active", "boolean", True),
        _FakeProjectionField("created_on", "date", True),
        _FakeProjectionField("row_count", "int64", True),
        _FakeProjectionField("seen_at", "utc_instant", True),
        _FakeProjectionField("amount", _FakeDecimalType(10, 2), True),
    ]
    schema = relation_schema_from_landing_projection(domain="ecommerce", name="widgets", projection=projection)
    by_name = {f.name: f for f in schema.fields}
    assert by_name["customer_id"].type == "string" and by_name["customer_id"].required is True
    assert by_name["is_active"].type == "boolean" and by_name["is_active"].required is False
    assert by_name["created_on"].type == "date"
    assert by_name["row_count"].type == "integer"
    assert by_name["seen_at"].type == "timestamp"
    assert by_name["amount"].type == {"name": "decimal", "precision": 10, "scale": 2}


def test_relation_schema_from_landing_projection_fails_closed_on_binary() -> None:
    projection = [_FakeProjectionField("payload", "binary", True)]
    try:
        relation_schema_from_landing_projection(domain="ecommerce", name="widgets", projection=projection)
    except UnmappedNeutralTypeError as exc:
        assert exc.type_value == "binary"
    else:
        raise AssertionError("expected UnmappedNeutralTypeError for a binary logical type")


# --------------------------------------------------------------------------- non-landing schema derivation


def test_declared_shape_derives_fields_from_schema_transform_and_calculated_fields() -> None:
    document = _load_fixture("integration.yml")
    validated = _validate(document)
    relation = derive_relation_schema(document, validated, source_schemas=LANDING_SCHEMAS)
    assert relation.name == "ecommerce.customer"
    names = [f.name for f in relation.fields]
    assert names == [
        "customer_id", "email", "status_code", "customer_name", "created_on", "tags", "is_active", "segment_name",
    ], names
    by_name = {f.name: f for f in relation.fields}
    assert by_name["tags"].type == {"name": "array"}
    assert by_name["created_on"].type == "date"
    # Renamed away: the landing product's own source-side names are gone.
    assert "cust_nm" not in by_name and "crtd_dt" not in by_name and "tag_list" not in by_name
    assert by_name["customer_id"].required is True


def test_a_landing_products_relation_comes_only_from_its_supplied_schema() -> None:
    document = _load_fixture("landing.yml")
    validated = _validate(document)
    entries = {"ecommerce.crm_customer_landing": (document, validated)}
    registry = resolve_relation_registry(entries, landing_schemas=LANDING_SCHEMAS)
    relations = registry["ecommerce.crm_customer_landing"]
    assert len(relations) == 1, relations
    assert relations[0].schema.fields == LANDING_SCHEMAS["ecommerce.crm_customer_landing"].fields


def test_a_landing_product_with_no_supplied_schema_fails_closed() -> None:
    document = _load_fixture("landing.yml")
    validated = _validate(document)
    entries = {"ecommerce.crm_customer_landing": (document, validated)}
    try:
        resolve_relation_registry(entries, landing_schemas={})
    except MissingLandingSchemaError as exc:
        assert exc.product == "ecommerce.crm_customer_landing"
    else:
        raise AssertionError("expected MissingLandingSchemaError when no landing schema is supplied")


def test_not_null_and_full_completeness_rules_mark_a_field_required() -> None:
    document = {
        "sources": [],
        "steps": [
            {"pattern": "schema_transform", "mapping": [{"from": "a", "to": "customer_id", "type": "string"}]},
            {"pattern": "calculated_fields", "fields": [{"name": "is_active", "type": "boolean", "expression": "1=1"}]},
            {
                "pattern": "data_validation",
                "rules": [
                    {"field": "customer_id", "not_null": True},
                    {"field": "is_active", "completeness": 1.0},
                ],
            },
        ],
    }

    class _Validated:
        shape = "declared"
        profile = "integration"
        domain = "ecommerce"
        name = "fixture_product"

    relation = derive_relation_schema(document, _Validated(), source_schemas={})
    by_name = {f.name: f for f in relation.fields}
    assert by_name["customer_id"].required is True
    assert by_name["is_active"].required is True


def test_a_partial_completeness_rule_does_not_mark_the_field_required() -> None:
    document = {
        "sources": [],
        "steps": [
            {"pattern": "schema_transform", "mapping": [{"from": "a", "to": "email", "type": "string"}]},
            {"pattern": "data_validation", "rules": [{"field": "email", "completeness": 0.9}]},
        ],
    }

    class _Validated:
        shape = "declared"
        profile = "integration"
        domain = "ecommerce"
        name = "fixture_product"

    relation = derive_relation_schema(document, _Validated(), source_schemas={})
    assert relation.fields[0].required is False


def test_a_required_field_stays_required_after_passing_through_unchanged() -> None:
    document = {"sources": [{"contract": "ecommerce.crm_customer_landing@1"}], "steps": [{"pattern": "batch_transfer"}]}

    class _Validated:
        shape = "declared"
        profile = "derivation"
        domain = "ecommerce"
        name = "passthrough_product"

    relation = derive_relation_schema(document, _Validated(), source_schemas=LANDING_SCHEMAS)
    by_name = {f.name: f for f in relation.fields}
    assert by_name["customer_id"].required is True


def test_unresolved_source_fails_closed() -> None:
    document = {"sources": [{"contract": "ecommerce.nonexistent@1"}], "steps": []}

    class _Validated:
        shape = "declared"
        profile = "integration"
        domain = "ecommerce"
        name = "fixture_product"

    try:
        derive_relation_schema(document, _Validated(), source_schemas={})
    except UnresolvedSourceError as exc:
        assert exc.product == "ecommerce.fixture_product"
        assert exc.source == "ecommerce.nonexistent@1"
    else:
        raise AssertionError("expected UnresolvedSourceError for a source with no known schema")


def test_source_expectation_is_checked_and_fails_closed_naming_the_field() -> None:
    document = {
        "sources": [{"contract": "ecommerce.crm_customer_landing@1", "expect": {"fields": ["not_a_real_field"]}}],
        "steps": [],
    }

    class _Validated:
        shape = "declared"
        profile = "integration"
        domain = "ecommerce"
        name = "fixture_product"

    try:
        derive_relation_schema(document, _Validated(), source_schemas=LANDING_SCHEMAS)
    except SourceExpectationError as exc:
        assert exc.consumer == "ecommerce.fixture_product"
        assert exc.producer == "ecommerce.crm_customer_landing@1"
        assert exc.field == "not_a_real_field"
    else:
        raise AssertionError("expected SourceExpectationError for an unmet expectation")


def test_check_source_expectation_passes_when_every_field_is_present() -> None:
    check_source_expectation(
        consumer="ecommerce.customer@1",
        producer="ecommerce.crm_customer_landing@1",
        expected_fields=["customer_id", "email"],
        schema=LANDING_SCHEMAS["ecommerce.crm_customer_landing"],
    )


def test_data_enrichment_adds_the_looked_up_field_with_the_referenced_type() -> None:
    document = {
        "sources": [{"contract": "ecommerce.crm_customer_landing@1"}],
        "steps": [
            {"pattern": "batch_transfer"},
            {
                "pattern": "data_enrichment",
                "lookups": [
                    {"contract": "ecommerce.customer_segment@1", "on": {"a": "a"}, "fields": ["segment_name"]}
                ],
            },
        ],
    }

    class _Validated:
        shape = "declared"
        profile = "integration"
        domain = "ecommerce"
        name = "enriched_product"

    relation = derive_relation_schema(document, _Validated(), source_schemas=LANDING_SCHEMAS)
    by_name = {f.name: f for f in relation.fields}
    assert by_name["segment_name"].type == "string"


def test_data_enrichment_fails_closed_on_a_field_the_referenced_contract_lacks() -> None:
    document = {
        "sources": [{"contract": "ecommerce.crm_customer_landing@1"}],
        "steps": [
            {
                "pattern": "data_enrichment",
                "lookups": [
                    {"contract": "ecommerce.customer_segment@1", "on": {"a": "a"}, "fields": ["no_such_field"]}
                ],
            },
        ],
    }

    class _Validated:
        shape = "declared"
        profile = "integration"
        domain = "ecommerce"
        name = "enriched_product"

    try:
        derive_relation_schema(document, _Validated(), source_schemas=LANDING_SCHEMAS)
    except UnresolvedFieldError as exc:
        assert exc.product == "ecommerce.enriched_product"
        assert exc.field == "no_such_field"
    else:
        raise AssertionError("expected UnresolvedFieldError for a lookup field the reference lacks")


def test_data_aggregation_replaces_the_field_set_with_grain_and_typed_aggregates() -> None:
    document = {
        "sources": [{"contract": "ecommerce.crm_customer_landing@1"}],
        "steps": [
            {"pattern": "batch_transfer"},
            {
                "pattern": "data_aggregation",
                "grain": ["status_code"],
                "aggregates": [{"name": "customer_count", "expression": "count(*)", "type": "integer"}],
            },
        ],
    }

    class _Validated:
        shape = "declared"
        profile = "derivation"
        domain = "ecommerce"
        name = "aggregated_product"

    relation = derive_relation_schema(document, _Validated(), source_schemas=LANDING_SCHEMAS)
    names = {f.name for f in relation.fields}
    assert names == {"status_code", "customer_count"}, names


def test_data_aggregation_fails_closed_on_an_aggregate_with_no_declared_type() -> None:
    # The brief's stop condition made explicit: today's data_aggregation
    # pattern schema declares no "type" key for an aggregate, so this
    # occurrence always fails closed rather than publish an untyped field.
    document = {
        "sources": [{"contract": "ecommerce.crm_customer_landing@1"}],
        "steps": [
            {"pattern": "batch_transfer"},
            {
                "pattern": "data_aggregation",
                "grain": ["status_code"],
                "aggregates": [{"name": "customer_count", "expression": "count(*)"}],
            },
        ],
    }

    class _Validated:
        shape = "declared"
        profile = "derivation"
        domain = "ecommerce"
        name = "aggregated_product"

    try:
        derive_relation_schema(document, _Validated(), source_schemas=LANDING_SCHEMAS)
    except UnresolvedFieldError as exc:
        assert exc.field == "customer_count"
    else:
        raise AssertionError("expected UnresolvedFieldError for an aggregate with no declared type")


def test_data_aggregation_fails_closed_on_an_ungrouped_grain_field() -> None:
    document = {
        "sources": [{"contract": "ecommerce.crm_customer_landing@1"}],
        "steps": [
            {"pattern": "batch_transfer"},
            {
                "pattern": "data_aggregation",
                "grain": ["not_a_real_field"],
                "aggregates": [{"name": "n", "expression": "count(*)", "type": "integer"}],
            },
        ],
    }

    class _Validated:
        shape = "declared"
        profile = "derivation"
        domain = "ecommerce"
        name = "aggregated_product"

    try:
        derive_relation_schema(document, _Validated(), source_schemas=LANDING_SCHEMAS)
    except UnresolvedFieldError as exc:
        assert exc.field == "not_a_real_field"
    else:
        raise AssertionError("expected UnresolvedFieldError for a grain field the current schema lacks")


def test_unsupported_shape_fails_closed() -> None:
    class _Validated:
        shape = "data_vault"
        profile = "integration"
        domain = "ecommerce"
        name = "fixture_product"

    entries = {"ecommerce.fixture_product": ({"sources": [], "steps": []}, _Validated())}
    try:
        resolve_relation_registry(entries, landing_schemas={})
    except ContractGenerationError as exc:
        assert exc.product == "fixture_product"
        assert exc.shape == "data_vault"
    else:
        raise AssertionError("expected ContractGenerationError for an unregistered shape")


def test_registry_fails_closed_on_a_genuinely_unresolvable_reference() -> None:
    class _Validated:
        shape = "declared"
        profile = "integration"
        domain = "ecommerce"
        name = "orphan_product"

    entries = {"ecommerce.orphan_product": ({"sources": [{"contract": "ecommerce.nowhere@1"}], "steps": []}, _Validated())}
    try:
        resolve_relation_registry(entries, landing_schemas={})
    except UnresolvedSourceError as exc:
        assert "ecommerce.orphan_product" in exc.product
    else:
        raise AssertionError("expected UnresolvedSourceError when nothing in the estate can resolve the source")


def test_invalid_version_fails_closed() -> None:
    document = _load_fixture("landing.yml")
    document = dict(document)
    document["product"] = dict(document["product"])
    document["product"]["version"] = "not-a-version"
    validated = _validate(_load_fixture("landing.yml"))  # layer 1 does not police version content
    relation = LANDING_SCHEMAS["ecommerce.crm_customer_landing"]
    try:
        build_product_contract(document, validated, ownership=_OWNERSHIP, relations=(relation,))
    except InvalidVersionError as exc:
        assert exc.product == "crm_customer_landing"
        assert exc.version == "not-a-version"
    else:
        raise AssertionError("expected InvalidVersionError for an unparseable major version")


# --------------------------------------------------------------------------- quality, versioning, lineage


def test_quality_guarantees_cover_every_rule_kind() -> None:
    document = {
        "steps": [
            {
                "pattern": "data_validation",
                "rules": [
                    {"field": "a", "not_null": True},
                    {"field": "b", "unique": True},
                    {"field": "c", "min": 0, "max": 10},
                    {"field": "d", "allowed_values": ["x", "y"]},
                    {"field": "e", "regex": "^[a-z]+$"},
                ],
            }
        ]
    }
    guarantees = build_quality_guarantees(document)
    dimensions = {g.dimension for g in guarantees}
    assert dimensions == {"completeness", "uniqueness", "conformity"}, dimensions
    assert len(guarantees) == 5, guarantees


def test_versioning_policy_uses_the_declared_value_when_present() -> None:
    document = {"steps": [{"pattern": "schema_publish", "versioning_policy": "custom policy text"}]}
    assert build_versioning_policy(document) == "custom policy text"


def test_versioning_policy_falls_back_to_the_fixed_doctrine() -> None:
    document = _load_fixture("landing.yml")  # carries no schema_publish.versioning_policy
    policy = build_versioning_policy(document)
    assert "additive" in policy and "major" in policy


def test_lineage_lists_every_source_contract_in_order() -> None:
    document = _load_fixture("consolidation.yml")
    assert build_lineage(document) == ("ecommerce.customer@1", "ecommerce.storefront_customer@1")


def test_a_landing_product_declares_no_lineage() -> None:
    document = _load_fixture("landing.yml")
    assert build_lineage(document) == ()


# --------------------------------------------------------------------------- contract / ODCS / ODPS / compliance-check


def test_contract_document_validates_against_its_own_schema() -> None:
    validator = ec.load_product_contract_schema_validator()
    document = _load_fixture("integration.yml")
    validated = _validate(document)
    relation = derive_relation_schema(document, validated, source_schemas=LANDING_SCHEMAS)
    contract = build_product_contract(document, validated, ownership=_OWNERSHIP, relations=(relation,))
    doc = contract_document(contract)
    errors = sorted(validator.iter_errors(doc), key=lambda e: list(e.path))
    assert not errors, [e.message for e in errors]
    assert doc["identity"] == {"namespace": "io.antikas.ergasterion", "domain": "ecommerce", "name": "customer", "major": 1}
    assert doc["support"] == "platform-support"
    assert doc["team"] == "data-product-factory"


def test_odcs_and_odps_documents_validate_against_the_vendored_schemas() -> None:
    odcs_validator = ec.load_schema_validator()
    odps_validator = eo.load_schema_validator()
    document = _load_fixture("integration.yml")
    validated = _validate(document)
    relation = derive_relation_schema(document, validated, source_schemas=LANDING_SCHEMAS)
    contract = build_product_contract(document, validated, ownership=_OWNERSHIP, relations=(relation,))

    odcs_doc = build_odcs_document(contract, relation)
    errors = sorted(odcs_validator.iter_errors(odcs_doc), key=lambda e: list(e.absolute_path))
    assert not errors, [e.message for e in errors]
    assert odcs_doc["name"] == "customer"
    assert any(p["name"] == "is_active" for p in odcs_doc["schema"][0]["properties"])
    tags_property = next(p for p in odcs_doc["schema"][0]["properties"] if p["name"] == "tags")
    assert tags_property["logicalType"] == "array"
    assert next(p for p in odcs_doc["schema"][0]["properties"] if p["name"] == "customer_id")["required"] is True

    odps_doc = build_odps_document(contract)
    errors = sorted(odps_validator.iter_errors(odps_doc), key=lambda e: list(e.absolute_path))
    assert not errors, [e.message for e in errors]
    assert odps_doc["outputPorts"][0]["contractId"] == odcs_doc["id"]


def test_support_and_team_carry_no_fabricated_url_or_description() -> None:
    document = _load_fixture("landing.yml")
    validated = _validate(document)
    relation = LANDING_SCHEMAS["ecommerce.crm_customer_landing"]
    contract = build_product_contract(document, validated, ownership=_OWNERSHIP, relations=(relation,))

    odcs_doc = build_odcs_document(contract, relation)
    assert odcs_doc["support"] == [{"channel": "platform-support"}], odcs_doc["support"]
    assert odcs_doc["team"] == {"name": "data-product-factory"}, odcs_doc["team"]
    assert "url" not in odcs_doc["support"][0]

    odps_doc = build_odps_document(contract)
    assert "support" not in odps_doc, "ODPS support requires a url schema does not let us invent -- must be omitted"
    assert odps_doc["team"] == {"name": "data-product-factory"}, odps_doc["team"]


def test_an_unmapped_neutral_type_fails_closed_building_the_odcs_document() -> None:
    document = _load_fixture("landing.yml")
    validated = _validate(document)
    contract = build_product_contract(
        document,
        validated,
        ownership=_OWNERSHIP,
        relations=(LANDING_SCHEMAS["ecommerce.crm_customer_landing"],),
    )
    relation = RelationSchema("ecommerce.fixture", (RelationField("weird", {"name": "not_a_real_type"}, False),))
    try:
        build_odcs_document(contract, relation)
    except UnmappedNeutralTypeError as exc:
        assert exc.type_value == {"name": "not_a_real_type"}
    else:
        raise AssertionError("expected UnmappedNeutralTypeError for an unrecognised structured type")


def test_compliance_check_names_every_relation() -> None:
    document = _load_fixture("consolidation.yml")
    validated = _validate(document)
    relation = derive_relation_schema(document, validated, source_schemas={**LANDING_SCHEMAS, **_fixture_registry()})
    contract = build_product_contract(document, validated, ownership=_OWNERSHIP, relations=(relation,))
    check = build_compliance_check(contract)
    assert check["schema"] == "ergasterion.contract-compliance/v1"
    assert check["product"] == "ecommerce.customer_360"
    assert check["relations"] == ["ecommerce.customer_360"]
    assert check["description"]


# --------------------------------------------------------------------------- publication translator


def test_publication_translator_registers_the_four_patterns_for_every_adapter() -> None:
    translator = PublicationTranslator()
    caps = translator.capabilities()
    assert PUBLICATION_PATTERNS == (
        PatternId.DATA_CONTRACTS,
        PatternId.SCHEMA_PUBLISH,
        PatternId.METADATA_CAPTURE,
        PatternId.LINEAGE_CAPTURE,
    )
    for pattern in PUBLICATION_PATTERNS:
        for adapter in ("duckdb", "bigquery"):
            assert (pattern.value, "publication", adapter) in {(c.pattern_or_shape, c.translator, c.adapter) for c in caps}
    assert all(c.translator == "publication" for c in caps)
    assert len(caps) == 8


def test_publication_translator_translate_matches_direct_emitter_output() -> None:
    ctx = EstateContext.default()
    contracts = tuple(ec.build_product_contracts(ctx, products_dir=FIXTURES_DIR, landing_schemas=LANDING_SCHEMAS).values())
    translator = PublicationTranslator(contracts=contracts)
    result = translator.translate(None)  # never reads the plan; see the module docstring

    direct_odcs = ec.generate_products(ctx, products_dir=FIXTURES_DIR, landing_schemas=LANDING_SCHEMAS)
    direct_odps = eo.generate_products(ctx, products_dir=FIXTURES_DIR, landing_schemas=LANDING_SCHEMAS)

    for path, text in direct_odcs.items():
        if not path.name.endswith(".odcs.yml"):
            continue
        rel = path.relative_to(ctx.contracts_dir / "products").as_posix()
        assert result.artefacts[rel] == text, rel
    for path, text in direct_odps.items():
        rel = path.relative_to(ctx.contracts_dir / "products").as_posix()
        assert result.artefacts[rel] == text, rel


# --------------------------------------------------------------------------- table-driven routing


def _publication_only_plan(prefix: str, profile: str) -> ExecutionPlan:
    contract_occ = Occurrence(f"{prefix}.contract", PatternId.DATA_CONTRACTS, (Role.POLICY, Role.BARRIER), True)
    schema_occ = Occurrence(f"{prefix}.schema", PatternId.SCHEMA_PUBLISH, (Role.OBSERVER, Role.BARRIER), True)
    metadata_occ = Occurrence(f"{prefix}.metadata", PatternId.METADATA_CAPTURE, (Role.OBSERVER,), True)
    occurrences = tuple(sorted((contract_occ, schema_occ, metadata_occ), key=lambda o: o.occurrence_id))
    members = tuple(sorted(o.occurrence_id for o in occurrences if o.occurrence_id != contract_occ.occurrence_id))
    return ExecutionPlan(
        profile=profile, occurrences=occurrences, edges=(), wrapper_id=contract_occ.occurrence_id, wrapper_members=members
    )


def test_routes_the_three_patterns_for_a_landing_fixture_product() -> None:
    ctx = EstateContext.default()
    table = load_translator_table(ctx.estate_file)
    adapters = load_estate_adapters(ctx.estate_file)
    plan = _publication_only_plan("bronzefix", "landing")
    router = TranslationRouter(plan, [PublicationTranslator()])
    result = router.route(label="bronze", translator_table=table, adapters=adapters.names())
    assert len(result.assignments) == 3 * len(adapters.names())
    assert all(a.translator_name == "publication" for a in result.assignments)
    assert set(result.translations) == {"publication"}


def test_routes_the_three_patterns_for_a_non_landing_fixture_product() -> None:
    ctx = EstateContext.default()
    table = load_translator_table(ctx.estate_file)
    adapters = load_estate_adapters(ctx.estate_file)
    for label, profile in (("silver", "integration"), ("gold", "serving")):
        plan = _publication_only_plan(f"{label}fix", profile)
        router = TranslationRouter(plan, [PublicationTranslator()])
        result = router.route(label=label, translator_table=table, adapters=adapters.names())
        assert len(result.assignments) == 3 * len(adapters.names())
        assert all(a.translator_name == "publication" for a in result.assignments)


def test_routing_fails_closed_when_an_adapter_capability_is_missing() -> None:
    # Drives the real verdict path red: the publication translator never
    # registered a capability for an adapter the estate did not declare.
    ctx = EstateContext.default()
    table = load_translator_table(ctx.estate_file)
    plan = _publication_only_plan("redfix", "landing")
    router = TranslationRouter(plan, [PublicationTranslator()])
    try:
        router.route(label="bronze", translator_table=table, adapters=("duckdb", "bigquery", "postgres"))
    except MissingCapabilityError as exc:
        assert exc.translator_name == "publication"
        assert exc.adapter == "postgres"
    else:
        raise AssertionError("expected missing_capability for an undeclared adapter")


# --------------------------------------------------------------------------- schema evolution / consumer compatibility


def test_additive_minor_change_passes_a_consumer_pinned_to_the_major() -> None:
    prior = RelationSchema("ecommerce.customer", (RelationField("customer_id", "string", True),))
    candidate = RelationSchema(
        "ecommerce.customer", (RelationField("customer_id", "string", True), RelationField("phone", "string", False))
    )
    change_class, offending = classify_relation_change(prior, candidate)
    assert change_class is ChangeClass.MINOR and offending == ()
    check_consumer_compatibility(
        consumer="ecommerce.customer_mart@1", producer="ecommerce.customer@1", prior=prior, candidate=candidate
    )


def test_no_change_classifies_as_none() -> None:
    schema = RelationSchema("ecommerce.customer", (RelationField("customer_id", "string", True),))
    change_class, offending = classify_relation_change(schema, schema)
    assert change_class is ChangeClass.NONE and offending == ()


def test_removed_field_fails_closed_naming_consumer_producer_and_field() -> None:
    prior = RelationSchema(
        "ecommerce.customer", (RelationField("customer_id", "string", True), RelationField("email", "string", False))
    )
    candidate = RelationSchema("ecommerce.customer", (RelationField("customer_id", "string", True),))
    try:
        check_consumer_compatibility(
            consumer="ecommerce.customer_mart@1", producer="ecommerce.customer@1", prior=prior, candidate=candidate
        )
    except ConsumerCompatibilityError as exc:
        assert exc.consumer == "ecommerce.customer_mart@1"
        assert exc.producer == "ecommerce.customer@1"
        assert exc.field == "email"
        assert exc.change_class is ChangeClass.MAJOR
        # A pure removal (nothing added in its place) names only the one field.
        assert exc.added == ()
    else:
        raise AssertionError("expected ConsumerCompatibilityError for a removed field")


def test_renamed_field_fails_closed_naming_both_the_old_and_the_new_name() -> None:
    prior = RelationSchema("ecommerce.customer", (RelationField("email", "string", False),))
    candidate = RelationSchema("ecommerce.customer", (RelationField("email_address", "string", False),))
    try:
        check_consumer_compatibility(consumer="c@1", producer="p@1", prior=prior, candidate=candidate)
    except ConsumerCompatibilityError as exc:
        assert exc.field == "email"
        assert exc.removed == ("email",)
        assert exc.added == ("email_address",)
        assert "email" in str(exc) and "email_address" in str(exc)
    else:
        raise AssertionError("expected ConsumerCompatibilityError for a renamed field")


def test_type_change_fails_closed() -> None:
    prior = RelationSchema("ecommerce.customer", (RelationField("customer_id", "string", True),))
    candidate = RelationSchema("ecommerce.customer", (RelationField("customer_id", "integer", True),))
    try:
        check_consumer_compatibility(consumer="c@1", producer="p@1", prior=prior, candidate=candidate)
    except ConsumerCompatibilityError as exc:
        assert exc.field == "customer_id"
    else:
        raise AssertionError("expected ConsumerCompatibilityError for a type change")


def test_new_required_field_fails_closed() -> None:
    prior = RelationSchema("ecommerce.customer", (RelationField("customer_id", "string", True),))
    candidate = RelationSchema(
        "ecommerce.customer", (RelationField("customer_id", "string", True), RelationField("status", "string", True))
    )
    try:
        check_consumer_compatibility(consumer="c@1", producer="p@1", prior=prior, candidate=candidate)
    except ConsumerCompatibilityError as exc:
        assert exc.field == "status"
    else:
        raise AssertionError("expected ConsumerCompatibilityError for a new required field")


# --------------------------------------------------------------------------- manifest-driven cross-check


def test_manifest_agreement_is_silent() -> None:
    relation = RelationSchema("ecommerce.customer", (RelationField("customer_id", "string", True),))
    cross_check_manifest_agreement(
        table="customer", manifest_fields=frozenset({"customer_id"}), declaration_relation=relation
    )


def test_manifest_disagreement_fails_closed_naming_the_table_and_field() -> None:
    relation = RelationSchema("ecommerce.customer", (RelationField("customer_id", "string", True),))
    try:
        cross_check_manifest_agreement(
            table="customer", manifest_fields=frozenset({"customer_id", "legacy_col"}), declaration_relation=relation
        )
    except ManifestDisagreementError as exc:
        assert exc.table == "customer"
        assert exc.extra == ("legacy_col",)
        assert exc.missing == ()
    else:
        raise AssertionError("expected ManifestDisagreementError for a disagreeing manifest")


# --------------------------------------------------------------------------- duplicate published names


def test_two_products_claiming_the_same_published_name_fail_closed() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        products_dir = Path(tmp)
        landing_doc = _load_fixture("landing.yml")
        first = products_dir / "a.yml"
        second = products_dir / "b.yml"
        first.write_text(yaml.safe_dump(landing_doc, sort_keys=False), encoding="utf-8")
        second.write_text(yaml.safe_dump(landing_doc, sort_keys=False), encoding="utf-8")
        try:
            ec.build_product_contracts(
                EstateContext.default(), products_dir=products_dir, landing_schemas=LANDING_SCHEMAS
            )
        except ec.DuplicateProductError as exc:
            assert exc.published_name == "ecommerce.crm_customer_landing"
            assert {exc.first.name, exc.second.name} == {"a.yml", "b.yml"}
        else:
            raise AssertionError("expected DuplicateProductError for two files claiming one published name")


# --------------------------------------------------------------------------- every fixture product, end to end


def test_every_fixture_product_emits_one_contract_one_odcs_and_one_odps_byte_stable() -> None:
    ctx = EstateContext.default()
    contract_files_a = ec.generate_products(ctx, products_dir=FIXTURES_DIR, landing_schemas=LANDING_SCHEMAS)
    contract_files_b = ec.generate_products(ctx, products_dir=FIXTURES_DIR, landing_schemas=LANDING_SCHEMAS)
    assert contract_files_a == contract_files_b, "product-contract generation is not byte-stable"

    odps_files_a = eo.generate_products(ctx, products_dir=FIXTURES_DIR, landing_schemas=LANDING_SCHEMAS)
    odps_files_b = eo.generate_products(ctx, products_dir=FIXTURES_DIR, landing_schemas=LANDING_SCHEMAS)
    assert odps_files_a == odps_files_b, "product-ODPS generation is not byte-stable"

    # customer_360 merges its two sources' columns on the customer key, and
    # the storefront source conforms its own email column to a name of its
    # own as it is read, so the merged relation carries both (a merge never
    # picks one of two). customer_mart consumes customer_360 and carries the
    # same width forward.
    expected_field_counts = {
        "crm_customer_landing": 6,
        "customer": 8,
        "customer_360": 10,
        "customer_risk_score": 9,
        "customer_mart": 10,
    }
    seen_names: dict[str, int] = {}
    for path in contract_files_a:
        if path.name == "contract.json":
            doc = json.loads(contract_files_a[path])
            seen_names[doc["identity"]["name"]] = len(doc["relations"][0]["fields"])
    assert seen_names == expected_field_counts, seen_names
    # The reviewer's own worked example: "customer" publishes customer_id
    # and its eight columns.
    customer_doc = next(
        json.loads(contract_files_a[p]) for p in contract_files_a if p.name == "contract.json" and p.parent.name == "customer"
    )
    field_names = {f["name"] for f in customer_doc["relations"][0]["fields"]}
    assert "customer_id" in field_names and len(field_names) == 8

    for name in expected_field_counts:
        odcs_paths = [p for p in contract_files_a if p.name == f"{name}.odcs.yml"]
        assert len(odcs_paths) == 1, f"{name}: expected exactly one ODCS document, got {odcs_paths}"
        odcs_doc = yaml.safe_load(contract_files_a[odcs_paths[0]])
        assert odcs_doc["schema"][0]["properties"], f"{name}: ODCS properties must never be empty"
        contract_paths = [p for p in contract_files_a if p.parent.name == name and p.name == "contract.json"]
        assert len(contract_paths) == 1
        odps_paths = [p for p in odps_files_a if p.name == f"{name}.odps.yml"]
        assert len(odps_paths) == 1, f"{name}: expected exactly one ODPS descriptor, got {odps_paths}"
        compliance_paths = [p for p in contract_files_a if p.parent.name == name and p.name == "contract-compliance.json"]
        assert len(compliance_paths) == 1

    odcs_validator = ec.load_schema_validator()
    pc_validator = ec.load_product_contract_schema_validator()
    errors = ec.validate_products(contract_files_a, odcs_validator, pc_validator, ctx=ctx)
    assert not errors, errors
    odps_validator = eo.load_schema_validator()
    odps_errors = eo.validate_all(odps_files_a, odps_validator, ctx=ctx)
    assert not odps_errors, odps_errors


def test_an_aggregation_product_generates_its_contract_with_the_declared_aggregate_types() -> None:
    ctx = EstateContext.resolve(estate_root=GRAPH_SIX_ROOT)
    contracts = ec.build_product_contracts(
        ctx, products_dir=GRAPH_SIX_PRODUCTS, landing_schemas=GRAPH_SIX_LANDING_SCHEMAS
    )
    assert set(contracts) == {
        "retail.catalogue_landing",
        "retail.orders_landing",
        "retail.order",
        "retail.product_catalogue",
        "retail.order_360",
        "retail.order_mart",
    }
    mart = contracts["retail.order_mart"]
    assert len(mart.relations) == 1
    fields = {field.name: field.type for field in mart.relations[0].fields}
    # The aggregation replaces the field set with its grain, its groups and
    # its aggregates, each aggregate carrying the type its declaration
    # states -- an aggregate's type is never derivable from an input field.
    assert fields == {
        "customer_id": "string",
        "ordered_on": "date",
        "order_count": "integer",
        "order_value": {"name": "decimal", "precision": 14, "scale": 2},
    }


# --------------------------------------------------------------------------- which relation a consumer reads


def _two_shapes_entries(mutate=None) -> dict[str, tuple[dict, declaration_mod.ValidatedProduct]]:
    """The two-shapes estate's declarations, keyed by published name, with
    an optional mutation applied to one document before validation. That
    estate publishes two products whose shapes render several relations
    each, so it is where the consumer-side relation key can be resolved
    against real contracts."""

    policy = declaration_mod.load_estate_policy(TWO_SHAPES_ROOT / "estate.yml")
    entries: dict[str, tuple[dict, declaration_mod.ValidatedProduct]] = {}
    for path in sorted((TWO_SHAPES_ROOT / "declarations" / "products").glob("*.yml")):
        document = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        if mutate is not None:
            mutate(path.name, document)
        entries[f"{document['product']['domain']}.{document['product']['name']}"] = (
            document,
            declaration_mod.validate_declaration(document, policy=policy),
        )
    return entries


def _two_shapes_registry(mutate=None):
    entries = _two_shapes_entries(mutate)
    return resolve_relation_registry(entries, landing_schemas=fixture_relation_schemas(entries))


def _relations(names: tuple[str, ...]) -> tuple[ShapeRelation, ...]:
    return tuple(
        ShapeRelation(
            suffix=None,
            schema=RelationSchema(name, (RelationField(f"field_of_{index}", "string"),)),
            derivation="composition",
        )
        for index, name in enumerate(names)
    )


def test_the_lookup_key_separates_two_reads_of_one_producer() -> None:
    # Both sides of the pipe key a read the same way: the producer's
    # published name, with the relation appended where the read names one,
    # so two reads of one producer never collide.
    assert source_relation_key("sales.order_star@1") == "sales.order_star"
    assert (
        source_relation_key("sales.order_star@1", "fact_order_line")
        == "sales.order_star#fact_order_line"
    )
    assert source_relation_key("sales.order_star@1", "dim_channel") != source_relation_key(
        "sales.order_star@1", "fact_order_line"
    )


def test_a_relation_is_declared_as_its_producer_names_it() -> None:
    # The declarable name of a relation is the producer's own name for it,
    # without the product prefix the contract reference already carries.
    assert relation_choices(
        "sales.order_star",
        ("sales.order_star__dim_channel", "sales.order_star__fact_order_line"),
    ) == ("dim_channel", "fact_order_line")
    # A product's own composition relation carries no name of its own, so
    # there is nothing to choose and nothing to declare.
    assert relation_choices("sales.order_feed", ("sales.order_feed",)) == ()


def test_a_producer_publishing_one_relation_resolves_without_a_key() -> None:
    assert (
        resolve_relation_name(
            ("crm.customer",), consumer="crm.report", producer="crm.customer", relation=None
        )
        == "crm.customer"
    )


def test_a_key_naming_one_of_several_relations_resolves_to_that_relation() -> None:
    relations = _relations(("p.q__a", "p.q__b"))
    chosen = resolve_source_relation(
        relations, consumer="p.consumer", producer="p.q", relation="b"
    )
    assert chosen.schema.name == "p.q__b", chosen


def test_a_key_on_a_producer_with_no_named_relation_fails_closed() -> None:
    # A product's own composition relation has no name of its own, so no
    # key can name it: the read simply omits the key.
    try:
        resolve_relation_name(
            ("crm.customer",), consumer="crm.report", producer="crm.customer", relation="anything"
        )
    except UnknownSourceRelationError as exc:
        assert exc.consumer == "crm.report" and exc.producer == "crm.customer"
        assert exc.relation == "anything" and exc.choices == ()
        assert "publishes: none" in str(exc), str(exc)
    else:
        raise AssertionError("expected UnknownSourceRelationError for a key naming another relation")


def test_a_key_repeating_the_product_prefix_is_told_the_form_expected() -> None:
    try:
        resolve_relation_name(
            ("sales.order_star__fact_order_line", "sales.order_star__dim_channel"),
            consumer="sales.report",
            producer="sales.order_star",
            relation="sales.order_star__fact_order_line",
        )
    except UnknownSourceRelationError as exc:
        assert exc.prefixed is True
        assert "without the 'sales.order_star' prefix" in str(exc), str(exc)
        assert "fact_order_line, dim_channel" in str(exc), str(exc)
    else:
        raise AssertionError("expected UnknownSourceRelationError for a prefixed relation key")


def test_a_consumer_reads_only_the_relation_its_source_names() -> None:
    registry = _two_shapes_registry()
    relations = registry["sales.order_line_revenue"]
    assert len(relations) == 1, relations
    fields = {field.name for field in relations[0].schema.fields}
    # The named fact's own columns, plus what this composition calculates.
    assert fields == {
        "order_id",
        "line_number",
        "customer_id",
        "channel_code",
        "order_date",
        "quantity",
        "net_amount",
        "multi_unit_line",
        "channel_name",
    }, sorted(fields)
    # The sibling fact's measure and the customer dimension's attributes
    # belong to other relations of the same producer, so they are not
    # visible here; channel_name is present only because a lookup named the
    # relation that publishes it.
    assert "tax_amount" not in fields and "customer_name" not in fields

    segmentation = registry["crm.customer_segment_report"]
    assert len(segmentation) == 1, segmentation
    published = {field.name for field in segmentation[0].schema.fields}
    assert published == {"customer_id", "customer_segment", "changed_at"}, sorted(published)
    # customer_name is the sibling entity view's column, not this one's.
    assert "customer_name" not in published


def test_the_resolved_reads_name_the_relation_of_the_source_and_of_the_lookup() -> None:
    entries = _two_shapes_entries()
    registry = _two_shapes_registry()
    document, _validated = entries["sales.order_line_revenue"]
    reads = resolve_consumer_reads(
        document,
        consumer="sales.order_line_revenue",
        resolved=registry,
        opening_schemas=fixture_relation_schemas(entries),
    )
    assert len(reads) == 2, reads
    source, lookup = reads
    assert (source.kind, source.producer) == (READ_SOURCE, "sales.order_star")
    assert source.relation == "sales.order_star__fact_order_line"
    assert source.suffix == "fact_order_line"
    assert source.key == "sales.order_star#fact_order_line"
    # The same producer, a second relation: the two reads resolve apart.
    assert (lookup.kind, lookup.producer) == (READ_LOOKUP, "sales.order_star")
    assert lookup.relation == "sales.order_star__dim_channel"
    assert lookup.suffix == "dim_channel"
    assert lookup.key == "sales.order_star#dim_channel"


def test_a_source_of_several_relations_naming_none_fails_closed() -> None:
    def drop_the_key(name: str, document: dict) -> None:
        if name == "order_line_revenue.yml":
            document["sources"][0].pop("relation")

    try:
        _two_shapes_registry(drop_the_key)
    except AmbiguousSourceRelationError as exc:
        assert exc.consumer == "sales.order_line_revenue"
        assert exc.producer == "sales.order_star"
        assert exc.relations == (
            "sales.order_star__dim_customer",
            "sales.order_star__dim_channel",
            "sales.order_star__fact_order_line",
            "sales.order_star__fact_order_line_tax",
        ), exc.relations
        assert exc.choices == (
            "dim_customer",
            "dim_channel",
            "fact_order_line",
            "fact_order_line_tax",
        ), exc.choices
        assert exc.read == READ_SOURCE, exc.read
        # Nothing resolves for that consumer: taking the first of several
        # is exactly the guess this failure exists to prevent.
        assert "which relation it reads" in str(exc)
    else:
        raise AssertionError("expected AmbiguousSourceRelationError for a source naming no relation")


def test_a_source_naming_a_relation_the_contract_does_not_list_fails_closed() -> None:
    def rename_the_key(name: str, document: dict) -> None:
        if name == "order_line_revenue.yml":
            document["sources"][0]["relation"] = "fact_absent"

    try:
        _two_shapes_registry(rename_the_key)
    except UnknownSourceRelationError as exc:
        assert exc.consumer == "sales.order_line_revenue"
        assert exc.producer == "sales.order_star"
        assert exc.relation == "fact_absent"
        assert exc.read == READ_SOURCE, exc.read
        assert "fact_order_line" in str(exc), str(exc)
    else:
        raise AssertionError("expected UnknownSourceRelationError for a relation no contract lists")


def test_a_lookup_of_several_relations_naming_none_fails_closed_as_a_lookup() -> None:
    def drop_the_lookup_key(name: str, document: dict) -> None:
        if name == "order_line_revenue.yml":
            step = next(s for s in document["steps"] if s["pattern"] == "data_enrichment")
            step["lookups"][0].pop("relation")

    try:
        _two_shapes_registry(drop_the_lookup_key)
    except AmbiguousSourceRelationError as exc:
        assert exc.consumer == "sales.order_line_revenue"
        assert exc.producer == "sales.order_star"
        assert exc.read == READ_LOOKUP, exc.read
        # The sentence names the lookup, so the fix it prescribes is one a
        # lookup can carry out.
        assert "lookup 'sales.order_star'" in str(exc), str(exc)
        assert "key on the lookup naming one of: dim_customer" in str(exc), str(exc)
    else:
        raise AssertionError("expected AmbiguousSourceRelationError for a lookup naming no relation")


def test_a_lookup_naming_a_relation_the_producer_does_not_publish_fails_closed() -> None:
    def rename_the_lookup_key(name: str, document: dict) -> None:
        if name == "order_line_revenue.yml":
            step = next(s for s in document["steps"] if s["pattern"] == "data_enrichment")
            step["lookups"][0]["relation"] = "dim_absent"

    try:
        _two_shapes_registry(rename_the_lookup_key)
    except UnknownSourceRelationError as exc:
        assert exc.consumer == "sales.order_line_revenue"
        assert exc.producer == "sales.order_star"
        assert exc.relation == "dim_absent"
        assert exc.read == READ_LOOKUP, exc.read
        assert "dim_channel" in str(exc), str(exc)
    else:
        raise AssertionError("expected UnknownSourceRelationError for a lookup relation nothing publishes")


def test_a_lookup_carries_in_the_fields_of_the_relation_it_names() -> None:
    registry = _two_shapes_registry()
    published = {
        field.name: field.type
        for field in registry["sales.order_line_revenue"][0].schema.fields
    }
    # channel_name is the channel dimension's attribute, carried in with
    # the type that relation publishes for it.
    assert published["channel_name"] == "string", published
    # customer_name is the customer dimension's attribute: a relation of
    # the same producer this consumer never reads.
    assert "customer_name" not in published, published


def test_a_source_of_a_single_relation_producer_needs_no_key_and_resolves() -> None:
    # crm.customer reads sales.order_feed, which publishes its composition
    # relation and nothing else, with no key at all.
    registry = _two_shapes_registry()
    assert [relation.schema.name for relation in registry["crm.customer"]] == ["crm.customer"]


def test_a_key_on_a_producer_publishing_only_its_own_relation_fails_closed() -> None:
    def name_a_relation(name: str, document: dict) -> None:
        if name == "customer.yml":
            document["sources"][0]["relation"] = "extra"

    try:
        _two_shapes_registry(name_a_relation)
    except UnknownSourceRelationError as exc:
        assert exc.consumer == "crm.customer" and exc.producer == "sales.order_feed"
        assert exc.relation == "extra" and exc.choices == ()
    else:
        raise AssertionError("expected UnknownSourceRelationError on a single-relation producer")


def test_the_graph_and_the_contract_derive_the_same_published_relations() -> None:
    """Two derivations of what a product publishes exist -- the
    graph's, from the declaration alone, and the contract's, from the
    resolved field set. They cannot be one function (the graph resolves no
    schemas), so this holds them to the same answer on every fixture
    estate, relation for relation and in order."""

    for root, landing in (
        (TWO_SHAPES_ROOT, None),
        (GRAPH_SIX_ROOT, GRAPH_SIX_LANDING_SCHEMAS),
    ):
        policy = declaration_mod.load_estate_policy(root / "estate.yml")
        entries: dict[str, tuple[dict, declaration_mod.ValidatedProduct]] = {}
        for path in sorted((root / "declarations" / "products").glob("*.yml")):
            document = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
            validated = declaration_mod.validate_declaration(document, policy=policy)
            entries[published_name(validated)] = (document, validated)
        schemas = fixture_relation_schemas(entries) if landing is None else landing
        registry = resolve_relation_registry(entries, landing_schemas=schemas)
        for name, (document, validated) in entries.items():
            assert published_relation_names(validated, document) == tuple(
                relation.schema.name for relation in registry[name]
            ), (root.name, name)


def test_an_expectation_on_a_sibling_relations_field_fails_closed() -> None:
    def expect_the_sibling_field(name: str, document: dict) -> None:
        if name == "order_line_revenue.yml":
            document["sources"][0]["expect"]["fields"].append("tax_amount")

    try:
        _two_shapes_registry(expect_the_sibling_field)
    except SourceExpectationError as exc:
        assert exc.consumer == "sales.order_line_revenue"
        assert exc.field == "tax_amount"
    else:
        raise AssertionError("expected SourceExpectationError for a sibling relation's field")


def test_a_fixture_bound_source_cannot_also_name_a_relation() -> None:
    def name_a_relation(name: str, document: dict) -> None:
        if name == "order_feed.yml":
            document["sources"][0]["relation"] = "customer_orders"

    try:
        _two_shapes_entries(name_a_relation)
    except declaration_mod.DeclarationError as exc:
        assert exc.rule == "fixture_source_names_a_relation", exc.rule
        assert "sources[0]" in str(exc)
    else:
        raise AssertionError("expected a DeclarationError for a fixture-bound source naming a relation")


# ------------------------------------------- how two or more sources combine

CONSOLIDATING_ROOT = Path(__file__).resolve().parent.parent / "fixtures" / "estates" / "consolidating_two"

DECIMAL_12_2 = {"name": "decimal", "precision": 12, "scale": 2}


def _consolidating_entries(mutate=None) -> dict[str, tuple[dict, declaration_mod.ValidatedProduct]]:
    """The consolidating estate's declarations, keyed by published name,
    with an optional mutation applied to one document before validation."""

    policy = declaration_mod.load_estate_policy(CONSOLIDATING_ROOT / "estate.yml")
    entries: dict[str, tuple[dict, declaration_mod.ValidatedProduct]] = {}
    for path in sorted((CONSOLIDATING_ROOT / "declarations" / "products").glob("*.yml")):
        document = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        if mutate is not None:
            mutate(path.name, document)
        entries[f"{document['product']['domain']}.{document['product']['name']}"] = (
            document,
            declaration_mod.validate_declaration(document, policy=policy),
        )
    return entries


def _consolidating_registry(mutate=None):
    entries = _consolidating_entries(mutate)
    return resolve_relation_registry(entries, landing_schemas=fixture_relation_schemas(entries))


def _fields(registry, published: str) -> list[tuple[str, object]]:
    relations = registry[published]
    assert len(relations) == 1, relations
    return [(field.name, field.type) for field in relations[0].schema.fields]


def test_a_landing_product_takes_its_schema_from_the_relation_it_binds() -> None:
    # Owner ruling R5: seed-backed data enters through a landing product.
    # No landing contract is supplied for this estate, so each landing
    # product's columns are the fields the relation it binds delivers.
    registry = _consolidating_registry()
    assert _fields(registry, "crm.customer_feed_b") == [
        ("cust_ref", "string"),
        ("ltv", "integer"),
        ("email_address", "string"),
        ("signup_date", "date"),
    ]


def test_a_union_publishes_the_conformed_schema_and_nothing_source_private() -> None:
    registry = _consolidating_registry()
    assert _fields(registry, "crm.customer_union") == [
        ("customer_id", "string"),
        ("email", "string"),
        ("signed_up_on", "date"),
        ("lifetime_value", DECIMAL_12_2),
        ("loyalty_tier", "string"),
        ("source_system", "string"),
    ]
    published = {name for name, _type in _fields(registry, "crm.customer_union")}
    for private in ("cust_ref", "email_address", "signup_date", "ltv"):
        assert private not in published, published


def test_a_merge_publishes_the_declared_keys_then_each_sources_own_columns() -> None:
    registry = _consolidating_registry()
    assert _fields(registry, "crm.customer_master") == [
        ("customer_id", "string"),
        ("email", "string"),
        ("signed_up_on", "date"),
        ("lifetime_value", DECIMAL_12_2),
        ("loyalty_tier", "string"),
        ("source_system", "string"),
        ("is_active", "boolean"),
        ("is_dormant", "boolean"),
    ]


def test_a_consumer_expecting_a_fields_pre_conformance_name_fails_closed() -> None:
    def expect_the_feeds_own_name(name: str, document: dict) -> None:
        if name == "customer_directory.yml":
            document["sources"][0]["expect"]["fields"].append("cust_ref")

    try:
        _consolidating_registry(expect_the_feeds_own_name)
    except SourceExpectationError as exc:
        assert exc.consumer == "crm.customer_directory", exc.consumer
        assert exc.producer == "crm.customer_union@1", exc.producer
        assert exc.field == "cust_ref", exc.field
    else:
        raise AssertionError("expected SourceExpectationError for a source-private field")


def test_the_resolved_composition_is_what_the_translator_reads() -> None:
    """One resolution, two readers. The composition the schema propagation
    used carries each source's key exactly as the translator looks its
    model up, and each conformed column under both names, so a rendered
    read can never be a different relation or a different column from the
    one the contract was derived from."""

    entries = _consolidating_entries()
    registry = resolve_relation_registry(
        entries, landing_schemas=fixture_relation_schemas(entries)
    )
    document, validated = entries["crm.customer_union"]
    schemas = consumer_source_schemas(
        document,
        consumer="crm.customer_union",
        resolved=registry,
        opening_schemas=fixture_relation_schemas(entries),
    )
    composition = resolve_opening_composition(
        document, consumer="crm.customer_union", source_schemas=schemas
    )
    assert composition.method == "union"
    assert composition.keys == ()
    assert [source.key for source in composition.sources] == [
        source_relation_key("crm.customer_a@1"),
        source_relation_key("crm.customer_b@1"),
    ]
    conformed = {
        (column.source_name, column.name, column.cast_type is not None)
        for column in composition.sources[1].columns
    }
    assert conformed == {
        ("cust_ref", "customer_id", True),
        ("email_address", "email", True),
        ("signup_date", "signed_up_on", True),
        ("ltv", "lifetime_value", True),
        ("loyalty_tier", "loyalty_tier", False),
        ("source_system", "source_system", False),
    }, conformed

    # The same resolution is what the propagation opened with.
    opening, _per_occurrence = propagate_relation_fields(document, validated, source_schemas=schemas)
    assert [field.name for field in opening] == [field.name for field in composition.fields]


def test_a_landing_product_with_two_schema_origins_fails_closed() -> None:
    # A supplied landing schema and a bound relation are two statements of
    # one relation's columns, free to drift apart. Reaching this needs an
    # estate that carries both, which no fixture estate does, so the branch
    # is driven directly, naming the product and what it declares.
    entries = _consolidating_entries()
    document, _validated = entries["crm.customer_feed_a"]
    try:
        landing_schema_for(
            document,
            product="crm.customer_feed_a",
            landing_schemas={
                "crm.customer_feed_a": RelationSchema(
                    "crm.customer_feed_a", (RelationField("customer_id", "string"),)
                )
            },
        )
    except LandingSchemaOriginError as exc:
        assert exc.product == "crm.customer_feed_a", exc.product
        assert "exactly one origin" in str(exc), str(exc)
        assert "crm.customer_feed_a_extract@1" in str(exc), str(exc)
    else:
        raise AssertionError("expected LandingSchemaOriginError for two schema origins")


def test_a_union_source_is_read_in_the_schemas_order_not_its_own() -> None:
    # A union stacks rows, and rows are matched by position, so a source
    # whose own relation carries the same fields in another order must
    # still contribute them in the order the schema publishes them. The
    # committed fixture estate states the second feed's columns in its own
    # order; this holds the rule on its own.
    document = {
        "sources": [
            {"contract": "crm.a@1"},
            {
                "contract": "crm.b@1",
                "conform": {"mapping": [{"from": "ref", "to": "id", "type": "string"}]},
            },
        ],
        "combine": {"method": "union"},
    }
    schemas = {
        "crm.a": RelationSchema(
            "crm.a", (RelationField("id", "string"), RelationField("tag", "string"))
        ),
        "crm.b": RelationSchema(
            "crm.b", (RelationField("tag", "string"), RelationField("ref", "string"))
        ),
    }
    composition = resolve_opening_composition(
        document, consumer="crm.pair", source_schemas=schemas
    )
    assert [field.name for field in composition.fields] == ["id", "tag"]
    for source in composition.sources:
        assert [column.name for column in source.columns] == ["id", "tag"], source


def test_two_sources_with_no_declared_combination_fail_closed_at_the_pipe() -> None:
    # Layer 1 rejects this before anything is generated. The pipe refuses
    # it too, at the point the schemas would actually be combined, so
    # nothing anywhere picks between a union and a merge.
    document = {
        "product": {"name": "pair", "domain": "ecommerce"},
        "sources": [
            {"contract": "ecommerce.crm_customer_landing@1"},
            {"contract": "ecommerce.storefront_customer@1"},
        ],
    }
    try:
        resolve_opening_composition(
            document, consumer="ecommerce.pair", source_schemas=LANDING_SCHEMAS
        )
    except UndeclaredCompositionError as exc:
        assert exc.product == "ecommerce.pair", exc.product
        assert exc.sources == 2, exc.sources
        assert "combine" in str(exc) and "union" in str(exc) and "merge" in str(exc)
    else:
        raise AssertionError("expected UndeclaredCompositionError for two sources and no combination")


def test_a_merge_with_no_declared_join_fails_closed_at_the_pipe() -> None:
    # Layer 1 rejects this before anything is generated. The pipe refuses it
    # too, where the schemas would actually be merged, because how required
    # each merged column is depends on which rows the merge keeps, and
    # nothing anywhere picks that for a declaration that named none.
    document = {
        "product": {"name": "pair", "domain": "ecommerce"},
        "sources": [
            {"contract": "ecommerce.crm_customer_landing@1"},
            {"contract": "ecommerce.storefront_customer@1"},
        ],
        "combine": {"method": "merge", "keys": ["customer_id"]},
    }
    try:
        resolve_opening_composition(
            document, consumer="ecommerce.pair", source_schemas=LANDING_SCHEMAS
        )
    except UndeclaredMergeJoinError as exc:
        assert exc.product == "ecommerce.pair", exc.product
        assert exc.join is None, exc.join
        message = str(exc)
        assert "'merge' combination declares which rows it keeps" in message, message
        assert "'inner', 'outer'" in message, message
        assert "'combine'.'join'" in message, message
        assert "got None" in message, message
    else:
        raise AssertionError("expected UndeclaredMergeJoinError for a merge with no join")


def test_a_merge_declaring_a_join_the_engine_does_not_know_fails_closed_at_the_pipe() -> None:
    document = {
        "product": {"name": "pair", "domain": "ecommerce"},
        "sources": [
            {"contract": "ecommerce.crm_customer_landing@1"},
            {"contract": "ecommerce.storefront_customer@1"},
        ],
        "combine": {"method": "merge", "keys": ["customer_id"], "join": "cross"},
    }
    try:
        resolve_opening_composition(
            document, consumer="ecommerce.pair", source_schemas=LANDING_SCHEMAS
        )
    except UndeclaredMergeJoinError as exc:
        assert exc.join == "cross", exc.join
        assert "got 'cross'" in str(exc), str(exc)
    else:
        raise AssertionError("expected UndeclaredMergeJoinError for an unknown join")


def main() -> int:
    tests = [obj for name, obj in sorted(globals().items()) if name.startswith("test_") and callable(obj)]
    failures = 0
    for test in tests:
        try:
            test()
            print(f"PASS {test.__name__}")
        except Exception:  # noqa: BLE001 -- report-and-continue harness
            failures += 1
            print(f"FAIL {test.__name__}")
            traceback.print_exc()
    print(f"\n{len(tests) - failures}/{len(tests)} passed")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
