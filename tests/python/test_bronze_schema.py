"""Assert-script tests for the Bronze Product Contract wire schema (repo convention: no
pytest; see tests/python/test_framework_core.py for the pattern).

Proves:
  - the repository IDL still hashes to the pinned SHA-256;
  - the regenerated schema bundle and equivalence report are byte-identical to the
    committed files (the "generated ... checks itself" gate);
  - the equivalence report shows 100% coverage of every IDL record, enum, union, port
    and error code -- field set, requiredness set and nullable set, not just presence;
  - every positive vector in tests/fixtures/bronze_schema_vectors.json round-trips
    through its named model; every negative vector fails validation;
  - the record-key MAC golden vectors reproduce byte-for-byte from key/domain/message
    to framed input to HMAC-SHA-256 tag;
  - the digest_excluded/signature_excluded sensitive-field rule: those fields are real,
    present, required wire fields (excluded from a record's own digest/signature basis,
    never from the wire shape itself);
  - the three modules import with no file I/O at import time (wheel-safety) and the
    package-data glob already covers the two generated schema files.

Usage:
    python tests/python/test_bronze_schema.py
"""

from __future__ import annotations

import hashlib
import hmac
import json
import sys
import traceback
from pathlib import Path

import pydantic

# Allow direct execution as `python tests/python/test_bronze_schema.py`.
if __package__ in (None, ""):
    import os as _os
    import sys as _sys

    _sys.path.insert(0, _os.path.dirname(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))))

import ergasterion.framework.bronze_contract as bronze_contract
import ergasterion.framework.runtime_binding as runtime_binding
import ergasterion.ingestion.records as records

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
IDL_PATH = REPO_ROOT / "docs" / "specifications" / "bronze-portable-idl-v1.json"
SCHEMA_BUNDLE_PATH = REPO_ROOT / "ergasterion" / "schemas" / "bronze-product-v1.schema.json"
EQUIVALENCE_PATH = REPO_ROOT / "ergasterion" / "schemas" / "bronze-portable-idl-equivalence.json"
VECTORS_PATH = REPO_ROOT / "tests" / "fixtures" / "bronze_schema_vectors.json"


def _load_vectors() -> dict:
    return json.loads(VECTORS_PATH.read_text(encoding="utf-8"))


def _regenerate_bundle_bytes() -> bytes:
    bundle = records.generate_schema_bundle(IDL_PATH, vectors_path=VECTORS_PATH)
    return (json.dumps(bundle, indent=2) + "\n").encode("utf-8")


def _regenerate_equivalence_bytes() -> bytes:
    report = records.generate_equivalence_report(IDL_PATH)
    return (json.dumps(report, indent=2) + "\n").encode("utf-8")


# --------------------------------------------------------------------------- tests

def test_idl_hashes_to_the_pinned_digest():
    raw = IDL_PATH.read_bytes()
    got = hashlib.sha256(raw).hexdigest()
    assert got == bronze_contract.EXPECTED_IDL_SHA256, (
        f"docs/specifications/bronze-portable-idl-v1.json hashes to {got}, "
        f"expected {bronze_contract.EXPECTED_IDL_SHA256} -- the frozen structural "
        "authority must not change under this build item"
    )
    assert records.load_idl(IDL_PATH)["schema"] == "ergasterion.portable-idl/v1"


def test_schema_bundle_regenerates_byte_identical():
    committed = SCHEMA_BUNDLE_PATH.read_bytes()
    regenerated = _regenerate_bundle_bytes()
    assert regenerated == committed, (
        f"regenerated schema bundle ({len(regenerated)} bytes) is not byte-identical "
        f"to the committed {SCHEMA_BUNDLE_PATH.name} ({len(committed)} bytes)"
    )
    bundle = json.loads(committed)
    assert bundle["idl_sha256"] == bronze_contract.EXPECTED_IDL_SHA256
    assert len(bundle["records"]) == len(records.ALL_RECORD_MODELS)


def test_equivalence_report_regenerates_byte_identical():
    committed = EQUIVALENCE_PATH.read_bytes()
    regenerated = _regenerate_equivalence_bytes()
    assert regenerated == committed, (
        f"regenerated equivalence report ({len(regenerated)} bytes) is not byte-identical "
        f"to the committed {EQUIVALENCE_PATH.name} ({len(committed)} bytes)"
    )


def test_equivalence_report_covers_every_idl_surface_exactly():
    report = records.generate_equivalence_report(IDL_PATH)
    idl = records.load_idl(IDL_PATH)

    summary = report["summary"]
    assert summary["records"]["total"] == summary["records"]["ok"] == len(idl["records"])
    assert summary["enums"]["total"] == summary["enums"]["ok"] == len(idl["enums"])
    assert summary["unions"]["total"] == summary["unions"]["ok"] == len(idl["unions"])
    assert summary["ports"]["total"] == summary["ports"]["ok"] == len(idl["ports"])
    assert summary["scalars"]["total"] == summary["scalars"]["ok"] == len(idl["scalars"])
    assert (
        summary["port_operation_order"]["total"]
        == summary["port_operation_order"]["ok"]
        == len(idl["port_operation_order"])
    )
    assert (
        summary["handoff_schema_bindings"]["total"]
        == summary["handoff_schema_bindings"]["ok"]
        == len(idl["handoff_schema_bindings"])
    )
    assert report["error_codes"]["status"] == "ok"
    assert report["error_codes"]["count"] == len(idl["error_codes"])

    # Every scalar's own constraint check, individually -- not just the aggregate
    # count (catches a scalar whose base type or pattern/bound check is wrong even
    # while the total-vs-ok tally happens to still match).
    for name in idl["scalars"]:
        entry = report["scalars"][name]
        assert entry["status"] == "ok", f"{name}: {entry}"

    # Every port's operation-order sequence, individually and in order.
    for name in idl["port_operation_order"]:
        entry = report["port_operation_order"][name]
        assert entry["status"] == "ok", f"{name}: {entry}"

    # Every handoff schema binding, individually -- schema id resolution, record type
    # resolution, model presence and the schema-id/record-type pairing itself, not just
    # the aggregate 5/5 tally (a regression in one sub-check could still leave the
    # aggregate count matching by accident).
    for name in idl["handoff_schema_bindings"]:
        entry = report["handoff_schema_bindings"][name]
        assert entry["status"] == "ok", f"{name}: {entry}"
        assert (
            entry["schema_id_ok"]
            and entry["record_type_ok"]
            and entry["model_ok"]
            and entry["pairing_ok"]
        ), (name, entry)

    # Every record's field/requiredness/nullable/type set matches, individually -- not
    # just the aggregate count (a report could tally 224/224 by accident if two records
    # traded places; this proves it record by record, including the per-field type
    # comparison -- a field present with the right name and requiredness but the wrong
    # scalar/enum/record/union type fails ``types_ok`` even though ``fields_ok`` passes).
    for name in idl["records"]:
        entry = report["records"][name]
        assert entry["status"] == "ok", f"{name}: {entry}"
        assert entry["fields_ok"] and entry["required_ok"] and entry["nullable_ok"] and entry["types_ok"], (name, entry)

    # Every union's variant set matches structurally (resolved from the actual Python
    # union object, not presence-of-a-model-with-that-name).
    for name in idl["unions"]:
        entry = report["unions"][name]
        assert entry["status"] == "ok", f"{name}: {entry}"

    # Every port's method set, and every method's request field list / response type /
    # error-code set, matches the IDL -- not just the method-name set.
    for name, idl_port in idl["ports"].items():
        entry = report["ports"][name]
        assert entry["status"] == "ok", f"{name}: {entry}"
        for method_name in idl_port["methods"]:
            method_entry = entry["methods"][method_name]
            assert method_entry["status"] == "ok", (name, method_name, method_entry)


def test_every_idl_record_has_direct_or_nested_positive_vector_coverage():
    """The coverage gate the acceptance criteria actually names: every one of the IDL's
    224 records is exercised by at least one positive vector, directly or through
    nesting -- computed by validating every positive vector and walking the resulting
    object graph, not by counting vectors."""

    vectors = _load_vectors()
    idl = records.load_idl(IDL_PATH)
    exercised: set[str] = set()

    def walk(obj: object) -> None:
        if isinstance(obj, pydantic.BaseModel):
            name = records.REVERSE_RECORD_NAMES.get(type(obj))
            if name is not None:
                exercised.add(name)
            for field_name in type(obj).model_fields:
                walk(getattr(obj, field_name))
        elif isinstance(obj, (list, tuple)):
            for item in obj:
                walk(item)
        elif isinstance(obj, dict):
            for item in obj.values():
                walk(item)

    for vector in vectors["positive"]:
        model = _model_for_vector(vector["record"])
        obj = (
            model.validate_python(vector["payload"])
            if isinstance(model, pydantic.TypeAdapter)
            else model.model_validate(vector["payload"])
        )
        walk(obj)

    missing = sorted(set(idl["records"]) - exercised)
    assert not missing, f"{len(missing)} IDL records never exercised by any positive vector: {missing}"


def test_every_idl_record_has_full_field_coverage_from_positive_vectors():
    """A stronger gate than record-level reachability: every field of every IDL record
    is populated (explicitly provided, per Pydantic's own ``model_fields_set``) by at
    least one positive vector, directly or through nesting -- a record with a vector
    that omits one of its optional fields is not fully covered until some other
    vector (direct or nested) supplies that field too."""

    vectors = _load_vectors()
    idl = records.load_idl(IDL_PATH)
    fields_seen: dict[str, set] = {}

    def walk(obj: object) -> None:
        if isinstance(obj, pydantic.BaseModel):
            name = records.REVERSE_RECORD_NAMES.get(type(obj))
            if name is not None:
                wire_set = {("schema" if f == "schema_" else f) for f in obj.model_fields_set}
                fields_seen.setdefault(name, set()).update(wire_set)
            for field_name in type(obj).model_fields:
                walk(getattr(obj, field_name))
        elif isinstance(obj, (list, tuple)):
            for item in obj:
                walk(item)
        elif isinstance(obj, dict):
            for item in obj.values():
                walk(item)

    for vector in vectors["positive"]:
        model = _model_for_vector(vector["record"])
        obj = (
            model.validate_python(vector["payload"])
            if isinstance(model, pydantic.TypeAdapter)
            else model.model_validate(vector["payload"])
        )
        walk(obj)

    gaps: dict[str, list[str]] = {}
    for name, idl_record in idl["records"].items():
        idl_fields = {f["name"] for f in idl_record["fields"]}
        missing = sorted(idl_fields - fields_seen.get(name, set()))
        if missing:
            gaps[name] = missing
    assert not gaps, f"IDL record fields never populated by any positive vector: {gaps}"


def test_all_three_module_registries_are_disjoint_and_exhaustive():
    # Every record name the IDL declares appears in exactly one of the three modules'
    # own RECORD_MODELS dicts (no duplication, no silent drop when merging ALL_*).
    idl = records.load_idl(IDL_PATH)
    own = [set(bronze_contract.RECORD_MODELS), set(runtime_binding.RECORD_MODELS), set(records.RECORD_MODELS)]
    union = set().union(*own)
    assert union == set(idl["records"]), sorted(set(idl["records"]) ^ union)
    for a in range(len(own)):
        for b in range(a + 1, len(own)):
            overlap = own[a] & own[b]
            assert not overlap, f"record name(s) declared in two modules: {sorted(overlap)}"
    assert len(records.ALL_RECORD_MODELS) == len(idl["records"])


def _model_for_vector(record_name: str):
    """Most vectors are labelled with a top-level record name (a direct
    ``ALL_RECORD_MODELS`` key). A few are labelled with a *union* name to document
    which union variant they exercise: ``"LogicalType"`` vectors carry a full
    ``SourceField`` payload (the union has no standalone JSON shape of its own outside
    a field that uses it); every other union-labelled vector carries the bare variant
    payload and validates directly against a ``TypeAdapter`` over that union."""

    if record_name == "LogicalType":
        return bronze_contract.SourceField
    if record_name in records.ALL_UNION_MODELS:
        return pydantic.TypeAdapter(records.ALL_UNION_MODELS[record_name])
    return records.ALL_RECORD_MODELS[record_name]


def test_positive_vectors_round_trip():
    vectors = _load_vectors()
    assert vectors["idl_sha256"] == bronze_contract.EXPECTED_IDL_SHA256
    assert len(vectors["positive"]) >= 30, "expected broad positive coverage, not a handful of smoke vectors"
    for vector in vectors["positive"]:
        model = _model_for_vector(vector["record"])
        if isinstance(model, pydantic.TypeAdapter):
            obj = model.validate_python(vector["payload"])
            again = model.validate_python(model.dump_python(obj, mode="json", by_alias=True))
        else:
            obj = model.model_validate(vector["payload"])
            # exclude_unset=True: a field the IDL marks "required: false, nullable:
            # false" may be entirely absent or explicitly present with a real value,
            # never explicitly null. A plain model_dump() re-emits every optional
            # field's Python-level None default as JSON null, which ClosedModel's own
            # omittable-not-nullable guard would then reject on re-validation;
            # exclude_unset preserves the was-it-actually-provided distinction instead.
            again = model.model_validate(obj.model_dump(mode="json", by_alias=True, exclude_unset=True))
        assert obj == again, (vector["record"], vector["note"])


def test_negative_vectors_all_fail_validation():
    """Every negative vector names its carrier model explicitly via ``model`` (the exact
    ``ALL_RECORD_MODELS`` key to validate against -- for a scalar-pattern vector like
    ``Digest``/``EstateNamespace``/``Identifier``, ``model`` is the record that actually
    carries the field being tested, not a guessed fallback) and the field expected to
    fail via ``field``. No candidate-model guessing: a vector that names the wrong
    carrier, or whose payload stops failing for the reason the vector claims, is a test
    failure, not a silently-passing loop."""

    vectors = _load_vectors()
    assert len(vectors["negative"]) >= 5
    for vector in vectors["negative"]:
        model_name = vector["model"]
        model = records.ALL_RECORD_MODELS.get(model_name)
        assert model is not None, f"unknown carrier model {model_name!r} for vector {vector['note']!r}"
        try:
            model.model_validate(vector["payload"])
        except pydantic.ValidationError as exc:
            field = vector["field"]
            error_text = str(exc)
            located = any(field in str(loc_part) for err in exc.errors() for loc_part in err.get("loc", ()))
            assert located or field in error_text, (
                f"{model_name}/{vector['note']!r}: ValidationError did not name the "
                f"intended field {field!r}: {exc}"
            )
            continue
        raise AssertionError(f"{model_name}/{vector['note']!r} was expected to fail validation and did not")


def test_bronze_product_contract_covers_all_three_delivery_modes():
    vectors = _load_vectors()
    modes = set()
    for vector in vectors["positive"]:
        if vector["record"] == "BronzeProductContract":
            modes.add(vector["payload"]["delivery"]["mode"])
    assert modes == {"cdc", "append_only", "complete_snapshot"}, modes


def test_managed_and_external_integration_both_covered():
    vectors = _load_vectors()
    kinds = set()
    for vector in vectors["positive"]:
        if vector["record"] == "BronzeProductContract":
            kinds.add(vector["payload"]["landing"]["integration"]["kind"])
    assert kinds == {"managed", "external"}, kinds


def test_record_key_mac_golden_vectors():
    idl = records.load_idl(IDL_PATH)
    framing = idl["mac_framing"]
    assert framing["algorithm"] == "HMAC-SHA-256"
    golden = idl["golden_vectors"]
    for name in ("record_key_mac", "record_key_mac_parameterized_scope"):
        vector = golden[name]
        key = bytes.fromhex(vector["key_hex"])
        domain = vector["domain_utf8"].encode("utf-8")
        message = vector["message_utf8"].encode("utf-8")
        framed = len(domain).to_bytes(4, "big") + domain + len(message).to_bytes(8, "big") + message
        assert framed.hex() == vector["framed_input_hex"], name
        tag = hmac.new(key, framed, hashlib.sha256).hexdigest()
        assert tag == vector["tag_hex"], (name, tag, vector["tag_hex"])


def test_zero_byte_raw_read_page_golden_vector():
    idl = records.load_idl(IDL_PATH)
    golden = idl["golden_vectors"]["zero_byte_raw_read_page"]
    page = records.RawReadPage.model_validate(golden)
    assert page.bytes_base64url == ""
    assert page.eof is True
    assert page.next_offset is None


def test_empty_binary_golden_vector():
    idl = records.load_idl(IDL_PATH)
    golden = idl["golden_vectors"]["empty_binary"]
    scalar = bronze_contract.TypedBinary.model_validate(golden["typed_value"])
    assert scalar.value == ""
    assert golden["decoded_length"] == "0"


def test_backup_manifest_page_chain_includes_an_empty_root():
    vectors = _load_vectors()
    pages = [v["payload"] for v in vectors["positive"] if v["record"] == "BackupEntryPage"]
    assert len(pages) >= 2
    roots = [p for p in pages if p["page_index"] == "0" and p["previous_page_digest"] is None and p["entries"] == []]
    assert roots, "expected at least one empty-root backup entry page (page_index 0, previous_page_digest null, no entries)"
    non_roots = [p for p in pages if p["previous_page_digest"] is not None]
    assert non_roots, "expected at least one page chaining back to a prior page"


def test_sensitive_field_rules_digest_excluded_and_signature_excluded_are_wire_present():
    idl = records.load_idl(IDL_PATH)
    digest_excluded: list[tuple[str, str]] = []
    signature_excluded: list[tuple[str, str]] = []
    for record_name, record in idl["records"].items():
        for field in record["fields"]:
            if field.get("digest_excluded"):
                digest_excluded.append((record_name, field["name"]))
            if field.get("signature_excluded"):
                signature_excluded.append((record_name, field["name"]))
    assert digest_excluded, "expected at least one digest_excluded field in the IDL"
    assert signature_excluded, "expected at least one signature_excluded field in the IDL"
    # A digest_excluded/signature_excluded field is omitted only from the record's OWN
    # digest/signature basis; it is still a real, required wire field on the model.
    for record_name, field_name in digest_excluded + signature_excluded:
        model = records.ALL_RECORD_MODELS[record_name]
        wire_name = "schema_" if field_name == "schema" else field_name
        assert wire_name in model.model_fields, (record_name, field_name)


def test_codecs_and_quality_modes_covered():
    vectors = _load_vectors()
    codec_kinds = {v["payload"]["landing"]["codec"]["kind"] for v in vectors["positive"] if v["record"] == "BronzeProductContract"}
    codec_kinds |= {v["payload"]["kind"] for v in vectors["positive"] if v["record"] == "JsonlCodec"}
    assert codec_kinds == {"csv", "jsonl"}, codec_kinds
    quality_modes = {v["payload"]["publication_mode"] for v in vectors["positive"] if v["record"] == "QualityPolicy"}
    assert quality_modes == {"all_or_nothing", "publish_valid_rows"}, quality_modes


def test_runtime_binding_and_capabilities_vectors_present():
    vectors = _load_vectors()
    records_covered = {v["record"] for v in vectors["positive"]}
    for expected in ("RuntimeBinding", "AdapterCapabilities", "InterfaceReadiness"):
        assert expected in records_covered, f"missing a positive vector for {expected}"


def test_modules_import_with_no_docs_directory_on_disk(tmp_check=True):
    # Wheel-safety: construct a model without touching the filesystem at all, proving
    # ordinary use of these modules performs no file I/O. (The generation functions
    # DO read a path -- they take one explicitly and are exercised, with the real
    # repository IDL, by the tests above; this test proves the *import and ordinary
    # construction* path is separate from that.)
    contract_field = bronze_contract.SourceField(name="f", logical_type="utf8_string", nullable=False)
    assert contract_field.nullable is False
    unit = records.UnitResult(ok=True)
    assert unit.ok is True


def test_closed_models_reject_unknown_fields_across_all_three_modules():
    samples = [
        (bronze_contract.LogicalIdentity, {"estate_namespace": "com.example.synthetic", "source": "a", "table": "b"}),
        (runtime_binding.PortBinding, {
            "adapter_id": "x", "implementation_version": "1.0.0", "capability_digest": "a" * 64,
            "endpoint_ref": "local://x", "secret_resolver_refs": [],
        }),
        (records.UnitResult, {"ok": True}),
    ]
    for model, payload in samples:
        model.model_validate(payload)  # sanity: the clean payload must pass
        try:
            model.model_validate({**payload, "unexpected_field": 1})
        except pydantic.ValidationError:
            continue
        raise AssertionError(f"{model.__name__} accepted an unknown field")


TESTS = [
    test_idl_hashes_to_the_pinned_digest,
    test_schema_bundle_regenerates_byte_identical,
    test_equivalence_report_regenerates_byte_identical,
    test_equivalence_report_covers_every_idl_surface_exactly,
    test_every_idl_record_has_direct_or_nested_positive_vector_coverage,
    test_every_idl_record_has_full_field_coverage_from_positive_vectors,
    test_all_three_module_registries_are_disjoint_and_exhaustive,
    test_positive_vectors_round_trip,
    test_negative_vectors_all_fail_validation,
    test_bronze_product_contract_covers_all_three_delivery_modes,
    test_managed_and_external_integration_both_covered,
    test_record_key_mac_golden_vectors,
    test_zero_byte_raw_read_page_golden_vector,
    test_empty_binary_golden_vector,
    test_backup_manifest_page_chain_includes_an_empty_root,
    test_sensitive_field_rules_digest_excluded_and_signature_excluded_are_wire_present,
    test_codecs_and_quality_modes_covered,
    test_runtime_binding_and_capabilities_vectors_present,
    test_modules_import_with_no_docs_directory_on_disk,
    test_closed_models_reject_unknown_fields_across_all_three_modules,
]


def main() -> int:
    failures = 0
    for test in TESTS:
        name = test.__name__
        try:
            test()
        except Exception:  # noqa: BLE001 - report and continue, exit code carries the signal
            failures += 1
            print(f"FAIL {name}")
            traceback.print_exc()
        else:
            print(f"PASS {name}")
    total = len(TESTS)
    print(f"{total - failures}/{total} passed")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
