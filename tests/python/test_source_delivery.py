"""Python-level unit tests for ergasterion/source_delivery.py, the Bronze
Product Contract compiler.

The compiler turns authored YAML into validated, digested Bronze Product
Contracts. This file proves the eleven behaviours the contract rests on:

  * the typed declaration loader is the single source of typed Bronze intent,
    and it demands every required identity, version, ownership, domain, support,
    access, classification and retention fact before it will compile anything;
  * lineage interface names are derived from logical identity, never authored;
  * the semantic validator enforces every cross-field rule the wire shapes
    cannot express: the mode matrix, codec rules, typed-scalar rules, projection
    lineage and schedule grammar;
  * canonicalisation is stable: declared-set order never moves a digest, and a
    canonical document re-parses into the record it came from;
  * the derived-digest family (delivery claim, reprocessing id, migration id)
    excludes exactly the fields the frozen IDL marks digest_excluded;
  * the compatibility classifier and SemVer gate agree on what a change costs;
  * the migration state machine handles candidate activation, carry and reset,
    visibility ancestry and the in-flight race;
  * the schedule engine resolves interval and cron boundaries correctly across
    both daylight-saving discontinuities, against a pinned zone database;
  * ergasterion.emit.load_declarations() keeps its exact legacy behaviour and the
    committed estate still generates byte-identical output;
  * no Jinja template performs semantic validation.

Same plain assert/report convention as tests/python/test_emit.py (no pytest in
this repo's .venv): each test_* raises AssertionError on failure, main() runs
them all and reports PASS/FAIL, exit code 0 = all green, 1 = any failure.

Usage:
    python tests/python/test_source_delivery.py
"""

from __future__ import annotations

import contextlib
import copy
import hashlib
import io
import json
import sys
import tempfile
import traceback
from datetime import datetime, timezone
from pathlib import Path

import yaml

if __package__ in (None, ""):
    import os as _os, sys as _sys
    _sys.path.insert(0, _os.path.dirname(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))))

from ergasterion import emit, source_delivery as sd
from ergasterion.estate import EstateContext
from ergasterion.framework.bronze_contract import (
    EXPECTED_IDL_SHA256,
    BronzeProductContract,
    ContractActivationState,
    CronSchedule,
    IntervalSchedule,
    MigrationKind,
)
from ergasterion.ingestion.records import DeliveryClaim, DeliveryManifest, ReprocessingClaim

REPO_ROOT = Path(__file__).resolve().parents[2]
VECTORS_PATH = REPO_ROOT / "tests" / "fixtures" / "source_delivery_vectors.json"
IDL_PATH = REPO_ROOT / "docs" / "specifications" / "bronze-portable-idl-v1.json"

ZERO_DIGEST = "0" * 64
ONE_DIGEST = "1" * 64


# --- fixture plumbing ---------------------------------------------------------------

def _vectors() -> dict:
    return json.loads(VECTORS_PATH.read_text(encoding="utf-8"))


def _positive_payloads() -> dict[str, dict]:
    return {entry["case"]: entry["payload"] for entry in _vectors()["positive"]}


def _contract(case: str) -> BronzeProductContract:
    return BronzeProductContract.model_validate(_positive_payloads()[case])


def _apply_patch(payload: dict, path: str, value: object) -> dict:
    """Replace the node a dotted path names. A path segment that indexes a list
    is a plain integer, so a vector can patch one quality rule in place."""
    patched = copy.deepcopy(payload)
    cursor: object = patched
    parts = path.split(".")
    for part in parts[:-1]:
        cursor = cursor[int(part)] if isinstance(cursor, list) else cursor[part]
    last = parts[-1]
    if isinstance(cursor, list):
        cursor[int(last)] = value
    else:
        cursor[last] = value
    return patched


# --- vector-driven validation -------------------------------------------------------

def test_positive_vectors_validate_and_digest_deterministically() -> None:
    """Every positive vector is a whole valid contract, and each of its four
    digests is a pure function of the contract."""
    vectors = _vectors()
    assert len(vectors["positive"]) >= 4, "expected a vector per delivery mode plus the CSV/external case"
    seen_digests = set()
    for entry in vectors["positive"]:
        contract = BronzeProductContract.model_validate(entry["payload"])
        sd.validate_contract(contract, where=entry["case"])
        digests = (
            sd.compute_contract_digest(contract),
            sd.compute_source_schema_digest(contract),
            sd.compute_published_schema_digest(contract),
            sd.compute_ruleset_digest(
                sd.compute_source_schema_digest(contract),
                sd.compute_published_schema_digest(contract),
                contract.delivery.quality.rules,
            ),
        )
        for digest in digests:
            assert len(digest) == 64 and digest == digest.lower(), (
                f"{entry['case']}: expected lowercase hex SHA-256, got {digest!r}"
            )
        repeated = (
            sd.compute_contract_digest(contract),
            sd.compute_source_schema_digest(contract),
            sd.compute_published_schema_digest(contract),
            sd.compute_ruleset_digest(
                sd.compute_source_schema_digest(contract),
                sd.compute_published_schema_digest(contract),
                contract.delivery.quality.rules,
            ),
        )
        assert digests == repeated, f"{entry['case']}: digests must be deterministic"
        assert digests[0] not in seen_digests, f"{entry['case']}: distinct contracts share a digest"
        seen_digests.add(digests[0])


def test_positive_vectors_cover_every_mode_codec_integration_and_schedule() -> None:
    """The vector set exercises all three delivery modes, both codecs, both
    integrations and both schedule kinds, so no branch of the matrix is untested."""
    payloads = list(_positive_payloads().values())
    modes = {payload["delivery"]["mode"] for payload in payloads}
    codecs = {payload["landing"]["codec"]["kind"] for payload in payloads}
    integrations = {payload["landing"]["integration"]["kind"] for payload in payloads}
    schedules = {payload["delivery"]["schedule"]["kind"] for payload in payloads}
    assert modes == {"cdc", "append_only", "complete_snapshot"}, modes
    assert codecs == {"csv", "jsonl"}, codecs
    assert integrations == {"managed", "external"}, integrations
    assert schedules == {"interval", "cron"}, schedules

    scalar_kinds = set()
    for payload in payloads:
        for rule in payload["delivery"]["quality"]["rules"]:
            for scalar in list(rule.get("values", [])) + [
                bound for bound in (rule.get("min"), rule.get("max")) if isinstance(bound, dict)
            ]:
                scalar_kinds.add(scalar["logical_type"])
        tombstone = payload["delivery"].get("tombstone")
        if tombstone:
            for scalar in tombstone["values"]:
                scalar_kinds.add(scalar["logical_type"])
    assert {"boolean", "utf8_string", "decimal", "date"} <= scalar_kinds, scalar_kinds


def test_negative_vectors_fail_their_named_validator() -> None:
    """Every negative vector is rejected by the entry point it names, with the
    violation the vector expects. A vector that stops failing is a validator that
    stopped validating."""
    payloads = _positive_payloads()
    for entry in _vectors()["negative"]:
        payload = payloads[entry["base"]]
        for path, value in entry["patch"].items():
            payload = _apply_patch(payload, path, value)
        which = entry.get("validator", "delivery")
        try:
            contract = BronzeProductContract.model_validate(payload)
            if which == "contract":
                sd.validate_contract(contract)
            else:
                sd.validate_delivery_policy(contract.delivery)
        except sd.ContractValidationError as exc:
            assert entry["expected_error_substring"] in str(exc), (
                f"{entry['case']}: expected {entry['expected_error_substring']!r} in: {exc}"
            )
        else:
            raise AssertionError(f"{entry['case']}: expected a ContractValidationError, none raised")


def test_validation_error_reports_every_violation_at_once() -> None:
    """A contract with several independent faults names them all, so an author
    fixes one round of errors rather than one error per round."""
    payload = _positive_payloads()["append_only_managed_opaque_batch"]
    payload = _apply_patch(payload, "delivery.record_key", {"fields": []})
    payload = _apply_patch(
        payload, "delivery.schedule_lateness", {"warn_after_minutes": 60, "error_after_minutes": 60}
    )
    payload = _apply_patch(payload, "landing.content_encodings", ["identity", "identity"])
    contract = BronzeProductContract.model_validate(payload)
    try:
        sd.validate_contract(contract)
    except sd.ContractValidationError as exc:
        assert len(exc.violations) >= 3, f"expected every violation reported, got: {exc.violations}"
        joined = str(exc)
        for expected in ("record_key.fields must be nonempty", "schedule_lateness", "repeat an encoding"):
            assert expected in joined, f"expected {expected!r} in: {joined}"
    else:
        raise AssertionError("expected a ContractValidationError, none raised")


# --- canonicalisation ---------------------------------------------------------------

def test_canonical_document_omits_absent_optionals_and_reparses() -> None:
    """The IDL canonicalisation rule is `absent_optional: omit`. A field marked
    `required: false, nullable: false` rejects an explicit null on the way back
    in, so canonical bytes carrying that null would describe a record that cannot
    re-parse into the one they came from."""
    contract = _contract("append_only_managed_opaque_batch")
    document = sd.canonical_contract_document(contract)["contract"]

    delivery = document["delivery"]
    for absent in ("tombstone", "snapshot", "maximum_age"):
        assert absent not in delivery, f"an absent omittable field must be omitted, found {absent}"
    for absent in ("event_field", "effective_field"):
        assert absent not in delivery["timestamps"], f"found {absent} on an absent timestamp"
    for absent in ("fingerprint_scope", "hmac_key_id"):
        assert absent not in delivery["record_key"], f"found {absent} on an absent record-key fact"

    reparsed = BronzeProductContract.model_validate(document)
    assert sd.canonical_contract_document(reparsed)["contract"] == document, (
        "canonicalisation must be idempotent"
    )
    assert sd.compute_contract_digest(reparsed) == sd.compute_contract_digest(contract), (
        "a canonical document must digest to the contract it came from"
    )


def test_declared_set_reordering_leaves_every_digest_equal() -> None:
    """Physical columns, projection entries, quality rules and every declared set
    are normalised before hashing, so authoring order never moves a digest."""
    payload = _positive_payloads()["csv_external_append_only"]
    contract = BronzeProductContract.model_validate(payload)

    shuffled = copy.deepcopy(payload)
    shuffled["landing"]["physical_columns"].reverse()
    shuffled["landing"]["content_encodings"].reverse()
    shuffled["landing"]["integration"]["receipt_trust"]["allowed_key_ids"].reverse()
    shuffled["projection"].reverse()
    shuffled["delivery"]["quality"]["rules"].reverse()
    shuffled["delivery"]["quality"]["rules"][3]["values"].reverse()
    reordered = BronzeProductContract.model_validate(shuffled)

    assert sd.compute_contract_digest(reordered) == sd.compute_contract_digest(contract)
    assert sd.compute_source_schema_digest(reordered) == sd.compute_source_schema_digest(contract)
    assert sd.compute_published_schema_digest(reordered) == sd.compute_published_schema_digest(contract)
    assert sd.compute_ruleset_digest(
        sd.compute_source_schema_digest(reordered),
        sd.compute_published_schema_digest(reordered),
        reordered.delivery.quality.rules,
    ) == sd.compute_ruleset_digest(
        sd.compute_source_schema_digest(contract),
        sd.compute_published_schema_digest(contract),
        contract.delivery.quality.rules,
    )
    assert sd.classify_contract_change(contract, reordered) == sd.ChangeClass.NONE, (
        "reordering a declared set is not a contract change"
    )


def test_canonicalisation_honours_every_ordering_hint_the_contract_carries() -> None:
    """Each list field in the contract-declaration records carries an IDL
    `ordering` hint, and canonicalisation applies exactly one normalisation per
    hint. This reads the hints from the frozen IDL, so a hint that changes or a
    list field that appears fails here rather than drifting silently."""
    records = _idl()["records"]
    contract_records = (
        "BronzeProductContract", "LandingContract", "ExternalTrustPolicy", "CsvCodec",
        "RecordKeyContract", "QualityPolicy", "AcceptedValuesRule", "UniqueKeyRule",
        "TombstoneContract", "SnapshotContract",
    )
    hints = {
        f"{name}.{field['name']}": field["ordering"]
        for name in contract_records
        for field in records[name].get("fields", [])
        if "ordering" in field
    }
    assert hints == {
        "BronzeProductContract.projection": "declared",
        "LandingContract.physical_columns": "declared",
        "LandingContract.content_encodings": "set",
        "ExternalTrustPolicy.allowed_key_ids": "set",
        "CsvCodec.null_tokens": "authored",
        "RecordKeyContract.fields": "ordered",
        "QualityPolicy.rules": "rule_id",
        "AcceptedValuesRule.values": "set",
        "UniqueKeyRule.fields": "ordered",
        "TombstoneContract.values": "set",
        "SnapshotContract.allowed_key_ids": "set",
    }, hints

    contract = _contract("csv_external_append_only")
    document = sd.canonical_contract_document(contract)["contract"]
    landing = document["landing"]
    assert landing["content_encodings"] == sorted(landing["content_encodings"]), "set"
    assert landing["integration"]["receipt_trust"]["allowed_key_ids"] == ["key-a", "key-b"], "set"
    assert landing["codec"]["null_tokens"] == ["", "NULL"], "authored order preserved"
    assert [column["name"] for column in landing["physical_columns"]] == sorted(
        column["name"] for column in landing["physical_columns"]
    ), "physical columns sort by name"
    assert [entry["name"] for entry in document["projection"]] == sorted(
        entry["name"] for entry in document["projection"]
    ), "projection sorts by output name"
    assert document["delivery"]["record_key"]["fields"] == ["txn_id", "seq"], "ordered"
    rule_ids = [sd.compute_rule_id(rule) for rule in contract.delivery.quality.rules]
    canonical_rule_ids = [
        sd.compute_rule_id(type(rule).model_validate(dumped))
        for rule, dumped in zip(
            sorted(contract.delivery.quality.rules, key=sd.compute_rule_id),
            document["delivery"]["quality"]["rules"],
        )
    ]
    assert canonical_rule_ids == sorted(rule_ids), "quality rules sort by rule id"


def test_declared_set_rejects_a_repeated_value() -> None:
    """A set-like list rejects duplicates before it sorts them, so a repeated
    accepted value fails rather than collapsing silently."""
    payload = _positive_payloads()["csv_external_append_only"]
    payload["delivery"]["quality"]["rules"][2]["values"].append(
        {"logical_type": "utf8_string", "value": "settled"}
    )
    contract = BronzeProductContract.model_validate(payload)
    try:
        sd.compute_contract_digest(contract)
    except sd.ContractValidationError as exc:
        assert "must not repeat a value" in str(exc), str(exc)
    else:
        raise AssertionError("a repeated value in a declared set must fail canonicalisation")


def test_record_key_field_order_is_load_bearing() -> None:
    """Record-key fields keep their authored order: the composite-key encoding
    depends on it, so reordering them is a real change to the contract."""
    contract = _contract("csv_external_append_only")
    payload = _apply_patch(
        _positive_payloads()["csv_external_append_only"],
        "delivery.record_key",
        {"fields": ["seq", "txn_id"]},
    )
    swapped = BronzeProductContract.model_validate(payload)
    assert sd.compute_contract_digest(swapped) != sd.compute_contract_digest(contract), (
        "record_key field order must reach the contract digest"
    )
    assert sd.classify_contract_change(contract, swapped) == sd.ChangeClass.MAJOR


def test_schema_digests_isolate_the_facts_they_name() -> None:
    """The source-schema digest covers the parsed shape and the published-schema
    digest covers the consumer-visible shape, so a change to one leaves the other
    equal."""
    base_payload = _positive_payloads()["csv_external_append_only"]
    contract = BronzeProductContract.model_validate(base_payload)

    quality_changed = copy.deepcopy(base_payload)
    quality_changed["delivery"]["quality"]["rules"][5] = {
        "kind": "row_count", "min": "2", "max": "1000000", "severity": "warn"
    }
    quality = BronzeProductContract.model_validate(quality_changed)
    assert sd.compute_source_schema_digest(quality) == sd.compute_source_schema_digest(contract)
    assert sd.compute_published_schema_digest(quality) == sd.compute_published_schema_digest(contract)
    assert sd.compute_contract_digest(quality) != sd.compute_contract_digest(contract)
    assert sd.compute_ruleset_digest(
        sd.compute_source_schema_digest(quality),
        sd.compute_published_schema_digest(quality),
        quality.delivery.quality.rules,
    ) != sd.compute_ruleset_digest(
        sd.compute_source_schema_digest(contract),
        sd.compute_published_schema_digest(contract),
        contract.delivery.quality.rules,
    )

    column_added = copy.deepcopy(base_payload)
    column_added["landing"]["physical_columns"].append(
        {"name": "note", "logical_type": "utf8_string", "nullable": True}
    )
    widened = BronzeProductContract.model_validate(column_added)
    assert sd.compute_source_schema_digest(widened) != sd.compute_source_schema_digest(contract)
    assert sd.compute_published_schema_digest(widened) == sd.compute_published_schema_digest(contract)

    renamed_output = copy.deepcopy(base_payload)
    renamed_output["projection"][4]["name"] = "note"
    republished = BronzeProductContract.model_validate(renamed_output)
    assert sd.compute_source_schema_digest(republished) == sd.compute_source_schema_digest(contract)
    assert sd.compute_published_schema_digest(republished) != sd.compute_published_schema_digest(contract)


def test_rule_id_ignores_severity_and_ruleset_ignores_authored_order() -> None:
    """Two rules differing only in severity carry one rule identity, and the
    ruleset digest orders rules by that identity."""
    contract = _contract("csv_external_append_only")
    rules = contract.delivery.quality.rules
    escalated = rules[0].model_copy(update={"severity": rules[0].severity})
    assert sd.compute_rule_id(escalated) == sd.compute_rule_id(rules[0])

    warn_variant = type(rules[0]).model_validate(
        {**rules[0].model_dump(mode="json"), "severity": "warn"}
    )
    assert sd.compute_rule_id(warn_variant) == sd.compute_rule_id(rules[0]), (
        "severity must not change a rule identity"
    )

    source_digest = sd.compute_source_schema_digest(contract)
    published_digest = sd.compute_published_schema_digest(contract)
    assert sd.compute_ruleset_digest(
        source_digest, published_digest, tuple(reversed(rules))
    ) == sd.compute_ruleset_digest(source_digest, published_digest, rules)


# --- derived digests ----------------------------------------------------------------

def _idl() -> dict:
    raw = IDL_PATH.read_bytes()
    assert hashlib.sha256(raw).hexdigest() == EXPECTED_IDL_SHA256, (
        "the frozen IDL bytes changed; the compiler projects it and never edits it"
    )
    return json.loads(raw.decode("utf-8"))


def test_derived_digest_exclusions_match_the_frozen_idl() -> None:
    """The compiler's digest-exclusion table is a projection of the IDL's own
    digest_excluded markers, checked against the frozen bytes so it cannot drift
    from the interface it projects."""
    expected: dict[str, set[str]] = {}
    for record_name, record in _idl()["records"].items():
        excluded = {
            field["name"] for field in record.get("fields", []) if field.get("digest_excluded")
        }
        if excluded:
            expected[record_name] = excluded
    actual = {name: set(fields) for name, fields in sd.DERIVED_DIGEST_EXCLUSIONS.items()}
    assert actual == expected, (
        f"digest-exclusion drift: missing {sorted(set(expected) - set(actual))}, "
        f"extra {sorted(set(actual) - set(expected))}, "
        f"differing {sorted(name for name in set(expected) & set(actual) if expected[name] != actual[name])}"
    )


def _delivery_manifest() -> DeliveryManifest:
    return DeliveryManifest.model_validate(
        {
            "schema": "ergasterion.delivery-manifest/v1",
            "logical_identity": {
                "estate_namespace": "com.example.ergasterion", "source": "acme", "table": "orders"
            },
            "product_version": "1.0.0",
            "contract_digest": ZERO_DIGEST,
            "delivery_id": "delivery-2026-08-19",
            "batch_id": None,
            "scheduled_boundary_at": "2026-08-19T00:00:00.000000Z",
            "effective_boundary_at": None,
            "payload": {
                "media_type": "application/x-ndjson",
                "content_encoding": "identity",
                "codec_version": 1,
                "byte_length": "2048",
                "sha256": ONE_DIGEST,
            },
            "frame_sequence_digest": None,
            "progress_claim": {"kind": "opaque_batch"},
            "declared_row_count": "17",
            "snapshot_attestation": None,
        }
    )


def test_delivery_claim_digest_excludes_only_its_own_field() -> None:
    """A delivery claim's digest is a function of the manifest it carries: the
    field holding the digest is outside its own basis, and every other field is
    inside it. That is what makes a replayed claim detectable."""
    manifest = _delivery_manifest()
    fields = {"schema": "ergasterion.delivery-claim/v1", "claim": manifest}
    digest = sd.compute_delivery_claim_digest(fields)
    claim = DeliveryClaim(delivery_claim_digest=digest, **fields)

    assert sd.compute_delivery_claim_digest(claim) == digest, (
        "a built claim must digest to the value it was built with"
    )
    stale = claim.model_copy(update={"delivery_claim_digest": ZERO_DIGEST})
    assert sd.compute_delivery_claim_digest(stale) == digest, (
        "the excluded field must sit outside its own digest basis"
    )
    replay = DeliveryClaim(delivery_claim_digest=digest, **fields)
    assert sd.compute_delivery_claim_digest(replay) == digest, "one manifest claims one identity"

    changed = claim.model_copy(
        update={"claim": manifest.model_copy(update={"declared_row_count": "18"})}
    )
    assert sd.compute_delivery_claim_digest(changed) != digest, (
        "every field outside the exclusion must reach the digest"
    )


def test_reprocessing_id_covers_the_original_claim_and_every_target_digest() -> None:
    """Reprocessing the same preserved bytes to the same target contract is one
    identity; a different target version, ruleset or plan is a different one."""
    fields = {
        "schema": "ergasterion.reprocessing-claim/v1",
        "original_claim_digest": ZERO_DIGEST,
        "raw_receipt_digest": ONE_DIGEST,
        "target_product_version": "2.0.0",
        "target_contract_digest": "2" * 64,
        "target_source_schema_digest": "3" * 64,
        "target_published_schema_digest": "4" * 64,
        "target_ruleset_digest": "5" * 64,
        "execution_plan_digest": "6" * 64,
    }
    reprocessing_id = sd.compute_reprocessing_id(fields)
    claim = ReprocessingClaim(reprocessing_id=reprocessing_id, **fields)
    assert sd.compute_reprocessing_id(claim) == reprocessing_id
    assert sd.compute_reprocessing_id(
        claim.model_copy(update={"reprocessing_id": ZERO_DIGEST})
    ) == reprocessing_id, "the excluded field sits outside its own basis"

    for key in ("target_product_version", "target_ruleset_digest", "execution_plan_digest"):
        altered = dict(fields)
        altered[key] = "9.9.9" if key == "target_product_version" else "7" * 64
        assert sd.compute_reprocessing_id(altered) != reprocessing_id, (
            f"{key} must reach the reprocessing identity"
        )


def test_derived_digest_refuses_a_record_carrying_no_derived_field() -> None:
    """Asking for the derived digest of a record the IDL gives none names the
    mistake rather than inventing a digest."""
    try:
        sd.compute_derived_digest("BronzeProductContract", {"schema": "x"})
    except ValueError as exc:
        assert "digest_excluded" in str(exc), str(exc)
    else:
        raise AssertionError("expected a ValueError for a record with no derived digest")


# --- compatibility classifier and SemVer --------------------------------------------

def test_classifier_grades_every_change_class() -> None:
    """The classifier grades an unchanged contract, a documentation-only edit, an
    additive change, a breaking change and a new product."""
    base_payload = _positive_payloads()["csv_external_append_only"]
    base = BronzeProductContract.model_validate(base_payload)

    assert sd.classify_contract_change(None, base) == sd.ChangeClass.NEW_PRODUCT
    assert sd.classify_contract_change(base, base) == sd.ChangeClass.NONE

    described = _apply_patch(base_payload, "product.description", "A revised description.")
    assert sd.classify_contract_change(
        base, BronzeProductContract.model_validate(described)
    ) == sd.ChangeClass.PATCH

    widened = copy.deepcopy(base_payload)
    widened["landing"]["physical_columns"].append(
        {"name": "note", "logical_type": "utf8_string", "nullable": True}
    )
    assert sd.classify_contract_change(
        base, BronzeProductContract.model_validate(widened)
    ) == sd.ChangeClass.MINOR

    reclassified = _apply_patch(base_payload, "product.classification", "restricted")
    assert sd.classify_contract_change(
        base, BronzeProductContract.model_validate(reclassified)
    ) == sd.ChangeClass.MINOR

    required_column = copy.deepcopy(base_payload)
    required_column["landing"]["physical_columns"].append(
        {"name": "note", "logical_type": "utf8_string", "nullable": False}
    )
    assert sd.classify_contract_change(
        base, BronzeProductContract.model_validate(required_column)
    ) == sd.ChangeClass.MAJOR

    dropped_column = copy.deepcopy(base_payload)
    dropped_column["landing"]["physical_columns"] = [
        column for column in dropped_column["landing"]["physical_columns"] if column["name"] != "memo"
    ]
    dropped_column["projection"] = [
        entry for entry in dropped_column["projection"] if entry["source"] != "memo"
    ]
    assert sd.classify_contract_change(
        base, BronzeProductContract.model_validate(dropped_column)
    ) == sd.ChangeClass.MAJOR

    renamed = _apply_patch(base_payload, "logical_identity.table", "entries")
    renamed = _apply_patch(renamed, "interfaces", sd.derive_interfaces("ledger", "entries").model_dump())
    assert sd.classify_contract_change(
        base, BronzeProductContract.model_validate(renamed)
    ) == sd.ChangeClass.NEW_PRODUCT

    rescheduled = _apply_patch(
        base_payload,
        "delivery.schedule",
        {"kind": "interval", "every_minutes": 30, "anchor_at": "2026-01-01T00:00:00.000000Z"},
    )
    assert sd.classify_contract_change(
        base, BronzeProductContract.model_validate(rescheduled)
    ) == sd.ChangeClass.MINOR


def test_classifier_covers_every_migration_matrix_row() -> None:
    """One vector per remaining migration-matrix row, in the direction the row
    names, per docs/specifications/bronze-product-v1.md. Attestation
    issuer/trust policy is Minor, not Major -- the row a prior round of this
    compiler misgraded."""
    external_base = _positive_payloads()["csv_external_append_only"]
    external_contract = BronzeProductContract.model_validate(external_base)
    snapshot_base = _positive_payloads()["complete_snapshot_managed"]
    snapshot_contract = BronzeProductContract.model_validate(snapshot_base)
    hmac_base = _positive_payloads()["cdc_managed_explicit_tombstone"]
    hmac_contract = BronzeProductContract.model_validate(hmac_base)

    def classify(base_payload: dict, path: str, value: object) -> sd.ChangeClass:
        candidate = BronzeProductContract.model_validate(_apply_patch(base_payload, path, value))
        prior = {
            id(external_base): external_contract,
            id(snapshot_base): snapshot_contract,
            id(hmac_base): hmac_contract,
        }[id(base_payload)]
        return sd.classify_contract_change(prior, candidate)

    minor_cases = (
        # attestation issuer/trust policy -- the row this compiler misgraded
        (external_base, "landing.integration.receipt_trust.policy_ref", "external-receipt-rotated"),
        (external_base, "landing.integration.receipt_trust.allowed_key_ids", ["key-c"]),
        (external_base, "landing.integration.receipt_trust.future_clock_skew_seconds", 90),
        (snapshot_base, "delivery.snapshot.attestation_policy_ref", "attest-rotated"),
        (snapshot_base, "delivery.snapshot.allowed_key_ids", ["key-b"]),
        (snapshot_base, "delivery.snapshot.future_clock_skew_seconds", 90),
        # product display rename
        (external_base, "product.display_name", "Ledger postings (renamed)"),
    )
    for base_payload, path, value in minor_cases:
        assert classify(base_payload, path, value) == sd.ChangeClass.MINOR, (path, value)

    # add nullable published field: `seq` is already a physical column here,
    # not yet projected
    added_projection = copy.deepcopy(external_base)
    added_projection["projection"].append(
        {"source": "seq", "name": "seq", "logical_type": "int64", "nullable": True}
    )
    assert sd.classify_contract_change(
        external_contract, BronzeProductContract.model_validate(added_projection)
    ) == sd.ChangeClass.MINOR

    patch_cases = (
        (external_base, "delivery.retry.max_attempts", 8),
        (external_base, "delivery.retry.backoff", "exponential"),
        (external_base, "delivery.schedule_lateness", {"warn_after_minutes": 45, "error_after_minutes": 120}),
        (external_base, "delivery.maximum_age", {"warn_after_minutes": 2000, "error_after_minutes": 2880}),
        (external_base, "product.owner", "team-finance-data-v2"),
        (external_base, "product.support", "runbook-postings-v2"),
    )
    for base_payload, path, value in patch_cases:
        assert classify(base_payload, path, value) == sd.ChangeClass.PATCH, (path, value)

    major_cases = (
        (external_base, "landing.codec.header", False),
        (external_base, "product.domain", "risk"),
        (external_base, "delivery.mode", "cdc"),
        (external_base, "delivery.progress", {"kind": "opaque_batch"}),
        (external_base, "delivery.timestamps", {"load_field": "loaded_at", "effective_field": "loaded_at"}),
        (external_base, "delivery.record_key", {"fields": ["seq", "txn_id"]}),
        (hmac_base, "delivery.record_key.hmac_key_id", "hmac-key-2"),
        (
            external_base,
            "landing.integration",
            {"kind": "managed"},
        ),
        (external_base, "landing.integration.delivery_id_column", "delivery_ref"),
    )
    for base_payload, path, value in major_cases:
        assert classify(base_payload, path, value) == sd.ChangeClass.MAJOR, (path, value)


def test_attestation_rotation_carries_on_a_minor_bump() -> None:
    """An attestation-issuer/trust-policy rotation is Minor: it plans a carry
    on a minor version bump, and a patch-only bump that understates it is
    rejected. This is the row the compiler previously misgraded as Major,
    which would have forced a rotation through a reset and orphaned the prior
    epoch's published history."""
    base_payload = _positive_payloads()["csv_external_append_only"]
    prior = BronzeProductContract.model_validate(base_payload)

    rotated = copy.deepcopy(base_payload)
    rotated["landing"]["integration"]["receipt_trust"]["allowed_key_ids"] = ["key-c"]
    rotated["product"]["product_version"] = "2.4.0"
    candidate = BronzeProductContract.model_validate(rotated)

    assert sd.classify_contract_change(prior, candidate) == sd.ChangeClass.MINOR
    plan = sd.plan_migration(sd.ContractRegistryState.initial(), prior, candidate)
    assert plan.kind == MigrationKind.CARRY

    understated = copy.deepcopy(rotated)
    understated["product"]["product_version"] = "2.3.2"
    understated_candidate = BronzeProductContract.model_validate(understated)
    try:
        sd.plan_migration(sd.ContractRegistryState.initial(), prior, understated_candidate)
    except sd.ContractValidationError as exc:
        assert "minor bump" in str(exc), str(exc)
    else:
        raise AssertionError("an attestation rotation on a patch-only bump must fail planning")


def test_semver_bump_must_meet_the_classified_change() -> None:
    """A bump that understates the change fails; a bump larger than required
    passes."""
    sd.validate_semver_bump("1.2.3", "1.2.3", sd.ChangeClass.NONE)
    sd.validate_semver_bump("1.2.3", "1.2.4", sd.ChangeClass.PATCH)
    sd.validate_semver_bump("1.2.3", "1.3.0", sd.ChangeClass.MINOR)
    sd.validate_semver_bump("1.2.3", "2.0.0", sd.ChangeClass.MAJOR)
    sd.validate_semver_bump("1.2.3", "2.0.0", sd.ChangeClass.PATCH)

    for prior, candidate, required in (
        ("1.2.3", "1.2.3", sd.ChangeClass.PATCH),
        ("1.2.3", "1.2.2", sd.ChangeClass.PATCH),
        ("1.2.3", "1.2.4", sd.ChangeClass.MINOR),
        ("1.2.3", "1.3.0", sd.ChangeClass.MAJOR),
    ):
        try:
            sd.validate_semver_bump(prior, candidate, required)
        except sd.ContractValidationError:
            pass
        else:
            raise AssertionError(
                f"expected {prior} -> {candidate} to fail for a {required.value} change"
            )


def test_plan_migration_rejects_an_understated_bump() -> None:
    """The plan step refuses to migrate a breaking change carried on a patch
    bump, before any activation is attempted."""
    base_payload = _positive_payloads()["csv_external_append_only"]
    prior = BronzeProductContract.model_validate(base_payload)
    breaking = copy.deepcopy(base_payload)
    breaking["product"]["product_version"] = "2.3.2"
    breaking["landing"]["physical_columns"].append(
        {"name": "note", "logical_type": "utf8_string", "nullable": False}
    )
    candidate = BronzeProductContract.model_validate(breaking)
    try:
        sd.plan_migration(sd.ContractRegistryState.initial(), prior, candidate)
    except sd.ContractValidationError as exc:
        assert "major bump" in str(exc), str(exc)
    else:
        raise AssertionError("a breaking change on a patch bump must fail planning")


# --- migration state machine --------------------------------------------------------

_DEFAULT_ANCESTRY_ROWS = 1000
_DEFAULT_WIRE_BYTES = 1_000_000


def _register_and_activate(
    state: sd.ContractRegistryState,
    prior: BronzeProductContract | None,
    candidate: BronzeProductContract,
    activated_at: str = "2026-08-19T00:00:00.000000Z",
    *,
    max_visibility_ancestry_rows: int = _DEFAULT_ANCESTRY_ROWS,
    max_wire_record_bytes: int = _DEFAULT_WIRE_BYTES,
):
    """Register then fully settle activation. A carry reaches ACTIVE in the one
    ``activate_contract`` call; a reset stages ``PENDING_BASELINE`` there and
    this helper confirms it immediately, for tests that only care about the
    settled result."""
    registered = sd.register_candidate(state, candidate, expected_revision=state.state_revision)
    kind = sd.plan_migration(registered, prior, candidate).kind
    staged, migration = sd.activate_contract(
        registered,
        prior,
        candidate,
        expected_revision=registered.state_revision,
        activated_at=None if kind == MigrationKind.RESET else activated_at,
        max_visibility_ancestry_rows=max_visibility_ancestry_rows,
        max_wire_record_bytes=max_wire_record_bytes,
    )
    if staged.activation_state == ContractActivationState.PENDING_BASELINE:
        return sd.confirm_baseline_activation(
            staged, expected_revision=staged.state_revision, activated_at=activated_at
        )
    return staged, migration


def test_first_activation_is_a_reset_that_opens_a_pending_baseline() -> None:
    """An empty registry has no contract to carry from, so the first activation
    is a reset. It stages a pending baseline with a null ``activated_at`` and
    does not switch the active contract until the confirmation CAS runs."""
    candidate = _contract("csv_external_append_only")
    registered = sd.register_candidate(sd.ContractRegistryState.initial(), candidate, expected_revision=0)
    pending_state, pending_migration = sd.activate_contract(
        registered,
        None,
        candidate,
        expected_revision=registered.state_revision,
        activated_at=None,
        max_visibility_ancestry_rows=_DEFAULT_ANCESTRY_ROWS,
        max_wire_record_bytes=_DEFAULT_WIRE_BYTES,
    )
    assert pending_state.activation_state == ContractActivationState.PENDING_BASELINE
    assert pending_migration.kind == MigrationKind.RESET
    assert pending_migration.from_contract_digest is None
    assert pending_migration.to_contract_digest == sd.compute_contract_digest(candidate)
    assert pending_migration.activated_at is None
    assert (pending_migration.from_visibility_epoch, pending_migration.to_visibility_epoch) == ("0", "1")
    assert pending_state.active_contract_digest is None, "not yet switched over"
    assert pending_migration.migration_id == sd.compute_migration_id(pending_migration)

    confirmed_state, confirmed_migration = sd.confirm_baseline_activation(
        pending_state, expected_revision=pending_state.state_revision,
        activated_at="2026-08-19T00:00:00.000000Z",
    )
    assert confirmed_state.activation_state == ContractActivationState.ACTIVE
    assert confirmed_migration.activated_at == "2026-08-19T00:00:00.000000Z"
    assert confirmed_migration.to_contract_digest == pending_migration.to_contract_digest
    assert confirmed_state.active_contract_digest == sd.compute_contract_digest(candidate)
    assert confirmed_state.active_product_version == "2.3.1"
    assert confirmed_state.candidate_contract_digest is None, "confirmation clears the candidate"
    assert confirmed_state.visibility_ancestry == (1,)
    assert confirmed_migration.migration_id == sd.compute_migration_id(confirmed_migration)


def test_activation_reaching_active_rejects_a_null_activated_at() -> None:
    """A carry reaches ACTIVE inside the one ``activate_contract`` call, so it
    must supply a real ``activated_at``; confirming a baseline with no instant
    is refused the same way."""
    base_payload = _positive_payloads()["csv_external_append_only"]
    prior = BronzeProductContract.model_validate(base_payload)
    state, _first = _register_and_activate(sd.ContractRegistryState.initial(), None, prior)

    additive = copy.deepcopy(base_payload)
    additive["product"]["product_version"] = "2.4.0"
    additive["landing"]["physical_columns"].append(
        {"name": "note", "logical_type": "utf8_string", "nullable": True}
    )
    candidate = BronzeProductContract.model_validate(additive)
    registered = sd.register_candidate(state, candidate, expected_revision=state.state_revision)
    try:
        sd.activate_contract(
            registered,
            prior,
            candidate,
            expected_revision=registered.state_revision,
            activated_at=None,
            max_visibility_ancestry_rows=_DEFAULT_ANCESTRY_ROWS,
            max_wire_record_bytes=_DEFAULT_WIRE_BYTES,
        )
    except ValueError as exc:
        assert "activated_at" in str(exc), str(exc)
    else:
        raise AssertionError("a carry reaching ACTIVE without activated_at must fail")

    fresh_candidate = _contract("csv_external_append_only")
    fresh_registered = sd.register_candidate(
        sd.ContractRegistryState.initial(), fresh_candidate, expected_revision=0
    )
    pending_state, _pending_migration = sd.activate_contract(
        fresh_registered,
        None,
        fresh_candidate,
        expected_revision=fresh_registered.state_revision,
        activated_at=None,
        max_visibility_ancestry_rows=_DEFAULT_ANCESTRY_ROWS,
        max_wire_record_bytes=_DEFAULT_WIRE_BYTES,
    )
    try:
        sd.confirm_baseline_activation(
            pending_state, expected_revision=pending_state.state_revision, activated_at=None
        )
    except ValueError as exc:
        assert "activated_at" in str(exc), str(exc)
    else:
        raise AssertionError("confirming a baseline without activated_at must fail")


def test_carry_opens_the_next_epoch_and_extends_the_ancestry_closure() -> None:
    """A compatible change carries: it still opens the next visibility epoch,
    but the new epoch's ancestry closure extends the epoch it carried from, so
    published history stays one continuous series across the change."""
    base_payload = _positive_payloads()["csv_external_append_only"]
    prior = BronzeProductContract.model_validate(base_payload)
    state, _first = _register_and_activate(sd.ContractRegistryState.initial(), None, prior)

    additive = copy.deepcopy(base_payload)
    additive["product"]["product_version"] = "2.4.0"
    additive["landing"]["physical_columns"].append(
        {"name": "note", "logical_type": "utf8_string", "nullable": True}
    )
    candidate = BronzeProductContract.model_validate(additive)
    carried, migration = _register_and_activate(state, prior, candidate)

    assert migration.kind == MigrationKind.CARRY
    assert (migration.from_visibility_epoch, migration.to_visibility_epoch) == ("1", "2")
    assert migration.from_contract_digest == sd.compute_contract_digest(prior)
    assert carried.visibility_epoch == 2
    assert carried.visibility_ancestry == (1, 2), (
        "a carry opens the next epoch, whose ancestry closure extends the epoch it carried from"
    )
    assert carried.active_product_version == "2.4.0"


def test_carry_ancestry_capacity_is_enforced_before_activation() -> None:
    """The extended ancestry closure must fit both the row and byte ceilings
    before a carry activates; a tight row ceiling fails closed with
    ``capacity_exceeded`` and changes no state."""
    base_payload = _positive_payloads()["csv_external_append_only"]
    prior = BronzeProductContract.model_validate(base_payload)
    state, _first = _register_and_activate(sd.ContractRegistryState.initial(), None, prior)
    assert state.visibility_ancestry == (1,)

    additive = copy.deepcopy(base_payload)
    additive["product"]["product_version"] = "2.4.0"
    additive["landing"]["physical_columns"].append(
        {"name": "note", "logical_type": "utf8_string", "nullable": True}
    )
    candidate = BronzeProductContract.model_validate(additive)
    registered = sd.register_candidate(state, candidate, expected_revision=state.state_revision)
    try:
        sd.activate_contract(
            registered,
            prior,
            candidate,
            expected_revision=registered.state_revision,
            activated_at="2026-08-19T00:00:00.000000Z",
            max_visibility_ancestry_rows=1,
            max_wire_record_bytes=_DEFAULT_WIRE_BYTES,
        )
    except sd.CapacityExceededError as exc:
        assert "capacity_exceeded" in str(exc), str(exc)
    else:
        raise AssertionError("a carry whose extended ancestry closure exceeds the row ceiling must fail")
    # capacity_exceeded must fail before any state change: replaying the exact
    # same call against the exact same `registered` state and `expected_revision`,
    # only with the ceiling raised, must still activate cleanly. If the failed
    # attempt above had persisted any state change (bumped a revision, recorded
    # a Migration), this CAS would now see a stale expected_revision and refuse.
    activated, migration = sd.activate_contract(
        registered,
        prior,
        candidate,
        expected_revision=registered.state_revision,
        activated_at="2026-08-19T00:00:00.000000Z",
        max_visibility_ancestry_rows=_DEFAULT_ANCESTRY_ROWS,
        max_wire_record_bytes=_DEFAULT_WIRE_BYTES,
    )
    assert migration.kind == MigrationKind.CARRY
    assert activated.visibility_epoch == 2


def test_activation_refuses_a_stale_prior_contract() -> None:
    """A caller must pass the contract that is actually active, not a
    remembered older one: a stale prior could let a breaking change reach
    activation as if it were still compatible with what is truly active."""
    base_payload = _positive_payloads()["csv_external_append_only"]
    v1 = BronzeProductContract.model_validate(base_payload)
    state, _first = _register_and_activate(sd.ContractRegistryState.initial(), None, v1)

    additive = copy.deepcopy(base_payload)
    additive["product"]["product_version"] = "2.4.0"
    additive["landing"]["physical_columns"].append(
        {"name": "note", "logical_type": "utf8_string", "nullable": True}
    )
    v2 = BronzeProductContract.model_validate(additive)
    state, _second = _register_and_activate(state, v1, v2)  # active is now v2, not v1

    breaking = copy.deepcopy(base_payload)
    breaking["product"]["product_version"] = "3.0.0"
    breaking["landing"]["physical_columns"].append(
        {"name": "note", "logical_type": "utf8_string", "nullable": False}
    )
    v3 = BronzeProductContract.model_validate(breaking)
    registered = sd.register_candidate(state, v3, expected_revision=state.state_revision)
    try:
        sd.activate_contract(
            registered,
            v1,  # stale: v1 is no longer active
            v3,
            expected_revision=registered.state_revision,
            activated_at=None,
            max_visibility_ancestry_rows=_DEFAULT_ANCESTRY_ROWS,
            max_wire_record_bytes=_DEFAULT_WIRE_BYTES,
        )
    except sd.MigrationConflictError as exc:
        assert "prior_contract" in str(exc), str(exc)
    else:
        raise AssertionError("a stale prior_contract must not reach activation")


def test_reset_opens_the_next_epoch_rooted_alone() -> None:
    """A breaking change resets: the new epoch's ancestry closure discards the
    epoch it reset from entirely rather than extending it."""
    base_payload = _positive_payloads()["csv_external_append_only"]
    prior = BronzeProductContract.model_validate(base_payload)
    state, _first = _register_and_activate(sd.ContractRegistryState.initial(), None, prior)

    breaking = copy.deepcopy(base_payload)
    breaking["product"]["product_version"] = "3.0.0"
    breaking["landing"]["physical_columns"].append(
        {"name": "note", "logical_type": "utf8_string", "nullable": False}
    )
    candidate = BronzeProductContract.model_validate(breaking)
    reset, migration = _register_and_activate(state, prior, candidate)

    assert migration.kind == MigrationKind.RESET
    assert (migration.from_visibility_epoch, migration.to_visibility_epoch) == ("1", "2")
    assert reset.visibility_epoch == 2
    assert reset.visibility_ancestry == (2,), "a reset roots a new ancestry at the new epoch alone"
    assert 1 not in reset.visibility_ancestry


def test_activation_requires_the_candidate_to_be_registered_first() -> None:
    """A contract reaches production through registration then activation. An
    unregistered contract cannot be activated."""
    candidate = _contract("append_only_managed_opaque_batch")
    try:
        sd.activate_contract(
            sd.ContractRegistryState.initial(),
            None,
            candidate,
            expected_revision=0,
            activated_at=None,
            max_visibility_ancestry_rows=_DEFAULT_ANCESTRY_ROWS,
            max_wire_record_bytes=_DEFAULT_WIRE_BYTES,
        )
    except sd.MigrationConflictError as exc:
        assert "registered candidate" in str(exc), str(exc)
    else:
        raise AssertionError("activating an unregistered candidate must fail")


def test_in_flight_race_loses_on_a_stale_revision() -> None:
    """Two activators read the same registry revision; the second loses the
    compare-and-swap rather than overwriting the winner."""
    base_payload = _positive_payloads()["csv_external_append_only"]
    prior = BronzeProductContract.model_validate(base_payload)
    state, _first = _register_and_activate(sd.ContractRegistryState.initial(), None, prior)
    read_revision = state.state_revision

    additive = copy.deepcopy(base_payload)
    additive["product"]["product_version"] = "2.4.0"
    additive["landing"]["physical_columns"].append(
        {"name": "note", "logical_type": "utf8_string", "nullable": True}
    )
    winner_candidate = BronzeProductContract.model_validate(additive)
    won, _migration = _register_and_activate(state, prior, winner_candidate)
    assert won.state_revision > read_revision

    other = copy.deepcopy(base_payload)
    other["product"]["product_version"] = "2.5.0"
    loser_candidate = BronzeProductContract.model_validate(other)
    try:
        sd.register_candidate(won, loser_candidate, expected_revision=read_revision)
    except sd.MigrationConflictError as exc:
        assert "concurrent activation" in str(exc), str(exc)
    else:
        raise AssertionError("a stale expected_revision must lose the race")


def test_a_second_candidate_conflicts_while_one_is_in_flight() -> None:
    """One candidate at a time: registering a different contract while one is
    pending conflicts rather than silently replacing it, and re-registering the
    same candidate is idempotent in effect."""
    first = _contract("append_only_managed_opaque_batch")
    second = _contract("csv_external_append_only")
    state = sd.ContractRegistryState.initial()
    registered = sd.register_candidate(state, first, expected_revision=state.state_revision)

    again = sd.register_candidate(registered, first, expected_revision=registered.state_revision)
    assert again.candidate_contract_digest == registered.candidate_contract_digest

    try:
        sd.register_candidate(again, second, expected_revision=again.state_revision)
    except sd.MigrationConflictError as exc:
        assert "already in flight" in str(exc), str(exc)
    else:
        raise AssertionError("a second in-flight candidate must conflict")


def test_carry_refuses_an_empty_registry() -> None:
    """A compatible change classifies as a carry, but there is nothing to carry
    from while the registry holds no active contract: a caller who still
    supplies that would-be prior against an empty registry is holding a stale
    prior, and the guard names it rather than producing an ancestry with no
    root."""
    base_payload = _positive_payloads()["csv_external_append_only"]
    prior = BronzeProductContract.model_validate(base_payload)
    revised = _apply_patch(base_payload, "product.description", "A revised description.")
    revised = _apply_patch(revised, "product.product_version", "2.3.2")
    candidate = BronzeProductContract.model_validate(revised)
    assert sd.classify_contract_change(prior, candidate) == sd.ChangeClass.PATCH
    assert sd.required_migration_kind(sd.ChangeClass.PATCH) == MigrationKind.CARRY

    empty = sd.ContractRegistryState.initial()
    registered = sd.register_candidate(empty, candidate, expected_revision=empty.state_revision)
    assert registered.active_contract_digest is None, "nothing is active to carry from"
    try:
        sd.activate_contract(
            registered,
            prior,
            candidate,
            expected_revision=registered.state_revision,
            activated_at="2026-08-19T00:00:00.000000Z",
            max_visibility_ancestry_rows=_DEFAULT_ANCESTRY_ROWS,
            max_wire_record_bytes=_DEFAULT_WIRE_BYTES,
        )
    except sd.MigrationConflictError as exc:
        assert "prior_contract" in str(exc), str(exc)
    else:
        raise AssertionError("carrying from an empty registry must fail")


def test_migration_id_reaches_every_field_outside_its_own_exclusion() -> None:
    """A migration's identity covers its kind, both contract digests, the
    activation instant and both epochs."""
    base = {
        "kind": "carry",
        "from_contract_digest": ZERO_DIGEST,
        "to_contract_digest": ONE_DIGEST,
        "activated_at": "2026-08-19T00:00:00.000000Z",
        "from_visibility_epoch": "1",
        "to_visibility_epoch": "1",
    }
    identity = sd.compute_migration_id(base)
    for key, value in (
        ("kind", "reset"),
        ("to_contract_digest", "2" * 64),
        ("activated_at", "2026-08-20T00:00:00.000000Z"),
        ("to_visibility_epoch", "2"),
    ):
        altered = {**base, key: value}
        assert sd.compute_migration_id(altered) != identity, f"{key} must reach the migration id"


# --- schedule engine ----------------------------------------------------------------

def _utc(*args: int) -> datetime:
    return datetime(*args, tzinfo=timezone.utc)


def test_interval_boundaries_resolve_from_the_anchor() -> None:
    """An interval schedule steps from its anchor. Before the anchor there is no
    current boundary at all, and the next boundary is the anchor itself."""
    schedule = IntervalSchedule.model_validate(
        {"kind": "interval", "every_minutes": 15, "anchor_at": "2026-01-01T00:00:00.000000Z"}
    )
    assert sd.current_boundary_at(schedule, _utc(2025, 12, 31, 23, 59)) is None
    assert sd.next_boundary_after(schedule, _utc(2025, 12, 31, 23, 59)) == _utc(2026, 1, 1, 0, 0)
    assert sd.current_boundary_at(schedule, _utc(2026, 1, 1, 0, 0)) == _utc(2026, 1, 1, 0, 0)
    assert sd.current_boundary_at(schedule, _utc(2026, 1, 1, 0, 37)) == _utc(2026, 1, 1, 0, 30)
    assert sd.next_boundary_after(schedule, _utc(2026, 1, 1, 0, 30)) == _utc(2026, 1, 1, 0, 45)
    assert sd.is_eligible_boundary(schedule, _utc(2026, 1, 1, 0, 30), _utc(2026, 1, 1, 0, 37))
    assert not sd.is_eligible_boundary(schedule, _utc(2026, 1, 1, 0, 31), _utc(2026, 1, 1, 0, 37)), (
        "an instant that is not a schedule occurrence is not an eligible boundary"
    )
    assert not sd.is_eligible_boundary(schedule, _utc(2026, 1, 1, 0, 45), _utc(2026, 1, 1, 0, 37)), (
        "a future boundary is not yet eligible"
    )


def test_cron_grammar_admits_lists_ranges_and_steps_and_rejects_the_rest() -> None:
    """The v1 grammar is deliberately small: five fields, integers, ascending
    ranges, comma lists and positive steps."""
    parsed = sd.parse_cron_expression("0,30 */6 * * 1-5")
    assert parsed.minutes == frozenset({0, 30})
    assert parsed.hours == frozenset({0, 6, 12, 18})
    assert parsed.days_of_week == frozenset({1, 2, 3, 4, 5})
    assert parsed.day_of_week_restricted and not parsed.day_of_month_restricted

    for expression in ("30 2 * *", "30 2 * * MON", "30 25 * * *", "30 2 * * 5-1", "30 2/5 * * *"):
        try:
            sd.parse_cron_expression(expression)
        except ValueError:
            pass
        else:
            raise AssertionError(f"expected {expression!r} to be rejected by the v1 grammar")


def test_cron_day_of_month_and_day_of_week_take_their_union() -> None:
    """When both day fields are restricted, cron schedules a day matching either
    one. June 2026 starts on a Monday, so `0 0 1 * 0` fires on the 1st and on
    every Sunday, and on no other day."""
    schedule = CronSchedule.model_validate(
        {
            "kind": "cron", "expression": "0 0 1 * 0", "timezone": "UTC",
            "starts_at": "2026-06-01T00:00:00.000000Z", "timezone_data_version": "2026.2",
        }
    )
    occurrences = []
    cursor = _utc(2026, 5, 31, 23, 0)
    for _ in range(5):
        cursor = sd.next_boundary_after(schedule, cursor)
        occurrences.append(cursor)
    assert occurrences == [
        _utc(2026, 6, 1, 0, 0),   # the first of the month
        _utc(2026, 6, 7, 0, 0),   # a Sunday
        _utc(2026, 6, 14, 0, 0),
        _utc(2026, 6, 21, 0, 0),
        _utc(2026, 6, 28, 0, 0),
    ], occurrences
    assert not sd.is_eligible_boundary(schedule, _utc(2026, 6, 9, 0, 0), _utc(2026, 6, 30, 0, 0)), (
        "a Tuesday that is not the first matches neither day field"
    )


def test_cron_boundaries_skip_a_spring_forward_gap() -> None:
    """A local time inside the spring-forward gap never occurs, so the schedule
    produces no occurrence that day rather than inventing one."""
    schedule = CronSchedule.model_validate(
        {
            "kind": "cron", "expression": "30 1 * * *", "timezone": "Europe/London",
            "starts_at": "2026-01-01T00:00:00.000000Z", "timezone_data_version": "2026.2",
        }
    )
    # Europe/London moves to BST at 01:00 on 2026-03-29, so 01:30 local never occurs.
    following = sd.next_boundary_after(schedule, _utc(2026, 3, 28, 12, 0))
    assert following == _utc(2026, 3, 28, 1, 30) or following > _utc(2026, 3, 28, 12, 0)
    assert sd.next_boundary_after(schedule, _utc(2026, 3, 29, 0, 0)) == _utc(2026, 3, 30, 0, 30), (
        "the skipped day yields no occurrence; the next one is the following day at 01:30 BST"
    )
    for candidate in (_utc(2026, 3, 29, 1, 30), _utc(2026, 3, 29, 0, 30)):
        assert not sd.is_eligible_boundary(schedule, candidate, _utc(2026, 3, 30, 0, 0)), (
            f"{candidate} is not an occurrence of a schedule whose local time was skipped"
        )


def test_cron_boundaries_resolve_both_sides_of_a_fall_back_fold() -> None:
    """A repeated local time occurs twice, so the schedule produces both distinct
    UTC instants rather than collapsing them into one."""
    schedule = CronSchedule.model_validate(
        {
            "kind": "cron", "expression": "30 1 * * *", "timezone": "Europe/London",
            "starts_at": "2026-01-01T00:00:00.000000Z", "timezone_data_version": "2026.2",
        }
    )
    first = sd.next_boundary_after(schedule, _utc(2026, 10, 24, 12, 0))
    second = sd.next_boundary_after(schedule, first)
    assert first == _utc(2026, 10, 25, 0, 30), first
    assert second == _utc(2026, 10, 25, 1, 30), second
    assert sd.is_eligible_boundary(schedule, first, _utc(2026, 10, 25, 6, 0))
    assert sd.is_eligible_boundary(schedule, second, _utc(2026, 10, 25, 6, 0))
    assert sd.current_boundary_at(schedule, _utc(2026, 10, 25, 6, 0)) == second, (
        "the current boundary is the greatest eligible occurrence"
    )


def test_cron_respects_its_lower_bound() -> None:
    """A schedule produces nothing before its declared start."""
    schedule = CronSchedule.model_validate(
        {
            "kind": "cron", "expression": "0 12 * * *", "timezone": "UTC",
            "starts_at": "2026-06-01T00:00:00.000000Z", "timezone_data_version": "2026.2",
        }
    )
    assert sd.current_boundary_at(schedule, _utc(2026, 5, 31, 23, 0)) is None
    assert sd.next_boundary_after(schedule, _utc(2026, 5, 31, 23, 0)) == _utc(2026, 6, 1, 12, 0)
    assert sd.current_boundary_at(schedule, _utc(2026, 6, 1, 12, 0)) == _utc(2026, 6, 1, 12, 0)


def test_a_migrated_schedule_evaluates_nothing_retroactively() -> None:
    """The first boundary of a newly activated schedule lands strictly after the
    activation instant, so a migration never back-fills occurrences."""
    schedule = IntervalSchedule.model_validate(
        {"kind": "interval", "every_minutes": 60, "anchor_at": "2026-01-01T00:00:00.000000Z"}
    )
    activated_at = _utc(2026, 8, 19, 10, 30)
    first = sd.next_boundary_after(schedule, activated_at)
    assert first > activated_at, first
    assert first == _utc(2026, 8, 19, 11, 0), first


def test_timezone_data_version_is_pinned_to_the_installed_release() -> None:
    """Cron schedules resolve local wall-clock times against the zone database,
    so a contract declaring a different release than the one installed fails to
    compile."""
    assert sd.installed_timezone_data_version() == sd.TIMEZONE_DATA_VERSION, (
        f"expected tzdata {sd.TIMEZONE_DATA_VERSION}, found "
        f"{sd.installed_timezone_data_version()}"
    )
    contract = _contract("csv_external_append_only")
    assert contract.delivery.schedule.timezone_data_version == sd.TIMEZONE_DATA_VERSION


# --- typed declaration loader -------------------------------------------------------

BRONZE_DOMAIN = {
    "bronze": {
        "domain": {"name": "operations", "display_name": "Operations"},
        "products": [{"source": "acme", "table": "orders"}],
    }
}

PRODUCT_BLOCK = {
    "product_version": "1.0.0",
    "display_name": "Orders",
    "description": "Synthetic source-aligned orders.",
    "owner": "team-data-platform",
    "support": "runbook-orders",
    "classification": "synthetic",
    "access_policy_ref": "local-process-user",
    "retention_policy_ref": "local-ephemeral",
}


def _production_table() -> dict:
    payload = _positive_payloads()["append_only_managed_opaque_batch"]
    return {
        "landing": payload["landing"],
        "product": copy.deepcopy(PRODUCT_BLOCK),
        "delivery": payload["delivery"],
        "projection": [
            {"source": "order_id", "name": "order_id", "logical_type": "utf8_string", "nullable": False},
            {"source": "loaded_at", "name": "loaded_at", "logical_type": "utc_instant", "nullable": False},
        ],
    }


def _estate(root: Path, *, tables: dict, namespace: str | None = "com.example.ergasterion",
            domain: dict | None = None) -> EstateContext:
    (root / "declarations").mkdir(parents=True, exist_ok=True)
    (root / "domains").mkdir(parents=True, exist_ok=True)
    if namespace is not None:
        (root / "estate.yml").write_text(
            yaml.safe_dump({"estate": {"namespace": namespace}}, sort_keys=False), encoding="utf-8"
        )
    (root / "domains" / "fixture.yml").write_text(
        yaml.safe_dump(BRONZE_DOMAIN if domain is None else domain, sort_keys=False), encoding="utf-8"
    )
    (root / "declarations" / "acme.yml").write_text(
        yaml.safe_dump({"source": {"name": "acme"}, "tables": tables}, sort_keys=False),
        encoding="utf-8",
    )
    return EstateContext.resolve(estate_root=root)


def test_production_declaration_compiles_to_a_digested_contract() -> None:
    """A production table resolves to a validated contract with its domain taken
    from the bronze membership block, its lineage derived, and all four digests
    computed."""
    with tempfile.TemporaryDirectory() as tmp:
        ctx = _estate(Path(tmp), tables={"orders": _production_table()})
        typed = sd.load_typed_declarations(ctx)

        assert typed.estate_namespace == "com.example.ergasterion"
        table = typed.tables[("acme", "orders")]
        assert table.kind == "production"
        assert table.domain == "operations", "domain comes from the bronze membership block"
        assert table.contract is not None
        assert table.contract.logical_identity.estate_namespace == "com.example.ergasterion"
        assert table.contract.product.domain == "operations"
        assert table.contract.interfaces == sd.derive_interfaces("acme", "orders")
        assert table.contract.interfaces.published == "bronze-acme-orders-published"
        for digest in (
            table.contract_digest, table.source_schema_digest,
            table.published_schema_digest, table.ruleset_digest,
        ):
            assert digest is not None and len(digest) == 64, digest
        assert table.contract_digest == sd.compute_contract_digest(table.contract)
        assert typed.production_contracts() == [table.contract]
        assert typed.drafts() == []


def test_production_requires_every_product_fact() -> None:
    """Identity, version, ownership, support, access, classification and
    retention are each mandatory: omitting any one fails, naming the field."""
    required = (
        "product_version", "display_name", "description", "owner",
        "support", "classification", "access_policy_ref", "retention_policy_ref",
    )
    for omitted in required:
        with tempfile.TemporaryDirectory() as tmp:
            table = _production_table()
            del table["product"][omitted]
            ctx = _estate(Path(tmp), tables={"orders": table})
            try:
                sd.load_typed_declarations(ctx)
            except ValueError as exc:
                assert omitted in str(exc), f"expected {omitted!r} named in: {exc}"
            else:
                raise AssertionError(f"a missing {omitted} must fail compilation")

    with tempfile.TemporaryDirectory() as tmp:
        table = _production_table()
        table["product"]["domain"] = "operations"
        ctx = _estate(Path(tmp), tables={"orders": table})
        try:
            sd.load_typed_declarations(ctx)
        except ValueError as exc:
            assert "domain" in str(exc), str(exc)
        else:
            raise AssertionError("domain is resolved from membership and cannot be authored inline")


def test_production_requires_the_estate_namespace_and_a_domain_membership() -> None:
    """A globally qualified identity needs the estate namespace, and generation
    needs exactly one explicit domain membership."""
    with tempfile.TemporaryDirectory() as tmp:
        ctx = _estate(Path(tmp), tables={"orders": _production_table()}, namespace=None)
        try:
            sd.load_typed_declarations(ctx)
        except ValueError as exc:
            assert "estate.namespace" in str(exc), str(exc)
        else:
            raise AssertionError("production delivery without estate.yml must fail")

    with tempfile.TemporaryDirectory() as tmp:
        ctx = _estate(
            Path(tmp), tables={"orders": _production_table()},
            domain={"bronze": {"domain": {"name": "operations", "display_name": "Operations"},
                               "products": [{"source": "acme", "table": "other"}]}},
        )
        try:
            sd.load_typed_declarations(ctx)
        except ValueError as exc:
            assert "domain membership" in str(exc), str(exc)
        else:
            raise AssertionError("production delivery without a membership must fail")


def test_a_product_claimed_by_two_domains_fails_naming_both() -> None:
    """One Bronze product belongs to one domain."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        ctx = _estate(root, tables={"orders": _production_table()})
        (root / "domains" / "second.yml").write_text(
            yaml.safe_dump(
                {"bronze": {"domain": {"name": "finance", "display_name": "Finance"},
                            "products": [{"source": "acme", "table": "orders"}]}},
                sort_keys=False,
            ),
            encoding="utf-8",
        )
        try:
            sd.load_typed_declarations(ctx)
        except ValueError as exc:
            assert "already a member of domain" in str(exc), str(exc)
            assert "fixture.yml" in str(exc), str(exc)
        else:
            raise AssertionError("a product claimed twice must fail")


def test_a_source_landing_needs_an_explicit_draft_or_production_delivery() -> None:
    """A source-backed table states its delivery intent explicitly: there is no
    implicit default that silently skips the contract."""
    payload = _positive_payloads()["append_only_managed_opaque_batch"]
    with tempfile.TemporaryDirectory() as tmp:
        ctx = _estate(Path(tmp), tables={"orders": {"landing": payload["landing"]}})
        try:
            sd.load_typed_declarations(ctx)
        except ValueError as exc:
            assert "explicit table delivery block" in str(exc), str(exc)
        else:
            raise AssertionError("a source landing with no delivery block must fail")

    with tempfile.TemporaryDirectory() as tmp:
        ctx = _estate(
            Path(tmp),
            tables={"orders": {"landing": payload["landing"], "delivery": {"kind": "sketch"}}},
        )
        try:
            sd.load_typed_declarations(ctx)
        except ValueError as exc:
            assert "expected 'draft' or 'production'" in str(exc), str(exc)
        else:
            raise AssertionError("an unknown delivery kind must fail")


def test_a_draft_delivery_resolves_to_an_explicit_placeholder() -> None:
    """A draft is a stated position, not a missing one: it compiles to a
    placeholder with a reason and no contract."""
    payload = _positive_payloads()["append_only_managed_opaque_batch"]
    with tempfile.TemporaryDirectory() as tmp:
        ctx = _estate(
            Path(tmp),
            tables={
                "orders": {
                    "landing": {
                        "kind": "source",
                        "source_name": payload["landing"]["source_name"],
                        "identifier": payload["landing"]["identifier"],
                    },
                    "delivery": {"kind": "draft", "reason": "delivery_contract_required"},
                }
            },
        )
        typed = sd.load_typed_declarations(ctx)
        table = typed.tables[("acme", "orders")]
        assert table.kind == "draft"
        assert table.draft_reason == "delivery_contract_required"
        assert table.contract is None and table.contract_digest is None
        assert typed.production_contracts() == []
        assert typed.drafts() == [table]


def test_seed_tables_contribute_no_bronze_contract() -> None:
    """Seed fixture meaning stays owned by the landing discriminator: the typed
    loader passes seed tables by without demanding a delivery block."""
    with tempfile.TemporaryDirectory() as tmp:
        ctx = _estate(Path(tmp), tables={"fixtures": {"landing": {"kind": "seed"}}, "implicit": {}})
        typed = sd.load_typed_declarations(ctx)
        assert typed.tables == {}, typed.tables


def test_source_delivery_defaults_overlay_and_a_mode_switch_clears_mode_specific_keys() -> None:
    """Source-level defaults overlay onto a table's delivery block, and changing
    the mode drops every inherited mode-specific object rather than mixing two
    modes' fields."""
    payload = _positive_payloads()["append_only_managed_opaque_batch"]
    cdc_payload = _positive_payloads()["cdc_managed_explicit_tombstone"]

    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        _estate(root, tables={})
        defaults = copy.deepcopy(payload["delivery"])
        table = _production_table()
        overridden = {"kind": "production", "retry": {
            "max_attempts": 9, "backoff": "fixed", "base_seconds": 2, "cap_seconds": 30
        }}
        (root / "declarations" / "acme.yml").write_text(
            yaml.safe_dump(
                {
                    "source": {"name": "acme", "delivery": defaults},
                    "tables": {"orders": {**table, "delivery": overridden}},
                },
                sort_keys=False,
            ),
            encoding="utf-8",
        )
        typed = sd.load_typed_declarations(EstateContext.resolve(estate_root=root))
        delivery = typed.tables[("acme", "orders")].contract.delivery
        assert delivery.mode.value == "append_only", "the default mode is inherited"
        assert delivery.retry.max_attempts == 9, "the table override replaces the default whole"
        assert delivery.record_key.fields == ("order_id",), "unset keys keep the default"

    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        _estate(root, tables={})
        defaults = copy.deepcopy(cdc_payload["delivery"])
        table = _production_table()
        table["landing"] = copy.deepcopy(cdc_payload["landing"])
        table["projection"] = [
            {"source": "account_id", "name": "account_id", "logical_type": "utf8_string", "nullable": False},
            {"source": "loaded_at", "name": "loaded_at", "logical_type": "utc_instant", "nullable": False},
        ]
        switched = {
            "kind": "production", "mode": "append_only",
            "progress": {"kind": "opaque_batch"},
            "delete_strategy": "none",
            "timestamps": {"load_field": "loaded_at"},
            "record_key": {"fields": ["account_id"]},
        }
        (root / "declarations" / "acme.yml").write_text(
            yaml.safe_dump(
                {
                    "source": {"name": "acme", "delivery": defaults},
                    "tables": {"orders": {**table, "delivery": switched}},
                },
                sort_keys=False,
            ),
            encoding="utf-8",
        )
        typed = sd.load_typed_declarations(EstateContext.resolve(estate_root=root))
        delivery = typed.tables[("acme", "orders")].contract.delivery
        assert delivery.mode.value == "append_only"
        assert delivery.tombstone is None, "the mode switch dropped the inherited tombstone block"
        assert delivery.delete_strategy.value == "none"


def test_the_loader_rejects_a_projection_column_it_cannot_type() -> None:
    """A production projection column names the physical column it publishes and
    the type it publishes it as."""
    with tempfile.TemporaryDirectory() as tmp:
        table = _production_table()
        table["projection"] = [{"name": "order_id", "expression": "order_id"}]
        ctx = _estate(Path(tmp), tables={"orders": table})
        try:
            sd.load_typed_declarations(ctx)
        except ValueError as exc:
            assert "production projection column needs" in str(exc), str(exc)
        else:
            raise AssertionError("an untyped projection column must fail production compilation")

    with tempfile.TemporaryDirectory() as tmp:
        table = _production_table()
        table["projection"][0]["logial_type"] = "utf8_string"
        ctx = _estate(Path(tmp), tables={"orders": table})
        try:
            sd.load_typed_declarations(ctx)
        except ValueError as exc:
            assert "logial_type" in str(exc), str(exc)
        else:
            raise AssertionError("a misspelled projection field must fail")


def test_estate_namespace_absence_is_valid_and_a_malformed_file_is_not() -> None:
    """A seed-only legacy estate may carry no estate.yml at all; a present file
    with a bad namespace or an unknown key fails."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        (root / "declarations").mkdir()
        (root / "domains").mkdir()
        assert sd.load_estate_namespace(EstateContext.resolve(estate_root=root)) is None

        (root / "estate.yml").write_text("estate:\n  namespace: NOT_A_NAMESPACE\n", encoding="utf-8")
        try:
            sd.load_estate_namespace(EstateContext.resolve(estate_root=root))
        except ValueError as exc:
            assert "EstateNamespace" in str(exc) or "namespace" in str(exc), str(exc)
        else:
            raise AssertionError("a malformed namespace must fail")

        (root / "estate.yml").write_text(
            "estate:\n  namespace: com.example.ergasterion\n  extra: no\n", encoding="utf-8"
        )
        try:
            sd.load_estate_namespace(EstateContext.resolve(estate_root=root))
        except ValueError as exc:
            assert "extra" in str(exc), str(exc)
        else:
            raise AssertionError("an unknown estate.yml field must fail")


def test_the_committed_estate_carries_its_namespace_and_no_bronze_products_yet() -> None:
    """The committed estate is seed-backed throughout, so the typed loader reads
    its namespace and compiles nothing -- which is why the committed generated
    output is unaffected by this compiler existing."""
    ctx = EstateContext.resolve(estate_root=REPO_ROOT)
    typed = sd.load_typed_declarations(ctx)
    assert typed.estate_namespace == "io.antikas.ergasterion", typed.estate_namespace
    assert typed.tables == {}, f"expected no source-backed tables yet, got {sorted(typed.tables)}"


# --- legacy preservation ------------------------------------------------------------

def test_the_legacy_loader_reads_a_bronze_declaration_unchanged() -> None:
    """One authored declaration serves both loaders. emit.load_declarations()
    keeps its exact legacy behaviour: it projects a typed projection column's
    `source` onto the `expression` its templates read, and carries the product
    and delivery blocks through untouched as ordinary dict entries."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        ctx = _estate(root, tables={"orders": _production_table()})
        legacy_ctx = emit.EstateContext.resolve(
            estate_root=emit.REPO_ROOT, declarations_dir=root / "declarations"
        )
        declarations = emit.load_declarations(ctx=legacy_ctx)
        table = declarations[0]["tables"]["orders"]

        assert table["staging_model"] == "stg_acme_orders", "the legacy defaults still apply"
        assert [column["expression"] for column in table["projection"]] == ["order_id", "loaded_at"], (
            "a typed projection column gains its legacy expression from `source`"
        )
        assert table["product"]["owner"] == "team-data-platform", "product rides through untouched"
        assert table["delivery"]["kind"] == "production", "delivery rides through untouched"
        assert table["landing"]["kind"] == "source"

        typed = sd.load_typed_declarations(ctx)
        assert typed.tables[("acme", "orders")].contract is not None, (
            "both loaders read the same file independently"
        )


def test_the_legacy_loader_still_rejects_a_projection_column_with_neither_key() -> None:
    """A projection column declaring neither `expression` nor `source` fails with
    the message it has always failed with."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        (root / "declarations").mkdir()
        (root / "declarations" / "acme.yml").write_text(
            yaml.safe_dump(
                {"source": {"name": "acme"}, "tables": {"orders": {"projection": [{"name": "x"}]}}},
                sort_keys=False,
            ),
            encoding="utf-8",
        )
        legacy_ctx = emit.EstateContext.resolve(
            estate_root=emit.REPO_ROOT, declarations_dir=root / "declarations"
        )
        try:
            emit.load_declarations(ctx=legacy_ctx)
        except ValueError as exc:
            assert "projection columns need name/expression" in str(exc), str(exc)
        else:
            raise AssertionError("a projection column with neither key must fail")


def test_the_committed_estate_generates_byte_identical_output() -> None:
    """The compiler adds a parallel typed loader and changes no generated byte:
    a --check run over the committed estate reports no file changed and no
    orphan."""
    old_argv = sys.argv
    out = io.StringIO()
    try:
        sys.argv = ["emit.py", "--check"]
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(io.StringIO()):
            exit_code = emit.main()
    finally:
        sys.argv = old_argv
    printed = out.getvalue()
    assert exit_code == 0, f"expected a clean --check over the committed estate, got {exit_code}:\n{printed}"
    change_lines = [line for line in printed.splitlines() if line.startswith("would change ")]
    assert change_lines, f"expected a 'would change' summary line, got:\n{printed}"
    assert change_lines[-1].startswith("would change 0 of "), (
        f"expected zero changed files, got: {change_lines[-1]}"
    )
    assert "ORPHANS=0" in printed, f"expected zero orphans, got:\n{printed}"


def test_no_template_owns_semantic_validation() -> None:
    """Contract semantics live in this compiler alone. Pipeline-rendering
    templates do not read Bronze contract facts, and no template raises a
    validation error of its own. The declaration seeder is an authoring surface:
    it necessarily writes a safe draft delivery block and names the fields a
    person must complete, but it does not decide whether that contract is valid."""
    contract_tokens = (
        "delivery", "contract_digest", "delete_strategy", "physical_columns",
        "logical_type", "product_version", "content_encodings", "fingerprint_scope",
        "publication_mode", "estate_namespace", "schedule_lateness",
    )
    raising_tokens = ("{{ raise", "ValueError", "{% do raise")
    templates = sorted((emit.REPO_ROOT / "ergasterion" / "templates").glob("*.j2"))
    assert templates, "expected the packaged Jinja templates to exist"
    for template in templates:
        text = template.read_text(encoding="utf-8")
        if template.name != "declaration_seed.yml.j2":
            for token in contract_tokens:
                assert token not in text, (
                    f"{template.name} references the Bronze contract fact {token!r}; contract "
                    "semantics belong to ergasterion.source_delivery"
                )
        for token in raising_tokens:
            assert token not in text, f"{template.name} raises {token!r}; templates do not validate"


TESTS = [
    test_positive_vectors_validate_and_digest_deterministically,
    test_positive_vectors_cover_every_mode_codec_integration_and_schedule,
    test_negative_vectors_fail_their_named_validator,
    test_validation_error_reports_every_violation_at_once,
    test_canonical_document_omits_absent_optionals_and_reparses,
    test_declared_set_reordering_leaves_every_digest_equal,
    test_canonicalisation_honours_every_ordering_hint_the_contract_carries,
    test_declared_set_rejects_a_repeated_value,
    test_record_key_field_order_is_load_bearing,
    test_schema_digests_isolate_the_facts_they_name,
    test_rule_id_ignores_severity_and_ruleset_ignores_authored_order,
    test_derived_digest_exclusions_match_the_frozen_idl,
    test_delivery_claim_digest_excludes_only_its_own_field,
    test_reprocessing_id_covers_the_original_claim_and_every_target_digest,
    test_derived_digest_refuses_a_record_carrying_no_derived_field,
    test_classifier_grades_every_change_class,
    test_classifier_covers_every_migration_matrix_row,
    test_attestation_rotation_carries_on_a_minor_bump,
    test_semver_bump_must_meet_the_classified_change,
    test_plan_migration_rejects_an_understated_bump,
    test_first_activation_is_a_reset_that_opens_a_pending_baseline,
    test_activation_reaching_active_rejects_a_null_activated_at,
    test_carry_opens_the_next_epoch_and_extends_the_ancestry_closure,
    test_carry_ancestry_capacity_is_enforced_before_activation,
    test_activation_refuses_a_stale_prior_contract,
    test_reset_opens_the_next_epoch_rooted_alone,
    test_activation_requires_the_candidate_to_be_registered_first,
    test_in_flight_race_loses_on_a_stale_revision,
    test_a_second_candidate_conflicts_while_one_is_in_flight,
    test_carry_refuses_an_empty_registry,
    test_migration_id_reaches_every_field_outside_its_own_exclusion,
    test_interval_boundaries_resolve_from_the_anchor,
    test_cron_grammar_admits_lists_ranges_and_steps_and_rejects_the_rest,
    test_cron_day_of_month_and_day_of_week_take_their_union,
    test_cron_boundaries_skip_a_spring_forward_gap,
    test_cron_boundaries_resolve_both_sides_of_a_fall_back_fold,
    test_cron_respects_its_lower_bound,
    test_a_migrated_schedule_evaluates_nothing_retroactively,
    test_timezone_data_version_is_pinned_to_the_installed_release,
    test_production_declaration_compiles_to_a_digested_contract,
    test_production_requires_every_product_fact,
    test_production_requires_the_estate_namespace_and_a_domain_membership,
    test_a_product_claimed_by_two_domains_fails_naming_both,
    test_a_source_landing_needs_an_explicit_draft_or_production_delivery,
    test_a_draft_delivery_resolves_to_an_explicit_placeholder,
    test_seed_tables_contribute_no_bronze_contract,
    test_source_delivery_defaults_overlay_and_a_mode_switch_clears_mode_specific_keys,
    test_the_loader_rejects_a_projection_column_it_cannot_type,
    test_estate_namespace_absence_is_valid_and_a_malformed_file_is_not,
    test_the_committed_estate_carries_its_namespace_and_no_bronze_products_yet,
    test_the_legacy_loader_reads_a_bronze_declaration_unchanged,
    test_the_legacy_loader_still_rejects_a_projection_column_with_neither_key,
    test_the_committed_estate_generates_byte_identical_output,
    test_no_template_owns_semantic_validation,
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
