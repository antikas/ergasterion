"""One-shot generator for contracts.json (not part of the test run itself).

Run manually with the repo's own interpreter whenever the three acceptance
contracts need to change:

    python tests/fixtures/bronze_acceptance/build_contracts.py

Each payload is validated against ``BronzeProductContract`` before it is
written, so the checked-in ``contracts.json`` is always a model-valid
snapshot -- the acceptance test only reads it, never regenerates it.
"""

from __future__ import annotations

import json
from pathlib import Path

from ergasterion.framework.bronze_contract import BronzeProductContract

HERE = Path(__file__).resolve().parent
NAMESPACE = "com.example.ergasterion.acceptance"
HMAC_KEY_ID = "synthetic-local-hmac"  # ergasterion.ingestion.settings.SYNTHETIC_HMAC_KEY_ID
SNAPSHOT_KEY_ID = "acceptance-snapshot-key-1"

CDC = {
    "schema": "ergasterion.bronze-product/v1",
    "logical_identity": {"estate_namespace": NAMESPACE, "source": "acceptance", "table": "accounts_cdc"},
    "product": {
        "product_version": "1.0.0",
        "display_name": "Acceptance accounts (CDC)",
        "description": "Synthetic CDC accounts feed with explicit tombstone deletes.",
        "owner": "local-process-user",
        "domain": "acceptance",
        "support": "local-process-user",
        "classification": "synthetic",
        "access_policy_ref": "local-process-user",
        "retention_policy_ref": "local-ephemeral",
    },
    "landing": {
        "kind": "source",
        "source_name": "acceptance_accounts",
        "identifier": "accounts_cdc",
        "integration": {"kind": "managed"},
        "content_encodings": ["identity"],
        "codec": {
            "kind": "jsonl", "version": 1, "charset": "utf-8", "newline": "lf", "top_level": "object",
            "duplicate_keys": "reject", "number_mode": "exact_decimal", "allow_blank_lines": False,
        },
        "physical_columns": [
            {"name": "account_id", "logical_type": "utf8_string", "nullable": False},
            {"name": "is_deleted", "logical_type": "boolean", "nullable": True},
            {"name": "seq", "logical_type": "int64", "nullable": False},
            {"name": "event_at", "logical_type": "utc_instant", "nullable": False},
            {"name": "loaded_at", "logical_type": "utc_instant", "nullable": False},
        ],
    },
    "delivery": {
        "kind": "production",
        "mode": "cdc",
        "progress": {"kind": "sequence", "field": "seq"},
        "delete_strategy": "explicit_tombstone",
        "schedule": {"kind": "interval", "every_minutes": 15, "anchor_at": "2026-01-01T00:00:00.000000Z"},
        "schedule_lateness": {"warn_after_minutes": 15, "error_after_minutes": 60},
        "timestamps": {"load_field": "loaded_at", "event_field": "event_at"},
        "record_key": {
            "fields": ["account_id"],
            "fingerprint_scope": {"scope_id": "acceptance_accounts_scope", "scope_parameters": {}},
            "hmac_key_id": HMAC_KEY_ID,
        },
        "tombstone": {"field": "is_deleted", "values": [{"logical_type": "boolean", "value": True}]},
        "quality": {
            "publication_mode": "all_or_nothing",
            "max_error_fraction": "0",
            "rules": [
                {"kind": "not_null", "field": "account_id", "severity": "error"},
                {"kind": "unique_key", "fields": ["account_id"], "severity": "error"},
            ],
        },
        "retry": {"max_attempts": 4, "backoff": "exponential", "base_seconds": 5, "cap_seconds": 300},
    },
    "projection": [
        {"source": "account_id", "name": "account_id", "logical_type": "utf8_string", "nullable": False},
        {"source": "loaded_at", "name": "loaded_at", "logical_type": "utc_instant", "nullable": False},
    ],
    "interfaces": {
        "raw": "bronze-acceptance-accounts_cdc-raw",
        "source_native": "bronze-acceptance-accounts_cdc-source-native",
        "published": "bronze-acceptance-accounts_cdc-published",
        "quarantine": "bronze-acceptance-accounts_cdc-quarantine",
        "deletion_evidence": "bronze-acceptance-accounts_cdc-deletion-evidence",
    },
}

APPEND_V1 = {
    "schema": "ergasterion.bronze-product/v1",
    "logical_identity": {"estate_namespace": NAMESPACE, "source": "acceptance", "table": "postings_append"},
    "product": {
        "product_version": "1.0.0",
        "display_name": "Acceptance ledger postings (append-only CSV)",
        "description": "Synthetic append-only ledger postings delivered as CSV.",
        "owner": "local-process-user",
        "domain": "acceptance",
        "support": "local-process-user",
        "classification": "synthetic",
        "access_policy_ref": "local-process-user",
        "retention_policy_ref": "local-ephemeral",
    },
    "landing": {
        "kind": "source",
        "source_name": "acceptance_postings",
        "identifier": "postings_append",
        "integration": {"kind": "managed"},
        "content_encodings": ["identity"],
        "codec": {
            "kind": "csv", "version": 1, "charset": "utf-8", "delimiter": ",", "header": True,
            "quote": "\"", "escape": "\\", "newline": "lf", "null_tokens": ["", "NULL"], "trim_whitespace": False,
        },
        "physical_columns": [
            {"name": "txn_id", "logical_type": "utf8_string", "nullable": False},
            {"name": "status", "logical_type": "utf8_string", "nullable": False},
            {"name": "amount", "logical_type": {"kind": "decimal", "precision": 18, "scale": 2}, "nullable": False},
            {"name": "booked_on", "logical_type": "date", "nullable": False},
            {"name": "loaded_at", "logical_type": "utc_instant", "nullable": False},
        ],
    },
    "delivery": {
        "kind": "production",
        "mode": "append_only",
        "progress": {"kind": "opaque_batch"},
        "delete_strategy": "none",
        "schedule": {"kind": "interval", "every_minutes": 60, "anchor_at": "2026-01-01T00:00:00.000000Z"},
        "schedule_lateness": {"warn_after_minutes": 15, "error_after_minutes": 60},
        "maximum_age": {"warn_after_minutes": 1440, "error_after_minutes": 2880},
        "timestamps": {"load_field": "loaded_at"},
        "record_key": {"fields": ["txn_id"]},
        "quality": {
            "publication_mode": "publish_valid_rows",
            "max_error_fraction": "0.5",
            "rules": [
                {"kind": "not_null", "field": "txn_id", "severity": "error"},
                {"kind": "unique_key", "fields": ["txn_id"], "severity": "error"},
                {
                    "kind": "accepted_values", "field": "status", "allow_null": False, "severity": "error",
                    "values": [
                        {"logical_type": "utf8_string", "value": "settled"},
                        {"logical_type": "utf8_string", "value": "pending"},
                        {"logical_type": "utf8_string", "value": "failed"},
                    ],
                },
            ],
        },
        "retry": {"max_attempts": 4, "backoff": "exponential", "base_seconds": 5, "cap_seconds": 300},
    },
    "projection": [
        {"source": "txn_id", "name": "txn_id", "logical_type": "utf8_string", "nullable": False},
        {"source": "status", "name": "status", "logical_type": "utf8_string", "nullable": False},
        {
            "source": "amount", "name": "amount",
            "logical_type": {"kind": "decimal", "precision": 18, "scale": 2}, "nullable": False,
        },
        {"source": "booked_on", "name": "booked_on", "logical_type": "date", "nullable": False},
        {"source": "loaded_at", "name": "loaded_at", "logical_type": "utc_instant", "nullable": False},
    ],
    "interfaces": {
        "raw": "bronze-acceptance-postings_append-raw",
        "source_native": "bronze-acceptance-postings_append-source-native",
        "published": "bronze-acceptance-postings_append-published",
        "quarantine": "bronze-acceptance-postings_append-quarantine",
        "deletion_evidence": "bronze-acceptance-postings_append-deletion-evidence",
    },
}

# The additive migration candidate: same identity, minor version bump, one new
# nullable physical column + projection field, both schema digests therefore
# change while accepted progress (opaque_batch) still carries under "carry".
APPEND_V1_1 = json.loads(json.dumps(APPEND_V1))
APPEND_V1_1["product"]["product_version"] = "1.1.0"
APPEND_V1_1["landing"]["physical_columns"] = APPEND_V1_1["landing"]["physical_columns"] + [
    {"name": "channel", "logical_type": "utf8_string", "nullable": True},
]
APPEND_V1_1["projection"] = APPEND_V1_1["projection"] + [
    {"source": "channel", "name": "channel", "logical_type": "utf8_string", "nullable": True},
]

SNAPSHOT = {
    "schema": "ergasterion.bronze-product/v1",
    "logical_identity": {"estate_namespace": NAMESPACE, "source": "acceptance", "table": "customers_snapshot"},
    "product": {
        "product_version": "1.0.0",
        "display_name": "Acceptance customers (complete snapshot)",
        "description": "Synthetic signed customer population snapshot.",
        "owner": "local-process-user",
        "domain": "acceptance",
        "support": "local-process-user",
        "classification": "synthetic",
        "access_policy_ref": "local-process-user",
        "retention_policy_ref": "local-ephemeral",
    },
    "landing": {
        "kind": "source",
        "source_name": "acceptance_customers",
        "identifier": "customers_snapshot",
        "integration": {"kind": "managed"},
        "content_encodings": ["identity"],
        "codec": {
            "kind": "jsonl", "version": 1, "charset": "utf-8", "newline": "lf", "top_level": "object",
            "duplicate_keys": "reject", "number_mode": "exact_decimal", "allow_blank_lines": False,
        },
        "physical_columns": [
            {"name": "customer_id", "logical_type": "utf8_string", "nullable": False},
            {"name": "effective_at", "logical_type": "utc_instant", "nullable": False},
            {"name": "loaded_at", "logical_type": "utc_instant", "nullable": False},
        ],
    },
    "delivery": {
        "kind": "production",
        "mode": "complete_snapshot",
        "progress": {"kind": "opaque_batch"},
        "delete_strategy": "snapshot_diff",
        "schedule": {"kind": "interval", "every_minutes": 1440, "anchor_at": "2026-01-01T00:00:00.000000Z"},
        "schedule_lateness": {"warn_after_minutes": 60, "error_after_minutes": 240},
        "timestamps": {"load_field": "loaded_at", "effective_field": "effective_at"},
        "record_key": {
            "fields": ["customer_id"],
            "fingerprint_scope": {"scope_id": "acceptance_customer_population", "scope_parameters": {}},
            "hmac_key_id": HMAC_KEY_ID,
        },
        "snapshot": {
            "scope_id": "acceptance_customer_population",
            "scope_parameters": {},
            "attestation_policy_ref": "attest-default",
            "allowed_key_ids": [SNAPSHOT_KEY_ID],
            "future_clock_skew_seconds": 30,
        },
        "quality": {
            "publication_mode": "all_or_nothing",
            "max_error_fraction": "0",
            "rules": [{"kind": "not_null", "field": "customer_id", "severity": "error"}],
        },
        "retry": {"max_attempts": 4, "backoff": "exponential", "base_seconds": 5, "cap_seconds": 300},
    },
    "projection": [
        {"source": "customer_id", "name": "customer_id", "logical_type": "utf8_string", "nullable": False},
        {"source": "loaded_at", "name": "loaded_at", "logical_type": "utc_instant", "nullable": False},
    ],
    "interfaces": {
        "raw": "bronze-acceptance-customers_snapshot-raw",
        "source_native": "bronze-acceptance-customers_snapshot-source-native",
        "published": "bronze-acceptance-customers_snapshot-published",
        "quarantine": "bronze-acceptance-customers_snapshot-quarantine",
        "deletion_evidence": "bronze-acceptance-customers_snapshot-deletion-evidence",
    },
}


def main() -> None:
    document = {
        "cdc": CDC,
        "append_v1": APPEND_V1,
        "append_v1_1_additive": APPEND_V1_1,
        "snapshot": SNAPSHOT,
    }
    for name, payload in document.items():
        BronzeProductContract.model_validate(payload)
        print(f"{name}: valid")
    out = HERE / "contracts.json"
    out.write_text(json.dumps(document, indent=1, sort_keys=False) + "\n", encoding="utf-8")
    print(f"wrote {out}")


if __name__ == "__main__":
    main()
