# Bronze Product Contract specification v1

## The problem this contract solves

A source system delivers data to Ergasterion in batches: a file drop, a change-data-capture
stream landed in a staging area, an exported snapshot. Before that data can be trusted
downstream, three questions need one fixed answer, agreed once and never guessed per run:

- **What is this data?** Its field names, types, nullability, owner, classification and
  retention rules.
- **How does it arrive?** Which delivery pattern (change events, an append-only stream, or a
  full snapshot), on what schedule, with what quality bar, and what a deletion looks like.
- **What can a consumer rely on?** Which published columns are stable, what evidence proves a
  row passed validation, and what happens when it does not.

## Terms

**IDL (interface definition language).** A language-neutral description of a wire format:
which records exist, which fields each record has, and which type, requiredness and
nullability rule applies to each field. Ergasterion's Bronze IDL is one JSON file. It is
closed, meaning no field or record outside the ones it names is valid, and pinned by content
hash so an unattended build cannot silently accept a changed interface.

**Wire record.** Any of the closed, named shapes the IDL declares: a request, a response, a
piece of evidence, a state record. These are the shapes that cross a process or storage
boundary, such as a contract handed to a compiler, a delivery handed to a connector, or an
evidence record written to a store.

**Required and nullable.** Two independent facts about a field. Required means the field's key
must be present in the payload; its absence is rejected. Nullable means the field's value may
be the JSON literal `null`. The four combinations have distinct meanings:

- required and nullable: always present, sometimes null;
- required and not nullable: always present with a real value;
- optional and nullable: may be absent or present with a real or null value;
- optional and not nullable: may be absent, but an explicit `null` is rejected.

**Closed.** A record accepts exactly its declared fields. An unrecognised field in a payload
is a validation error.

**Managed and external integration.** Two ways a source's raw data reaches Ergasterion.
Managed means Ergasterion's own connector fetches and preserves the payload. External means
another system already landed the payload and hands Ergasterion a signed receipt describing
where it is and what its evidence identifiers are, in place of the bytes themselves.

**Delivery mode.** One of three shapes a delivery can take: `cdc` (a stream of change events,
each carrying its own operation), `append_only` (new rows only, no update or delete
semantics), or `complete_snapshot` (a full replacement of the source's current state, from
which the difference against the prior snapshot is derived).

**Digest.** A lowercase hexadecimal SHA-256 hash, used throughout the contract family to pin
one exact version of another record without repeating its full content.

**Sidecar.** A contract-level policy block that is present only when the source needs it:
`maximum_age` (a native-freshness SLA on top of the mandatory schedule-boundary timeliness),
`tombstone` (how a delete is recognised inside a CDC or append stream), and `snapshot` (the
attestation policy a complete-snapshot delivery must satisfy). A source with none of these
needs still has a complete, valid contract; the sidecars exist for the sources that do.

**Execution plan.** The resolved graph a Bronze declaration compiles to: the pattern
occurrences, the edges between them and the handoff schemas they exchange, pinned by its own
digest (`ExecutionPlan` in the IDL).

**Runtime binding.** The record declaring how one environment runs a contract: which adapter
implements each of the nine Bronze ports, which target relations the projection writes to, and
the operating envelope (schedule, retry, resource ceilings, retention, protection profile).

## What the contract is

The **Bronze Product Contract** is the one authored record that answers all three questions. A
team writes it once per source table. The execution plan and runtime bindings are generated
from it. Running it produces the lineage and quality records. Every downstream fact therefore
traces back to this authored record in the shape it was declared.

The contract's meaning and how its pieces fit together follow below. The contract's exact
shape has one authority: the frozen portable interface definition language (IDL) file
`docs/specifications/bronze-portable-idl-v1.json`, and one generated projection of it, the
JSON Schema bundle `ergasterion/schemas/bronze-product-v1.schema.json`. Every field name,
type, requiredness and nullability rule described below is a fact read from those two files.

## What the contract identifies and declares

Every Bronze Product Contract carries, in one authored record:

- **Identity** (`logical_identity`): the estate namespace, source name and table name that
  together name this product uniquely across the estate.
- **Product facts** (`product`): version, display name, description, owner, domain, support
  contact, classification and the access/retention policy references that govern it.
- **Landing** (`landing`): the source-native physical schema (field names, logical types,
  nullability), its codec (CSV or JSON Lines, with every parsing rule pinned: delimiter,
  quoting, newline convention, null tokens), and its integration (managed or external).
- **Delivery policy** (`delivery`): the delivery mode, progress tracking, delete strategy,
  schedule, timeliness SLA, timestamp fields, record key (including an optional keyed
  fingerprint scope for pseudonymised matching), quality policy and retry policy, plus
  whichever sidecars this source needs.
- **Projection** (`projection`): the published, source-derived output columns. Each one names
  its source physical column, its published name, its logical type and its nullability.
- **Interfaces** (`interfaces`): the five named consumer-facing surfaces every Bronze product
  exposes. `raw` holds immutable source evidence, `source_native` holds parsed,
  quality-annotated records, `published` holds accepted, downstream-visible rows, `quarantine`
  holds rejected records with stable locators back to their raw evidence, and
  `deletion_evidence` holds accepted deletion facts. An unaccepted deletion attempt never
  produces a deletion-evidence record.

Product and field lineage are computed facts, derived mechanically from the logical identity,
the landing schema and the projection mapping, plus the execution graph a Bronze declaration
always resolves to. A contract author writes source facts and policy; the platform computes
lineage from that authoring.

## Field reference: `BronzeProductContract` and its direct children

The frozen IDL's `records.BronzeProductContract` entry and the matching JSON Schema bundle
entry set the exact type expression, requiredness and nullability of every field named here
and in every other record named below.

| Field | Type | Required | Nullable | Meaning |
| --- | --- | --- | --- | --- |
| `schema` | constant string `ergasterion.bronze-product/v1` | yes | no | Fixes which wire shape this JSON document is. |
| `logical_identity` | `LogicalIdentity` | yes | no | Estate namespace, source, table. |
| `product` | `ProductFacts` | yes | no | Version, ownership, classification, policy references. |
| `landing` | `LandingContract` | yes | no | Source-native schema, codec, integration. |
| `delivery` | `DeliveryPolicy` | yes | no | Mode, schedule, quality, retry, sidecars. |
| `projection` | list of `ProjectionField` | yes | no | The published output columns, in declared order. |
| `interfaces` | `BronzeInterfaces` | yes | no | The five named consumer-facing surface identifiers. |

`LogicalIdentity` carries `estate_namespace` (a reverse-DNS-shaped namespace), `source` and
`table`, all three required and all three not nullable. See IDL for the exact pattern each
scalar type enforces.

`LandingContract` carries `source_name`, `identifier`, `integration` (a managed-or-external
union), `content_encodings` (a set of `identity`/`gzip`), `codec` (a CSV-or-JSONL union) and
`physical_columns` (the declared-order source-native schema). All are required and not
nullable.

`DeliveryPolicy` carries `mode`, `progress`, `delete_strategy`, `schedule`,
`schedule_lateness`, `timestamps`, `record_key`, `quality` and `retry` as required, not
nullable fields. `maximum_age`, `tombstone` and `snapshot` are the three sidecars: each is
optional, and, when given, not nullable. A contract either carries the sidecar with a real
policy or omits it entirely; an explicit `null` for any of the three is rejected.

## Complete managed and external declarations

Both worked examples below are drawn unchanged from the same positive vectors
`tests/fixtures/bronze_schema_vectors.json` validates against the live `BronzeProductContract`
model: each is the exact payload the passing test suite checks, copied value for value.

### External integration, CDC mode, every sidecar populated

```json
{
  "schema": "ergasterion.bronze-product/v1",
  "logical_identity": {
    "estate_namespace": "com.example.synthetic",
    "source": "ledger",
    "table": "accounts"
  },
  "product": {
    "product_version": "1.0.0",
    "display_name": "Accounts",
    "description": "Synthetic accounts feed.",
    "owner": "team-data",
    "domain": "finance",
    "support": "team-data",
    "classification": "internal",
    "access_policy_ref": "policy-default",
    "retention_policy_ref": "retention-default"
  },
  "landing": {
    "kind": "source",
    "source_name": "ledger",
    "identifier": "accounts",
    "integration": {
      "kind": "external",
      "delivery_id_column": "delivery_id",
      "visibility_epoch_column": "epoch",
      "visibility_kind_column": "visibility_kind",
      "visibility_id_column": "visibility_id",
      "raw_reference_field": "raw_ref",
      "candidate_reference_field": "candidate_ref",
      "frame_index_reference_field": "frame_index_ref",
      "receipt_trust": {
        "policy_ref": "trust-default",
        "allowed_key_ids": ["key-a", "key-b"],
        "future_clock_skew_seconds": 30
      }
    },
    "content_encodings": ["identity", "gzip"],
    "codec": {
      "kind": "csv", "version": 1, "charset": "utf-8", "delimiter": ",", "header": true,
      "quote": "\"", "escape": "\\", "newline": "lf", "null_tokens": [""], "trim_whitespace": false
    },
    "physical_columns": [
      {"name": "acct_id", "logical_type": "utf8_string", "nullable": false},
      {"name": "is_deleted", "logical_type": "boolean", "nullable": true}
    ]
  },
  "delivery": {
    "kind": "production",
    "mode": "cdc",
    "progress": {"kind": "sequence", "field": "seq"},
    "delete_strategy": "explicit_tombstone",
    "schedule": {"kind": "interval", "every_minutes": 15, "anchor_at": "2026-01-01T00:00:00.000000Z"},
    "schedule_lateness": {"warn_after_minutes": 30, "error_after_minutes": 60},
    "maximum_age": {"warn_after_minutes": 120, "error_after_minutes": 240},
    "timestamps": {"load_field": "loaded_at", "event_field": "event_at", "effective_field": "effective_at"},
    "record_key": {
      "fields": ["acct_id"],
      "fingerprint_scope": {"scope_id": "account_population", "scope_parameters": {}},
      "hmac_key_id": "hmac-key-1"
    },
    "tombstone": {"field": "is_deleted", "values": [{"logical_type": "boolean", "value": true}]},
    "snapshot": {
      "scope_id": "account_population", "scope_parameters": {}, "attestation_policy_ref": "attest-default",
      "allowed_key_ids": ["key-a"], "future_clock_skew_seconds": 30
    },
    "quality": {
      "publication_mode": "all_or_nothing", "max_error_fraction": "0.0",
      "rules": [
        {"kind": "not_null", "field": "acct_id", "severity": "error"},
        {"kind": "unique_key", "fields": ["acct_id"], "severity": "error"}
      ]
    },
    "retry": {"max_attempts": 3, "backoff": "exponential", "base_seconds": 5, "cap_seconds": 60}
  },
  "projection": [
    {"source": "acct_id", "name": "acct_id", "logical_type": "utf8_string", "nullable": false}
  ],
  "interfaces": {
    "raw": "raw.accounts", "source_native": "sn.accounts", "published": "pub.accounts",
    "quarantine": "q.accounts", "deletion_evidence": "del.accounts"
  }
}
```

The `external` integration names four columns the upstream landing table must carry:
`delivery_id_column`, `visibility_epoch_column`, `visibility_kind_column`, and
`visibility_id_column`. It also names three evidence-reference fields. Its `receipt_trust`
policy declares accepted signing keys and the permitted future clock skew for a receipt.

### Managed integration, append-only mode, every sidecar populated

```json
{
  "schema": "ergasterion.bronze-product/v1",
  "logical_identity": {
    "estate_namespace": "com.example.synthetic",
    "source": "ledger",
    "table": "accounts"
  },
  "product": {
    "product_version": "1.0.0",
    "display_name": "Accounts",
    "description": "Synthetic accounts feed.",
    "owner": "team-data",
    "domain": "finance",
    "support": "team-data",
    "classification": "internal",
    "access_policy_ref": "policy-default",
    "retention_policy_ref": "retention-default"
  },
  "landing": {
    "kind": "source",
    "source_name": "ledger",
    "identifier": "accounts",
    "integration": {"kind": "managed"},
    "content_encodings": ["identity", "gzip"],
    "codec": {
      "kind": "csv", "version": 1, "charset": "utf-8", "delimiter": ",", "header": true,
      "quote": "\"", "escape": "\\", "newline": "lf", "null_tokens": [""], "trim_whitespace": false
    },
    "physical_columns": [
      {"name": "acct_id", "logical_type": "utf8_string", "nullable": false},
      {"name": "is_deleted", "logical_type": "boolean", "nullable": true}
    ]
  },
  "delivery": {
    "kind": "production",
    "mode": "append_only",
    "progress": {"kind": "sequence", "field": "seq"},
    "delete_strategy": "explicit_tombstone",
    "schedule": {"kind": "interval", "every_minutes": 15, "anchor_at": "2026-01-01T00:00:00.000000Z"},
    "schedule_lateness": {"warn_after_minutes": 30, "error_after_minutes": 60},
    "maximum_age": {"warn_after_minutes": 120, "error_after_minutes": 240},
    "timestamps": {"load_field": "loaded_at", "event_field": "event_at", "effective_field": "effective_at"},
    "record_key": {
      "fields": ["acct_id"],
      "fingerprint_scope": {"scope_id": "account_population", "scope_parameters": {}},
      "hmac_key_id": "hmac-key-1"
    },
    "tombstone": {"field": "is_deleted", "values": [{"logical_type": "boolean", "value": true}]},
    "snapshot": {
      "scope_id": "account_population", "scope_parameters": {}, "attestation_policy_ref": "attest-default",
      "allowed_key_ids": ["key-a"], "future_clock_skew_seconds": 30
    },
    "quality": {
      "publication_mode": "all_or_nothing", "max_error_fraction": "0.0",
      "rules": [
        {"kind": "not_null", "field": "acct_id", "severity": "error"},
        {"kind": "unique_key", "fields": ["acct_id"], "severity": "error"}
      ]
    },
    "retry": {"max_attempts": 3, "backoff": "exponential", "base_seconds": 5, "cap_seconds": 60}
  },
  "projection": [
    {"source": "acct_id", "name": "acct_id", "logical_type": "utf8_string", "nullable": false}
  ],
  "interfaces": {
    "raw": "raw.accounts", "source_native": "sn.accounts", "published": "pub.accounts",
    "quarantine": "q.accounts", "deletion_evidence": "del.accounts"
  }
}
```

A `managed` integration is the whole of `{"kind": "managed"}`. Every other landing fact (codec,
physical columns, content encodings) is unchanged from the external case, because managed and
external is purely a statement about who fetches and preserves the raw payload, separate from
the source's own schema. A third worked declaration, `complete_snapshot` mode, is in
`tests/fixtures/bronze_schema_vectors.json` under the note "production declaration 3/3".
Together the three vectors exercise all three delivery modes this contract can declare.

## Logical types and typed canonical scalars

A `physical_columns` or `projection` entry's `logical_type` is either a bare token
(`binary`, `boolean`, `date`, `int64`, `utc_instant`, `utf8_string`) or, for the two
parameterised types, an object: `{"kind": "decimal", "precision": 18, "scale": 4}` or
`{"kind": "local_datetime", "timezone": "Europe/London"}`.

A value carried in a typed context is a **typed scalar**. This applies to a quality rule's
comparison value, a tombstone marker, and a record-key component. Its `logical_type` selects
one of eight shapes, each pairing the type with a canonical value. `int64` and `decimal`
values are decimal strings because a bare JSON number
cannot represent every value exactly. `binary` values are unpadded base64url
(`ByteStringBase64Url`, a pattern that explicitly permits the empty string for a zero-length
value). `tests/fixtures/bronze_schema_vectors.json` carries one positive vector per typed
scalar shape, including the empty-binary case.

## Runtime binding, deployment and capabilities

A contract declares what a source is. A **runtime binding** declares how one environment runs
it. The binding selects an adapter for each of nine Bronze ports: source connector, raw
store, scratch store, state store, landing adapter, remediation repository, projection
publisher, lifecycle sink, and key resolver. It also names the projection targets and the
operating envelope: scheduler cadence, retry policy, resource ceilings, retention windows,
and a protection profile.

Every adapter and translator additionally declares its own **capabilities**
(`AdapterCapabilities`). This record covers supported input kinds, delivery modes, codecs,
content encodings, and logical types. It declares structural guarantees such as immutable
write, compare-and-swap, atomic projection, gap-free revision, idempotent replay, and bounded
streaming. It also records resource limits and the protection profile: encryption, policy
binding, audit evidence, retention enforcement, backup and restore, and the secret boundary.
A production activation checks the selected adapters against their contract and execution
plan before accepting traffic. The resulting `InterfaceReadiness` record carries its
own digest and a `ready`/`rejected` verdict.

`tests/fixtures/bronze_schema_vectors.json` carries one complete `RuntimeBinding` vector (all
nine ports bound, every projection relation named), one complete `AdapterCapabilities`
vector, and one `InterfaceReadiness` vector recording a passed check.

## Ports: the nine runtime interfaces

Every Bronze runtime interacts with its environment through exactly nine named ports. Each
port's methods, request fields, response type and possible error codes are fixed by the IDL's
`ports` section. `ergasterion.ingestion.records.PORTS` is the Python projection of that same
section, used to build the equivalence report described below.

| Port | Kind | Methods |
| --- | --- | --- |
| `SourceConnector` | `source_connector` | `submit_managed`, `verify_external` |
| `RawStore` | `raw_store` | `get_receipt`, `open_raw`, `read_raw`, `preserve`, `verify_open` |
| `ScratchStore` | `scratch_store` | `create_scope`, `write_sequential`, `read_sequential`, `close_scope`, `delete_scope`, `cleanup_orphans` |
| `DeliveryStateStore` | `state_store` | `contract_lifecycle`, `deployment_lifecycle`, `attempts`, `state_transaction`, `lease_outbox`, `load_outbox_payload`, `fail_outbox`, `projection_log`, `projection_confirmation_log`, `lifecycle_event_log`, `status_query`, plus the snapshot-keyset, tombstone-keyset and reconciliation methods |
| `LandingAdapter` | `landing_adapter` | `begin_prepare`, `append_raw`, `finish_prepare`, `read_candidate`, `begin_materialization`, `append_dispositions`, `finish_materialization`, `bind_release_visibility`, `source_native_query`, `disposition_query`, `verify_open` |
| `RemediationRepository` | `remediation_repository` | `record_decision`, `decision_query` |
| `ProjectionPublisher` | `projection_publisher` | `apply_gap_ordered`, `read_cursor`, `rebuild_read_models` |
| `LifecycleSink` | `lifecycle_sink` | `project_events`, `evidence_query` |
| `KeyResolver` | `key_resolver` | `resolve_verification_key`, `key_commitment`, `mac` |

A port's exact request field list, response type and error-code set are IDL facts. See
`docs/specifications/bronze-portable-idl-v1.json` `ports.<PortName>.methods` for each one.
The wire schema covers which records exist and what shape they have. The generated
equivalence report checks the wire schema for every port method against the IDL: the request
field name and type list, the response type, and the error-code set.

## Attestations, signatures and key commitments

Two record families carry a cryptographic signature. A **signed external receipt**
(`SignedExternalReceipt`, wrapping an `ExternalReceiptPayload`) proves an external system's
claim about a delivery's evidence identifiers. A **signed snapshot attestation**
(`SignedAttestation`, wrapping a `SnapshotAttestationPayload`) proves a complete-snapshot
delivery's completeness claim. Both use the same shape: an algorithm (`Ed25519`), a key
identifier, the signed payload, and the signature itself.

A **key commitment** (`KeyCommitmentRecord`) binds an HMAC key's identity (`HMAC-SHA-256`) so
a record-key fingerprint computed under that key can be verified as having used the committed
key, with the key itself absent from every wire record. `VerificationKeyRecord` is the third
piece: the
registry entry for one Ed25519 signing key, its enablement window, and the policies it is
authorised under.

**Sensitive-field rule.** A field marked `digest_excluded` in the IDL (for example a
`SignedAttestation`'s own `signature`, or `RawReceipt`'s own `raw_receipt_digest`) is omitted
only from that record's own derived-ID or content-digest computation. It stays a real,
required field on the wire, present in every payload and every generated schema, and visible
to every consumer. The same is true of `signature_excluded` fields with respect to the signed
envelope's own signing basis: both markings keep a field on the wire while excluding it from a
computation that would otherwise have to cover its own output.

## Record-key fingerprints and the MAC framing

A record key (`RecordKeyContract`) may carry an optional `fingerprint_scope` and
`hmac_key_id`. Together they let two deliveries be matched on a pseudonymised key without
either side learning the plaintext key value. The fingerprint is an HMAC-SHA-256 tag over
this fixed framing: `uint32be(domain_length) || domain_utf8 || uint64be(message_length) ||
message_bytes`. The domain is fixed to `ergasterion.record-key/v1`. The message is canonical
JSON containing the key's typed components, logical identity, schema token, and fingerprint
scope. The IDL's own `golden_vectors.record_key_mac` and
`golden_vectors.record_key_mac_parameterized_scope` pin one exact key, message and expected
tag each. `tests/python/test_bronze_schema.py` recomputes both from scratch and asserts
byte-for-byte and tag-for-tag equality against the frozen values.

## Local backup: manifest and page chain

A local backup (`BackupManifest`) records one point-in-time, verified copy of a runtime root:
the runtime binding and manifest digests it was taken against, the state and projection
revisions it captured, and a chain of `BackupEntryPage` records. Each page names its files
(relative path, file mode, size, SHA-256) and the digest of the page before it, so the chain
can be walked and verified without loading every page at once. The chain's root page carries
`page_index: 0` and `previous_page_digest: null`. A backup of an empty runtime root is valid
and produces a root page with zero entries.
`tests/fixtures/bronze_schema_vectors.json` carries exactly this: an empty root page, a second
page chaining back to it by digest, and the manifest referencing both.

## Codecs and quality modes

Landing accepts exactly two codecs: `csv` (delimiter, quote, escape, newline, null-token and
trim-whitespace rules, all pinned) and `jsonl` (newline-delimited JSON objects, rejecting
duplicate keys and blank lines, numbers always read as exact decimals). A quality policy's
`publication_mode` is one of two values: `all_or_nothing` (any error-severity finding rejects
the whole delivery) or `publish_valid_rows` (rows below the finding threshold publish; the
rest are quarantined). Both codecs and both quality modes appear in
`tests/fixtures/bronze_schema_vectors.json`, validated against the same Pydantic models in
`ergasterion/framework/bronze_contract.py`.

## Vectors, coverage and the generated equivalence report

`tests/fixtures/bronze_schema_vectors.json` holds two kinds of positive vector. Curated
vectors exercise a named scenario: the three production
delivery modes, both integration kinds, both codecs, both quality modes, the runtime-binding
and capability records, the attestation and backup families, and the golden vectors for the
MAC framing, the zero-byte raw page and the empty-binary scalar. Synthetic vectors, one per
IDL record, exercise every field of every record with a type-faithful value read from the
IDL's own field definitions for that record. Together the two kinds give every one of the
IDL's records a positive vector, directly or through nesting inside another vector's payload,
and populate every field of every record across the full vector set.

`ergasterion/schemas/bronze-portable-idl-equivalence.json` is the generated equivalence
report: for every IDL record, enum, union, port, error code, scalar and
port-operation-order entry, a verdict that the Pydantic projection matches the IDL exactly. A
record's verdict checks its field-name set, its required-field set, its nullable-field set,
and, field by field, its resolved type: a scalar, enum, record or union identity, recursed
through `list<T>` and `map<Token,T>`. A union's verdict checks its variant set, resolved
structurally from the actual Python union object. A port's verdict checks its method set and,
per method, the request field name and type list, the response type, and the error-code set
against the IDL. A scalar's verdict checks its base type and, where the IDL states one, its
pattern or numeric/length bound. A port-operation-order verdict checks the operation sequence
for that port matches the IDL, in order.

## How the IDL and the generated schema relate

Three artefacts carry three distinct jobs, checked against each other every time
`tests/python/test_bronze_schema.py` runs.

`docs/specifications/bronze-portable-idl-v1.json` is the frozen, hash-pinned structural
authority. Every record, field, type, union, port and error code originates here.

`ergasterion/schemas/bronze-product-v1.schema.json` is the generated JSON Schema bundle, one
entry per IDL record, derived directly from the closed Pydantic models in
`ergasterion/framework/bronze_contract.py`, `ergasterion/framework/runtime_binding.py` and
`ergasterion/ingestion/records.py`. Regenerating it from those models reproduces the committed
file byte for byte.

The equivalence report at `ergasterion/schemas/bronze-portable-idl-equivalence.json`
regenerates from the same models and reproduces the committed file byte for byte.

Exact record, field and port facts point at the IDL and the generated schema and equivalence
report, the single source for each of those facts.
