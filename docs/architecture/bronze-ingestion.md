# Bronze: receiving a source's delivered batch

[Architecture overview](README.md) places Bronze at the front of the generated pipeline,
before the typed staging the rest of that document walks. This document is the deep dive:
the exact mechanism a delivered batch passes through before anything downstream reads it.
[`docs/specifications/bronze-product-v1.md`](../specifications/bronze-product-v1.md) is the
field-by-field contract reference; every wire fact in this document traces back to that
specification and the frozen IDL it names. [`demo/bronze-ingestion/`](../../demo/bronze-ingestion/)
runs the mechanism this document describes, account-free and network-free.

![Bronze preserves a payload and manifest as raw evidence, parses source-native rows, and separates published rows, quarantined rows, and accepted deletion evidence. The product contract governs parsing and validation; typed staging reads the published interface.](bronze-ingestion.svg)

## The problem this layer solves

A source hands your estate a batch: a file drop, an exported snapshot, a stream of change
events landed somewhere you can read. Before any model, any test, or any person downstream
can trust a single row of it, three things have to happen in a fixed order. The exact bytes
must be preserved untouched. The batch must be checked against rules agreed in advance,
and its accepted or rejected outcome must be recorded permanently. Bronze applies these
steps the same way to every declared source.

## What you declare, and what the runtime applies

A team writes one **Bronze Product Contract** per source table: its native schema, how it
delivers (a stream of change events, append-only rows, or a full snapshot), its quality
rules, and the policies that govern it. Writing the contract is the one place human
judgment enters. The runtime then parses each delivery under the declared codec and checks
every rule. It publishes accepted rows and quarantines rejected rows with a locator back
to the exact bytes that failed. No person decides a single delivery's outcome at run
time. The contract decided it when it was written; the runtime only carries out what it
says.

A **runtime binding** declares how one environment runs a contract: which adapter
implements each of Bronze's nine ports, which target relations the projection writes to,
and the operating envelope (schedule, retry, resource ceilings, retention, a named
protection profile). The contract states what a source is. The binding states how one
environment runs it. The two stay separate records so the same contract can run in more
than one environment without being rewritten.

## The received-batch boundary

A delivery arrives as two parts: a **payload** (the bytes the source sent) and a **sidecar
manifest**, a small JSON document that names the delivery, declared row count, and
transport fingerprint. For a complete snapshot, it also carries a signed attestation of
completeness. The runtime's `ingest file` command takes both and carries the delivery
through four steps, in order, every time:

1. **Preserve.** The raw payload and its sidecar manifest are written to the raw store
   exactly as received, before any parsing. This is the batch's permanent evidence: if a
   later question is "where did this number come from", the answer always starts here.
2. **Land.** The payload is parsed under the contract's declared codec (CSV or JSON Lines,
   every parsing rule pinned: delimiter, quoting, newline convention, null tokens) into
   typed, source-native records.
3. **Validate.** Every declared quality rule runs against the typed records: not-null
   checks, uniqueness, accepted-value sets, and the rest the contract names. Each record
   gets a disposition, accepted or rejected, and each disposition carries a locator back to
   its exact raw bytes.
4. **Publish.** Accepted records enter the published interface. The contract's
   `publication_mode` decides what happens when some rows are rejected. Under
   `publish_valid_rows`, accepted rows publish and rejected rows quarantine, up to the
   declared error-fraction ceiling. Under `all_or_nothing`, one rejected row rejects the
   whole delivery, so none of its rows enter the published interface. A delivery that
   fails under `all_or_nothing` leaves whatever was already current in place. It never
   partially overwrites it.

This is the **received-batch boundary**: the point where a source's bytes stop being the
source's problem and become a checked, evidenced, addressable record inside the estate. A
row that crosses it carries proof of exactly which rule it passed and which raw bytes it
came from. A row that fails to cross it is quarantined with the same proof, in reverse: an
exact locator back to the raw bytes that failed, and which rule rejected it.

## The five interfaces

Every Bronze product exposes the same five named surfaces, declared in the contract's
`interfaces` block:

- **`raw`** holds the immutable, exactly-preserved source evidence: the payload bytes
  and their manifest, before any parsing.
- **`source_native`** holds the parsed, quality-annotated records, before the
  accept/reject decision is applied.
- **`published`** holds the accepted, downstream-visible rows. This is the interface the
  rest of the factory's generated pipeline reads: the typed staging models further down
  the architecture overview are generated against a Bronze product's `published`
  interface.
- **`quarantine`** holds the rejected records, each with a stable locator back to its raw
  evidence, so a person can inspect exactly what failed and why.
- **`deletion_evidence`** holds accepted deletion facts. A deletion attempt that is never
  accepted never produces a deletion-evidence record.

## Mandatory, optional and forbidden patterns

Bronze classifies fifteen structural patterns as mandatory, optional, or forbidden. Every
Bronze product requires Batch Ingestion, Data Validation, Data Contracts, Lineage Capture,
Metadata Capture, Schema Publish, Data Publish, and Checkpoint & Retries. Batch Transfer
moves an already-landed payload between storage locations without changing it. A managed
integration needs it; a source landed by an external system does not. Schema
Transform, Calculated Fields, Data Enrichment, Data Filtering, Data Aggregation and Data
Curation are forbidden in Bronze. A Bronze product's published rows are the source's own
fields, typed and checked; deriving a new value, joining across sources, or filtering rows
by a business predicate all belong to a later layer, generated once Bronze's published
interface exists.

## Delivery modes and how a deletion is recognised

A contract declares exactly one delivery mode. `cdc` is a stream of change events, each
carrying its own operation; a deletion is recognised through an explicit tombstone field
and value the contract names. `append_only` is new rows only, with no update or delete
semantics of its own. `complete_snapshot` is a full replacement of the source's current
state; the runtime derives what changed, including what was deleted, by comparing the new
snapshot against the prior one under the contract's declared record key.

A complete-snapshot delivery carries one more requirement the other two modes do not: a
**signed attestation** proving the delivery's completeness claim, checked against a
registered verification key before the delivery is accepted. `demo/bronze-ingestion/`
walks a complete-snapshot delivery that is source-complete (every row the source meant to
send is present) but acceptance-incomplete (one row fails a declared quality rule), and
shows the prior snapshot staying current while the incomplete delivery is quarantined.

## Managed and external integration

A source's raw data reaches Ergasterion one of two ways. A **managed** integration means
Ergasterion's own connector fetches and preserves the payload directly. An **external**
integration means another system already landed the payload and hands Ergasterion a signed
receipt describing where it is and what its evidence identifiers are, in place of the bytes
themselves; the receipt's signature is checked against a registered key before it is
trusted. Both integrations produce the same received-batch boundary and the same five
interfaces; the difference is only in who moves the bytes.

## The operator command surface

Every Bronze product is operated through one closed CLI surface, `ergasterion <command>
--project-dir PATH --source NAME --table KEY --binding PATH --environment NAME`, plus each
command's own arguments:

| Command | What it does |
| --- | --- |
| `plan` | Compiles the Bronze execution graph and the resolved runtime manifest. Read-only. |
| `contract register` / `contract activate` | Registers a candidate contract, then carries or resets it to active. A `carry` migration keeps visibility progress; a `reset` authorises a new baseline. |
| `deployment register` / `deployment activate` | Registers and activates a binding-only runtime relocation. This never moves a durable store; it only changes which adapters and target relations a contract runs against. |
| `ingest file` | Preserves a delivery's sidecar and payload, then lands, validates and publishes it: the received-batch boundary described above. |
| `ingest due` | Evaluates due heartbeats and schedules catch-up work. `--dry-run` makes this read-only. |
| `reconcile` | Resumes a commit-blocked projection and rebuilds a lagging target cursor. |
| `local-backup` | Creates or restores a verified copy of the complete local runtime root. |
| `status` | Read-only operator and stream status: freshness, accepted progress, the latest attempt's state. |
| `inspect` | Read-only contract, schema, receipt, quality and lineage evidence. `--delivery-id` narrows the evidence to one received batch. |
| `quarantine` | Lists quarantined rows, or revalidates and releases a specific rejected row once its underlying cause is fixed. |

Every mutating command is idempotent: replaying the same `ingest file` call, or the same
`quarantine release`, reproduces the same durable outcome once.
[`RUNBOOK.md`](../../RUNBOOK.md) walks the full sequence, contract through backup, against
the local reference platform.

## Checks: freshness, timeliness and quality

A delivery is checked against three independent kinds of rule. **Quality rules** run per
row at validation time, including not-null, uniqueness, accepted-value sets, and the other
rules named in `quality.rules`. **Schedule-boundary timeliness** records whether a delivery
arrived within its declared `schedule` and `schedule_lateness` policy. **Native freshness**
is optional and exists only when a contract declares `maximum_age`. It records how old the
most recently accepted data may be, independently of the delivery schedule. `status`
reports timeliness and freshness separately because either one can fail while the other
still passes.

## Local raw access, retention, and the production boundary

The local reference platform stores raw payloads, state, and the projection database as
plain files under the estate's `runtime/data/`. The local process can read them, and they
remain until an operator removes them or a declared retention policy prunes them. This
keeps an account-free checkout inspectable without a separate access-control layer.

A production deployment does not carry that same openness forward unexamined. Every
runtime binding declares a **protection profile**, and every adapter declares the
protection capabilities it actually holds: encryption at rest, transport encryption,
access-policy binding, audit evidence, retention enforcement, a backup/restore capability
class, and a secret boundary stating where a credential may live. A production activation
checks the declared adapters against the contract and execution plan before accepting
traffic, then records an `InterfaceReadiness` verdict. The contract and binding make the
required encryption, IAM, and retention controls explicit. Each production environment
must choose, operate, and prove those controls. The local platform proves the mechanism,
not a production access-control configuration.

## Orchestration: Composer, Airflow, or another scheduler

Bronze's runtime is orchestrator-neutral. An external orchestrator, Cloud Composer,
Airflow, or another, invokes the same coarse, idempotent commands (`ingest due` to
evaluate what is due, `ingest file` to submit a delivery, `reconcile` to resume blocked
work) on whatever cadence the operator configures. The orchestrator decides when to call;
it holds no delivery state of its own. Every accepted fact, every disposition, every
attempt's outcome lives in Bronze's own configured state store, the sole writer of accepted
progress. Swapping which orchestrator triggers the commands changes nothing about what a
delivery's outcome is, because the orchestrator was never the place that outcome lived.

## Running Bronze on your platform

The runtime reaches every backend through its declared runtime ports, and a runtime
binding names the adapter that fills each port. The local reference platform is one
complete adapter set: SQLite for operational state, DuckDB for the Bronze store and
projections, local files for delivery and raw storage. An estate running on another
platform brings the adapter set for its own components: its scheduler, state database,
warehouse, and policy authority. The contract, binding, and
readiness records stay identical across every set. The packaged conformance runner
checks an adapter implementation against the same suites the reference adapters pass:
plan, state, raw storage, Bronze, projection, publication, and crash recovery, with
protection, policy resolution, and verified backup and restore conformance. Passing
those suites establishes compatibility with the runtime contract. Production suitability
also depends on the environment's security controls, operating model, and recovery proof.

## Where Bronze fits in the generated pipeline

The [architecture overview](README.md#architecture-overview) begins its
walk of the generated pipeline at typed staging. Bronze is the layer that makes typed
staging possible: every generated staging model reads from a Bronze product's `published`
interface. A source declaration carries both its Bronze contract and its domain mapping.
The generated pipeline therefore keeps an exact, machine-checked boundary between a
delivery Bronze has published and a row the rest of the factory resolves, vaults, and
serves.
