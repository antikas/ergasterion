# Landing: receiving a source's delivered batch

A source hands your estate a batch: a file drop, an exported snapshot, a stream of change
events landed somewhere you can read. Before any model, any test, or any person downstream
can use a row, the received bytes need permanent evidence. The batch also needs checks
against rules agreed in advance, followed by a recorded publish or quarantine decision.

Ergasterion applies that process through a landing product. A person writes the contract.
The runtime preserves the bytes, applies the contract, and records each outcome before any
later product can read the accepted rows.

## What you can watch it do

The [landing walkthrough](../../demo/landing-ingestion/README.md) runs three local scenarios:
a clean publication, an incomplete snapshot that leaves the earlier snapshot visible, and
a verified backup and restore. It needs no warehouse account or network call.

![The landing boundary preserves a payload and manifest as raw evidence, parses source-native rows, and separates published rows, quarantined rows, and accepted deletion evidence. The product contract governs parsing and validation; the published interface is what the next generation reads.](bronze-ingestion.svg)

The [complete architecture guide](README.md) shows where landing sits in the estate. The
[Landing Product Contract specification](../specifications/landing-product-v1.md) defines
every wire field and points to the frozen interface definition.

## What you declare, and what the runtime applies

A team writes one **Landing Product Contract** per source table: its native schema, how it
delivers (a stream of change events, append-only rows, or a full snapshot), its quality
rules, and the policies that govern it. Writing the contract is the one place human
judgment enters. The runtime then parses each delivery under the declared codec and checks
every rule. It publishes accepted rows and quarantines rejected rows with a locator back
to the exact bytes that failed. No person decides a single delivery's outcome at run
time. The contract decided it when it was written; the runtime only carries out what it
says.

A **runtime binding** declares how one environment runs a contract: which adapter
implements each of the runtime's nine ports, which target relations the projection writes to,
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

Every landing product exposes the same five named surfaces, declared in the contract's
`interfaces` block:

- **`raw`** holds the immutable, exactly-preserved source evidence: the payload bytes
  and their manifest, before any parsing.
- **`source_native`** holds the parsed, quality-annotated records, before the
  accept/reject decision is applied.
- **`published`** holds the accepted, downstream-visible rows. This is the interface the
  rest of the estate reads: a landing product's contract publishes it, and every product
  of the next generation consumes that contract. The physical relation stays behind the
  interface.
- **`quarantine`** holds the rejected records, each with a stable locator back to its raw
  evidence, so a person can inspect exactly what failed and why.
- **`deletion_evidence`** holds accepted deletion facts. A deletion attempt that is never
  accepted never produces a deletion-evidence record.

## Mandatory, optional and forbidden patterns

The landing profile classifies the fifteen patterns as mandatory, optional or forbidden.
Every landing product requires Batch Ingestion, Data Validation, Data Contracts, Lineage
Capture, Metadata Capture, Schema Publish, Data Publish, and Checkpoint and Retries. Batch Transfer
moves an already-landed payload between storage locations without changing it. A managed
integration needs it; a source landed by an external system does not. Schema
Transform, Calculated Fields, Data Enrichment, Data Filtering, Data Aggregation and Data
Curation are forbidden. A landing product publishes the source's own fields after typing
and checks. A later product handles derived values, joins across sources, and filters based
on business rules. That product reads the landing contract.

## Delivery modes and how a deletion is recognised

A contract declares exactly one delivery mode. `cdc` is a stream of change events, each
carrying its own operation; a deletion is recognised through an explicit tombstone field
and value the contract names. `append_only` is new rows only, with no update or delete
semantics of its own. `complete_snapshot` is a full replacement of the source's current
state; the runtime derives what changed, including what was deleted, by comparing the new
snapshot against the prior one under the contract's declared record key.

A complete-snapshot delivery carries one more requirement the other two modes do not: a
**signed attestation** proving the delivery's completeness claim, checked against a
registered verification key before the delivery is accepted. `demo/landing-ingestion/`
walks a complete-snapshot delivery that is source-complete (every row the source meant to
send is present) but acceptance-incomplete (one row fails a declared quality rule), and
shows the prior snapshot staying current while the incomplete delivery is quarantined.

## Managed and external integration

A source's raw data reaches Ergasterion in one of two ways. A **managed** integration means
Ergasterion's connector fetches and preserves the payload. An **external** integration
means another system already landed it. That system supplies a signed receipt with the
location and evidence identifiers. The runtime checks the signature against a registered
key. Both routes produce the same received-batch boundary and five interfaces; only the
system moving the bytes changes.

## The operator command surface

Every landing product is operated through one closed CLI surface, `ergasterion <command>
--project-dir PATH --source NAME --table KEY --binding PATH --environment NAME`, plus each
command's own arguments:

| Command | What it does |
| --- | --- |
| `plan` | Compiles the landing execution graph and the resolved runtime manifest. Read-only. |
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

## What keeps it honest

A delivery is checked against three independent kinds of rule. **Quality rules** run per
row at validation time, including not-null, uniqueness, accepted-value sets, and the other
rules named in `quality.rules`. **Schedule-boundary timeliness** records whether a delivery
arrived within its declared `schedule` and `schedule_lateness` policy. **Native freshness**
is optional and exists only when a contract declares `maximum_age`. It records how old the
most recently accepted data may be, independently of the delivery schedule. `status`
reports timeliness and freshness separately because either one can fail while the other
still passes.

## What this is not: the local security boundary

The local reference platform stores raw payloads, state, and the projection database as
plain files under the estate's `runtime/data/`. The local process can read them, and they
remain until an operator removes them or a declared retention policy prunes them. This
keeps an account-free checkout inspectable without a separate access-control layer.

A production deployment needs its own controls. Every runtime binding declares a
**protection profile**. Each adapter states the protection capabilities it holds, including
encryption, access policy, audit evidence, retention, backup and restore, and credential
boundaries. A production activation checks those capabilities against the contract and
execution plan before accepting traffic. It records the result as an `InterfaceReadiness`
verdict. Each production environment must choose, operate, and prove its controls. The
local platform proves only the mechanism.

## Orchestration: Composer, Airflow, or another scheduler

The landing runtime is orchestrator-neutral. An external orchestrator, Cloud Composer,
Airflow, or another, invokes the same coarse, idempotent commands (`ingest due` to
evaluate what is due, `ingest file` to submit a delivery, `reconcile` to resume blocked
work) on whatever cadence the operator configures. The orchestrator decides when to call;
it holds no delivery state of its own. Every accepted fact, every disposition, every
attempt's outcome lives in the runtime's own configured state store, the sole writer of accepted
progress. Swapping which orchestrator triggers the commands changes nothing about what a
delivery's outcome is, because the orchestrator was never the place that outcome lived.

## Running the landing runtime on your platform

The runtime reaches every backend through its declared runtime ports, and a runtime
binding names the adapter that fills each port. The local reference platform is one
complete adapter set: SQLite for operational state, DuckDB for the landing store and
projections, local files for delivery and raw storage. An estate running on another
platform brings the adapter set for its own components: its scheduler, state database,
warehouse, and policy authority. The contract, binding, and
readiness records stay identical across every set. The packaged conformance runner
checks an adapter implementation against the same suites the reference adapters pass:
plan, state, raw storage, landing, projection, publication, and crash recovery, with
protection, policy resolution, and verified backup and restore conformance. Passing
those suites establishes compatibility with the runtime contract. Production suitability
also depends on the environment's security controls, operating model, and recovery proof.

## Where landing fits in the estate

A landing product declares a composition, passes the landing profile, and publishes one
contract. Its first occurrence is Batch Ingestion. Other profiles begin with Batch
Transfer from an upstream product contract.

Which translator renders it is estate configuration, not a property of the profile. A label
whose translator table gives Batch Ingestion, landing validation, Data Publish and
Checkpoint and Retries to the ingestion runtime is the route this document describes. A
label that gives them to the SQL translator instead materialises the landing relation from
an extract the source system delivered, and reaches the same published contract by a
different road. Changing which one owns a pattern is an estate configuration change with
no engine change.

Either way the estate keeps an exact, machine-checked boundary between what a source
delivered and what a later product may read: the published contract, and nothing else.
