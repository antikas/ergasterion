# Landing walkthrough

A source delivery needs a clear answer before the warehouse reads it: publish the accepted
rows or hold the delivery for review. This account-free walkthrough shows Ergasterion
making that decision from a written contract.

The [landing architecture](../../docs/architecture/bronze-ingestion.md) explains the
mechanism. The [contract specification](../../docs/specifications/landing-product-v1.md)
defines every field.

## What you can watch it do

1. Publish a clean append-only delivery.
2. Reject an incomplete snapshot while the earlier accepted snapshot stays visible.
3. Back up the complete local runtime, delete it, restore it, and verify the same state.

## Run it

```bash
bash demo/landing-ingestion/run_landing_demo.sh
```

This needs the repository's `.venv` with the `duckdb` extra installed (see
[RUNBOOK.md](../../RUNBOOK.md) section 1). It makes no network call and touches no
warehouse: everything runs against a fresh, temporary local runtime root that this
run creates and deletes. Nothing is written inside this repository checkout.

Run one scenario at a time with an argument:

```bash
bash demo/landing-ingestion/run_landing_demo.sh normal-publication
bash demo/landing-ingestion/run_landing_demo.sh acceptance-incomplete-snapshot
bash demo/landing-ingestion/run_landing_demo.sh backup-restore
```

## Scenario details

**`normal-publication`.** A clean append-only CSV delivery lands. Every row clears
the two declared quality rules (a required transaction id, no duplicate transaction
id) and every row publishes; the quarantine surface stays empty.

**`acceptance-incomplete-snapshot`.** Two signed complete-snapshot deliveries land
for the same product. The first is clean and becomes the current snapshot. The
second carries every row the source meant to send (it is source-complete) but one
row has a missing customer id, which fails the contract's mandatory not-null rule
(it is acceptance-incomplete). Under the contract's `all_or_nothing` publication
mode the whole second delivery is rejected, and a reader querying the product still
sees the first snapshot: a rejected delivery never blends into the one accepted
before it.

**`backup-restore`.** One delivery publishes. The operator's `local-backup` command
copies the complete local runtime root, state, raw evidence and the published
surface together, to a verified location outside both the project root and the
runtime root it copies. The runtime root is then deleted outright and restored from
that backup. The delivery's claim identity, visibility, accepted progress, and
commit/schedule times are read before the deletion and again after the restore and
shown unchanged.

## Reading the output

`landing_demo.py` runs the same closed operator CLI surface an estate uses in
production: `plan`, `contract register/activate`, `deployment register/activate`,
`ingest file`, `status`, `inspect`, `quarantine`, and `local-backup`. Every command
below the narration lines (`-- OK ...`) is the real command Landing exposes; nothing
in this walkthrough is simulated separately from what the CLI actually runs. Each
scenario builds its own tiny, synthetic contract inline in the script; no production
schema, connector configuration or credential is read anywhere in it.
