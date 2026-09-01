# Ergasterion - project context

## Context authority

This file owns runtime-neutral project context. Provider-specific files import it and contain mechanics only.

Read `README.md` and the nearest design or contract document before changing behaviour. Keep each contract in one authoritative place.

## Project boundary

Ergasterion is the public data-product factory. Metadata and configuration drive generated data-product assets, validation, and execution evidence.

DuckDB is the executable reference target. Other targets may provide generated or offline evidence according to their documented support level.

## Change rules

- Preserve metadata as the source for generated assets.
- Keep target capability claims aligned with the evidence each target actually provides.
- Extend shared generation and validation paths before adding target-specific duplication.
- Keep public content free of private paths, operational records, and unpublished source context.
- Use the documented focused verification lane for changed contracts. Instruction-only changes need import, scope, and public-safety checks.

## Publication relationship

The private `data-product-factory` repository is the complete source and build home. This repository is its reviewed public projection, not a separate implementation line.
