# Third-party notices

Ergasterion includes the following vendored schema documents as package data so
contracts can be validated without network access.

## Open Data Contract Standard

- Component: Open Data Contract Standard JSON Schema v3.1.0
- Distributed file: `ergasterion/schemas/odcs-json-schema-v3.1.0.json`
- Upstream: https://github.com/bitol-io/open-data-contract-standard/tree/v3.1.0
- Licence: Apache License 2.0
- Local SHA-256: `4380f28463e8bf5f2721c39aaa03d2392e2a24b2b97dd150b836822d1359e26e`

## Open Data Product Standard

- Component: Open Data Product Standard JSON Schema v1.0.0
- Distributed file: `ergasterion/schemas/odps-json-schema-v1.0.0.json`
- Upstream: https://github.com/bitol-io/open-data-product-standard/tree/v1.0.0
- Licence: Apache License 2.0
- Local SHA-256: `b134f8fe79f2eabd8861f29e770a4c756cc4fa5d0deece5d84baea4e43e03898`

Both upstream projects are maintained by the Bitol community under LF AI & Data.
The complete Apache License 2.0 text is distributed in
`LICENSES/Apache-2.0.txt`. Ergasterion's own source code remains available under
the MIT License in `LICENSE`.

## Pinned runtime dependencies

The dependencies pinned in `pyproject.toml` carry their own licences, distributed with each
package and reproduced here by licence family. The full text of every family in use lives in
`LICENSES/`.

- pydantic 2.13.4 -- MIT. `LICENSES/MIT.txt`.
- duckdb 1.5.5 (the `local-ingestion` and `duckdb` extras) -- MIT. `LICENSES/MIT.txt`.
- rfc8785 0.1.4 -- Apache License 2.0. `LICENSES/Apache-2.0.txt`.
- tzdata 2026.2 -- Apache License 2.0. `LICENSES/Apache-2.0.txt`.
- dbt-core 1.11.12, dbt-duckdb 1.11.0, dbt-snowflake 1.11.6, dbt-bigquery 1.11.3 -- Apache
  License 2.0. `LICENSES/Apache-2.0.txt`.
- cryptography 49.0.0 -- dual-licensed Apache License 2.0 OR BSD 3-Clause; recorded here under
  the BSD 3-Clause option. `LICENSES/BSD-3-Clause.txt`.

## D2 (diagram rendering tool)

- Component: D2, a text-to-diagram language and renderer used to build the diagrams under
  `docs/architecture/`.
- Upstream: https://github.com/terrastruct/d2
- Licence: Mozilla Public License 2.0
- The tool is a build-time executable, fetched from the upstream release and cached locally by
  path. The complete Mozilla Public License 2.0 text is distributed in `LICENSES/MPL-2.0.txt`.
