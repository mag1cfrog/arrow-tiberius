# v0.1 Release Boundary

This document defines the release boundary for `arrow-tiberius` v0.1. It is a
maintainer checklist and decision record, not a user guide.

## Release Goal

The v0.1 release provides a reusable Arrow-to-SQL Server write path through the
Tiberius TDS driver.

The release should make these workflows practical:

- Plan a SQL Server-compatible schema from an Arrow schema.
- Render deterministic SQL Server DDL for that plan.
- Report unsupported Arrow types and policy-dependent mappings as structured
  diagnostics.
- Write Arrow `RecordBatch` values into SQL Server with a selectable writer
  backend.
- Validate the write path against SQL Server 2016 behavior with database
  compatibility level 100.

SQL Server-to-Arrow reads are intentionally outside the v0.1 release boundary.

## Public API Boundary

The v0.1 public API is expected to cover these areas:

- Arrow schema references and field metadata used by planning diagnostics.
- SQL Server profile metadata, including engine version and compatibility
  level.
- SQL Server identifiers, table names, type metadata, columns, and DDL rendering.
- Arrow-to-SQL Server schema planning and mapping outcomes.
- Structured diagnostics for planning and runtime write failures.
- Write options and conversion policies.
- Writer backend selection, including `BaselineTokenRow`, `DirectRawBulk`, and
  `Auto`.
- Bulk writer execution over a compatible `tiberius-raw-bulk` client.

`WriteBackend::Auto` should resolve to the optimized direct raw backend for
v0.1. The baseline writer remains available as a compatibility and reference
path.

## Explicit Non-Goals

These are not part of the v0.1 crate boundary:

- SQL Server-to-Arrow reads.
- Delta Lake integration.
- S3 or other object-store integration.
- CLI runtime, application configuration, secrets, or credential loading.
- Runtime orchestration, job scheduling, retries, or workflow ownership.
- Connection pool ownership.
- Table publish workflows, synonym swaps, or multi-step deployment management.
- Broad SQL Server administration.
- PyO3 or Python packaging.

Applications built on top of this crate can own those concerns. A future Delta
Lake to SQL Server exporter should use this crate for Arrow-to-SQL Server
planning, DDL, diagnostics, and writing, while keeping Delta, object-store,
runtime, configuration, orchestration, and publish semantics in that downstream
application.

## SQL Server Target

The v0.1 behavioral target is SQL Server 2016 with database compatibility level
100. The integration-test harness configures this compatibility level so the
crate does not accidentally rely on newer SQL Server syntax or behavior.

The public profile constructor for this target is
`MssqlProfile::sql_server_2016_compat_100()`.

## Tiberius Fork Package Model

`arrow-tiberius` depends on a forked Tiberius package:

```toml
tiberius = { package = "tiberius-raw-bulk", version = "=0.12.3-raw-bulk.13", default-features = false, features = [
    "tds73",
    "winauth",
    "native-tls",
] }
```

The crate imports that package as `tiberius`, but the Cargo package name is
`tiberius-raw-bulk`. Downstream users should not add upstream `tiberius` as a
separate dependency for clients passed to `BulkWriter`; that creates a distinct
crate type.

The fork is required for v0.1 because the optimized direct raw writer depends on
bulk-load packet APIs that are not exposed by upstream Tiberius. The fork should
stay narrow and track upstream where possible.

Fork maintenance policy for v0.1:

- Pin the fork package exactly in `Cargo.toml`.
- Keep the fork package license and attribution visible. The fork package is
  MIT/Apache-2.0; this crate is Apache-2.0.
- Audit dependency and feature changes before each fork version bump.
- Preserve TLS and Windows authentication feature support unless there is a
  documented release decision to change them.
- Prefer upstreaming the raw bulk APIs when feasible, but do not block v0.1 on
  upstream acceptance.
- Do not depend on both upstream `tiberius` and `tiberius-raw-bulk` in this
  crate.

## Required Features

These capabilities must be present before publishing v0.1:

- Arrow-to-SQL Server planning for the supported scalar Arrow types.
- SQL Server profile, identifier, type metadata, and DDL rendering APIs.
- Structured diagnostics for unsupported types and policy-dependent mappings.
- Runtime write validation for values that cannot be represented in the chosen
  SQL Server target type.
- `BulkWriter` execution through Tiberius.
- `BaselineTokenRow` writer backend.
- `DirectRawBulk` writer backend.
- `Auto` backend selection that resolves to `DirectRawBulk`.
- SQL Server integration-test harness through `cargo xtask sqlserver-test`.
- Writer benchmark harness through `cargo xtask writer-bench`.
- At least one documented SQL Server example before publication.

## Required Documentation

These docs must be current before publishing v0.1:

- `README.md` for user-facing scope, quick start, validation, related crates,
  and project status.
- Crate-level rustdoc for public API orientation.
- `docs/type-mapping.md` for supported mappings, policy-dependent mappings,
  runtime checks, and unsupported Arrow families.
- `docs/integration-tests.md` for SQL Server test setup.
- `docs/benchmarks.md` for writer benchmark setup and interpretation.
- `docs/dependency-audit.md` for dependency audit status.
- This release boundary document.

## Required Tests And Checks

Run these local gates before publishing v0.1:

```bash
cargo fmt --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace
cargo test --doc --all-features
cargo test --examples
cargo doc --no-deps --all-features
RUSTDOCFLAGS='--cfg docsrs' cargo doc --no-deps --all-features
cargo +1.88.0 check --workspace --all-targets --all-features
```

`Cargo.toml` currently declares `rust-version = "1.88"`, so the MSRV gate is
`cargo +1.88.0 check --workspace --all-targets --all-features`. If the MSRV
toolchain is not available on the release runner, document that explicitly in
the release notes and run the check on a machine where it is available.

Run the SQL Server gate on a Docker-compatible runner or an existing SQL Server
instance:

```bash
cargo xtask sqlserver-test
```

The harness should configure the test database to compatibility level 100. See
`docs/integration-tests.md` for environment variables and existing-server
options.

Run examples before publishing:

```bash
cargo test --examples
ARROW_TIBERIUS_EXAMPLE_MSSQL_URL=... cargo run --example sqlserver_batch_write
```

The manual example command should use a real SQL Server connection string. The
example name is intentionally a placeholder until the v0.1 example work is
complete.

Run a benchmark smoke or release-evidence pass:

```bash
cargo xtask writer-bench
```

For performance claims, use release-mode benchmark commands as documented in
`docs/benchmarks.md` and preserve the generated evidence or a summarized result
under `docs/benchmark-results/`.

## Packaging And Publication Gates

Run these package gates on a clean working tree:

```bash
cargo package --list
cargo publish --dry-run
```

Before publishing, verify:

- `README.md` renders correctly on crates.io.
- Crate-level rustdoc renders correctly on docs.rs settings.
- Links from `README.md` to included `docs/` files are present in
  `cargo package --list`.
- Public docs do not link to local-only files under `target/`.
- The package includes release-relevant documentation, benchmark summaries, and
  SQL Server integration-test documentation.
- The package does not include large generated benchmark logs or other
  unnecessary local artifacts.
- The docs.rs metadata in `Cargo.toml` still builds all features with
  `--cfg docsrs`.

## Known Limitations

Known v0.1 limitations:

- The crate writes Arrow data to SQL Server only; it does not read SQL Server
  rows back into Arrow.
- The bulk writer writes to an existing SQL Server table. Callers must create
  the table themselves, for example by running the generated DDL.
- Supported mappings are intentionally scalar. Nested, encoded, and view Arrow
  families are rejected unless a later release adds explicit support.
- Some mappings require caller policy choices, including `UInt64`, `Date64`,
  timezone-aware timestamps, nanosecond timestamp precision, negative decimal
  scales, `Decimal256`, and observed string or binary lengths.
- Runtime value checks can still fail a write when a batch contains values not
  representable by the chosen SQL Server type.
- SQL Server integration tests and examples require access to SQL Server.
- Benchmark results are environment dependent. Published numbers are evidence
  for observed runs, not a guarantee for every deployment.

## Deferred Work

Deferred beyond the v0.1 boundary:

- SQL Server-to-Arrow read APIs.
- Delta Lake to SQL Server exporter crate or application.
- Additional SQL Server examples and example coverage.
- CI coverage for the full release gate matrix.
- Additional SQL Server profile discovery or runtime probing.
- Additional Arrow type families such as nested, dictionary, run-end encoded,
  and view arrays.
- Optional higher-level adapters such as ADBC or Flight SQL layers, if a future
  design needs them.

## Release Decision Checklist

Use this checklist immediately before publishing:

- [ ] README scope, related-crates wording, validation commands, and project
      status are current.
- [ ] Crate-level rustdoc links and examples render cleanly.
- [ ] `docs/type-mapping.md` matches the current planner behavior.
- [ ] `docs/integration-tests.md` matches `cargo xtask sqlserver-test`.
- [ ] `docs/benchmarks.md` matches `cargo xtask writer-bench`.
- [ ] Forked `tiberius-raw-bulk` package version, features, license notes, and
      audit status are current.
- [ ] Required local gates pass.
- [ ] SQL Server integration gate passes.
- [ ] Example compile and manual example run pass.
- [ ] Benchmark smoke or release-evidence run is recorded.
- [ ] `cargo package --list` includes the intended docs and excludes local
      artifacts.
- [ ] `cargo publish --dry-run` passes.
