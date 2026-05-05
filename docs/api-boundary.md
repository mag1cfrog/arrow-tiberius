# v0.1 Public API Boundary

## Scope

This document designs the public API boundary for `arrow-tiberius` v0.1.
The v0.1 crate writes caller-supplied Arrow batches into SQL Server through a
caller-supplied Tiberius client. The design should keep enough namespace and
conceptual room for future SQL Server-to-Arrow reads without implementing reads
in v0.1.

This is a design document, not an implementation. It should be detailed enough
for later issues to add public types, planner logic, DDL rendering, baseline
writing, and direct raw-bulk encoding without revisiting the crate boundary.

## Goals

- Define public module names and responsibilities.
- Define the intended public type surface for SQL Server profiles, identifiers,
  planning, policies, DDL rendering, writing, diagnostics, and errors.
- Keep the crate identity broader than a writer-only package.
- Make the write path usable with caller-owned Tiberius clients and
  caller-owned Arrow data.
- Reserve future read-side placement without implementing read behavior.
- Document how `arrow-tiberius` should depend on the forked Tiberius package as
  its single SQL Server/TDS driver dependency.

## Non-Goals

- Implementing Arrow-to-SQL Server type mappings.
- Implementing DDL rendering.
- Implementing Tiberius writes.
- Implementing direct TDS bulk encoding.
- Implementing SQL Server-to-Arrow reads.
- Adding Arrow, Tiberius, futures, or runtime dependencies.
- Adding connection pooling, CLI, config, secrets, Delta, S3, scheduling, or
  job-level retry behavior.

## Source Inventory

Primary project sources:

- Issue #4, public API boundary for write path and future read path:
  <https://github.com/mag1cfrog/arrow-tiberius/issues/4>
- Issue #3, evidence matrix for Arrow, SQL Server/TDS, Tiberius, and ODBC:
  <https://github.com/mag1cfrog/arrow-tiberius/issues/3>
- Issue #13, published Tiberius fork package for raw bulk support:
  <https://github.com/mag1cfrog/arrow-tiberius/issues/13>
- Evidence matrix in this repository: `docs/evidence-matrix.md`
- Crate root documentation: `src/lib.rs`
- Workspace/package metadata: `Cargo.toml`

Relevant decisions already captured in project sources:

- `arrow-tiberius` is a reusable library bridge, not a CLI or exporter runtime.
- The caller owns Tiberius client construction, connection lifecycle, optional
  pooling, configuration, secrets, source data acquisition, and job orchestration.
- The crate owns Arrow schema planning for SQL Server, DDL rendering, Arrow batch
  writing, backend selection, diagnostics, and conversion policy.
- v0.1 should not support two incompatible Tiberius client types.
- The forked Tiberius package should expose bulk metadata and raw-row sending
  while keeping Tiberius responsible for connection state, packet framing, auth,
  TLS, metadata querying, packet flushing, and finalization.

## Document Structure

The remaining sections should be filled in small reviewed steps:

1. Public module map.
2. SQL Server profile API.
3. Table name and identifier API.
4. Planning and DDL API.
5. Conversion policy API.
6. Writer API.
7. Batch and stream writing API.
8. Backend selection API.
9. Diagnostics and error API.
10. Write-time schema compatibility.
11. Forked Tiberius dependency strategy.
12. Feature flag posture.
13. Future read-side placement.
14. Deferred decisions.

## Candidate User Flow

The final design should support a flow equivalent to:

```rust
let profile = MssqlProfile::sql_server_2016_compat_100();
let plan = WritePlan::from_arrow_schema(schema, profile, PlanOptions::default())?;
let ddl = plan.create_table_sql(TableName::new("dbo", "target_table"))?;

let mut writer = BulkWriter::new(
    &mut client,
    TableName::new("dbo", "target_table"),
    plan,
    WriteOptions::default(),
)
.await?;

writer.write_batch(&batch).await?;
writer.finish().await?;
```

The design should also decide the shape of a stream convenience function
equivalent to:

```rust
write_record_batch_stream(&mut client, table, plan, stream, options).await?;
```

## Initial Design Constraints

- Prefer a design document over compiled public stubs in this issue.
- Do not add dependencies until an implementation issue needs them.
- Public plans should be immutable after successful construction.
- Planning may return warnings with a usable plan.
- Unsupported mappings and lossy mappings without an explicit policy should be
  planning errors.
- Write-time batch schema checks should be strict by default.
- The intended stream shape should be compatible with `futures_core::Stream`,
  but this issue should not add the dependency.
- Backend selection should include baseline, direct raw-bulk, and automatic
  selection concepts.
- The public API should expose one Tiberius-compatible client type, not both
  upstream and forked client types.
- Future read support should have reserved module placement without v0.1 read
  implementation.

## Deferred Until Later Steps

- Exact public module names.
- Exact type and constructor names.
- Exact warning and error structs.
- Exact identifier quoting API.
- Exact stream item and error bounds.
- Exact fork package name, import name, and dependency aliasing.
- Exact feature flags, if any.
