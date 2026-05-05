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

## Public Module Map

The public module layout should separate concepts that may be shared by future
read support from write-only behavior:

```text
arrow_tiberius
├── diagnostic
├── error
├── identifier
├── profile
├── read
└── write
```

Recommended responsibilities:

- `diagnostic`: structured warnings and validation messages shared by planning,
  DDL, write, and future read APIs.
- `error`: crate-level `Error` and `Result` aliases.
- `identifier`: SQL Server identifier and table-name types, including quoting
  behavior.
- `profile`: SQL Server capability/profile types such as SQL Server 2016 with
  database compatibility level 100.
- `write`: v0.1 write planning, DDL rendering, writer options, writer types, and
  backend selection.
- `read`: reserved module for future SQL Server-to-Arrow APIs. It should be
  present only when read design or implementation starts; v0.1 does not need to
  expose an empty module.

Crate-level re-exports should favor the common write flow while keeping module
ownership clear:

```rust
pub use error::{Error, Result};
pub use identifier::TableName;
pub use profile::MssqlProfile;
pub use write::{BulkWriter, PlanOptions, WriteOptions, WritePlan};
```

Implementation-only code should stay under private modules inside `write` or
crate-private modules such as `ddl`, `encode`, or `tds` once implementation
issues need them. Those internal module names should not become public API by
accident.

## SQL Server Profile API

`MssqlProfile` should describe target SQL Server behavior that affects planning
and DDL generation. It is not a connection string, server probe result, or
runtime environment object.

Candidate public shape:

```rust
pub struct MssqlProfile {
    // private fields
}

impl MssqlProfile {
    pub fn sql_server_2016_compat_100() -> Self;
}
```

The initial profile should capture:

- SQL Server engine family: SQL Server 2016.
- Database compatibility level: 100.
- Default Unicode string posture: prefer `nvarchar` for Arrow UTF-8 strings.
- Temporal precision limits: `datetime2` and `datetimeoffset` precision 0
  through 7.
- Decimal limits: precision 1 through 38 and scale 0 through precision.
- Identifier limits: regular and delimited identifier length constraints
  relevant to SQL Server.

Planning implications:

- `MssqlProfile` should be copied into or referenced by `WritePlan` so the plan
  remains stable after construction.
- Profiles should be value objects with no network access.
- Future profiles may add newer SQL Server behavior, but v0.1 should not require
  runtime feature detection.

## Table Name And Identifier API

SQL Server identifiers need explicit handling because table names may be
schema-qualified, user data may contain reserved words or special characters,
and dot handling must not accidentally change object identity.

Candidate public types:

```rust
pub struct Identifier {
    // private fields
}

pub struct TableName {
    // private fields
}
```

Candidate constructors:

```rust
impl Identifier {
    pub fn new(name: impl Into<String>) -> Result<Self>;
    pub fn quoted_sql(&self) -> String;
    pub fn as_str(&self) -> &str;
}

impl TableName {
    pub fn new(schema: impl Into<String>, table: impl Into<String>) -> Result<Self>;
    pub fn unqualified(table: impl Into<String>) -> Result<Self>;
    pub fn quoted_sql(&self) -> String;
}
```

Design decisions:

- `TableName::new("dbo", "target_table")` should mean exactly two identifier
  parts: schema and table.
- Dots inside one identifier string should be treated as literal identifier
  characters and bracket-quoted, not parsed as multipart names.
- Bracket quoting should be the default rendering strategy for generated SQL.
- `]` inside an identifier should be escaped by doubling it when rendered inside
  brackets.
- Empty identifiers should be rejected.
- Over-length identifiers should be rejected during construction or planning
  with a structured diagnostic.
- v0.1 should not accept raw SQL table-name strings in the primary API. A later
  issue may add an unsafe/raw escape hatch if there is a concrete need.

Column names should use the same `Identifier` validation and quoting rules.
The Arrow field name remains the source identity, while the planned SQL column
identifier is the target identity after policy is applied.

## Planning And DDL API

Planning converts an Arrow schema plus SQL Server profile and policy options
into an immutable `WritePlan`. The plan should be reusable across batches with
the same expected schema.

Candidate public shape:

```rust
pub struct WritePlan {
    // private fields
}

pub struct PlannedColumn {
    // private fields
}

pub struct PlanOptions {
    // public builder or private fields with accessors
}

pub struct PlanOutcome {
    pub plan: WritePlan,
    pub diagnostics: Vec<Diagnostic>,
}
```

Candidate constructors and accessors:

```rust
impl WritePlan {
    pub fn from_arrow_schema(
        schema: impl Into<Arc<Schema>>,
        profile: MssqlProfile,
        options: PlanOptions,
    ) -> Result<PlanOutcome>;

    pub fn arrow_schema(&self) -> &Schema;
    pub fn profile(&self) -> &MssqlProfile;
    pub fn columns(&self) -> &[PlannedColumn];
    pub fn create_table_sql(&self, table: &TableName) -> Result<String>;
}
```

Design decisions:

- `WritePlan` should be immutable after construction.
- Planning should preserve Arrow field order as SQL column order unless a future
  issue adds explicit reordering.
- Planning should preserve source Arrow field identity for diagnostics.
- `PlanOutcome` should allow a usable plan with warnings.
- Unsupported Arrow types, invalid identifiers, incompatible precision/scale,
  and lossy conversions without explicit policy should be planning errors.
- DDL rendering should be a method on `WritePlan` or a small `DdlRenderer`
  wrapper over `WritePlan`; v0.1 can start with `WritePlan::create_table_sql`.
- DDL rendering should produce deterministic SQL text for reviewable tests.
- v0.1 should only design `CREATE TABLE` rendering. `DROP`, `MERGE`, `TRUNCATE`,
  migration, and schema-diff APIs are out of scope.

`PlannedColumn` should expose read-only information needed by users and later
writer implementations:

```rust
impl PlannedColumn {
    pub fn source_index(&self) -> usize;
    pub fn source_name(&self) -> &str;
    pub fn target_name(&self) -> &Identifier;
    pub fn nullable(&self) -> bool;
    pub fn sql_type(&self) -> &SqlType;
}
```

`SqlType` can be public if users need to inspect plans or test DDL decisions.
It should describe supported SQL Server target types structurally instead of as
pre-rendered SQL strings, for example `NVarChar(Max)`, `Decimal { precision,
scale }`, or `DateTime2 { precision }`. Exact enum design belongs to the later
implementation issue, but the API boundary should reserve this structural type.

## Conversion Policy API

`PlanOptions` should contain the policy decisions from the evidence matrix.
Defaults should be conservative and lossless. More permissive, lossy, or
semantic-changing behavior should require explicit user choice.

Candidate public shape:

```rust
pub struct PlanOptions {
    pub string_policy: StringPolicy,
    pub binary_policy: BinaryPolicy,
    pub timezone_policy: TimezonePolicy,
    pub nanosecond_policy: NanosecondPolicy,
    pub uint64_policy: UInt64Policy,
    pub decimal_policy: DecimalPolicy,
    pub decimal256_policy: Decimal256Policy,
    pub float_policy: FloatPolicy,
    pub date64_policy: Date64Policy,
}
```

The exact field visibility can be decided during implementation. Public fields
are simple and ergonomic; builder methods allow better forward compatibility.
If public fields are used, `#[non_exhaustive]` should be considered so new
policy fields can be added later without a semver break.

Recommended defaults:

- `StringPolicy::NVarCharMax`
- `BinaryPolicy::VarBinaryMax`
- `TimezonePolicy::Reject`
- `NanosecondPolicy::RejectNon100ns`
- `UInt64Policy::Reject`
- `DecimalPolicy::RejectNegativeScale`
- `Decimal256Policy::CheckedDowncast`
- `FloatPolicy::RejectNonFinite`
- `Date64Policy::RejectNonMidnight`

Policy enum variants should mirror the evidence matrix:

```rust
pub enum StringPolicy {
    NVarCharMax,
    NVarChar(usize),
    ObservedNVarChar,
}

pub enum BinaryPolicy {
    VarBinaryMax,
    VarBinary(usize),
    ObservedVarBinary,
}

pub enum TimezonePolicy {
    Reject,
    DateTimeOffset,
    NormalizeUtcDateTime2,
}

pub enum NanosecondPolicy {
    RejectNon100ns,
    RoundTo100ns,
    TruncateTo100ns,
}

pub enum UInt64Policy {
    Reject,
    Decimal20_0,
    CheckedBigInt,
}

pub enum DecimalPolicy {
    RejectNegativeScale,
    NormalizeNegativeScale,
}

pub enum Decimal256Policy {
    CheckedDowncast,
    Reject,
}

pub enum FloatPolicy {
    RejectNonFinite,
}

pub enum Date64Policy {
    RejectNonMidnight,
    TimestampDateTime2,
}
```

Planning implications:

- Batch-sensitive options such as `ObservedNVarChar` and `ObservedVarBinary`
  need access to observed values, not only schema. They may require a later
  planning API that accepts a sample batch or statistics. Until then, schema-only
  planning should reject these policies or produce a diagnostic explaining that
  observed planning needs data.
- Lossy or semantic-changing options such as timestamp normalization,
  nanosecond rounding/truncation, negative-scale normalization, and `Date64` to
  timestamp remapping should produce warnings naming the selected policy.
- The policy types should live in `write`, not globally, unless future read APIs
  reuse them.

## Deferred Until Later Steps

- Exact warning and error structs.
- Exact stream item and error bounds.
- Exact fork package name, import name, and dependency aliasing.
- Exact feature flags, if any.
