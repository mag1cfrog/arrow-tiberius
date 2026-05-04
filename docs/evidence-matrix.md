# Evidence Matrix

This document records source-backed facts that inform `arrow-tiberius` v0.1
schema planning, SQL Server DDL choices, Tiberius write integration, and
benchmark boundaries.

The matrix is intentionally evidence-first. When the sources do not justify a
mapping decision, the entry should stay marked as an open question rather than
becoming an implicit implementation choice.

## Scope

`arrow-tiberius` v0.1 is focused on writing Apache Arrow `RecordBatch` values
to Microsoft SQL Server through Tiberius. The first target profile is SQL
Server 2016 with database compatibility level 100.

This document covers:

- Arrow Rust schema and array model evidence.
- SQL Server type, length, precision, scale, nullability, identifier, and
  compatibility evidence.
- Tiberius public APIs and private bulk-load gaps relevant to baseline and
  direct writers.
- Cargo/crates.io publication constraints for a Tiberius fork package.
- `arrow-odbc` evidence for benchmark and fallback comparison.
- Candidate v0.1 Arrow-to-SQL Server mapping status.

This document does not implement mappings, benchmarks, direct TDS encoding, or
SQL Server integration tests.

## Inspected Versions

Arrow Rust evidence in this document is pinned to version 58.2.0.

Later implementation issues should prefer split Arrow crates such as
`arrow-schema = "58.2.0"` and `arrow-array = "58.2.0"` over the umbrella
`arrow` crate unless the umbrella crate is specifically needed. Cargo caret
semantics may allow compatible 58.x updates, but v0.1 should not attempt to
support multiple Arrow major versions at once because Arrow Rust public types
such as `RecordBatch` are version-specific Rust types.

If evidence from a newer Arrow major is considered later, update this document
or create a follow-up issue before changing implementation dependencies.

## Status Labels

Use one of these labels for each candidate mapping:

- `supported-exact`: expected to be representable without lossy conversion
  under the selected profile.
- `supported-policy-dependent`: viable only after an explicit policy choice,
  such as string length, binary length, timestamp unit, or timezone behavior.
- `lossy-requires-policy`: may lose range, precision, scale, timezone, or
  representation detail and must require explicit policy before implementation.
- `unsupported-v0.1`: should produce diagnostics in v0.1 rather than silently
  map.
- `open-question`: evidence is not sufficient yet to make a planning decision.

## Inspected Sources

### Arrow Rust

Primary sources:

- `arrow` 58.2.0 crate page: <https://docs.rs/crate/arrow/58.2.0>
- `arrow-schema` 58.2.0 crate page: <https://docs.rs/crate/arrow-schema/58.2.0>
- `arrow-array` 58.2.0 crate page: <https://docs.rs/crate/arrow-array/58.2.0>
- `arrow_schema::DataType`: <https://docs.rs/arrow-schema/58.2.0/arrow_schema/datatype/enum.DataType.html>
- `arrow_schema::Field`: <https://docs.rs/arrow-schema/58.2.0/arrow_schema/struct.Field.html>
- `arrow_schema::Schema`: <https://docs.rs/arrow-schema/58.2.0/arrow_schema/struct.Schema.html>
- `arrow_array` crate overview: <https://docs.rs/arrow-array/58.2.0/arrow_array/>
- `arrow_array::RecordBatch`: <https://docs.rs/arrow-array/58.2.0/arrow_array/struct.RecordBatch.html>

Facts to capture in later sections:

- Logical Arrow data type variants relevant to v0.1.
- Field name, data type, nullability, and metadata behavior.
- Schema field ordering and metadata behavior.
- Record batch schema and column-length invariants.
- Array variants needed for primitive, string, binary, decimal, date, and
  timestamp values.

### SQL Server

Primary sources:

- Data types overview: <https://learn.microsoft.com/en-us/sql/t-sql/data-types/data-types-transact-sql>
- Precision, scale, and length: <https://learn.microsoft.com/en-us/sql/t-sql/data-types/precision-scale-and-length-transact-sql>
- `decimal` and `numeric`: <https://learn.microsoft.com/en-us/sql/t-sql/data-types/decimal-and-numeric-transact-sql>
- `char` and `varchar`: <https://learn.microsoft.com/en-us/sql/t-sql/data-types/char-and-varchar-transact-sql>
- `nchar` and `nvarchar`: <https://learn.microsoft.com/en-us/sql/t-sql/data-types/nchar-and-nvarchar-transact-sql>
- `binary` and `varbinary`: <https://learn.microsoft.com/en-us/sql/t-sql/data-types/binary-and-varbinary-transact-sql>
- Date and time types overview: <https://learn.microsoft.com/en-us/sql/t-sql/data-types/date-and-time-types>
- `date`: <https://learn.microsoft.com/en-us/sql/t-sql/data-types/date-transact-sql>
- `time`: <https://learn.microsoft.com/en-us/sql/t-sql/data-types/time-transact-sql>
- `datetime`: <https://learn.microsoft.com/en-us/sql/t-sql/data-types/datetime-transact-sql>
- `datetime2`: <https://learn.microsoft.com/en-us/sql/t-sql/data-types/datetime2-transact-sql>
- `datetimeoffset`: <https://learn.microsoft.com/en-us/sql/t-sql/data-types/datetimeoffset-transact-sql>
- Database compatibility level: <https://learn.microsoft.com/en-us/sql/t-sql/statements/alter-database-transact-sql-compatibility-level>
- Database identifiers: <https://learn.microsoft.com/en-us/sql/relational-databases/databases/database-identifiers>
- Delimited identifiers: <https://learn.microsoft.com/en-us/sql/t-sql/statements/set-quoted-identifier-transact-sql>

Facts to capture in later sections:

- Supported SQL Server v0.1 target types and their exact limits.
- `decimal`/`numeric` precision and scale limits.
- Character and Unicode length semantics.
- Binary length semantics.
- Temporal ranges, precision, and timezone behavior.
- Identifier length, delimiter, and escaping rules.
- Precise meaning of SQL Server 2016 with compatibility level 100.

### Tiberius

Primary sources:

- Tiberius crate docs: <https://docs.rs/tiberius/latest/tiberius/>
- `BulkLoadRequest`: <https://docs.rs/tiberius/latest/tiberius/struct.BulkLoadRequest.html>
- `TokenRow`: <https://docs.rs/tiberius/latest/tiberius/struct.TokenRow.html>
- `ColumnData`: <https://docs.rs/tiberius/latest/tiberius/enum.ColumnData.html>
- `ColumnType`: <https://docs.rs/tiberius/latest/tiberius/enum.ColumnType.html>
- `TypeInfo` source references through docs.rs source links where public docs
  do not expose enough detail.
- Tiberius GitHub repository: <https://github.com/prisma/tiberius>

Facts to capture in later sections:

- Baseline public bulk insert path through `BulkLoadRequest::send(TokenRow)`.
- Finalization behavior and buffering requirements.
- Public value and metadata representations available to external crates.
- Private APIs that block a clean external direct Arrow-to-TDS bulk encoder.
- Upstream issues or PRs relevant to bulk-load metadata, column ordering,
  raw bulk row APIs, and maintenance expectations.

### Cargo And crates.io

Primary sources:

- Cargo dependency specification: <https://doc.rust-lang.org/cargo/reference/specifying-dependencies.html>
- Cargo publishing reference: <https://doc.rust-lang.org/cargo/reference/publishing.html>
- Cargo registries reference: <https://doc.rust-lang.org/cargo/reference/registries.html>
- crates.io package policies: <https://crates.io/policies>

Facts to capture in later sections:

- Whether crates.io packages may depend on Git or path-only dependencies.
- How `version` interacts with `git` or `path` dependencies during local
  development and publication.
- Why a registry-published Tiberius fork package is needed if `arrow-tiberius`
  itself will be publishable.
- Naming, dependency aliasing, and maintenance implications for the fork.

### arrow-odbc

Primary sources:

- `arrow-odbc` crate docs: <https://docs.rs/arrow-odbc/latest/arrow_odbc/>
- `arrow-odbc` package page: <https://docs.rs/crate/arrow-odbc/latest>
- `arrow-odbc` repository: <https://github.com/pacman82/arrow-odbc>

Facts to capture in later sections:

- Existing Arrow read support from ODBC data sources.
- Existing Arrow record batch insertion support into database tables.
- ODBC driver/runtime deployment implications.
- Why `arrow-odbc` is a benchmark and fallback reference rather than the
  initial `arrow-tiberius` runtime dependency.

## Arrow Rust Evidence

TODO: Capture schema, field, record batch, and array facts relevant to v0.1.

## SQL Server Evidence

TODO: Capture SQL Server type, DDL, identifier, and compatibility facts.

## Tiberius Evidence

TODO: Capture baseline bulk-load path, value/type representations, and direct
encoder API gaps.

## Cargo And crates.io Evidence

TODO: Capture publication constraints that affect the forked Tiberius package
decision.

## arrow-odbc Evidence

TODO: Capture existing Arrow read/write support and benchmark boundary.

## Candidate v0.1 Mapping Matrix

| Arrow source type | Candidate SQL Server target | Tiberius representation | Null handling | Constraints | Status | SQL Server 2016 compat-100 note | Test fixture idea | Sources | Open question |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Boolean | TODO | TODO | TODO | TODO | `open-question` | TODO | TODO | TODO | TODO |
| Int8 | TODO | TODO | TODO | TODO | `open-question` | TODO | TODO | TODO | TODO |
| Int16 | TODO | TODO | TODO | TODO | `open-question` | TODO | TODO | TODO | TODO |
| Int32 | TODO | TODO | TODO | TODO | `open-question` | TODO | TODO | TODO | TODO |
| Int64 | TODO | TODO | TODO | TODO | `open-question` | TODO | TODO | TODO | TODO |
| UInt8 | TODO | TODO | TODO | TODO | `open-question` | TODO | TODO | TODO | TODO |
| UInt16 | TODO | TODO | TODO | TODO | `open-question` | TODO | TODO | TODO | TODO |
| UInt32 | TODO | TODO | TODO | TODO | `open-question` | TODO | TODO | TODO | TODO |
| UInt64 | TODO | TODO | TODO | TODO | `open-question` | TODO | TODO | TODO | TODO |
| Float32 | TODO | TODO | TODO | TODO | `open-question` | TODO | TODO | TODO | TODO |
| Float64 | TODO | TODO | TODO | TODO | `open-question` | TODO | TODO | TODO | TODO |
| Utf8 | TODO | TODO | TODO | TODO | `open-question` | TODO | TODO | TODO | TODO |
| LargeUtf8 | TODO | TODO | TODO | TODO | `open-question` | TODO | TODO | TODO | TODO |
| Binary | TODO | TODO | TODO | TODO | `open-question` | TODO | TODO | TODO | TODO |
| LargeBinary | TODO | TODO | TODO | TODO | `open-question` | TODO | TODO | TODO | TODO |
| Decimal128 | TODO | TODO | TODO | TODO | `open-question` | TODO | TODO | TODO | TODO |
| Decimal256 | TODO | TODO | TODO | TODO | `open-question` | TODO | TODO | TODO | TODO |
| Date32 | TODO | TODO | TODO | TODO | `open-question` | TODO | TODO | TODO | TODO |
| Date64 | TODO | TODO | TODO | TODO | `open-question` | TODO | TODO | TODO | TODO |
| Timestamp second | TODO | TODO | TODO | TODO | `open-question` | TODO | TODO | TODO | TODO |
| Timestamp millisecond | TODO | TODO | TODO | TODO | `open-question` | TODO | TODO | TODO | TODO |
| Timestamp microsecond | TODO | TODO | TODO | TODO | `open-question` | TODO | TODO | TODO | TODO |
| Timestamp nanosecond | TODO | TODO | TODO | TODO | `open-question` | TODO | TODO | TODO | TODO |
| List | TODO | TODO | TODO | TODO | `open-question` | TODO | TODO | TODO | TODO |
| Struct | TODO | TODO | TODO | TODO | `open-question` | TODO | TODO | TODO | TODO |
| Map | TODO | TODO | TODO | TODO | `open-question` | TODO | TODO | TODO | TODO |
| Dictionary | TODO | TODO | TODO | TODO | `open-question` | TODO | TODO | TODO | TODO |
| Union | TODO | TODO | TODO | TODO | `open-question` | TODO | TODO | TODO | TODO |

## Open Questions

- Which SQL Server string target should be the default for Arrow `Utf8`:
  `nvarchar(n)`, `nvarchar(max)`, `varchar(n)`, or a policy-selected type?
- Should SQL Server `datetime2` be the default timestamp target for timezone-free
  Arrow timestamps?
- Should timezone-aware Arrow timestamps target `datetimeoffset`, fail by
  default, or require an explicit normalization policy?
- How should Arrow `UInt64` be handled when values exceed SQL Server `bigint`?
- Should dictionary arrays be rejected in v0.1 or decoded according to their
  value type before planning?
- What exact public API should the Tiberius fork expose for raw bulk row bytes?
