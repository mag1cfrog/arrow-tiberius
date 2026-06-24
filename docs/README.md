# Documentation

This directory contains maintained user and maintainer documentation for
`arrow-tiberius`.

## User Guides

- [Arrow to SQL Server Type Mapping](type-mapping.md): supported Arrow-to-SQL
  Server mappings, policy-dependent mappings, and writer support.
- [Observability](observability.md): tracing setup, stable spans/events, safe
  fields, and redaction policy.
- [Integration Tests](integration-tests.md): how to run SQL Server integration
  tests with the xtask harness.
- [Writer Benchmarks](benchmarks.md): how to run local writer benchmark commands
  and interpret their output.

## Scope

The documentation intentionally focuses on the current crate surface and
repeatable workflows. Historical design notes, issue work logs, dependency
audit baselines, and one-off local benchmark records are kept out of the
published docs.
