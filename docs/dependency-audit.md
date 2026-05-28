# Dependency Audit

Date: 2026-05-28

This document captures the phase 1 dependency audit baseline for
`arrow-tiberius`. Phase 1 is intentionally observational: it records the
current dependency shape, tool output, and follow-up work without changing
runtime behavior.

## Scope

Audited packages:

- `arrow-tiberius`
- workspace `xtask`

Out of scope for this first pass:

- `xtask/arrow-odbc-runner`
- `xtask/odbc-bcp-runner`
- behavioral changes to dependency features
- full source review of the `tiberius-raw-bulk` fork

The excluded runner crates have their own `Cargo.lock` files and should get
separate follow-up checks if they are part of release or CI workflows.

## Commands

The following commands were used for this baseline:

```sh
cargo audit
cargo deny check advisories bans sources
cargo machete
cargo outdated --workspace --root-deps-only
cargo tree -p arrow-tiberius --edges normal --no-default-features
cargo tree -p arrow-tiberius --edges features --no-default-features
cargo tree -p arrow-tiberius --target all --duplicates
cargo tree -p arrow-tiberius --target all --invert winauth
cargo tree -p arrow-tiberius --invert async-native-tls
cargo metadata --format-version 1
```

Installed tool versions:

- `cargo-deny 0.19.7`
- `cargo-audit 0.22.1`
- `cargo-machete 0.9.2`

## Summary

The direct dependency list is small, but `tiberius-raw-bulk` default features
expand the transitive graph significantly. The most important phase 1 finding
is that the default `winauth` feature pulls in `rand 0.7.3`, which has a
RustSec warning, and also contributes to duplicate legacy dependency versions.

The first remediation to test should be making the `tiberius` dependency use
explicit features instead of accepting all defaults.

## Direct Dependencies

Runtime dependencies:

| Dependency | Current requirement | Default features | Notes |
| --- | --- | --- | --- |
| `arrow-array` | `58.2.0` | yes | Enables `chrono-tz`; part of public data model. |
| `arrow-buffer` | `58.2.0` | yes | Arrow buffer types. |
| `arrow-schema` | `58.2.0` | yes | Arrow schema types. |
| `chrono` | `0.4.40` | no | Date/time values. Resolved to `0.4.44`. |
| `futures-util` | `0.3` | no | Uses `io`. |
| `snafu` | `0.9.0` | yes | Error handling. |
| `tiberius` | `=0.12.3-raw-bulk.12` | yes | Renamed from `tiberius-raw-bulk`. Main graph risk. |

Dev dependencies:

| Dependency | Current requirement | Default features | Notes |
| --- | --- | --- | --- |
| `arrow-data` | `58.2.0` | yes | Test support. |
| `tokio` | `1` | yes | Test runtime with `macros`, `net`, `rt`. |
| `tokio-util` | `0.7` | yes | Test compatibility helpers. |

`cargo machete` did not find unused direct dependencies.

## Security

`cargo audit` reported one warning:

| Advisory | Crate | Version | Path | Notes |
| --- | --- | --- | --- | --- |
| `RUSTSEC-2026-0097` | `rand` | `0.7.3` | `tiberius-raw-bulk -> winauth -> rand` | Warning classified as unsound. |

This warning is caused by the `winauth` feature enabled through
`tiberius-raw-bulk` defaults.

`cargo deny check advisories bans sources` completed with advisories, bans, and
sources passing under the default config, but it also emitted duplicate-version
warnings described below.

## Duplicate Versions

`cargo deny` and `cargo tree --target all --duplicates` found notable duplicate
entries:

| Crate | Versions | Main cause |
| --- | --- | --- |
| `bitflags` | `1.3.2`, `2.11.1` | `winauth` uses `bitflags 1`; TLS stack uses `bitflags 2`. |
| `getrandom` | `0.1.16`, `0.2.17`, `0.3.4` | `rand 0.7` via `winauth`, Arrow `ahash`, and TLS/tempfile paths. |
| `wasi` | `0.9.0`, `0.11.1` | Legacy `getrandom 0.1` via `rand 0.7`. |

The duplicates are not all actionable directly, but the legacy `rand 0.7` path
is actionable if Windows authentication is not required by this crate's default
behavior.

## Tiberius Feature Surface

`tiberius-raw-bulk` exposes this default feature set:

```text
default = ["tds73", "winauth", "native-tls"]
```

Feature impact:

- `winauth` pulls `winauth 0.0.4`, `rand 0.7.3`, `md5 0.6.1`, `winapi 0.3.9`,
  and older `getrandom`/`wasi` versions.
- `native-tls` pulls `async-native-tls`, `native-tls`, platform TLS crates,
  OpenSSL-related crates on Unix, `url`, IDNA, and ICU crates.
- `tds73` has no transitive dependencies.

Recommended follow-up:

1. Test `tiberius = { package = "tiberius-raw-bulk", version = "=0.12.3-raw-bulk.12", default-features = false, features = ["tds73", "native-tls"] }`.
2. If TLS can be optional, test a no-TLS default and expose TLS as an
   `arrow-tiberius` feature.
3. If Windows authentication is needed, expose it as an explicit opt-in feature
   instead of inheriting it through defaults.
4. Consider a rustls feature path if the fork supports it cleanly and the target
   users do not require platform-native TLS.

## Outdated Root Dependencies

`cargo outdated --workspace --root-deps-only` reported:

| Package | Dependency | Current | Latest |
| --- | --- | --- | --- |
| `arrow-tiberius` | `arrow-array` | `58.2.0` | `58.3.0` |
| `arrow-tiberius` | `arrow-buffer` | `58.2.0` | `58.3.0` |
| `arrow-tiberius` | `arrow-schema` | `58.2.0` | `58.3.0` |
| `arrow-tiberius` | `arrow-data` | `58.2.0` | `58.3.0` |
| `xtask` | `arrow-array` | `58.2.0` | `58.3.0` |
| `xtask` | `arrow-ipc` | `58.2.0` | `58.3.0` |
| `xtask` | `arrow-schema` | `58.2.0` | `58.3.0` |

These look like normal patch/minor updates in the Arrow family and should be
handled together in one follow-up.

## Maintenance Snapshot

Crates.io metadata checked on 2026-05-28:

| Crate | Latest version | Last crates.io update | Repository |
| --- | --- | --- | --- |
| `arrow-array` | `58.3.0` | `2026-05-11` | `apache/arrow-rs` |
| `arrow-buffer` | `58.3.0` | `2026-05-11` | `apache/arrow-rs` |
| `arrow-schema` | `58.3.0` | `2026-05-11` | `apache/arrow-rs` |
| `arrow-data` | `58.3.0` | `2026-05-11` | `apache/arrow-rs` |
| `chrono` | `0.4.44` | `2026-02-23` | `chronotope/chrono` |
| `futures-util` | `0.3.32` | `2026-02-15` | `rust-lang/futures-rs` |
| `snafu` | `0.9.0` | `2026-03-02` | `shepmaster/snafu` |
| `tiberius-raw-bulk` | `0.12.3-raw-bulk.12` | `2026-05-27` | `mag1cfrog/tiberius-raw-bulk` |
| `async-native-tls` | `0.6.0` | `2026-02-20` | `async-email/async-native-tls` |
| `winauth` | `0.0.5` | `2024-03-22` | `steffengy/winauth-rs` |
| `connection-string` | `0.2.0` | `2023-03-31` | `prisma/connection-string` |
| `pretty-hex` | `0.4.2` | `2026-03-15` | `wolandr/pretty-hex` |
| `native-tls` | `0.2.18` | `2026-02-18` | `rust-native-tls/rust-native-tls` |

The main direct dependencies look actively maintained. The notable maintenance
questions are in transitive dependencies under `tiberius-raw-bulk`, especially
`winauth` and `connection-string`.

## Recommended Follow-ups

1. Add a branch that disables `tiberius` defaults and opts back into only the
   features that are required by `arrow-tiberius`.
2. Add explicit `arrow-tiberius` feature flags for TLS and Windows auth if those
   capabilities are required by users.
3. Update Arrow crates from `58.2.0` to `58.3.0` across the workspace and
   `xtask`.
4. Add a checked-in `deny.toml` and CI job for `cargo deny check`.
5. Run a fork-specific audit in `tiberius-raw-bulk`, focused on whether
   `winauth`, `connection-string`, `pretty-hex`, and TLS dependencies are
   required in default builds.
6. Audit the excluded runner crates separately because they are not workspace
   members and have independent lockfiles.

## Proposed Issue Checklist

- [ ] Confirm required default capabilities for `arrow-tiberius`.
- [ ] Test `tiberius` with `default-features = false`.
- [ ] Decide whether TLS should be default, optional, or split by backend.
- [ ] Decide whether Windows authentication should be opt-in.
- [ ] Add `deny.toml` with license, advisory, duplicate, and source policy.
- [ ] Add CI checks for dependency audit commands.
- [ ] Open linked follow-up issue in `tiberius-raw-bulk` for fork-level
      dependency minimization.
