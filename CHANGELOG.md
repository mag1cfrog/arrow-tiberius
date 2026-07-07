# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.0](https://github.com/mag1cfrog/arrow-tiberius/compare/v0.1.6...v0.2.0) - 2026-07-07

### Added

- support SQL Server 2019, 2022, and 2025 profiles with compatibility-level validation
- *(write)* add SQL Server compatibility profiles and profile-bound write planning
- *(write)* expose safe phase, cause, and diagnostic details for write failures

### Fixed

- preserve SQL Server datetime compatibility-level rounding for timestamp writes
- *(ci)* run release-plz with a Rust toolchain compatible with semver checks
- *(write)* allow DirectRawBulk to write non-null Arrow timestamps as SQL Server datetime

## [0.1.6](https://github.com/mag1cfrog/arrow-tiberius/compare/v0.1.5...v0.1.6) - 2026-07-04

### Added

- *(write)* support target-aware timestamp writes for datetime types

## [0.1.5](https://github.com/mag1cfrog/arrow-tiberius/compare/v0.1.4...v0.1.5) - 2026-07-03

### Fixed

- support Arrow view representations for writes

### Other

- gate release-plz publish job
