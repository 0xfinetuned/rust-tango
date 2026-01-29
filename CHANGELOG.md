# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.2] - 2025-01-30

### Changed

- Release workflow now runs CI checks before publishing

### Fixed

- Update cargo-deny config to v2 format
- Remove --allow-dirty from publish

## [0.1.1] - 2025-01-30

### Fixed

- Fixed crate name references in examples and documentation

## [0.1.0] - 2025-01-30

### Added

- Initial release
- Lock-free SPSC channel with zero-copy reads
- `MCache` - Ring buffer for metadata entries with sequence-based validation
- `DCache` - Fixed-size chunk storage for payloads (cache-line aligned)
- `Fseq` - Atomic sequence counter for producers
- `Fctl` - Credit-based flow control for backpressure
- `Metrics` - Built-in observability for throughput, lag, and errors
- `Producer` and `Consumer` APIs with optional flow control and metrics
- `ChannelBuilder` for ergonomic channel creation
- Batch operations: `publish_batch()` and `poll_batch()`
- `no_std` support (disable default `std` feature)
- Comprehensive testing: unit tests, Miri, Loom, proptest, fuzz targets
- Examples: basic usage, flow control, metrics

[Unreleased]: https://github.com/0xfinetuned/rust-tango/compare/v0.1.2...HEAD
[0.1.2]: https://github.com/0xfinetuned/rust-tango/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/0xfinetuned/rust-tango/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/0xfinetuned/rust-tango/releases/tag/v0.1.0
