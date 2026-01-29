# tango

A lock-free, high-performance SPSC (single-producer single-consumer) channel inspired by [Firedancer's Tango](https://github.com/firedancer-io/firedancer) IPC subsystem.

## Performance

Benchmarked on Apple M3 Pro, comparing against popular Rust channel implementations:

### SPSC Throughput (10K messages, 64-byte payload)

| Channel | Throughput | Relative |
|---------|-----------|----------|
| **tango** | **28.7 M msg/s** | **1.0x** |
| ringbuf | 10.7 M msg/s | 2.7x slower |
| std bounded | 11.5 M msg/s | 2.5x slower |
| std unbounded | 9.0 M msg/s | 3.2x slower |
| crossbeam bounded | 7.2 M msg/s | 4.0x slower |
| crossbeam unbounded | 6.8 M msg/s | 4.2x slower |

### Ping-Pong Latency (100 round trips, 8-byte payload)

| Channel | Latency | Relative |
|---------|---------|----------|
| **tango** | **88 µs** | **1.0x** |
| ringbuf | 100 µs | 1.1x slower |
| crossbeam | 123 µs | 1.4x slower |
| std | 388 µs | 4.4x slower |

### Large Payload Throughput (2K messages, 1024-byte payload)

| Channel | Throughput | Relative |
|---------|-----------|----------|
| **tango** | **12.6 GiB/s** | **1.0x** |
| ringbuf | 9.2 GiB/s | 1.4x slower |
| crossbeam | 6.6 GiB/s | 1.9x slower |

Run benchmarks yourself:
```bash
RUST_MIN_STACK=67108864 cargo bench
```

## Features

- **Zero-copy reads** - Access message payloads directly without allocation
- **Lock-free** - No mutexes, just atomic operations with careful memory ordering
- **Backpressure** - Optional credit-based flow control prevents overruns
- **Overrun detection** - Consumers detect when they've been lapped by producers
- **Metrics** - Built-in observability for throughput, lag, and errors
- **`no_std` support** - Works in embedded/kernel environments
- **Thoroughly tested** - Unit tests, Miri, Loom, proptest, and fuzz testing

## Quick Start

```rust
use tango::{Consumer, DCache, Fseq, MCache, Producer};

// Create the channel components
let mcache = MCache::<64>::new();      // 64-slot metadata ring buffer
let dcache = DCache::<64, 256>::new(); // 64 chunks of 256 bytes each
let fseq = Fseq::new(1);               // Sequence counter starting at 1

let producer = Producer::new(&mcache, &dcache, &fseq);
let mut consumer = Consumer::new(&mcache, &dcache, 1);

// Publish a message
producer.publish(b"hello", 0, 0, 0).unwrap();

// Consume it (zero-copy)
if let Ok(Some(fragment)) = consumer.poll() {
    assert_eq!(fragment.payload.as_slice(), b"hello");
}
```

## With Flow Control

Use `Fctl` to prevent the producer from overwriting unconsumed messages:

```rust
use tango::{Consumer, DCache, Fctl, Fseq, MCache, Producer};

let mcache = MCache::<64>::new();
let dcache = DCache::<64, 256>::new();
let fseq = Fseq::new(1);
let fctl = Fctl::new(64); // 64 credits = buffer capacity

let producer = Producer::with_flow_control(&mcache, &dcache, &fseq, &fctl);
let mut consumer = Consumer::with_flow_control(&mcache, &dcache, &fctl, 1);

// Producer returns NoCredits error when buffer is full
// Consumer automatically releases credits after consuming
```

## With Metrics

Track throughput, lag, and errors:

```rust
use tango::{Consumer, DCache, Fseq, MCache, Metrics, Producer};

let mcache = MCache::<64>::new();
let dcache = DCache::<64, 256>::new();
let fseq = Fseq::new(1);
let metrics = Metrics::new();

let producer = Producer::new(&mcache, &dcache, &fseq).with_metrics(&metrics);
let mut consumer = Consumer::new(&mcache, &dcache, 1).with_metrics(&metrics);

// ... publish and consume ...

let snapshot = metrics.snapshot();
println!("Published: {}", snapshot.published);
println!("Consumed: {}", snapshot.consumed);
println!("Lag: {} messages", snapshot.lag());
```

## Using the Builder

For ergonomic channel creation:

```rust
use tango::{ChannelBuilder, Producer, Consumer};

let (mcache, dcache, fseq, fctl, metrics) = ChannelBuilder::<64, 64, 256>::new()
    .with_flow_control()
    .with_metrics()
    .build();

let producer = Producer::with_flow_control(&mcache, &dcache, &fseq, fctl.as_ref().unwrap())
    .with_metrics(metrics.as_ref().unwrap());
let mut consumer = Consumer::with_flow_control(&mcache, &dcache, fctl.as_ref().unwrap(), 1)
    .with_metrics(metrics.as_ref().unwrap());
```

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
tango = "0.1"
```

## Feature Flags

| Feature | Default | Description |
|---------|---------|-------------|
| `std` | Yes | Enable standard library support (Vec, Error trait) |
| `loom` | No | Enable Loom testing for concurrency verification |

For `no_std` environments:
```toml
[dependencies]
tango = { version = "0.1", default-features = false }
```

## Architecture

Tango uses a split metadata/data architecture for cache efficiency:

- **MCache**: Ring buffer of 32-byte metadata entries with sequence-based validation
- **DCache**: Fixed-size chunk storage for payloads (cache-line aligned)
- **Fseq**: Atomic sequence counter shared between producer threads
- **Fctl**: Credit counter for backpressure between producer and consumer

The lock-free protocol uses Release/Acquire ordering:
1. Producer writes payload to DCache
2. Producer writes metadata to MCache slot
3. Producer stores sequence number with `Release` ordering
4. Consumer loads sequence with `Acquire`, reads metadata, re-checks sequence

This double-read validation detects overwrites without locks.

## Safety & Testing

Tango is extensively tested:

- **Unit tests**: Core functionality
- **Miri**: Undefined behavior detection
- **Loom**: Exhaustive thread interleaving exploration
- **Proptest**: Property-based testing for invariants
- **Fuzz testing**: Edge case discovery with cargo-fuzz

All `unsafe` blocks are documented with `// SAFETY:` comments explaining the invariants.

## When to Use Tango

**Best for:**
- Single-producer single-consumer scenarios
- Latency-sensitive applications (trading, gaming, audio)
- High-throughput message passing
- When you can dedicate a core to busy-polling

**Consider alternatives if:**
- You need multiple producers or consumers (use crossbeam)
- You can't busy-poll (use async channels)
- Messages are very large (consider shared memory + handles)

## Minimum Supported Rust Version

Rust 1.85 or later (edition 2024).

## License

Licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.
