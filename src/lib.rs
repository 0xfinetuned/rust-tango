//! A lock-free, stack-allocated sketch of Firedancer's Tango IPC subsystem.
//!
//! This crate models the core Tango concepts using fixed-size, stack-friendly
//! structures and explicit atomic synchronization. The implementation is
//! intentionally minimal and self-contained to serve as a learning aid and a
//! building block for higher level systems.
//!
//! ## Architecture overview
//! - **`FragmentMetadata`** is the fixed-size (32-byte) header describing a
//!   payload fragment: sequence number, signature, chunk index, size, control
//!   bits, and timestamp.
//! - **`MCache`** is a ring buffer of fragment metadata. Producers publish by
//!   writing metadata and then atomically storing the sequence number. Consumers
//!   use a double-read validation pattern to detect overwrites without locks.
//! - **`DCache`** is the backing payload store. Producers allocate a fixed-size
//!   chunk, write payload bytes, and hand the chunk index to the `MCache`. The
//!   chunk storage is an `UnsafeCell<[u8; N]>` guarded by the ordering between
//!   the payload write and the `MCache` sequence store.
//! - **`Fseq`** provides a shared sequence counter for producers.
//! - **`Fctl`** provides a lock-free credit counter for backpressure.
//! - **`Tcache`** is a small, lock-free tag cache using an atomic bitset for
//!   deduplication hints.
//! - **`Cnc`** models a command-and-control state machine (BOOT → RUN → HALT).
//! - **`Producer` / `Consumer`** wrap the caches into ergonomic publish and
//!   consume APIs.
//!
//! ## Memory ordering model
//! The lock-free behavior depends on a simple publication rule:
//! 1. A producer writes the payload into `DCache`.
//! 2. The producer writes metadata into the `MCache` slot.
//! 3. The producer stores the sequence number with `Release` ordering.
//! 4. A consumer reads the sequence number with `Acquire` ordering, copies the
//!    metadata, and then re-reads the sequence number to ensure the slot was
//!    not overwritten.
//!
//! This mirrors the typical Tango pattern: speculative reads with validation
//! instead of locks. The `UnsafeCell` usage is safe because consumers only read
//! payloads that are published via the above sequence protocol.
//!
//! ## Usage sketch
//! ```no_run
//! use tango::{Consumer, DCache, Fseq, MCache, Producer};
//!
//! const MCACHE_DEPTH: usize = 1024;
//! const CHUNK_COUNT: usize = 1024;
//! const CHUNK_SIZE: usize = 2048;
//!
//! let mcache = MCache::<MCACHE_DEPTH>::new();
//! let dcache = DCache::<CHUNK_COUNT, CHUNK_SIZE>::new();
//! let fseq = Fseq::new(1);
//! let producer = Producer::new(&mcache, &dcache, &fseq);
//! let mut consumer = Consumer::new(&mcache, &dcache, 1);
//!
//! producer.publish(b"hello", 0xDEAD_BEEF, 0, 0).unwrap();
//! if let Some(fragment) = consumer.poll().unwrap() {
//!     assert_eq!(fragment.payload.read(), b"hello");
//! }
//! ```
use std::cell::UnsafeCell;
use std::fmt;
use std::mem::size_of;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[repr(C, align(16))]
pub struct FragmentMetadata {
    /// Monotonic sequence number used for ordering and overwrite detection.
    pub seq: u64,
    /// Signature or tag used for identification or filtering.
    pub sig: u64,
    /// Index into the data cache for the payload bytes.
    pub chunk: u32,
    /// Payload size in bytes.
    pub size: u32,
    /// Control bits for application-specific signaling.
    pub ctl: u16,
    /// Reserved bits for future expansion.
    pub reserved: u16,
    /// Timestamp or timing metadata.
    pub ts: u32,
}

const _: () = {
    assert!(size_of::<FragmentMetadata>() == 32);
};

#[derive(Debug, thiserror::Error)]
pub enum TangoError {
    #[error("dcache is out of capacity")]
    DcacheFull,
    #[error("chunk index {0} out of range")]
    ChunkOutOfRange(u32),
    #[error("consumer overrun: producer lapped the consumer")]
    Overrun,
    #[error("no credits available for backpressure")]
    NoCredits,
}

/// Result of attempting to read from the MCache.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadResult<T> {
    /// Successfully read the data.
    Ok(T),
    /// The sequence number has not been published yet.
    NotReady,
    /// The consumer was too slow and the slot was overwritten.
    Overrun,
}

/// Metrics for observability and monitoring.
///
/// All counters are atomically updated and can be read from any thread.
/// Use `snapshot()` to get a consistent point-in-time view.
#[derive(Debug)]
pub struct Metrics {
    /// Total messages published.
    published: AtomicU64,
    /// Total messages consumed.
    consumed: AtomicU64,
    /// Total overruns detected (consumer was lapped).
    overruns: AtomicU64,
    /// Total times producer was blocked due to no credits.
    backpressure_events: AtomicU64,
}

/// A point-in-time snapshot of metrics.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MetricsSnapshot {
    /// Total messages published.
    pub published: u64,
    /// Total messages consumed.
    pub consumed: u64,
    /// Total overruns detected.
    pub overruns: u64,
    /// Total backpressure events.
    pub backpressure_events: u64,
}

impl MetricsSnapshot {
    /// Returns the current consumer lag (published - consumed).
    #[inline]
    pub fn lag(&self) -> u64 {
        self.published.saturating_sub(self.consumed)
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

impl Metrics {
    /// Create a new metrics instance with all counters at zero.
    pub fn new() -> Self {
        Self {
            published: AtomicU64::new(0),
            consumed: AtomicU64::new(0),
            overruns: AtomicU64::new(0),
            backpressure_events: AtomicU64::new(0),
        }
    }

    /// Record a published message.
    #[inline]
    pub fn record_publish(&self) {
        self.published.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a consumed message.
    #[inline]
    pub fn record_consume(&self) {
        self.consumed.fetch_add(1, Ordering::Relaxed);
    }

    /// Record an overrun event.
    #[inline]
    pub fn record_overrun(&self) {
        self.overruns.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a backpressure event (producer blocked on credits).
    #[inline]
    pub fn record_backpressure(&self) {
        self.backpressure_events.fetch_add(1, Ordering::Relaxed);
    }

    /// Get a consistent snapshot of all metrics.
    pub fn snapshot(&self) -> MetricsSnapshot {
        // Use Acquire to ensure we see all prior increments
        MetricsSnapshot {
            published: self.published.load(Ordering::Acquire),
            consumed: self.consumed.load(Ordering::Acquire),
            overruns: self.overruns.load(Ordering::Acquire),
            backpressure_events: self.backpressure_events.load(Ordering::Acquire),
        }
    }

    /// Reset all counters to zero.
    pub fn reset(&self) {
        self.published.store(0, Ordering::Release);
        self.consumed.store(0, Ordering::Release);
        self.overruns.store(0, Ordering::Release);
        self.backpressure_events.store(0, Ordering::Release);
    }
}

/// Command-and-control state for coordinating threads.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum CncState {
    Boot = 0,
    Run = 1,
    Halt = 2,
}

impl CncState {
    fn from_u8(value: u8) -> Self {
        match value {
            0 => CncState::Boot,
            1 => CncState::Run,
            _ => CncState::Halt,
        }
    }
}

#[derive(Debug)]
pub struct Cnc {
    state: AtomicU8,
}

impl Default for Cnc {
    fn default() -> Self {
        Self::new()
    }
}

impl Cnc {
    pub fn new() -> Self {
        Self {
            state: AtomicU8::new(CncState::Boot as u8),
        }
    }

    pub fn state(&self) -> CncState {
        CncState::from_u8(self.state.load(Ordering::Acquire))
    }

    pub fn set_state(&self, state: CncState) {
        self.state.store(state as u8, Ordering::Release);
    }
}

#[derive(Debug)]
pub struct Fseq {
    next: AtomicU64,
}

impl Fseq {
    pub fn new(initial: u64) -> Self {
        Self {
            next: AtomicU64::new(initial),
        }
    }

    pub fn next(&self) -> u64 {
        self.next.fetch_add(1, Ordering::AcqRel)
    }

    pub fn current(&self) -> u64 {
        self.next.load(Ordering::Acquire)
    }
}

#[derive(Debug)]
pub struct Fctl {
    credits: AtomicU64,
}

impl Fctl {
    pub fn new(initial: u64) -> Self {
        Self {
            credits: AtomicU64::new(initial),
        }
    }

    pub fn acquire(&self, amount: u64) -> bool {
        let mut current = self.credits.load(Ordering::Acquire);
        loop {
            if current < amount {
                return false;
            }
            match self.credits.compare_exchange(
                current,
                current - amount,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return true,
                Err(next) => current = next,
            }
        }
    }

    pub fn release(&self, amount: u64) {
        self.credits.fetch_add(amount, Ordering::AcqRel);
    }

    pub fn available(&self) -> u64 {
        self.credits.load(Ordering::Acquire)
    }
}

const fn is_power_of_two(value: usize) -> bool {
    value != 0 && (value & (value - 1)) == 0
}

/// Lock-free tag cache using a hashed bitset.
#[derive(Debug)]
pub struct Tcache<const WORDS: usize> {
    bits: [AtomicU64; WORDS],
    mask: u64,
}

impl<const WORDS: usize> Default for Tcache<WORDS> {
    fn default() -> Self {
        Self::new()
    }
}

impl<const WORDS: usize> Tcache<WORDS> {
    const BIT_COUNT: usize = WORDS * 64;
    const ASSERT_POWER_OF_TWO: () = assert!(is_power_of_two(Self::BIT_COUNT));

    /// Create a tag cache backed by a power-of-two number of bits.
    pub fn new() -> Self {
        let _ = Self::ASSERT_POWER_OF_TWO;
        Self {
            bits: std::array::from_fn(|_| AtomicU64::new(0)),
            mask: (Self::BIT_COUNT - 1) as u64,
        }
    }

    pub fn check_and_insert(&self, tag: u64) -> bool {
        let bit = tag.wrapping_mul(0x9E37_79B9_7F4A_7C15) & self.mask;
        let word_idx = (bit / 64) as usize;
        let bit_mask = 1u64 << (bit % 64);
        let prev = self.bits[word_idx].fetch_or(bit_mask, Ordering::AcqRel);
        (prev & bit_mask) == 0
    }

    pub fn len(&self) -> usize {
        self.bits
            .iter()
            .map(|word| word.load(Ordering::Acquire).count_ones() as usize)
            .sum()
    }
}

/// Cache line size for padding to prevent false sharing.
const CACHE_LINE_SIZE: usize = 64;

/// A single entry in the MCache ring buffer.
///
/// Layout is carefully designed to prevent false sharing:
/// - `seq` is on its own cache line (read by consumer, written by producer)
/// - `meta` is on a separate cache line (written by producer, read by consumer)
#[repr(C, align(64))]
struct MCacheEntry {
    /// Sequence number - atomically updated by producer, read by consumer.
    seq: AtomicU64,
    /// Padding to push metadata to a separate cache line.
    _pad: [u8; CACHE_LINE_SIZE - size_of::<AtomicU64>()],
    /// Fragment metadata - written by producer before seq update.
    meta: UnsafeCell<FragmentMetadata>,
}

impl fmt::Debug for MCacheEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MCacheEntry")
            .field("seq", &self.seq.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

// SAFETY: MCacheEntry is Sync because:
// - `seq` is atomic and provides synchronization
// - `meta` is only written before `seq` is updated (Release) and read after
//   `seq` is loaded (Acquire), establishing a happens-before relationship
unsafe impl Sync for MCacheEntry {}

impl MCacheEntry {
    fn new() -> Self {
        Self {
            seq: AtomicU64::new(0),
            _pad: [0u8; CACHE_LINE_SIZE - size_of::<AtomicU64>()],
            meta: UnsafeCell::new(FragmentMetadata::default()),
        }
    }
}

#[derive(Debug)]
pub struct MCache<const DEPTH: usize> {
    mask: u64,
    entries: [MCacheEntry; DEPTH],
    running: AtomicBool,
}

impl<const DEPTH: usize> Default for MCache<DEPTH> {
    fn default() -> Self {
        Self::new()
    }
}

impl<const DEPTH: usize> MCache<DEPTH> {
    const ASSERT_POWER_OF_TWO: () = assert!(is_power_of_two(DEPTH));

    /// Create a ring buffer with a power-of-two number of slots.
    pub fn new() -> Self {
        let _ = Self::ASSERT_POWER_OF_TWO;
        Self {
            mask: (DEPTH - 1) as u64,
            entries: std::array::from_fn(|_| MCacheEntry::new()),
            running: AtomicBool::new(true),
        }
    }

    pub fn stop(&self) {
        self.running.store(false, Ordering::Release);
    }

    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::Acquire)
    }

    /// Publish a fragment's metadata into the ring.
    pub fn publish(&self, meta: FragmentMetadata) {
        let idx = (meta.seq & self.mask) as usize;
        let entry = &self.entries[idx];
        unsafe {
            *entry.meta.get() = meta;
        }
        entry.seq.store(meta.seq, Ordering::Release);
    }

    /// Busy-wait for a specific sequence number to appear.
    ///
    /// Returns:
    /// - `ReadResult::Ok(meta)` if the sequence was successfully read
    /// - `ReadResult::Overrun` if the consumer was lapped by the producer
    /// - `ReadResult::NotReady` if the mcache was stopped before the sequence appeared
    pub fn wait(&self, seq: u64) -> ReadResult<FragmentMetadata> {
        while self.is_running() {
            match self.try_read(seq) {
                ReadResult::Ok(meta) => return ReadResult::Ok(meta),
                ReadResult::Overrun => return ReadResult::Overrun,
                ReadResult::NotReady => std::hint::spin_loop(),
            }
        }
        ReadResult::NotReady
    }

    /// Attempt a lock-free read of the metadata at a specific sequence.
    ///
    /// Returns:
    /// - `ReadResult::Ok(meta)` if the sequence was successfully read
    /// - `ReadResult::NotReady` if the sequence has not been published yet
    /// - `ReadResult::Overrun` if the consumer was lapped by the producer
    pub fn try_read(&self, seq: u64) -> ReadResult<FragmentMetadata> {
        let idx = (seq & self.mask) as usize;
        let entry = &self.entries[idx];

        let seq_before = entry.seq.load(Ordering::Acquire);

        // Not published yet
        if seq_before < seq {
            return ReadResult::NotReady;
        }

        // Slot has been overwritten - consumer was too slow
        if seq_before > seq {
            return ReadResult::Overrun;
        }

        // seq_before == seq: attempt to read
        // SAFETY: The Acquire load above synchronizes with the Release store
        // in publish(), ensuring we see the complete metadata write.
        let meta = unsafe { *entry.meta.get() };

        // Double-check the sequence hasn't changed during our read
        let seq_after = entry.seq.load(Ordering::Acquire);
        if seq_before == seq_after {
            ReadResult::Ok(meta)
        } else {
            // Producer overwrote while we were reading
            ReadResult::Overrun
        }
    }
}

/// A single chunk in the DCache.
#[repr(C, align(64))]
struct DcacheChunk<const CHUNK_SIZE: usize> {
    data: UnsafeCell<[u8; CHUNK_SIZE]>,
}

impl<const CHUNK_SIZE: usize> fmt::Debug for DcacheChunk<CHUNK_SIZE> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DcacheChunk")
            .field("size", &CHUNK_SIZE)
            .finish_non_exhaustive()
    }
}

impl<const CHUNK_SIZE: usize> DcacheChunk<CHUNK_SIZE> {
    fn new() -> Self {
        Self {
            data: UnsafeCell::new([0u8; CHUNK_SIZE]),
        }
    }
}

// SAFETY: DcacheChunk is Sync because access is synchronized through the
// MCache sequence protocol - writes happen before sequence update (Release),
// reads happen after sequence load (Acquire).
unsafe impl<const CHUNK_SIZE: usize> Sync for DcacheChunk<CHUNK_SIZE> {}

#[derive(Debug, Clone, Copy)]
pub struct DcacheView<'a, const CHUNK_SIZE: usize> {
    chunk: &'a DcacheChunk<CHUNK_SIZE>,
    size: usize,
}

impl<'a, const CHUNK_SIZE: usize> DcacheView<'a, CHUNK_SIZE> {
    /// Zero-copy access to the payload bytes.
    ///
    /// # Safety
    /// This is safe because the DcacheView can only be obtained through
    /// Consumer::poll() or Consumer::wait(), which validate via the MCache
    /// sequence protocol that the data has been fully written (Release/Acquire).
    #[inline]
    pub fn as_slice(&self) -> &'a [u8] {
        // SAFETY: The Acquire load in MCache::try_read synchronizes with
        // the Release store in MCache::publish, ensuring the payload write
        // in DCache::write_chunk is visible to us.
        let data = unsafe { &*self.chunk.data.get() };
        &data[..self.size]
    }

    /// Copy the payload into a new Vec.
    ///
    /// Prefer `as_slice()` for zero-copy access when possible.
    pub fn read(&self) -> Vec<u8> {
        self.as_slice().to_vec()
    }

    /// Access the payload through a closure.
    ///
    /// Prefer `as_slice()` for direct zero-copy access.
    pub fn with_reader<T>(&self, f: impl FnOnce(&[u8]) -> T) -> T {
        f(self.as_slice())
    }

    /// Returns the size of the payload in bytes.
    #[inline]
    pub fn len(&self) -> usize {
        self.size
    }

    /// Returns true if the payload is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.size == 0
    }
}

#[derive(Debug)]
pub struct DCache<const CHUNK_COUNT: usize, const CHUNK_SIZE: usize> {
    chunks: [DcacheChunk<CHUNK_SIZE>; CHUNK_COUNT],
    next: AtomicU64,
    mask: u64,
}

const _: () = {
    // DCache CHUNK_COUNT must be a power of two for ring buffer masking
    // This is checked at runtime in new() via the same pattern as MCache
};

impl<const CHUNK_COUNT: usize, const CHUNK_SIZE: usize> Default for DCache<CHUNK_COUNT, CHUNK_SIZE> {
    fn default() -> Self {
        Self::new()
    }
}

impl<const CHUNK_COUNT: usize, const CHUNK_SIZE: usize> DCache<CHUNK_COUNT, CHUNK_SIZE> {
    const ASSERT_POWER_OF_TWO: () = assert!(is_power_of_two(CHUNK_COUNT));

    /// Create a fixed-size cache of payload chunks.
    ///
    /// CHUNK_COUNT must be a power of two.
    pub fn new() -> Self {
        let _ = Self::ASSERT_POWER_OF_TWO;
        Self {
            chunks: std::array::from_fn(|_| DcacheChunk::new()),
            next: AtomicU64::new(0),
            mask: (CHUNK_COUNT - 1) as u64,
        }
    }

    /// Allocate a chunk index for a new payload (ring buffer style).
    ///
    /// This wraps around, so callers must use flow control (Fctl) to ensure
    /// chunks are not overwritten before consumers are done with them.
    pub fn allocate(&self) -> u32 {
        let seq = self.next.fetch_add(1, Ordering::AcqRel);
        (seq & self.mask) as u32
    }

    /// Returns the number of chunks in the cache.
    pub fn capacity(&self) -> usize {
        CHUNK_COUNT
    }

    /// Write payload bytes into a chunk, truncating to the chunk size.
    pub fn write_chunk(&self, chunk: u32, payload: &[u8]) -> Result<usize, TangoError> {
        let idx = chunk as usize;
        let Some(target) = self.chunks.get(idx) else {
            return Err(TangoError::ChunkOutOfRange(chunk));
        };
        let size = payload.len().min(CHUNK_SIZE);
        unsafe {
            let data = &mut *target.data.get();
            data[..size].copy_from_slice(&payload[..size]);
        }
        Ok(size)
    }

    /// Read a view of a chunk with the provided size limit.
    pub fn read_chunk(
        &self,
        chunk: u32,
        size: usize,
    ) -> Result<DcacheView<'_, CHUNK_SIZE>, TangoError> {
        let idx = chunk as usize;
        let Some(target) = self.chunks.get(idx) else {
            return Err(TangoError::ChunkOutOfRange(chunk));
        };
        Ok(DcacheView {
            chunk: target,
            size: size.min(CHUNK_SIZE),
        })
    }

    /// Return the configured chunk size in bytes.
    pub fn chunk_size(&self) -> usize {
        CHUNK_SIZE
    }
}

#[derive(Clone, Copy)]
pub struct Producer<
    'a,
    const MCACHE_DEPTH: usize,
    const CHUNK_COUNT: usize,
    const CHUNK_SIZE: usize,
> {
    mcache: &'a MCache<MCACHE_DEPTH>,
    dcache: &'a DCache<CHUNK_COUNT, CHUNK_SIZE>,
    fseq: &'a Fseq,
    fctl: Option<&'a Fctl>,
    metrics: Option<&'a Metrics>,
}

impl<'a, const MCACHE_DEPTH: usize, const CHUNK_COUNT: usize, const CHUNK_SIZE: usize> fmt::Debug
    for Producer<'a, MCACHE_DEPTH, CHUNK_COUNT, CHUNK_SIZE>
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Producer")
            .field("has_flow_control", &self.fctl.is_some())
            .field("has_metrics", &self.metrics.is_some())
            .finish()
    }
}

impl<'a, const MCACHE_DEPTH: usize, const CHUNK_COUNT: usize, const CHUNK_SIZE: usize>
    Producer<'a, MCACHE_DEPTH, CHUNK_COUNT, CHUNK_SIZE>
{
    /// Create a producer for a shared mcache/dcache pair.
    ///
    /// Without flow control, this producer will freely overwrite chunks.
    /// Use `with_flow_control` for backpressure.
    pub fn new(
        mcache: &'a MCache<MCACHE_DEPTH>,
        dcache: &'a DCache<CHUNK_COUNT, CHUNK_SIZE>,
        fseq: &'a Fseq,
    ) -> Self {
        Self {
            mcache,
            dcache,
            fseq,
            fctl: None,
            metrics: None,
        }
    }

    /// Create a producer with credit-based flow control.
    ///
    /// The producer will acquire a credit before allocating a chunk.
    /// Initialize `fctl` with `CHUNK_COUNT` credits.
    pub fn with_flow_control(
        mcache: &'a MCache<MCACHE_DEPTH>,
        dcache: &'a DCache<CHUNK_COUNT, CHUNK_SIZE>,
        fseq: &'a Fseq,
        fctl: &'a Fctl,
    ) -> Self {
        Self {
            mcache,
            dcache,
            fseq,
            fctl: Some(fctl),
            metrics: None,
        }
    }

    /// Attach metrics tracking to this producer.
    ///
    /// Returns a new producer with the same configuration plus metrics.
    pub fn with_metrics(mut self, metrics: &'a Metrics) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Publish a payload fragment and its metadata.
    ///
    /// If flow control is enabled, returns `TangoError::NoCredits` when
    /// no credits are available (consumer hasn't caught up).
    pub fn publish(
        &self,
        payload: &[u8],
        sig: u64,
        ctl: u16,
        ts: u32,
    ) -> Result<FragmentMetadata, TangoError> {
        // Acquire credit if flow control is enabled
        if let Some(fctl) = self.fctl {
            if !fctl.acquire(1) {
                if let Some(metrics) = self.metrics {
                    metrics.record_backpressure();
                }
                return Err(TangoError::NoCredits);
            }
        }

        let seq = self.fseq.next();
        let chunk = self.dcache.allocate();
        let size = self.dcache.write_chunk(chunk, payload)? as u32;
        let meta = FragmentMetadata {
            seq,
            sig,
            chunk,
            size,
            ctl,
            reserved: 0,
            ts,
        };
        self.mcache.publish(meta);

        if let Some(metrics) = self.metrics {
            metrics.record_publish();
        }

        Ok(meta)
    }

    /// Try to publish, spinning until credits are available or mcache stops.
    ///
    /// Only useful when flow control is enabled.
    pub fn publish_blocking(
        &self,
        payload: &[u8],
        sig: u64,
        ctl: u16,
        ts: u32,
    ) -> Result<FragmentMetadata, TangoError> {
        loop {
            match self.publish(payload, sig, ctl, ts) {
                Ok(meta) => return Ok(meta),
                Err(TangoError::NoCredits) => {
                    if !self.mcache.is_running() {
                        return Err(TangoError::NoCredits);
                    }
                    std::hint::spin_loop();
                }
                Err(e) => return Err(e),
            }
        }
    }
}

#[derive(Debug)]
pub struct Fragment<'a, const CHUNK_SIZE: usize> {
    pub meta: FragmentMetadata,
    pub payload: DcacheView<'a, CHUNK_SIZE>,
}

pub struct Consumer<
    'a,
    const MCACHE_DEPTH: usize,
    const CHUNK_COUNT: usize,
    const CHUNK_SIZE: usize,
> {
    mcache: &'a MCache<MCACHE_DEPTH>,
    dcache: &'a DCache<CHUNK_COUNT, CHUNK_SIZE>,
    fctl: Option<&'a Fctl>,
    metrics: Option<&'a Metrics>,
    next_seq: u64,
}

impl<'a, const MCACHE_DEPTH: usize, const CHUNK_COUNT: usize, const CHUNK_SIZE: usize> fmt::Debug
    for Consumer<'a, MCACHE_DEPTH, CHUNK_COUNT, CHUNK_SIZE>
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Consumer")
            .field("next_seq", &self.next_seq)
            .field("has_flow_control", &self.fctl.is_some())
            .field("has_metrics", &self.metrics.is_some())
            .finish()
    }
}

impl<'a, const MCACHE_DEPTH: usize, const CHUNK_COUNT: usize, const CHUNK_SIZE: usize>
    Consumer<'a, MCACHE_DEPTH, CHUNK_COUNT, CHUNK_SIZE>
{
    /// Create a consumer starting at the given sequence number.
    pub fn new(
        mcache: &'a MCache<MCACHE_DEPTH>,
        dcache: &'a DCache<CHUNK_COUNT, CHUNK_SIZE>,
        initial_seq: u64,
    ) -> Self {
        Self {
            mcache,
            dcache,
            fctl: None,
            metrics: None,
            next_seq: initial_seq,
        }
    }

    /// Create a consumer with credit-based flow control.
    ///
    /// The consumer will release a credit after consuming each fragment.
    /// Use the same `fctl` instance as the producer.
    pub fn with_flow_control(
        mcache: &'a MCache<MCACHE_DEPTH>,
        dcache: &'a DCache<CHUNK_COUNT, CHUNK_SIZE>,
        fctl: &'a Fctl,
        initial_seq: u64,
    ) -> Self {
        Self {
            mcache,
            dcache,
            fctl: Some(fctl),
            metrics: None,
            next_seq: initial_seq,
        }
    }

    /// Attach metrics tracking to this consumer.
    ///
    /// Returns a new consumer with the same configuration plus metrics.
    pub fn with_metrics(mut self, metrics: &'a Metrics) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Poll for the next fragment without blocking.
    ///
    /// Returns:
    /// - `Ok(Some(fragment))` if a fragment was available
    /// - `Ok(None)` if the sequence is not ready yet
    /// - `Err(TangoError::Overrun)` if the consumer was lapped
    pub fn poll(&mut self) -> Result<Option<Fragment<'a, CHUNK_SIZE>>, TangoError> {
        let seq = self.next_seq;
        match self.mcache.try_read(seq) {
            ReadResult::Ok(meta) => {
                let payload = self.dcache.read_chunk(meta.chunk, meta.size as usize)?;
                self.next_seq = seq + 1;

                // Release credit after consuming
                if let Some(fctl) = self.fctl {
                    fctl.release(1);
                }

                if let Some(metrics) = self.metrics {
                    metrics.record_consume();
                }

                Ok(Some(Fragment { meta, payload }))
            }
            ReadResult::NotReady => Ok(None),
            ReadResult::Overrun => {
                if let Some(metrics) = self.metrics {
                    metrics.record_overrun();
                }
                Err(TangoError::Overrun)
            }
        }
    }

    /// Busy-wait for the next fragment.
    ///
    /// Returns:
    /// - `Ok(Some(fragment))` if a fragment was received
    /// - `Ok(None)` if the mcache was stopped
    /// - `Err(TangoError::Overrun)` if the consumer was lapped
    pub fn wait(&mut self) -> Result<Option<Fragment<'a, CHUNK_SIZE>>, TangoError> {
        let seq = self.next_seq;
        match self.mcache.wait(seq) {
            ReadResult::Ok(meta) => {
                let payload = self.dcache.read_chunk(meta.chunk, meta.size as usize)?;
                self.next_seq = seq + 1;

                // Release credit after consuming
                if let Some(fctl) = self.fctl {
                    fctl.release(1);
                }

                if let Some(metrics) = self.metrics {
                    metrics.record_consume();
                }

                Ok(Some(Fragment { meta, payload }))
            }
            ReadResult::NotReady => Ok(None),
            ReadResult::Overrun => {
                if let Some(metrics) = self.metrics {
                    metrics.record_overrun();
                }
                Err(TangoError::Overrun)
            }
        }
    }

    /// Return the next sequence number the consumer expects.
    pub fn next_seq(&self) -> u64 {
        self.next_seq
    }

    /// Manually release credits (useful for batch processing).
    ///
    /// Call this after you're done processing a batch of fragments
    /// if you want to delay credit release for better throughput.
    pub fn release_credits(&self, count: u64) {
        if let Some(fctl) = self.fctl {
            fctl.release(count);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::thread;

    const MCACHE_DEPTH: usize = 8;
    const CHUNK_COUNT: usize = 8;
    const CHUNK_SIZE: usize = 64;

    #[test]
    fn publish_and_consume() {
        let mcache = MCache::<MCACHE_DEPTH>::new();
        let dcache = DCache::<CHUNK_COUNT, CHUNK_SIZE>::new();
        let fseq = Fseq::new(1);
        let producer = Producer::new(&mcache, &dcache, &fseq);
        let mut consumer = Consumer::new(&mcache, &dcache, 1);

        let meta = producer.publish(b"hello", 42, 7, 1234).expect("publish");
        assert_eq!(meta.seq, 1);

        let fragment = consumer.poll().expect("poll").expect("fragment");
        assert_eq!(fragment.meta.sig, 42);
        assert_eq!(fragment.payload.read(), b"hello");
    }

    #[test]
    fn publish_and_consume_with_flow_control() {
        let mcache = MCache::<MCACHE_DEPTH>::new();
        let dcache = DCache::<CHUNK_COUNT, CHUNK_SIZE>::new();
        let fseq = Fseq::new(1);
        let fctl = Fctl::new(CHUNK_COUNT as u64);

        let producer = Producer::with_flow_control(&mcache, &dcache, &fseq, &fctl);
        let mut consumer = Consumer::with_flow_control(&mcache, &dcache, &fctl, 1);

        // Should be able to publish up to CHUNK_COUNT messages
        for i in 0..CHUNK_COUNT {
            producer
                .publish(b"test", i as u64, 0, 0)
                .expect("publish should succeed");
        }

        // Next publish should fail - no credits
        assert!(matches!(
            producer.publish(b"fail", 0, 0, 0),
            Err(TangoError::NoCredits)
        ));

        // Consume one message - releases a credit
        let _ = consumer.poll().expect("poll").expect("fragment");

        // Now we can publish again
        producer
            .publish(b"success", 0, 0, 0)
            .expect("publish should succeed after credit release");
    }

    #[test]
    fn detect_overrun() {
        let mcache = MCache::<4>::new();
        let dcache = DCache::<8, 64>::new();
        let fseq = Fseq::new(1);

        let producer = Producer::new(&mcache, &dcache, &fseq);
        let mut consumer = Consumer::new(&mcache, &dcache, 1);

        // Publish more messages than mcache depth, causing overwrite
        for i in 0..8u64 {
            producer.publish(b"msg", i, 0, 0).expect("publish");
        }

        // Consumer at seq=1 should detect overrun (slot was overwritten)
        assert!(matches!(consumer.poll(), Err(TangoError::Overrun)));
    }

    #[test]
    fn read_result_not_ready() {
        let mcache = MCache::<8>::new();

        // Try to read seq=1 before anything is published
        assert!(matches!(mcache.try_read(1), ReadResult::NotReady));
    }

    #[test]
    fn publish_and_consume_across_threads() {
        let mcache = MCache::<64>::new();
        let dcache = DCache::<64, 64>::new();
        let fseq = Fseq::new(1);
        let producer = Producer::new(&mcache, &dcache, &fseq);
        let consumer = Consumer::new(&mcache, &dcache, 1);
        let received = AtomicUsize::new(0);

        thread::scope(|scope| {
            scope.spawn(|| {
                let mut consumer = consumer;
                while received.load(Ordering::Acquire) < 3 {
                    match consumer.poll() {
                        Ok(Some(fragment)) => {
                            let payload = fragment.payload.read();
                            println!("received: {:?}", String::from_utf8_lossy(&payload));
                            assert!(payload.starts_with(b"msg-"));
                            received.fetch_add(1, Ordering::AcqRel);
                        }
                        Ok(None) => thread::yield_now(),
                        Err(e) => panic!("unexpected error: {}", e),
                    }
                }
            });

            scope.spawn(|| {
                for idx in 0..3u8 {
                    let payload = [b'm', b's', b'g', b'-', b'0' + idx];
                    producer
                        .publish(&payload, 0xAA, 0, idx as u32)
                        .expect("publish");
                }
            });
        });

        assert_eq!(received.load(Ordering::Acquire), 3);
    }

    #[test]
    fn flow_control_across_threads() {
        let mcache = MCache::<64>::new();
        let dcache = DCache::<64, 64>::new();
        let fseq = Fseq::new(1);
        let fctl = Fctl::new(64);

        let producer = Producer::with_flow_control(&mcache, &dcache, &fseq, &fctl);
        let consumer = Consumer::with_flow_control(&mcache, &dcache, &fctl, 1);
        let received = AtomicUsize::new(0);

        thread::scope(|scope| {
            scope.spawn(|| {
                let mut consumer = consumer;
                while received.load(Ordering::Acquire) < 100 {
                    match consumer.poll() {
                        Ok(Some(_)) => {
                            received.fetch_add(1, Ordering::AcqRel);
                        }
                        Ok(None) => thread::yield_now(),
                        Err(e) => panic!("unexpected error: {}", e),
                    }
                }
            });

            scope.spawn(|| {
                for i in 0..100u32 {
                    // Use blocking publish since consumer might be slow
                    producer
                        .publish_blocking(b"test", i as u64, 0, i)
                        .expect("publish");
                }
            });
        });

        assert_eq!(received.load(Ordering::Acquire), 100);
    }

    #[test]
    fn metrics_tracking() {
        let mcache = MCache::<8>::new();
        let dcache = DCache::<16, 64>::new();
        let fseq = Fseq::new(1);
        let fctl = Fctl::new(8);
        let metrics = Metrics::new();

        let producer = Producer::with_flow_control(&mcache, &dcache, &fseq, &fctl)
            .with_metrics(&metrics);
        let mut consumer = Consumer::with_flow_control(&mcache, &dcache, &fctl, 1)
            .with_metrics(&metrics);

        // Publish 5 messages
        for i in 0..5 {
            producer.publish(b"test", i, 0, 0).expect("publish");
        }

        // Consume 3 messages
        for _ in 0..3 {
            consumer.poll().expect("poll").expect("fragment");
        }

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.published, 5);
        assert_eq!(snapshot.consumed, 3);
        assert_eq!(snapshot.lag(), 2);
        assert_eq!(snapshot.overruns, 0);
        assert_eq!(snapshot.backpressure_events, 0);
    }

    #[test]
    fn metrics_backpressure_tracking() {
        let mcache = MCache::<8>::new();
        let dcache = DCache::<8, 64>::new();
        let fseq = Fseq::new(1);
        let fctl = Fctl::new(2); // Only 2 credits
        let metrics = Metrics::new();

        let producer = Producer::with_flow_control(&mcache, &dcache, &fseq, &fctl)
            .with_metrics(&metrics);

        // Publish 2 messages (uses all credits)
        producer.publish(b"1", 1, 0, 0).expect("first");
        producer.publish(b"2", 2, 0, 0).expect("second");

        // Third publish should fail and record backpressure
        assert!(producer.publish(b"3", 3, 0, 0).is_err());

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.published, 2);
        assert_eq!(snapshot.backpressure_events, 1);
    }

    #[test]
    fn zero_copy_read() {
        let mcache = MCache::<8>::new();
        let dcache = DCache::<8, 64>::new();
        let fseq = Fseq::new(1);
        let producer = Producer::new(&mcache, &dcache, &fseq);
        let mut consumer = Consumer::new(&mcache, &dcache, 1);

        producer.publish(b"hello world", 42, 0, 0).expect("publish");

        let fragment = consumer.poll().expect("poll").expect("fragment");

        // Zero-copy access
        let slice = fragment.payload.as_slice();
        assert_eq!(slice, b"hello world");
        assert_eq!(fragment.payload.len(), 11);
        assert!(!fragment.payload.is_empty());
    }
}
