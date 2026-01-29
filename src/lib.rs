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
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU32, AtomicU64, Ordering};

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

#[derive(Debug)]
struct MCacheEntry {
    seq: AtomicU64,
    meta: UnsafeCell<FragmentMetadata>,
}

unsafe impl Sync for MCacheEntry {}

impl MCacheEntry {
    fn new() -> Self {
        Self {
            seq: AtomicU64::new(0),
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
    pub fn wait(&self, seq: u64) -> Option<FragmentMetadata> {
        while self.is_running() {
            if let Some(meta) = self.try_read(seq) {
                return Some(meta);
            }
            std::hint::spin_loop();
        }
        None
    }

    /// Attempt a lock-free read of the metadata at a specific sequence.
    pub fn try_read(&self, seq: u64) -> Option<FragmentMetadata> {
        let idx = (seq & self.mask) as usize;
        let entry = &self.entries[idx];
        let seq_before = entry.seq.load(Ordering::Acquire);
        if seq_before < seq {
            return None;
        }
        if seq_before == seq {
            let meta = unsafe { *entry.meta.get() };
            let seq_after = entry.seq.load(Ordering::Acquire);
            if seq_before == seq_after {
                return Some(meta);
            }
        }
        None
    }
}

#[derive(Debug)]
struct DcacheChunk<const CHUNK_SIZE: usize> {
    data: UnsafeCell<[u8; CHUNK_SIZE]>,
}

impl<const CHUNK_SIZE: usize> DcacheChunk<CHUNK_SIZE> {
    fn new() -> Self {
        Self {
            data: UnsafeCell::new([0u8; CHUNK_SIZE]),
        }
    }
}

unsafe impl<const CHUNK_SIZE: usize> Sync for DcacheChunk<CHUNK_SIZE> {}

#[derive(Debug, Clone, Copy)]
pub struct DcacheView<'a, const CHUNK_SIZE: usize> {
    chunk: &'a DcacheChunk<CHUNK_SIZE>,
    size: usize,
}

impl<'a, const CHUNK_SIZE: usize> DcacheView<'a, CHUNK_SIZE> {
    pub fn read(&self) -> Vec<u8> {
        let data = unsafe { &*self.chunk.data.get() };
        data[..self.size].to_vec()
    }

    pub fn with_reader<T>(&self, f: impl FnOnce(&[u8]) -> T) -> T {
        let data = unsafe { &*self.chunk.data.get() };
        f(&data[..self.size])
    }
}

#[derive(Debug)]
pub struct DCache<const CHUNK_COUNT: usize, const CHUNK_SIZE: usize> {
    chunks: [DcacheChunk<CHUNK_SIZE>; CHUNK_COUNT],
    next: AtomicU32,
}

impl<const CHUNK_COUNT: usize, const CHUNK_SIZE: usize> DCache<CHUNK_COUNT, CHUNK_SIZE> {
    /// Create a fixed-size cache of payload chunks.
    pub fn new() -> Self {
        Self {
            chunks: std::array::from_fn(|_| DcacheChunk::new()),
            next: AtomicU32::new(0),
        }
    }

    /// Allocate a chunk index for a new payload.
    pub fn allocate(&self) -> Result<u32, TangoError> {
        let idx = self.next.fetch_add(1, Ordering::AcqRel) as usize;
        if idx >= CHUNK_COUNT {
            return Err(TangoError::DcacheFull);
        }
        Ok(idx as u32)
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
}

impl<'a, const MCACHE_DEPTH: usize, const CHUNK_COUNT: usize, const CHUNK_SIZE: usize> fmt::Debug
    for Producer<'a, MCACHE_DEPTH, CHUNK_COUNT, CHUNK_SIZE>
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Producer").finish()
    }
}

impl<'a, const MCACHE_DEPTH: usize, const CHUNK_COUNT: usize, const CHUNK_SIZE: usize>
    Producer<'a, MCACHE_DEPTH, CHUNK_COUNT, CHUNK_SIZE>
{
    /// Create a producer for a shared mcache/dcache pair.
    pub fn new(
        mcache: &'a MCache<MCACHE_DEPTH>,
        dcache: &'a DCache<CHUNK_COUNT, CHUNK_SIZE>,
        fseq: &'a Fseq,
    ) -> Self {
        Self {
            mcache,
            dcache,
            fseq,
        }
    }

    /// Publish a payload fragment and its metadata.
    pub fn publish(
        &self,
        payload: &[u8],
        sig: u64,
        ctl: u16,
        ts: u32,
    ) -> Result<FragmentMetadata, TangoError> {
        let seq = self.fseq.next();
        let chunk = self.dcache.allocate()?;
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
        Ok(meta)
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
    next_seq: u64,
}

impl<'a, const MCACHE_DEPTH: usize, const CHUNK_COUNT: usize, const CHUNK_SIZE: usize> fmt::Debug
    for Consumer<'a, MCACHE_DEPTH, CHUNK_COUNT, CHUNK_SIZE>
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Consumer")
            .field("next_seq", &self.next_seq)
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
            next_seq: initial_seq,
        }
    }

    /// Poll for the next fragment without blocking.
    pub fn poll(&mut self) -> Result<Option<Fragment<'a, CHUNK_SIZE>>, TangoError> {
        let seq = self.next_seq;
        let Some(meta) = self.mcache.try_read(seq) else {
            return Ok(None);
        };
        let payload = self.dcache.read_chunk(meta.chunk, meta.size as usize)?;
        self.next_seq = seq + 1;
        Ok(Some(Fragment { meta, payload }))
    }

    /// Busy-wait for the next fragment.
    pub fn wait(&mut self) -> Result<Option<Fragment<'a, CHUNK_SIZE>>, TangoError> {
        let seq = self.next_seq;
        let Some(meta) = self.mcache.wait(seq) else {
            return Ok(None);
        };
        let payload = self.dcache.read_chunk(meta.chunk, meta.size as usize)?;
        self.next_seq = seq + 1;
        Ok(Some(Fragment { meta, payload }))
    }

    /// Return the next sequence number the consumer expects.
    pub fn next_seq(&self) -> u64 {
        self.next_seq
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::thread;

    const MCACHE_DEPTH: usize = 8;
    const CHUNK_COUNT: usize = 4;
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
                    if let Some(fragment) = consumer.poll().expect("poll") {
                        let payload = fragment.payload.read();
                        println!("received: {:?}", String::from_utf8_lossy(&payload));
                        assert!(payload.starts_with(b"msg-"));
                        received.fetch_add(1, Ordering::AcqRel);
                    } else {
                        thread::yield_now();
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
}
