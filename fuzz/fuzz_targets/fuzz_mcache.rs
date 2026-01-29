#![no_main]

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use rust_tango::{FragmentMetadata, MCache, ReadResult};

#[derive(Arbitrary, Debug)]
struct FuzzInput {
    /// Sequence numbers to publish
    publish_seqs: Vec<u64>,
    /// Sequence numbers to read
    read_seqs: Vec<u64>,
}

fuzz_target!(|input: FuzzInput| {
    const DEPTH: usize = 32;
    let mcache = MCache::<DEPTH>::new();

    // Publish metadata entries
    for &seq in &input.publish_seqs {
        // Only publish valid sequence numbers (non-zero, reasonable range)
        if seq > 0 && seq < 1_000_000 {
            let meta = FragmentMetadata {
                seq,
                sig: seq.wrapping_mul(0x123456789ABCDEF0),
                chunk: (seq % DEPTH as u64) as u32,
                size: ((seq % 256) + 1) as u32,
                ctl: (seq % 65536) as u16,
                reserved: 0,
                ts: seq as u32,
            };
            mcache.publish(meta);
        }
    }

    // Try to read various sequence numbers
    for &seq in &input.read_seqs {
        if seq > 0 && seq < 1_000_000 {
            match mcache.try_read(seq) {
                ReadResult::Ok(meta) => {
                    // If we got a result, the sequence must match
                    assert_eq!(meta.seq, seq);
                    // Verify metadata consistency
                    assert_eq!(meta.sig, seq.wrapping_mul(0x123456789ABCDEF0));
                    assert_eq!(meta.chunk, (seq % DEPTH as u64) as u32);
                }
                ReadResult::NotReady => {
                    // Sequence hasn't been published yet
                }
                ReadResult::Overrun => {
                    // Sequence was overwritten - this is valid
                }
            }
        }
    }
});
