#![no_main]

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use rust_tango::{Consumer, DCache, Fctl, Fseq, MCache, Producer, TangoError};

#[derive(Arbitrary, Debug)]
struct FuzzInput {
    /// Operations: true = publish, false = consume
    operations: Vec<bool>,
    /// Payload data for publish operations
    payloads: Vec<Vec<u8>>,
}

fuzz_target!(|input: FuzzInput| {
    const MCACHE_DEPTH: usize = 16;
    const CHUNK_COUNT: usize = 16;
    const CHUNK_SIZE: usize = 64;

    let mcache = MCache::<MCACHE_DEPTH>::new();
    let dcache = DCache::<CHUNK_COUNT, CHUNK_SIZE>::new();
    let fseq = Fseq::new(1);
    let fctl = Fctl::new(CHUNK_COUNT as u64);

    let producer = Producer::with_flow_control(&mcache, &dcache, &fseq, &fctl);
    let mut consumer = Consumer::with_flow_control(&mcache, &dcache, &fctl, 1);

    let mut publish_idx = 0;
    let mut published_count = 0u64;
    let mut consumed_count = 0u64;

    for op in &input.operations {
        if *op {
            // Publish
            let payload = input.payloads.get(publish_idx).map(|v| v.as_slice()).unwrap_or(&[]);
            publish_idx += 1;

            match producer.publish(payload, 0, 0, 0) {
                Ok(_) => published_count += 1,
                Err(TangoError::NoCredits) => {
                    // Expected when buffer is full
                    assert_eq!(fctl.available(), 0);
                }
                Err(e) => panic!("unexpected error: {:?}", e),
            }
        } else {
            // Consume
            match consumer.poll() {
                Ok(Some(_)) => consumed_count += 1,
                Ok(None) => {
                    // Nothing to consume yet
                    assert!(consumed_count >= published_count || published_count == 0);
                }
                Err(TangoError::Overrun) => {
                    // Should not happen with flow control
                    panic!("overrun with flow control enabled");
                }
                Err(e) => panic!("unexpected error: {:?}", e),
            }
        }
    }

    // Invariant: with flow control, consumed <= published
    assert!(consumed_count <= published_count);

    // Invariant: available credits = initial - (published - consumed)
    let expected_credits = CHUNK_COUNT as u64 - (published_count - consumed_count);
    assert_eq!(fctl.available(), expected_credits);
});
