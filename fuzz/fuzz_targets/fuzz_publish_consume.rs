#![no_main]

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use tango::{Consumer, DCache, Fseq, MCache, Producer};

#[derive(Arbitrary, Debug)]
struct FuzzInput {
    payloads: Vec<Vec<u8>>,
    sigs: Vec<u64>,
}

fuzz_target!(|input: FuzzInput| {
    const MCACHE_DEPTH: usize = 64;
    const CHUNK_COUNT: usize = 64;
    const CHUNK_SIZE: usize = 256;

    let mcache = MCache::<MCACHE_DEPTH>::new();
    let dcache = DCache::<CHUNK_COUNT, CHUNK_SIZE>::new();
    let fseq = Fseq::new(1);

    let producer = Producer::new(&mcache, &dcache, &fseq);
    let mut consumer = Consumer::new(&mcache, &dcache, 1);

    let count = input.payloads.len().min(CHUNK_COUNT);

    // Publish all messages
    for i in 0..count {
        let payload = &input.payloads[i];
        let sig = input.sigs.get(i).copied().unwrap_or(0);
        let _ = producer.publish(payload, sig, 0, i as u32);
    }

    // Consume all messages
    let mut consumed = 0;
    while consumed < count {
        match consumer.poll() {
            Ok(Some(fragment)) => {
                // Verify the payload matches what we sent (truncated to chunk size)
                let expected = &input.payloads[consumed];
                let expected_len = expected.len().min(CHUNK_SIZE);
                assert_eq!(fragment.payload.as_slice(), &expected[..expected_len]);
                consumed += 1;
            }
            Ok(None) => {
                // Should not happen since we published first
                panic!("unexpected NotReady");
            }
            Err(_) => {
                // Overrun - can happen if we exceed mcache depth
                break;
            }
        }
    }
});
