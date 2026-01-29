//! Property-based tests using proptest.
//!
//! Run with: cargo test --test proptest

use proptest::prelude::*;
use tango::{Consumer, DCache, Fctl, Fseq, MCache, Producer, TangoError};

/// Generate arbitrary payloads up to a maximum size.
fn payload_strategy(max_size: usize) -> impl Strategy<Value = Vec<u8>> {
    proptest::collection::vec(any::<u8>(), 0..=max_size)
}

proptest! {
    /// Property: Every published message can be consumed exactly once.
    #[test]
    fn publish_consume_roundtrip(
        payloads in proptest::collection::vec(payload_strategy(64), 1..32)
    ) {
        let mcache = MCache::<64>::new();
        let dcache = DCache::<64, 128>::new();
        let fseq = Fseq::new(1);

        let producer = Producer::new(&mcache, &dcache, &fseq);
        let mut consumer = Consumer::new(&mcache, &dcache, 1);

        // Publish all payloads
        let mut expected = Vec::new();
        for payload in &payloads {
            producer.publish(payload, 0, 0, 0).unwrap();
            // Store truncated payload (what DCache will store)
            let truncated: Vec<u8> = payload.iter().take(128).copied().collect();
            expected.push(truncated);
        }

        // Consume and verify
        for expected_payload in &expected {
            let fragment = consumer.poll().unwrap().expect("should have fragment");
            prop_assert_eq!(fragment.payload.as_slice(), expected_payload.as_slice());
        }

        // No more messages
        prop_assert!(consumer.poll().unwrap().is_none());
    }

    /// Property: With flow control, we never lose messages (no overruns).
    #[test]
    fn flow_control_prevents_overrun(
        count in 1usize..100,
        credits in 1u64..16,
    ) {
        let mcache = MCache::<64>::new();
        let dcache = DCache::<64, 64>::new();
        let fseq = Fseq::new(1);
        let fctl = Fctl::new(credits);

        let producer = Producer::with_flow_control(&mcache, &dcache, &fseq, &fctl);
        let mut consumer = Consumer::with_flow_control(&mcache, &dcache, &fctl, 1);

        let mut published = 0;
        let mut consumed = 0;

        // Interleave publish and consume
        for _ in 0..count {
            // Try to publish
            match producer.publish(b"test", 0, 0, 0) {
                Ok(_) => published += 1,
                Err(TangoError::NoCredits) => {
                    // Expected when buffer is full
                }
                Err(e) => prop_assert!(false, "unexpected error: {:?}", e),
            }

            // Try to consume
            match consumer.poll() {
                Ok(Some(_)) => consumed += 1,
                Ok(None) => {}
                Err(TangoError::Overrun) => {
                    prop_assert!(false, "overrun should not happen with flow control");
                }
                Err(e) => prop_assert!(false, "unexpected error: {:?}", e),
            }
        }

        // Drain remaining
        while let Ok(Some(_)) = consumer.poll() {
            consumed += 1;
        }

        prop_assert_eq!(consumed, published, "all published messages should be consumed");
    }

    /// Property: Sequence numbers are strictly monotonic.
    #[test]
    fn sequence_numbers_monotonic(count in 1usize..64) {
        // Use count < mcache depth to avoid overruns
        let mcache = MCache::<64>::new();
        let dcache = DCache::<64, 64>::new();
        let fseq = Fseq::new(1);
        let fctl = Fctl::new(64);

        let producer = Producer::with_flow_control(&mcache, &dcache, &fseq, &fctl);
        let mut consumer = Consumer::with_flow_control(&mcache, &dcache, &fctl, 1);

        for _ in 0..count {
            producer.publish(b"x", 0, 0, 0).unwrap();
        }

        let mut last_seq = 0u64;
        for _ in 0..count {
            let fragment = consumer.poll().unwrap().expect("should have fragment");
            prop_assert!(fragment.meta.seq > last_seq, "seq should be monotonic");
            last_seq = fragment.meta.seq;
        }
    }

    /// Property: Payload truncation works correctly.
    #[test]
    fn payload_truncation(
        payload in payload_strategy(256),
    ) {
        // Use a fixed chunk size (64 bytes) for this test
        let mcache = MCache::<8>::new();
        let dcache = DCache::<8, 64>::new();
        let fseq = Fseq::new(1);

        let producer = Producer::new(&mcache, &dcache, &fseq);
        let mut consumer = Consumer::new(&mcache, &dcache, 1);

        let meta = producer.publish(&payload, 0, 0, 0).unwrap();

        // Size should be min(payload.len(), CHUNK_SIZE)
        let expected_size = payload.len().min(64);
        prop_assert_eq!(meta.size as usize, expected_size);

        let fragment = consumer.poll().unwrap().expect("should have fragment");
        prop_assert_eq!(fragment.payload.len(), expected_size);

        // Content should match (truncated)
        let expected: Vec<u8> = payload.iter().take(64).copied().collect();
        prop_assert_eq!(fragment.payload.as_slice(), expected.as_slice());
    }

    /// Property: Credits are conserved in flow control.
    #[test]
    fn credits_conserved(
        initial_credits in 1u64..32,
        operations in proptest::collection::vec(any::<bool>(), 1..100),
    ) {
        let fctl = Fctl::new(initial_credits);
        let mcache = MCache::<64>::new();
        let dcache = DCache::<64, 64>::new();
        let fseq = Fseq::new(1);

        let producer = Producer::with_flow_control(&mcache, &dcache, &fseq, &fctl);
        let mut consumer = Consumer::with_flow_control(&mcache, &dcache, &fctl, 1);

        let mut in_flight = 0u64;

        for &is_publish in &operations {
            if is_publish {
                if producer.publish(b"x", 0, 0, 0).is_ok() {
                    in_flight += 1;
                }
            } else if consumer.poll().unwrap().is_some() {
                in_flight -= 1;
            }

            // Invariant: available + in_flight == initial
            let available = fctl.available();
            prop_assert_eq!(
                available + in_flight,
                initial_credits,
                "credits should be conserved"
            );
        }
    }

    /// Property: Signature and metadata are preserved.
    #[test]
    fn metadata_preserved(
        sig in any::<u64>(),
        ctl in any::<u16>(),
        ts in any::<u32>(),
    ) {
        let mcache = MCache::<8>::new();
        let dcache = DCache::<8, 64>::new();
        let fseq = Fseq::new(1);

        let producer = Producer::new(&mcache, &dcache, &fseq);
        let mut consumer = Consumer::new(&mcache, &dcache, 1);

        let meta = producer.publish(b"test", sig, ctl, ts).unwrap();
        prop_assert_eq!(meta.sig, sig);
        prop_assert_eq!(meta.ctl, ctl);
        prop_assert_eq!(meta.ts, ts);

        let fragment = consumer.poll().unwrap().expect("should have fragment");
        prop_assert_eq!(fragment.meta.sig, sig);
        prop_assert_eq!(fragment.meta.ctl, ctl);
        prop_assert_eq!(fragment.meta.ts, ts);
    }
}
