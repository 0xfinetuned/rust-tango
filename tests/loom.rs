//! Loom-based concurrency tests for verifying memory ordering correctness.
//!
//! Run with: cargo test --test loom --features loom --release
//!
//! Loom exhaustively explores all possible thread interleavings to find
//! concurrency bugs that might only manifest under specific schedules.

use loom::sync::Arc;
use loom::thread;

use tango::{Fctl, Fseq, ReadResult};

/// Test sequence counter is correctly incremented across threads.
#[test]
fn loom_fseq_increment() {
    loom::model(|| {
        let fseq = Arc::new(Fseq::new(0));

        let fseq1 = fseq.clone();
        let t1 = thread::spawn(move || fseq1.next());

        let fseq2 = fseq.clone();
        let t2 = thread::spawn(move || fseq2.next());

        let s1 = t1.join().unwrap();
        let s2 = t2.join().unwrap();

        // Both threads got unique sequence numbers
        assert_ne!(s1, s2);
        assert!(s1 < 2 && s2 < 2);
        assert_eq!(fseq.current(), 2);
    });
}

/// Test credit acquire/release is correct under concurrency.
#[test]
fn loom_fctl_acquire_race() {
    loom::model(|| {
        let fctl = Arc::new(Fctl::new(1));

        let fctl1 = fctl.clone();
        let t1 = thread::spawn(move || fctl1.acquire(1));

        let fctl2 = fctl.clone();
        let t2 = thread::spawn(move || fctl2.acquire(1));

        let r1 = t1.join().unwrap();
        let r2 = t2.join().unwrap();

        // Exactly one thread should succeed (only 1 credit available)
        assert!(
            (r1 && !r2) || (!r1 && r2),
            "expected exactly one success, got r1={}, r2={}",
            r1,
            r2
        );
    });
}

/// Test credit release allows subsequent acquire.
#[test]
fn loom_fctl_release_acquire() {
    loom::model(|| {
        let fctl = Arc::new(Fctl::new(0)); // Start with 0 credits

        let fctl1 = fctl.clone();
        let t1 = thread::spawn(move || {
            fctl1.release(1); // Release a credit
        });

        let fctl2 = fctl.clone();
        let t2 = thread::spawn(move || {
            // Try to acquire - may or may not succeed depending on ordering
            fctl2.acquire(1)
        });

        t1.join().unwrap();
        let acquired = t2.join().unwrap();

        // After t1 releases, either t2 got it or it's still available
        if !acquired {
            assert_eq!(fctl.available(), 1);
        } else {
            assert_eq!(fctl.available(), 0);
        }
    });
}

/// Test multiple acquires and releases.
#[test]
fn loom_fctl_multi_credit() {
    loom::model(|| {
        let fctl = Arc::new(Fctl::new(2));

        let fctl1 = fctl.clone();
        let t1 = thread::spawn(move || fctl1.acquire(1));

        let fctl2 = fctl.clone();
        let t2 = thread::spawn(move || fctl2.acquire(1));

        let fctl3 = fctl.clone();
        let t3 = thread::spawn(move || fctl3.acquire(1));

        let r1 = t1.join().unwrap();
        let r2 = t2.join().unwrap();
        let r3 = t3.join().unwrap();

        // Exactly 2 threads should succeed (only 2 credits available)
        let successes = [r1, r2, r3].iter().filter(|&&x| x).count();
        assert_eq!(successes, 2, "expected 2 successes with 2 credits");
    });
}

// Note: Full MCache/DCache/Producer/Consumer tests with loom are complex
// because they require 'static lifetimes. The core synchronization primitives
// (Fseq, Fctl) are tested above. The MCache double-read protocol is tested
// with Miri which catches memory ordering issues.

/// Test that ReadResult enum variants are distinct.
#[test]
fn loom_read_result_variants() {
    loom::model(|| {
        let ok: ReadResult<u32> = ReadResult::Ok(42);
        let not_ready: ReadResult<u32> = ReadResult::NotReady;
        let overrun: ReadResult<u32> = ReadResult::Overrun;

        assert!(matches!(ok, ReadResult::Ok(42)));
        assert!(matches!(not_ready, ReadResult::NotReady));
        assert!(matches!(overrun, ReadResult::Overrun));
    });
}
