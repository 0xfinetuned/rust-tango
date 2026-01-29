//! Example demonstrating metrics and observability.
//!
//! Run with: cargo run --example metrics

use std::thread;
use std::time::Duration;
use tango::{Consumer, DCache, Fctl, Fseq, MCache, Metrics, Producer};

fn main() {
    // Create channel with metrics
    let mcache = MCache::<64>::new();
    let dcache = DCache::<64, 256>::new();
    let fseq = Fseq::new(1);
    let fctl = Fctl::new(16);
    let metrics = Metrics::new();

    println!("Starting producer/consumer with metrics tracking\n");

    thread::scope(|s| {
        // Metrics reporter thread
        let metrics_handle = s.spawn(|| {
            for _ in 0..10 {
                thread::sleep(Duration::from_millis(100));
                let snapshot = metrics.snapshot();
                println!(
                    "[Metrics] published={}, consumed={}, lag={}, backpressure={}",
                    snapshot.published,
                    snapshot.consumed,
                    snapshot.lag(),
                    snapshot.backpressure_events
                );
            }
        });

        // Consumer thread
        let consumer_handle = s.spawn(|| {
            let mut consumer =
                Consumer::with_flow_control(&mcache, &dcache, &fctl, 1).with_metrics(&metrics);
            let mut count = 0;

            while count < 50 {
                match consumer.poll() {
                    Ok(Some(_)) => {
                        count += 1;
                        // Variable processing time
                        thread::sleep(Duration::from_millis(10 + (count % 20)));
                    }
                    Ok(None) => thread::yield_now(),
                    Err(_) => break,
                }
            }
        });

        // Producer thread
        let producer_handle = s.spawn(|| {
            let producer =
                Producer::with_flow_control(&mcache, &dcache, &fseq, &fctl).with_metrics(&metrics);

            for i in 0..50 {
                let payload = format!("message-{}", i);
                // Use non-blocking publish to demonstrate backpressure counting
                match producer.publish(payload.as_bytes(), i as u64, 0, 0) {
                    Ok(_) => {}
                    Err(tango::TangoError::NoCredits) => {
                        // Wait and retry
                        thread::sleep(Duration::from_millis(5));
                        let _ = producer.publish_blocking(payload.as_bytes(), i as u64, 0, 0);
                    }
                    Err(e) => {
                        eprintln!("Error: {}", e);
                        break;
                    }
                }
                // Fast producer
                thread::sleep(Duration::from_millis(5));
            }
        });

        producer_handle.join().unwrap();
        consumer_handle.join().unwrap();
        metrics_handle.join().unwrap();
    });

    // Final metrics
    let final_snapshot = metrics.snapshot();
    println!("\n=== Final Metrics ===");
    println!("{}", final_snapshot);
}
