//! Example demonstrating credit-based flow control for backpressure.
//!
//! Run with: cargo run --example flow_control

use std::thread;
use tango::{Consumer, DCache, Fctl, Fseq, MCache, Producer};

fn main() {
    // Create channel components with flow control
    let mcache = MCache::<64>::new();
    let dcache = DCache::<64, 256>::new();
    let fseq = Fseq::new(1);
    let fctl = Fctl::new(8); // Only 8 credits - producer will block after 8 messages

    println!("Starting producer/consumer with flow control (8 credits)");
    println!("Producer will block when buffer is full\n");

    thread::scope(|s| {
        // Consumer thread
        let consumer_handle = s.spawn(|| {
            let mut consumer = Consumer::with_flow_control(&mcache, &dcache, &fctl, 1);
            let mut count = 0;

            while count < 20 {
                match consumer.poll() {
                    Ok(Some(fragment)) => {
                        let payload = std::str::from_utf8(fragment.payload.as_slice())
                            .unwrap_or("<invalid>");
                        println!(
                            "  Consumer: received seq={} \"{}\" (credits available: {})",
                            fragment.meta.seq,
                            payload,
                            fctl.available()
                        );
                        count += 1;

                        // Simulate slow consumer
                        thread::sleep(std::time::Duration::from_millis(50));
                    }
                    Ok(None) => {
                        thread::yield_now();
                    }
                    Err(e) => {
                        eprintln!("Consumer error: {}", e);
                        break;
                    }
                }
            }
            println!("\nConsumer finished: {} messages received", count);
        });

        // Producer thread
        let producer_handle = s.spawn(|| {
            let producer = Producer::with_flow_control(&mcache, &dcache, &fseq, &fctl);

            for i in 0..20 {
                let payload = format!("msg-{:02}", i);

                // This will block when credits are exhausted
                match producer.publish_blocking(payload.as_bytes(), i as u64, 0, 0) {
                    Ok(meta) => {
                        println!(
                            "Producer: published seq={} (credits remaining: {})",
                            meta.seq,
                            fctl.available()
                        );
                    }
                    Err(e) => {
                        eprintln!("Producer error: {}", e);
                        break;
                    }
                }
            }
            println!("\nProducer finished: 20 messages sent");
        });

        producer_handle.join().unwrap();
        consumer_handle.join().unwrap();
    });

    println!("\nFinal credits available: {}", fctl.available());
}
