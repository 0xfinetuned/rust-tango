//! Basic example of publishing and consuming messages with tango.
//!
//! Run with: cargo run --example basic

use rust_tango::{Consumer, DCache, Fseq, MCache, Producer};

fn main() {
    // Create channel components
    // - MCache: 64-slot ring buffer for metadata
    // - DCache: 64 chunks of 256 bytes each for payloads
    // - Fseq: Sequence counter starting at 1
    let mcache = MCache::<64>::new();
    let dcache = DCache::<64, 256>::new();
    let fseq = Fseq::new(1);

    // Create producer and consumer
    let producer = Producer::new(&mcache, &dcache, &fseq);
    let mut consumer = Consumer::new(&mcache, &dcache, 1);

    // Publish some messages
    for i in 0..5 {
        let payload = format!("Hello, message {}!", i);
        let meta = producer
            .publish(payload.as_bytes(), i as u64, 0, 0)
            .expect("publish failed");
        println!("Published: seq={}, size={}", meta.seq, meta.size);
    }

    // Consume all messages
    println!("\nConsuming messages:");
    loop {
        match consumer.poll() {
            Ok(Some(fragment)) => {
                let payload = fragment.payload.as_slice();
                let message = std::str::from_utf8(payload).unwrap_or("<invalid utf8>");
                println!(
                    "  Received: seq={}, sig={}, payload=\"{}\"",
                    fragment.meta.seq, fragment.meta.sig, message
                );
            }
            Ok(None) => {
                println!("No more messages available");
                break;
            }
            Err(e) => {
                eprintln!("Error: {}", e);
                break;
            }
        }
    }
}
