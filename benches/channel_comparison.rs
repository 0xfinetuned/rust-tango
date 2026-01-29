//! Benchmark comparing Tango channels with std::sync::mpsc and crossbeam-channel.
//!
//! Run with: cargo bench

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::sync::mpsc;
use std::thread;

use tango::{Consumer, DCache, Fseq, MCache, Producer};

const MSG_COUNT: u64 = 10_000;
const PAYLOAD_SIZE: usize = 64;

// Tango channel configuration
const MCACHE_DEPTH: usize = 16384;
const CHUNK_COUNT: usize = 16384;
const CHUNK_SIZE: usize = 128;

fn bench_spsc_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("spsc_throughput");
    group.throughput(Throughput::Elements(MSG_COUNT));

    let payload = vec![0u8; PAYLOAD_SIZE];

    // Tango channel benchmark
    group.bench_function("tango", |b| {
        b.iter(|| {
            let payload = payload.clone();
            let mcache = MCache::<MCACHE_DEPTH>::new();
            let dcache = DCache::<CHUNK_COUNT, CHUNK_SIZE>::new();
            let fseq = Fseq::new(1);

            thread::scope(|s| {
                let producer = Producer::new(&mcache, &dcache, &fseq);
                let mut consumer = Consumer::new(&mcache, &dcache, 1);

                let sender = s.spawn(move || {
                    for _ in 0..MSG_COUNT {
                        producer.publish(&payload, 0, 0, 0).unwrap();
                    }
                });

                let receiver = s.spawn(move || {
                    let mut received = 0u64;
                    while received < MSG_COUNT {
                        if let Some(fragment) = consumer.poll().unwrap() {
                            black_box(fragment.payload.read());
                            received += 1;
                        } else {
                            std::hint::spin_loop();
                        }
                    }
                });

                sender.join().unwrap();
                receiver.join().unwrap();
            });
        });
    });

    // std::sync::mpsc unbounded
    group.bench_function("std_mpsc_unbounded", |b| {
        b.iter(|| {
            let payload = payload.clone();
            let (tx, rx) = mpsc::channel::<Vec<u8>>();

            thread::scope(|s| {
                let sender = s.spawn(move || {
                    for _ in 0..MSG_COUNT {
                        tx.send(payload.clone()).unwrap();
                    }
                });

                let receiver = s.spawn(move || {
                    for _ in 0..MSG_COUNT {
                        black_box(rx.recv().unwrap());
                    }
                });

                sender.join().unwrap();
                receiver.join().unwrap();
            });
        });
    });

    // std::sync::mpsc bounded (sync_channel)
    group.bench_function("std_mpsc_bounded", |b| {
        b.iter(|| {
            let payload = payload.clone();
            let (tx, rx) = mpsc::sync_channel::<Vec<u8>>(1024);

            thread::scope(|s| {
                let sender = s.spawn(move || {
                    for _ in 0..MSG_COUNT {
                        tx.send(payload.clone()).unwrap();
                    }
                });

                let receiver = s.spawn(move || {
                    for _ in 0..MSG_COUNT {
                        black_box(rx.recv().unwrap());
                    }
                });

                sender.join().unwrap();
                receiver.join().unwrap();
            });
        });
    });

    // crossbeam unbounded
    group.bench_function("crossbeam_unbounded", |b| {
        b.iter(|| {
            let payload = payload.clone();
            let (tx, rx) = crossbeam_channel::unbounded::<Vec<u8>>();

            thread::scope(|s| {
                let sender = s.spawn(move || {
                    for _ in 0..MSG_COUNT {
                        tx.send(payload.clone()).unwrap();
                    }
                });

                let receiver = s.spawn(move || {
                    for _ in 0..MSG_COUNT {
                        black_box(rx.recv().unwrap());
                    }
                });

                sender.join().unwrap();
                receiver.join().unwrap();
            });
        });
    });

    // crossbeam bounded
    group.bench_function("crossbeam_bounded", |b| {
        b.iter(|| {
            let payload = payload.clone();
            let (tx, rx) = crossbeam_channel::bounded::<Vec<u8>>(1024);

            thread::scope(|s| {
                let sender = s.spawn(move || {
                    for _ in 0..MSG_COUNT {
                        tx.send(payload.clone()).unwrap();
                    }
                });

                let receiver = s.spawn(move || {
                    for _ in 0..MSG_COUNT {
                        black_box(rx.recv().unwrap());
                    }
                });

                sender.join().unwrap();
                receiver.join().unwrap();
            });
        });
    });

    group.finish();
}

fn bench_payload_sizes(c: &mut Criterion) {
    let mut group = c.benchmark_group("payload_size_scaling");

    // Use smaller message count for payload scaling test to fit DCache on stack
    // 2048 chunks * 2048 bytes = 4MB (fits on default stack)
    const PAYLOAD_MSG_COUNT: u64 = 2048;

    for size in [8, 64, 256, 1024].iter() {
        group.throughput(Throughput::Bytes((*size as u64) * PAYLOAD_MSG_COUNT));

        group.bench_with_input(BenchmarkId::new("tango", size), size, |b, &size| {
            b.iter(|| {
                let payload = vec![0u8; size];
                let mcache = MCache::<2048>::new();
                let dcache = DCache::<{ PAYLOAD_MSG_COUNT as usize }, 2048>::new();
                let fseq = Fseq::new(1);

                thread::scope(|s| {
                    let producer = Producer::new(&mcache, &dcache, &fseq);
                    let mut consumer = Consumer::new(&mcache, &dcache, 1);

                    let sender = s.spawn(move || {
                        for _ in 0..PAYLOAD_MSG_COUNT {
                            producer.publish(&payload, 0, 0, 0).unwrap();
                        }
                    });

                    let receiver = s.spawn(move || {
                        let mut received = 0u64;
                        while received < PAYLOAD_MSG_COUNT {
                            if let Some(fragment) = consumer.poll().unwrap() {
                                black_box(fragment.payload.read());
                                received += 1;
                            } else {
                                std::hint::spin_loop();
                            }
                        }
                    });

                    sender.join().unwrap();
                    receiver.join().unwrap();
                });
            });
        });

        group.bench_with_input(
            BenchmarkId::new("crossbeam_bounded", size),
            size,
            |b, &size| {
                b.iter(|| {
                    let payload = vec![0u8; size];
                    let (tx, rx) = crossbeam_channel::bounded::<Vec<u8>>(1024);

                    thread::scope(|s| {
                        let sender = s.spawn(move || {
                            for _ in 0..PAYLOAD_MSG_COUNT {
                                tx.send(payload.clone()).unwrap();
                            }
                        });

                        let receiver = s.spawn(move || {
                            for _ in 0..PAYLOAD_MSG_COUNT {
                                black_box(rx.recv().unwrap());
                            }
                        });

                        sender.join().unwrap();
                        receiver.join().unwrap();
                    });
                });
            },
        );
    }

    group.finish();
}

fn bench_single_thread_overhead(c: &mut Criterion) {
    let mut group = c.benchmark_group("single_thread_overhead");
    group.throughput(Throughput::Elements(MSG_COUNT));

    let payload = vec![0u8; PAYLOAD_SIZE];

    // Tango - single thread publish then consume
    group.bench_function("tango", |b| {
        b.iter(|| {
            let mcache = MCache::<MCACHE_DEPTH>::new();
            let dcache = DCache::<CHUNK_COUNT, CHUNK_SIZE>::new();
            let fseq = Fseq::new(1);
            let producer = Producer::new(&mcache, &dcache, &fseq);
            let mut consumer = Consumer::new(&mcache, &dcache, 1);

            for _ in 0..MSG_COUNT {
                producer.publish(&payload, 0, 0, 0).unwrap();
            }

            for _ in 0..MSG_COUNT {
                while let Some(fragment) = consumer.poll().unwrap() {
                    black_box(fragment.payload.read());
                    break;
                }
            }
        });
    });

    // std mpsc - single thread
    group.bench_function("std_mpsc", |b| {
        b.iter(|| {
            let (tx, rx) = mpsc::channel::<Vec<u8>>();

            for _ in 0..MSG_COUNT {
                tx.send(payload.clone()).unwrap();
            }

            for _ in 0..MSG_COUNT {
                black_box(rx.recv().unwrap());
            }
        });
    });

    // crossbeam - single thread
    group.bench_function("crossbeam", |b| {
        b.iter(|| {
            let (tx, rx) = crossbeam_channel::unbounded::<Vec<u8>>();

            for _ in 0..MSG_COUNT {
                tx.send(payload.clone()).unwrap();
            }

            for _ in 0..MSG_COUNT {
                black_box(rx.recv().unwrap());
            }
        });
    });

    group.finish();
}

fn bench_latency(c: &mut Criterion) {
    let mut group = c.benchmark_group("ping_pong_latency");

    let payload = vec![0u8; 8];

    // Tango ping-pong (uses two channel pairs for bidirectional communication)
    group.bench_function("tango", |b| {
        b.iter(|| {
            let payload_a = payload.clone();
            let payload_b = payload.clone();

            let mcache_a = MCache::<1024>::new();
            let dcache_a = DCache::<1024, 64>::new();
            let fseq_a = Fseq::new(1);

            let mcache_b = MCache::<1024>::new();
            let dcache_b = DCache::<1024, 64>::new();
            let fseq_b = Fseq::new(1);

            thread::scope(|s| {
                let producer_a = Producer::new(&mcache_a, &dcache_a, &fseq_a);
                let mut consumer_b = Consumer::new(&mcache_a, &dcache_a, 1);

                let producer_b = Producer::new(&mcache_b, &dcache_b, &fseq_b);
                let mut consumer_a = Consumer::new(&mcache_b, &dcache_b, 1);

                let thread_a = s.spawn(move || {
                    for _ in 0..100 {
                        producer_a.publish(&payload_a, 0, 0, 0).unwrap();
                        while consumer_a.poll().unwrap().is_none() {
                            std::hint::spin_loop();
                        }
                    }
                });

                let thread_b = s.spawn(move || {
                    for _ in 0..100 {
                        while consumer_b.poll().unwrap().is_none() {
                            std::hint::spin_loop();
                        }
                        producer_b.publish(&payload_b, 0, 0, 0).unwrap();
                    }
                });

                thread_a.join().unwrap();
                thread_b.join().unwrap();
            });
        });
    });

    // crossbeam ping-pong
    group.bench_function("crossbeam", |b| {
        b.iter(|| {
            let (tx_a, rx_a) = crossbeam_channel::bounded::<Vec<u8>>(1);
            let (tx_b, rx_b) = crossbeam_channel::bounded::<Vec<u8>>(1);

            let payload_a = payload.clone();
            let payload_b = payload.clone();

            thread::scope(|s| {
                let thread_a = s.spawn(move || {
                    for _ in 0..100 {
                        tx_a.send(payload_a.clone()).unwrap();
                        black_box(rx_b.recv().unwrap());
                    }
                });

                let thread_b = s.spawn(move || {
                    for _ in 0..100 {
                        black_box(rx_a.recv().unwrap());
                        tx_b.send(payload_b.clone()).unwrap();
                    }
                });

                thread_a.join().unwrap();
                thread_b.join().unwrap();
            });
        });
    });

    // std mpsc ping-pong
    group.bench_function("std_mpsc", |b| {
        b.iter(|| {
            let (tx_a, rx_a) = mpsc::sync_channel::<Vec<u8>>(1);
            let (tx_b, rx_b) = mpsc::sync_channel::<Vec<u8>>(1);

            let payload_a = payload.clone();
            let payload_b = payload.clone();

            thread::scope(|s| {
                let thread_a = s.spawn(move || {
                    for _ in 0..100 {
                        tx_a.send(payload_a.clone()).unwrap();
                        black_box(rx_b.recv().unwrap());
                    }
                });

                let thread_b = s.spawn(move || {
                    for _ in 0..100 {
                        black_box(rx_a.recv().unwrap());
                        tx_b.send(payload_b.clone()).unwrap();
                    }
                });

                thread_a.join().unwrap();
                thread_b.join().unwrap();
            });
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_spsc_throughput,
    bench_payload_sizes,
    bench_single_thread_overhead,
    bench_latency,
);
criterion_main!(benches);
