// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Benchmarks comparing the memory-bounded channel against tokio's mpsc channels.
//!
//! The memory-bounded channel is an unbounded mpsc paired with a semaphore that
//! accounts for the estimated byte size of in-flight messages. These benchmarks
//! quantify the overhead of that accounting relative to:
//!
//! - `tokio::sync::mpsc::unbounded_channel`: the same underlying queue with no
//!   accounting at all (lower bound), and
//! - `tokio::sync::mpsc::channel`: tokio's built-in message-count-bounded
//!   channel (the natural alternative for backpressure).
//!
//! Every message reports the same fixed estimated size, so a byte budget of
//! `capacity * MSG_SIZE` admits exactly as many messages as a bounded channel
//! with `capacity` slots — the three channels apply identical limits.
//!
//! Scenarios:
//! - **uncontended:** a single task alternately fills and drains the channel in
//!   batches that fit within the budget. Measures raw per-message overhead
//!   without blocking or cross-task wakeups.
//! - **pipelined:** producer tasks push through a small-capacity channel while
//!   the receiver drains concurrently. Measures steady-state throughput under
//!   backpressure (the unbounded channel never blocks and serves as baseline).

use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use tokio::runtime::Builder;
use tokio::sync::mpsc;
use tokio::task::JoinSet;

use restate_futures_util::mem_bounded::{self, EstimatedSize};

/// Estimated size reported by every message. All messages are the same size so
/// that byte budgets translate directly into message counts.
const MSG_SIZE: u32 = 256;

struct Msg(u64);

impl EstimatedSize for Msg {
    fn estimated_size(&self) -> u32 {
        MSG_SIZE
    }
}

fn bench_uncontended(c: &mut Criterion) {
    let rt = Builder::new_multi_thread().build().unwrap();

    const N: usize = 10_000;
    const BATCH: usize = 100;

    let mut group = c.benchmark_group("mem_bounded/uncontended");
    group.throughput(Throughput::Elements(N as u64));

    group.bench_function("mem_bounded", |b| {
        b.to_async(&rt).iter(|| async {
            let (tx, mut rx) = mem_bounded::channel::<Msg>(BATCH * MSG_SIZE as usize);
            for _ in 0..N / BATCH {
                for i in 0..BATCH {
                    tx.send(Msg(i as u64)).await.unwrap();
                }
                for _ in 0..BATCH {
                    black_box(rx.recv().await.unwrap().0);
                }
            }
        });
    });

    group.bench_function("tokio_bounded", |b| {
        b.to_async(&rt).iter(|| async {
            let (tx, mut rx) = mpsc::channel::<Msg>(BATCH);
            for _ in 0..N / BATCH {
                for i in 0..BATCH {
                    tx.send(Msg(i as u64)).await.unwrap();
                }
                for _ in 0..BATCH {
                    black_box(rx.recv().await.unwrap().0);
                }
            }
        });
    });

    group.bench_function("tokio_unbounded", |b| {
        b.to_async(&rt).iter(|| async {
            let (tx, mut rx) = mpsc::unbounded_channel::<Msg>();
            for _ in 0..N / BATCH {
                for i in 0..BATCH {
                    tx.send(Msg(i as u64)).unwrap();
                }
                for _ in 0..BATCH {
                    black_box(rx.recv().await.unwrap().0);
                }
            }
        });
    });

    group.finish();
}

fn bench_pipelined(c: &mut Criterion) {
    let rt = Builder::new_multi_thread().build().unwrap();

    const N: u64 = 100_000;
    const CAPACITY: usize = 128;

    let mut group = c.benchmark_group("mem_bounded/pipelined");
    group.throughput(Throughput::Elements(N));

    for producers in [1u64, 4] {
        group.bench_with_input(
            BenchmarkId::new("mem_bounded", producers),
            &producers,
            |b, &producers| {
                b.to_async(&rt).iter(|| async move {
                    let (tx, mut rx) = mem_bounded::channel::<Msg>(CAPACITY * MSG_SIZE as usize);
                    let mut tasks = JoinSet::new();
                    for _ in 0..producers {
                        let tx = tx.clone();
                        tasks.spawn(async move {
                            for i in 0..N / producers {
                                tx.send(Msg(i)).await.unwrap();
                            }
                        });
                    }
                    drop(tx);
                    for _ in 0..N {
                        black_box(rx.recv().await.unwrap().0);
                    }
                    while let Some(v) = tasks.join_next().await {
                        v.unwrap();
                    }
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("tokio_bounded", producers),
            &producers,
            |b, &producers| {
                b.to_async(&rt).iter(|| async move {
                    let (tx, mut rx) = mpsc::channel::<Msg>(CAPACITY);
                    let mut tasks = JoinSet::new();
                    for _ in 0..producers {
                        let tx = tx.clone();
                        tasks.spawn(async move {
                            for i in 0..N / producers {
                                tx.send(Msg(i)).await.unwrap();
                            }
                        });
                    }
                    drop(tx);
                    for _ in 0..N {
                        black_box(rx.recv().await.unwrap().0);
                    }
                    while let Some(v) = tasks.join_next().await {
                        v.unwrap();
                    }
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("tokio_unbounded", producers),
            &producers,
            |b, &producers| {
                b.to_async(&rt).iter(|| async move {
                    let (tx, mut rx) = mpsc::unbounded_channel::<Msg>();
                    let mut tasks = JoinSet::new();
                    for _ in 0..producers {
                        let tx = tx.clone();
                        tasks.spawn(async move {
                            for i in 0..N / producers {
                                tx.send(Msg(i)).unwrap();
                            }
                        });
                    }
                    drop(tx);
                    for _ in 0..N {
                        black_box(rx.recv().await.unwrap().0);
                    }
                    while let Some(v) = tasks.join_next().await {
                        v.unwrap();
                    }
                });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_uncontended, bench_pipelined);
criterion_main!(benches);
