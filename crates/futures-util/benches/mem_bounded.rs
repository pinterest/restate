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
//! quantify the overhead of that accounting relative to
//! `tokio::sync::mpsc::channel`, tokio's built-in message-count-bounded
//! channel (the natural alternative for backpressure).
//!
//! Every message reports the same fixed estimated size, so a byte budget of
//! `capacity * MSG_SIZE` admits exactly as many messages as a bounded channel
//! with `capacity` slots — both channels apply identical limits.
//!
//! Scenarios:
//! - **uncontended:** a single task alternately fills and drains the channel in
//!   batches that fit within the budget. Measures raw per-message overhead
//!   without blocking or cross-task wakeups.
//! - **pipelined:** producer tasks push through a small-capacity channel while
//!   the receiver drains concurrently. Measures steady-state throughput under
//!   backpressure.

use std::hint::black_box;
use std::time::{Duration, Instant};

use criterion::{
    BenchmarkId, Criterion, SamplingMode, Throughput, criterion_group, criterion_main,
};
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

    group.finish();
}

fn bench_pipelined(c: &mut Criterion) {
    const N: u64 = 100_000;
    const CAPACITY: usize = 128;
    const MAX_PRODUCERS: usize = 4;

    // A fixed worker count (producers + receiver) keeps task placement and
    // work-stealing behavior comparable across machines and runs; the default
    // (one worker per core) makes contention patterns depend on the host.
    //
    // Each sample gets a fresh runtime (built outside the timed region): where
    // the OS places the worker threads (SMT siblings vs. separate cores)
    // dominates cross-thread wakeup cost, so a placement decided once per
    // process would bias the whole run. Re-rolling it per sample averages the
    // placement lottery into every run instead.
    let fresh_rt = || {
        Builder::new_multi_thread()
            .worker_threads(MAX_PRODUCERS + 1)
            .build()
            .unwrap()
    };

    let mut group = c.benchmark_group("mem_bounded/pipelined");
    group.throughput(Throughput::Elements(N));
    // Iterations are long (100k messages each); flat sampling runs the same
    // number of iterations per sample instead of linearly increasing them,
    // and fewer-but-longer samples average out scheduler jitter within each
    // sample rather than surfacing it as run-to-run variance.
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(30);
    group.warm_up_time(Duration::from_secs(3));
    group.measurement_time(Duration::from_secs(10));
    // Measured run-to-run drift for this group is ~5% even on a quiet machine;
    // don't report smaller swings as regressions.
    group.noise_threshold(0.05);

    for producers in [1u64, MAX_PRODUCERS as u64] {
        group.bench_with_input(
            BenchmarkId::new("mem_bounded", producers),
            &producers,
            |b, &producers| {
                b.iter_custom(|iters| {
                    fresh_rt().block_on(async move {
                        let start = Instant::now();
                        for _ in 0..iters {
                            let (tx, mut rx) =
                                mem_bounded::channel::<Msg>(CAPACITY * MSG_SIZE as usize);
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
                        }
                        start.elapsed()
                    })
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("tokio_bounded", producers),
            &producers,
            |b, &producers| {
                b.iter_custom(|iters| {
                    fresh_rt().block_on(async move {
                        let start = Instant::now();
                        for _ in 0..iters {
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
                        }
                        start.elapsed()
                    })
                });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_uncontended, bench_pipelined);
criterion_main!(benches);
