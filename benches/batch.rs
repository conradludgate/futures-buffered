use std::time::Duration;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use futures::{
    stream::{self, FuturesUnordered},
    StreamExt,
};
use futures_buffered::{BufferedStreamExt, FuturesUnorderedBounded};

fn batch(c: &mut Criterion) {
    // setup a tokio runtime for our tests
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    async fn sleep() {
        tokio::time::sleep(Duration::from_micros(100)).await
    }

    let mut g = c.benchmark_group("FuturesUnordered");
    for i in [16, 64, 256].iter() {
        g.bench_with_input(BenchmarkId::new("futures-rs", i), i, |b, &batch| {
            let mut queue = FuturesUnordered::new();
            let total = batch * batch;
            b.iter(|| {
                for _ in 0..batch {
                    queue.push(sleep())
                }
                for _ in batch..total {
                    runtime.block_on(queue.next());
                    queue.push(sleep())
                }
                for _ in 0..batch {
                    runtime.block_on(queue.next());
                }
            })
        });
        g.bench_with_input(BenchmarkId::new("futures-buffered", i), i, |b, &batch| {
            let mut queue = FuturesUnorderedBounded::new(batch);
            let total = batch * batch;
            b.iter(|| {
                for _ in 0..batch {
                    queue.push(sleep())
                }
                for _ in batch..total {
                    runtime.block_on(queue.next());
                    queue.push(sleep())
                }
                for _ in 0..batch {
                    runtime.block_on(queue.next());
                }
            })
        });
        g.bench_with_input(BenchmarkId::new("futures-buffered2", i), i, |b, &batch| {
            let mut queue = FuturesUnordered::new();
            let total = batch * batch;
            b.iter(|| {
                for _ in 0..batch {
                    queue.push(sleep())
                }
                for _ in batch..total {
                    runtime.block_on(queue.next());
                    queue.push(sleep())
                }
                for _ in 0..batch {
                    runtime.block_on(queue.next());
                }
            })
        });
    }
    g.finish();

    let mut g = c.benchmark_group("BufferUnordered");
    for i in [16, 64, 256].iter() {
        g.bench_with_input(BenchmarkId::new("futures-rs", i), i, |b, &batch| {
            let total = batch * batch;
            b.iter(|| {
                let mut s = stream::iter((0..total).map(|_| sleep())).buffer_unordered(batch);
                while runtime.block_on(s.next()).is_some() {}
            })
        });
        g.bench_with_input(BenchmarkId::new("futures-buffered", i), i, |b, &batch| {
            let total = batch * batch;
            b.iter(|| {
                let mut s = stream::iter((0..total).map(|_| sleep())).buffered_unordered(batch);
                while runtime.block_on(s.next()).is_some() {}
            })
        });
    }
    g.finish();

    let mut g = c.benchmark_group("Buffered");
    for i in [16, 64, 256].iter() {
        g.bench_with_input(BenchmarkId::new("futures-rs", i), i, |b, &batch| {
            let total = batch * batch;
            b.iter(|| {
                let mut s = stream::iter((0..total).map(|_| sleep())).buffered(batch);
                while runtime.block_on(s.next()).is_some() {}
            })
        });
        g.bench_with_input(BenchmarkId::new("futures-buffered", i), i, |b, &batch| {
            let total = batch * batch;
            b.iter(|| {
                let mut s = stream::iter((0..total).map(|_| sleep())).buffered_ordered(batch);
                while runtime.block_on(s.next()).is_some() {}
            })
        });
    }
    g.finish();

    let mut g = c.benchmark_group("join_all");
    for i in [16, 64, 256, 1024].iter() {
        g.bench_with_input(BenchmarkId::new("futures-rs", i), i, |b, &batch| {
            b.iter(|| {
                let futs = (0..batch * 8).map(|_| sleep());
                runtime.block_on(futures::future::join_all(futs));
            })
        });
        g.bench_with_input(BenchmarkId::new("futures-buffered", i), i, |b, &batch| {
            b.iter(|| {
                let futs = (0..batch * 8).map(|_| sleep());
                runtime.block_on(futures_buffered::join_all(futs));
            })
        });
    }
    g.finish();
}

criterion_group!(benches, batch);

criterion_main!(benches);
