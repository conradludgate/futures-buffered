use std::time::Duration;

use criterion::{criterion_group, criterion_main, Criterion};
use futures::{
    stream::{self, FuturesUnordered},
    StreamExt,
};
use futures_buffered::{BufferedStreamExt, FuturesUnorderedBounded};

const BATCH: usize = 256;
const TOTAL: usize = 65536;

fn batch(c: &mut Criterion) {
    // setup a tokio runtime for our tests
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    async fn sleep() {
        tokio::time::sleep(Duration::from_micros(100)).await
    }

    let mut queue = FuturesUnordered::new();
    c.bench_function("FuturesUnordered", |b| {
        b.iter(|| {
            for _ in 0..BATCH {
                queue.push(sleep())
            }
            for _ in BATCH..TOTAL {
                runtime.block_on(queue.next());
                queue.push(sleep())
            }
            for _ in 0..BATCH {
                runtime.block_on(queue.next());
            }
        })
    });

    let mut queue = FuturesUnorderedBounded::new(BATCH);
    c.bench_function("FuturesUnorderedBounded", |b| {
        b.iter(|| {
            for _ in 0..BATCH {
                queue.push(sleep());
            }
            for _ in BATCH..TOTAL {
                runtime.block_on(queue.next());
                queue.push(sleep());
            }
            for _ in 0..BATCH {
                runtime.block_on(queue.next());
            }
        })
    });

    c.bench_function("BufferUnordered", |b| {
        b.iter(|| {
            let mut s = stream::iter((0..TOTAL).map(|_| sleep())).buffer_unordered(BATCH);
            while runtime.block_on(s.next()).is_some() {}
        })
    });

    c.bench_function("BufferedUnordered", |b| {
        b.iter(|| {
            let mut s = stream::iter((0..TOTAL).map(|_| sleep())).buffered_unordered(BATCH);
            while runtime.block_on(s.next()).is_some() {}
        })
    });

    c.bench_function("futures::join_all", |b| {
        b.iter(|| {
            let futs = (0..BATCH * 8).map(|_| sleep());
            runtime.block_on(futures::future::join_all(futs));
        })
    });

    c.bench_function("crate::join_all", |b| {
        b.iter(|| {
            let futs = (0..BATCH * 8).map(|_| sleep());
            runtime.block_on(futures_buffered::join_all(futs));
        })
    });

    c.bench_function("Buffered", |b| {
        b.iter(|| {
            let mut s = stream::iter((0..TOTAL).map(|_| sleep())).buffered(BATCH);
            while runtime.block_on(s.next()).is_some() {}
        })
    });

    c.bench_function("BufferedOrdered", |b| {
        b.iter(|| {
            let mut s = stream::iter((0..TOTAL).map(|_| sleep())).buffered_ordered(BATCH);
            while runtime.block_on(s.next()).is_some() {}
        })
    });
}

criterion_group!(benches, batch);

criterion_main!(benches);
