//! # futures-buffered
//!
//! This project provides a single future structure: `FuturesUnorderedBounded`.
//!
//! Much like [`futures::stream::FuturesUnordered`](https://docs.rs/futures/0.3.25/futures/stream/struct.FuturesUnordered.html),
//! this is a thread-safe, `Pin` friendly, lifetime friendly, concurrent processing stream.
//!
//! The is different to `FuturesUnordered` in that `FuturesUnorderedBounded` has a fixed capacity for processing count.
//! This means it's less flexible, but produces better memory efficiency.
//!
//! ## Benchmarks
//!
//! ### Speed
//!
//! Running 512000 http requests (over an already establish HTTP2 connection) with 256 concurrent jobs
//! in a single threaded tokio runtime:
//!
//! ```text
//! FuturesUnordered         time:   [220.20 ms 220.97 ms 221.80 ms]
//! FuturesUnorderedBounded  time:   [208.73 ms 209.26 ms 209.86 ms]
//! ```
//!
//! ### Memory usage
//!
//! Running 512000 `Ready<i32>` futures with 256 concurrent jobs in a single threaded tokio runtime.
//!
//! - count: the number of times alloc/dealloc was called
//! - alloc: the number of cumulative bytes allocated
//! - dealloc: the number of cumulative bytes deallocated
//!
//! ```text
//! FuturesUnordered
//!     count:    1024002
//!     alloc:    36864136 B
//!     dealloc:  36864000 B
//!
//! FuturesUnorderedBounded
//!     count:    260
//!     alloc:    20544 B
//!     dealloc:  0 B
//! ```
//!
//! ### Conclusion
//!
//! As you can see, `FuturesUnorderedBounded` massively reduces you memory overhead while providing a small performance gain.
//! Perfect for if you want a fixed batch size
//!
//! # Example
//! ```
//! use futures::stream::StreamExt;
//! use futures_buffered::FuturesUnorderedBounded;
//! use hyper::{client::conn::{handshake, ResponseFuture, SendRequest}, Body, Request };
//! use tokio::net::TcpStream;
//!
//! # #[tokio::main]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! // create a tcp connection
//! let stream = TcpStream::connect("example.com:80").await?;
//!
//! // perform the http handshakes
//! let (mut rs, conn) = handshake(stream).await?;
//! tokio::spawn(conn);
//!
//! /// make http request to example.com and read the response
//! fn make_req(rs: &mut SendRequest<Body>) -> ResponseFuture {
//!     let req = Request::builder()
//!         .header("Host", "example.com")
//!         .method("GET")
//!         .body(Body::from(""))
//!         .unwrap();
//!     rs.send_request(req)
//! }
//!
//! // create a queue that can hold 128 concurrent requests
//! let mut queue = FuturesUnorderedBounded::new(128);
//!
//! // start up 128 requests
//! for _ in 0..128 {
//!     queue.push(make_req(&mut rs));
//! }
//! // wait for a request to finish and start another to fill its place - up to 1024 total requests
//! for _ in 128..1024 {
//!     queue.next().await;
//!     queue.push(make_req(&mut rs));
//! }
//! // wait for the tail end to finish
//! for _ in 0..128 {
//!     queue.next().await;
//! }
//! # Ok(()) }
//! ```

use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Weak},
    task::Wake,
};

use atomic_sparse::AtomicSparseSet;
use futures_util::task::AtomicWaker;

mod atomic_sparse;
mod buffered_unordered;
mod futures_unordered_bounded;
mod join_all;
mod sparse;
mod try_join_all;

pub use buffered_unordered::{BufferUnordered, BufferedStreamExt};
pub use futures_unordered_bounded::FuturesUnorderedBounded;
pub use join_all::{join_all, JoinAll};
pub use try_join_all::{try_join_all, TryJoinAll};

fn project_slice<T>(slice: Pin<&mut [T]>, i: usize) -> Pin<&mut T> {
    // SAFETY: slice fields are pinned since the whole slice is pinned
    // <https://discord.com/channels/273534239310479360/818964227783262209/1035563044887072808>
    unsafe { slice.map_unchecked_mut(|futs| &mut futs[i]) }
}

struct Shared {
    ready: AtomicSparseSet,
    waker: AtomicWaker,
}

struct InnerWaker {
    index: usize,
    shared: Weak<Shared>,
}

impl Wake for InnerWaker {
    fn wake(self: std::sync::Arc<Self>) {
        self.wake_by_ref();
    }
    /// on wake, insert the future index into the queue, and then wake the original waker too
    fn wake_by_ref(self: &Arc<Self>) {
        if let Some(shared) = self.shared.upgrade() {
            shared.ready.push(self.index);
            shared.waker.wake();
        }
    }
}

mod private_try_future {
    use std::future::Future;

    pub trait Sealed {}

    impl<F, T, E> Sealed for F where F: ?Sized + Future<Output = Result<T, E>> {}
}

/// A convenience for futures that return `Result` values that includes
/// a variety of adapters tailored to such futures.
///
/// This is [`futures::TryFuture`](futures_core::future::TryFuture) except it's stricter on the future super-trait.
pub trait TryFuture:
    Future<Output = Result<Self::Ok, Self::Err>> + private_try_future::Sealed
{
    type Ok;
    type Err;
}

impl<T, E, F: ?Sized + Future<Output = Result<T, E>>> TryFuture for F {
    type Ok = T;
    type Err = E;
}

#[cfg(test)]
mod tests {
    use std::{
        cell::Cell,
        pin::Pin,
        task::{Context, Poll},
        time::{Duration, Instant},
    };

    use futures::{Future, StreamExt};
    use pin_project_lite::pin_project;
    use tokio::time::Sleep;

    use crate::FuturesUnorderedBounded;

    pin_project!(
        struct PollCounter<'c, F> {
            count: &'c Cell<usize>,
            #[pin]
            inner: F,
        }
    );

    impl<F: Future> Future for PollCounter<'_, F> {
        type Output = F::Output;
        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            self.count.set(self.count.get() + 1);
            self.project().inner.poll(cx)
        }
    }

    fn sleep(count: &Cell<usize>, dur: Duration) -> PollCounter<'_, Sleep> {
        PollCounter {
            count,
            inner: tokio::time::sleep(dur),
        }
    }

    #[tokio::test]
    async fn single() {
        let c = Cell::new(0);

        let mut buffer = FuturesUnorderedBounded::new(10);
        buffer.push(sleep(&c, Duration::from_secs(1)));
        buffer.next().await;

        assert_eq!(c.into_inner(), 2);
    }

    #[tokio::test]
    async fn multi() {
        fn wait(count: &Cell<usize>, i: usize) -> PollCounter<'_, Sleep> {
            sleep(count, Duration::from_secs(1) / (i as u32 % 10 + 5))
        }

        let c = Cell::new(0);

        let mut buffer = FuturesUnorderedBounded::new(10);
        // build up
        for i in 0..10 {
            buffer.push(wait(&c, i));
        }
        // poll and insert
        for i in 0..100 {
            assert!(buffer.next().await.is_some());
            buffer.push(wait(&c, i));
        }
        // drain down
        for _ in 0..10 {
            assert!(buffer.next().await.is_some());
        }

        let count = c.into_inner();
        assert_eq!(count, 220);
    }

    #[tokio::test]
    async fn very_slow_task() {
        let c = Cell::new(0);

        let now = Instant::now();

        let mut buffer = FuturesUnorderedBounded::new(10);
        // build up
        for _ in 0..9 {
            buffer.push(sleep(&c, Duration::from_millis(10)));
        }
        // spawn a slow future among a bunch of fast ones.
        // the test is to make sure this doesn't block the rest getting completed
        buffer.push(sleep(&c, Duration::from_secs(2)));
        // poll and insert
        for _ in 0..100 {
            assert!(buffer.next().await.is_some());
            buffer.push(sleep(&c, Duration::from_millis(10)));
        }
        // drain down
        for _ in 0..10 {
            assert!(buffer.next().await.is_some());
        }

        let dur = now.elapsed();
        assert!(dur < Duration::from_millis(2050));

        let count = c.into_inner();
        assert_eq!(count, 220);
    }

    #[tokio::test]
    async fn buffered_unordered() {
        use crate::BufferedStreamExt;
        use futures::channel::oneshot;
        use futures::stream::{self, StreamExt};

        let (send_one, recv_one) = oneshot::channel();
        let (send_two, recv_two) = oneshot::channel();

        let stream_of_futures = stream::iter(vec![recv_one, recv_two]);
        let mut buffered = stream_of_futures.buffered_unordered(10);

        send_two.send(2i32).unwrap();
        assert_eq!(buffered.next().await, Some(Ok(2i32)));

        send_one.send(1i32).unwrap();
        assert_eq!(buffered.next().await, Some(Ok(1i32)));

        assert_eq!(buffered.next().await, None);
    }
}
