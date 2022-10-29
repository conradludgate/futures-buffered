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
//!     queue.push(make_req(&mut rs)).map_err(drop).unwrap();
//! }
//! // wait for a request to finish and start another to fill its place - up to 1024 total requests
//! for _ in 128..1024 {
//!     queue.next().await;
//!     queue.push(make_req(&mut rs)).map_err(drop).unwrap();
//! }
//! // wait for the tail end to finish
//! for _ in 0..128 {
//!     queue.next().await;
//! }
//! # Ok(()) }
//! ```
use std::{
    future::Future,
    mem::MaybeUninit,
    pin::Pin,
    sync::{Arc, Weak},
    task::{Context, Poll, Wake},
};

use atomic_sparse::AtomicSparseSet;
use futures_util::{stream::Fuse, task::AtomicWaker, Stream, StreamExt};
use pin_project_lite::pin_project;
use sparse::SparseSet;

mod atomic_sparse;
mod sparse;

fn project_slice<T>(slice: Pin<&mut [T]>, i: usize) -> Pin<&mut T> {
    // SAFETY: slice fields are pinned since the whole slice is pinned
    // <https://discord.com/channels/273534239310479360/818964227783262209/1035563044887072808>
    unsafe { slice.map_unchecked_mut(|futs| &mut futs[i]) }
}

/// A set of futures which may complete in any order.
///
/// Much like [`futures::stream::FuturesUnordered`](https://docs.rs/futures/0.3.25/futures/stream/struct.FuturesUnordered.html),
/// this is a thread-safe, `Pin` friendly, lifetime friendly, concurrent processing stream.
///
/// The is different to `FuturesUnordered` in that `FuturesUnorderedBounded` has a fixed capacity for processing count.
/// This means it's less flexible, but produces better memory efficiency.
///
/// ## Benchmarks
///
/// ### Speed
///
/// Running 512000 http requests (over an already establish HTTP2 connection) with 256 concurrent jobs
/// in a single threaded tokio runtime:
///
/// ```text
/// FuturesUnordered         time:   [220.20 ms 220.97 ms 221.80 ms]
/// FuturesUnorderedBounded  time:   [208.73 ms 209.26 ms 209.86 ms]
/// ```
///
/// ### Memory usage
///
/// Running 512000 `Ready<i32>` futures with 256 concurrent jobs in a single threaded tokio runtime.
///
/// - count: the number of times alloc/dealloc was called
/// - alloc: the number of cumulative bytes allocated
/// - dealloc: the number of cumulative bytes deallocated
///
/// ```text
/// FuturesUnordered
///     count:    1024002
///     alloc:    36864136 B
///     dealloc:  36864000 B
///
/// FuturesUnorderedBounded
///     count:    260
///     alloc:    20544 B
///     dealloc:  0 B
/// ```
///
/// ### Conclusion
///
/// As you can see, `FuturesUnorderedBounded` massively reduces you memory overhead while providing a small performance gain.
/// Perfect for if you want a fixed batch size
///
/// # Example
///
/// Making 1024 total HTTP requests, with a max concurrency of 128
///
/// ```
/// use futures::stream::StreamExt;
/// use futures_buffered::FuturesUnorderedBounded;
/// use hyper::{client::conn::{handshake, ResponseFuture, SendRequest}, Body, Request };
/// use tokio::net::TcpStream;
///
/// # #[tokio::main]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// // create a tcp connection
/// let stream = TcpStream::connect("example.com:80").await?;
///
/// // perform the http handshakes
/// let (mut rs, conn) = handshake(stream).await?;
/// tokio::spawn(conn);
///
/// /// make http request to example.com and read the response
/// fn make_req(rs: &mut SendRequest<Body>) -> ResponseFuture {
///     let req = Request::builder()
///         .header("Host", "example.com")
///         .method("GET")
///         .body(Body::from(""))
///         .unwrap();
///     rs.send_request(req)
/// }
///
/// // create a queue that can hold 128 concurrent requests
/// let mut queue = FuturesUnorderedBounded::new(128);
///
/// // start up 128 requests
/// for _ in 0..128 {
///     queue.push(make_req(&mut rs)).map_err(drop).unwrap();
/// }
/// // wait for a request to finish and start another to fill its place - up to 1024 total requests
/// for _ in 128..1024 {
///     queue.next().await;
///     queue.push(make_req(&mut rs)).map_err(drop).unwrap();
/// }
/// // wait for the tail end to finish
/// for _ in 0..128 {
///     queue.next().await;
/// }
/// # Ok(()) }
/// ```
pub struct FuturesUnorderedBounded<F> {
    slots: SparseSet,
    inner: Pin<Box<[Task<F>]>>,
    shared: Arc<Shared>,
}
impl<F> Unpin for FuturesUnorderedBounded<F> {}

struct Shared {
    ready: AtomicSparseSet,
    waker: AtomicWaker,
}

pin_project!(
    struct Task<F> {
        #[pin]
        slot: Option<F>,
        waker: Arc<InnerWaker>,
    }
);

impl<F> FuturesUnorderedBounded<F> {
    pub fn new(cap: usize) -> Self {
        // create the shared data that is part of the queue and
        // the wakers
        let shared = Arc::new(Shared {
            ready: AtomicSparseSet::new(cap),
            waker: AtomicWaker::new(),
        });

        // create the task buffer + slot stack
        let mut v: Vec<Task<F>> = Vec::with_capacity(cap);
        let mut slots = SparseSet::new(cap);
        for i in 0..cap {
            let waker = Arc::new(InnerWaker {
                index: i,
                shared: Arc::downgrade(&shared),
            });
            v.push(Task { slot: None, waker });
            slots.push(i);
        }

        Self {
            inner: v.into_boxed_slice().into(),
            shared,
            slots,
        }
    }
    pub fn push(&mut self, fut: F) -> Result<(), F> {
        let mut inner: Pin<&mut [Task<F>]> = self.inner.as_mut();
        if let Some(i) = self.slots.pop() {
            // if there's a slot available, push the future in
            // and mark it as ready for polling
            project_slice(inner.as_mut(), i)
                .project()
                .slot
                .set(Some(fut));
            self.shared.ready.push(i);
            Ok(())
        } else {
            // if no slots, return back the future
            Err(fut)
        }
    }
    pub fn is_empty(&self) -> bool {
        self.inner.len() == self.slots.len()
    }
    pub fn len(&self) -> usize {
        self.inner.len() - self.slots.len()
    }
    pub fn capacity(&self) -> usize {
        self.inner.len()
    }
}

impl<F: Future> FuturesUnorderedBounded<F> {
    fn poll_inner(&mut self, cx: &mut Context<'_>) -> Poll<Option<(usize, F::Output)>> {
        self.shared.waker.register(cx.waker());
        while let Some(i) = self.shared.ready.pop() {
            let mut inner = self.inner.as_mut();
            let mut task = project_slice(inner.as_mut(), i).project();

            let waker = task.waker.clone().into();
            let mut cx = Context::from_waker(&waker);

            let res = match task.slot.as_mut().as_pin_mut() {
                // poll the current task
                Some(fut) => fut.poll(&mut cx),
                None => continue,
            };

            if let Poll::Ready(x) = res {
                task.slot.set(None);
                self.slots.push(i);
                return Poll::Ready(Some((i, x)));
            }
        }
        if self.inner.iter().filter_map(|x| x.slot.as_ref()).count() == 0 {
            Poll::Ready(None)
        } else {
            Poll::Pending
        }
    }
}

impl<F: Future> Stream for FuturesUnorderedBounded<F> {
    type Item = F::Output;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.poll_inner(cx) {
            Poll::Ready(Some((_, x))) => Poll::Ready(Some(x)),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.len();
        (len, Some(len))
    }
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
#[must_use = "futures do nothing unless you `.await` or poll them"]
/// Future for the [`join_all`] function.
pub struct JoinAll<F: Future> {
    queue: FuturesUnorderedBounded<F>,
    output: Box<[MaybeUninit<F::Output>]>,
}

impl<F: Future> Unpin for JoinAll<F> {}

/// Creates a future which represents a collection of the outputs of the futures
/// given.
///
/// The returned future will drive execution for all of its underlying futures,
/// collecting the results into a destination `Vec<T>` in the same order as they
/// were provided.
///
/// # Examples
///
/// ```
/// # futures::executor::block_on(async {
/// use futures::future::join_all;
///
/// async fn foo(i: u32) -> u32 { i }
///
/// let futures = vec![foo(1), foo(2), foo(3)];
///
/// assert_eq!(join_all(futures).await, [1, 2, 3]);
/// # });
/// ```
pub fn join_all<I>(iter: I) -> JoinAll<<I as IntoIterator>::Item>
where
    I: IntoIterator,
    <I as IntoIterator>::Item: Future,
{
    let iter = iter.into_iter();

    // determine a suitable initial capacity
    let cap = match iter.size_hint() {
        (_, Some(max)) => max,
        (min, None) => min,
    };

    // store the futures in our task list
    let mut v: Vec<Task<<I as IntoIterator>::Item>> = Vec::with_capacity(cap);
    for (i, fut) in iter.enumerate() {
        let waker = Arc::new(InnerWaker {
            index: i,
            shared: Weak::new(),
        });
        v.push(Task {
            slot: Some(fut),
            waker,
        });
    }

    // determine the actual capacity and create the shared state
    let cap = v.len();
    let shared = Arc::new(Shared {
        ready: AtomicSparseSet::new(cap),
        waker: AtomicWaker::new(),
    });
    let mut slots = SparseSet::new(cap);

    // register the shared state on our tasks
    for (i, task) in v.iter_mut().enumerate() {
        slots.push(i);
        shared.ready.push(i);

        // we know that we haven't cloned this arc before since it was created
        // just a few lines above
        Arc::get_mut(&mut task.waker).unwrap().shared = Arc::downgrade(&shared);
    }

    // create the queue
    let queue = FuturesUnorderedBounded {
        inner: v.into_boxed_slice().into(),
        shared,
        slots,
    };

    // create the output buffer
    let mut output = Vec::with_capacity(cap);
    output.resize_with(cap, MaybeUninit::uninit);

    JoinAll {
        queue,
        output: output.into_boxed_slice(),
    }
}

impl<F: Future> Future for JoinAll<F> {
    type Output = Vec<F::Output>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        loop {
            match self.as_mut().queue.poll_inner(cx) {
                Poll::Ready(Some((i, x))) => {
                    self.output[i].write(x);
                }
                Poll::Ready(None) => {
                    // SAFETY: for Ready(None) to be returned, we know that every future in the queue
                    // must be consumed. Since we have a 1:1 mapping in the queue to our output, we
                    // know that every output entry is init.
                    let boxed = unsafe {
                        // take the boxed slice
                        let boxed =
                            std::mem::replace(&mut self.output, Vec::new().into_boxed_slice());

                        // Box::assume_init
                        let raw = Box::into_raw(boxed);
                        Box::from_raw(raw as *mut [F::Output])
                    };

                    break Poll::Ready(boxed.into_vec());
                }
                Poll::Pending => break Poll::Pending,
            }
        }
    }
}

impl<T: ?Sized + Stream> BufferedStreamExt for T {}

/// An extension trait for `Stream`s that provides a variety of convenient
/// combinator functions.
pub trait BufferedStreamExt: Stream {
    /// An adaptor for creating a buffered list of pending futures (unordered).
    ///
    /// If this stream's item can be converted into a future, then this adaptor
    /// will buffer up to `n` futures and then return the outputs in the order
    /// in which they complete. No more than `n` futures will be buffered at
    /// any point in time, and less than `n` may also be buffered depending on
    /// the state of each future.
    ///
    /// The returned stream will be a stream of each future's output.
    ///
    /// This method is only available when the `std` or `alloc` feature of this
    /// library is activated, and it is activated by default.
    ///
    /// # Examples
    ///
    /// ```
    /// # futures::executor::block_on(async {
    /// use futures::channel::oneshot;
    /// use futures::stream::{self, StreamExt};
    /// use futures_buffered::BufferedStreamExt;
    ///
    /// let (send_one, recv_one) = oneshot::channel();
    /// let (send_two, recv_two) = oneshot::channel();
    ///
    /// let stream_of_futures = stream::iter(vec![recv_one, recv_two]);
    /// let mut buffered = stream_of_futures.buffered_unordered(10);
    ///
    /// send_two.send(2i32)?;
    /// assert_eq!(buffered.next().await, Some(Ok(2i32)));
    ///
    /// send_one.send(1i32)?;
    /// assert_eq!(buffered.next().await, Some(Ok(1i32)));
    ///
    /// assert_eq!(buffered.next().await, None);
    /// # Ok::<(), i32>(()) }).unwrap();
    /// ```
    fn buffered_unordered(self, n: usize) -> BufferUnordered<Self>
    where
        Self::Item: Future,
        Self: Sized,
    {
        BufferUnordered {
            stream: StreamExt::fuse(self),
            in_progress_queue: FuturesUnorderedBounded::new(n),
        }
    }
}

pin_project!(
    /// Stream for the [`buffered_unordered`](BufferedStreamExt::buffered_unordered)
    /// method.
    #[must_use = "streams do nothing unless polled"]
    pub struct BufferUnordered<S: Stream> {
        #[pin]
        stream: Fuse<S>,
        in_progress_queue: FuturesUnorderedBounded<S::Item>,
    }
);

impl<St> Stream for BufferUnordered<St>
where
    St: Stream,
    St::Item: Future,
{
    type Item = <St::Item as Future>::Output;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        // First up, try to spawn off as many futures as possible by filling up
        // our queue of futures.

        while let Some(i) = this.in_progress_queue.slots.pop() {
            match this.stream.as_mut().poll_next(cx) {
                Poll::Ready(Some(fut)) => {
                    project_slice(this.in_progress_queue.inner.as_mut(), i)
                        .project()
                        .slot
                        .set(Some(fut));
                    this.in_progress_queue.shared.ready.push(i);
                }
                Poll::Ready(None) | Poll::Pending => {
                    this.in_progress_queue.slots.push(i);
                    break;
                }
            }
        }

        // Attempt to pull the next value from the in_progress_queue
        match this.in_progress_queue.poll_next_unpin(cx) {
            x @ (Poll::Pending | Poll::Ready(Some(_))) => return x,
            Poll::Ready(None) => {}
        }

        // If more values are still coming from the stream, we're not done yet
        if this.stream.is_done() {
            Poll::Ready(None)
        } else {
            Poll::Pending
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let queue_len = self.in_progress_queue.len();
        let (lower, upper) = self.stream.size_hint();
        let lower = lower.saturating_add(queue_len);
        let upper = match upper {
            Some(x) => x.checked_add(queue_len),
            None => None,
        };
        (lower, upper)
    }
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
        buffer
            .push(sleep(&c, Duration::from_secs(1)))
            .map_err(drop)
            .unwrap();
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
            buffer.push(wait(&c, i)).map_err(drop).unwrap();
        }
        // poll and insert
        for i in 0..100 {
            assert!(buffer.next().await.is_some());
            buffer.push(wait(&c, i)).map_err(drop).unwrap();
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
            buffer
                .push(sleep(&c, Duration::from_millis(10)))
                .map_err(drop)
                .unwrap();
        }
        // spawn a slow future among a bunch of fast ones.
        // the test is to make sure this doesn't block the rest getting completed
        buffer
            .push(sleep(&c, Duration::from_secs(2)))
            .map_err(drop)
            .unwrap();
        // poll and insert
        for _ in 0..100 {
            assert!(buffer.next().await.is_some());
            buffer
                .push(sleep(&c, Duration::from_millis(10)))
                .map_err(drop)
                .unwrap();
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
