use core::{
    fmt,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{arc_slice::ArcSlice, slot_map::SlotMap};
use futures_core::{FusedStream, Stream};

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
/// Running 65536 100us timers with 256 concurrent jobs in a single threaded tokio runtime:
///
/// ```text
/// FuturesUnordered         time:   [420.47 ms 422.21 ms 423.99 ms]
/// FuturesUnorderedBounded  time:   [366.02 ms 367.54 ms 369.05 ms]
/// ```
///
/// ### Memory usage
///
/// Running 512000 `Ready<i32>` futures with 256 concurrent jobs.
///
/// - count: the number of times alloc/dealloc was called
/// - alloc: the number of cumulative bytes allocated
/// - dealloc: the number of cumulative bytes deallocated
///
/// ```text
/// FuturesUnordered
///     count:    1024002
///     alloc:    40960144 B
///     dealloc:  40960000 B
///
/// FuturesUnorderedBounded
///     count:    2
///     alloc:    8264 B
///     dealloc:  0 B
/// ```
///
/// ### Conclusion
///
/// As you can see, `FuturesUnorderedBounded` massively reduces you memory overhead while providing a significant performance gain.
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
///     queue.push(make_req(&mut rs));
/// }
/// // wait for a request to finish and start another to fill its place - up to 1024 total requests
/// for _ in 128..1024 {
///     queue.next().await;
///     queue.push(make_req(&mut rs));
/// }
/// // wait for the tail end to finish
/// for _ in 0..128 {
///     queue.next().await;
/// }
/// # Ok(()) }
/// ```
pub struct FuturesUnorderedBounded<F> {
    pub(crate) tasks: SlotMap<F>,
    pub(crate) shared: ArcSlice,
}

impl<F> Unpin for FuturesUnorderedBounded<F> {}

impl<F> FuturesUnorderedBounded<F> {
    /// Constructs a new, empty [`FuturesUnorderedBounded`] with the given fixed capacity.
    ///
    /// The returned [`FuturesUnorderedBounded`] does not contain any futures.
    /// In this state, [`FuturesUnorderedBounded::poll_next`](Stream::poll_next) will
    /// return [`Poll::Ready(None)`](Poll::Ready).
    pub fn new(cap: usize) -> Self {
        Self {
            tasks: SlotMap::new(cap),
            shared: ArcSlice::new(cap),
        }
    }

    /// Push a future into the set.
    ///
    /// This method adds the given future to the set. This method will not
    /// call [`poll`](core::future::Future::poll) on the submitted future. The caller must
    /// ensure that [`FuturesUnorderedBounded::poll_next`](Stream::poll_next) is called
    /// in order to receive wake-up notifications for the given future.
    ///
    /// # Panics
    /// This method will panic if the buffer is currently full. See [`FuturesUnorderedBounded::try_push`] to get a result instead
    #[track_caller]
    pub fn push(&mut self, fut: F) {
        if self.try_push(fut).is_err() {
            panic!("attempted to push into a full `FuturesUnorderedBounded`")
        }
    }

    /// Push a future into the set.
    ///
    /// This method adds the given future to the set. This method will not
    /// call [`poll`](core::future::Future::poll) on the submitted future. The caller must
    /// ensure that [`FuturesUnorderedBounded::poll_next`](Stream::poll_next) is called
    /// in order to receive wake-up notifications for the given future.
    ///
    /// # Errors
    /// This method will error if the buffer is currently full, returning the future back
    pub fn try_push(&mut self, fut: F) -> Result<(), F> {
        self.try_push_with(fut, core::convert::identity)
    }

    #[inline]
    pub(crate) fn try_push_with<T>(&mut self, t: T, f: impl FnMut(T) -> F) -> Result<(), T> {
        let i = self.tasks.insert_with(t, f)?;
        self.shared.push(i);
        Ok(())
    }

    /// Returns `true` if the set contains no futures.
    pub fn is_empty(&self) -> bool {
        self.tasks.is_empty()
    }

    /// Returns the number of futures contained in the set.
    ///
    /// This represents the total number of in-flight futures.
    pub fn len(&self) -> usize {
        self.tasks.len()
    }

    /// Returns the number of futures that can be contained in the set.
    pub fn capacity(&self) -> usize {
        self.tasks.capacity()
    }
}

impl<F: Future> FuturesUnorderedBounded<F> {
    pub(crate) fn poll_inner(&mut self, cx: &mut Context<'_>) -> Poll<Option<(usize, F::Output)>> {
        self.shared.meta.waker.register(cx.waker());

        const MAX: usize = 61;
        let mut count = 0;
        loop {
            count += 1;
            if count > MAX {
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }

            let i = unsafe { self.shared.pop() };
            // empty
            if i == self.tasks.capacity() {
                break;
            }
            // inconsistent
            if i > self.tasks.capacity() {
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }

            if let Some(task) = self.tasks.get(i) {
                let waker = self.shared.get(i).waker();
                let mut cx = Context::from_waker(&waker);

                let res = task.poll(&mut cx);

                if let Poll::Ready(x) = res {
                    self.tasks.remove(i);
                    return Poll::Ready(Some((i, x)));
                }
            }
        }

        if self.is_empty() {
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

impl<F> FromIterator<F> for FuturesUnorderedBounded<F> {
    /// Constructs a new, empty [`FuturesUnorderedBounded`] with a fixed capacity that is the length of the iterator.
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
    /// // create a queue with an initial 128 concurrent requests
    /// let mut queue: FuturesUnorderedBounded<_> = (0..128).map(|_| make_req(&mut rs)).collect();
    ///
    /// // wait for a request to finish and start another to fill its place - up to 1024 total requests
    /// for _ in 128..1024 {
    ///     queue.next().await;
    ///     queue.push(make_req(&mut rs));
    /// }
    /// // wait for the tail end to finish
    /// for _ in 0..128 {
    ///     queue.next().await;
    /// }
    /// # Ok(()) }
    /// ```
    fn from_iter<T: IntoIterator<Item = F>>(iter: T) -> Self {
        // store the futures in our task list
        let tasks = SlotMap::from_iter(iter);

        // determine the actual capacity and create the shared state
        let cap = tasks.len();
        let shared = ArcSlice::new(cap);

        for i in 0..cap {
            shared.push(i);
        }

        // // we know that we haven't cloned this arc before, since it was created just above
        // let meta = unsafe { shared.get_mut_unchecked() };

        // // register the shared state on our tasks
        // meta.push_all();

        // create the queue
        Self { tasks, shared }
    }
}

impl<Fut: Future> FusedStream for FuturesUnorderedBounded<Fut> {
    fn is_terminated(&self) -> bool {
        self.is_empty()
    }
}

impl<Fut: Future> fmt::Debug for FuturesUnorderedBounded<Fut> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FuturesUnorderedBounded {{ ... }}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::{
        cell::Cell,
        future::{poll_fn, ready},
        time::Duration,
    };
    use futures::{channel::oneshot, StreamExt};
    use futures_test::task::noop_context;
    use pin_project_lite::pin_project;
    use std::{thread, time::Instant};

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

    struct Sleep {
        until: Instant,
    }
    impl Unpin for Sleep {}
    impl Future for Sleep {
        type Output = ();

        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            let until = self.until;
            if until > Instant::now() {
                let waker = cx.waker().clone();
                thread::spawn(move || {
                    thread::sleep(until.duration_since(Instant::now()));
                    waker.wake()
                });
                Poll::Pending
            } else {
                Poll::Ready(())
            }
        }
    }

    fn sleep(count: &Cell<usize>, dur: Duration) -> PollCounter<'_, Sleep> {
        PollCounter {
            count,
            inner: Sleep {
                until: Instant::now() + dur,
            },
        }
    }

    #[test]
    fn single() {
        let c = Cell::new(0);

        let mut buffer = FuturesUnorderedBounded::new(10);
        buffer.push(sleep(&c, Duration::from_secs(1)));
        futures::executor::block_on(buffer.next());

        drop(buffer);
        assert_eq!(c.into_inner(), 2);
    }

    #[test]
    #[should_panic(expected = "attempted to push into a full `FuturesUnorderedBounded`")]
    fn full() {
        let mut buffer = FuturesUnorderedBounded::new(1);
        buffer.push(ready(()));
        buffer.push(ready(()));
    }

    #[test]
    fn len() {
        let mut buffer = FuturesUnorderedBounded::new(1);

        assert_eq!(buffer.len(), 0);
        assert!(buffer.is_empty());
        assert_eq!(buffer.capacity(), 1);
        assert_eq!(buffer.size_hint(), (0, Some(0)));
        assert!(buffer.is_terminated());

        buffer.push(ready(()));

        assert_eq!(buffer.len(), 1);
        assert!(!buffer.is_empty());
        assert_eq!(buffer.capacity(), 1);
        assert_eq!(buffer.size_hint(), (1, Some(1)));
        assert!(!buffer.is_terminated());

        futures::executor::block_on(buffer.next());

        assert_eq!(buffer.len(), 0);
        assert!(buffer.is_empty());
        assert_eq!(buffer.capacity(), 1);
        assert_eq!(buffer.size_hint(), (0, Some(0)));
        assert!(buffer.is_terminated());
    }

    #[test]
    fn from_iter() {
        let buffer = FuturesUnorderedBounded::from_iter((0..10).map(|_| ready(())));

        assert_eq!(buffer.len(), 10);
        assert_eq!(buffer.capacity(), 10);
        assert_eq!(buffer.size_hint(), (10, Some(10)));
    }

    #[test]
    fn drop_while_waiting() {
        let mut buffer = FuturesUnorderedBounded::new(10);
        let waker = Cell::new(None);
        buffer.push(poll_fn(|cx| {
            waker.set(Some(cx.waker().clone()));
            Poll::<()>::Pending
        }));

        assert_eq!(buffer.poll_next_unpin(&mut noop_context()), Poll::Pending);
        drop(buffer);

        let cx = waker.take().unwrap();
        drop(cx);
    }

    #[test]
    fn multi() {
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
            assert!(futures::executor::block_on(buffer.next()).is_some());
            buffer.push(wait(&c, i));
        }
        // drain down
        for _ in 0..10 {
            assert!(futures::executor::block_on(buffer.next()).is_some());
        }

        let count = c.into_inner();
        assert_eq!(count, 220);
    }

    #[test]
    fn very_slow_task() {
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
            assert!(futures::executor::block_on(buffer.next()).is_some());
            buffer.push(sleep(&c, Duration::from_millis(10)));
        }
        // drain down
        for _ in 0..10 {
            assert!(futures::executor::block_on(buffer.next()).is_some());
        }

        let dur = now.elapsed();
        assert!(dur < Duration::from_millis(2050));

        let count = c.into_inner();
        assert_eq!(count, 220);
    }

    #[cfg(not(miri))]
    #[tokio::test]
    async fn unordered_large() {
        for i in 0..256 {
            let mut queue: FuturesUnorderedBounded<_> = ((0..i).map(|_| async move {
                tokio::time::sleep(Duration::from_nanos(1)).await;
            }))
            .collect();
            for _ in 0..i {
                queue.next().await.unwrap();
            }
        }
    }

    #[test]
    fn correct_fairer_order() {
        const LEN: usize = 256;

        let mut buffer = FuturesUnorderedBounded::new(LEN);
        let mut txs = vec![];
        for _ in 0..LEN {
            let (tx, rx) = oneshot::channel();
            buffer.push(rx);
            txs.push(tx);
        }

        for _ in 0..(LEN / 61) + 1 {
            assert!(buffer.poll_next_unpin(&mut noop_context()).is_pending());
        }

        for (i, tx) in txs.into_iter().enumerate() {
            let _ = tx.send(i);
        }

        for i in 0..LEN {
            let poll = buffer.poll_next_unpin(&mut noop_context());
            assert_eq!(poll, Poll::Ready(Some(Ok(i))));
        }
    }
}
