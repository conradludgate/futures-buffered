use alloc::vec::Vec;
use core::{
    fmt,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::FuturesUnorderedBounded;
use futures_core::{FusedStream, Stream};

/// A set of futures which may complete in any order.
///
/// Much like [`futures::stream::FuturesUnordered`](https://docs.rs/futures/0.3.25/futures/stream/struct.FuturesUnordered.html),
/// this is a thread-safe, `Pin` friendly, lifetime friendly, concurrent processing stream.
///
/// The is different to [`FuturesUnorderedBounded`] because it doesn't have a fixed capacity.
/// It still manages to achieve good efficiency however
///
/// ## Benchmarks
///
/// All benchmarks are run with `FuturesUnordered::new()`, no predefined capacity.
///
/// ### Speed
///
/// Running 65536 100us timers with 256 concurrent jobs in a single threaded tokio runtime:
///
/// ```text
/// futures::FuturesUnordered time:   [412.52 ms 414.47 ms 416.41 ms]
/// crate::FuturesUnordered   time:   [412.96 ms 414.69 ms 416.65 ms]
/// FuturesUnorderedBounded   time:   [361.81 ms 362.96 ms 364.13 ms]
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
/// futures::FuturesUnordered
///     count:    1024002
///     alloc:    40960144 B
///     dealloc:  40960000 B
///
/// crate::FuturesUnordered
///     count:    9
///     alloc:    15840 B
///     dealloc:  0 B
/// ```
///
/// ### Conclusion
///
/// As you can see, our `FuturesUnordered` massively reduces you memory overhead while maintaining good performance.
///
/// # Example
///
/// Making 1024 total HTTP requests, with a max concurrency of 128
///
/// ```
/// use futures::stream::StreamExt;
/// use futures_buffered::FuturesUnordered;
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
/// let mut queue = FuturesUnordered::with_capacity(128);
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
pub struct FuturesUnordered<F> {
    pub(crate) queues: Vec<FuturesUnorderedBounded<F>>,
    len: usize,
    min_poll: usize,
    max_free: usize,
}

const MIN_CAPACITY: usize = 32;

impl<F> Unpin for FuturesUnordered<F> {}

impl<F> Default for FuturesUnordered<F> {
    fn default() -> Self {
        Self::new()
    }
}

impl<F> FuturesUnordered<F> {
    /// Constructs a new, empty [`FuturesUnordered`].
    ///
    /// The returned [`FuturesUnordered`] does not contain any futures.
    /// In this state, [`FuturesUnordered::poll_next`](Stream::poll_next) will
    /// return [`Poll::Ready(None)`](Poll::Ready).
    pub const fn new() -> Self {
        Self {
            queues: Vec::new(),
            len: 0,
            min_poll: 0,
            max_free: usize::MAX,
        }
    }

    /// Constructs a new, empty [`FuturesUnordered`] with the given fixed capacity.
    ///
    /// The returned [`FuturesUnordered`] does not contain any futures.
    /// In this state, [`FuturesUnordered::poll_next`](Stream::poll_next) will
    /// return [`Poll::Ready(None)`](Poll::Ready).
    pub fn with_capacity(n: usize) -> Self {
        if n > 0 {
            Self {
                queues: alloc::vec![FuturesUnorderedBounded::new(n)],
                len: 0,
                min_poll: 0,
                max_free: 0,
            }
        } else {
            Self::new()
        }
    }

    /// Push a future into the set.
    ///
    /// This method adds the given future to the set. This method will not
    /// call [`poll`](core::future::Future::poll) on the submitted future. The caller must
    /// ensure that [`FuturesUnordered::poll_next`](Stream::poll_next) is called
    /// in order to receive wake-up notifications for the given future.
    pub fn push(&mut self, mut fut: F) {
        loop {
            let queue = match self.queues.get_mut(self.max_free) {
                Some(queue) => queue,
                None => {
                    debug_assert_eq!(
                        self.len(),
                        self.capacity(),
                        "max_free should be in bounds if not full: {self:?}"
                    );

                    self.max_free = self.queues.len();
                    let cap = self
                        .queues
                        .last()
                        .map_or(MIN_CAPACITY, |queue| queue.capacity() * 2);
                    let mut queue = FuturesUnorderedBounded::new(cap);
                    queue.push(fut);
                    self.queues.push(queue);
                    break;
                }
            };
            let Err(f) = queue.try_push(fut) else { break };
            fut = f;
            // this in fact was not the max_free queue
            self.max_free = self.max_free.wrapping_sub(1);
        }
        self.min_poll = usize::min(self.max_free, self.min_poll);
        self.len += 1;
    }

    /// Returns `true` if the set contains no futures.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Returns the number of futures contained in the set.
    ///
    /// This represents the total number of in-flight futures.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns the number of futures that can be contained in the set.
    pub fn capacity(&self) -> usize {
        match self.queues.as_slice() {
            [] => 0,
            [only] => only.capacity(),
            [first, .., last] => 2 * last.capacity() - first.capacity(),
        }
    }
}

impl<F: Future> Stream for FuturesUnordered<F> {
    type Item = F::Output;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let Self {
            queues,
            len,
            min_poll,
            max_free,
        } = self.get_mut();
        if *len == 0 {
            return Poll::Ready(None);
        }
        for (i, queue) in queues.iter_mut().enumerate().skip(*min_poll) {
            match queue.poll_inner(cx) {
                Poll::Ready(Some((_, p))) => {
                    *len -= 1;
                    *max_free = usize::max(i, *max_free);
                    return Poll::Ready(Some(p));
                }
                // move min_poll up if the current min_poll
                // is empty
                Poll::Ready(None) if i == *min_poll => {
                    *min_poll += 1;
                }
                _ => continue,
            }
        }
        Poll::Pending
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.len, Some(self.len))
    }
}
impl<F: Future> FusedStream for FuturesUnordered<F> {
    fn is_terminated(&self) -> bool {
        self.is_empty()
    }
}

impl<F> FromIterator<F> for FuturesUnordered<F> {
    /// Constructs a new, empty [`FuturesUnordered`] with a fixed capacity that is the length of the iterator.
    ///
    /// # Example
    ///
    /// Making 1024 total HTTP requests, with a max concurrency of 128
    ///
    /// ```
    /// use futures::stream::StreamExt;
    /// use futures_buffered::FuturesUnordered;
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
    /// let mut queue: FuturesUnordered<_> = (0..128).map(|_| make_req(&mut rs)).collect();
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
        let iter = iter.into_iter();
        let mut this =
            FuturesUnordered::with_capacity(usize::max(iter.size_hint().0, MIN_CAPACITY));
        for fut in iter {
            this.push(fut);
        }
        this
    }
}

impl<Fut> fmt::Debug for FuturesUnordered<Fut> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FuturesUnordered")
            .field("queues", &self.queues)
            .field("len", &self.len)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::{cell::Cell, future::ready, time::Duration};
    use futures::StreamExt;
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

        let mut buffer = FuturesUnordered::new();
        buffer.push(sleep(&c, Duration::from_secs(1)));
        futures::executor::block_on(buffer.next());

        drop(buffer);
        assert_eq!(c.into_inner(), 2);
    }

    #[test]
    fn len() {
        let mut buffer = FuturesUnordered::with_capacity(1);

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

        buffer.push(ready(()));

        assert_eq!(buffer.len(), 2);
        assert!(!buffer.is_empty());
        assert_eq!(buffer.capacity(), 3);
        assert_eq!(buffer.size_hint(), (2, Some(2)));
        assert!(!buffer.is_terminated());

        futures::executor::block_on(buffer.next());
        futures::executor::block_on(buffer.next());

        assert_eq!(buffer.len(), 0);
        assert!(buffer.is_empty());
        assert_eq!(buffer.capacity(), 3);
        assert_eq!(buffer.size_hint(), (0, Some(0)));
        assert!(buffer.is_terminated());
    }

    #[test]
    fn from_iter() {
        let buffer = FuturesUnordered::from_iter((0..10).map(|_| ready(())));

        assert_eq!(buffer.len(), 10);
        assert_eq!(buffer.capacity(), 32);
        assert_eq!(buffer.size_hint(), (10, Some(10)));
    }

    #[test]
    fn multi() {
        fn wait(count: &Cell<usize>, i: usize) -> PollCounter<'_, Sleep> {
            sleep(count, Duration::from_secs(1) / (i as u32 % 10 + 5))
        }

        let c = Cell::new(0);

        let mut buffer = FuturesUnordered::with_capacity(1);
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

        let mut buffer = FuturesUnordered::with_capacity(1);
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
}
