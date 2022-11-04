use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Weak},
    task::{Context, Poll},
};

// use crate::sparse::SparseSet;
use crate::{atomic_sparse::AtomicSparseSet, project_slice, InnerWaker, Shared};
use futures_util::{task::AtomicWaker, Stream};
use pin_project_lite::pin_project;

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
    pub(crate) slots: AtomicSparseSet,
    pub(crate) inner: Pin<Box<[Task<F>]>>,
    pub(crate) shared: Arc<Shared>,
}
impl<F> Unpin for FuturesUnorderedBounded<F> {}

pin_project!(
    pub(crate) struct Task<F> {
        #[pin]
        pub(crate) slot: Option<F>,
        pub(crate) waker: Arc<InnerWaker>,
    }
);

impl<F> FuturesUnorderedBounded<F> {
    /// Constructs a new, empty [`FuturesUnorderedBounded`] with the given fixed capacity.
    ///
    /// The returned [`FuturesUnorderedBounded`] does not contain any futures.
    /// In this state, [`FuturesUnorderedBounded::poll_next`](Stream::poll_next) will
    /// return [`Poll::Ready(None)`](Poll::Ready).
    pub fn new(cap: usize) -> Self {
        // create the shared data that is part of the queue and
        // the wakers
        let shared = Arc::new(Shared {
            ready: AtomicSparseSet::new(cap),
            waker: AtomicWaker::new(),
        });

        // create the task buffer + slot stack
        let mut v: Vec<Task<F>> = Vec::with_capacity(cap);
        let mut slots = AtomicSparseSet::new(cap);
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

    /// Push a future into the set.
    ///
    /// This method adds the given future to the set. This method will not
    /// call [`poll`](core::future::Future::poll) on the submitted future. The caller must
    /// ensure that [`FuturesUnorderedBounded::poll_next`](Stream::poll_next) is called
    /// in order to receive wake-up notifications for the given future.
    ///
    /// # Panics
    /// This method will panic if the buffer is currently full. See [`FuturesUnorderedBounded::try_push`] to get a result instead
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
        let mut inner: Pin<&mut [Task<F>]> = self.inner.as_mut();
        if let Some(i) = self.slots.pop() {
            // if there's a slot available, push the future in
            // and mark it as ready for polling
            project_slice(inner.as_mut(), i)
                .project()
                .slot
                .set(Some(fut));
            self.shared.ready.push_sync(i);
            Ok(())
        } else {
            // if no slots, return back the future
            Err(fut)
        }
    }

    /// Returns `true` if the set contains no futures.
    pub fn is_empty(&self) -> bool {
        self.inner.len() == self.slots.len()
    }

    /// Returns the number of futures contained in the set.
    ///
    /// This represents the total number of in-flight futures.
    pub fn len(&self) -> usize {
        self.inner.len() - self.slots.len()
    }

    /// Returns the number of futures that can be contained in the set.
    pub fn capacity(&self) -> usize {
        self.inner.len()
    }
}

impl<F: Future> FuturesUnorderedBounded<F> {
    pub(crate) fn poll_inner(&mut self, cx: &mut Context<'_>) -> Poll<Option<(usize, F::Output)>> {
        self.shared.waker.register(cx.waker());
        while let Some(i) = self.shared.ready.pop_sync() {
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
        let iter = iter.into_iter();

        // determine a suitable initial capacity
        let cap = match iter.size_hint() {
            (_, Some(max)) => max,
            (min, None) => min,
        };

        // store the futures in our task list
        let mut v: Vec<Task<F>> = Vec::with_capacity(cap);
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
        let mut slots = AtomicSparseSet::new(cap);

        // register the shared state on our tasks
        for (i, task) in v.iter_mut().enumerate() {
            slots.push(i);
            shared.ready.push_sync(i);

            // we know that we haven't cloned this arc before since it was created
            // just a few lines above
            Arc::get_mut(&mut task.waker).unwrap().shared = Arc::downgrade(&shared);
        }

        // create the queue
        Self {
            inner: v.into_boxed_slice().into(),
            shared,
            slots,
        }
    }
}
