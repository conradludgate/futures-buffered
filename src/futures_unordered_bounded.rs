use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{atomic_sparse::AtomicSparseSet, project_slice};
use futures_util::Stream;

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
/// FuturesUnordered         time:   [204.44 ms 205.47 ms 206.61 ms]
/// FuturesUnorderedBounded  time:   [191.92 ms 192.52 ms 193.15 ms]
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
///     count:    4
///     alloc:    14400 B
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
    pub(crate) inner: Pin<Box<[Option<F>]>>,
    pub(crate) shared: arc_slice::ArcSlice,
}
impl<F> Unpin for FuturesUnorderedBounded<F> {}

impl<F> FuturesUnorderedBounded<F> {
    /// Constructs a new, empty [`FuturesUnorderedBounded`] with the given fixed capacity.
    ///
    /// The returned [`FuturesUnorderedBounded`] does not contain any futures.
    /// In this state, [`FuturesUnorderedBounded::poll_next`](Stream::poll_next) will
    /// return [`Poll::Ready(None)`](Poll::Ready).
    pub fn new(cap: usize) -> Self {
        // create the shared data that is part of the queue and
        // the wakers
        let shared = arc_slice::ArcSlice::new(cap);

        // create the task buffer + slot stack
        let mut v: Vec<Option<F>> = Vec::with_capacity(cap);
        let mut slots = AtomicSparseSet::new(cap);
        for i in 0..cap {
            v.push(None);
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
        let mut inner: Pin<&mut [Option<F>]> = self.inner.as_mut();
        if let Some(i) = self.slots.pop() {
            // if there's a slot available, push the future in
            // and mark it as ready for polling
            project_slice(inner.as_mut(), i).set(Some(fut));
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
            let mut task = project_slice(inner.as_mut(), i);

            let waker = self.shared.get(i).waker();
            let mut cx = Context::from_waker(&waker);

            let res = match task.as_mut().as_pin_mut() {
                // poll the current task
                Some(fut) => fut.poll(&mut cx),
                None => continue,
            };

            if let Poll::Ready(x) = res {
                task.set(None);
                self.slots.push(i);
                return Poll::Ready(Some((i, x)));
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
        let iter = iter.into_iter();

        // determine a suitable initial capacity
        let cap = match iter.size_hint() {
            (_, Some(max)) => max,
            (min, None) => min,
        };

        // store the futures in our task list
        let mut v: Vec<Option<F>> = Vec::with_capacity(cap);
        for fut in iter {
            v.push(Some(fut));
        }

        // determine the actual capacity and create the shared state
        let cap = v.len();
        let slots = AtomicSparseSet::new(cap);
        let mut shared = arc_slice::ArcSlice::new(cap);
        // we know that we haven't cloned this arc before, since it was created just above
        let meta = unsafe { shared.get_mut_unchecked() };

        // register the shared state on our tasks
        for i in 0..cap {
            meta.ready.push(i);
        }

        // create the queue
        Self {
            inner: v.into_boxed_slice().into(),
            shared,
            slots,
        }
    }
}

mod arc_slice {
    use std::{
        alloc::{dealloc, handle_alloc_error, Layout},
        marker::PhantomData,
        mem::{align_of, ManuallyDrop},
        ops::Deref,
        process::abort,
        ptr::{self, drop_in_place, NonNull},
        sync::atomic::{self, AtomicUsize},
        task::{RawWaker, RawWakerVTable, Waker},
    };

    use futures_util::task::AtomicWaker;

    use crate::atomic_sparse::AtomicSparseSet;

    pub(crate) struct ArcSlice {
        ptr: NonNull<ArcSliceInner>,
        phantom: PhantomData<ArcSliceInner>,
    }
    pub(crate) struct ArcSlot {
        ptr: NonNull<usize>,
        phantom: PhantomData<ArcSliceInner>,
    }

    impl Deref for ArcSlice {
        type Target = ArcSliceInnerMeta;

        fn deref(&self) -> &Self::Target {
            &unsafe { self.ptr.as_ref() }.meta
        }
    }
    impl ArcSlice {
        pub(crate) fn get(&self, index: usize) -> ArcSlot {
            self.inc_strong();
            let ptr: *mut ArcSliceInner = NonNull::as_ptr(self.ptr);

            // SAFETY: This cannot go through Deref::deref or RcBoxPtr::inner because
            // this is required to retain raw/mut provenance such that e.g. `get_mut` can
            // write through the pointer after the Rc is recovered through `from_raw`.
            let slice = unsafe { ptr::addr_of_mut!((*ptr).slice) } as *mut usize;
            let slot = unsafe { slice.add(index) };
            ArcSlot {
                ptr: unsafe { NonNull::new_unchecked(slot) },
                phantom: PhantomData,
            }
        }
    }

    impl ArcSlot {
        unsafe fn meta_raw_ptr(ptr: *mut usize) -> *mut ArcSliceInnerMeta {
            fn padding_needed_for(layout: &Layout, align: usize) -> usize {
                let len = layout.size();

                // Rounded up value is:
                //   len_rounded_up = (len + align - 1) & !(align - 1);
                // and then we return the padding difference: `len_rounded_up - len`.
                //
                // We use modular arithmetic throughout:
                //
                // 1. align is guaranteed to be > 0, so align - 1 is always
                //    valid.
                //
                // 2. `len + align - 1` can overflow by at most `align - 1`,
                //    so the &-mask with `!(align - 1)` will ensure that in the
                //    case of overflow, `len_rounded_up` will itself be 0.
                //    Thus the returned padding, when added to `len`, yields 0,
                //    which trivially satisfies the alignment `align`.
                //
                // (Of course, attempts to allocate blocks of memory whose
                // size and padding overflow in the above manner should cause
                // the allocator to yield an error anyway.)

                let len_rounded_up =
                    len.wrapping_add(align).wrapping_sub(1) & !align.wrapping_sub(1);
                len_rounded_up.wrapping_sub(len)
            }

            let index = *ptr;
            let slice_start = ptr.sub(index);

            let layout = Layout::new::<ArcSliceInnerMeta>();
            let offset = layout.size() + padding_needed_for(&layout, align_of::<usize>());

            unsafe { slice_start.cast::<u8>().sub(offset) }.cast::<ArcSliceInnerMeta>()
        }
        unsafe fn meta_raw<'a>(ptr: *const usize) -> &'a ArcSliceInnerMeta {
            unsafe { &*Self::meta_raw_ptr(ptr as *mut usize) }
        }

        fn meta(&self) -> &ArcSliceInnerMeta {
            unsafe { Self::meta_raw(self.ptr.as_ptr()) }
        }

        pub(crate) fn waker(self) -> Waker {
            unsafe { Waker::from_raw(self.raw_waker()) }
        }
        fn raw_waker(self) -> RawWaker {
            // Increment the reference count of the arc to clone it.
            unsafe fn clone_waker(waker: *const ()) -> RawWaker {
                ArcSlot::meta_raw(waker.cast()).inc_strong();
                RawWaker::new(
                    waker,
                    &RawWakerVTable::new(clone_waker, wake, wake_by_ref, drop_waker),
                )
            }

            unsafe fn wake(waker: *const ()) {
                wake_by_ref(waker);
                drop_waker(waker);
            }

            unsafe fn wake_by_ref(waker: *const ()) {
                let slot = waker.cast();
                let meta = ArcSlot::meta_raw(slot);
                meta.ready.push_sync(*slot);
                meta.waker.wake();
            }

            // Decrement the reference count of the Arc on drop
            unsafe fn drop_waker(waker: *const ()) {
                drop(ArcSlot {
                    ptr: NonNull::new_unchecked(waker as *mut _),
                    phantom: PhantomData,
                });
            }

            let waker = ManuallyDrop::new(self);

            RawWaker::new(
                waker.ptr.as_ptr() as *const (),
                &RawWakerVTable::new(clone_waker, wake, wake_by_ref, drop_waker),
            )
        }
    }

    impl ArcSliceInnerMeta {
        fn inc_strong(&self) {
            // Using a relaxed ordering is alright here, as knowledge of the
            // original reference prevents other threads from erroneously deleting
            // the object.
            //
            // As explained in the [Boost documentation][1], Increasing the
            // reference counter can always be done with memory_order_relaxed: New
            // references to an object can only be formed from an existing
            // reference, and passing an existing reference from one thread to
            // another must already provide any required synchronization.
            //
            // [1]: (www.boost.org/doc/libs/1_55_0/doc/html/atomic/usage_examples.html)
            let old_size = self
                .strong
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

            // However we need to guard against massive refcounts in case someone is `mem::forget`ing
            // Arcs. If we don't do this the count can overflow and users will use-after free. This
            // branch will never be taken in any realistic program. We abort because such a program is
            // incredibly degenerate, and we don't care to support it.
            //
            // This check is not 100% water-proof: we error when the refcount grows beyond `isize::MAX`.
            // But we do that check *after* having done the increment, so there is a chance here that
            // the worst already happened and we actually do overflow the `usize` counter. However, that
            // requires the counter to grow from `isize::MAX` to `usize::MAX` between the increment
            // above and the `abort` below, which seems exceedingly unlikely.
            if old_size > (isize::MAX) as usize {
                abort();
            }
        }
        fn dec_strong(&self) -> bool {
            // Because `fetch_sub` is already atomic, we do not need to synchronize
            // with other threads unless we are going to delete the object. This
            // same logic applies to the below `fetch_sub` to the `weak` count.
            let old_size = self
                .strong
                .fetch_sub(1, std::sync::atomic::Ordering::Release);
            if old_size != 1 {
                return false;
            }

            // This fence is needed to prevent reordering of use of the data and
            // deletion of the data.  Because it is marked `Release`, the decreasing
            // of the reference count synchronizes with this `Acquire` fence. This
            // means that use of the data happens before decreasing the reference
            // count, which happens before this fence, which happens before the
            // deletion of the data.
            //
            // As explained in the [Boost documentation][1],
            //
            // > It is important to enforce any possible access to the object in one
            // > thread (through an existing reference) to *happen before* deleting
            // > the object in a different thread. This is achieved by a "release"
            // > operation after dropping a reference (any access to the object
            // > through this reference must obviously happened before), and an
            // > "acquire" operation before deleting the object.
            //
            // In particular, while the contents of an Arc are usually immutable, it's
            // possible to have interior writes to something like a Mutex<T>. Since a
            // Mutex is not acquired when it is deleted, we can't rely on its
            // synchronization logic to make writes in thread A visible to a destructor
            // running in thread B.
            //
            // Also note that the Acquire fence here could probably be replaced with an
            // Acquire load, which could improve performance in highly-contended
            // situations. See [2].
            //
            // [1]: (www.boost.org/doc/libs/1_55_0/doc/html/atomic/usage_examples.html)
            // [2]: (https://github.com/rust-lang/rust/pull/41714)
            atomic::fence(atomic::Ordering::Acquire);
            true
        }
    }

    impl Drop for ArcSlot {
        fn drop(&mut self) {
            let meta = self.meta();
            if meta.dec_strong() {
                let layout = ArcSlice::layout(meta.capacity);
                unsafe {
                    let p = ArcSlot::meta_raw_ptr(self.ptr.as_ptr());
                    drop_in_place(p);
                    dealloc(p.cast(), layout);
                }
            }
        }
    }

    impl Drop for ArcSlice {
        fn drop(&mut self) {
            if self.dec_strong() {
                let layout = ArcSlice::layout(self.capacity);
                debug_assert_eq!(unsafe { Layout::for_value(self.ptr.as_ref()) }, layout);
                unsafe {
                    let p = self.ptr.as_ptr();
                    drop_in_place(p);
                    dealloc(p.cast(), layout);
                }
            }
        }
    }

    // This is repr(C) to future-proof against possible field-reordering, which
    // would interfere with otherwise safe [into|from]_raw() of transmutable
    // inner types.
    #[repr(C)]
    pub(crate) struct ArcSliceInner {
        pub(crate) meta: ArcSliceInnerMeta,
        slice: [usize],
    }

    #[repr(C)]
    pub(crate) struct ArcSliceInnerMeta {
        strong: AtomicUsize,
        pub(crate) ready: AtomicSparseSet,
        pub(crate) waker: AtomicWaker,
        capacity: usize,
    }

    impl ArcSlice {
        pub(crate) unsafe fn get_mut_unchecked(&mut self) -> &mut ArcSliceInnerMeta {
            &mut self.ptr.as_mut().meta
        }

        /// Allocates an `ArcInner<T>` with sufficient space for
        /// a possibly-unsized inner value where the value has the layout provided.
        pub(crate) fn new(cap: usize) -> Self {
            // code taken and modified from `Arc::allocate_for_layout`

            let arc_slice_layout = Self::layout(cap);

            // safety: layout size is > 0 because it has at least 7 usizes
            // in the metadata alone
            let ptr = unsafe { std::alloc::alloc(arc_slice_layout) };
            if ptr.is_null() {
                handle_alloc_error(arc_slice_layout)
            }

            // Initialize the ArcInner
            let inner =
                ptr::slice_from_raw_parts_mut(ptr.cast::<usize>(), cap) as *mut ArcSliceInner;
            debug_assert_eq!(unsafe { Layout::for_value(&*inner) }, arc_slice_layout);

            unsafe {
                ptr::write(&mut (*inner).meta.strong, AtomicUsize::new(1));
                ptr::write(&mut (*inner).meta.ready, AtomicSparseSet::new(cap));
                ptr::write(&mut (*inner).meta.waker, AtomicWaker::new());
                ptr::write(&mut (*inner).meta.capacity, cap);

                for i in 0..cap {
                    ptr::write(&mut (*inner).slice[i], i);
                }
            }

            Self {
                ptr: unsafe { NonNull::new_unchecked(inner) },
                phantom: PhantomData,
            }
        }

        fn layout(cap: usize) -> Layout {
            let padded = Layout::new::<usize>().pad_to_align();
            let alloc_size = padded.size().checked_mul(cap).unwrap();
            let slice_layout =
                Layout::from_size_align(alloc_size, Layout::new::<usize>().align()).unwrap();

            Layout::new::<ArcSliceInnerMeta>()
                .extend(slice_layout)
                .unwrap()
                .0
                .pad_to_align()
        }
    }
}
