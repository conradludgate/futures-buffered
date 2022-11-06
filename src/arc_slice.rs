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

/// [`ArcSlice`] is a fun optimisation. For `FuturesUnorderedBounded`, we have `n` slots for futures,
/// and we create a separate context when polling each individual future to avoid having n^2 polling.
///
/// Originally, we pre-allocated `n` `Arc<Wake>` types and stored those along side the future slots.
/// These wakers would have a `Weak` pointing to some shared state, as well as an index for which slot this
/// waker is associated with.
///
/// [`RawWaker`] only gives us 1 pointer worth of data to play with - but we need 2. So unfortunately we needed
/// the extra allocations here
///
/// ... unless we hack around a little bit!
///
/// [`ArcSlice`] represents the shared state, as well as having a long tail of indices. The layout is as follows
/// ```text
/// [ strong_count | ready_set | waker | 0 | 1 | 2 | 3 | ... ]
/// ```
///
/// [`ArcSlot`] represents our [`RawWaker`]. It points to one of the numbers in the list.
/// Since the layouts of the internals are fixed (`repr(C)`) - we can count back from the index to find
/// the shared data.
///
/// For example, if we have an ArcSlot pointing at the number 2, we can count back to pointer 2 `usize`s + [`ArcSliceInnerMeta`] to find
/// the start of the [`ArcSlice`], and then we can insert `2` into the `ready_set`, finally calling `waker.wake()`.
pub(crate) struct ArcSlice {
    ptr: NonNull<ArcSliceInner>,
    phantom: PhantomData<ArcSliceInner>,
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
}

pub(crate) struct ArcSlot {
    ptr: NonNull<usize>,
    phantom: PhantomData<ArcSliceInner>,
}

const fn __assert_send_sync<T: Send + Sync>() {}
const _: () = {
    __assert_send_sync::<ArcSliceInnerMeta>();
    __assert_send_sync::<usize>();

    // SAFETY: The contents of the ArcSlice are Send+Sync
    unsafe impl Send for ArcSlice {}
    unsafe impl Sync for ArcSlice {}
    unsafe impl Send for ArcSlot {}
    unsafe impl Sync for ArcSlot {}
};

impl Deref for ArcSlice {
    type Target = ArcSliceInnerMeta;

    fn deref(&self) -> &Self::Target {
        // This unsafety is ok because while this arc is alive we're guaranteed
        // that the inner pointer is valid. Furthermore, we know that the
        // `ArcInner` structure itself is `Sync` because the inner data is
        // `Sync` as well, so we're ok loaning out an immutable pointer to these
        // contents.
        &unsafe { self.ptr.as_ref() }.meta
    }
}

impl ArcSlice {
    /// Return an owned [`ArcSlot`] for this index.
    pub(crate) fn get(&self, index: usize) -> ArcSlot {
        self.inc_strong();
        let ptr: *mut ArcSliceInner = NonNull::as_ptr(self.ptr);

        // SAFETY: This cannot go through Deref::deref or RcBoxPtr::inner because
        // this is required to retain raw/mut provenance such that e.g. `get_mut` can
        // write through the pointer after the Rc is recovered through `from_raw`.
        let slot = unsafe { (ptr::addr_of_mut!((*ptr).slice) as *mut usize).add(index) };
        debug_assert_eq!(
            unsafe { *slot },
            index,
            "the slot should point at our index"
        );
        ArcSlot {
            // SAFETY: since this is derived from a `NonNull` pointer, we know this is also non-null
            ptr: unsafe { NonNull::new_unchecked(slot) },
            phantom: PhantomData,
        }
    }
}

impl ArcSlot {
    /// Traverses back the [`ArcSlice`] to find the [`ArcSliceInnerMeta`] pointer
    ///
    /// # Safety:
    /// `ptr` must be from an `ArcSlot` originally
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

            let len_rounded_up = len.wrapping_add(align).wrapping_sub(1) & !align.wrapping_sub(1);
            len_rounded_up.wrapping_sub(len)
        }

        let index = *ptr;
        let slice_start = ptr.sub(index);

        let layout = Layout::new::<ArcSliceInnerMeta>();
        let offset = layout.size() + padding_needed_for(&layout, align_of::<usize>());

        unsafe { slice_start.cast::<u8>().sub(offset) }.cast::<ArcSliceInnerMeta>()
    }

    /// Traverses back the [`ArcSlice`] to find the [`ArcSliceInnerMeta`] pointer
    ///
    /// # Safety:
    /// * `ptr` must be from an `ArcSlot` originally
    /// * The original `ArcSlot` must outlive `'a`
    unsafe fn meta_raw<'a>(ptr: *const usize) -> &'a ArcSliceInnerMeta {
        unsafe { &*Self::meta_raw_ptr(ptr as *mut usize) }
    }

    fn meta(&self) -> &ArcSliceInnerMeta {
        // SAFETY: `self.ptr` is clearly from an `ArcSlot`, and the output lifetime is the same
        // as the input lifetime
        unsafe { Self::meta_raw(self.ptr.as_ptr()) }
    }

    pub(crate) fn waker(self) -> Waker {
        static VTABLE: RawWakerVTable =
            RawWakerVTable::new(clone_waker, wake, wake_by_ref, drop_waker);

        // Increment the reference count of the arc to clone it.
        unsafe fn clone_waker(waker: *const ()) -> RawWaker {
            ArcSlot::meta_raw(waker.cast()).inc_strong();
            RawWaker::new(waker, &VTABLE)
        }

        // We don't need ownership. Just wake_by_ref and drop the waker
        unsafe fn wake(waker: *const ()) {
            wake_by_ref(waker);
            drop_waker(waker);
        }

        // Find the `ArcSliceInnerMeta` and push the current index value into it,
        // then call the stored waker to trigger a poll
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

        // we are transferring ownership into the RawWaker,
        // so don't drop this and decrement the strong count
        let waker = ManuallyDrop::new(self);

        let raw_waker = RawWaker::new(waker.ptr.as_ptr() as *const (), &VTABLE);
        unsafe { Waker::from_raw(raw_waker) }
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

/// Drops the internals of the [`ArcSlice`].
///
/// # Safety:
/// The pointer must point to a currently allocated [`ArcSlice`].
unsafe fn drop_inner(p: *mut ArcSliceInnerMeta, capacity: usize) {
    let layout = ArcSlice::layout(capacity);

    // SAFETY: the pointer points to an aligned and init instance of `ArcSliceInnerMeta`
    drop_in_place(p);

    // SAFETY: this pointer has been allocated in the global allocator with the given layout
    dealloc(p.cast(), layout);
}

impl Drop for ArcSlot {
    fn drop(&mut self) {
        let meta = self.meta();
        if meta.dec_strong() {
            unsafe {
                drop_inner(
                    ArcSlot::meta_raw_ptr(self.ptr.as_ptr()),
                    meta.ready.capacity(),
                )
            }
        }
    }
}

impl Drop for ArcSlice {
    fn drop(&mut self) {
        if self.dec_strong() {
            unsafe { drop_inner(self.ptr.as_ptr().cast(), self.ready.capacity()) }
        }
    }
}

impl ArcSlice {
    /// Gets a mut reference to the metadata of this [`ArcSlice`]
    ///
    /// # Safety
    /// This `ArcSlice` must have a strong count of 1
    pub(crate) unsafe fn get_mut_unchecked(&mut self) -> &mut ArcSliceInnerMeta {
        let meta = &mut self.ptr.as_mut().meta;
        debug_assert_eq!(
            *meta.strong.get_mut(),
            1,
            "strong count should be 1 for mut access"
        );
        meta
    }

    /// Allocates an `ArcInner<T>` with sufficient space for
    /// a possibly-unsized inner value where the value has the layout provided.
    pub(crate) fn new(cap: usize) -> Self {
        // code taken and modified from `Arc::allocate_for_layout`

        let arc_slice_layout = Self::layout(cap);

        // safety: layout size is > 0 because it has at least 7 usizes
        // in the metadata alone
        debug_assert!(std::mem::size_of::<ArcSliceInnerMeta>() > 0);
        let ptr = unsafe { std::alloc::alloc(arc_slice_layout) };
        if ptr.is_null() {
            handle_alloc_error(arc_slice_layout)
        }

        // Initialize the ArcInner
        let inner = ptr::slice_from_raw_parts_mut(ptr.cast::<usize>(), cap) as *mut ArcSliceInner;
        debug_assert_eq!(unsafe { Layout::for_value(&*inner) }, arc_slice_layout);

        // SAFETY:
        // The inner pointer is allocated and aligned, they just need to be initialised
        unsafe {
            let meta = ArcSliceInnerMeta {
                strong: AtomicUsize::new(1),
                ready: AtomicSparseSet::new(cap),
                waker: AtomicWaker::new(),
            };
            ptr::write(ptr::addr_of_mut!((*inner).meta), meta);
            for i in 0..cap {
                ptr::write(ptr::addr_of_mut!((*inner).slice[i]), i);
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
