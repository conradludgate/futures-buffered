use alloc::alloc::handle_alloc_error;
use alloc::boxed::Box;
use core::alloc::Layout;
use core::borrow;
use core::cmp::Ordering;
use core::convert::From;
use core::fmt;
use core::hash::{Hash, Hasher};
use core::marker::PhantomData;
use core::mem::ManuallyDrop;
use core::ops::Deref;
use core::ptr::{self, NonNull};
use core::sync::atomic;
use core::sync::atomic::Ordering::{Acquire, Relaxed, Release};

use crate::triomphe::{abort, HeaderSlice};

/// A soft limit on the amount of references that may be made to an `Arc`.
///
/// Going above this limit will abort your program (although not
/// necessarily) at _exactly_ `MAX_REFCOUNT + 1` references.
const MAX_REFCOUNT: usize = (isize::MAX) as usize;

/// The object allocated by an `Arc<T>`
#[repr(C)]
pub(crate) struct ArcInner<T: ?Sized> {
    pub(crate) count: atomic::AtomicUsize,
    pub(crate) data: T,
}

unsafe impl<T: ?Sized + Sync + Send> Send for ArcInner<T> {}
unsafe impl<T: ?Sized + Sync + Send> Sync for ArcInner<T> {}

/// An atomically reference counted shared pointer
///
/// See the documentation for [`Arc`] in the standard library. Unlike the
/// standard library `Arc`, this `Arc` does not support weak reference counting.
///
/// [`Arc`]: https://doc.rust-lang.org/stable/std/sync/struct.Arc.html
#[repr(transparent)]
pub struct Arc<T: ?Sized> {
    pub(crate) p: ptr::NonNull<ArcInner<T>>,
    pub(crate) phantom: PhantomData<T>,
}

unsafe impl<T: ?Sized + Sync + Send> Send for Arc<T> {}
unsafe impl<T: ?Sized + Sync + Send> Sync for Arc<T> {}

impl<T> Arc<T> {
    /// Construct an `Arc<T>`
    #[inline]
    pub fn new(data: T) -> Self {
        let ptr = Box::into_raw(Box::new(ArcInner {
            count: atomic::AtomicUsize::new(1),
            data,
        }));

        unsafe {
            Arc {
                p: ptr::NonNull::new_unchecked(ptr),
                phantom: PhantomData,
            }
        }
    }
}

impl<T: ?Sized> Arc<T> {
    #[inline]
    pub(super) fn into_raw_inner(this: Self) -> *mut ArcInner<T> {
        let this = ManuallyDrop::new(this);
        this.ptr()
    }

    /// Construct an `Arc` from an allocated `ArcInner`.
    /// # Safety
    /// The `ptr` must point to a valid instance, allocated by an `Arc`. The reference could will
    /// not be modified.
    pub(super) unsafe fn from_raw_inner(ptr: *mut ArcInner<T>) -> Self {
        Arc {
            p: ptr::NonNull::new_unchecked(ptr),
            phantom: PhantomData,
        }
    }

    #[inline]
    pub(super) fn inner(&self) -> &ArcInner<T> {
        // This unsafety is ok because while this arc is alive we're guaranteed
        // that the inner pointer is valid. Furthermore, we know that the
        // `ArcInner` structure itself is `Sync` because the inner data is
        // `Sync` as well, so we're ok loaning out an immutable pointer to these
        // contents.
        unsafe { &*self.ptr() }
    }

    // Non-inlined part of `drop`. Just invokes the destructor.
    #[inline(never)]
    unsafe fn drop_slow(&mut self) {
        let _ = Box::from_raw(self.ptr());
    }

    /// Returns `true` if the two `Arc`s point to the same allocation in a vein similar to
    /// [`ptr::eq`]. This function ignores the metadata of  `dyn Trait` pointers.
    #[inline]
    pub fn ptr_eq(this: &Self, other: &Self) -> bool {
        ptr::addr_eq(this.ptr(), other.ptr())
    }

    pub(crate) fn ptr(&self) -> *mut ArcInner<T> {
        self.p.as_ptr()
    }

    /// Allocates an `ArcInner<T>` with sufficient space for
    /// a possibly-unsized inner value where the value has the layout provided.
    ///
    /// The function `mem_to_arcinner` is called with the data pointer
    /// and must return back a (potentially fat)-pointer for the `ArcInner<T>`.
    ///
    /// ## Safety
    ///
    /// `mem_to_arcinner` must return the same pointer, the only things that can change are
    /// - its type
    /// - its metadata
    ///
    /// `value_layout` must be correct for `T`.
    #[allow(unused_unsafe)]
    pub(super) unsafe fn allocate_for_layout(
        value_layout: Layout,
        mem_to_arcinner: impl FnOnce(*mut u8) -> *mut ArcInner<T>,
    ) -> NonNull<ArcInner<T>> {
        let layout = Layout::new::<ArcInner<()>>()
            .extend(value_layout)
            .unwrap()
            .0
            .pad_to_align();

        // Safety: we propagate safety requirements to the caller
        unsafe {
            Arc::try_allocate_for_layout(value_layout, mem_to_arcinner)
                .unwrap_or_else(|_| handle_alloc_error(layout))
        }
    }

    /// Allocates an `ArcInner<T>` with sufficient space for
    /// a possibly-unsized inner value where the value has the layout provided,
    /// returning an error if allocation fails.
    ///
    /// The function `mem_to_arcinner` is called with the data pointer
    /// and must return back a (potentially fat)-pointer for the `ArcInner<T>`.
    ///
    /// ## Safety
    ///
    /// `mem_to_arcinner` must return the same pointer, the only things that can change are
    /// - its type
    /// - its metadata
    ///
    /// `value_layout` must be correct for `T`.
    #[allow(unused_unsafe)]
    unsafe fn try_allocate_for_layout(
        value_layout: Layout,
        mem_to_arcinner: impl FnOnce(*mut u8) -> *mut ArcInner<T>,
    ) -> Result<NonNull<ArcInner<T>>, ()> {
        let layout = Layout::new::<ArcInner<()>>()
            .extend(value_layout)
            .unwrap()
            .0
            .pad_to_align();

        let ptr = NonNull::new(alloc::alloc::alloc(layout)).ok_or(())?;

        // Initialize the ArcInner
        let inner = mem_to_arcinner(ptr.as_ptr());
        debug_assert_eq!(unsafe { Layout::for_value(&*inner) }, layout);

        unsafe {
            ptr::write(&mut (*inner).count, atomic::AtomicUsize::new(1));
        }

        // Safety: `ptr` is checked to be non-null,
        //         `inner` is the same as `ptr` (per the safety requirements of this function)
        unsafe { Ok(NonNull::new_unchecked(inner)) }
    }
}

impl<H, T> Arc<HeaderSlice<H, [T]>> {
    pub(super) fn allocate_for_header_and_slice(
        len: usize,
    ) -> NonNull<ArcInner<HeaderSlice<H, [T]>>> {
        let layout = Layout::new::<H>()
            .extend(Layout::array::<T>(len).unwrap())
            .unwrap()
            .0
            .pad_to_align();

        unsafe {
            // Safety:
            // - the provided closure does not change the pointer (except for meta & type)
            // - the provided layout is valid for `HeaderSlice<H, [T]>`
            Arc::allocate_for_layout(layout, |mem| {
                // Synthesize the fat pointer. We do this by claiming we have a direct
                // pointer to a [T], and then changing the type of the borrow. The key
                // point here is that the length portion of the fat pointer applies
                // only to the number of elements in the dynamically-sized portion of
                // the type, so the value will be the same whether it points to a [T]
                // or something else with a [T] as its last member.
                let fake_slice = ptr::slice_from_raw_parts_mut(mem as *mut T, len);
                fake_slice as *mut ArcInner<HeaderSlice<H, [T]>>
            })
        }
    }
}

impl<T: ?Sized> Clone for Arc<T> {
    #[inline]
    fn clone(&self) -> Self {
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
        let old_size = self.inner().count.fetch_add(1, Relaxed);

        // However we need to guard against massive refcounts in case someone
        // is `mem::forget`ing Arcs. If we don't do this the count can overflow
        // and users will use-after free. We racily saturate to `isize::MAX` on
        // the assumption that there aren't ~2 billion threads incrementing
        // the reference count at once. This branch will never be taken in
        // any realistic program.
        //
        // We abort because such a program is incredibly degenerate, and we
        // don't care to support it.
        if old_size > MAX_REFCOUNT {
            abort();
        }

        unsafe {
            Arc {
                p: ptr::NonNull::new_unchecked(self.ptr()),
                phantom: PhantomData,
            }
        }
    }
}

impl<T: ?Sized> Deref for Arc<T> {
    type Target = T;

    #[inline]
    fn deref(&self) -> &T {
        &self.inner().data
    }
}

impl<T: ?Sized> Arc<T> {
    fn drop_inner(&mut self) {
        // Because `fetch_sub` is already atomic, we do not need to synchronize
        // with other threads unless we are going to delete the object.
        if self.inner().count.fetch_sub(1, Release) != 1 {
            return;
        }

        // FIXME(bholley): Use the updated comment when [2] is merged.
        //
        // This load is needed to prevent reordering of use of the data and
        // deletion of the data.  Because it is marked `Release`, the decreasing
        // of the reference count synchronizes with this `Acquire` load. This
        // means that use of the data happens before decreasing the reference
        // count, which happens before this load, which happens before the
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
        // [1]: (www.boost.org/doc/libs/1_55_0/doc/html/atomic/usage_examples.html)
        // [2]: https://github.com/rust-lang/rust/pull/41714
        self.inner().count.load(Acquire);

        unsafe {
            self.drop_slow();
        }
    }
}

impl<T: ?Sized> Drop for Arc<T> {
    #[inline]
    fn drop(&mut self) {
        self.drop_inner();
    }
}

impl<T: ?Sized + PartialEq> PartialEq for Arc<T> {
    fn eq(&self, other: &Arc<T>) -> bool {
        // TODO: pointer equality is incorrect if `T` is not `Eq`.
        Self::ptr_eq(self, other) || *(*self) == *(*other)
    }

    #[allow(clippy::partialeq_ne_impl)]
    fn ne(&self, other: &Arc<T>) -> bool {
        !Self::ptr_eq(self, other) && *(*self) != *(*other)
    }
}

impl<T: ?Sized + PartialOrd> PartialOrd for Arc<T> {
    fn partial_cmp(&self, other: &Arc<T>) -> Option<Ordering> {
        (**self).partial_cmp(&**other)
    }

    fn lt(&self, other: &Arc<T>) -> bool {
        *(*self) < *(*other)
    }

    fn le(&self, other: &Arc<T>) -> bool {
        *(*self) <= *(*other)
    }

    fn gt(&self, other: &Arc<T>) -> bool {
        *(*self) > *(*other)
    }

    fn ge(&self, other: &Arc<T>) -> bool {
        *(*self) >= *(*other)
    }
}

impl<T: ?Sized + Ord> Ord for Arc<T> {
    fn cmp(&self, other: &Arc<T>) -> Ordering {
        (**self).cmp(&**other)
    }
}

impl<T: ?Sized + Eq> Eq for Arc<T> {}

impl<T: ?Sized + fmt::Display> fmt::Display for Arc<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Display::fmt(&**self, f)
    }
}

impl<T: ?Sized + fmt::Debug> fmt::Debug for Arc<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Debug::fmt(&**self, f)
    }
}

impl<T: ?Sized> fmt::Pointer for Arc<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Pointer::fmt(&self.ptr(), f)
    }
}

impl<T: Default> Default for Arc<T> {
    #[inline]
    fn default() -> Arc<T> {
        Arc::new(Default::default())
    }
}

impl<T: ?Sized + Hash> Hash for Arc<T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        (**self).hash(state)
    }
}

impl<T> From<T> for Arc<T> {
    #[inline]
    fn from(t: T) -> Self {
        Arc::new(t)
    }
}

impl<T: ?Sized> borrow::Borrow<T> for Arc<T> {
    #[inline]
    fn borrow(&self) -> &T {
        self
    }
}

impl<T: ?Sized> AsRef<T> for Arc<T> {
    #[inline]
    fn as_ref(&self) -> &T {
        self
    }
}

#[cfg(test)]
mod tests {
    use crate::triomphe::arc::Arc;

    #[test]
    fn arc_eq_and_cmp() {
        [
            [("*", &b"AB"[..]), ("*", &b"ab"[..])],
            [("*", &b"AB"[..]), ("*", &b"a"[..])],
            [("*", &b"A"[..]), ("*", &b"ab"[..])],
            [("A", &b"*"[..]), ("a", &b"*"[..])],
            [("a", &b"*"[..]), ("A", &b"*"[..])],
            [("AB", &b"*"[..]), ("a", &b"*"[..])],
            [("A", &b"*"[..]), ("ab", &b"*"[..])],
        ]
        .iter()
        .for_each(|[lt @ (lh, ls), rt @ (rh, rs)]| {
            let l = Arc::from_header_and_slice(lh, ls);
            let r = Arc::from_header_and_slice(rh, rs);

            assert_eq!(l, l);
            assert_eq!(r, r);

            assert_ne!(l, r);
            assert_ne!(r, l);

            assert_eq!(l <= l, lt <= lt, "{lt:?} <= {lt:?}");
            assert_eq!(l >= l, lt >= lt, "{lt:?} >= {lt:?}");

            assert_eq!(l < l, lt < lt, "{lt:?} < {lt:?}");
            assert_eq!(l > l, lt > lt, "{lt:?} > {lt:?}");

            assert_eq!(r <= r, rt <= rt, "{rt:?} <= {rt:?}");
            assert_eq!(r >= r, rt >= rt, "{rt:?} >= {rt:?}");

            assert_eq!(r < r, rt < rt, "{rt:?} < {rt:?}");
            assert_eq!(r > r, rt > rt, "{rt:?} > {rt:?}");

            assert_eq!(l < r, lt < rt, "{lt:?} < {rt:?}");
            assert_eq!(r > l, rt > lt, "{rt:?} > {lt:?}");
        })
    }

    #[test]
    fn arc_eq_and_partial_cmp() {
        [
            [(0.0, &[0.0, 0.0][..]), (1.0, &[0.0, 0.0][..])],
            [(1.0, &[0.0, 0.0][..]), (0.0, &[0.0, 0.0][..])],
            [(0.0, &[0.0][..]), (0.0, &[0.0, 0.0][..])],
            [(0.0, &[0.0, 0.0][..]), (0.0, &[0.0][..])],
            [(0.0, &[1.0, 2.0][..]), (0.0, &[10.0, 20.0][..])],
        ]
        .iter()
        .for_each(|[lt @ (lh, ls), rt @ (rh, rs)]| {
            let l = Arc::from_header_and_slice(lh, ls);
            let r = Arc::from_header_and_slice(rh, rs);

            assert_eq!(l, l);
            assert_eq!(r, r);

            assert_ne!(l, r);
            assert_ne!(r, l);

            assert_eq!(l <= l, lt <= lt, "{lt:?} <= {lt:?}");
            assert_eq!(l >= l, lt >= lt, "{lt:?} >= {lt:?}");

            assert_eq!(l < l, lt < lt, "{lt:?} < {lt:?}");
            assert_eq!(l > l, lt > lt, "{lt:?} > {lt:?}");

            assert_eq!(r <= r, rt <= rt, "{rt:?} <= {rt:?}");
            assert_eq!(r >= r, rt >= rt, "{rt:?} >= {rt:?}");

            assert_eq!(r < r, rt < rt, "{rt:?} < {rt:?}");
            assert_eq!(r > r, rt > rt, "{rt:?} > {rt:?}");

            assert_eq!(l < r, lt < rt, "{lt:?} < {rt:?}");
            assert_eq!(r > l, rt > lt, "{rt:?} > {lt:?}");
        })
    }

    #[test]
    fn test_partial_eq_bug() {
        let float = f32::NAN;
        assert_ne!(float, float);
        let arc = Arc::new(f32::NAN);
        // TODO: this is a bug.
        assert_eq!(arc, arc);
    }

    #[allow(dead_code)]
    const fn is_partial_ord<T: ?Sized + PartialOrd>() {}

    #[allow(dead_code)]
    const fn is_ord<T: ?Sized + Ord>() {}

    // compile-time check that PartialOrd/Ord is correctly derived
    const _: () = is_partial_ord::<Arc<f64>>();
    const _: () = is_ord::<Arc<u64>>();
}
