use core::cmp::Ordering;
use core::ffi::c_void;
use core::fmt;
use core::hash::{Hash, Hasher};
use core::iter::{ExactSizeIterator, Iterator};
use core::marker::PhantomData;
use core::mem::ManuallyDrop;
use core::ops::Deref;
use core::ptr;

use super::{Arc, ArcInner, HeaderSlice, HeaderSliceWithLengthProtected, HeaderWithLength};
use crate::triomphe::header::HeaderSliceWithLengthUnchecked;

/// A "thin" `Arc` containing dynamically sized data
///
/// This is functionally equivalent to `Arc<(H, [T])>`
///
/// When you create an `Arc` containing a dynamically sized type
/// like `HeaderSlice<H, [T]>`, the `Arc` is represented on the stack
/// as a "fat pointer", where the length of the slice is stored
/// alongside the `Arc`'s pointer. In some situations you may wish to
/// have a thin pointer instead, perhaps for FFI compatibility
/// or space efficiency.
///
/// Note that we use `[T; 0]` in order to have the right alignment for `T`.
///
/// `ThinArc` solves this by storing the length in the allocation itself,
/// via `HeaderSliceWithLengthProtected`.
#[repr(transparent)]
pub struct ThinArc<H, T> {
    // We can pointer-cast between this target type
    // of `ArcInner<HeaderSlice<HeaderWithLength<H>, [T; 0]>`
    // and the types
    // `ArcInner<HeaderSliceWithLengthProtected<H, T>>` and
    // `ArcInner<HeaderSliceWithLengthUnchecked<H, T>>` (= `ArcInner<HeaderSlice<HeaderWithLength<H>, [T]>>`).
    // [By adding appropriate length metadata to the pointer.]
    // All types involved are #[repr(C)] or #[repr(transparent)], to ensure the safety of such casts
    // (in particular `HeaderSlice`, `HeaderWithLength`, `HeaderSliceWithLengthProtected`).
    //
    // The safe API of `ThinArc` ensures that the length in the `HeaderWithLength`
    // corretcly set - or verified - upon creation of a `ThinArc` and can't be modified
    // to fall out of sync with the true slice length for this value & allocation.
    ptr: ptr::NonNull<ArcInner<HeaderSlice<HeaderWithLength<H>, [T; 0]>>>,
    phantom: PhantomData<(H, T)>,
}

unsafe impl<H: Sync + Send, T: Sync + Send> Send for ThinArc<H, T> {}
unsafe impl<H: Sync + Send, T: Sync + Send> Sync for ThinArc<H, T> {}

// Synthesize a fat pointer from a thin pointer.
//
// See the comment around the analogous operation in from_header_and_iter.
#[inline]
fn thin_to_thick<H, T>(arc: &ThinArc<H, T>) -> *mut ArcInner<HeaderSliceWithLengthProtected<H, T>> {
    let thin = arc.ptr.as_ptr();
    let len = unsafe { (*thin).data.header.length };
    let fake_slice = ptr::slice_from_raw_parts_mut(thin as *mut T, len);

    fake_slice as *mut ArcInner<HeaderSliceWithLengthProtected<H, T>>
}

impl<H, T> ThinArc<H, T> {
    /// Temporarily converts |self| into a bonafide Arc and exposes it to the
    /// provided callback. The refcount is not modified.
    #[inline]
    pub fn with_arc<F, U>(&self, f: F) -> U
    where
        F: FnOnce(&Arc<HeaderSliceWithLengthUnchecked<H, T>>) -> U,
    {
        // Synthesize transient Arc, which never touches the refcount of the ArcInner.
        let transient = ManuallyDrop::new(Arc::from_protected(unsafe {
            Arc::from_raw_inner(thin_to_thick(self))
        }));

        // Expose the transient Arc to the callback, which may clone it if it wants
        // and forward the result to the user
        f(&transient)
    }

    /// Temporarily converts |self| into a bonafide Arc and exposes it to the
    /// provided callback. The refcount is not modified.
    #[inline]
    fn with_protected_arc<F, U>(&self, f: F) -> U
    where
        F: FnOnce(&Arc<HeaderSliceWithLengthProtected<H, T>>) -> U,
    {
        // Synthesize transient Arc, which never touches the refcount of the ArcInner.
        let transient = ManuallyDrop::new(unsafe { Arc::from_raw_inner(thin_to_thick(self)) });

        // Expose the transient Arc to the callback, which may clone it if it wants
        // and forward the result to the user
        f(&transient)
    }

    /// Creates a `ThinArc` for a HeaderSlice using the given header struct and
    /// iterator to generate the slice.
    pub fn from_iter<I, F>(items: I, f: F) -> Self
    where
        I: Iterator<Item = T> + ExactSizeIterator,
        F: FnOnce(&mut [T]) -> H,
    {
        let len = items.len();
        Arc::into_thin(Arc::from_iter(items, |i| HeaderWithLength::new(f(i), len)))
    }

    /// Creates a `ThinArc` for a HeaderSlice using the given header struct and
    /// iterator to generate the slice.
    pub fn from_header_and_iter<I>(header: H, items: I) -> Self
    where
        I: Iterator<Item = T> + ExactSizeIterator,
    {
        let header = HeaderWithLength::new(header, items.len());
        Arc::into_thin(Arc::from_header_and_iter(header, items))
    }

    /// Returns the address on the heap of the ThinArc itself -- not the T
    /// within it -- for memory reporting.
    #[inline]
    pub fn ptr(&self) -> *const c_void {
        self.ptr.cast().as_ptr()
    }
}

impl<H, T> Deref for ThinArc<H, T> {
    type Target = HeaderSliceWithLengthUnchecked<H, T>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        unsafe { (*thin_to_thick(self)).data.inner() }
    }
}

impl<H, T> Clone for ThinArc<H, T> {
    #[inline]
    fn clone(&self) -> Self {
        ThinArc::with_protected_arc(self, |a| Arc::protected_into_thin(a.clone()))
    }
}

impl<H, T> Drop for ThinArc<H, T> {
    #[inline]
    fn drop(&mut self) {
        let _ = Arc::protected_from_thin(ThinArc {
            ptr: self.ptr,
            phantom: PhantomData,
        });
    }
}

impl<H, T> Arc<HeaderSliceWithLengthUnchecked<H, T>> {
    /// Converts an `Arc` into a `ThinArc`. This consumes the `Arc`, so the refcount
    /// is not modified.
    ///
    /// # Safety
    /// Assumes that the header length matches the slice length.
    #[inline]
    unsafe fn into_thin_unchecked(a: Self) -> ThinArc<H, T> {
        // Safety: invariant bubbled up
        let this_protected: Arc<HeaderSliceWithLengthProtected<H, T>> =
            unsafe { Arc::from_unprotected_unchecked(a) };

        Arc::protected_into_thin(this_protected)
    }

    /// Converts an `Arc` into a `ThinArc`. This consumes the `Arc`, so the refcount
    /// is not modified.
    #[inline]
    pub fn into_thin(a: Self) -> ThinArc<H, T> {
        assert_eq!(
            a.header.length,
            a.slice.len(),
            "Length needs to be correct for ThinArc to work"
        );
        // Safety: invariant checked in assertion above
        unsafe { Self::into_thin_unchecked(a) }
    }

    /// Converts an `Arc` into a `ThinArc`. This consumes the `Arc`, so the refcount
    /// is not modified.
    #[inline]
    fn from_protected(a: Arc<HeaderSliceWithLengthProtected<H, T>>) -> Self {
        // Safety: HeaderSliceWithLengthProtected and HeaderSliceWithLengthUnchecked have the same layout
        // The whole `Arc` should also be layout compatible (as a transparent wrapper around `NonNull` pointers with the same
        // metadata type) but we still conservatively avoid a direct transmute here and use a pointer-cast instead.
        unsafe { Arc::from_raw_inner(Arc::into_raw_inner(a) as _) }
    }
}

impl<H, T> Arc<HeaderSliceWithLengthProtected<H, T>> {
    /// Converts an `Arc` into a `ThinArc`. This consumes the `Arc`, so the refcount
    /// is not modified.
    #[inline]
    pub fn protected_into_thin(a: Self) -> ThinArc<H, T> {
        debug_assert_eq!(
            a.length(),
            a.slice().len(),
            "Length needs to be correct for ThinArc to work"
        );

        let fat_ptr: *mut ArcInner<HeaderSliceWithLengthProtected<H, T>> = Arc::into_raw_inner(a);
        // Safety: The pointer comes from a valid Arc, and HeaderSliceWithLengthProtected has the correct length invariant
        let thin_ptr: *mut ArcInner<HeaderSlice<HeaderWithLength<H>, [T; 0]>> = fat_ptr.cast();
        ThinArc {
            ptr: unsafe { ptr::NonNull::new_unchecked(thin_ptr) },
            phantom: PhantomData,
        }
    }

    /// Converts a `ThinArc` into an `Arc`. This consumes the `ThinArc`, so the refcount
    /// is not modified.
    #[inline]
    pub fn protected_from_thin(a: ThinArc<H, T>) -> Self {
        let a = ManuallyDrop::new(a);
        let ptr = thin_to_thick(&a);
        unsafe { Arc::from_raw_inner(ptr) }
    }

    /// Obtains a HeaderSliceWithLengthProtected from an unchecked HeaderSliceWithLengthUnchecked, wrapped in an Arc
    ///
    /// # Safety
    /// Assumes that the header length matches the slice length.
    #[inline]
    unsafe fn from_unprotected_unchecked(a: Arc<HeaderSliceWithLengthUnchecked<H, T>>) -> Self {
        // Safety: HeaderSliceWithLengthProtected and HeaderSliceWithLengthUnchecked have the same layout
        // and the safety invariant on HeaderSliceWithLengthProtected.inner is bubbled up
        // The whole `Arc` should also be layout compatible (as a transparent wrapper around `NonNull` pointers with the same
        // metadata type) but we still conservatively avoid a direct transmute here and use a pointer-cast instead.
        unsafe { Arc::from_raw_inner(Arc::into_raw_inner(a) as _) }
    }
}

impl<H: PartialEq, T: PartialEq> PartialEq for ThinArc<H, T> {
    #[inline]
    fn eq(&self, other: &ThinArc<H, T>) -> bool {
        ThinArc::with_arc(self, |a| ThinArc::with_arc(other, |b| *a == *b))
    }
}

impl<H: Eq, T: Eq> Eq for ThinArc<H, T> {}

impl<H: PartialOrd, T: PartialOrd> PartialOrd for ThinArc<H, T> {
    #[inline]
    fn partial_cmp(&self, other: &ThinArc<H, T>) -> Option<Ordering> {
        ThinArc::with_arc(self, |a| ThinArc::with_arc(other, |b| a.partial_cmp(b)))
    }
}

impl<H: Ord, T: Ord> Ord for ThinArc<H, T> {
    #[inline]
    fn cmp(&self, other: &ThinArc<H, T>) -> Ordering {
        ThinArc::with_arc(self, |a| ThinArc::with_arc(other, |b| a.cmp(b)))
    }
}

impl<H: Hash, T: Hash> Hash for ThinArc<H, T> {
    fn hash<HSR: Hasher>(&self, state: &mut HSR) {
        ThinArc::with_arc(self, |a| a.hash(state))
    }
}

impl<H: fmt::Debug, T: fmt::Debug> fmt::Debug for ThinArc<H, T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Debug::fmt(&**self, f)
    }
}

impl<H, T> fmt::Pointer for ThinArc<H, T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Pointer::fmt(&self.ptr(), f)
    }
}

#[cfg(test)]
mod tests {
    use crate::triomphe::{Arc, HeaderWithLength, ThinArc};
    use alloc::vec;
    use core::clone::Clone;
    use core::ops::Drop;
    use core::sync::atomic;
    use core::sync::atomic::Ordering::{Acquire, SeqCst};

    #[derive(PartialEq)]
    struct Canary(*mut atomic::AtomicUsize);

    impl Drop for Canary {
        fn drop(&mut self) {
            unsafe {
                (*self.0).fetch_add(1, SeqCst);
            }
        }
    }

    #[test]
    fn empty_thin() {
        let header = HeaderWithLength::new(100u32, 0);
        let x = Arc::from_header_and_iter(header, core::iter::empty::<i32>());
        let y = Arc::into_thin(x.clone());
        assert_eq!(y.header.header, 100);
        assert!(y.slice.is_empty());
        assert_eq!(x.header.header, 100);
        assert!(x.slice.is_empty());
    }

    #[test]
    fn thin_assert_padding() {
        #[derive(Clone, Default)]
        #[repr(C)]
        struct Padded {
            i: u16,
        }

        // The header will have more alignment than `Padded`
        let header = HeaderWithLength::new(0i32, 2);
        let items = vec![Padded { i: 0xdead }, Padded { i: 0xbeef }];
        let a = ThinArc::from_header_and_iter(header, items.into_iter());
        assert_eq!(a.slice.len(), 2);
        assert_eq!(a.slice[0].i, 0xdead);
        assert_eq!(a.slice[1].i, 0xbeef);
    }

    #[test]
    #[allow(clippy::redundant_clone, clippy::eq_op)]
    fn slices_and_thin() {
        let mut canary = atomic::AtomicUsize::new(0);
        let c = Canary(&mut canary as *mut atomic::AtomicUsize);
        let v = vec![5, 6];
        let header = HeaderWithLength::new(c, v.len());
        {
            let x = Arc::into_thin(Arc::from_header_and_slice(header, &v));
            let y = ThinArc::with_arc(&x, |q| q.clone());
            let _ = y.clone();
            let _ = x == x;
            Arc::from_thin(x.clone());
        }
        assert_eq!(canary.load(Acquire), 1);
    }

    #[test]
    #[allow(clippy::redundant_clone, clippy::eq_op)]
    fn iter_and_thin() {
        let mut canary = atomic::AtomicUsize::new(0);
        let c = Canary(&mut canary as *mut atomic::AtomicUsize);
        let v = vec![5, 6];
        let header = HeaderWithLength::new(c, v.len());
        {
            let x = Arc::into_thin(Arc::from_header_and_iter(header, v.into_iter()));
            let y = ThinArc::with_arc(&x, |q| q.clone());
            let _ = y.clone();
            let _ = x == x;
            Arc::from_thin(x.clone());
        }
        assert_eq!(canary.load(Acquire), 1);
    }

    #[allow(dead_code)]
    const fn is_partial_ord<T: ?Sized + PartialOrd>() {}

    #[allow(dead_code)]
    const fn is_ord<T: ?Sized + Ord>() {}

    // compile-time check that PartialOrd/Ord is correctly derived
    const _: () = is_partial_ord::<ThinArc<f64, f64>>();
    const _: () = is_partial_ord::<ThinArc<f64, u64>>();
    const _: () = is_partial_ord::<ThinArc<u64, f64>>();
    const _: () = is_ord::<ThinArc<u64, u64>>();
}
