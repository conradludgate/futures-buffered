use core::fmt;
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
    pub(super) ptr: ptr::NonNull<ArcInner<HeaderSlice<HeaderWithLength<H>, [T; 0]>>>,
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
        Arc::into_thin(Arc::from_iter(items, |_| header))
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
    #[inline]
    pub fn into_thin(a: Self) -> ThinArc<H, T> {
        assert_eq!(
            a.header.length,
            a.slice.len(),
            "Length needs to be correct for ThinArc to work"
        );

        // Safety: HeaderSliceWithLengthProtected and HeaderSliceWithLengthUnchecked have the same layout
        // and the safety invariant on HeaderSliceWithLengthProtected.inner is bubbled up
        // The whole `Arc` should also be layout compatible (as a transparent wrapper around `NonNull` pointers with the same
        // metadata type) but we still conservatively avoid a direct transmute here and use a pointer-cast instead.
        let this_protected: Arc<HeaderSliceWithLengthProtected<H, T>> =
            unsafe { Arc::from_raw_inner(Arc::into_raw_inner(a) as _) };

        Arc::protected_into_thin(this_protected)
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
}

impl<H: fmt::Debug, T: fmt::Debug> fmt::Debug for ThinArc<H, T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Debug::fmt(&**self, f)
    }
}

#[cfg(test)]
mod tests {
    use crate::triomphe::{HeaderWithLength, ThinArc};
    use alloc::vec;
    use core::clone::Clone;

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
}
