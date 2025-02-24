use core::{
    marker::PhantomData,
    mem::{self, offset_of, ManuallyDrop},
    ops::Deref,
    ptr::{self},
};

use super::{Arc, ArcInner, HeaderSlice, HeaderWithLength, ThinArc};

#[derive(Clone, Copy)]
struct WithOffset<T> {
    /// the offset in the `ThinArcList` this value is stored
    offset: usize,
    /// the value stored
    value: T,
}

pub struct ThinArcItem<H, T> {
    ptr: ptr::NonNull<WithOffset<T>>,
    phantom: PhantomData<ThinArcList<H, T>>,
}

impl<H, T> Deref for ThinArcItem<H, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe { &self.ptr.as_ref().value }
    }
}

pub struct ThinArcList<H, T> {
    inner: ThinArc<H, WithOffset<T>>,
}

impl<H, T> ThinArcList<H, T> {
    /// Creates a `ThinArcList` for a HeaderSlice using the given header struct and
    /// iterator to generate the slice.
    pub fn from_header_and_iter<I>(header: H, items: I) -> Self
    where
        I: Iterator<Item = T> + ExactSizeIterator,
    {
        let header = HeaderWithLength::new(header, items.len());
        Self {
            inner: Arc::into_thin(Arc::from_header_and_iter(
                header,
                items
                    .enumerate()
                    .map(|(offset, value)| WithOffset { offset, value }),
            )),
        }
    }

    /// Creates a `ThinArcList` for a HeaderSlice using the given header struct and
    /// a slice to copy.
    pub fn from_header_and_slice(header: H, items: &[T]) -> Self
    where
        T: Copy,
    {
        Self::from_header_and_iter(header, items.iter().copied())
    }
}

impl<H, T> ThinArcItem<H, T> {
    pub fn with_parent<F, U>(&self, f: F) -> U
    where
        F: FnOnce(&ThinArcList<H, T>) -> U,
    {
        unsafe {
            let transient = ManuallyDrop::new(self.ref_into_parent());
            f(&transient)
        }
    }

    pub fn into_parent(self) -> ThinArcList<H, T> {
        unsafe { ManuallyDrop::new(self).ref_into_parent() }
    }

    unsafe fn ref_into_parent(&self) -> ThinArcList<H, T> {
        let length_offset = offset_of!(
            ArcInner<HeaderSlice<HeaderWithLength<H>, [WithOffset<T>; 0]>>,
            data.header.length
        );

        let slice_offset = offset_of!(
            ArcInner<HeaderSlice<HeaderWithLength<H>, [WithOffset<T>; 0]>>,
            data.slice
        );

        let value_ptr = self.ptr.as_ptr().cast_const();
        let offset = unsafe { (*value_ptr).offset };

        // inner.data.slice[offset] -> inner.data.slice[0]
        let slice_root = unsafe { value_ptr.sub(offset) };

        // inner.data.slice[0] -> inner
        let arc = unsafe {
            slice_root
                .byte_sub(slice_offset)
                .cast::<ArcInner<HeaderSlice<HeaderWithLength<H>, [WithOffset<T>; 0]>>>()
        };

        // inner -> inner.data.header.length
        let len = unsafe { *arc.byte_add(length_offset).cast::<usize>() };

        // Synthesize the fat pointer. We do this by claiming we have a direct
        // pointer to a [T], and then changing the type of the borrow. The key
        // point here is that the length portion of the fat pointer applies
        // only to the number of elements in the dynamically-sized portion of
        // the type, so the value will be the same whether it points to a [T]
        // or something else with a [T] as its last member.
        let fake_slice = ptr::slice_from_raw_parts_mut(arc as *mut WithOffset<T>, len);
        let arc_slice =
            fake_slice as *mut ArcInner<HeaderSlice<HeaderWithLength<H>, [WithOffset<T>]>>;

        unsafe {
            ThinArcList {
                inner: Arc::into_thin(Arc::from_raw_inner(arc_slice)),
            }
        }
    }
}

impl<H, T> Clone for ThinArcList<H, T> {
    #[inline]
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<H, T> Drop for ThinArcList<H, T> {
    #[inline]
    fn drop(&mut self) {}
}

impl<H, T> Clone for ThinArcItem<H, T> {
    #[inline]
    fn clone(&self) -> Self {
        mem::forget(self.with_parent(ThinArcList::clone));
        Self {
            ptr: self.ptr,
            phantom: self.phantom,
        }
    }
}

impl<H, T> Drop for ThinArcItem<H, T> {
    #[inline]
    fn drop(&mut self) {
        let _ = unsafe { self.ref_into_parent() };
    }
}
