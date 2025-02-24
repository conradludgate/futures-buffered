use core::iter::{ExactSizeIterator, Iterator};
use core::marker::PhantomData;
use core::mem;
use core::ptr;

use super::Arc;

/// Structure to allow Arc-managing some fixed-sized data and a variably-sized
/// slice in a single allocation.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash, PartialOrd, Ord)]
#[repr(C)]
pub struct HeaderSlice<H, T: ?Sized> {
    /// The fixed-sized data.
    pub header: H,

    /// The dynamically-sized data.
    pub slice: T,
}

impl<H, T> Arc<HeaderSlice<H, [T]>> {
    /// Creates an Arc for a HeaderSlice using the given header struct and
    /// iterator to generate the slice. The resulting Arc will be fat.
    pub fn from_iter<I, F>(mut items: I, f: F) -> Self
    where
        I: Iterator<Item = T> + ExactSizeIterator,
        F: FnOnce(&mut [T]) -> H,
    {
        assert_ne!(mem::size_of::<T>(), 0, "Need to think about ZST");

        let num_items = items.len();

        let inner = Arc::allocate_for_header_and_slice(num_items);

        unsafe {
            // Write the data.
            //
            // Note that any panics here (i.e. from the iterator) are safe, since
            // we'll just leak the uninitialized memory.
            let mut current = (*inner.as_ptr()).data.slice.as_mut_ptr();
            for _ in 0..num_items {
                ptr::write(
                    current,
                    items
                        .next()
                        .expect("ExactSizeIterator over-reported length"),
                );
                current = current.offset(1);
            }
            assert!(
                items.next().is_none(),
                "ExactSizeIterator under-reported length"
            );

            let header = f(&mut (*inner.as_ptr()).data.slice);
            ptr::write(&mut ((*inner.as_ptr()).data.header), header);
        }

        // Safety: ptr is valid & the inner structure is fully initialized
        Arc {
            p: inner,
            phantom: PhantomData,
        }
    }
}

/// Header data with an inline length. Consumers that use HeaderWithLength as the
/// Header type in HeaderSlice can take advantage of ThinArc.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
#[repr(C)]
pub struct HeaderWithLength<H> {
    /// The fixed-sized data.
    pub header: H,

    /// The slice length.
    pub length: usize,
}

impl<H> HeaderWithLength<H> {
    /// Creates a new HeaderWithLength.
    #[inline]
    pub fn new(header: H, length: usize) -> Self {
        HeaderWithLength { header, length }
    }
}

/// A type wrapping `HeaderSlice<HeaderWithLength<H>, T>` that is used internally in `ThinArc`.
///
/// # Safety
///
/// Safety-usable invariants:
///
/// - This is guaranteed to have the same representation as `HeaderSlice<HeaderWithLength<H>, [T]>`
/// - The header length (`.length()`) is checked to be the slice length
#[derive(Debug)]
#[repr(transparent)]
pub struct HeaderSliceWithLengthProtected<H, T> {
    // Invariant: the header's length field must be the slice length
    inner: HeaderSliceWithLengthUnchecked<H, T>,
}

pub(crate) type HeaderSliceWithLengthUnchecked<H, T> = HeaderSlice<HeaderWithLength<H>, [T]>;

impl<H, T> HeaderSliceWithLengthProtected<H, T> {
    pub fn length(&self) -> usize {
        self.inner.header.length
    }

    pub fn slice(&self) -> &[T] {
        &self.inner.slice
    }

    pub(crate) fn inner(&self) -> &HeaderSliceWithLengthUnchecked<H, T> {
        // This is safe in an immutable context
        &self.inner
    }
}
