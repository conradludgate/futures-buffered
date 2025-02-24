use core::{
    marker::PhantomData,
    mem::{self, offset_of, ManuallyDrop},
    ops::Deref,
    ptr::{self, NonNull},
};

use super::{Arc, ArcInner, HeaderSlice, HeaderWithLength, ThinArc};

#[derive(Clone, Copy)]
pub struct WithOffset<T> {
    /// the offset in the `ThinArcList` this value is stored
    offset: usize,
    /// the value stored
    pub value: T,
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

unsafe impl<H: Sync + Send, T: Sync + Send> Send for ThinArcItem<H, T> {}
unsafe impl<H: Sync + Send, T: Sync + Send> Sync for ThinArcItem<H, T> {}

pub struct ThinArcList<H, T> {
    inner: ThinArc<H, WithOffset<T>>,
}

impl<H, T> ThinArcList<H, T> {
    /// Creates a `ThinArc` for a HeaderSlice using the given header struct and
    /// iterator to generate the slice.
    pub fn from_iter<I, F>(items: I, f: F) -> Self
    where
        I: Iterator<Item = T> + ExactSizeIterator,
        F: FnOnce(&mut [WithOffset<T>]) -> H,
    {
        Self {
            inner: ThinArc::from_iter(
                items
                    .enumerate()
                    .map(|(offset, value)| WithOffset { offset, value }),
                f,
            ),
        }
    }

    /// Creates a `ThinArcList` for a HeaderSlice using the given header struct and
    /// iterator to generate the slice.
    pub fn from_header_and_iter<I>(header: H, items: I) -> Self
    where
        I: Iterator<Item = T> + ExactSizeIterator,
    {
        Self {
            inner: ThinArc::from_header_and_iter(
                header,
                items
                    .enumerate()
                    .map(|(offset, value)| WithOffset { offset, value }),
            ),
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

    pub fn header(&self) -> &H {
        &self.inner.header.header
    }

    pub fn with_item<F, U>(&self, index: usize, f: F) -> U
    where
        F: FnOnce(&ThinArcItem<H, T>) -> U,
    {
        self.inner.with_arc(|inner| {
            let transient = ManuallyDrop::new(ThinArcItem {
                ptr: NonNull::from(&inner.slice[index]),
                phantom: PhantomData,
            });
            f(&transient)
        })
    }
}

impl<H, T> ThinArcItem<H, T> {
    pub fn into_raw(self) -> NonNull<WithOffset<T>> {
        self.ptr
    }

    /// Safety: must come from into_raw, must have the same H.
    pub unsafe fn from_raw(ptr: NonNull<WithOffset<T>>) -> Self {
        Self {
            ptr,
            phantom: PhantomData,
        }
    }

    pub fn as_raw(&self) -> NonNull<WithOffset<T>> {
        self.ptr
    }

    /// Safety: must come from as_ref, must have the same H.
    /// Must not be dropped
    pub unsafe fn from_raw_ref(ptr: NonNull<WithOffset<T>>) -> ManuallyDrop<Self> {
        ManuallyDrop::new(Self {
            ptr,
            phantom: PhantomData,
        })
    }

    pub fn index(&self) -> usize {
        unsafe { self.ptr.as_ref().offset }
    }

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

pub mod wake {
    use super::*;
    use core::task::{RawWaker, Waker};

    pub trait ThinItemWake<H>: Sized {
        fn wake(this: ThinArcItem<H, Self>) {
            Self::wake_by_ref(&this);
        }

        fn wake_by_ref(this: &ThinArcItem<H, Self>);
    }

    #[cfg(target_has_atomic = "ptr")]
    impl<H, W: ThinItemWake<H> + Send + Sync + 'static> From<ThinArcItem<H, W>> for Waker {
        /// Use a [`Wake`]-able type as a `Waker`.
        ///
        /// No heap allocations or atomic operations are used for this conversion.
        fn from(waker: ThinArcItem<H, W>) -> Waker {
            // SAFETY: This is safe because raw_waker safely constructs
            // a RawWaker from ThinArcItem<H, W>.
            unsafe { Waker::from_raw(raw_waker(waker)) }
        }
    }

    #[cfg(target_has_atomic = "ptr")]
    impl<H, W: ThinItemWake<H> + Send + Sync + 'static> From<ThinArcItem<H, W>> for RawWaker {
        /// Use a `Wake`-able type as a `RawWaker`.
        ///
        /// No heap allocations or atomic operations are used for this conversion.
        fn from(waker: ThinArcItem<H, W>) -> RawWaker {
            raw_waker(waker)
        }
    }

    // NB: This private function for constructing a RawWaker is used, rather than
    // inlining this into the `From<ThinArcItem<H, W>> for RawWaker` impl, to ensure that
    // the safety of `From<ThinArcItem<H, W>> for Waker` does not depend on the correct
    // trait dispatch - instead both impls call this function directly and
    // explicitly.
    #[cfg(target_has_atomic = "ptr")]
    #[inline(always)]
    fn raw_waker<H, W: ThinItemWake<H> + Send + Sync + 'static>(
        waker: ThinArcItem<H, W>,
    ) -> RawWaker {
        use core::{
            ptr::NonNull,
            task::{RawWaker, RawWakerVTable},
        };

        // Increment the reference count of the arc to clone it.
        //
        // The #[inline(always)] is to ensure that raw_waker and clone_waker are
        // always generated in the same code generation unit as one another, and
        // therefore that the structurally identical const-promoted RawWakerVTable
        // within both functions is deduplicated at LLVM IR code generation time.
        // This allows optimizing Waker::will_wake to a single pointer comparison of
        // the vtable pointers, rather than comparing all four function pointers
        // within the vtables.
        #[inline(always)]
        unsafe fn clone_waker<H, W: ThinItemWake<H> + Send + Sync + 'static>(
            waker: *const (),
        ) -> RawWaker {
            let waker_ref = unsafe {
                ManuallyDrop::new(ThinArcItem::<H, W> {
                    ptr: NonNull::new_unchecked(waker.cast::<WithOffset<W>>().cast_mut()),
                    phantom: PhantomData,
                })
            };
            let _clone = ManuallyDrop::new(waker_ref.clone());

            RawWaker::new(
                waker,
                &RawWakerVTable::new(
                    clone_waker::<H, W>,
                    wake::<H, W>,
                    wake_by_ref::<H, W>,
                    drop_waker::<H, W>,
                ),
            )
        }

        // Wake by value, moving the Arc into the Wake::wake function
        unsafe fn wake<H, W: ThinItemWake<H> + Send + Sync + 'static>(waker: *const ()) {
            let waker = unsafe {
                ThinArcItem::<H, W> {
                    ptr: NonNull::new_unchecked(waker.cast::<WithOffset<W>>().cast_mut()),
                    phantom: PhantomData,
                }
            };
            <W as ThinItemWake<H>>::wake(waker);
        }

        // Wake by reference, wrap the waker in ManuallyDrop to avoid dropping it
        unsafe fn wake_by_ref<H, W: ThinItemWake<H> + Send + Sync + 'static>(waker: *const ()) {
            let waker = unsafe {
                ManuallyDrop::new(ThinArcItem::<H, W> {
                    ptr: NonNull::new_unchecked(waker.cast::<WithOffset<W>>().cast_mut()),
                    phantom: PhantomData,
                })
            };
            <W as ThinItemWake<H>>::wake_by_ref(&waker);
        }

        // Decrement the reference count of the Arc on drop
        unsafe fn drop_waker<H, W: ThinItemWake<H> + Send + Sync + 'static>(waker: *const ()) {
            let _ = unsafe {
                ThinArcItem::<H, W> {
                    ptr: NonNull::new_unchecked(waker.cast::<WithOffset<W>>().cast_mut()),
                    phantom: PhantomData,
                }
            };
        }

        RawWaker::new(
            ManuallyDrop::new(waker).ptr.as_ptr().cast(),
            &RawWakerVTable::new(
                clone_waker::<H, W>,
                wake::<H, W>,
                wake_by_ref::<H, W>,
                drop_waker::<H, W>,
            ),
        )
    }
}
