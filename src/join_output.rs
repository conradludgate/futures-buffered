//! Owns initialized outputs until they are dropped or transferred to the result.
use alloc::{boxed::Box, vec::Vec};
use core::mem::{needs_drop, MaybeUninit};
use fixedbitset::FixedBitSet;

pub(crate) struct JoinOutput<T> {
    values: Box<[MaybeUninit<T>]>,
    // Empty in release builds when T has no drop glue.
    initialized: FixedBitSet,
}

impl<T> JoinOutput<T> {
    pub(crate) fn new(len: usize) -> Self {
        let mut values = Vec::with_capacity(len);
        values.resize_with(len, MaybeUninit::uninit);
        Self {
            values: values.into_boxed_slice(),
            initialized: if cfg!(debug_assertions) || needs_drop::<T>() {
                FixedBitSet::with_capacity(len)
            } else {
                FixedBitSet::new()
            },
        }
    }

    pub(crate) fn write(&mut self, index: usize, value: T) {
        debug_assert!(
            !self.initialized.contains(index),
            "output already initialized"
        );
        self.values[index].write(value);
        if cfg!(debug_assertions) || needs_drop::<T>() {
            self.initialized.insert(index);
        }
    }

    /// Transfers the original allocation into the result vector.
    ///
    /// # Safety
    ///
    /// Every output slot must have been initialized by `write`, unless the
    /// allocation has already been taken and `values` is empty.
    pub(crate) unsafe fn take_vec(&mut self) -> Vec<T> {
        debug_assert!(
            self.values.is_empty() || self.initialized.is_full(),
            "missing join output"
        );
        let values = core::mem::replace(&mut self.values, Vec::new().into_boxed_slice());
        // SAFETY: the caller guarantees every slot is initialized, and
        // MaybeUninit<T> has T's layout.
        // Emptying self.values disarms Drop before transferring ownership.
        unsafe { Box::from_raw(Box::into_raw(values) as *mut [T]) }.into_vec()
    }
}

impl<T> Drop for JoinOutput<T> {
    fn drop(&mut self) {
        if !needs_drop::<T>() || self.values.is_empty() {
            return;
        }
        struct Guard<'a, T> {
            values: &'a mut [MaybeUninit<T>],
            initialized: fixedbitset::Ones<'a>,
        }
        impl<T> Guard<'_, T> {
            fn drop_remaining(&mut self) {
                for index in self.initialized.by_ref() {
                    // SAFETY: only initialized slots have set bits. The iterator
                    // advances before invoking user code, so the guard resumes at
                    // the next value if this destructor panics.
                    unsafe { self.values[index].assume_init_drop() };
                }
            }
        }
        impl<T> Drop for Guard<'_, T> {
            fn drop(&mut self) {
                self.drop_remaining();
            }
        }
        let mut guard = Guard {
            values: &mut self.values,
            initialized: self.initialized.ones(),
        };
        guard.drop_remaining();
    }
}

#[cfg(test)]
mod tests {
    use super::JoinOutput;
    #[cfg(debug_assertions)]
    use std::panic::{catch_unwind, AssertUnwindSafe};

    #[test]
    fn outputs_can_be_taken_after_out_of_order_writes() {
        let mut output = JoinOutput::new(2);
        output.write(1, 20);
        output.write(0, 10);
        // SAFETY: both output slots were initialized above.
        assert_eq!(unsafe { output.take_vec() }, [10, 20]);
        // SAFETY: the allocation has already been taken.
        assert!(unsafe { output.take_vec() }.is_empty());
    }

    #[test]
    fn zero_sized_outputs_with_drop_are_cleaned_up() {
        use core::sync::atomic::{AtomicUsize, Ordering};
        static DROPS: AtomicUsize = AtomicUsize::new(0);
        struct Value;
        impl Drop for Value {
            fn drop(&mut self) {
                DROPS.fetch_add(1, Ordering::Relaxed);
            }
        }
        let mut output = JoinOutput::new(65);
        for i in (0..65).step_by(2) {
            output.write(i, Value);
        }
        drop(output);
        assert_eq!(DROPS.load(Ordering::Relaxed), 33);
    }

    #[test]
    #[cfg(debug_assertions)]
    fn duplicate_write_is_caught_in_debug_builds() {
        let mut output = JoinOutput::new(1);
        output.write(0, 10);
        assert!(catch_unwind(AssertUnwindSafe(|| output.write(0, 20))).is_err());
        // SAFETY: the single output slot remains initialized.
        assert_eq!(unsafe { output.take_vec() }, [10]);
    }
}
