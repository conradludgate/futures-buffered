//! Owns initialized outputs until they are dropped or transferred to the result.
use alloc::{boxed::Box, vec::Vec};
use core::mem::{needs_drop, MaybeUninit};
use fixedbitset::FixedBitSet;

pub(crate) struct JoinOutput<T> {
    // Safety invariant: Until take_vec transfers ownership or Drop starts,
    // each set bit in initialized identifies a valid, owned T in values at
    // that index. No other owner may read or drop those values. When tracking
    // is elided, values may still be uninitialized: needs_drop::<T>() == false
    // just removes cleanup obligations, not take_vec's initialization contract.
    // Once taken, values is empty and initialized may retain stale bits.
    values: Box<[MaybeUninit<T>]>,
    // Safety invariant: With tracking enabled (debug assertions or T needs
    // drop), its length equals the original values length and no bit outside
    // that range is set. Otherwise this is empty. During Drop, Guard's remaining
    // iterator, rather than the original bitmap, identifies values still owned.
    initialized: FixedBitSet,
}

impl<T> JoinOutput<T> {
    /// # Safety-usable invariant
    ///
    /// Creates exactly len output slots, all uninitialized. Tracking, when
    /// enabled, has the same length and no set bits.
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

    /// # Safety-usable invariant
    ///
    /// On return, the bounds-checked index contains value, owned by this helper,
    /// and its bit is set when tracking is enabled. Overwriting an occupied slot
    /// in release can leak its previous value, but does not leave this slot
    /// uninitialized. Join callers use each original queue index only once.
    pub(crate) fn write(&mut self, index: usize, value: T) {
        debug_assert!(
            !self.initialized.contains(index),
            "output already initialized"
        );
        self.values[index].write(value);
        // The write completed before marking. The bounds check above implies
        // index is also within initialized when tracking is enabled; inserting
        // into its preallocated storage invokes no caller code.
        if cfg!(debug_assertions) || needs_drop::<T>() {
            self.initialized.insert(index);
        }
    }

    /// Transfers the original allocation into the result vector.
    ///
    /// # Safety
    ///
    /// The caller must ensure every current output slot contains a valid T
    /// initialized by write, with ownership still held by this helper. This
    /// applies even when release-mode tracking is elided. An empty buffer,
    /// including one whose allocation was already taken, satisfies the contract.
    ///
    /// On return the vector owns those values and their storage; this helper's
    /// values buffer is empty and will not drop or transfer them again.
    pub(crate) unsafe fn take_vec(&mut self) -> Vec<T> {
        debug_assert!(
            self.values.is_empty() || self.initialized.is_full(),
            "missing join output"
        );
        let values = core::mem::replace(&mut self.values, Vec::new().into_boxed_slice());
        // SAFETY: take_vec's caller supplies valid, exclusively owned T values.
        // Box::into_raw supplies the original Box pointer and slice length;
        // casting the slice pointer retains that length. MaybeUninit<T> has T's
        // size/alignment, so the slice layout and allocation identity are
        // unchanged. This also preserves Box's aligned, non-null pointer for
        // empty/ZST slices, which need no allocated bytes. No callback runs
        // between detaching this Box and reconstructing it once as Box<[T]>.
        // self.values is already empty, disarming its drop/transfer path;
        // ownership now passes through Box<[T]> to the returned Vec<T>.
        unsafe { Box::from_raw(Box::into_raw(values) as *mut [T]) }.into_vec()
    }
}

impl<T> Drop for JoinOutput<T> {
    fn drop(&mut self) {
        if !needs_drop::<T>() || self.values.is_empty() {
            return;
        }
        struct Guard<'a, T> {
            // Safety invariant: Every remaining initialized iterator index is
            // in bounds and contains one valid T still owned by this guard.
            values: &'a mut [MaybeUninit<T>],
            // Consumed indices are never yielded again, including after panic.
            initialized: fixedbitset::Ones<'a>,
        }
        impl<T> Guard<'_, T> {
            fn drop_remaining(&mut self) {
                for index in self.initialized.by_ref() {
                    // SAFETY: Guard's invariant supplies an in-bounds valid T
                    // exclusively owned here, as assume_init_drop requires.
                    // FixedBitSet::ones yields each set index once. next() has
                    // advanced before user Drop runs, relinquishing this slot's
                    // drop obligation even if it panics. Guard::drop therefore
                    // resumes only with other live values; the surrounding
                    // MaybeUninit slice never drops any T a second time.
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
            // JoinOutput's tracked-slot invariant establishes Guard's invariant.
            // Drop's early return excludes both elided tracking and taken data.
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
