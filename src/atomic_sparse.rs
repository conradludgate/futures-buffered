use alloc::{boxed::Box, vec::Vec};
use core::{hint::spin_loop, sync::atomic::AtomicUsize};

pub struct AtomicSparseSet {
    /// max len is set.len() / 2
    set: Box<[AtomicUsize]>,
    len: AtomicUsize,
}

impl AtomicSparseSet {
    pub fn new(cap: usize) -> Self {
        let mut v: Vec<AtomicUsize> = Vec::with_capacity(cap * 2);
        v.resize_with(cap * 2, || AtomicUsize::new(0));
        AtomicSparseSet {
            set: v.into_boxed_slice(),
            len: AtomicUsize::new(0),
        }
    }

    pub fn push_sync(&self, x: usize) {
        let batch = self.set.len() / 2;
        let mask = (batch + 1).next_power_of_two();
        if x >= batch {
            return;
        }

        let mut len = self.len.load(core::sync::atomic::Ordering::Acquire);

        let sparse = self.set[x + batch].load(core::sync::atomic::Ordering::Relaxed);
        let dense = self.set[sparse].load(core::sync::atomic::Ordering::Relaxed);

        if sparse < (len & !mask) && dense == x {
            return;
        }

        loop {
            // claim the slot
            match self.len.compare_exchange_weak(
                len,
                len | mask,
                core::sync::atomic::Ordering::AcqRel,
                core::sync::atomic::Ordering::Relaxed,
            ) {
                Ok(len) if len == batch => {
                    self.len.store(0, core::sync::atomic::Ordering::SeqCst);
                    return;
                }
                // we only claim the slot if len doesn't have the claim bit
                Ok(len) if len & mask == 0 => {
                    // this is our slot, there should be no sync happeneing here
                    self.set[batch + x].store(len, core::sync::atomic::Ordering::Release);
                    self.set[len].store(x, core::sync::atomic::Ordering::Release);
                    self.len
                        .store(len + 1, core::sync::atomic::Ordering::SeqCst);
                    break;
                }
                Ok(l) | Err(l) => len = l,
            }
            spin_loop();
        }
    }

    pub fn pop_sync(&self) -> Option<usize> {
        let batch = self.set.len() / 2;
        let mask = (batch + 1).next_power_of_two();

        let mut len = self.len.load(core::sync::atomic::Ordering::Acquire);

        loop {
            // claim the slot
            match self.len.compare_exchange_weak(
                len,
                len | mask,
                core::sync::atomic::Ordering::AcqRel,
                core::sync::atomic::Ordering::Relaxed,
            ) {
                Ok(len) if len == 0 => {
                    self.len.store(0, core::sync::atomic::Ordering::SeqCst);
                    break None;
                }
                // we only claim the slot if len doesn't have the claim bit
                Ok(len) if len & mask == 0 => {
                    // this is our slot, there should be no sync happeneing here
                    let x = self.set[len - 1].load(core::sync::atomic::Ordering::Acquire);
                    self.len
                        .store(len - 1, core::sync::atomic::Ordering::SeqCst);
                    break Some(x);
                }
                Ok(l) | Err(l) => len = l,
            }
            spin_loop();
        }
    }

    pub fn len(&self) -> usize {
        self.len.load(core::sync::atomic::Ordering::Relaxed)
    }

    pub fn capacity(&self) -> usize {
        self.set.len() / 2
    }

    pub(crate) fn init(&mut self) {
        let len = self.set.len() / 2;
        for (i, x) in self.set.iter_mut().enumerate() {
            *x.get_mut() = i % len;
        }
        *self.len.get_mut() = len;
    }

    fn split(&mut self) -> (&mut [AtomicUsize], &mut [AtomicUsize]) {
        self.set.split_at_mut(self.capacity())
    }

    pub fn contains(&mut self, x: usize) -> bool {
        let len = *self.len.get_mut();

        let (dense, sparse) = self.split();

        let i = *sparse[x].get_mut();
        *dense[i].get_mut() == x && i < len
    }

    pub(crate) fn push(&mut self, x: usize) {
        if self.contains(x) {
            return;
        }
        let len = *self.len.get_mut();
        let (dense, sparse) = self.split();

        *sparse[x].get_mut() = len;
        *dense[len].get_mut() = x;

        *self.len.get_mut() += 1;
    }

    pub fn pop(&mut self) -> Option<usize> {
        let len = self.len.get_mut();
        if *len == 0 {
            None
        } else {
            *len -= 1;
            Some(*self.set[*len].get_mut())
        }
    }
}

#[cfg(test)]
#[cfg(loom)]
mod loom_tests {
    use super::AtomicSparseSet;
    use loom::{sync::Arc, thread};

    #[test]
    fn test_concurrent_logic() {
        loom::model(|| {
            let set = Arc::new(AtomicSparseSet::new(1));
            let set2 = Arc::clone(&set);

            let handle = thread::spawn(move || {
                set2.push_sync(0);
            });

            set.push_sync(0);

            handle.join().unwrap();

            assert_eq!(set.pop_sync(), Some(0));
            assert_eq!(set.pop_sync(), None);
        });
    }
}
