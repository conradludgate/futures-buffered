use std::{hint::spin_loop, sync::atomic::AtomicUsize};

#[derive(Debug)]
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

    pub fn push(&self, x: usize) {
        let batch = self.set.len() / 2;
        let mask = (batch + 1).next_power_of_two();
        if x >= batch {
            return;
        }

        let mut len = self.len.load(std::sync::atomic::Ordering::Acquire);

        let sparse = self.set[x + batch].load(std::sync::atomic::Ordering::Relaxed);
        let dense = self.set[sparse].load(std::sync::atomic::Ordering::Relaxed);

        if sparse < (len & !mask) && dense == x {
            return;
        }

        loop {
            // claim the slot
            match self.len.compare_exchange_weak(
                len,
                len | mask,
                std::sync::atomic::Ordering::AcqRel,
                std::sync::atomic::Ordering::Relaxed,
            ) {
                Ok(len) if len == batch => {
                    self.len.store(0, std::sync::atomic::Ordering::SeqCst);
                    return;
                }
                // we only claim the slot if len doesn't have the claim bit
                Ok(len) if len & mask == 0 => {
                    // this is our slot, there should be no sync happeneing here
                    self.set[batch + x].store(len, std::sync::atomic::Ordering::Release);
                    self.set[len].store(x, std::sync::atomic::Ordering::Release);
                    self.len.store(len + 1, std::sync::atomic::Ordering::SeqCst);
                    break;
                }
                Ok(l) | Err(l) => len = l,
            }
            spin_loop();
        }
    }
    pub fn pop(&self) -> Option<usize> {
        let batch = self.set.len() / 2;
        let mask = (batch + 1).next_power_of_two();

        let mut len = self.len.load(std::sync::atomic::Ordering::Acquire);

        loop {
            // claim the slot
            match self.len.compare_exchange_weak(
                len,
                len | mask,
                std::sync::atomic::Ordering::AcqRel,
                std::sync::atomic::Ordering::Relaxed,
            ) {
                Ok(len) if len == 0 => {
                    self.len.store(0, std::sync::atomic::Ordering::SeqCst);
                    break None;
                }
                // we only claim the slot if len doesn't have the claim bit
                Ok(len) if len & mask == 0 => {
                    // this is our slot, there should be no sync happeneing here
                    let x = self.set[len - 1].load(std::sync::atomic::Ordering::Acquire);
                    self.len.store(len - 1, std::sync::atomic::Ordering::SeqCst);
                    break Some(x);
                }
                Ok(l) | Err(l) => len = l,
            }
            spin_loop();
        }
    }
}
