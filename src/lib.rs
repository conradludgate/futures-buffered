use std::{
    hint::spin_loop,
    pin::Pin,
    sync::{atomic::AtomicUsize, Arc},
    task::{Context, Poll, Wake, Waker},
};

use futures::{Future, Stream};
use pin_project_lite::pin_project;

const BATCH: usize = 10;
const MASK: usize = (BATCH + 1).next_power_of_two();

fn project_slice<T>(slice: Pin<&mut [T]>, i: usize) -> Pin<&mut T> {
    // SAFETY: slice fields are pinned since the whole slice is pinned
    // <https://discord.com/channels/273534239310479360/818964227783262209/1035563044887072808>
    unsafe { slice.map_unchecked_mut(|futs| &mut futs[i]) }
}

pin_project!(
    pub struct ConcurrentProcessQueue<F> {
        #[pin]
        inner: [Option<F>; BATCH],
        sparse: Arc<AtomicSparseSet>,
    }
);

#[derive(Debug, Default)]
struct AtomicSparseSet {
    dense: [AtomicUsize; BATCH],
    sparse: [AtomicUsize; BATCH],
    len: AtomicUsize,
}

impl AtomicSparseSet {
    pub fn push(&self, x: usize) {
        if x >= BATCH {
            return;
        }

        let mut len = self.len.load(std::sync::atomic::Ordering::Acquire);

        let sparse = self.sparse[x].load(std::sync::atomic::Ordering::Relaxed);
        let dense = self.dense[sparse].load(std::sync::atomic::Ordering::Relaxed);

        if sparse < (len & !MASK) && dense == x {
            return;
        }

        loop {
            // claim the slot
            match self.len.compare_exchange_weak(
                len,
                len | MASK,
                std::sync::atomic::Ordering::AcqRel,
                std::sync::atomic::Ordering::Relaxed,
            ) {
                Ok(len) if len == BATCH => {
                    self.len.store(0, std::sync::atomic::Ordering::SeqCst);
                    return;
                }
                // we only claim the slot if len doesn't have the claim bit
                Ok(len) if len & MASK == 0 => {
                    // this is our slot, there should be no sync happeneing here
                    self.sparse[x].store(len, std::sync::atomic::Ordering::Release);
                    self.dense[len].store(x, std::sync::atomic::Ordering::Release);
                    self.len.store(len + 1, std::sync::atomic::Ordering::SeqCst);
                    break;
                }
                Ok(l) => len = l,
                Err(l) => len = l,
            }
            spin_loop()
        }
    }
    pub fn pop(&self) -> Option<usize> {
        let mut len = self.len.load(std::sync::atomic::Ordering::Acquire);

        loop {
            // claim the slot
            match self.len.compare_exchange_weak(
                len,
                len | MASK,
                std::sync::atomic::Ordering::AcqRel,
                std::sync::atomic::Ordering::Relaxed,
            ) {
                Ok(len) if len == 0 => {
                    self.len.store(0, std::sync::atomic::Ordering::SeqCst);
                    break None;
                }
                // we only claim the slot if len doesn't have the claim bit
                Ok(len) if len & MASK == 0 => {
                    // this is our slot, there should be no sync happeneing here
                    let x = self.dense[len - 1].load(std::sync::atomic::Ordering::Acquire);
                    self.len.store(len - 1, std::sync::atomic::Ordering::SeqCst);
                    break Some(x);
                }
                Ok(l) => len = l,
                Err(l) => len = l,
            }
            spin_loop()
        }
    }
}

impl<F> ConcurrentProcessQueue<F> {
    pub fn new() -> Self {
        Self {
            inner: [(); BATCH].map(|()| None),
            sparse: Arc::default(),
        }
    }
    pub fn push(mut self: Pin<&mut Self>, fut: F) {
        let mut inner = self.as_mut().project().inner;
        for i in 0..BATCH {
            let mut x = project_slice(inner.as_mut(), i);
            if x.is_none() {
                x.set(Some(fut));
                self.sparse.push(i);
                break;
            }
        }
    }
}

impl<F> Default for ConcurrentProcessQueue<F> {
    fn default() -> Self {
        Self::new()
    }
}

impl<F: Future> Stream for ConcurrentProcessQueue<F> {
    type Item = F::Output;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.inner.iter().filter_map(|x| x.as_ref()).count() == 0 {
            return Poll::Ready(None);
        }
        loop {
            match self.sparse.pop() {
                Some(i) => {
                    struct InnerWaker {
                        index: usize,
                        waker: Waker,
                        sparse: Arc<AtomicSparseSet>,
                    }
                    impl Wake for InnerWaker {
                        fn wake(self: std::sync::Arc<Self>) {
                            self.wake_by_ref()
                        }
                        /// on wake, insert the future back into the queue, and then wake the original waker too
                        fn wake_by_ref(self: &Arc<Self>) {
                            self.sparse.push(self.index);
                            self.waker.wake_by_ref();
                        }
                    }

                    // create the waker with the current waker and the queue. no future
                    let waker = Arc::new(InnerWaker {
                        index: i,
                        waker: cx.waker().clone(),
                        sparse: self.sparse.clone(),
                    })
                    .into();
                    let mut cx = Context::from_waker(&waker);

                    let mut inner = self.as_mut().project().inner;
                    let mut fut = project_slice(inner.as_mut(), i);

                    let res = match fut.as_mut().as_pin_mut() {
                        // poll the current task
                        Some(fut) => fut.poll(&mut cx),
                        None => continue,
                    };

                    if let Poll::Ready(x) = res {
                        fut.set(None);
                        break Poll::Ready(Some(x));
                    }
                }
                None => break Poll::Pending,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        pin::Pin,
        sync::{atomic::AtomicUsize, Arc},
        task::{Context, Poll},
        time::Duration,
    };

    use futures::{pin_mut, Future, StreamExt};
    use pin_project_lite::pin_project;
    use tokio::time::Sleep;

    use crate::{ConcurrentProcessQueue, BATCH};

    #[tokio::test]
    async fn single() {
        let buffer = ConcurrentProcessQueue::new();
        pin_mut!(buffer);
        buffer
            .as_mut()
            .push(tokio::time::sleep(Duration::from_secs(1)));
        buffer.next().await;
    }

    #[tokio::test]
    async fn multi() {
        let poll_count = Arc::new(AtomicUsize::new(0));
        pin_project!(
            struct PollCounter<F> {
                count: Arc<AtomicUsize>,
                #[pin]
                inner: F,
            }
        );

        impl<F: Future> Future for PollCounter<F> {
            type Output = F::Output;
            fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
                self.count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                self.project().inner.poll(cx)
            }
        }

        fn wait(poll_count: &Arc<AtomicUsize>, i: usize) -> PollCounter<Sleep> {
            PollCounter {
                count: poll_count.clone(),
                inner: tokio::time::sleep(Duration::from_secs(1) / (i as u32 % 10 + 5)),
            }
        }

        let buffer = ConcurrentProcessQueue::new();
        pin_mut!(buffer);
        // build up
        for i in 0..BATCH {
            buffer.as_mut().push(wait(&poll_count, i));
        }
        // poll and insert
        for i in 0..100 {
            assert!(buffer.next().await.is_some());
            buffer.as_mut().push(wait(&poll_count, i));
        }
        // drain down
        for _ in 0..BATCH {
            assert!(buffer.next().await.is_some());
        }

        let count = poll_count.load(std::sync::atomic::Ordering::SeqCst);
        assert_eq!(count, (100 + BATCH) * 2);
    }
}
