use std::{
    hint::spin_loop,
    pin::Pin,
    sync::{atomic::AtomicUsize, Arc},
    task::{Context, Poll, Wake, Waker},
};

use futures::{Future, Stream};

fn project_slice<T>(slice: Pin<&mut [T]>, i: usize) -> Pin<&mut T> {
    // SAFETY: slice fields are pinned since the whole slice is pinned
    // <https://discord.com/channels/273534239310479360/818964227783262209/1035563044887072808>
    unsafe { slice.map_unchecked_mut(|futs| &mut futs[i]) }
}

pub struct ConcurrentProcessQueue<F> {
    inner: Pin<Box<[Option<F>]>>,
    sparse: Arc<AtomicSparseSet>,
}

#[derive(Debug)]
struct AtomicSparseSet {
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
                Ok(l) => len = l,
                Err(l) => len = l,
            }
            spin_loop()
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
                Ok(l) => len = l,
                Err(l) => len = l,
            }
            spin_loop()
        }
    }
}

impl<F> ConcurrentProcessQueue<F> {
    pub fn new(cap: usize) -> Self {
        let mut v: Vec<Option<F>> = Vec::with_capacity(cap);
        v.resize_with(cap, || None);
        Self {
            inner: v.into_boxed_slice().into(),
            sparse: Arc::new(AtomicSparseSet::new(cap)),
        }
    }
    pub fn push(&mut self, fut: F) {
        let mut inner: Pin<&mut [Option<F>]> = self.inner.as_mut();
        for i in 0..inner.as_ref().len() {
            let mut x = project_slice(inner.as_mut(), i);
            if x.is_none() {
                x.set(Some(fut));
                self.sparse.push(i);
                break;
            }
        }
    }
}

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

impl<F: Future> Stream for ConcurrentProcessQueue<F> {
    type Item = F::Output;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        while let Some(i) = self.sparse.pop() {
            // create the waker with the current waker and the queue. no future
            let waker = Arc::new(InnerWaker {
                index: i,
                waker: cx.waker().clone(),
                sparse: self.sparse.clone(),
            })
            .into();
            let mut cx = Context::from_waker(&waker);

            let mut inner = self.inner.as_mut();
            let mut fut = project_slice(inner.as_mut(), i);

            let res = match fut.as_mut().as_pin_mut() {
                // poll the current task
                Some(fut) => fut.poll(&mut cx),
                None => continue,
            };

            if let Poll::Ready(x) = res {
                fut.set(None);
                return Poll::Ready(Some(x));
            }
        }
        if self.inner.iter().filter_map(|x| x.as_ref()).count() == 0 {
            Poll::Ready(None)
        } else {
            Poll::Pending
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

    use futures::{Future, StreamExt};
    use pin_project_lite::pin_project;
    use tokio::time::Sleep;

    use crate::ConcurrentProcessQueue;

    #[tokio::test]
    async fn single() {
        let mut buffer = ConcurrentProcessQueue::new(10);
        buffer.push(tokio::time::sleep(Duration::from_secs(1)));
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

        let mut buffer = ConcurrentProcessQueue::new(10);
        // build up
        for i in 0..10 {
            buffer.push(wait(&poll_count, i));
        }
        // poll and insert
        for i in 0..100 {
            assert!(buffer.next().await.is_some());
            buffer.push(wait(&poll_count, i));
        }
        // drain down
        for _ in 0..10 {
            assert!(buffer.next().await.is_some());
        }

        let count = poll_count.load(std::sync::atomic::Ordering::SeqCst);
        assert_eq!(count, 220);
    }
}
