use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll, Wake},
};

use atomic_sparse::AtomicSparseSet;
use futures::{task::AtomicWaker, Future, Stream};
use pin_project_lite::pin_project;
use sparse::SparseSet;

mod atomic_sparse;
mod sparse;

fn project_slice<T>(slice: Pin<&mut [T]>, i: usize) -> Pin<&mut T> {
    // SAFETY: slice fields are pinned since the whole slice is pinned
    // <https://discord.com/channels/273534239310479360/818964227783262209/1035563044887072808>
    unsafe { slice.map_unchecked_mut(|futs| &mut futs[i]) }
}

pub struct ConcurrentProcessQueue<F> {
    slots: SparseSet,
    inner: Pin<Box<[Task<F>]>>,
    shared: Arc<Shared>,
}

struct Shared {
    sparse: AtomicSparseSet,
    waker: AtomicWaker,
}

pin_project!(
    struct Task<F> {
        #[pin]
        slot: Option<F>,
        waker: Arc<InnerWaker>,
    }
);

impl<F> ConcurrentProcessQueue<F> {
    pub fn new(cap: usize) -> Self {
        let shared = Arc::new(Shared {
            sparse: AtomicSparseSet::new(cap),
            waker: AtomicWaker::new(),
        });

        let mut v: Vec<Task<F>> = Vec::with_capacity(cap);
        let mut slots = SparseSet::new(cap);
        for i in 0..cap {
            let waker = Arc::new(InnerWaker {
                index: i,
                shared: shared.clone(),
            });
            v.push(Task { slot: None, waker });
            slots.push(i);
        }
        Self {
            inner: v.into_boxed_slice().into(),
            shared,
            slots,
        }
    }
    pub fn push(&mut self, fut: F) -> Result<(), F> {
        let mut inner: Pin<&mut [Task<F>]> = self.inner.as_mut();
        if let Some(i) = self.slots.pop() {
            project_slice(inner.as_mut(), i)
                .project()
                .slot
                .set(Some(fut));
            self.shared.sparse.push(i);
            Ok(())
        } else {
            Err(fut)
        }
    }
}

struct InnerWaker {
    index: usize,
    shared: Arc<Shared>,
}
impl Wake for InnerWaker {
    fn wake(self: std::sync::Arc<Self>) {
        self.wake_by_ref()
    }
    /// on wake, insert the future index into the queue, and then wake the original waker too
    fn wake_by_ref(self: &Arc<Self>) {
        self.shared.sparse.push(self.index);
        self.shared.waker.wake();
    }
}

impl<F: Future> Stream for ConcurrentProcessQueue<F> {
    type Item = F::Output;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.shared.waker.register(cx.waker());
        while let Some(i) = self.shared.sparse.pop() {
            let mut inner = self.inner.as_mut();
            let mut task = project_slice(inner.as_mut(), i).project();

            let waker = task.waker.clone().into();
            let mut cx = Context::from_waker(&waker);

            let res = match task.slot.as_mut().as_pin_mut() {
                // poll the current task
                Some(fut) => fut.poll(&mut cx),
                None => continue,
            };

            if let Poll::Ready(x) = res {
                task.slot.set(None);
                self.slots.push(i);
                return Poll::Ready(Some(x));
            }
        }
        if self.inner.iter().filter_map(|x| x.slot.as_ref()).count() == 0 {
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
        buffer
            .push(tokio::time::sleep(Duration::from_secs(1)))
            .unwrap();
        buffer.next().await;
    }

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

    #[tokio::test]
    async fn multi() {
        let poll_count = Arc::new(AtomicUsize::new(0));

        let mut buffer = ConcurrentProcessQueue::new(10);
        // build up
        for i in 0..10 {
            buffer.push(wait(&poll_count, i)).map_err(drop).unwrap();
        }
        // poll and insert
        for i in 0..100 {
            assert!(buffer.next().await.is_some());
            buffer.push(wait(&poll_count, i)).map_err(drop).unwrap();
        }
        // drain down
        for _ in 0..10 {
            assert!(buffer.next().await.is_some());
        }

        let count = poll_count.load(std::sync::atomic::Ordering::SeqCst);
        assert_eq!(count, 220);
    }
}
