use cordyceps::{
    mpsc_queue::{Links, TryDequeueError},
    Linked, MpscQueue,
};
use core::{
    ptr::{self, NonNull},
    task::Waker,
};
use diatomic_waker::primitives::DiatomicWaker;
use spin::mutex::SpinMutex;

use triomphe::{
    task::{ThinItemWake, WakerRef},
    ThinArcItem, ThinArcList, WithOffset,
};

pub(crate) struct WakerList {
    inner: ThinArcList<WakerHeader, WakerItem>,
}

impl WakerList {
    /// Allocates a `WakerList` suitable for `cap` tasks.
    pub(crate) fn new(cap: usize) -> Self {
        let items = (0..cap + 1).map(|i| WakerItem {
            links: if i == cap {
                Links::new_stub()
            } else {
                Links::new()
            },
            wake_lock: SpinMutex::new(false),
        });
        let inner = ThinArcList::header_from_iter(items, |items| {
            let stub = &items[cap];
            WakerHeader {
                waker: DiatomicWaker::new(),
                queue: MpscQueue::new_with_stub(WakerItemHandle(NonNull::from(stub))),
            }
        });
        Self { inner }
    }

    /// Register that the task corresponding to this index is ready.
    pub(crate) fn task_is_ready(&self, index: usize) {
        self.inner.with_item(index, |item| {
            let mut wake_lock = item.wake_lock.lock();
            let prev = core::mem::replace(&mut *wake_lock, true);

            if !prev {
                self.inner
                    .header()
                    .queue
                    .enqueue(WakerItemHandle(item.as_raw()));
            }
        });
    }

    /// Register the parent waker
    pub(crate) fn register(&mut self, waker: &Waker) {
        // Safety:
        // Diatomic waker requires we do not concurrently run
        // "register", "unregister", and "wait_until".
        // we only call register with mut access, thus we are safe.
        unsafe { self.inner.header().waker.register(waker) }
    }

    /// Request a waker for a ready task
    pub(crate) fn next_ready_task(&mut self) -> ReadySlot<(usize, WakerRef)> {
        // Safety: since we have `&mut` access to the `WakerList`, which is never cloned,
        // and no other code paths ever execute `try_dequeue` on the queue,
        // we know this will not run concurrently.
        match unsafe { self.inner.header().queue.try_dequeue_unchecked() } {
            Ok(item) => {
                // Safety: we know that the item comes from the queue, and we only ever queue items
                // previously inserted with `as_raw()`.
                // We also know that these items are bound to the lifetime of the WakerList.
                let item = unsafe { ThinArcItem::<WakerHeader, WakerItem>::from_raw_ref(item.0) };
                *item.wake_lock.lock() = false;

                let index = item.with_arc(|a| a.index());
                ReadySlot::Ready((index, WakerRef::from(item)))
            }
            Err(TryDequeueError::Inconsistent) => ReadySlot::Inconsistent,
            Err(TryDequeueError::Empty) => ReadySlot::None,
            Err(TryDequeueError::Busy) => unreachable!(),
        }
    }
}

struct WakerHeader {
    waker: DiatomicWaker,
    queue: MpscQueue<WakerItemWrapper>,
}

struct WakerItem {
    links: Links<WakerItemWrapper>,
    // if true, then this slot is already woken and queued for processing.
    // if false, then this slot is available to be queued.
    wake_lock: SpinMutex<bool>,
}

#[repr(transparent)]
struct WakerItemWrapper(WithOffset<WakerItem>);

struct WakerItemHandle(NonNull<WithOffset<WakerItem>>);

// Safety: our WakerItemWrapper is from an `ThinArcItem<WakerHeader, WakerItem>`.
// We know that `ThinArcItem<WakerHeader, WakerItem>: Send` if `WakerItem: Send + Sync`.
// `WakerItem` is already `Sync`, but it would be `Send` if `WakerItemHandle` is `Send`` :)
unsafe impl Send for WakerItemHandle {}

// SAFETY:
// 1. ThinArcItem will be pinned in memory within the ThinArcList allocation
// 2. The `Links` object enforces the ThinArcItem to be !Unpin
//
// Important caveat! The item the handle points to must be locked!
unsafe impl Linked<Links<Self>> for WakerItemWrapper {
    type Handle = WakerItemHandle;

    fn into_ptr(r: Self::Handle) -> NonNull<Self> {
        r.0.cast()
    }

    unsafe fn from_ptr(ptr: NonNull<Self>) -> Self::Handle {
        WakerItemHandle(ptr.cast())
    }

    unsafe fn links(ptr: NonNull<Self>) -> NonNull<Links<Self>> {
        let this = ptr.as_ptr().cast::<WithOffset<WakerItem>>();
        // Safety: caller guarantees that `ptr` is a valid handle to derefence.
        let links = unsafe { ptr::addr_of_mut!((*this).value.links) };
        // Safety: Since this came from a non-null pointer, the link pointer must also be non-null
        unsafe { NonNull::new_unchecked(links) }
    }
}

impl ThinItemWake<WakerHeader> for WakerItem {
    fn wake_by_ref(this: &triomphe::ThinArcItem<WakerHeader, Self>) {
        let mut wake_lock = this.wake_lock.lock();
        let prev = core::mem::replace(&mut *wake_lock, true);

        if !prev {
            this.with_parent(|r| {
                r.header().queue.enqueue(WakerItemHandle(this.as_raw()));
                r.header().waker.notify();
            })
        }
    }
}

pub(crate) enum ReadySlot<T> {
    Ready(T),
    Inconsistent,
    None,
}

#[cfg(test)]
mod tests {
    use loom::thread;

    use super::{ReadySlot, WakerList};

    #[test]
    #[cfg(not(miri))]
    fn concurrent_push_and_wake() {
        loom::model(|| {
            let mut queue = WakerList::new(1);
            queue.task_is_ready(0);

            let ReadySlot::Ready((_, waker)) = queue.next_ready_task() else {
                panic!()
            };

            let waker1 = (*waker).clone();
            thread::spawn(move || waker1.wake());
            queue.task_is_ready(0);
        })
    }

    #[test]
    #[cfg(not(miri))]
    fn concurrent_wakers() {
        loom::model(|| {
            let mut queue = WakerList::new(2);
            queue.task_is_ready(0);
            queue.task_is_ready(1);

            let ReadySlot::Ready((_, waker0)) = queue.next_ready_task() else {
                panic!()
            };
            let waker0 = waker0.clone();

            let ReadySlot::Ready((_, waker1)) = queue.next_ready_task() else {
                panic!()
            };

            thread::spawn(move || waker0.wake());
            waker1.wake_by_ref();
        })
    }
}
