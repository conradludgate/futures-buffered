#![warn(unsafe_op_in_unsafe_fn)]

use cordyceps::{
    mpsc_queue::{Links, TryDequeueError},
    Linked, MpscQueue,
};
use core::{
    mem::ManuallyDrop,
    ptr::{self, NonNull},
    task::Waker,
};
use diatomic_waker::primitives::DiatomicWaker;
use spin::mutex::SpinMutex;

use triomphe::{task::ThinItemWake, ThinArcItem, ThinArcList, WithOffset};

pub(crate) struct WakerList {
    inner: ThinArcList<WakerHeader, WakerItem>,
}

impl WakerList {
    /// Allocates an `ArcInner<T>` with sufficient space for
    /// a possibly-unsized inner value where the value has the layout provided.
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
                queue: MpscQueue::new_with_stub(NonNull::from(stub)),
            }
        });
        Self { inner }
    }

    /// Register that a future is ready
    /// Safety: index must be within capacity
    pub(crate) unsafe fn push(&self, index: usize) {
        self.inner.with_item(index, |item| {
            let mut wake_lock = item.wake_lock.lock();
            let prev = core::mem::replace(&mut *wake_lock, true);

            if !prev {
                self.inner.header().queue.enqueue(item.as_raw());
            }
        });
    }

    /// Register the waker
    pub(crate) fn register(&mut self, waker: &Waker) {
        // Safety:
        // Diatomic waker requires we do not concurrently run
        // "register", "unregister", and "wait_until".
        // we only call register with mut access, thus we are safe.
        unsafe { self.inner.header().waker.register(waker) }
    }

    /// The pop function from the 1024cores intrusive MPSC queue algorithm
    ///
    /// Note that this is unsafe as it required mutual exclusion (only one
    /// thread can call this) to be guaranteed elsewhere.
    pub(crate) unsafe fn pop(&self) -> ReadySlot<(usize, ManuallyDrop<Waker>)> {
        match unsafe { self.inner.header().queue.try_dequeue_unchecked() } {
            Ok(slot) => {
                let mut slot = unsafe { ThinArcItem::<WakerHeader, WakerItem>::from_raw_ref(slot) };
                let index = slot.index();

                *slot.wake_lock.lock() = false;

                let waker =
                    unsafe { ManuallyDrop::new(Waker::from(ManuallyDrop::take(&mut slot))) };

                ReadySlot::Ready((index, waker))
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

unsafe impl Send for WakerHeader {}
unsafe impl Sync for WakerHeader {}

struct WakerItem {
    links: Links<WakerItemWrapper>,
    // if true, then this slot is already woken and queued for processing.
    // if false, then this slot is available to be queued.
    wake_lock: SpinMutex<bool>,
}

#[repr(transparent)]
struct WakerItemWrapper(WithOffset<WakerItem>);

// SAFETY:
// 1. ArcSlotInner will be pinned in memory within the ArcSlice allocation
// 2. The `Links` object enforces ArcSlotInner to be !Unpin
unsafe impl Linked<Links<Self>> for WakerItemWrapper {
    type Handle = NonNull<WithOffset<WakerItem>>;

    fn into_ptr(r: Self::Handle) -> NonNull<Self> {
        r.cast()
    }

    unsafe fn from_ptr(ptr: NonNull<Self>) -> Self::Handle {
        ptr.cast()
    }

    unsafe fn links(ptr: NonNull<Self>) -> NonNull<Links<Self>> {
        let this = ptr.as_ptr().cast::<WithOffset<WakerItem>>();
        let links = unsafe { ptr::addr_of_mut!((*this).value.links) };
        unsafe { NonNull::new_unchecked(links) }
    }
}

impl ThinItemWake<WakerHeader> for WakerItem {
    fn wake_by_ref(this: &triomphe::ThinArcItem<WakerHeader, Self>) {
        let mut wake_lock = this.wake_lock.lock();
        let prev = core::mem::replace(&mut *wake_lock, true);

        if !prev {
            this.with_parent(|r| {
                r.header().queue.enqueue(this.as_raw());
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
