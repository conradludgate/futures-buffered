//! Child futures do not allocate, so any allocation in poll is library overhead.
#![cfg(not(miri))]
use futures_test::task::noop_context;
use std::{
    alloc::{GlobalAlloc, Layout, System},
    cell::Cell,
    future::{ready, Future},
    pin::pin,
};

thread_local! {
    static ALLOCATIONS: Cell<Option<usize>> = const { Cell::new(None) };
}
struct Allocator;
#[global_allocator]
static ALLOCATOR: Allocator = Allocator;
fn record() {
    let _ = ALLOCATIONS.try_with(|count| {
        if let Some(n) = count.get() {
            count.set(Some(n + 1));
        }
    });
}
unsafe impl GlobalAlloc for Allocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        record();
        unsafe { System.alloc(layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        record();
        unsafe { System.realloc(ptr, layout, size) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}
fn count_allocations<T>(f: impl FnOnce() -> T) -> (T, usize) {
    ALLOCATIONS.with(|count| count.set(Some(0)));
    let result = f();
    let allocations = ALLOCATIONS.with(|count| count.replace(None).unwrap());
    (result, allocations)
}
fn assert_poll_does_not_allocate(future: impl Future) {
    let mut future = pin!(future);
    let mut cx = noop_context();
    let (result, allocations) = count_allocations(|| future.as_mut().poll(&mut cx));
    assert!(result.is_ready());
    assert_eq!(allocations, 0, "poll allocated or reallocated");
}

#[test]
fn bitsets_are_allocated_for_drop_or_debug_checks() {
    struct Droppable(usize);
    impl Drop for Droppable {
        fn drop(&mut self) {
            let _ = self.0;
        }
    }
    let no_drop_allocations = if cfg!(debug_assertions) { 4 } else { 3 };
    for n in [1_usize, 16, 65, 256, 1025] {
        let (joined, allocations) =
            count_allocations(|| futures_buffered::join_all((0..n).map(ready)));
        assert_eq!(allocations, no_drop_allocations);
        assert_poll_does_not_allocate(joined);
        let (joined, allocations) =
            count_allocations(|| futures_buffered::join_all((0..n).map(|i| ready(Droppable(i)))));
        assert_eq!(allocations, 4);
        assert_poll_does_not_allocate(joined);

        let (joined, allocations) = count_allocations(|| {
            futures_buffered::try_join_all((0..n).map(|i| ready(Ok::<_, ()>(i))))
        });
        assert_eq!(allocations, no_drop_allocations);
        assert_poll_does_not_allocate(joined);
        let (joined, allocations) = count_allocations(|| {
            futures_buffered::try_join_all((0..n).map(|i| ready(Ok::<_, ()>(Droppable(i)))))
        });
        assert_eq!(allocations, 4);
        assert_poll_does_not_allocate(joined);
    }
}
#[test]
fn polling_does_not_allocate() {
    for n in [0, 1, 64, 65, 256, 1025] {
        assert_poll_does_not_allocate(futures_buffered::join_all((0..n).map(ready)));
        assert_poll_does_not_allocate(futures_buffered::try_join_all(
            (0..n).map(|i| ready(Ok::<_, ()>(i))),
        ));
    }
    assert_poll_does_not_allocate(futures_buffered::try_join_all(
        (0..130).map(|i| ready(if i == 64 { Err(()) } else { Ok(i) })),
    ));
}
