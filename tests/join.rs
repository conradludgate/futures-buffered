use futures_test::task::noop_context;
use std::{
    future::{pending, ready, Future},
    panic::{catch_unwind, AssertUnwindSafe},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    task::Poll,
};

#[repr(align(256))]
struct Value {
    id: usize,
    drops: Arc<Vec<AtomicUsize>>,
    panic: bool,
}
impl Drop for Value {
    fn drop(&mut self) {
        assert_eq!(
            self.drops[self.id].fetch_add(1, Ordering::Relaxed),
            0,
            "double drop"
        );
        assert!(!self.panic, "output destructor panic");
    }
}
fn counts(n: usize) -> Arc<Vec<AtomicUsize>> {
    Arc::new((0..n).map(|_| AtomicUsize::new(0)).collect())
}
fn value(id: usize, drops: &Arc<Vec<AtomicUsize>>) -> Value {
    Value {
        id,
        drops: drops.clone(),
        panic: false,
    }
}
fn assert_counts(drops: &Arc<Vec<AtomicUsize>>, count: usize) {
    for (i, drops) in drops.iter().enumerate() {
        assert_eq!(drops.load(Ordering::Relaxed), count, "value {i}");
    }
}

#[test]
fn repoll_after_error_panics() {
    let mut joined = Box::pin(futures_buffered::try_join_all([ready(Err::<u64, ()>(()))]));
    assert!(matches!(
        joined.as_mut().poll(&mut noop_context()),
        Poll::Ready(Err(()))
    ));
    assert!(catch_unwind(AssertUnwindSafe(|| joined
        .as_mut()
        .poll(&mut noop_context())))
    .is_err());
}

#[test]
fn cancellation_drops_sparse_outputs() {
    let drops = counts(130);
    let futures = (0..130).map(|i| {
        let value = value(i, &drops);
        async move {
            if i % 3 == 0 {
                pending::<()>().await;
            }
            value
        }
    });
    let mut joined = Box::pin(futures_buffered::join_all(futures));
    assert!(joined.as_mut().poll(&mut noop_context()).is_pending());
    assert_counts(&drops, 0);
    drop(joined);
    assert_counts(&drops, 1);
}

#[test]
fn try_join_drops_outputs_on_cancellation_and_error() {
    for error in [false, true] {
        let drops = counts(130);
        let futures = (0..130).map(|i| {
            let value = value(i, &drops);
            async move {
                if !error && i % 3 == 0 {
                    pending::<()>().await;
                }
                if error && i == 64 {
                    Err(())
                } else {
                    Ok(value)
                }
            }
        });
        let mut joined = Box::pin(futures_buffered::try_join_all(futures));
        let result = joined.as_mut().poll(&mut noop_context());
        if error {
            assert!(matches!(result, Poll::Ready(Err(()))));
        } else {
            assert!(result.is_pending());
        }
        drop(joined);
        assert_counts(&drops, 1);
    }
}

#[test]
fn success_transfers_outputs_in_order_without_dropping_them() {
    for fallible in [false, true] {
        let drops = counts(130);
        let output = if fallible {
            futures::executor::block_on(futures_buffered::try_join_all(
                (0..130).map(|i| ready(Ok::<_, ()>(value(i, &drops)))),
            ))
            .ok()
            .unwrap()
        } else {
            futures::executor::block_on(futures_buffered::join_all(
                (0..130).map(|i| ready(value(i, &drops))),
            ))
        };
        assert_eq!(output.as_ptr() as usize % 256, 0);
        assert_eq!(
            output.iter().map(|value| value.id).collect::<Vec<_>>(),
            (0..130).collect::<Vec<_>>()
        );
        assert_counts(&drops, 0);
        drop(output);
        assert_counts(&drops, 1);
    }
}

#[test]
fn child_panic_drops_completed_outputs() {
    for fallible in [false, true] {
        let drops = counts(3);
        assert!(catch_unwind(AssertUnwindSafe(|| {
            let futures = (0..3).map(|i| {
                let value = value(i, &drops);
                async move {
                    assert_ne!(i, 1, "child poll panic");
                    if i == 2 {
                        pending::<()>().await;
                    }
                    value
                }
            });
            if fallible {
                let mut joined = Box::pin(futures_buffered::try_join_all(
                    futures.map(|f| async { Ok::<_, ()>(f.await) }),
                ));
                let _ = joined.as_mut().poll(&mut noop_context());
            } else {
                let mut joined = Box::pin(futures_buffered::join_all(futures));
                let _ = joined.as_mut().poll(&mut noop_context());
            }
        }))
        .is_err());
        assert_counts(&drops, 1);
    }
}

#[test]
fn output_panic_does_not_leak_other_outputs() {
    for fallible in [false, true] {
        let drops = counts(3);
        let futures = (0..3).map(|i| {
            let mut value = value(i, &drops);
            value.panic = i == 0;
            async move {
                if i == 2 {
                    pending::<()>().await;
                }
                value
            }
        });
        if fallible {
            let mut joined = Box::pin(futures_buffered::try_join_all(
                futures.map(|f| async { Ok::<_, ()>(f.await) }),
            ));
            assert!(joined.as_mut().poll(&mut noop_context()).is_pending());
            assert!(catch_unwind(AssertUnwindSafe(|| drop(joined))).is_err());
        } else {
            let mut joined = Box::pin(futures_buffered::join_all(futures));
            assert!(joined.as_mut().poll(&mut noop_context()).is_pending());
            assert!(catch_unwind(AssertUnwindSafe(|| drop(joined))).is_err());
        }
        assert_counts(&drops, 1);
    }
}

#[test]
fn empty_and_zero_sized_outputs() {
    assert_eq!(
        futures::executor::block_on(futures_buffered::join_all(std::iter::empty::<
            std::future::Ready<()>,
        >())),
        []
    );
    assert_eq!(
        futures::executor::block_on(futures_buffered::try_join_all(std::iter::empty::<
            std::future::Ready<Result<(), ()>>,
        >())),
        Ok(vec![])
    );
    assert_eq!(
        futures::executor::block_on(futures_buffered::join_all([ready(()), ready(())])),
        [(), ()]
    );
    assert_eq!(
        futures::executor::block_on(futures_buffered::try_join_all([
            ready(Ok::<_, ()>(())),
            ready(Ok(()))
        ])),
        Ok(vec![(), ()])
    );
}
