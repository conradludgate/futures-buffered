use std::{
    future::Future,
    mem::MaybeUninit,
    pin::Pin,
    task::{Context, Poll},
};

use crate::FuturesUnorderedBounded;

#[must_use = "futures do nothing unless you `.await` or poll them"]
/// Future for the [`join_all`] function.
pub struct JoinAll<F: Future> {
    queue: FuturesUnorderedBounded<F>,
    output: Box<[MaybeUninit<F::Output>]>,
}

impl<F: Future> Unpin for JoinAll<F> {}

/// Creates a future which represents a collection of the outputs of the futures
/// given.
///
/// The returned future will drive execution for all of its underlying futures,
/// collecting the results into a destination `Vec<T>` in the same order as they
/// were provided.
///
/// # Examples
///
/// ```
/// # futures::executor::block_on(async {
/// use futures_buffered::join_all;
///
/// async fn foo(i: u32) -> u32 { i }
///
/// let futures = vec![foo(1), foo(2), foo(3)];
/// assert_eq!(join_all(futures).await, [1, 2, 3]);
/// # });
/// ```
///
/// ## Benchmarks
///
/// ### Speed
///
/// Running 256 http requests (over an already establish HTTP2 connection) in a single threaded tokio runtime:
///
/// ```text
/// futures::future::join_all   time:   [746.38 µs 749.82 µs 753.38 µs]
/// futures_buffered::join_all  time:   [654.01 µs 666.69 µs 682.90 µs]
/// ```
///
/// ### Memory usage
///
/// Running 256 `Ready<i32>` futures.
///
/// - count: the number of times alloc/dealloc was called
/// - alloc: the number of cumulative bytes allocated
/// - dealloc: the number of cumulative bytes deallocated
///
/// ```text
/// futures::future::join_all
///     count:    512
///     alloc:    26744 B
///     dealloc:  26744 B
///
/// futures_buffered::join_all
///     count:    10
///     alloc:    16448 B
///     dealloc:  16448 B
/// ```
pub fn join_all<I>(iter: I) -> JoinAll<<I as IntoIterator>::Item>
where
    I: IntoIterator,
    <I as IntoIterator>::Item: Future,
{
    // create the queue
    let queue = FuturesUnorderedBounded::from_iter(iter);

    // create the output buffer
    let mut output = Vec::with_capacity(queue.capacity());
    output.resize_with(queue.capacity(), MaybeUninit::uninit);

    JoinAll {
        queue,
        output: output.into_boxed_slice(),
    }
}

impl<F: Future> Future for JoinAll<F> {
    type Output = Vec<F::Output>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        loop {
            match self.as_mut().queue.poll_inner(cx) {
                Poll::Ready(Some((i, x))) => {
                    self.output[i].write(x);
                }
                Poll::Ready(None) => {
                    // SAFETY: for Ready(None) to be returned, we know that every future in the queue
                    // must be consumed. Since we have a 1:1 mapping in the queue to our output, we
                    // know that every output entry is init.
                    let boxed = unsafe {
                        // take the boxed slice
                        let boxed =
                            std::mem::replace(&mut self.output, Vec::new().into_boxed_slice());

                        // Box::assume_init
                        let raw = Box::into_raw(boxed);
                        Box::from_raw(raw as *mut [F::Output])
                    };

                    break Poll::Ready(boxed.into_vec());
                }
                Poll::Pending => break Poll::Pending,
            }
        }
    }
}
