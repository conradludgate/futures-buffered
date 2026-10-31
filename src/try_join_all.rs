use alloc::vec::Vec;
use core::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{join_output::JoinOutput, FuturesUnorderedBounded, TryFuture};

#[must_use = "futures do nothing unless you `.await` or poll them"]
/// Future for the [`try_join_all`] function.
pub struct TryJoinAll<F: TryFuture> {
    queue: FuturesUnorderedBounded<F>,
    output: JoinOutput<F::Ok>,
    errored: bool,
}

impl<F: TryFuture> Unpin for TryJoinAll<F> {}

/// Creates a future which represents a collection of the outputs of the futures
/// given.
///
/// The returned future will drive execution for all of its underlying futures,
/// collecting the results into a destination `Vec<T>` in the same order as they
/// were provided.
///
/// If any future returns an error then all other futures will be canceled and
/// an error will be returned immediately. If all futures complete successfully,
/// however, then the returned future will succeed with a `Vec` of all the
/// successful results.
///
/// # Examples
///
/// ```
/// # futures::executor::block_on(async {
/// use futures_buffered::try_join_all;
///
/// async fn foo(i: u32) -> Result<u32, u32> {
///     if i < 4 { Ok(i) } else { Err(i) }
/// }
///
/// let futures = vec![foo(1), foo(2), foo(3)];
/// assert_eq!(try_join_all(futures).await, Ok(vec![1, 2, 3]));
///
/// let futures = vec![foo(1), foo(2), foo(3), foo(4)];
/// assert_eq!(try_join_all(futures).await, Err(4));
/// # });
/// ```
///
/// See [`join_all`](crate::join_all()) for benchmark results
pub fn try_join_all<I>(iter: I) -> TryJoinAll<<I as IntoIterator>::Item>
where
    I: IntoIterator,
    <I as IntoIterator>::Item: TryFuture,
{
    // create the queue
    let queue = FuturesUnorderedBounded::from_iter(iter);

    // create the output buffer
    let output = JoinOutput::new(queue.capacity());

    TryJoinAll {
        queue,
        output,
        errored: false,
    }
}

impl<F: TryFuture> Future for TryJoinAll<F> {
    type Output = Result<Vec<F::Ok>, F::Err>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        assert!(!self.errored, "TryJoinAll polled after an error");
        loop {
            match self.as_mut().queue.poll_inner(cx) {
                Poll::Ready(Some((i, Ok(t)))) => {
                    self.output.write(i, t);
                }
                Poll::Ready(Some((_, Err(e)))) => {
                    self.errored = true;
                    break Poll::Ready(Err(e));
                }
                Poll::Ready(None) => {
                    // SAFETY: FromIterator populated every original queue
                    // index and this private queue is never refilled. By
                    // poll_inner's invariant, None follows one returned output
                    // per index. An Err sets errored before returning and the
                    // assertion above prevents subsequent conversion, so every
                    // output on this path was Ok and passed to write. Its
                    // invariant supplies a valid owned T in each slot, or the
                    // buffer is empty after a previous successful take_vec.
                    break Poll::Ready(Ok(unsafe { self.output.take_vec() }));
                }
                Poll::Pending => break Poll::Pending,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use core::future::ready;

    #[test]
    fn try_join_all() {
        let x = futures::executor::block_on(crate::try_join_all(
            (0..10).map(|i| ready(Result::<_, ()>::Ok(i))),
        ))
        .unwrap();

        assert_eq!(x, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
        assert_eq!(x.capacity(), 10);

        futures::executor::block_on(crate::try_join_all(
            (0..10).map(|i| ready(if i == 9 { Err(()) } else { Ok(i) })),
        ))
        .unwrap_err();
    }
}
