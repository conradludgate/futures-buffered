use core::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::FuturesOrderedBounded;
use crate::FuturesUnorderedBounded;
use futures_core::ready;
use futures_core::Stream;
use pin_project_lite::pin_project;

impl<T: ?Sized + Stream> BufferedStreamExt for T {}

/// An extension trait for `Stream`s that provides a variety of convenient
/// combinator functions.
pub trait BufferedStreamExt: Stream {
    /// An adaptor for creating a buffered list of pending futures.
    ///
    /// If this stream's item can be converted into a future, then this adaptor
    /// will buffer up to at most `n` futures and then return the outputs in the
    /// same order as the underlying stream. No more than `n` futures will be
    /// buffered at any point in time, and less than `n` may also be buffered
    /// depending on the state of each future.
    ///
    /// The returned stream will be a stream of each future's output.
    fn buffered_ordered(self, n: usize) -> BufferedOrdered<Self>
    where
        Self::Item: Future,
        Self: Sized,
    {
        BufferedOrdered {
            stream: Some(self),
            in_progress_queue: FuturesOrderedBounded::new(n),
        }
    }

    /// An adaptor for creating a buffered list of pending futures (unordered).
    ///
    /// If this stream's item can be converted into a future, then this adaptor
    /// will buffer up to `n` futures and then return the outputs in the order
    /// in which they complete. No more than `n` futures will be buffered at
    /// any point in time, and less than `n` may also be buffered depending on
    /// the state of each future.
    ///
    /// The returned stream will be a stream of each future's output.
    ///
    /// # Examples
    ///
    /// ```
    /// # futures::executor::block_on(async {
    /// use futures::channel::oneshot;
    /// use futures::stream::{self, StreamExt};
    /// use futures_buffered::BufferedStreamExt;
    ///
    /// let (send_one, recv_one) = oneshot::channel();
    /// let (send_two, recv_two) = oneshot::channel();
    ///
    /// let stream_of_futures = stream::iter(vec![recv_one, recv_two]);
    /// let mut buffered = stream_of_futures.buffered_unordered(10);
    ///
    /// send_two.send(2i32)?;
    /// assert_eq!(buffered.next().await, Some(Ok(2i32)));
    ///
    /// send_one.send(1i32)?;
    /// assert_eq!(buffered.next().await, Some(Ok(1i32)));
    ///
    /// assert_eq!(buffered.next().await, None);
    /// # Ok::<(), i32>(()) }).unwrap();
    /// ```
    ///
    /// See [`BufferUnordered`] for performance details
    fn buffered_unordered(self, n: usize) -> BufferUnordered<Self>
    where
        Self::Item: Future,
        Self: Sized,
    {
        BufferUnordered {
            stream: Some(self),
            in_progress_queue: FuturesUnorderedBounded::new(n),
        }
    }
}

pin_project! {
    /// Stream for the [`buffered_ordered`](BufferedStreamExt::buffered_ordered) method.
    #[must_use = "streams do nothing unless polled"]
    pub struct BufferedOrdered<St>
    where
        St: Stream,
        St::Item: Future,
    {
        #[pin]
        stream: Option<St>,
        in_progress_queue: FuturesOrderedBounded<St::Item>,
    }
}

impl<St> Stream for BufferedOrdered<St>
where
    St: Stream,
    St::Item: Future,
{
    type Item = <St::Item as Future>::Output;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        // First up, try to spawn off as many futures as possible by filling up
        // our queue of futures.
        let ordered = this.in_progress_queue;
        while ordered.in_progress_queue.tasks.len() < ordered.in_progress_queue.tasks.capacity() {
            if let Some(s) = this.stream.as_mut().as_pin_mut() {
                match s.poll_next(cx) {
                    Poll::Ready(Some(fut)) => {
                        ordered.push_back(fut);
                        continue;
                    }
                    Poll::Ready(None) => this.stream.as_mut().set(None),
                    Poll::Pending => {}
                }
            }
            break;
        }

        // Attempt to pull the next value from the in_progress_queue
        let res = Pin::new(ordered).poll_next(cx);
        if let Some(val) = ready!(res) {
            return Poll::Ready(Some(val));
        }

        // If more values are still coming from the stream, we're not done yet
        if this.stream.is_none() {
            Poll::Ready(None)
        } else {
            Poll::Pending
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match &self.stream {
            Some(s) => {
                let queue_len = self.in_progress_queue.len();
                let (lower, upper) = s.size_hint();
                let lower = lower.saturating_add(queue_len);
                let upper = match upper {
                    Some(x) => x.checked_add(queue_len),
                    None => None,
                };
                (lower, upper)
            }
            _ => (0, Some(0)),
        }
    }
}

pin_project!(
    /// Stream for the [`buffered_unordered`](BufferedStreamExt::buffered_unordered)
    /// method.
    ///
    /// # Examples
    ///
    /// ```
    /// # futures::executor::block_on(async {
    /// use futures::channel::oneshot;
    /// use futures::stream::{self, StreamExt};
    /// use futures_buffered::BufferedStreamExt;
    ///
    /// let (send_one, recv_one) = oneshot::channel();
    /// let (send_two, recv_two) = oneshot::channel();
    ///
    /// let stream_of_futures = stream::iter(vec![recv_one, recv_two]);
    /// let mut buffered = stream_of_futures.buffered_unordered(10);
    ///
    /// send_two.send(2i32)?;
    /// assert_eq!(buffered.next().await, Some(Ok(2i32)));
    ///
    /// send_one.send(1i32)?;
    /// assert_eq!(buffered.next().await, Some(Ok(1i32)));
    ///
    /// assert_eq!(buffered.next().await, None);
    /// # Ok::<(), i32>(()) }).unwrap();
    /// ```
    ///
    /// ## Benchmarks
    ///
    /// ### Speed
    ///
    /// Running 65536 100us timers with 256 concurrent jobs in a single threaded tokio runtime:
    ///
    /// ```text
    /// futures::stream::BufferUnordered    time:   [420.33 ms 422.57 ms 424.83 ms]
    /// futures_buffered::BufferUnordered   time:   [363.39 ms 365.59 ms 367.78 ms]
    /// ```
    ///
    /// ### Memory usage
    ///
    /// Running 512000 `Ready<i32>` futures with 256 concurrent jobs.
    ///
    /// - count: the number of times alloc/dealloc was called
    /// - alloc: the number of cumulative bytes allocated
    /// - dealloc: the number of cumulative bytes deallocated
    ///
    /// ```text
    /// futures::stream::BufferUnordered
    ///     count:    1024002
    ///     alloc:    40960144 B
    ///     dealloc:  40960000 B
    ///
    /// futures_buffered::BufferUnordered
    ///     count:    2
    ///     alloc:    8264 B
    ///     dealloc:  0 B
    /// ```
    #[must_use = "streams do nothing unless polled"]
    pub struct BufferUnordered<S: Stream> {
        #[pin]
        stream: Option<S>,
        in_progress_queue: FuturesUnorderedBounded<S::Item>,
    }
);

impl<St> Stream for BufferUnordered<St>
where
    St: Stream,
    St::Item: Future,
{
    type Item = <St::Item as Future>::Output;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        // First up, try to spawn off as many futures as possible by filling up
        // our queue of futures.
        let unordered = this.in_progress_queue;
        while unordered.tasks.len() < unordered.tasks.capacity() {
            if let Some(s) = this.stream.as_mut().as_pin_mut() {
                match s.poll_next(cx) {
                    Poll::Ready(Some(fut)) => {
                        unordered.push(fut);
                        continue;
                    }
                    Poll::Ready(None) => this.stream.as_mut().set(None),
                    Poll::Pending => {}
                }
            }
            break;
        }

        // Attempt to pull the next value from the in_progress_queue
        match Pin::new(unordered).poll_next(cx) {
            x @ (Poll::Pending | Poll::Ready(Some(_))) => return x,
            Poll::Ready(None) => {}
        }

        // If more values are still coming from the stream, we're not done yet
        if this.stream.as_pin_mut().is_none() {
            Poll::Ready(None)
        } else {
            Poll::Pending
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match &self.stream {
            Some(s) => {
                let queue_len = self.in_progress_queue.len();
                let (lower, upper) = s.size_hint();
                let lower = lower.saturating_add(queue_len);
                let upper = match upper {
                    Some(x) => x.checked_add(queue_len),
                    None => None,
                };
                (lower, upper)
            }
            _ => (0, Some(0)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::{channel::oneshot, stream, StreamExt};
    use futures_test::task::noop_context;

    #[test]
    fn buffered_ordered() {
        let (send_one, recv_one) = oneshot::channel();
        let (send_two, recv_two) = oneshot::channel();

        let stream_of_futures = stream::iter(vec![recv_one, recv_two]);
        let mut buffered = stream_of_futures.buffered_ordered(10);
        let mut cx = noop_context();

        // sized properly
        assert_eq!(buffered.size_hint(), (2, Some(2)));

        // make sure it returns pending
        assert_eq!(buffered.poll_next_unpin(&mut cx), Poll::Pending);

        // returns in a fixed order
        send_two.send(2i32).unwrap();
        assert_eq!(buffered.poll_next_unpin(&mut cx), Poll::Pending);

        send_one.send(1i32).unwrap();
        assert_eq!(
            buffered.poll_next_unpin(&mut cx),
            Poll::Ready(Some(Ok(1i32)))
        );
        assert_eq!(
            buffered.poll_next_unpin(&mut cx),
            Poll::Ready(Some(Ok(2i32)))
        );

        // completes properly
        assert_eq!(buffered.poll_next_unpin(&mut cx), Poll::Ready(None));
    }

    #[test]
    fn buffered_unordered() {
        let (send_one, recv_one) = oneshot::channel();
        let (send_two, recv_two) = oneshot::channel();

        let stream_of_futures = stream::iter(vec![recv_one, recv_two]);
        let mut buffered = stream_of_futures.buffered_unordered(10);
        let mut cx = noop_context();

        // sized properly
        assert_eq!(buffered.size_hint(), (2, Some(2)));

        // make sure it returns pending
        assert_eq!(buffered.poll_next_unpin(&mut cx), Poll::Pending);

        // returns in any order
        send_two.send(2i32).unwrap();
        assert_eq!(
            buffered.poll_next_unpin(&mut cx),
            Poll::Ready(Some(Ok(2i32)))
        );

        send_one.send(1i32).unwrap();
        assert_eq!(
            buffered.poll_next_unpin(&mut cx),
            Poll::Ready(Some(Ok(1i32)))
        );

        // completes properly
        assert_eq!(buffered.poll_next_unpin(&mut cx), Poll::Ready(None));
    }
}
