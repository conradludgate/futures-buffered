use core::{
    pin::Pin,
    task::{Context, Poll},
};

use futures_core::Stream;

use crate::FuturesUnorderedBounded;

pub struct Merge<S> {
    pub(crate) streams: FuturesUnorderedBounded<S>,
}

impl<S> Merge<S> {
    /// Push a stream into the set.
    ///
    /// This method adds the given stream to the set. This method will not
    /// call [`poll_next`](futures_core::Stream::poll_next) on the submitted stream. The caller must
    /// ensure that [`Merge::poll_next`](Stream::poll_next) is called
    /// in order to receive wake-up notifications for the given stream.
    ///
    /// # Panics
    /// This method will panic if the buffer is currently full. See [`Merge::try_push`] to get a result instead
    #[track_caller]
    pub fn push(&mut self, stream: S) {
        if self.try_push(stream).is_err() {
            panic!("attempted to push into a full `Merge`")
        }
    }

    /// Push a future into the set.
    ///
    /// This method adds the given future to the set. This method will not
    /// call [`poll`](core::future::Future::poll) on the submitted future. The caller must
    /// ensure that [`FuturesUnorderedBounded::poll_next`](Stream::poll_next) is called
    /// in order to receive wake-up notifications for the given future.
    ///
    /// # Errors
    /// This method will error if the buffer is currently full, returning the future back
    pub fn try_push(&mut self, stream: S) -> Result<(), S> {
        self.streams.try_push_with(stream, core::convert::identity)
    }
}

impl<S: Stream> Stream for Merge<S> {
    type Item = S::Item;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match self.streams.poll_inner_no_remove(cx, S::poll_next) {
                // if we have a value from the stream, wake up that slot again
                Poll::Ready(Some((i, Some(x)))) => {
                    self.streams.shared.push(i);
                    break Poll::Ready(Some(x));
                }
                // if a stream completed, remove it from the queue
                Poll::Ready(Some((i, None))) => {
                    self.streams.tasks.remove(i);
                }
                Poll::Pending => break Poll::Pending,
                Poll::Ready(None) => break Poll::Ready(None),
            }
        }
    }
}

impl<S: Stream> FromIterator<S> for Merge<S> {
    fn from_iter<T>(iter: T) -> Self
    where
        T: IntoIterator<Item = S>,
    {
        Self {
            streams: iter.into_iter().collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use core::future::ready;

    use super::*;
    use futures::executor::block_on;
    use futures::prelude::*;
    use futures::stream;

    #[test]
    fn merge_tuple_4() {
        block_on(async {
            let a = stream::once(ready(1));
            let b = stream::once(ready(2));
            let c = stream::once(ready(3));
            let d = stream::once(ready(4));
            let mut s: Merge<_> = [a, b, c, d].into_iter().collect();

            let mut counter = 0;
            while let Some(n) = s.next().await {
                counter += n;
            }
            assert_eq!(counter, 10);
        })
    }
}
