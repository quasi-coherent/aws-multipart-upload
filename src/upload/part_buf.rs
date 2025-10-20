use crate::client::SendUploadPart;
use crate::error::{Error as UploadError, Result};
use crate::sdk::CompletedParts;
use crate::upload::UnitReturn;

use futures::stream::FuturesUnordered;
use futures::{Stream, ready};
use multipart_write::prelude::*;
use std::fmt::{self, Debug, Formatter};
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::task::{Context, Poll};

/// `PartBuf` is a multipart writer whose write operation is to push to a buffer
/// of part upload request futures.  Flushing this writer will await the buffer
/// until all part uploads have finished.
///
/// Calling `poll_complete` returns the successfully completed part uploads in
/// the output [`CompletedParts`].
#[must_use = "futures do nothing unless polled"]
#[pin_project::pin_project]
pub struct PartBuf {
    #[pin]
    pending: FuturesUnordered<SendUploadPart>,
    completed: CompletedParts,
    capacity: Option<NonZeroUsize>,
}

impl Default for PartBuf {
    fn default() -> Self {
        Self {
            pending: FuturesUnordered::new(),
            completed: CompletedParts::default(),
            capacity: None,
        }
    }
}

impl PartBuf {
    /// Create a new default part upload buffer.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a `PartBuf` with capacity `capacity`.
    pub fn with_capacity<T: Into<Option<usize>>>(capacity: T) -> Self {
        Self {
            pending: FuturesUnordered::new(),
            completed: CompletedParts::default(),
            capacity: capacity.into().and_then(NonZeroUsize::new),
        }
    }
}

impl MultipartWrite<SendUploadPart> for PartBuf {
    type Ret = UnitReturn;
    type Output = CompletedParts;
    type Error = UploadError;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        let mut this = self.project();

        // Poke the pending uploads to see if any are ready.
        while let Poll::Ready(Some(res)) = this.pending.as_mut().poll_next(cx) {
            match res {
                Ok(v) => this.completed.push(v),
                Err(e) => return Poll::Ready(Err(e)),
            }
        }
        if this.capacity.is_none_or(|n| this.pending.len() < n.get()) {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }

    fn start_send(mut self: Pin<&mut Self>, part: SendUploadPart) -> Result<Self::Ret> {
        self.as_mut().pending.push(part);
        Ok(UnitReturn)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        let mut this = self.project();

        while !this.pending.is_empty() {
            match ready!(this.pending.as_mut().poll_next(cx)) {
                Some(Ok(v)) => {
                    this.completed.push(v);
                }
                Some(Err(e)) => return Poll::Ready(Err(e)),
                // The stream stopped producing, i.e., the collection is empty.
                _ => break,
            }
        }
        Poll::Ready(Ok(()))
    }

    fn poll_complete(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Result<Self::Output>> {
        Poll::Ready(Ok(std::mem::take(&mut self.completed)))
    }
}

impl Debug for PartBuf {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("PartBuf")
            .field("pending", &self.pending)
            .field("completed", &self.completed)
            .field("capacity", &self.capacity)
            .finish()
    }
}
