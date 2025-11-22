use crate::client::part::CompletedParts;
use crate::client::request::SendUploadPart;
use crate::error::{Error as UploadError, Result};

use futures::stream::FuturesUnordered;
use futures::{Stream, ready};
use multipart_write::MultipartWrite;
use std::fmt::{self, Debug, Formatter};
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::task::{Context, Poll};

/// Utility `MultipartWrite` for buffering upload request futures.
#[must_use = "futures do nothing unless polled"]
#[pin_project::pin_project]
pub struct PartBuffer {
    #[pin]
    pending: FuturesUnordered<SendUploadPart>,
    completed: CompletedParts,
    capacity: Option<NonZeroUsize>,
}

impl PartBuffer {
    pub(crate) fn new(capacity: Option<usize>) -> Self {
        Self {
            pending: FuturesUnordered::new(),
            completed: CompletedParts::default(),
            capacity: capacity.and_then(NonZeroUsize::new),
        }
    }
}

impl MultipartWrite<SendUploadPart> for PartBuffer {
    type Ret = ();
    type Output = CompletedParts;
    type Error = UploadError;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        let mut this = self.project();
        // Poke the pending uploads to see if any are ready.
        while let Poll::Ready(Some(res)) = this.pending.as_mut().poll_next(cx) {
            match res {
                Ok(v) => {
                    trace!(
                        id = %v.id,
                        etag = %v.etag,
                        part = ?v.part_number,
                        size = v.part_size,
                        "completed part",
                    );
                    this.completed.push(v)
                }
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
        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        let mut this = self.project();
        while !this.pending.is_empty() {
            match ready!(this.pending.as_mut().poll_next(cx)) {
                Some(Ok(v)) => {
                    trace!(
                        id = %v.id,
                        etag = %v.etag,
                        part = ?v.part_number,
                        size = v.part_size,
                        "flushed completed part",
                    );
                    this.completed.push(v);
                }
                Some(Err(e)) => return Poll::Ready(Err(e)),
                // The stream stopped producing, i.e., the collection is empty.
                _ => break,
            }
        }
        Poll::Ready(Ok(()))
    }

    fn poll_complete(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<Self::Output>> {
        ready!(self.as_mut().poll_flush(cx))?;
        Poll::Ready(Ok(std::mem::take(&mut self.completed)))
    }
}

impl Debug for PartBuffer {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("PartBuffer")
            .field("pending", &self.pending)
            .field("completed", &self.completed)
            .field("capacity", &self.capacity)
            .finish()
    }
}
