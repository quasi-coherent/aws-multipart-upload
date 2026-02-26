use std::fmt::{self, Debug, Formatter};
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::ready;
use futures::stream::{FusedStream, FuturesUnordered, Stream};
use multipart_write::{FusedMultipartWrite, MultipartWrite};

use crate::client::part::CompletedParts;
use crate::client::request::SendUploadPart;
use crate::error::{Error as UploadError, Result};

#[derive(Debug, Clone, Copy, Default)]
pub(super) struct TotalRx {
    pub(super) upload_bytes: usize,
    pub(super) upload_parts: usize,
}

impl TotalRx {
    fn new(parts: &CompletedParts) -> Self {
        Self { upload_bytes: parts.size(), upload_parts: parts.count() }
    }
}

/// Buffer for concurrently building multipart uploads.
#[must_use = "futures do nothing unless polled"]
#[pin_project::pin_project]
pub(super) struct PartBuffer {
    #[pin]
    pending: FuturesUnordered<SendUploadPart>,
    parts: CompletedParts,
    capacity: Option<NonZeroUsize>,
    flushing: bool,
}

impl PartBuffer {
    pub(super) fn new(capacity: Option<usize>) -> Self {
        Self {
            pending: FuturesUnordered::new(),
            parts: CompletedParts::default(),
            capacity: capacity.and_then(NonZeroUsize::new),
            flushing: false,
        }
    }
}

impl FusedMultipartWrite<SendUploadPart> for PartBuffer {
    fn is_terminated(&self) -> bool {
        self.pending.is_terminated()
    }
}

impl MultipartWrite<SendUploadPart> for PartBuffer {
    type Error = UploadError;
    type Output = CompletedParts;
    type Recv = TotalRx;

    fn poll_ready(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<()>> {
        let mut this = self.project();
        // Flushing means completely emptying the pending buffer, so we don't
        // want to allow any way to modify the buffer while that is in progress.
        if *this.flushing {
            return Poll::Pending;
        }
        // Poke the pending uploads to see if any are ready.
        while let Poll::Ready(Some(res)) = this.pending.as_mut().poll_next(cx) {
            match res {
                Ok(v) => {
                    trace!(
                        etag = %v.etag,
                        part = ?v.part_number,
                        size = v.part_size,
                        "completed part",
                    );
                    this.parts.insert(v);
                },
                Err(e) => return Poll::Ready(Err(e)),
            }
        }
        if this.capacity.is_none_or(|n| this.pending.len() < n.get()) {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }

    fn start_send(
        self: Pin<&mut Self>,
        part: SendUploadPart,
    ) -> Result<Self::Recv> {
        let this = self.project();
        this.pending.push(part);
        Ok(TotalRx::new(this.parts))
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<()>> {
        let mut this = self.project();
        *this.flushing = true;
        while !this.pending.is_empty() {
            match ready!(this.pending.as_mut().poll_next(cx)) {
                Some(Ok(v)) => {
                    trace!(
                        etag = %v.etag,
                        part = ?v.part_number,
                        size = v.part_size,
                        "completed part",
                    );
                    this.parts.insert(v);
                },
                Some(Err(e)) => return Poll::Ready(Err(e)),
                // The stream stopped producing, i.e., the collection is empty.
                _ => break,
            }
        }
        *this.flushing = false;
        Poll::Ready(Ok(()))
    }

    fn poll_complete(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Self::Output, Self::Error>> {
        ready!(self.as_mut().poll_flush(cx))?;
        // Replace completed parts with the default empty collection.
        Poll::Ready(Ok(std::mem::take(self.project().parts)))
    }
}

impl Debug for PartBuffer {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("PartBuffer")
            .field("pending", &self.pending)
            .field("parts", &self.parts)
            .field("capacity", &self.capacity)
            .field("flushing", &self.flushing)
            .finish()
    }
}
