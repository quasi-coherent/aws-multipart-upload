use super::UploadSent;
use crate::client::UploadId;
use crate::client::part::{PartBody, PartNumber};
use crate::codec::PartEncoder;
use crate::error::{Error as UploadError, Result};
use crate::request::CompletedUpload;

use futures::ready;
use multipart_write::{FusedMultipartWrite, MultipartWrite};
use std::fmt::{self, Debug, Formatter};
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

/// Value returned by the `EncodedUpload` writer.
#[derive(Debug, Clone)]
pub struct Status {
    /// The ID of the upload being written to.
    pub id: Option<UploadId>,
    /// The part number of the last sent.
    pub part: Option<PartNumber>,
    /// Total uptime of the upload.
    pub elapsed: Duration,
    /// Current count of items written to the upload.
    pub items: u64,
    /// Current number of parts in the upload.
    pub parts: u64,
    /// Current size in bytes of the upload.
    pub bytes: u64,
    /// Whether the upload should be completed according to configuration.
    pub should_complete: bool,
    /// Current size in bytes of the part.
    pub part_bytes: u64,
    /// Whether the part should be uploaded according to configuration.
    pub should_upload: bool,
}

/// Tracking size of the upload/part.
#[derive(Debug, Clone, Default)]
struct UploadState {
    id: Option<UploadId>,
    part: Option<PartNumber>,
    part_bytes: u64,
    total_bytes: u64,
    total_items: u64,
    total_parts: u64,
}

impl UploadState {
    fn to_status(&self, max_bytes: u64, max_part_bytes: u64, start: Instant) -> Status {
        Status {
            id: self.id.clone(),
            part: self.part,
            elapsed: start.elapsed(),
            items: self.total_items,
            bytes: self.total_bytes,
            should_complete: self.total_bytes >= max_bytes,
            parts: self.total_parts,
            part_bytes: self.part_bytes,
            should_upload: self.part_bytes >= max_part_bytes,
        }
    }

    fn update_encode(&mut self, bytes: usize) {
        let n = bytes as u64;
        self.total_bytes += n;
        self.part_bytes += n;
        self.total_items += 1;
    }

    fn update_sent(&mut self, sent: UploadSent) {
        self.id = Some(sent.id);
        self.part = Some(sent.part);
        self.part_bytes = 0;
        self.total_parts += 1;
    }
}

/// A type for creating, building, and completing a multipart upload.
///
/// This composes a [`PartEncoder`] in front of a multipart upload in order to
/// build the part upload request body from an arbitrary `Item`.  Parts are
/// uploaded according the target part size this value is configured with.
///
/// This writer itself is reusable, i.e., one can continue writing `Item`s after
/// completing an upload, if and only if `U` is.
///
/// [`PartEncoder`]: crate::codec::PartEncoder
#[must_use = "futures do nothing unless polled"]
#[pin_project::pin_project]
pub struct EncodedUpload<E, U> {
    #[pin]
    uploader: U,
    encoder: E,
    max_bytes: u64,
    max_part_bytes: u64,
    start: Instant,
    state: UploadState,
    empty: bool,
}

impl<E, U> EncodedUpload<E, U> {
    pub(crate) fn new(uploader: U, encoder: E, bytes: u64, part_bytes: u64) -> Self {
        Self {
            uploader,
            encoder,
            max_bytes: bytes,
            max_part_bytes: part_bytes,
            start: Instant::now(),
            state: UploadState::default(),
            empty: true,
        }
    }

    fn poll_send_body<Item>(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>>
    where
        E: PartEncoder<Item>,
        U: MultipartWrite<
                PartBody,
                Ret = UploadSent,
                Error = UploadError,
                Output = CompletedUpload,
            >,
    {
        let mut this = self.project();

        match this.uploader.as_mut().poll_ready(cx)? {
            Poll::Ready(()) => {
                this.encoder.flush()?;
                let new_encoder = this.encoder.clear()?;
                let encoder = std::mem::replace(this.encoder, new_encoder);
                let body = encoder.into_body()?;
                let ret = this.uploader.as_mut().start_send(body)?;
                this.state.update_sent(ret);
                *this.empty = true;

                Poll::Ready(Ok(()))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<Item, E, U> FusedMultipartWrite<Item> for EncodedUpload<E, U>
where
    E: PartEncoder<Item>,
    U: FusedMultipartWrite<
            PartBody,
            Ret = UploadSent,
            Error = UploadError,
            Output = CompletedUpload,
        >,
{
    fn is_terminated(&self) -> bool {
        self.uploader.is_terminated()
    }
}

impl<Item, E, U> MultipartWrite<Item> for EncodedUpload<E, U>
where
    E: PartEncoder<Item>,
    U: MultipartWrite<PartBody, Ret = UploadSent, Error = UploadError, Output = CompletedUpload>,
{
    type Ret = Status;
    type Error = UploadError;
    type Output = CompletedUpload;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        if self.state.part_bytes >= self.max_part_bytes {
            ready!(self.as_mut().poll_send_body(cx))?;
        }
        Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, part: Item) -> Result<Self::Ret> {
        let this = self.project();
        let bytes = this.encoder.encode(part)?;
        this.state.update_encode(bytes);
        *this.empty = false;
        let status = this
            .state
            .to_status(*this.max_bytes, *this.max_part_bytes, *this.start);
        Ok(status)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        if !self.empty {
            ready!(self.as_mut().poll_send_body(cx))?;
        }
        ready!(self.project().uploader.poll_flush(cx))?;
        Poll::Ready(Ok(()))
    }

    fn poll_complete(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<Self::Output>> {
        if !self.empty {
            ready!(self.as_mut().poll_send_body(cx))?;
        }
        let mut this = self.project();
        let out = ready!(this.uploader.as_mut().poll_complete(cx))?;
        let new_encoder = this.encoder.restore()?;
        *this.encoder = new_encoder;
        *this.state = UploadState::default();
        *this.start = Instant::now();
        Poll::Ready(Ok(out))
    }
}

impl<E, U> Debug for EncodedUpload<E, U>
where
    E: Debug,
    U: Debug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("EncodedUpload")
            .field("uploader", &self.uploader)
            .field("encoder", &self.encoder)
            .field("max_bytes", &self.max_bytes)
            .field("max_part_bytes", &self.max_part_bytes)
            .field("start", &self.start)
            .field("state", &self.state)
            .field("empty", &self.empty)
            .finish()
    }
}
