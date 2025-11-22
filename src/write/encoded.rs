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
        self.part_bytes += bytes as u64;
        self.total_items += 1;
    }

    fn update_sent(&mut self, sent: UploadSent) {
        self.id = Some(sent.id);
        self.part = Some(sent.part);
        self.part_bytes = 0;
        self.total_bytes += sent.bytes;
        self.total_parts += 1;
    }
}

/// A type for creating, building, and completing a multipart upload.
///
/// `EncodedUpload` is comprised of a [`PartEncoder`] and a multipart writer
/// accepting the output of the encoder, [`PartBody`].  As an implementor of
/// [`MultipartWrite`], it writes an an arbitrary `Item` type that the encoding
/// is capable of writing, returning the current [`Status`] of the upload.
///
/// Behind the scenes, the encoder writes items until it is of sufficient size,
/// at which point the encoder is converted into a part upload request body and
/// the request is sent to a buffer of such pending request futures.  Flushing
/// drains this buffer and polling for completion finishes the upload from all of
/// the parts that were sent and finished.
///
/// Being a `MultipartWrite`, the extension trait [`MultipartWriteExt`] provides
/// many useful combinators available on `EncodedUpload`.  See also the
/// extension [`MultipartStreamExt`], which has helpful methods for using the
/// multipart uploader in a streaming context.
///
/// [`MultipartWrite`]: multipart_write::MultipartWrite
/// [`MultipartWriteExt`]: multipart_write::MultipartWriteExt
/// [`MultipartStreamExt`]: multipart_write::MultipartStreamExt
#[pin_project::pin_project]
pub struct EncodedUpload<Item, E: PartEncoder<Item>, U> {
    #[pin]
    uploader: U,
    encoder: E,
    builder: E::Builder,
    max_bytes: u64,
    max_part_bytes: u64,
    start: Instant,
    state: UploadState,
    empty: bool,
    _it: std::marker::PhantomData<Item>,
}

impl<Item, E: PartEncoder<Item>, U> EncodedUpload<Item, E, U> {
    pub(crate) fn new(uploader: U, builder: E::Builder, bytes: u64, part_bytes: u64) -> Self {
        // `part_bytes` is bounded by usize::MAX.
        let capacity = part_bytes as usize;
        let encoder = E::build(&builder, capacity).expect("failed to build encoder");

        Self {
            uploader,
            encoder,
            builder,
            max_bytes: bytes,
            max_part_bytes: part_bytes,
            start: Instant::now(),
            state: UploadState::default(),
            empty: true,
            _it: std::marker::PhantomData,
        }
    }

    fn poll_send_body(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>>
    where
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
                let capacity = *this.max_part_bytes as usize;
                let new_encoder = E::reset(this.builder, capacity)?;
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

impl<Item, E: PartEncoder<Item>, U> FusedMultipartWrite<Item> for EncodedUpload<Item, E, U>
where
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

impl<Item, E: PartEncoder<Item>, U> MultipartWrite<Item> for EncodedUpload<Item, E, U>
where
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
        let capacity = *this.max_part_bytes as usize;
        let new_encoder = E::build(this.builder, capacity)?;
        *this.encoder = new_encoder;
        Poll::Ready(Ok(out))
    }
}

impl<Item, E: PartEncoder<Item>, U> Debug for EncodedUpload<Item, E, U>
where
    E: Debug,
    E::Builder: Debug,
    U: Debug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("EncodedUpload")
            .field("uploader", &self.uploader)
            .field("encoder", &self.encoder)
            .field("builder", &self.builder)
            .field("max_bytes", &self.max_bytes)
            .field("max_part_bytes", &self.max_part_bytes)
            .field("start", &self.start)
            .field("state", &self.state)
            .field("empty", &self.empty)
            .finish()
    }
}
