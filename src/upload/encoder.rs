use super::UploadSent;
use crate::DEFAULT_MAX_PART_SIZE;
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

/// Value returned by the `WithPartEncoder` writer.
#[derive(Debug, Clone)]
pub struct Status {
    /// The ID of the upload being written to.
    pub id: Option<UploadId>,
    /// Total uptime of the upload.
    pub elapsed: Duration,
    /// Current count of items written to the upload.
    pub items: usize,
    /// Current size in bytes of the upload.
    pub bytes: usize,
    /// Current number of parts in the upload.
    pub parts: usize,
    /// The current part being written.
    pub part: PartNumber,
    /// Current size in bytes of the part.
    pub part_bytes: usize,
}

impl Default for Status {
    fn default() -> Self {
        Self {
            id: None,
            elapsed: Duration::from_millis(0),
            items: 0,
            bytes: 0,
            parts: 0,
            part: PartNumber::default(),
            part_bytes: 0,
        }
    }
}

impl Status {
    fn update(&mut self, start: Instant, bytes: usize) {
        self.items += 1;
        self.part_bytes += bytes;
        self.elapsed = start.elapsed();
    }

    fn update_sent(&mut self, sent: UploadSent) {
        self.id = Some(sent.id);
        self.part = sent.part;
        self.bytes += sent.size;
        self.parts += 1;
        self.part_bytes = 0;
    }
}

/// Add a `PartEncoder` to a multipart uploader.
#[pin_project::pin_project]
pub struct WithPartEncoder<E, B, U> {
    #[pin]
    uploader: U,
    encoder: E,
    builder: B,
    max_part_size: usize,
    start: Instant,
    status: Status,
}

impl<E, B, U> WithPartEncoder<E, B, U> {
    pub(crate) fn new<P>(uploader: U, builder: B, part_size: Option<usize>) -> Self
    where
        E: PartEncoder<P, Builder = B>,
    {
        let size = part_size.unwrap_or(DEFAULT_MAX_PART_SIZE);
        let encoder = E::build(&builder, size).expect("failed to build encoder");
        Self {
            uploader,
            encoder,
            builder,
            max_part_size: size,
            start: Instant::now(),
            status: Status::default(),
        }
    }

    fn poll_send_body<P>(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>>
    where
        U: MultipartWrite<
                PartBody,
                Ret = UploadSent,
                Output = CompletedUpload,
                Error = UploadError,
            >,
        E: PartEncoder<P, Builder = B>,
    {
        let mut this = self.project();

        match this.uploader.as_mut().poll_ready(cx)? {
            Poll::Ready(()) => {
                this.encoder.flush()?;
                let new_encoder = E::reset(this.builder, *this.max_part_size)?;
                let encoder = std::mem::replace(this.encoder, new_encoder);
                let body = encoder.into_body()?;
                let ret = this.uploader.as_mut().start_send(body)?;
                this.status.update_sent(ret);

                Poll::Ready(Ok(()))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<E, B, U, Part> FusedMultipartWrite<Part> for WithPartEncoder<E, B, U>
where
    U: FusedMultipartWrite<
            PartBody,
            Ret = UploadSent,
            Output = CompletedUpload,
            Error = UploadError,
        >,
    E: PartEncoder<Part, Builder = B>,
{
    fn is_terminated(&self) -> bool {
        self.uploader.is_terminated()
    }
}

impl<E, B, U, Part> MultipartWrite<Part> for WithPartEncoder<E, B, U>
where
    U: MultipartWrite<PartBody, Ret = UploadSent, Output = CompletedUpload, Error = UploadError>,
    E: PartEncoder<Part, Builder = B>,
{
    type Ret = Status;
    type Error = UploadError;
    type Output = CompletedUpload;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        if self.status.part_bytes >= self.max_part_size {
            ready!(self.as_mut().poll_send_body(cx))?;
        }
        Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, part: Part) -> Result<Self::Ret> {
        let this = self.project();
        let bytes = this.encoder.encode(part)?;
        this.status.update(*this.start, bytes);
        Ok(this.status.clone())
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        if self.status.part_bytes >= self.max_part_size {
            ready!(self.as_mut().poll_send_body(cx))?;
        }
        ready!(self.project().uploader.poll_flush(cx))?;
        Poll::Ready(Ok(()))
    }

    fn poll_complete(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<Self::Output>> {
        ready!(self.as_mut().poll_send_body(cx))?;
        let mut this = self.project();
        let out = ready!(this.uploader.as_mut().poll_complete(cx))?;
        let new_encoder = E::build(this.builder, *this.max_part_size)?;
        *this.encoder = new_encoder;
        trace!(this.uploader.is_terminated(), "uploader status");
        Poll::Ready(Ok(out))
    }
}

impl<E, B, U> Debug for WithPartEncoder<E, B, U>
where
    E: Debug,
    B: Debug,
    U: Debug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("WithPartEncoder")
            .field("uploader", &self.uploader)
            .field("encoder", &self.encoder)
            .field("builder", &self.builder)
            .field("max_part_size", &self.max_part_size)
            .field("start", &self.start)
            .field("status", &self.status)
            .finish()
    }
}
