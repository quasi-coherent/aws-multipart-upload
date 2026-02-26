use std::fmt::{self, Debug, Formatter};
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use bytesize::ByteSize;
use futures::ready;
use multipart_write::{FusedMultipartWrite, MultipartWrite};

use super::{ShouldComplete, UploadStatus, Uploaded};
use crate::client::part::{PartBody, PartNumber};
use crate::encoder::PartEncoder;
use crate::error::{Error, Result};

/// Returned by `WithPartEncoder` when writing an item was successful.
#[derive(Debug, Clone, Copy)]
pub struct EncoderStatus {
    /// Last recorded size in bytes of all parts that have been added to the
    /// upload successfully.
    pub upload_bytes: u64,
    /// Total size in bytes of all items that have been written.
    pub total_bytes: u64,
    /// Total number of parts that have been written.
    pub total_parts: u64,
    /// Total number of items that have been written.
    pub total_items: u64,
    /// Size in bytes of the current part.
    pub part_bytes: u64,
    /// Number of items written to the current part.
    pub part_items: u64,
    /// Total duration of this upload.
    pub duration: Duration,
    /// Whether the current part should be sent.
    pub should_send: bool,
    /// Whether the current upload should be completed.
    pub should_complete: bool,
}

impl ShouldComplete for EncoderStatus {
    fn should_complete(&self) -> bool {
        self.should_complete
    }
}

/// Lift a `PartEncoder` in front of a multipart upload.
#[must_use = "futures do nothing unless polled"]
#[pin_project::pin_project]
pub(crate) struct WithPartEncoder<Wr, Item, E> {
    #[pin]
    writer: Wr,
    buf: PartBody,
    encoder: E,
    state: EncoderState,
    config: EncoderConfig,
    _it: PhantomData<Item>,
}

impl<Wr, Item, E> WithPartEncoder<Wr, Item, E> {
    pub(crate) fn new(
        writer: Wr,
        encoder: E,
        bytes: ByteSize,
        part_bytes: ByteSize,
    ) -> Self {
        let max_part_bytes = part_bytes.as_u64();
        let config =
            EncoderConfig { max_bytes: bytes.as_u64(), max_part_bytes };
        let state = EncoderState::new(config);
        // Safe because the value was clamped to fit this at configuration.
        let buf = PartBody::with_capacity(max_part_bytes as usize);
        Self { writer, buf, encoder, state, config, _it: PhantomData }
    }
}

impl<Wr, Item, E> FusedMultipartWrite<Item> for WithPartEncoder<Wr, Item, E>
where
    E: PartEncoder<Item>,
    Wr: FusedMultipartWrite<
            PartBody,
            Error = Error,
            Output = Uploaded,
            Recv = UploadStatus,
        >,
{
    fn is_terminated(&self) -> bool {
        self.writer.is_terminated()
    }
}

impl<Wr, Item, E> MultipartWrite<Item> for WithPartEncoder<Wr, Item, E>
where
    E: PartEncoder<Item>,
    Wr: MultipartWrite<
            PartBody,
            Error = Error,
            Output = Uploaded,
            Recv = UploadStatus,
        >,
{
    type Error = Error;
    type Output = Uploaded;
    type Recv = EncoderStatus;

    fn poll_ready(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        let mut this = self.project();
        // This writer is always ready for parts unless the current one has
        // enough bytes.
        if this.state.should_send() {
            ready!(this.writer.as_mut().poll_ready(cx))?;
            let part = this.buf.remove();
            let recv = this.writer.start_send(part)?;
            trace!(
                upload_bytes = recv.upload_bytes,
                total_bytes = recv.total_bytes,
                total_parts = recv.total_parts,
                duration = ?recv.duration,
                "part received",
            );
            this.state.flushed(recv);
        }
        Poll::Ready(Ok(()))
    }

    fn start_send(
        self: Pin<&mut Self>,
        part: Item,
    ) -> Result<Self::Recv, Self::Error> {
        let this = self.project();
        let before = this.buf.len();
        this.encoder.encode(this.buf, this.state.part_number, part)?;
        let after = this.buf.len();
        Ok(this.state.encoded(after - before))
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        let mut this = self.project();
        if this.state.should_send() {
            ready!(this.writer.as_mut().poll_ready(cx))?;
            let part = this.buf.split().into();
            let recv = this.writer.as_mut().start_send(part)?;
            trace!(
                upload_bytes = recv.upload_bytes,
                total_bytes = recv.total_bytes,
                total_parts = recv.total_parts,
                duration = ?recv.duration,
                "part received",
            );
            this.state.flushed(recv);
        }
        this.writer.as_mut().poll_flush(cx)
    }

    fn poll_complete(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Self::Output, Self::Error>> {
        let mut this = self.project();
        if !this.state.is_empty() {
            ready!(this.writer.as_mut().poll_ready(cx))?;
            let part = this.buf.split().into();
            let recv = this.writer.as_mut().start_send(part)?;
            trace!(
                upload_bytes = recv.upload_bytes,
                total_bytes = recv.total_bytes,
                total_parts = recv.total_parts,
                duration = ?recv.duration,
                "part received",
            );
            this.state.flushed(recv);
        }
        let output = ready!(this.writer.poll_complete(cx))
            .map(|v| this.state.uploaded(v));
        *this.state = EncoderState::new(*this.config);
        Poll::Ready(output)
    }
}

impl<Wr: Debug, Item, E: Debug> Debug for WithPartEncoder<Wr, Item, E> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("WithPartEncoder")
            .field("writer", &self.writer)
            .field("buf", &self.buf)
            .field("encoder", &self.encoder)
            .field("state", &self.state)
            .field("config", &self.config)
            .finish()
    }
}

#[derive(Debug, Clone, Copy)]
struct EncoderConfig {
    max_part_bytes: u64,
    max_bytes: u64,
}

#[derive(Debug, Clone, Copy)]
struct EncoderState {
    upload_bytes: u64,
    total_bytes: u64,
    total_parts: u64,
    total_items: u64,
    part_bytes: u64,
    part_items: u64,
    part_number: PartNumber,
    start: Instant,
    config: EncoderConfig,
}

impl EncoderState {
    fn new(config: EncoderConfig) -> Self {
        Self {
            upload_bytes: 0,
            total_bytes: 0,
            total_parts: 0,
            total_items: 0,
            part_bytes: 0,
            part_items: 0,
            part_number: PartNumber::new(),
            start: Instant::now(),
            config,
        }
    }

    fn is_empty(&self) -> bool {
        self.part_items == 0
    }

    fn should_send(&self) -> bool {
        self.part_bytes >= self.config.max_part_bytes
    }

    fn should_complete(&self) -> bool {
        self.total_bytes >= self.config.max_bytes
    }

    // Creates the current value of `WithPartEncoder::Recv`.
    fn encoded(&mut self, item_bytes: usize) -> EncoderStatus {
        let item_bytes = item_bytes as u64;
        self.total_bytes += item_bytes;
        self.part_bytes += item_bytes;
        self.total_items += 1;
        self.part_items += 1;
        EncoderStatus {
            upload_bytes: self.upload_bytes,
            total_bytes: self.total_bytes,
            total_parts: self.total_parts,
            total_items: self.total_items,
            part_bytes: self.part_bytes,
            part_items: self.part_items,
            duration: self.start.elapsed(),
            should_send: self.should_send(),
            should_complete: self.should_complete(),
        }
    }

    // When adding a part use the snapshot `UploadStatus` to (re)set the global
    // counters.
    fn flushed(&mut self, status: UploadStatus) {
        self.upload_bytes = status.upload_bytes;
        self.part_bytes = 0;
        self.part_items = 0;
        // Increment the part number; ignore the current one.
        // `PartEncoder` needs the correct, current part number.
        let _ = self.part_number.fetch_incr();
    }

    fn uploaded(&self, output: Uploaded) -> Uploaded {
        Uploaded { items: Some(self.total_items), ..output }
    }
}
