use std::fmt::{self, Debug, Formatter};
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use bytesize::ByteSize;
use futures::ready;
use multipart_write::{FusedMultipartWrite, MultipartWrite};

use super::part_buffer::PartBuffer;
use super::{ShouldComplete, Uploaded};
use crate::client::part::{CompletedParts, PartBody, PartNumber};
use crate::client::request::*;
use crate::client::{UploadClient, UploadData};
use crate::error::{Error, ErrorWithUpload as _};
use crate::uri::{ObjectUri, ObjectUriIter};

/// Returned by `Upload` when sending a part was successful.
#[derive(Debug, Clone, Copy)]
pub struct UploadStatus {
    /// Last recorded size in bytes of all parts that have been added to the
    /// upload successfully.
    pub upload_bytes: u64,
    /// Total size in bytes of all parts that have been sent.
    pub total_bytes: u64,
    /// Total number of parts that have been sent.
    pub total_parts: u64,
    /// Total duration of this upload.
    pub duration: Duration,
    /// Whether the current upload should be completed.
    pub should_complete: bool,
}

impl ShouldComplete for UploadStatus {
    fn should_complete(&self) -> bool {
        self.should_complete
    }
}

/// `MultipartUpload` is a multipart writer that also manages state transition
/// of `UploadInner`.
#[derive(Debug)]
#[must_use = "futures do nothing unless polled"]
#[pin_project::pin_project]
pub(crate) struct MultipartUpload {
    #[pin]
    inner: UploadInner,
    #[pin]
    fut: Option<SendCompleteUpload>,
    client: UploadClient,
    iter: ObjectUriIter,
    state: GlobalState,
    config: GlobalConfig,
}

impl MultipartUpload {
    pub(crate) fn new(
        client: UploadClient,
        mut iter: ObjectUriIter,
        bytes: ByteSize,
        capacity: Option<usize>,
    ) -> Self {
        let max_bytes = bytes.as_u64();
        let config = GlobalConfig { max_bytes, capacity };
        let inner = UploadInner::create_upload_maybe(&client, iter.next());
        let state = GlobalState::new(config);
        Self { inner, fut: None, client, iter, state, config }
    }
}

impl FusedMultipartWrite<PartBody> for MultipartUpload {
    fn is_terminated(&self) -> bool {
        self.inner.is_terminated()
    }
}

impl MultipartWrite<PartBody> for MultipartUpload {
    type Error = Error;
    type Output = Uploaded;
    type Recv = UploadStatus;

    fn poll_ready(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        let mut this = self.as_mut().project();
        match this.inner.as_mut().project() {
            // Can't send parts if there is no upload.
            UploadProj::Pending(fut) => {
                let data = ready!(fut.poll(cx))?;
                trace!(
                    id = %&data.id,
                    uri = %&data.uri,
                    "new upload created",
                );
                let new_inner = UploadInner::active(
                    this.client,
                    data,
                    this.config.capacity,
                );
                *this.state = GlobalState::new(*this.config);
                this.inner.set(new_inner);
                Poll::Ready(Ok(()))
            },
            UploadProj::Active(upl) => upl.poll_ready(cx),
            UploadProj::Terminated => {
                Poll::Ready(Err(Error::state("polled upload after completion")))
            },
        }
    }

    fn start_send(
        mut self: Pin<&mut Self>,
        part: PartBody,
    ) -> Result<Self::Recv, Self::Error> {
        let this = self.as_mut().project();
        let upl =
            this.inner.get_active_proj().expect("called send with no upload");
        let recv = upl.start_send(part)?;
        Ok(this.state.upload_status(recv))
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        let this = self.as_mut().project();
        // It should not be the case that `poll_flush` is called without an
        // active upload. We've finished an upload in this case and are
        // waiting for the next one, so flushing when there hasn't been
        // anything written doesn't make sense.
        //
        // I's not against the law though, and it makes sense to return
        // immediately if there's nothing to flush, so that's what we
        // do.
        let Some(upl) = this.inner.get_active_proj() else {
            return Poll::Ready(Ok(()));
        };
        upl.poll_flush(cx)
    }

    fn poll_complete(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Self::Output, Self::Error>> {
        let mut this = self.as_mut().project();
        // First poll drains active requests and sets the CompleteUpload future.
        if this.fut.is_none() {
            let upl = this
                .inner
                .as_mut()
                .get_active_proj()
                .expect("called complete with no upload");
            let out = ready!(upl.poll_complete(cx))?;
            trace!(
                id = %&out.data.id,
                uri = %&out.data.uri,
                bytes = out.parts.size(),
                parts = out.parts.count(),
                "starting complete upload",
            );
            let req = CompleteRequest::new(&out.data, out.parts);
            let fut = SendCompleteUpload::new(this.client, req);
            this.fut.set(Some(fut));
        }
        let fut = this.fut.as_mut().as_pin_mut().unwrap();
        let resp = ready!(fut.poll(cx))?;
        let output = this.state.uploaded(resp);
        // Now transition `UploadInner` to be `Pending` a new upload.
        //
        // If there isn't another URI to get, `Terminated` is the state instead.
        // Either way this value should have a valid state when the method
        // returns, just if it's `Terminated` the fuse behavior will mean it's
        // not polled again.
        let next = this.iter.next();
        trace!(
            uri = %&output.uri,
            etag = %&output.etag,
            terminated = next.is_none(),
            "completed upload",
        );
        let new_inner = UploadInner::create_upload_maybe(this.client, next);
        this.fut.set(None);
        this.inner.set(new_inner);
        Poll::Ready(Ok(output))
    }
}

#[derive(Debug)]
#[must_use = "futures do nothing unless polled"]
#[pin_project::pin_project(project = UploadProj)]
enum UploadInner {
    Active(#[pin] UploadParts),
    Pending(#[pin] SendCreateUpload),
    Terminated,
}

impl UploadInner {
    fn active(
        client: &UploadClient,
        data: UploadData,
        capacity: Option<usize>,
    ) -> Self {
        Self::Active(UploadParts::new(client, data, capacity))
    }

    /// Try to create a new upload, but return `Terminated` if the URI provided
    /// is `None`.
    fn create_upload_maybe(
        client: &UploadClient,
        uri: Option<ObjectUri>,
    ) -> Self {
        let Some(uri) = uri else {
            return Self::Terminated;
        };
        let req = CreateRequest::new(uri);
        let fut = SendCreateUpload::new(client, req);
        Self::Pending(fut)
    }

    fn is_terminated(&self) -> bool {
        matches!(self, Self::Terminated)
    }

    fn get_active_proj(self: Pin<&mut Self>) -> Option<Pin<&mut UploadParts>> {
        let UploadProj::Active(upl) = self.project() else {
            return None;
        };
        Some(upl)
    }
}

/// The main state of `Upload` manages polling the buffer of request futures.
#[must_use = "futures do nothing unless polled"]
#[pin_project::pin_project]
struct UploadParts {
    #[pin]
    buf: PartBuffer,
    client: UploadClient,
    data: UploadData,
    parts: CompletedParts,
    current: PartNumber,
    is_terminated: bool,
}

impl UploadParts {
    fn new(
        client: &UploadClient,
        data: UploadData,
        capacity: Option<usize>,
    ) -> Self {
        Self {
            buf: PartBuffer::new(capacity),
            client: client.clone(),
            data,
            parts: CompletedParts::default(),
            current: PartNumber::new(),
            is_terminated: false,
        }
    }
}

impl FusedMultipartWrite<PartBody> for UploadParts {
    fn is_terminated(&self) -> bool {
        self.buf.is_terminated() || self.is_terminated
    }
}

impl MultipartWrite<PartBody> for UploadParts {
    type Error = Error;
    type Output = PartBufOutput;
    type Recv = PartBufRecv;

    fn poll_ready(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        let this = self.project();
        ready!(this.buf.poll_ready(cx)).err_with_upl(
            &this.data.id,
            &this.data.uri,
            this.parts,
        )?;
        Poll::Ready(Ok(()))
    }

    fn start_send(
        self: Pin<&mut Self>,
        body: PartBody,
    ) -> Result<Self::Recv, Self::Error> {
        let mut this = self.project();
        // Increments the part number while returning the current value.
        let ptnum = this.current.fetch_incr();
        let ptb = body.size();
        let req = UploadPartRequest::new(this.data, body, ptnum);
        let fut = SendUploadPart::new(this.client, req);
        let upb = this.buf.as_mut().start_send(fut)?;
        trace!(
            id = %&this.data.id,
            uri = %&this.data.uri,
            part_number = %ptnum,
            part_bytes = ptb,
            upload_bytes = upb,
            "sent part upload",
        );
        Ok(PartBufRecv::new(upb, ptb))
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        let this = self.project();
        let completed = ready!(this.buf.poll_complete(cx)).err_with_upl(
            &this.data.id,
            &this.data.uri,
            this.parts,
        )?;
        this.parts.append(completed);
        Poll::Ready(Ok(()))
    }

    fn poll_complete(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Self::Output, Self::Error>> {
        let this = self.project();
        let completed = ready!(this.buf.poll_complete(cx)).err_with_upl(
            &this.data.id,
            &this.data.uri,
            this.parts,
        )?;
        let mut parts = std::mem::take(this.parts);
        parts.append(completed);
        let data = this.data.clone();
        trace!(
            id = %&data.id,
            uri = %&data.uri,
            bytes = parts.size(),
            parts = parts.count(),
            "finished uploading parts",
        );
        // To ensure this isn't accidentally polled again.
        *this.is_terminated = true;
        Poll::Ready(Ok(PartBufOutput { parts, data }))
    }
}

impl Debug for UploadParts {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("UploadParts")
            .field("buf", &self.buf)
            .field("client", &self.client)
            .field("data", &self.data)
            .field("parts", &self.parts)
            .field("current", &self.current)
            .field("is_terminated", &self.is_terminated)
            .finish()
    }
}

/// User-defined constraints on the overall upload.
#[derive(Debug, Clone, Copy)]
struct GlobalConfig {
    max_bytes: u64,
    // Capacity for the future buffer; limit to concurrency.
    capacity: Option<usize>,
}

/// To track overall upload progress.
#[derive(Debug, Clone, Copy)]
struct GlobalState {
    total_bytes: u64,
    total_parts: u64,
    start: Instant,
    config: GlobalConfig,
}

impl GlobalState {
    fn new(config: GlobalConfig) -> Self {
        Self { total_bytes: 0, total_parts: 0, start: Instant::now(), config }
    }

    fn should_complete(&self) -> bool {
        self.total_bytes >= self.config.max_bytes
    }

    fn upload_status(&mut self, recv: PartBufRecv) -> UploadStatus {
        let part_bytes = recv.part_bytes as u64;
        self.total_bytes += part_bytes;
        self.total_parts += 1;
        UploadStatus {
            total_bytes: self.total_bytes,
            total_parts: self.total_parts,
            upload_bytes: recv.upload_bytes as u64,
            duration: self.start.elapsed(),
            should_complete: self.should_complete(),
        }
    }

    fn uploaded(&self, resp: CompletedUpload) -> Uploaded {
        Uploaded {
            uri: resp.uri,
            etag: resp.etag,
            bytes: self.total_bytes,
            parts: self.total_parts,
            items: None,
            duration: self.start.elapsed(),
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct PartBufRecv {
    upload_bytes: usize,
    part_bytes: usize,
}

impl PartBufRecv {
    fn new(upload_bytes: usize, part_bytes: usize) -> Self {
        Self { upload_bytes, part_bytes }
    }
}

#[derive(Debug, Clone)]
struct PartBufOutput {
    parts: CompletedParts,
    data: UploadData,
}
