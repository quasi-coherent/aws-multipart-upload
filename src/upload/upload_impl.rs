use std::fmt::{self, Debug, Formatter};
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use futures::ready;
use multipart_write::{FusedMultipartWrite, MultipartWrite};

use super::part_buffer::{PartBuffer, TotalRx};
use crate::client::part::{CompletedParts, PartBody, PartNumber};
use crate::client::request::*;
use crate::client::{UploadClient, UploadData};
use crate::codec::PartEncoder;
use crate::error::{Error as UploadError, ErrorWithUpload as _, Result};
use crate::uri::{ObjectUri, ObjectUriIter};

/// User-defined constraints for when a part or upload should finish.
#[derive(Debug, Clone, Copy)]
pub(super) struct UploadParams {
    max_bytes: u64,
    max_part_bytes: u64,
    capacity: Option<usize>,
}

impl UploadParams {
    pub(super) fn new(
        max_bytes: u64,
        max_part_bytes: u64,
        capacity: Option<usize>,
    ) -> Self {
        Self { max_bytes, max_part_bytes, capacity }
    }
}

/// Tracking state of the uploaded parts of an in-progress upload.
#[derive(Debug, Clone, Copy)]
pub(super) struct UploadState {
    pub(super) duration: Duration,
    pub(super) total_bytes: u64,
    pub(super) part_bytes: u64,
    pub(super) upload_bytes: u64,
    pub(super) total_items: u64,
    pub(super) part_items: u64,
    pub(super) total_parts: u64,
    pub(super) upload_parts: u64,
    pub(super) max_bytes: u64,
    pub(super) max_part_bytes: u64,
}

impl UploadState {
    fn new(params: UploadParams) -> Self {
        Self {
            duration: Duration::default(),
            total_bytes: 0,
            part_bytes: 0,
            upload_bytes: 0,
            total_items: 0,
            part_items: 0,
            total_parts: 0,
            upload_parts: 0,
            max_bytes: params.max_bytes,
            max_part_bytes: params.max_part_bytes,
        }
    }

    fn update_encode(&mut self, item_bytes: usize, start: Instant) {
        self.duration = start.elapsed();
        // Number of bytes written can't exceed AWS_MAX_PART_SIZE < u64::MAX.
        let n = item_bytes as u64;
        self.total_bytes += n;
        self.part_bytes += n;
        self.total_items += 1;
        self.part_items += 1;
    }

    fn update_sent(&mut self, rx: TotalRx, start: Instant) {
        self.duration = start.elapsed();
        self.total_parts += 1;
        // Number of bytes written cannot exceed AWS_MAX_OBJECT_SIZE < u64::MAX.
        self.upload_bytes = rx.upload_bytes as u64;
        self.upload_parts = rx.upload_parts as u64;
        // Reset current part.
        self.part_bytes = 0;
        self.part_items = 0;
    }

    pub(super) fn is_part_complete(&self) -> bool {
        self.part_bytes >= self.max_part_bytes
    }

    pub(super) fn is_upload_complete(&self) -> bool {
        self.total_bytes >= self.max_bytes
    }

    fn is_empty_part(&self) -> bool {
        self.part_items == 0
    }
}

/// `poll_complete` output for `UploadImpl` adds some statistics to the
/// response.
#[derive(Debug, Clone)]
pub(super) struct UploadOutput {
    pub(super) completed: CompletedUpload,
    pub(super) items: u64,
    pub(super) bytes: u64,
    pub(super) parts: u64,
    pub(super) duration: Duration,
}

impl UploadOutput {
    fn new(completed: CompletedUpload, state: UploadState) -> Self {
        Self {
            completed,
            items: state.total_items,
            bytes: state.total_bytes,
            parts: state.total_parts,
            duration: state.duration,
        }
    }
}

/// Implements `MultipartWrite` as an S3 multipart upload.
#[derive(Debug)]
#[must_use = "futures do nothing unless polled"]
#[pin_project::pin_project]
pub(super) struct UploadImpl<E> {
    #[pin]
    inner: UploadInner,
    encoder: E,
    client: UploadClient,
    iter: ObjectUriIter,
    data: Option<UploadData>,
    state: UploadState,
    params: UploadParams,
    start: Instant,
}

impl<E> UploadImpl<E> {
    pub(super) fn new(
        client: UploadClient,
        encoder: E,
        mut iter: ObjectUriIter,
        params: UploadParams,
    ) -> Self {
        let uri = iter.next();
        let inner = UploadInner::new_upload_maybe(&client, uri);
        let state = UploadState::new(params);

        Self {
            inner,
            encoder,
            client,
            iter,
            data: None,
            state,
            params,
            start: Instant::now(),
        }
    }
}

impl<Item, E: PartEncoder<Item>> FusedMultipartWrite<Item> for UploadImpl<E> {
    fn is_terminated(&self) -> bool {
        self.inner.is_terminated()
    }
}

impl<Item, E: PartEncoder<Item>> MultipartWrite<Item> for UploadImpl<E> {
    type Error = UploadError;
    type Output = UploadOutput;
    type Recv = UploadState;

    fn poll_ready(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<()>> {
        let mut this = self.as_mut().project();

        match this.inner.as_mut().project() {
            // Don't start writing parts if there's no upload to send it to.
            UploadProj::Pending(fut) => {
                let data = ready!(fut.poll(cx))?;
                let new_inner = UploadInner::active(
                    this.client,
                    &data,
                    this.params.capacity,
                );
                let new_encoder = this.encoder.new_upload()?;
                *this.encoder = new_encoder;
                this.inner.set(new_inner);
                *this.start = Instant::now();
                *this.data = Some(data);
                Poll::Ready(Ok(()))
            },
            UploadProj::Active(mut upl) if this.state.is_part_complete() => {
                ready!(upl.as_mut().poll_buffer_ready(cx))?;
                this.encoder.flush()?;
                let new_encoder = this.encoder.new_part()?;
                let encoder = std::mem::replace(this.encoder, new_encoder);
                let body = encoder.into_body()?;
                let recv = upl.send_buffer(body)?;
                this.state.update_sent(recv, *this.start);
                Poll::Ready(Ok(()))
            },
            UploadProj::Active(_) => Poll::Ready(Ok(())),
            UploadProj::Terminated => Poll::Ready(Err(UploadError::state(
                "polled Upload after completion",
            ))),
        }
    }

    fn start_send(self: Pin<&mut Self>, part: Item) -> Result<Self::Recv> {
        let this = self.project();
        let item_bytes = this.encoder.encode(part)?;
        this.state.update_encode(item_bytes, *this.start);
        Ok(*this.state)
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<()>> {
        let this = self.as_mut().project();
        // If the current part has been written to, we do _not_ "flush" it to a
        // `PartBody` and send, which is perhaps reasonably not how one might
        // think `poll_flush` would work in this case.
        //
        // However, the AWS API specifies that a part upload request meet a
        // minimum part size if it is not the _last_ part. And `poll_flush` may
        // be called at any time--it need not be during building the last part.
        // So sending a partially-written encoder's part will cause problems.
        //
        // We could instead check here if the current part is at least the
        // minimum part size and send it if so, but `poll_ready` does the whole
        // job.
        let upl = this.inner.get_active_proj().expect("polled inactive Upload");
        ready!(upl.poll_buffer_flush(cx))?;
        // Put this after `upl.poll_buffer_flush` so it's only called once.
        this.encoder.flush()?;
        Poll::Ready(Ok(()))
    }

    fn poll_complete(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Self::Output>> {
        let mut this = self.as_mut().project();
        let mut upl = this
            .inner
            .as_mut()
            .get_active_proj()
            .expect("polled inactive Upload");
        // Unlike in `poll_flush`, we DO send whatever is in a partially-written
        // encoder because it is the last part, which may be any size.
        if !this.state.is_empty_part() {
            ready!(upl.as_mut().poll_buffer_ready(cx))?;
            this.encoder.flush()?;
            let new_encoder = this.encoder.new_upload()?;
            let encoder = std::mem::replace(this.encoder, new_encoder);
            let body = encoder.into_body()?;
            let recv = upl.as_mut().send_buffer(body)?;
            this.state.update_sent(recv, *this.start);
        }
        // Do not `?` here: we have to exit this method with the inner state set
        // to `Pending` (or `Terminated` if there is no next upload).
        let output = ready!(upl.poll_complete_upload(cx))
            .map(|out| UploadOutput::new(out, *this.state));

        let inner =
            UploadInner::new_upload_maybe(this.client, this.iter.next());
        *this.state = UploadState::new(*this.params);
        *this.data = None;
        this.inner.set(inner);
        Poll::Ready(output)
    }
}

#[derive(Debug)]
#[must_use = "futures do nothing unless polled"]
#[pin_project::pin_project(project = UploadProj)]
enum UploadInner {
    Active(#[pin] ActiveUpload),
    Pending(#[pin] SendCreateUpload),
    Terminated,
}

impl UploadInner {
    fn active(
        client: &UploadClient,
        data: &UploadData,
        capacity: Option<usize>,
    ) -> Self {
        Self::Active(ActiveUpload::new(client, data, capacity))
    }

    /// Try to create a new upload, but return `Terminated` if the URI provided
    /// is `None`.
    fn new_upload_maybe(client: &UploadClient, uri: Option<ObjectUri>) -> Self {
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

    fn get_active_proj(self: Pin<&mut Self>) -> Option<Pin<&mut ActiveUpload>> {
        let UploadProj::Active(upl) = self.project() else {
            return None;
        };
        Some(upl)
    }
}

#[must_use = "futures do nothing unless polled"]
#[pin_project::pin_project]
struct ActiveUpload {
    #[pin]
    buf: PartBuffer,
    #[pin]
    fut: Option<SendCompleteUpload>,
    client: UploadClient,
    data: UploadData,
    parts: CompletedParts,
    current: PartNumber,
}

impl ActiveUpload {
    fn new(
        client: &UploadClient,
        data: &UploadData,
        capacity: Option<usize>,
    ) -> Self {
        Self {
            buf: PartBuffer::new(capacity),
            fut: None,
            client: client.clone(),
            data: data.clone(),
            parts: CompletedParts::default(),
            current: PartNumber::new(),
        }
    }

    fn poll_buffer_ready(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<()>> {
        let this = self.project();
        // Prevent sending if this future exists, since the upload ID is no
        // longer valid.
        if this.fut.is_some() {
            return Poll::Pending;
        }
        ready!(this.buf.poll_ready(cx)).err_with_upl(
            &this.data.id,
            &this.data.uri,
            this.parts,
        )?;
        Poll::Ready(Ok(()))
    }

    fn send_buffer(self: Pin<&mut Self>, body: PartBody) -> Result<TotalRx> {
        let mut this = self.project();
        // Increments the part number while returning the current value.
        let part_num = this.current.fetch_incr();
        let _bytes = body.size();
        let req = UploadPartRequest::new(this.data, body, part_num);
        let fut = SendUploadPart::new(this.client, req);
        let recv = this.buf.as_mut().start_send(fut)?;
        trace!(
            id = %&this.data.id,
            uri = %&this.data.uri,
            part_num = %part_num,
            part_bytes = _bytes,
            upload_bytes = recv.upload_bytes,
            upload_parts = recv.upload_parts,
            "sent part upload",
        );
        Ok(recv)
    }

    fn poll_buffer_flush(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<()>> {
        let this = self.project();
        let completed = ready!(this.buf.poll_complete(cx)).err_with_upl(
            &this.data.id,
            &this.data.uri,
            this.parts,
        )?;
        this.parts.append(completed);
        Poll::Ready(Ok(()))
    }

    fn poll_complete_upload(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<CompletedUpload>> {
        let mut this = self.project();
        if this.fut.is_none() {
            let completed = ready!(this.buf.as_mut().poll_complete(cx))
                .err_with_upl(&this.data.id, &this.data.uri, this.parts)?;
            this.parts.append(completed);
            let req = CompleteRequest::new(this.data, this.parts);
            trace!(
                id = %req.id(),
                uri = %req.uri(),
                parts = ?req.completed_parts(),
                "completing upload",
            );
            let fut = SendCompleteUpload::new(this.client, req);
            this.fut.set(Some(fut));
        }
        let fut = this.fut.as_mut().as_pin_mut().unwrap();
        let output = ready!(fut.poll(cx)).err_with_upl(
            &this.data.id,
            &this.data.uri,
            this.parts,
        );
        // This method returns the output with `UploadInner` in an invalid
        // state, since the upload ID is for an upload that completed.
        //
        // But this is only called in `Upload::poll_complete` where the inner
        // state transitions to a `SendCreateUpload` future with no await point
        // in between, which ensures the upload remains valid and can be polled
        // during the complete -> create upload phase.
        this.fut.set(None);
        Poll::Ready(output)
    }
}

impl Debug for ActiveUpload {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("ActiveUpload")
            .field("buf", &self.buf)
            .field("client", &self.client)
            .field("data", &self.data)
            .field("parts", &self.parts)
            .field("current", &self.current)
            .finish()
    }
}
