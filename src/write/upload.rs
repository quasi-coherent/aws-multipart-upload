use crate::client::part::{CompletedParts, PartBody, PartNumber};
use crate::client::request::*;
use crate::client::{UploadClient, UploadData, UploadId};
use crate::error::{Error as UploadError, Result};
use crate::uri::{ObjectUri, ObjectUriIter};

use futures::ready;
use multipart_write::{FusedMultipartWrite, MultipartWrite};
use std::fmt::{self, Debug, Formatter};
use std::pin::Pin;
use std::task::{Context, Poll};

/// Returned when a part upload request was sent.
///
/// Note this does not mean that the request was successful, only that it was
/// able to be sent.
#[derive(Debug, Clone, Default)]
pub struct UploadSent {
    /// The id of the active upload.
    pub id: UploadId,
    /// The destination URI of the active upload.
    pub uri: ObjectUri,
    /// The part number that was used in the part upload request.
    pub part: PartNumber,
    /// The size in bytes of the body of the part upload request.
    pub bytes: u64,
}

impl UploadSent {
    fn new(data: &UploadData, part: PartNumber, bytes: usize) -> Self {
        Self {
            id: data.get_id(),
            uri: data.get_uri(),
            part,
            bytes: bytes as u64,
        }
    }
}

/// A type to manage the lifecycle of a multipart upload.
///
/// This `MultipartWrite` sends part upload requests from the input [`PartBody`]
/// and completes the upload when polled for completion.
///
/// On completion, a new upload is created using the `ObjectUriIter` it was
/// configured with, which makes the writer available to continue writing parts
/// to with a new upload ID.  As long as the iterator `ObjectUriIter` can produce
/// the next upload, this writer remains active.
///
/// [`PartBody`]: crate::client::part::PartBody
/// [`CompletedUpload`]: crate::client::request::CompletedUpload
#[must_use = "futures do nothing unless polled"]
#[pin_project::pin_project]
pub struct Upload<Buf> {
    #[pin]
    inner: UploadImpl<Buf>,
    #[pin]
    fut: Option<SendCreateUpload>,
    next_uri: Option<ObjectUri>,
    iter: ObjectUriIter,
}

impl<Buf> Upload<Buf> {
    pub(crate) fn new(buf: Buf, client: &UploadClient, mut iter: ObjectUriIter) -> Self {
        let inner = UploadImpl::new(buf, client);
        let fut = iter.next_upload(client);
        Self {
            inner,
            fut,
            next_uri: None,
            iter,
        }
    }

    fn poll_new_upload(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        let mut this = self.project();

        if let Some(uri) = this.next_uri.take() {
            trace!(?uri, "starting new upload");
            let req = CreateRequest::new(uri);
            let fut = SendCreateUpload::new(&this.inner.client, req);
            this.fut.set(Some(fut));
        }

        if let Some(fut) = this.fut.as_mut().as_pin_mut() {
            match ready!(fut.poll(cx)) {
                Ok(data) => {
                    this.fut.set(None);
                    trace!(id = %data.id, uri = ?data.uri, "started new upload");
                    this.inner.as_mut().set_upload_data(data);
                }
                Err(e) => {
                    this.fut.set(None);
                    return Poll::Ready(Err(e));
                }
            }
        }

        Poll::Ready(Ok(()))
    }
}

impl<Buf> FusedMultipartWrite<PartBody> for Upload<Buf>
where
    Buf: MultipartWrite<SendUploadPart, Output = CompletedParts, Error = UploadError>,
{
    fn is_terminated(&self) -> bool {
        // If the inner upload is not active, and there is no request for a new
        // upload nor next URI to make the request, we are terminated.
        self.inner.is_terminated() && self.fut.is_none() && self.next_uri.is_none()
    }
}

impl<Buf> MultipartWrite<PartBody> for Upload<Buf>
where
    Buf: MultipartWrite<SendUploadPart, Error = UploadError, Output = CompletedParts>,
{
    type Ret = UploadSent;
    type Error = UploadError;
    type Output = CompletedUpload;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        ready!(self.as_mut().poll_new_upload(cx))?;
        self.project().inner.poll_ready(cx)
    }

    fn start_send(self: Pin<&mut Self>, part: PartBody) -> Result<Self::Ret> {
        self.project().inner.start_send(part)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        self.project().inner.poll_flush(cx)
    }

    fn poll_complete(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<Self::Output>> {
        let mut this = self.project();
        let out = ready!(this.inner.as_mut().poll_complete(cx));
        *this.next_uri = this.iter.next();

        trace!(next_uri = ?this.next_uri, "completed upload");
        Poll::Ready(out)
    }
}

impl<Buf: Debug> Debug for Upload<Buf> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("Upload")
            .field("inner", &self.inner)
            .field("fut", &self.fut)
            .field("next_uri", &self.next_uri)
            .field("iter", &self.iter)
            .finish()
    }
}

/// Responsible for a single upload, which `Upload` orchestrates.
#[must_use = "futures do nothing unless polled"]
#[pin_project::pin_project]
struct UploadImpl<Buf> {
    #[pin]
    buf: Buf,
    #[pin]
    fut: Option<SendCompleteUpload>,
    data: Option<UploadData>,
    client: UploadClient,
    completed: CompletedParts,
    part: PartNumber,
}

impl<Buf> UploadImpl<Buf> {
    fn new(buf: Buf, client: &UploadClient) -> Self {
        Self {
            buf,
            fut: None,
            data: None,
            client: client.clone(),
            completed: CompletedParts::default(),
            part: PartNumber::default(),
        }
    }

    fn set_upload_data(self: Pin<&mut Self>, data: UploadData) {
        *self.project().data = Some(data);
    }
}

impl<Buf> FusedMultipartWrite<PartBody> for UploadImpl<Buf>
where
    Buf: MultipartWrite<SendUploadPart, Output = CompletedParts, Error = UploadError>,
{
    fn is_terminated(&self) -> bool {
        self.data.is_none()
    }
}

impl<Buf> MultipartWrite<PartBody> for UploadImpl<Buf>
where
    Buf: MultipartWrite<SendUploadPart, Error = UploadError, Output = CompletedParts>,
{
    type Ret = UploadSent;
    type Error = UploadError;
    type Output = CompletedUpload;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        self.project().buf.as_mut().poll_ready(cx)
    }

    fn start_send(self: Pin<&mut Self>, part: PartBody) -> Result<Self::Ret> {
        let mut this = self.project();
        let bytes = part.size();
        let data = this.data.as_ref().expect("polled Upload after completion");
        let pt_num = this.part.increment();

        let req = UploadPartRequest::new(data, part, pt_num);
        let fut = SendUploadPart::new(this.client, req);
        let _ = this.buf.as_mut().start_send(fut)?;
        let sent = UploadSent::new(data, pt_num, bytes);
        trace!(
            id = %sent.id,
            uri = %sent.uri,
            part = %sent.part,
            bytes = sent.bytes,
            "part upload initiated",
        );
        Ok(sent)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        let this = self.project();
        let parts = ready!(this.buf.poll_complete(cx))?;
        this.completed.extend(parts);
        Poll::Ready(Ok(()))
    }

    fn poll_complete(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<Self::Output>> {
        let mut this = self.project();

        if this.fut.is_none() {
            let data = this.data.as_ref().expect("polled Upload after completion");
            let parts = ready!(this.buf.poll_complete(cx))?;
            this.completed.extend(parts);
            let completed = std::mem::take(this.completed);
            let req = CompleteRequest::new(data, completed);
            trace!(
                id = %req.id(),
                uri = ?req.uri(),
                parts = ?req.completed_parts(),
                "completing upload",
            );
            let fut = SendCompleteUpload::new(this.client, req);
            this.fut.set(Some(fut));
        }

        let fut = this
            .fut
            .as_mut()
            .as_pin_mut()
            .expect("polled Upload after completion");
        let out = ready!(fut.poll(cx));

        this.fut.set(None);
        *this.data = None;
        *this.part = PartNumber::default();
        trace!(result = ?out, "completed upload");

        Poll::Ready(out)
    }
}

impl<Buf: Debug> Debug for UploadImpl<Buf> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("UploadImpl")
            .field("buf", &self.buf)
            .field("fut", &self.fut)
            .field("data", &self.data)
            .field("client", &self.client)
            .field("completed", &self.completed)
            .field("part", &self.part)
            .finish()
    }
}
