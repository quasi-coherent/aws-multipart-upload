use crate::client::part::{CompletedParts, PartBody, PartNumber};
use crate::client::request::*;
use crate::client::{UploadClient, UploadData, UploadId};
use crate::error::{Error as UploadError, Result};
use crate::uri::{NewObjectUri, ObjectUri, OneTimeUse};

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
    /// The part number that was used in the part upload request.
    pub part: PartNumber,
    /// The size in bytes of the body of the part upload request.
    pub size: usize,
}

impl UploadSent {
    fn new(id: &UploadId, part: PartNumber, size: usize) -> Self {
        Self {
            id: id.clone(),
            part,
            size,
        }
    }
}

/// A type to manage the lifecycle of a repeating series of multipart uploads.
///
/// This `MultipartWrite` implementation is over the [`PartBody`] of a part
/// upload request.  Sending a `PartBody` forms and submits the request to upload
/// it to a multipart upload, and polling for completion completes the upload,
/// returning the response in [`CompletedUpload`].
///
/// On completion, a new upload is created using the provided `NewObjectUri` and
/// makes the writer available to continue writing new parts.  This continues as
/// long as the iterator `NewObjectUri` can produce the next active upload.
///
/// [`PartBody`]: crate::client::part::PartBody
/// [`CompletedUpload`]: crate::client::request::CompletedUpload
#[must_use = "futures do nothing unless polled"]
#[pin_project::pin_project]
pub struct UploaderWithUri<Buf> {
    #[pin]
    uploader: Uploader<Buf>,
    #[pin]
    fut: Option<SendCreateUpload>,
    next_uri: Option<ObjectUri>,
    iter: NewObjectUri,
}

impl<Buf> UploaderWithUri<Buf> {
    pub(crate) fn new(buf: Buf, client: &UploadClient, mut iter: NewObjectUri) -> Self {
        let uploader = Uploader::new_inactive(buf, client);
        let fut = iter.new_upload(client);
        Self {
            uploader,
            fut,
            next_uri: None,
            iter,
        }
    }

    pub(crate) fn new_with_uri(buf: Buf, client: &UploadClient, uri: ObjectUri) -> Self {
        let iter = NewObjectUri::uri_iter(OneTimeUse::new(uri));
        Self::new(buf, client, iter)
    }

    fn poll_new_upload(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        let mut this = self.project();

        if let Some(uri) = this.next_uri.take() {
            trace!(?uri, "starting new upload");
            let req = CreateRequest::new(uri);
            let fut = SendCreateUpload::new(&this.uploader.client, req);
            this.fut.set(Some(fut));
        }

        if let Some(fut) = this.fut.as_mut().as_pin_mut() {
            match ready!(fut.poll(cx)) {
                Ok(data) => {
                    this.fut.set(None);
                    trace!(id = %data.id, uri = ?data.uri, "started new upload");
                    this.uploader.as_mut().set_upload_data(data);
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

impl<Buf> FusedMultipartWrite<PartBody> for UploaderWithUri<Buf>
where
    Buf: MultipartWrite<SendUploadPart, Output = CompletedParts, Error = UploadError>,
{
    fn is_terminated(&self) -> bool {
        // If the inner upload is not active, and there is no request for a new
        // upload nor next URI to make the request, we are terminated.
        self.uploader.is_terminated() && self.fut.is_none() && self.next_uri.is_none()
    }
}

impl<Buf> MultipartWrite<PartBody> for UploaderWithUri<Buf>
where
    Buf: MultipartWrite<SendUploadPart, Output = CompletedParts, Error = UploadError>,
{
    type Ret = UploadSent;
    type Error = UploadError;
    type Output = CompletedUpload;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        ready!(self.as_mut().poll_new_upload(cx))?;
        self.project().uploader.poll_ready(cx)
    }

    fn start_send(self: Pin<&mut Self>, part: PartBody) -> Result<Self::Ret> {
        self.project().uploader.start_send(part)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        self.project().uploader.poll_flush(cx)
    }

    fn poll_complete(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<Self::Output>> {
        let mut this = self.project();
        let out = ready!(this.uploader.as_mut().poll_complete(cx));
        *this.next_uri = this.iter.new_uri();

        trace!(next_uri = ?this.next_uri, "completed upload");
        Poll::Ready(out)
    }
}

impl<Buf: Debug> Debug for UploaderWithUri<Buf> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("UploaderWithUri")
            .field("uploader", &self.uploader)
            .field("fut", &self.fut)
            .field("iter", &self.iter)
            .finish()
    }
}

/// A type to manage the lifecycle of a multipart upload.
///
/// This `MultipartWrite` implementation is over the [`PartBody`] of a part
/// upload request.  Sending a `PartBody` forms and submits the request to upload
/// it to a multipart upload, and polling for completion completes the upload,
/// returning the response in [`CompletedUpload`].
///
/// Note that this writer becomes terminated after completion.  It is an error
/// to poll it after `poll_complete` has returned.
///
/// [`PartBody`]: crate::client::part::PartBody
/// [`CompletedUpload`]: crate::client::request::CompletedUpload
#[must_use = "futures do nothing unless polled"]
#[pin_project::pin_project]
pub struct Uploader<Buf> {
    #[pin]
    buf: Buf,
    #[pin]
    fut: Option<SendCompleteUpload>,
    data: Option<UploadData>,
    client: UploadClient,
    completed: CompletedParts,
    part: PartNumber,
}

impl<Buf> Uploader<Buf> {
    pub(crate) fn new_inactive(buf: Buf, client: &UploadClient) -> Self {
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

impl<Buf> FusedMultipartWrite<PartBody> for Uploader<Buf>
where
    Buf: MultipartWrite<SendUploadPart, Output = CompletedParts, Error = UploadError>,
{
    fn is_terminated(&self) -> bool {
        self.data.is_none()
    }
}

impl<Buf> MultipartWrite<PartBody> for Uploader<Buf>
where
    Buf: MultipartWrite<SendUploadPart, Output = CompletedParts, Error = UploadError>,
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
        let data = this
            .data
            .as_ref()
            .expect("polled Uploader after completion");
        let pt_num = this.part.increment();

        let req = UploadPartRequest::new(data, part, pt_num);
        let fut = SendUploadPart::new(this.client, req);
        let _ = this.buf.as_mut().start_send(fut)?;
        let sent = UploadSent::new(&data.id, pt_num, bytes);
        trace!(
            id = %sent.id,
            part = %sent.part,
            size = sent.size,
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
            let mut parts = ready!(this.buf.poll_complete(cx))?;
            let old_parts = std::mem::take(this.completed);
            parts.extend(old_parts);
            let data = this
                .data
                .as_ref()
                .expect("polled Uploader after completion");
            let req = CompleteRequest::new(data, parts);
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
            .expect("polled Uploader after completion");
        let out = ready!(fut.poll(cx));
        this.fut.set(None);
        *this.data = None;

        trace!(result = ?out, "completed upload");
        Poll::Ready(out)
    }
}

impl<Buf: Debug> Debug for Uploader<Buf> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("Uploader")
            .field("buf", &self.buf)
            .field("fut", &self.fut)
            .field("data", &self.data)
            .field("client", &self.client)
            .field("completed", &self.completed)
            .field("part", &self.part)
            .finish()
    }
}
