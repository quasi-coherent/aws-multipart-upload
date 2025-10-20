use crate::client::UploadClient;
use crate::error::{Error as UploadError, ErrorRepr, Result};
use crate::sdk::api::{CompleteRequest, UploadPartRequest};
use crate::sdk::{CompletedParts, CompletedUpload, PartBody, PartNumber, UploadData};
use crate::upload::{SendCompleteUpload, SendUploadPart, UploadProgress, UploadState};

use futures::ready;
use multipart_write::FusedMultipartWrite;
use multipart_write::prelude::*;
use std::fmt::{self, Debug, Formatter};
use std::pin::Pin;
use std::task::{Context, Poll};

/// `MultipartUpload` is a multipart upload writer with a buffer `Buf` for
/// writing part upload request futures.  Completing this writer sends a request
/// to complete the upload, returning the response data in the writer output
/// [`CompletedUpload`].
#[must_use = "futures do nothing unless polled"]
#[pin_project::pin_project]
pub struct MultipartUpload<Buf> {
    client: UploadClient,
    #[pin]
    buf: Buf,
    #[pin]
    future: Option<SendCompleteUpload>,
    data: UploadData,
    completed: CompletedParts,
    part_number: PartNumber,
    state: UploadState,
    is_active: bool,
}

impl<Buf> MultipartUpload<Buf> {
    /// Make this `MultipartUpload` active by setting the [`UploadData`] it uses
    /// to build part upload requests.
    ///
    /// # Errors
    ///
    /// Returns an error if this upload is already active.
    pub fn with_upload_data(mut self, data: UploadData) -> Result<Self> {
        if self.is_active {
            return Err(ErrorRepr::UploadStillActive)?;
        }
        self.data = data;
        self.is_active = true;
        Ok(self)
    }

    pub(super) fn new(buf: Buf, client: UploadClient, data: UploadData) -> Self {
        Self {
            client,
            buf,
            future: None,
            data,
            completed: CompletedParts::default(),
            part_number: PartNumber::default(),
            state: UploadState::default(),
            is_active: true,
        }
    }

    pub(super) fn new_inactive(buf: Buf, client: UploadClient) -> Self {
        Self {
            client,
            buf,
            future: None,
            data: UploadData::default(),
            completed: CompletedParts::default(),
            part_number: PartNumber::default(),
            state: UploadState::default(),
            is_active: false,
        }
    }

    pub(super) fn reactivate(self: Pin<&mut Self>, new: UploadData) -> Result<UploadData> {
        let this = self.project();
        if *this.is_active {
            return Err(ErrorRepr::UploadStillActive)?;
        }
        let old = std::mem::replace(this.data, new);
        *this.completed = CompletedParts::default();
        *this.part_number = PartNumber::default();
        *this.state = UploadState::default();
        *this.is_active = true;

        Ok(old)
    }
}

impl<Buf> FusedMultipartWrite<PartBody> for MultipartUpload<Buf>
where
    Buf: MultipartWrite<SendUploadPart, Output = CompletedParts, Error = UploadError>,
{
    fn is_terminated(&self) -> bool {
        !self.is_active
    }
}

impl<Buf> MultipartWrite<PartBody> for MultipartUpload<Buf>
where
    Buf: MultipartWrite<SendUploadPart, Output = CompletedParts, Error = UploadError>,
{
    type Ret = UploadProgress<Buf::Ret>;
    type Output = CompletedUpload;
    type Error = UploadError;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        ready!(self.project().buf.poll_ready(cx))?;
        Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, part: PartBody) -> Result<Self::Ret> {
        let this = self.project();
        let part_bytes = part.size();
        let req = UploadPartRequest::new(
            this.data.get_id(),
            this.data.get_uri(),
            part,
            *this.part_number,
        );
        let fut = SendUploadPart::new(&*this.client, req);
        let ret = this.buf.start_send(fut)?;
        this.state.update(part_bytes);
        this.part_number.incr();
        let progress = UploadProgress::new(this.data, *this.state, ret);
        Ok(progress)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        let mut this = self.project();
        ready!(this.buf.as_mut().poll_flush(cx))?;
        let completed = ready!(this.buf.poll_complete(cx))?;
        this.completed.extend(completed);
        Poll::Ready(Ok(()))
    }

    fn poll_complete(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<Self::Output>> {
        let mut this = self.project();
        if this.future.is_none() {
            let completed = std::mem::take(this.completed);
            let req = CompleteRequest::new(this.data.get_id(), this.data.get_uri(), completed);
            let fut = SendCompleteUpload::new(&*this.client, req);
            this.future.set(Some(fut));
        }
        let fut = this.future.as_mut().as_pin_mut().unwrap();
        let output = ready!(fut.poll(cx));
        *this.is_active = false;
        this.future.set(None);
        Poll::Ready(output)
    }
}

impl<Buf: Debug> Debug for MultipartUpload<Buf> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("MultipartUpload")
            .field("client", &self.client)
            .field("buf", &self.buf)
            .field("future", &self.future)
            .field("data", &self.data)
            .field("completed", &self.completed)
            .field("part_number", &self.part_number)
            .field("state", &self.state)
            .field("is_active", &self.is_active)
            .finish()
    }
}
