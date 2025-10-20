use crate::client::UploadClient;
use crate::error::Result;
use crate::sdk::api::{CompleteRequest, CreateRequest, UploadPartRequest};
use crate::sdk::{CompletedPart, CompletedUpload, UploadData};

use std::fmt::{self, Debug, Formatter};
use std::pin::Pin;
use std::task::{Context, Poll};

/// Future representing a request to create a new multipart upload.
pub struct SendCreateUpload(Pin<Box<dyn Future<Output = Result<UploadData>>>>);

impl SendCreateUpload {
    /// Create the new upload request to send.
    pub fn new(client: &UploadClient, req: CreateRequest) -> Self {
        let inner = client.inner.clone();
        Self(Box::pin(async move { inner.send_create(req).await }))
    }
}

impl Future for SendCreateUpload {
    type Output = Result<UploadData>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.0.as_mut().poll(cx)
    }
}

impl Debug for SendCreateUpload {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_tuple("SendCreateUpload")
            .field(&"Future<Output = Result<UploadData>>")
            .finish()
    }
}

/// Future representing a request to upload a part to an active upload.
pub struct SendUploadPart(Pin<Box<dyn Future<Output = Result<CompletedPart>>>>);

impl SendUploadPart {
    /// Create the upload part request to send.
    pub fn new(client: &UploadClient, req: UploadPartRequest) -> Self {
        let inner = client.inner.clone();
        Self(Box::pin(async move { inner.send_upload(req).await }))
    }
}

impl Future for SendUploadPart {
    type Output = Result<CompletedPart>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.0.as_mut().poll(cx)
    }
}

impl Debug for SendUploadPart {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_tuple("SendUploadPart")
            .field(&"Future<Output = Result<CompletedPart>>")
            .finish()
    }
}

/// Future representing a request to complete an active multipart upload.
pub struct SendCompleteUpload(Pin<Box<dyn Future<Output = Result<CompletedUpload>>>>);

impl SendCompleteUpload {
    /// Create the complete upload request to send.
    pub fn new(client: &UploadClient, req: CompleteRequest) -> Self {
        let inner = client.inner.clone();
        Self(Box::pin(async move { inner.send_complete(req).await }))
    }
}

impl Future for SendCompleteUpload {
    type Output = Result<CompletedUpload>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.0.as_mut().poll(cx)
    }
}

impl Debug for SendCompleteUpload {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_tuple("SendCompleteUpload")
            .field(&"Future<Output = Result<CompletedUpload>>")
            .finish()
    }
}
