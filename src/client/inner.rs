use super::SendRequest;
use crate::error::Result;
use crate::sdk::api::*;
use crate::sdk::{CompletedPart, CompletedUpload, UploadData};

use futures::future::BoxFuture;

/// Object-safe `SendRequest`.
pub(crate) trait BoxedSendRequest: Send + Sync + 'static {
    /// Create a new upload.
    fn send_create(&self, req: CreateRequest) -> BoxFuture<'_, Result<UploadData>>;

    /// Upload a new part.
    fn send_upload(&self, req: UploadPartRequest) -> BoxFuture<'_, Result<CompletedPart>>;

    /// Complete the upload.
    fn send_complete(&self, req: CompleteRequest) -> BoxFuture<'_, Result<CompletedUpload>>;
}

/// Implements `BoxedSendRequest` for the public `SendRequest`.
pub(super) struct SendRequestInner<T>(T);

impl<T: SendRequest> SendRequestInner<T> {
    pub(super) fn new(inner: T) -> Self {
        Self(inner)
    }
}

impl<T: SendRequest + 'static> BoxedSendRequest for SendRequestInner<T> {
    fn send_create(&self, req: CreateRequest) -> BoxFuture<'_, Result<UploadData>> {
        Box::pin(self.0.send_create_upload_request(req))
    }

    /// Upload a new part.
    fn send_upload(&self, req: UploadPartRequest) -> BoxFuture<'_, Result<CompletedPart>> {
        Box::pin(self.0.send_new_part_upload_request(req))
    }

    /// Complete the upload.
    fn send_complete(&self, req: CompleteRequest) -> BoxFuture<'_, Result<CompletedUpload>> {
        Box::pin(self.0.send_complete_upload_request(req))
    }
}
