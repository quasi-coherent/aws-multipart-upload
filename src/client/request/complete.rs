use super::CompleteRequestBuilder;
use crate::client::part::{CompletedParts, EntityTag};
use crate::client::{UploadClient, UploadData, UploadId};
use crate::error::{ErrorRepr, Result};
use crate::uri::ObjectUri;

use aws_sdk::types::CompletedMultipartUpload;
use std::fmt::{self, Debug, Formatter};
use std::pin::Pin;
use std::task::{Context, Poll};

/// Sending a request to complete an upload.
pub struct SendCompleteUpload(pub(crate) Pin<Box<dyn Future<Output = Result<CompletedUpload>>>>);

impl SendCompleteUpload {
    /// Create a new `SendCompleteUpload`.
    pub fn new(client: &UploadClient, req: CompleteRequest) -> Self {
        let cli = client.clone();
        Self(Box::pin(async move {
            cli.inner.send_complete_upload(req).await
        }))
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

/// Request object for completing a multipart upload.
#[derive(Debug, Clone)]
pub struct CompleteRequest {
    pub(crate) id: UploadId,
    pub(crate) uri: ObjectUri,
    pub(crate) completed_parts: CompletedParts,
}

impl CompleteRequest {
    /// Create a new `CompleteRequest` from the minimum required.
    pub fn new(data: &UploadData, completed_parts: CompletedParts) -> Self {
        Self {
            id: data.get_id(),
            uri: data.get_uri(),
            completed_parts,
        }
    }

    /// Set the required properties on the SDK request builder for the operation.
    pub fn with_builder(&self, builder: CompleteRequestBuilder) -> CompleteRequestBuilder {
        let parts = CompletedMultipartUpload::from(&self.completed_parts);

        builder
            .upload_id(&*self.id)
            .bucket(&*self.uri.bucket)
            .key(&*self.uri.key)
            .multipart_upload(parts)
    }

    /// Returns a reference to the assigned `UploadId` for this request.
    pub fn id(&self) -> &UploadId {
        &self.id
    }

    /// Returns a reference to the `ObjectUri` for this request.
    pub fn uri(&self) -> &ObjectUri {
        &self.uri
    }

    /// Returns a reference to the `CompletedParts` for this request.
    pub fn completed_parts(&self) -> &CompletedParts {
        &self.completed_parts
    }

    pub(crate) fn validate(&self) -> Result<()> {
        if self.id.is_empty() || self.uri.is_empty() {
            return Err(
                ErrorRepr::Missing("CompleteUploadRequest", "empty upload id and/or uri").into(),
            );
        }
        Ok(())
    }
}

/// The value for a successful multipart upload.
#[derive(Debug, Clone, Default)]
pub struct CompletedUpload {
    /// The URI of the created object.
    pub uri: ObjectUri,
    /// The entity tag of the created object.
    pub etag: EntityTag,
}

impl CompletedUpload {
    /// Create a new value from object URI and entity tag.
    pub fn new(uri: ObjectUri, etag: EntityTag) -> Self {
        Self { uri, etag }
    }
}
