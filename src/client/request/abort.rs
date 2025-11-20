use super::AbortRequestBuilder;
use crate::client::{UploadClient, UploadId};
use crate::error::Result;
use crate::uri::ObjectUri;

use std::fmt::{self, Debug, Formatter};
use std::pin::Pin;
use std::task::{Context, Poll};

/// Sending a request to abort an in-progress upload.
pub struct SendAbortUpload(pub(crate) Pin<Box<dyn Future<Output = Result<()>>>>);

impl SendAbortUpload {
    /// Create a new `SendAbortUpload`.
    pub fn new(client: &UploadClient, req: AbortRequest) -> Self {
        let cli = client.clone();
        Self(Box::pin(
            async move { cli.inner.send_abort_upload(req).await },
        ))
    }
}

impl Future for SendAbortUpload {
    type Output = Result<()>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.0.as_mut().poll(cx)
    }
}

impl Debug for SendAbortUpload {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_tuple("SendAbortUpload")
            .field(&"Future<Output = Result<()>>")
            .finish()
    }
}

/// Request object for aborting a multipart upload.
#[derive(Debug, Clone)]
pub struct AbortRequest {
    pub(crate) id: UploadId,
    pub(crate) uri: ObjectUri,
}

impl AbortRequest {
    /// Create a new `AbortRequest` from the minimum required.
    pub fn new(id: UploadId, uri: ObjectUri) -> Self {
        Self { id, uri }
    }

    /// Set the required properties on the SDK request builder for the operation.
    pub fn with_builder(&self, builder: AbortRequestBuilder) -> AbortRequestBuilder {
        builder
            .bucket(&*self.uri.bucket)
            .key(&*self.uri.key)
            .upload_id(&*self.id)
    }
}
