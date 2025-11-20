use super::CreateRequestBuilder;
use crate::client::{UploadClient, UploadData};
use crate::error::{ErrorRepr, Result};
use crate::uri::ObjectUri;

use std::fmt::{self, Debug, Formatter};
use std::pin::Pin;
use std::task::{Context, Poll};

/// Sending a request to create a new upload.
pub struct SendCreateUpload(pub(crate) Pin<Box<dyn Future<Output = Result<UploadData>>>>);

impl SendCreateUpload {
    /// Create a new `SendCreateUpload`.
    pub fn new(client: &UploadClient, req: CreateRequest) -> Self {
        let cli = client.clone();
        Self(Box::pin(
            async move { cli.inner.send_create_upload(req).await },
        ))
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

/// Request object for creating a new multipart upload.
#[derive(Debug, Clone)]
pub struct CreateRequest {
    pub(crate) uri: ObjectUri,
}

impl CreateRequest {
    /// Create a new `CreateRequest` from the minimum required.
    pub fn new(uri: ObjectUri) -> Self {
        Self { uri }
    }

    /// Set the required properties on the SDK request builder for the operation.
    pub fn with_builder(&self, builder: CreateRequestBuilder) -> CreateRequestBuilder {
        builder.bucket(&*self.uri.bucket).key(&*self.uri.key)
    }

    /// Returns a reference to the `ObjectUri` for this request.
    pub fn uri(&self) -> &ObjectUri {
        &self.uri
    }

    pub(crate) fn validate(&self) -> Result<()> {
        if self.uri.is_empty() {
            return Err(ErrorRepr::Missing("CreateRequest", "empty object uri").into());
        }
        Ok(())
    }
}
