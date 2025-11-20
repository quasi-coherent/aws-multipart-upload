use super::UploadPartRequestBuilder;
use crate::client::part::{CompletedPart, PartBody, PartNumber};
use crate::client::{UploadClient, UploadData, UploadId};
use crate::error::{ErrorRepr, Result};
use crate::uri::ObjectUri;

use std::fmt::{self, Debug, Formatter};
use std::pin::Pin;
use std::task::{Context, Poll};

/// Sending a request to add a part to an existing multpart upload.
pub struct SendUploadPart(pub(crate) Pin<Box<dyn Future<Output = Result<CompletedPart>>>>);

impl SendUploadPart {
    /// Create a new `SendUploadPart`.
    pub fn new(client: &UploadClient, req: UploadPartRequest) -> Self {
        let cli = client.clone();
        Self(Box::pin(
            async move { cli.inner.send_upload_part(req).await },
        ))
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

/// Request object for uploading a new part.
#[derive(Debug, Clone)]
pub struct UploadPartRequest {
    pub(crate) id: UploadId,
    pub(crate) uri: ObjectUri,
    pub(crate) body: PartBody,
    pub(crate) part_number: PartNumber,
}

impl UploadPartRequest {
    /// Create a new `UploadPartRequest` from the minimum required.
    pub fn new(data: &UploadData, body: PartBody, part_number: PartNumber) -> Self {
        Self {
            id: data.get_id(),
            uri: data.get_uri(),
            body,
            part_number,
        }
    }

    /// Set the required properties on the SDK request builder for the operation.
    pub fn with_builder(&mut self, builder: UploadPartRequestBuilder) -> UploadPartRequestBuilder {
        builder
            .upload_id(&*self.id)
            .bucket(&*self.uri.bucket)
            .key(&*self.uri.key)
            .part_number(*self.part_number)
            .body(self.body.as_sdk_body())
    }

    /// Returns a reference to the assigned `UploadId` for this request.
    pub fn id(&self) -> &UploadId {
        &self.id
    }

    /// Returns a reference to the `ObjectUri` for this request.
    pub fn uri(&self) -> &ObjectUri {
        &self.uri
    }

    /// Returns a reference to the `PartBody` for this request.
    pub fn body(&self) -> &PartBody {
        &self.body
    }

    /// Returns a reference to the `PartNumber` for this request.
    pub fn part_number(&self) -> PartNumber {
        self.part_number
    }

    pub(crate) fn validate(&self) -> Result<()> {
        if self.id.is_empty() || self.uri.is_empty() {
            return Err(
                ErrorRepr::Missing("UploadPartRequest", "empty upload id and/or uri").into(),
            );
        }
        Ok(())
    }
}
