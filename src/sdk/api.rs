use crate::error::{ErrorRepr, Result};
use crate::sdk::{CompletedParts, ObjectUri, PartBody, PartNumber, UploadId};

pub use crate::complete_upload::builders::CompleteMultipartUploadFluentBuilder as CompleteRequestBuilder;
pub use crate::create_upload::builders::CreateMultipartUploadFluentBuilder as CreateRequestBuilder;
pub use crate::upload_part::builders::UploadPartFluentBuilder as UploadPartRequestBuilder;

use aws_sdk_s3::types::CompletedMultipartUpload;

/// Minimum data to create a request to begin a multipart upload:
///
/// * `uri`: The target `ObjectUri` of the upload.
#[derive(Debug, Clone)]
pub struct CreateRequest {
    pub(crate) uri: ObjectUri,
}

impl CreateRequest {
    /// Create a new `CreateRequest` from the minimal parameters.
    pub fn new(uri: ObjectUri) -> Self {
        Self { uri }
    }

    /// Set the required properties on the SDK request builder for the operation.
    pub fn with_builder(&self, builder: CreateRequestBuilder) -> CreateRequestBuilder {
        builder.bucket(&*self.uri.bucket).key(&*self.uri.key)
    }

    pub fn uri(&self) -> &ObjectUri {
        &self.uri
    }

    pub(crate) fn validate(&self) -> Result<()> {
        if self.uri.is_empty() {
            return Err(ErrorRepr::Create {
                uri: self.uri.clone(),
                source: "empty object uri".into(),
            }
            .into());
        }
        Ok(())
    }
}

/// Minimum data to create a request to add a part to a multipart upload:
///
/// * `id`: The `UploadId` as assigned by AWS.
/// * `uri`: The target `ObjectUri` of the upload.
/// * `body`: The `PartBody` of the upload.
/// * `part_number`: The next `PartNumber` in the sequence.
#[derive(Debug, Clone)]
pub struct UploadPartRequest {
    pub(crate) id: UploadId,
    pub(crate) uri: ObjectUri,
    pub(crate) body: PartBody,
    pub(crate) part_number: PartNumber,
}

impl UploadPartRequest {
    /// Create a new `UploadPartRequest` from the minimal parameters.
    pub fn new(id: UploadId, uri: ObjectUri, body: PartBody, part_number: PartNumber) -> Self {
        Self {
            id,
            uri,
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

    pub fn id(&self) -> &UploadId {
        &self.id
    }

    pub fn uri(&self) -> &ObjectUri {
        &self.uri
    }

    pub fn body(&self) -> &PartBody {
        &self.body
    }

    pub fn part_number(&self) -> PartNumber {
        self.part_number
    }

    pub(crate) fn validate(&self) -> Result<()> {
        if self.id.is_empty() || self.uri.is_empty() {
            return Err(ErrorRepr::UploadPart {
                id: self.id.clone(),
                uri: self.uri.clone(),
                part: self.part_number,
                source: "empty upload id and/or uri".into(),
            }
            .into());
        }
        Ok(())
    }
}

/// Minimum data to create a request to complete an upload:
///
/// * `id`: The `UploadId` as assigned by AWS.
/// * `uri`: The target `ObjectUri` of the upload.
/// * `completed_parts`: `CompletedParts`, the collection of `EntityTag`s
/// that were obtained when uploading a part, and the `PartNumber` we used in
/// that part upload request.
///
/// [`EntityTag`]: crate::sdk::EntityTag
#[derive(Debug, Clone)]
pub struct CompleteRequest {
    pub(crate) id: UploadId,
    pub(crate) uri: ObjectUri,
    pub(crate) completed_parts: CompletedParts,
}

impl CompleteRequest {
    /// Create a new `CompleteRequest` from the minimal parameters.
    pub fn new(id: UploadId, uri: ObjectUri, completed_parts: CompletedParts) -> Self {
        Self {
            id,
            uri,
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

    pub fn id(&self) -> &UploadId {
        &self.id
    }

    pub fn uri(&self) -> &ObjectUri {
        &self.uri
    }

    pub fn completed_parts(&self) -> &CompletedParts {
        &self.completed_parts
    }

    pub(crate) fn validate(&self) -> Result<()> {
        if self.id.is_empty() || self.uri.is_empty() {
            return Err(ErrorRepr::Complete {
                id: self.id.clone(),
                uri: self.uri.clone(),
                parts: self.completed_parts.clone(),
                source: "empty upload id and/or uri".into(),
            }
            .into());
        }
        Ok(())
    }
}
