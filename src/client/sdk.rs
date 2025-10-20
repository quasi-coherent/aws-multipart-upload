use super::{NullRequestBuilder, RequestBuilder, SendRequest};
use crate::error::{ErrorRepr, Result};
use crate::sdk::api::*;
use crate::sdk::*;

use aws_config::SdkConfig;
use aws_sdk_s3 as s3;

/// S3 [`Client`] type from the AWS SDK.
///
/// [`Client`]: aws_sdk_s3::Client
#[derive(Debug, Clone)]
pub struct SdkClient<B = NullRequestBuilder>(s3::Client, B);

impl SdkClient {
    /// Create a new `SdkClient` from an existing SDK `Client`.
    ///
    /// [`Client`]: aws_sdk_s3::Client
    pub fn new(client: s3::Client) -> Self {
        SdkClient(client, NullRequestBuilder)
    }

    /// Create a new `SdkClient` from an [`SdkConfig`].
    ///
    /// [`SdkConfig`]: aws_config::SdkConfig
    pub fn from_sdk_config(config: SdkConfig) -> Self {
        let client = s3::Client::new(&config);
        Self::new(client)
    }

    /// Set a request builder for this S3 client.
    pub fn request_builder<B: RequestBuilder>(self, builder: B) -> SdkClient<B> {
        SdkClient(self.0, builder)
    }
}

impl<B: RequestBuilder> SdkClient<B> {
    /// Create a default `CreateRequestBuilder` to set properties on for a
    /// `CreateMultipartUpload` request.
    pub(crate) fn new_create_builder(&self) -> CreateRequestBuilder {
        self.0.create_multipart_upload()
    }

    /// Create a default `UploadPartRequestBuilder` to set properties on for an
    /// `UploadPart`.
    pub(crate) fn new_part_builder(&self) -> UploadPartRequestBuilder {
        self.0.upload_part()
    }

    /// Create a default `CompleteRequestBuilder` to set properties on for a
    /// `CompleteMultipartUpload` request.
    pub(crate) fn new_complete_builder(&self) -> CompleteRequestBuilder {
        self.0.complete_multipart_upload()
    }
}

impl<B: RequestBuilder> SendRequest for SdkClient<B> {
    async fn send_create_upload_request(&self, req: CreateRequest) -> Result<UploadData> {
        req.validate()?;
        let base = self.new_create_builder();
        let builder = req.with_builder(base);
        let request = self.1.with_create_builder(builder);

        let uri = req.uri();
        let id = request
            .send()
            .await
            .map_err(ErrorRepr::from_create_err(uri))
            .and_then(UploadId::try_from_create_resp)?;

        Ok(UploadData::new(id, uri.clone()))
    }

    async fn send_new_part_upload_request(
        &self,
        mut req: UploadPartRequest,
    ) -> Result<CompletedPart> {
        req.validate()?;
        let part_size = req.body.size();

        let base = self.new_part_builder();
        let builder = req.with_builder(base);
        let request = self.1.with_upload_part_builder(builder);

        let id = req.id();
        let uri = req.uri();
        let part = req.part_number();
        let etag = request
            .send()
            .await
            .map_err(ErrorRepr::from_upload_err(id, uri, part))
            .and_then(EntityTag::try_from_upload_resp)?;

        Ok(CompletedPart::new(etag, part, part_size))
    }

    async fn send_complete_upload_request(&self, req: CompleteRequest) -> Result<CompletedUpload> {
        req.validate()?;
        let base = self.new_complete_builder();
        let builder = req.with_builder(base);
        let request = self.1.with_complete_builder(builder);

        let id = req.id();
        let uri = req.uri();
        let parts = req.completed_parts();
        let etag = request
            .send()
            .await
            .map_err(ErrorRepr::from_complete_err(id, uri, parts))
            .and_then(EntityTag::try_from_complete_resp)?;

        Ok(CompletedUpload::new(uri.clone(), etag))
    }
}
