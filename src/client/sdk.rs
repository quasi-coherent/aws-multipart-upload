use crate::client::part::{CompletedPart, EntityTag};
use crate::client::request::*;
use crate::client::{DefaultRequestBuilder, RequestBuilder, SendRequest, UploadData, UploadId};
use crate::error::{ErrorRepr, Result, UploadContext as _};

use aws_config::ConfigLoader;

/// AWS S3 SDK client.
///
/// Pairs a [`Client`] with a [`RequestBuilder`] used to set additional
/// properties on request objects before sending.
///
/// [`Client`]: aws_sdk::Client
/// [`RequestBuilder`]: super::request::RequestBuilder
#[derive(Debug, Clone)]
pub struct SdkClient<B = DefaultRequestBuilder>(aws_sdk::Client, B);

impl SdkClient {
    /// Create a new `SdkClient` with default [`RequestBuilder`].
    ///
    /// [`RequestBuilder`]: super::request::RequestBuilder
    pub fn new(client: aws_sdk::Client) -> Self {
        SdkClient(client, DefaultRequestBuilder)
    }

    /// Create a new `SdkClient` from the supplied [`ConfigLoader`].
    ///
    /// [`ConfigLoader`]: aws_config::ConfigLoader
    pub async fn from_config(loader: ConfigLoader) -> Self {
        let config = loader.load().await;
        let client = aws_sdk::Client::new(&config);
        Self::new(client)
    }

    /// Create a new `SdkClient` with default [`RequestBuilder`] using the
    /// default [`ConfigLoader`].
    pub async fn defaults() -> Self {
        let loader = aws_config::from_env();
        Self::from_config(loader).await
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

    /// Create a default `CompleteRequestBuilder` to set properties on for a
    /// `CompleteMultipartUpload` request.
    pub(crate) fn new_abort_builder(&self) -> AbortRequestBuilder {
        self.0.abort_multipart_upload()
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
            .map_err(ErrorRepr::from)
            .and_then(|resp| UploadId::try_from_create_resp(&resp))?;

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
            .map_err(ErrorRepr::from)
            .and_then(|resp| EntityTag::try_from_upload_resp(&resp))
            .upload_ctx(id, uri, part)?;

        Ok(CompletedPart::new(id.clone(), etag, part, part_size))
    }

    async fn send_complete_upload_request(&self, req: CompleteRequest) -> Result<CompletedUpload> {
        req.validate()?;
        let base = self.new_complete_builder();
        let builder = req.with_builder(base);
        let request = self.1.with_complete_builder(builder);

        let id = req.id();
        let uri = req.uri();
        let part = req.completed_parts.max_part_number();
        let etag = request
            .send()
            .await
            .map_err(ErrorRepr::from)
            .and_then(|resp| EntityTag::try_from_complete_resp(&resp))
            .upload_ctx(id, uri, part)?;

        Ok(CompletedUpload::new(uri.clone(), etag))
    }

    async fn send_abort_upload_request(&self, req: AbortRequest) -> Result<()> {
        let base = self.new_abort_builder();
        let builder = req.with_builder(base);
        let request = self.1.with_abort_builder(builder);
        let _ = request.send().await.map_err(ErrorRepr::from)?;
        Ok(())
    }
}
