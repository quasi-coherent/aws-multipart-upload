use aws_config::SdkConfig;
use aws_sdk_s3::{self as s3, primitives::ByteStream};
use futures::future::BoxFuture;
use std::sync::Arc;

use crate::{
    client::UploadClient,
    types::{EntityTag, UploadAddress, UploadId, UploadParams, UploadedParts},
    AwsError,
};

/// An AWS client for S3 multipart uploads.
#[derive(Debug, Clone)]
pub struct AwsClient {
    inner: Arc<s3::Client>,
}

impl AwsClient {
    pub fn new(s3: s3::Client) -> Self {
        Self {
            inner: Arc::new(s3),
        }
    }

    pub fn new_with(config: SdkConfig) -> Self {
        let s3 = s3::Client::new(&config);
        Self::new(s3)
    }

    /// Get a reference to the AWS client.
    pub fn get_ref(&self) -> &s3::Client {
        &self.inner
    }
}

impl UploadClient for AwsClient {
    fn new_upload<'a, 'c: 'a>(
        &'c self,
        addr: &'a UploadAddress,
    ) -> BoxFuture<'a, Result<UploadParams, AwsError>> {
        Box::pin(async move {
            let resp = self
                .inner
                .create_multipart_upload()
                .bucket(addr.bucket())
                .key(addr.key())
                .send()
                .await?;
            let upload_id = UploadId::try_from(resp)?;
            let params = UploadParams::new(upload_id, addr.clone());

            Ok(params)
        })
    }

    fn upload_part<'a, 'c: 'a>(
        &'c self,
        params: &'a UploadParams,
        part_number: i32,
        part: ByteStream,
    ) -> BoxFuture<'a, Result<EntityTag, AwsError>> {
        Box::pin(async move {
            let resp = self
                .inner
                .upload_part()
                .upload_id(params.upload_id())
                .bucket(params.bucket())
                .key(params.key())
                .part_number(part_number)
                .body(part)
                .send()
                .await?;

            EntityTag::try_from(resp)
        })
    }

    fn complete_upload<'a, 'c: 'a>(
        &'c self,
        params: &'a UploadParams,
        parts: &'a UploadedParts,
    ) -> BoxFuture<'a, Result<EntityTag, AwsError>> {
        Box::pin(async move {
            let object_id = self
                .inner
                .complete_multipart_upload()
                .upload_id(params.upload_id())
                .bucket(params.bucket())
                .key(params.key())
                .multipart_upload(parts.into())
                .send()
                .await?
                .e_tag()
                .map(|t| EntityTag::new(t.to_string()))
                .ok_or_else(|| AwsError::Missing("entity_tag"))?;

            Ok(object_id)
        })
    }
}
