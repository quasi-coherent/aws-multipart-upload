use aws_config::SdkConfig;
use aws_sdk_s3::{self as s3, primitives::ByteStream};
use futures::future::BoxFuture;
use std::sync::Arc;

use crate::{types::api::*, AwsError};

/// An AWS client for S3 multipart uploads.
///
/// It has the required methods of `UploadClient`, a customized version of the
/// provided `on_complete_upload` can be added for a complete implementation.
#[derive(Debug, Clone)]
pub struct AwsClient {
    inner: Arc<s3::Client>,
}

impl AwsClient {
    pub fn new(config: SdkConfig) -> Self {
        let s3 = s3::Client::new(&config);

        Self {
            inner: Arc::new(s3),
        }
    }

    /// Get a reference to the AWS client.
    pub fn get_ref(&self) -> &s3::Client {
        &self.inner
    }

    /// Begin a new upload and return the ID obtained from the response.
    pub fn new_upload<'a, 'client: 'a>(
        &'client self,
        addr: &'a UploadAddress,
    ) -> BoxFuture<'a, Result<UploadRequestParams, AwsError>> {
        Box::pin(async move {
            let resp = self
                .inner
                .create_multipart_upload()
                .bucket(addr.bucket())
                .key(addr.key())
                .send()
                .await?;
            let upload_id = UploadId::try_from(resp)?;
            let params = UploadRequestParams::new(upload_id, addr.clone());

            Ok(params)
        })
    }

    /// Upload one part to the multipart upload with the given part number.
    pub fn upload_part<'a, 'client: 'a>(
        &'client self,
        params: &'a UploadRequestParams,
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

    /// Complete an upload returning the entity tag of the created object.
    pub fn complete_upload<'a, 'client: 'a>(
        &'client self,
        params: &'a UploadRequestParams,
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
