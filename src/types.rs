pub mod api;
pub mod iter_addr;
pub mod upload;
pub mod upload_forever;
mod write_parts;

use aws_sdk_s3::primitives::ByteStream;
use futures::future::{ready, BoxFuture};
use std::sync::Arc;

use self::api::*;
use crate::AwsError;

/// Operations in a multipart upload.
pub trait UploadClient {
    /// Create a new upload returning the ID of the upload.
    fn new_upload<'a, 'client: 'a>(
        &'client self,
        addr: &'a UploadAddress,
    ) -> BoxFuture<'a, Result<UploadRequestParams, AwsError>>;

    /// Upload one part to the multipart upload.
    fn upload_part<'a, 'client: 'a>(
        &'client self,
        params: &'a UploadRequestParams,
        part_number: i32,
        part: ByteStream,
    ) -> BoxFuture<'a, Result<EntityTag, AwsError>>;

    /// Complete the upload.
    fn complete_upload<'a, 'client: 'a>(
        &'client self,
        params: &'a UploadRequestParams,
        parts: &'a UploadedParts,
    ) -> BoxFuture<'a, Result<EntityTag, AwsError>>;

    /// A callback with the `EntityId` returned by `complete_upload`.
    fn on_upload_complete<'a, 'client: 'a>(
        &'client self,
        _etag: &'a EntityTag,
    ) -> BoxFuture<'a, Result<(), AwsError>> {
        Box::pin(ready(Ok(())))
    }
}

impl<U: UploadClient> UploadClient for Arc<U> {
    fn new_upload<'a, 'client: 'a>(
        &'client self,
        addr: &'a UploadAddress,
    ) -> BoxFuture<'a, Result<UploadRequestParams, AwsError>> {
        U::new_upload(self, addr)
    }

    fn upload_part<'a, 'client: 'a>(
        &'client self,
        params: &'a UploadRequestParams,
        part_number: i32,
        part: ByteStream,
    ) -> BoxFuture<'a, Result<EntityTag, AwsError>> {
        U::upload_part(self, params, part_number, part)
    }

    fn complete_upload<'a, 'client: 'a>(
        &'client self,
        params: &'a UploadRequestParams,
        parts: &'a UploadedParts,
    ) -> BoxFuture<'a, Result<EntityTag, AwsError>> {
        U::complete_upload(self, params, parts)
    }

    fn on_upload_complete<'a, 'client: 'a>(
        &'client self,
        etag: &'a EntityTag,
    ) -> BoxFuture<'a, Result<(), AwsError>> {
        U::on_upload_complete(self, etag)
    }
}

/// An interface for managing the lifecycle of a multipart upload.
pub trait UploadControl {
    /// The desired part size in bytes.
    fn target_part_size(&self) -> usize;

    /// Whether one part in the upload is complete.
    fn is_part_ready(&self, part_size: usize) -> bool {
        part_size >= self.target_part_size()
    }

    /// Whether the overall upload is complete.
    fn is_upload_ready(&self, upload_size: usize, num_parts: usize) -> bool;
}

impl<C: UploadControl> UploadControl for Arc<C> {
    fn target_part_size(&self) -> usize {
        C::target_part_size(self)
    }

    fn is_upload_ready(&self, upload_size: usize, num_parts: usize) -> bool {
        C::is_upload_ready(self, upload_size, num_parts)
    }
}
