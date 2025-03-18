pub mod aws;
pub mod hashmap;

use aws_sdk_s3::primitives::ByteStream;
use futures::future::{ready, BoxFuture};
use std::sync::Arc;

use crate::{
    types::{EntityTag, UploadAddress, UploadParams, UploadedParts},
    AwsError,
};

/// Operations in a multipart upload.
pub trait UploadClient {
    /// Create a new upload returning the ID of the upload.
    fn new_upload<'a, 'c: 'a>(
        &'c self,
        addr: &'a UploadAddress,
    ) -> BoxFuture<'a, Result<UploadParams, AwsError>>;

    /// Upload one part to the multipart upload.
    fn upload_part<'a, 'c: 'a>(
        &'c self,
        params: &'a UploadParams,
        part_number: i32,
        part: ByteStream,
    ) -> BoxFuture<'a, Result<EntityTag, AwsError>>;

    /// Complete the upload.
    fn complete_upload<'a, 'c: 'a>(
        &'c self,
        params: &'a UploadParams,
        parts: &'a UploadedParts,
    ) -> BoxFuture<'a, Result<EntityTag, AwsError>>;

    /// A callback with the entity tag returned by `upload_part` and the
    /// parameters used to call it.
    #[allow(unused_variables)]
    fn on_upload_part(
        &self,
        params: UploadParams,
        etag: EntityTag,
    ) -> BoxFuture<'_, Result<(), AwsError>> {
        Box::pin(ready(Ok(())))
    }

    /// A callback with the entity tag returned by `complete_upload` and the
    /// parameters used to call it.
    #[allow(unused_variables)]
    fn on_upload_complete(
        &self,
        params: UploadParams,
        etag: EntityTag,
    ) -> BoxFuture<'_, Result<(), AwsError>> {
        Box::pin(ready(Ok(())))
    }
}

impl<T: UploadClient> UploadClient for Arc<T> {
    fn new_upload<'a, 'c: 'a>(
        &'c self,
        addr: &'a UploadAddress,
    ) -> BoxFuture<'a, Result<UploadParams, AwsError>> {
        T::new_upload(self, addr)
    }

    fn upload_part<'a, 'c: 'a>(
        &'c self,
        params: &'a UploadParams,
        part_number: i32,
        part: ByteStream,
    ) -> BoxFuture<'a, Result<EntityTag, AwsError>> {
        T::upload_part(self, params, part_number, part)
    }

    fn complete_upload<'a, 'c: 'a>(
        &'c self,
        params: &'a UploadParams,
        parts: &'a UploadedParts,
    ) -> BoxFuture<'a, Result<EntityTag, AwsError>> {
        T::complete_upload(self, params, parts)
    }

    fn on_upload_part(
        &self,
        params: UploadParams,
        etag: EntityTag,
    ) -> BoxFuture<'_, Result<(), AwsError>> {
        T::on_upload_part(self, params, etag)
    }

    fn on_upload_complete(
        &self,
        params: UploadParams,
        etag: EntityTag,
    ) -> BoxFuture<'_, Result<(), AwsError>> {
        T::on_upload_complete(self, params, etag)
    }
}

/// The concrete implementations of [`UploadClient`] exposed by this crate do
/// not implement `on_upload_complete`.  You can implement this trait for some
/// type and use it with an existing client in `with_callback` from the
/// extension trait [`UploadClientExt`] in order to define custom behavior for
/// after an upload is completed.
pub trait OnUploadAction<T>
where
    T: UploadClient,
{
    fn on_upload_part<'a, 'c: 'a>(
        &'c self,
        client: &'a T,
        params: UploadParams,
        etag: EntityTag,
    ) -> BoxFuture<'a, Result<(), AwsError>>;

    fn on_upload_complete<'a, 'c: 'a>(
        &'c self,
        client: &'a T,
        params: UploadParams,
        etag: EntityTag,
    ) -> BoxFuture<'a, Result<(), AwsError>>;
}

/// Wrapped client with a type performing the complete upload callback.
pub struct WithCallback<T, F> {
    inner: T,
    callback: F,
}

impl<T, F> WithCallback<T, F> {
    pub fn new(inner: T, callback: F) -> Self {
        Self { inner, callback }
    }
}

impl<T, F> UploadClient for WithCallback<T, F>
where
    T: UploadClient + Send + Sync,
    F: OnUploadAction<T> + Send + Sync,
{
    fn new_upload<'a, 'c: 'a>(
        &'c self,
        addr: &'a UploadAddress,
    ) -> BoxFuture<'a, Result<UploadParams, AwsError>> {
        self.inner.new_upload(addr)
    }

    fn upload_part<'a, 'c: 'a>(
        &'c self,
        params: &'a UploadParams,
        part_number: i32,
        part: ByteStream,
    ) -> BoxFuture<'a, Result<EntityTag, AwsError>> {
        self.inner.upload_part(params, part_number, part)
    }

    fn complete_upload<'a, 'c: 'a>(
        &'c self,
        params: &'a UploadParams,
        parts: &'a UploadedParts,
    ) -> BoxFuture<'a, Result<EntityTag, AwsError>> {
        self.inner.complete_upload(params, parts)
    }

    fn on_upload_part(
        &self,
        params: UploadParams,
        etag: EntityTag,
    ) -> BoxFuture<'_, Result<(), AwsError>> {
        Box::pin(async move {
            self.callback
                .on_upload_part(&self.inner, params, etag)
                .await?;
            Ok(())
        })
    }

    fn on_upload_complete(
        &self,
        params: UploadParams,
        etag: EntityTag,
    ) -> BoxFuture<'_, Result<(), AwsError>> {
        Box::pin(async move {
            self.callback
                .on_upload_complete(&self.inner, params, etag)
                .await?;
            Ok(())
        })
    }
}

/// An extension trait adding convenience methods to existing [`UploadClient`]s.
pub trait UploadClientExt
where
    Self: UploadClient,
{
    /// An adapter that adds an implementation of `on_upload_complete` to an
    /// [`UploadClient`].
    fn with_callback<F>(self, callback: F) -> WithCallback<Self, F>
    where
        Self: Sized,
        F: OnUploadAction<Self>,
    {
        WithCallback::new(self, callback)
    }
}

impl<T> UploadClientExt for T where T: UploadClient {}
