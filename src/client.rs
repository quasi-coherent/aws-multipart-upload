pub mod aws;
pub mod fs;
pub mod hashmap;

use aws_sdk_s3::primitives::ByteStream;
use futures::future::BoxFuture;

use crate::{
    types::{api::*, UploadClient},
    AwsError,
};

/// The concrete implementations of `UploadClient` exposed by this crate do not
/// implement `on_upload_complete`.  To add a custom callback to be called after
/// an upload was completed, you can implement this trait to add to an existing
/// client and use with the extension trait below.
pub trait OnComplete<U>
where
    U: UploadClient,
{
    fn on_upload_complete<'a, 'c: 'a>(
        &'c self,
        client: &'a U,
        etag: EntityTag,
    ) -> BoxFuture<'a, Result<(), AwsError>>;
}

/// A wrapper of an upload client that implements the one provided method.
pub struct WithCallback<U, F> {
    inner: U,
    callback: F,
}

impl<U, F> WithCallback<U, F> {
    pub fn new(inner: U, callback: F) -> Self {
        Self { inner, callback }
    }
}

impl<U, F> UploadClient for WithCallback<U, F>
where
    U: UploadClient + Send + Sync,
    F: OnComplete<U> + Send + Sync,
{
    fn new_upload<'a, 'client: 'a>(
        &'client self,
        addr: &'a UploadAddress,
    ) -> BoxFuture<'a, Result<UploadRequestParams, AwsError>> {
        self.inner.new_upload(addr)
    }

    fn upload_part<'a, 'client: 'a>(
        &'client self,
        params: &'a UploadRequestParams,
        part_number: i32,
        part: ByteStream,
    ) -> BoxFuture<'a, Result<EntityTag, AwsError>> {
        self.inner.upload_part(params, part_number, part)
    }

    fn complete_upload<'a, 'client: 'a>(
        &'client self,
        params: &'a UploadRequestParams,
        parts: &'a UploadedParts,
    ) -> BoxFuture<'a, Result<EntityTag, AwsError>> {
        self.inner.complete_upload(params, parts)
    }

    fn on_upload_complete<'a>(&'a self, etag: EntityTag) -> BoxFuture<'a, Result<(), AwsError>> {
        Box::pin(async move {
            self.callback.on_upload_complete(&self.inner, etag).await?;
            Ok(())
        })
    }
}

impl<U> UploadClientExt for U where U: UploadClient {}

/// An extension trait adding convenience methods to existing `UploadClient`s.
pub trait UploadClientExt
where
    Self: UploadClient,
{
    /// Adds a custom implementation of the default method `on_upload_complete`
    /// adding specific handling of the entity tag of the completed upload in
    /// case it is required for some functionality.
    fn with_callback<F>(self, callback: F) -> WithCallback<Self, F>
    where
        Self: Sized,
        F: OnComplete<Self>,
    {
        WithCallback::new(self, callback)
    }
}
