//! This module contains `SendRequest`, which defines the core
//! operations needed during a multipart upload.
use self::inner::{BoxedSendRequest, SendRequestInner};
use crate::error::Result;
use crate::sdk::api::*;
use crate::sdk::{CompletedPart, CompletedUpload, UploadData};

use futures::future::Future;
use std::fmt::{self, Debug, Formatter};
use std::ops::Deref;
use std::sync::Arc;

mod future;
pub use future::{SendCompleteUpload, SendCreateUpload, SendUploadPart};

mod inner;

mod sdk;
pub use sdk::SdkClient;

/// `SendRequest` represents the atomic operations in a multipart upload.
pub trait SendRequest: Send + Sync {
    /// Send a request to create a new multipart upload, returning an
    /// [`UploadData`] having the upload ID assignment.
    fn send_create_upload_request(
        &self,
        req: CreateRequest,
    ) -> impl Future<Output = Result<UploadData>> + Send;

    /// Send a request to upload a part to a multipart upload, returning the
    /// [`CompletedPart`] containing entity tag and part number, which are required
    /// in the subsequent complete upload request.
    fn send_new_part_upload_request(
        &self,
        req: UploadPartRequest,
    ) -> impl Future<Output = Result<CompletedPart>> + Send;

    /// Send a request to complete a multipart upload, returning a
    /// [`CompletedUpload`], which has the unique entity tag of the object as well
    /// as the object URI.
    fn send_complete_upload_request(
        &self,
        req: CompleteRequest,
    ) -> impl Future<Output = Result<CompletedUpload>> + Send;
}

impl<D, T> SendRequest for T
where
    D: SendRequest,
    T: Deref<Target = D> + Send + Sync,
{
    async fn send_create_upload_request(&self, req: CreateRequest) -> Result<UploadData> {
        self.deref().send_create_upload_request(req).await
    }

    async fn send_new_part_upload_request(&self, req: UploadPartRequest) -> Result<CompletedPart> {
        self.deref().send_new_part_upload_request(req).await
    }

    async fn send_complete_upload_request(&self, req: CompleteRequest) -> Result<CompletedUpload> {
        self.deref().send_complete_upload_request(req).await
    }
}

/// `RequestBuilder` provides an SDK request object to specify additional
/// properties of the object when it is uploaded.
///
/// The [`SdkClient`] can be built with a `RequestBuilder` to have this
/// customization applied.
pub trait RequestBuilder: Send + Sync {
    /// Set additional properties on [`CreateRequestBuilder`] beyond what
    /// [`CreateRequest`] provides.
    fn with_create_builder(&self, builder: CreateRequestBuilder) -> CreateRequestBuilder {
        builder
    }

    /// Set additional properties on [`UploadPartRequestBuilder`] beyond what
    /// [`UploadPartRequest`] provides.
    fn with_upload_part_builder(
        &self,
        builder: UploadPartRequestBuilder,
    ) -> UploadPartRequestBuilder {
        builder
    }

    /// Set additional properties on [`CompleteRequestBuilder`] beyond what
    /// [`CompleteRequest`] provides.
    fn with_complete_builder(&self, builder: CompleteRequestBuilder) -> CompleteRequestBuilder {
        builder
    }
}

/// Default implementations of [`RequestBuilder`].
#[derive(Debug, Clone, Copy, Default)]
pub struct NullRequestBuilder;
impl RequestBuilder for NullRequestBuilder {}

/// `UploadClient` holds a type that can implement the interface of
/// [`SendRequest`].
#[derive(Clone)]
pub struct UploadClient {
    pub(crate) inner: Arc<dyn BoxedSendRequest + Send + Sync>,
}

impl UploadClient {
    /// Create this value where `NullRequestBuilder` is used to modify the
    /// request objects, which is to say that it does not modify the request
    /// objects.
    pub fn new<C>(client: C) -> Self
    where
        C: SendRequest + Send + Sync + 'static,
    {
        let inner = SendRequestInner::new(client);
        Self {
            inner: Arc::new(inner),
        }
    }
}

impl SendRequest for UploadClient {
    async fn send_create_upload_request(&self, req: CreateRequest) -> Result<UploadData> {
        self.inner.send_create(req).await
    }

    async fn send_new_part_upload_request(&self, req: UploadPartRequest) -> Result<CompletedPart> {
        self.inner.send_upload(req).await
    }

    async fn send_complete_upload_request(&self, req: CompleteRequest) -> Result<CompletedUpload> {
        self.inner.send_complete(req).await
    }
}

impl Debug for UploadClient {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("UploadClient")
            .field("inner", &"SendRequest")
            .finish()
    }
}
