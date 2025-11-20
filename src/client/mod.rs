use self::part::CompletedPart;
use self::request::*;
use crate::create_upload::CreateMultipartUploadOutput as CreateResponse;
use crate::error::{ErrorRepr, Result};
use crate::uri::ObjectUri;

use futures::future::LocalBoxFuture;
use std::borrow::Cow;
use std::fmt::{self, Formatter};
use std::ops::Deref;
use std::sync::Arc;

pub mod part;
pub mod request;
mod sdk;
pub use sdk::SdkClient;

/// `SendRequest` represents the atomic operations in a multipart upload.
pub trait SendRequest {
    /// Send a request to create a new multipart upload, returning an
    /// [`UploadData`] having the upload ID assignment.
    fn send_create_upload_request(
        &self,
        req: CreateRequest,
    ) -> impl Future<Output = Result<UploadData>>;

    /// Send a request to upload a part to a multipart upload, returning the
    /// [`CompletedPart`] containing entity tag and part number, which are required
    /// in the subsequent complete upload request.
    fn send_new_part_upload_request(
        &self,
        req: UploadPartRequest,
    ) -> impl Future<Output = Result<CompletedPart>>;

    /// Send a request to complete a multipart upload, returning a
    /// [`CompletedUpload`], which has the unique entity tag of the object as well
    /// as the object URI.
    fn send_complete_upload_request(
        &self,
        req: CompleteRequest,
    ) -> impl Future<Output = Result<CompletedUpload>>;

    /// Send a request to abort a multipart upload returning an empty response if
    /// successful.
    fn send_abort_upload_request(&self, req: AbortRequest) -> impl Future<Output = Result<()>>;
}

impl<D, T> SendRequest for T
where
    D: SendRequest,
    T: Deref<Target = D>,
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

    async fn send_abort_upload_request(&self, req: AbortRequest) -> Result<()> {
        self.deref().send_abort_upload_request(req).await
    }
}

/// A client of the multipart upload API.
///
/// This can be built from any type that implements `SendRequest`, such as the
/// [`SdkClient`].
#[derive(Clone)]
pub struct UploadClient {
    pub(crate) inner: Arc<dyn BoxedSendRequest>,
}

impl UploadClient {
    /// Create a new `UploadClient`.
    pub fn new<C>(client: C) -> Self
    where
        C: SendRequest + 'static,
    {
        let inner = SendRequestInner::new(client);
        Self {
            inner: Arc::new(inner),
        }
    }
}

impl SendRequest for UploadClient {
    async fn send_create_upload_request(&self, req: CreateRequest) -> Result<UploadData> {
        self.inner.send_create_upload(req).await
    }

    async fn send_new_part_upload_request(&self, req: UploadPartRequest) -> Result<CompletedPart> {
        self.inner.send_upload_part(req).await
    }

    async fn send_complete_upload_request(&self, req: CompleteRequest) -> Result<CompletedUpload> {
        self.inner.send_complete_upload(req).await
    }

    async fn send_abort_upload_request(&self, req: AbortRequest) -> Result<()> {
        self.inner.send_abort_upload(req).await
    }
}

impl fmt::Debug for UploadClient {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("UploadClient")
            .field("inner", &"SendRequest")
            .finish()
    }
}

/// ID assigned by AWS for this upload.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct UploadId(Cow<'static, str>);

impl UploadId {
    pub(crate) fn new<T: Into<Cow<'static, str>>>(id: T) -> Self {
        Self(id.into())
    }

    pub(crate) fn try_from_create_resp(value: &CreateResponse) -> Result<Self, ErrorRepr> {
        value
            .upload_id
            .as_deref()
            .map(Self::from)
            .ok_or_else(|| ErrorRepr::Missing("CreateResponse", "upload_id"))
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl Deref for UploadId {
    type Target = str;

    fn deref(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for UploadId {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl From<&str> for UploadId {
    fn from(value: &str) -> Self {
        Self::new(value.to_string())
    }
}

impl From<String> for UploadId {
    fn from(value: String) -> Self {
        Self(Cow::Owned(value))
    }
}

/// Data identifying a multipart upload.
///
/// The `UploadId` assigned by AWS and the `ObjectUri` that the user created the
/// upload with are required properties of any of the upload client's operations.
///
/// The [`SendCreateUpload`] request future resolves to this type if the request
/// was successful.
///
/// [`SendCreateUpload`]: self::request::SendCreateUpload
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct UploadData {
    /// The ID for the upload assigned by AWS.
    pub id: UploadId,
    /// The S3 URI of the object being uploaded.
    pub uri: ObjectUri,
}

impl UploadData {
    /// Create a new value from an upload ID and object URI.
    pub fn new<T, U>(id: T, uri: U) -> Self
    where
        T: Into<UploadId>,
        U: Into<ObjectUri>,
    {
        Self {
            id: id.into(),
            uri: uri.into(),
        }
    }

    /// Get an owned upload ID.
    pub fn get_id(&self) -> UploadId {
        self.id.clone()
    }

    /// Get an owned object URI.
    pub fn get_uri(&self) -> ObjectUri {
        self.uri.clone()
    }
}

/// Object-safe `SendRequest`.
pub(crate) trait BoxedSendRequest {
    fn send_create_upload(&self, req: CreateRequest) -> LocalBoxFuture<'_, Result<UploadData>>;

    fn send_upload_part(&self, req: UploadPartRequest)
    -> LocalBoxFuture<'_, Result<CompletedPart>>;

    fn send_complete_upload(
        &self,
        req: CompleteRequest,
    ) -> LocalBoxFuture<'_, Result<CompletedUpload>>;

    fn send_abort_upload(&self, req: AbortRequest) -> LocalBoxFuture<'_, Result<()>>;
}

/// Implements `BoxedSendRequest` for any `T: SendRequest` so that we can
/// construct `UploadClient`.
struct SendRequestInner<T>(T);

impl<T: SendRequest> SendRequestInner<T> {
    pub(super) fn new(inner: T) -> Self {
        Self(inner)
    }
}

impl<T: SendRequest> BoxedSendRequest for SendRequestInner<T> {
    fn send_create_upload(&self, req: CreateRequest) -> LocalBoxFuture<'_, Result<UploadData>> {
        Box::pin(self.0.send_create_upload_request(req))
    }

    fn send_upload_part(
        &self,
        req: UploadPartRequest,
    ) -> LocalBoxFuture<'_, Result<CompletedPart>> {
        Box::pin(self.0.send_new_part_upload_request(req))
    }

    fn send_complete_upload(
        &self,
        req: CompleteRequest,
    ) -> LocalBoxFuture<'_, Result<CompletedUpload>> {
        Box::pin(self.0.send_complete_upload_request(req))
    }

    fn send_abort_upload(&self, req: AbortRequest) -> LocalBoxFuture<'_, Result<()>> {
        Box::pin(self.0.send_abort_upload_request(req))
    }
}
