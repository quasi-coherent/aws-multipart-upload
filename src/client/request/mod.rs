pub use crate::abort_upload::builders::AbortMultipartUploadFluentBuilder as AbortRequestBuilder;
pub use crate::complete_upload::builders::CompleteMultipartUploadFluentBuilder as CompleteRequestBuilder;
pub use crate::create_upload::builders::CreateMultipartUploadFluentBuilder as CreateRequestBuilder;
pub use crate::part_upload::builders::UploadPartFluentBuilder as UploadPartRequestBuilder;

mod abort;
pub use abort::{AbortRequest, SendAbortUpload};

mod complete;
pub use complete::{CompleteRequest, CompletedUpload, SendCompleteUpload};

mod create;
pub use create::{CreateRequest, SendCreateUpload};

mod upload_part;
pub use upload_part::{SendUploadPart, UploadPartRequest};

/// Add additional properties to the request objects being sent.
pub trait RequestBuilder {
    /// Set additional properties on [`CreateRequestBuilder`] beyond what
    /// [`CreateRequest`] provides.
    ///
    /// [`CreateRequest`]: self::create::CreateRequest
    fn with_create_builder(&self, builder: CreateRequestBuilder) -> CreateRequestBuilder {
        builder
    }

    /// Set additional properties on [`UploadPartRequestBuilder`] beyond what
    /// [`UploadPartRequest`] provides.
    ///
    /// [`UploadPartRequest`]: self::upload_part::UploadPartRequest
    fn with_upload_part_builder(
        &self,
        builder: UploadPartRequestBuilder,
    ) -> UploadPartRequestBuilder {
        builder
    }

    /// Set additional properties on [`CompleteRequestBuilder`] beyond what
    /// [`CompleteRequest`] provides.
    ///
    /// [`CompleteRequest`]: self::complete::CompleteRequest
    fn with_complete_builder(&self, builder: CompleteRequestBuilder) -> CompleteRequestBuilder {
        builder
    }

    /// Set additional properties on [`AbortRequestBuilder`] beyond what
    /// [`AbortRequest`] provides.
    ///
    /// [`AbortRequest`]: self::abort::AbortRequest
    fn with_abort_builder(&self, builder: AbortRequestBuilder) -> AbortRequestBuilder {
        builder
    }
}

/// Default implementation of [`RequestBuilder`] that doesn't modify the request
/// object at all.
#[derive(Debug, Clone, Copy, Default)]
pub struct DefaultRequestBuilder;
impl RequestBuilder for DefaultRequestBuilder {}
