#![cfg_attr(docsrs, feature(doc_cfg))]
#![warn(missing_docs)]

//! # Description
//!
//! A high-level crate for building and working with AWS S3 multipart uploads
//! using the official [SDK] for Rust.
//!
//! # Examples
//!
//! ```rust
//! # use aws_multipart_upload::{SendRequest, UploadData, UploadSize};
//! # use aws_multipart_upload::error::Result;
//! # use aws_multipart_upload::request::*;
//! # use aws_multipart_upload::ObjectUri;
//! # use serde_json::Value;
//! # use std::sync::{Arc, RwLock};
//! # #[derive(Default)]
//! # struct SdkClient(Arc<RwLock<Vec<PartBody>>>);
//! # impl SdkClient { async fn defaults() -> Self { Self::default() } }
//! # impl SendRequest for SdkClient {
//! #     async fn send_create_upload_request(&self, req: CreateRequest) -> Result<UploadData> {
//! #         Ok(UploadData::new("example-upload-id", req.uri().clone()))
//! #     }
//! #     async fn send_new_part_upload_request(&self, req: UploadPartRequest) -> Result<CompletedPart> {
//! #         let mut inner = self.0.write().unwrap();
//! #         inner.push(req.body().clone());
//! #         Ok(CompletedPart::new("".into(), req.part_number(), req.body().size()))
//! #     }
//! #     async fn send_complete_upload_request(&self, req: CompleteRequest) -> Result<CompletedUpload> {
//! #         Ok(CompletedUpload::new(req.uri().clone(), "".into()))
//! #     }
//! #     async fn send_abort_upload_request(&self, _: AbortRequest) -> Result<()> {
//! #         Ok(())
//! #     }
//! # }
//! # mod __m {
//! use aws_multipart_upload::SdkClient;
//! use aws_multipart_upload::request::CompletedUpload;
//! # }
//! # async fn f() -> aws_multipart_upload::error::Result<()> {
//! use aws_multipart_upload::codec::JsonLinesEncoderBuilder;
//! use serde_json::{Value, json};
//!
//! // Build a default multipart upload client from `aws_sdk_s3::Client`.
//! //
//! // Can also be built with `SdkClient::from_loader` and an
//! // `aws_config::ConfigLoader` or with `SdkClient::new` and an existing
//! // `aws_sdk_s3::Client`.
//! //
//! // For convenience `aws_config` is re-exported, as is `aws_sdk_s3` under the
//! // symbol `aws_sdk`.
//! let client = SdkClient::defaults().await;
//!
//! #     Ok(())
//! # }
//! ```
//!
//! [SDK]: https://awslabs.github.io/aws-sdk-rust/
use aws_sdk::operation::abort_multipart_upload as abort_upload;
use aws_sdk::operation::complete_multipart_upload as complete_upload;
use aws_sdk::operation::create_multipart_upload as create_upload;
use aws_sdk::operation::upload_part as part_upload;

#[doc(hidden)]
pub extern crate aws_config;
#[doc(hidden)]
pub extern crate aws_sdk_s3 as aws_sdk;

#[doc(no_inline)]
pub use multipart_write::{
    FusedMultipartWrite, MultipartStreamExt, MultipartWrite, MultipartWriteExt,
};

#[macro_use]
mod trace;

mod client;
pub use client::{SdkClient, SendRequest, UploadClient, UploadData};

pub mod codec;
pub mod error;

pub mod prelude {
    //! Collects and re-exports methods of commonly used traits.
    //!
    //! Import this in its entirety to bring these methods into scope:
    //!
    //! ```rust
    //! use aws_multipart_upload::prelude::*;
    //! ```
    #[doc(no_inline)]
    #[allow(unreachable_pub)]
    pub use crate::upload::MultipartUploadWriterExt as _;
    #[doc(no_inline)]
    #[allow(unreachable_pub)]
    pub use crate::uri::ObjectUriIterExt as _;
    pub use multipart_write::{FusedMultipartWrite, MultipartWrite};
    #[doc(no_inline)]
    #[allow(unreachable_pub)]
    pub use multipart_write::{MultipartStreamExt as _, MultipartWriteExt as _};
}

pub mod request {
    //! Request interface of the multipart upload API.
    //!
    //! This module contains the trait [`RequestBuilder`] for customizing the
    //! request object sent for a multipart upload operation, futures that
    //! represent sending the request, and types appearing in request or response
    //! objects.
    pub use super::client::UploadId;
    pub use super::client::part::*;
    pub use super::client::request::*;
}

mod upload;
pub use upload::{PartBuffer, Status, UploadSent, Uploader, UploaderWithUri, WithPartEncoder};

pub mod uri;
#[doc(inline)]
pub use uri::{NewObjectUri, ObjectUri};

// https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html
const AWS_MAX_OBJECT_SIZE: usize = 5 * 1024 * 1024 * 1024 * 1024;
const AWS_MIN_PART_SIZE: usize = 5 * 1024 * 1024;
const AWS_MAX_PART_SIZE: usize = 5 * 1024 * 1024 * 1024;
const DEFAULT_MAX_OBJECT_SIZE: usize = 5 * 1024 * 1024 * 1024;
const DEFAULT_MAX_PART_SIZE: usize = 10 * 1024 * 1024;

/// Create a `PartBuffer` to buffer upload request futures.
///
/// The `capacity` limits the number of active request futures that can exist at
/// one time.  A capacity of `None` or `Some(0)` means no limit.
pub fn part_buffer<T: Into<Option<usize>>>(capacity: T) -> PartBuffer {
    PartBuffer::new(capacity.into())
}

/// Configuring the size of the upload.
#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub struct UploadConfig {
    /// Maximum size of the upload in bytes.
    ///
    /// Defaults to 5GiB.
    pub max_bytes: usize,
    /// Maximum size of a part in bytes.
    ///
    /// Must be at least 5MiB.  Defaults to 10MiB.
    pub max_part_bytes: usize,
    /// Set a limit to the number of active request futures.
    ///
    /// `None` or `Some(0)` does not impose a limit.  The default is 10.
    pub max_tasks: Option<usize>,
}

impl UploadConfig {
    /// Create an `UploadConfig` with all default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// `UploadConfig` with target total size in MiB and default part size.
    pub fn total_mb(n: usize) -> Self {
        Self {
            max_bytes: n * 1024 * 1024,
            ..Default::default()
        }
    }

    /// Set the maximum number of bytes in the upload.
    pub fn with_bytes(self, limit: usize) -> Self {
        let max_bytes = std::cmp::min(limit, AWS_MAX_OBJECT_SIZE);
        Self { max_bytes, ..self }
    }

    /// Set the maximum number of bytes in a part.
    pub fn with_part_bytes(self, limit: usize) -> Self {
        let limit = std::cmp::max(limit, AWS_MIN_PART_SIZE);
        let max_part_bytes = std::cmp::min(limit, AWS_MAX_PART_SIZE);
        Self {
            max_part_bytes,
            ..self
        }
    }

    /// Set the maximum number of active request futures allowed at one time.
    pub fn max_active_tasks(self, limit: usize) -> Self {
        Self {
            max_tasks: Some(limit),
            ..self
        }
    }
}

impl Default for UploadConfig {
    fn default() -> Self {
        Self {
            max_bytes: DEFAULT_MAX_OBJECT_SIZE,
            max_part_bytes: DEFAULT_MAX_PART_SIZE,
            max_tasks: Some(10),
        }
    }
}
