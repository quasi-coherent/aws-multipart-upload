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
//! # use aws_multipart_upload::{SendRequest, UploadData, Status, ObjectUri};
//! # use aws_multipart_upload::error::Result;
//! # use aws_multipart_upload::request::*;
//! # use serde_json::Value;
//! # use std::sync::{Arc, RwLock};
//! # #[derive(Default)]
//! # struct SdkClient(Arc<RwLock<Vec<PartBody>>>);
//! # impl SdkClient { async fn defaults() -> Self { Self::default() } }
//! # impl SendRequest for SdkClient {
//! #     async fn send_create_upload_request(&self, req: CreateRequest) -> Result<UploadData> {
//! #         Ok(UploadData::new("", req.uri().clone()))
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
//! use aws_multipart_upload::{ByteSize, SdkClient, UploadBuilder};
//! # }
//! # async fn f() -> aws_multipart_upload::error::Result<()> {
//! use aws_multipart_upload::codec::{JsonLinesBuilder, JsonLinesEncoder};
//! use aws_multipart_upload::prelude::*;
//! use serde_json::{Value, json};
//!
//! /// Build a default multipart upload client from `aws_sdk_s3::Client`.
//! ///
//! /// For convenience `aws_config` is re-exported, as is `aws_sdk_s3` under the
//! /// symbol `aws_sdk`.
//! let client = SdkClient::defaults().await;
//!
//! /// Use `UploadBuilder` to make a multipart upload with target size 20 MiB,
//! /// target part size 5 MiB, and which writes incoming `serde_json::Value`s
//! /// to parts as jsonlines.
//! let mut uploader = UploadBuilder::new(client)
//!     .upload_size(ByteSize::mib(20))
//!     .part_size(ByteSize::mib(5))
//!     .encoding(JsonLinesBuilder)
//!     .with_uri(("a-bucket-us-east-1", "an/object/key.jsonl"))
//!     .build::<Value, JsonLines>();
//!
//! /// Now the uploader can have `serde_json::Value`s written to it to build a
//! /// part of the upload.
//! ///
//! /// As parts reach the target size of 5 MiB, they'll be turned into a request
//! /// body for a part upload and the request will be sent.
//! for n in 0..100000 {
//!     let item = json!({"k1": n, "k2": n.to_string()});
//!     let status = uploader.send_part(item).await?;
//!     println!("current part size: {}", status.part_size);
//!
//!     // We've reached target upload size:
//!     if status.should_upload {
//!         let res = uploader.complete().await?;
//!         println!("created {} with entity tag {}", res.uri, res.etag);
//!         break;
//!     }
//! }
//! #     Ok(())
//! # }
//! ```
//!
//! [SDK]: https://awslabs.github.io/aws-sdk-rust/
use self::codec::{JsonLinesBuilder, PartEncoder};
use self::uri::EmptyUri;
use self::write::{PartBuffer, UploadWriteExt};

use aws_sdk::operation::abort_multipart_upload as abort_upload;
use aws_sdk::operation::complete_multipart_upload as complete_upload;
use aws_sdk::operation::create_multipart_upload as create_upload;
use aws_sdk::operation::upload_part as part_upload;

#[doc(hidden)]
pub extern crate aws_config;
#[doc(hidden)]
pub extern crate aws_sdk_s3 as aws_sdk;

pub use bytesize::ByteSize;

#[macro_use]
mod trace;

mod client;
pub use client::{SdkClient, SendRequest, UploadClient};

pub mod codec;
pub mod error;

pub mod write;
#[doc(inline)]
pub use write::{MultipartUpload, Status};

pub mod prelude {
    //! Collects and re-exports methods of commonly used traits.
    //!
    //! Import this in its entirety to bring these methods into scope:
    //!
    //! ```rust
    //! use aws_multipart_upload::prelude::*;
    //! ```
    #[allow(unreachable_pub)]
    pub use crate::uri::ObjectUriIterExt as _;
    #[allow(unreachable_pub)]
    pub use crate::write::{UploadStreamExt as _, UploadWriteExt as _};
    pub use multipart_write::{FusedMultipartWrite, MultipartWrite};
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
    pub use super::client::part::*;
    pub use super::client::request::*;
    pub use super::client::{UploadData, UploadId};
}

pub mod uri;
#[doc(inline)]
pub use uri::{NewObjectUri, ObjectUri};

// https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html
const AWS_MAX_OBJECT_SIZE: ByteSize = ByteSize::tib(5);
const AWS_MIN_PART_SIZE: ByteSize = ByteSize::mib(5);
const AWS_MAX_PART_SIZE: ByteSize = ByteSize::gib(5);
const DEFAULT_MAX_OBJECT_SIZE: ByteSize = ByteSize::gib(5);
const DEFAULT_MAX_PART_SIZE: ByteSize = ByteSize::mib(10);

/// Configures and builds a type for multipart uploads.
#[derive(Debug)]
#[non_exhaustive]
pub struct UploadBuilder<B = JsonLinesBuilder> {
    client: UploadClient,
    max_bytes: ByteSize,
    max_part_bytes: ByteSize,
    max_tasks: Option<usize>,
    builder: B,
    iter: NewObjectUri,
}

impl UploadBuilder {
    /// Create a `UploadBuilder` from a [`SendRequest`] client.
    pub fn new<C>(client: C) -> Self
    where
        C: SendRequest + 'static,
    {
        Self {
            client: UploadClient::new(client),
            max_bytes: DEFAULT_MAX_OBJECT_SIZE,
            max_part_bytes: DEFAULT_MAX_PART_SIZE,
            max_tasks: Some(10),
            builder: JsonLinesBuilder,
            iter: NewObjectUri::uri_iter(EmptyUri),
        }
    }

    /// Set a builder for what will be used as an encoding for items going into
    /// a part in the multipart upload.
    pub fn with_encoding<B>(self, builder: B) -> UploadBuilder<B> {
        UploadBuilder {
            client: self.client,
            max_bytes: self.max_bytes,
            max_part_bytes: self.max_part_bytes,
            max_tasks: self.max_tasks,
            builder,
            iter: self.iter,
        }
    }
}

impl<B> UploadBuilder<B> {
    /// Set the target size of the upload.
    pub fn upload_size(self, limit: ByteSize) -> Self {
        Self {
            max_bytes: limit.min(AWS_MAX_OBJECT_SIZE),
            ..self
        }
    }

    /// Set the target size of a part.
    pub fn part_size(self, limit: ByteSize) -> Self {
        Self {
            // Clamp to AWS_MIN <= max_part_bytes <= min(AWS_MAX, usize::MAX).
            max_part_bytes: limit
                .max(AWS_MIN_PART_SIZE)
                .min(AWS_MAX_PART_SIZE)
                .min(ByteSize::b(usize::MAX as u64)),
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

    /// Set the destination object URI for a single upload.
    ///
    /// The resulting `MultipartUpload` is only one-time-use.
    pub fn with_uri<T: Into<ObjectUri>>(self, uri: T) -> Self {
        let inner = uri::OneTimeUse::new(uri.into());
        Self {
            iter: NewObjectUri::uri_iter(inner),
            ..self
        }
    }

    /// Use the iterator to start a new upload when one completes.
    pub fn with_uri_iter(self, iter: NewObjectUri) -> Self {
        Self { iter, ..self }
    }

    /// Build a [`MultipartUpload`] from this configuration.
    pub fn build<Item, E>(self) -> MultipartUpload<Item, E>
    where
        E: PartEncoder<Item, Builder = B>,
    {
        let buf = PartBuffer::new(self.max_tasks);
        buf.upload(&self.client, self.iter).encoded_upload(
            self.builder,
            self.max_bytes,
            self.max_part_bytes,
        )
    }
}
