#![cfg_attr(docsrs, feature(doc_cfg))]
#![warn(missing_docs)]
//! # aws-multipart-upload
//!
//! A high-level API for building and working with AWS S3 multipart uploads using the official [SDK] for
//! Rust.
//!
//! ## Overview
//!
//! As explained in the [README][readme], the goal of this crate is to provide an API that simplifies
//! the process of performing S3 multipart uploads with abstractions that hide the tedious and precise
//! details, and in a way that is easily compatible with the more ubiquitous dependencies from the
//! ecosystem.
//!
//! The crate exports several types that implement the trait [`MultipartWrite`][multi-write], each being
//! an aspect of the multipart upload:
//!
//! * A buffer for polling part upload request futures.
//! * A type that creates part upload request objects, pushes them to such a buffer, and completes the
//!   upload when requested.
//! * An interface for encoding arbitrary values in the body of a part upload request.
//!
//! Combined with any [`SendRequest`], these components are collected in the type [`MultipartUpload`],
//! which is able to manage the end-to-end lifecycle of a single multipart upload, or a series of them
//! continuing indefinitely.  The capability is generally captured in the trait alias/abbreviation
//! [`AwsMultipartUpload`].
//!
//! Combinators from the `multipart-write` crate can be used to chain and compose types here.  The
//! extension traits [`UploadWriteExt`] and [`UploadStreamExt`] expand on this to allow creating
//! general `AwsMultipartUpload`s, futures for running uploads, and streams as an additional context
//! for a multipart upload.
//!
//! ## Example
//!
//! Generally, using the crate involves an [`UploadBuilder`] to configure several things:
//!
//! * The uploaded object itself where applicable: target size, part size, etc.
//! * A [`PartEncoder`][encoder] that specifies how values are written to parts.
//! * Some [`SendRequest`][upload-client], most likely an [`SdkClient`].
//!
//! Building this returns a [`MultipartUpload`], which can be used in a number of different ways.
//! The following example shows how a `MultipartUpload` can be used more manually, in that the upload
//! happens with explicit method calls.
//!
//! See the example in the [README][readme-eg] or the [examples][repo-eg] in the crate repository for
//! other uses.
//!
//! ```rust
//! # use aws_multipart_upload::{SendRequest, Status, ObjectUri, UploadBuilder, ByteSize};
//! # use aws_multipart_upload::codec::JsonLinesEncoder;
//! # use aws_multipart_upload::error::Result;
//! # use aws_multipart_upload::request::*;
//! # use multipart_write::MultipartWriteExt as _;
//! # use serde_json::{Value, json};
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
//! #         Ok(CompletedPart::new("".into(), "".into(), req.part_number(), req.body().size()))
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
//! use aws_multipart_upload::codec::JsonLinesEncoder;
//! use multipart_write::MultipartWriteExt as _;
//! use serde_json::{Value, json};
//! # }
//! # async fn f() -> aws_multipart_upload::error::Result<()> {
//!
//! // Build a default multipart upload client from `aws_sdk_s3::Client`.
//! //
//! // For convenience `aws_config` is re-exported, as is `aws_sdk_s3` under the
//! // symbol `aws_sdk`, for customization.
//! let client = SdkClient::defaults().await;
//!
//! // Use `UploadBuilder` to make a multipart upload with target size 20 MiB,
//! // target part size 5 MiB, and which writes incoming `serde_json::Value`s
//! // to parts as jsonlines.
//! let mut upl = UploadBuilder::new(client)
//!     .upload_size(ByteSize::mib(20))
//!     .part_size(ByteSize::mib(5))
//!     .with_encoder(JsonLinesEncoder)
//!     .with_uri(("a-bucket-us-east-1", "an/object/key.jsonl"))
//!     .build();
//!
//! // Now the uploader can have `serde_json::Value`s written to it to build a
//! // part of the upload.
//! //
//! // As parts reach the target size of 5 MiB, they'll be turned into a request
//! // body for a part upload and the request will be sent.
//! for n in 0..100000 {
//!     let item = json!({"k1": n, "k2": n.to_string()});
//!     let status = upl.send_part(item).await?;
//!     println!("bytes written to part: {}", status.part_bytes);
//!
//!     // We've reached target upload size:
//!     if status.should_upload {
//!         let res = upl.complete().await?;
//!         println!("created {} with entity tag {}", res.uri, res.etag);
//!         break;
//!     }
//! }
//! #     Ok(())
//! # }
//! ```
//!
//! [SDK]: https://awslabs.github.io/aws-sdk-rust/
//! [readme]: https://github.com/quasi-coherent/aws-multipart-upload/blob/master/README.md
//! [encoder]: self::codec::PartEncoder
//! [multi-write]: https://docs.rs/multipart-write/latest/multipart_write/
//! [`UploadWriteExt`]: self::write::UploadWriteExt
//! [`UploadStreamExt`]: self::write::UploadStreamExt
//! [readme-eg]: https://github.com/quasi-coherent/aws-multipart-upload/blob/master/README.md#Example
//! [repo-eg]: https://github.com/quasi-coherent/aws-multipart-upload/tree/master/examples
use self::request::PartBody;
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
pub use write::{AwsMultipartUpload, MultipartUpload, Status};

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
pub use uri::{ObjectUri, ObjectUriIter};

// https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html
const AWS_MAX_OBJECT_SIZE: ByteSize = ByteSize::tib(5);
const AWS_MIN_PART_SIZE: ByteSize = ByteSize::mib(5);
const AWS_MAX_PART_SIZE: ByteSize = ByteSize::gib(5);
const DEFAULT_MAX_OBJECT_SIZE: ByteSize = ByteSize::gib(5);
const DEFAULT_MAX_PART_SIZE: ByteSize = ByteSize::mib(10);

/// Configures and builds a type for multipart uploads.
#[derive(Debug)]
pub struct UploadBuilder<E = PartBody> {
    client: UploadClient,
    max_bytes: ByteSize,
    max_part_bytes: ByteSize,
    max_tasks: Option<usize>,
    encoder: E,
    iter: ObjectUriIter,
}

impl UploadBuilder {
    /// Create an `UploadBuilder` from a [`SendRequest`] client.
    pub fn new<C>(client: C) -> Self
    where
        C: SendRequest + 'static,
    {
        Self {
            client: UploadClient::new(client),
            max_bytes: DEFAULT_MAX_OBJECT_SIZE,
            max_part_bytes: DEFAULT_MAX_PART_SIZE,
            max_tasks: Some(10),
            encoder: PartBody::default(),
            iter: ObjectUriIter::new(EmptyUri),
        }
    }

    /// Set a builder for what will be used as an encoding for items going into
    /// a part in the multipart upload.
    pub fn with_encoder<E>(self, encoder: E) -> UploadBuilder<E> {
        UploadBuilder {
            client: self.client,
            max_bytes: self.max_bytes,
            max_part_bytes: self.max_part_bytes,
            max_tasks: self.max_tasks,
            encoder,
            iter: self.iter,
        }
    }
}

impl<E> UploadBuilder<E> {
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
            iter: ObjectUriIter::new(inner),
            ..self
        }
    }

    /// Use the iterator to start a new upload when one completes.
    pub fn with_uri_iter<I>(self, inner: I) -> Self
    where
        I: IntoIterator<Item = ObjectUri> + 'static,
    {
        let iter = ObjectUriIter::new(inner);
        Self { iter, ..self }
    }

    /// Build a `MultipartUpload` from this configuration.
    pub fn build(self) -> MultipartUpload<E> {
        let buf = PartBuffer::new(self.max_tasks);
        buf.upload(&self.client, self.iter).encoded_upload(
            self.encoder,
            self.max_bytes,
            self.max_part_bytes,
        )
    }
}
