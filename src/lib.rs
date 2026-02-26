#![cfg_attr(docsrs, feature(doc_cfg))]
#![warn(missing_docs)]
//! # aws-multipart-upload
//!
//! A high-level API for building and working with AWS S3 multipart uploads
//! using the official [SDK] for Rust.
//!
//! ## Overview
//!
//! As explained in the [README][readme], the goal of this crate is to provide
//! an API that simplifies the process of performing S3 multipart uploads with
//! abstractions that hide the tedious and precise details, and in a way that is
//! easily compatible with the more ubiquitous dependencies from the ecosystem.
//!
//! In general, the crate provides:
//!
//! * An abstract interface [`UploadApi`] representing the atomic operations of
//!   a multipart upload, and a stock implmentation for the AWS SDK S3 client in
//!   [`SdkClient`] that can be extended.
//! * Convenience methods for statically or dynamically constructing one or more
//!   [`ObjectUri`]s, which is the destination address of an upload and the only
//!   required value to initialize one.
//! * The module [`codec`], defining an API for writing arbitrary values with a
//!   specific encoding to the body of a part upload.
//! * The main type, [`Upload`], which is a realization of
//!   [`MultipartWrite`][multi-write] in an AWS S3 multipart upload.
//! * Combinators for using with streams and futures.
//!
//! ## Examples
//!
//! ```rust,no_run
//! use aws_multipart_upload::{ByteSize, SdkClient, UploadBuilder};
//! use aws_multipart_upload::codec::JsonLinesEncoder;
//! use aws_multipart_upload::stream::UploadStreamExt as _;
//! use futures::stream::{self, StreamExt as _};
//!
//! async fn upload_stream() {
//!     // Build a default multipart upload client.
//!     //
//!     // For convenience `aws_config` is re-exported, as is `aws_sdk_s3` under the
//!     // symbol `aws_sdk`, for customization.
//!     let client = SdkClient::defaults().await;
//!
//!     // Use `UploadBuilder` to make a multipart upload with a target part size of
//!     // 5 MiB, which writes incoming `serde_json::Value`s as lines of JSON.
//!     let upl = UploadBuilder::new(client)
//!         .with_part_size(ByteSize::mib(5))
//!         .with_encoder(JsonLinesEncoder::new())
//!         .with_uri(("a-bucket-us-east-1", "an/object/key.jsonl"))
//!         .build();
//!
//!     // Now the uploader can have `serde_json::Value`s written to it to build a
//!     // part of the upload. As parts reach the target size of 5 MiB, they'll be
//!     // turned into a part upload request and the request will be sent.
//!     //
//!     // The combinator `collect_upload` combines this uploader with a streaming
//!     // source.  The result is a future that, when awaited, runs the stream to
//!     // exhaustion, uploading the parts and sending a request to complete the
//!     // upload when the stream has stopped producing.
//!     let out = stream::iter(0..100000)
//!         .map(|n| serde_json::json!({"k1": n, "k2": n.to_string()}))
//!         .collect_upload(upl)
//!         .await
//!         .unwrap();
//!
//!     println!("uploaded {} bytes to {}", out.bytes, out.uri);
//! }
//! ```
//!
//! [SDK]: https://awslabs.github.io/aws-sdk-rust/
//! [readme]: https://github.com/quasi-coherent/aws-multipart-upload/blob/master/README.md
//! [encoder]: self::codec::PartEncoder
//! [multi-write]: https://docs.rs/multipart-write/latest/multipart_write/
//! [readme-eg]: https://github.com/quasi-coherent/aws-multipart-upload/blob/master/README.md#Example
//! [repo-eg]: https://github.com/quasi-coherent/aws-multipart-upload/tree/master/examples
use aws_sdk::operation::{
    abort_multipart_upload as abort_upload,
    complete_multipart_upload as complete_upload,
    create_multipart_upload as create_upload, upload_part as part_upload,
};

#[doc(hidden)]
pub extern crate aws_config;
#[doc(hidden)]
pub extern crate aws_sdk_s3 as aws_sdk;

pub use bytesize::ByteSize;

#[macro_use]
mod trace;

mod client;
pub use client::{SdkClient, UploadApi, UploadClient};

pub mod codec;
pub mod error;

pub mod request {
    //! Request interface of the multipart upload API.
    //!
    //! This module contains the trait [`RequestBuilder`] for customizing the
    //! request object sent for a multipart upload operation. It also defines
    //! futures representing the response, and types appearing in both values.
    pub use super::client::part::*;
    pub use super::client::request::*;
    pub use super::client::{UploadData, UploadId};
}

mod upload;
pub use upload::{Upload, UploadBuilder, UploadStatus, Uploaded, stream};

pub mod uri;
#[doc(inline)]
pub use uri::{ObjectUri, ObjectUriIter};
