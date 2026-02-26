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
//! * An abstract [`UploadApi`] representing atomic operations of a multipart
//!   upload, and a stock implementation for the AWS SDK S3 client in the value
//!   [`SdkClient`] that can be extended.
//! * Convenience methods for statically or dynamically constructing one or more
//!   [`ObjectUri`]s, which is the destination address of an upload and the only
//!   required value to initialize one.
//! * The module [`encoder`], which defines a way to write values to the body of
//!   a part upload in the trait [`PartEncoder`], and provides part encoders for
//!   jsonlines and CSV.
//! * The main types, [`Upload`] and [`EncodeUpload`], which realize the trait
//!   [`MultipartWrite`] for writing parts of a whole asynchronously as an AWS
//!   S3 multipart upload.
//! * Combinators to compose and combine uploads with other multipart writers,
//!   streams, and futures.
//!
//! ## Examples
//!
//! ```rust,no_run
//! use aws_multipart_upload::{ByteSize, SdkClient, UploadBuilder};
//! use aws_multipart_upload::encoder::JsonLinesEncoder;
//! use aws_multipart_upload::prelude::*;
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
//!         .with_uri(("a-bucket-us-east-1", "an/object/key.jsonl"))
//!         .build_upload_from(JsonLinesEncoder::new());
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
//! [`PartEncoder`]: self::encoder::PartEncoder
//! [`MultipartWrite`]: https://docs.rs/multipart-write/latest/multipart_write/
//! [`Upload`]: self::upload::Upload
//! [`EncodeUpload`]: self::upload::EncodeUpload
use aws_sdk::operation::{
    abort_multipart_upload as abort_upload,
    complete_multipart_upload as complete_upload,
    create_multipart_upload as create_upload, upload_part as part_upload,
};

#[doc(hidden)]
pub extern crate aws_config;
#[doc(hidden)]
pub extern crate aws_sdk_s3 as aws_sdk;
pub extern crate multipart_write;

pub use bytesize::ByteSize;

mod client;
pub use client::{SdkClient, UploadApi, UploadClient};

pub mod encoder;
pub mod error;

pub mod prelude {
    //! Re-export of useful extension traits without the trait symbol.
    //!
    //! This can safely be imported as a wildcard:
    //!
    //! ```rust
    //! use aws_multipart_upload::prelude::*;
    //! ```
    pub use super::stream::UploadStreamExt as _;
    pub use multipart_write::MultipartWriteExt as _;
}

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

#[macro_use]
mod trace;

pub mod upload;
#[doc(inline)]
pub use upload::{UploadBuilder, Uploaded, stream};

pub mod uri;
#[doc(inline)]
pub use uri::{ObjectUri, ObjectUriIter};

mod write;
