//! # Description
//!
//! A high-level crate for building AWS S3 multipart uploads using the official
//! AWS [SDK] for Rust.
//!
//! [SDK]: https://awslabs.github.io/aws-sdk-rust/
#![cfg_attr(docsrs, feature(doc_cfg))]
use self::client::SdkClient;
use self::upload::MultipartUploadBuilder;

use aws_sdk_s3::operation::complete_multipart_upload as complete_upload;
use aws_sdk_s3::operation::create_multipart_upload as create_upload;
use aws_sdk_s3::operation::upload_part;

pub mod client;
pub mod codec;
pub mod error;
pub mod sdk;
pub mod upload;

/// Returns a default [`MultipartUploadBuilder`] with an [`aws_sdk_s3::Client`]
/// as the client type.
///
/// This loads the AWS default configuration via:
///
/// ```rust,no_run
/// let config = aws_config::load_from_env().await;
/// let client = aws_sdk_s3::Client::new(&config);
/// ```
///
/// Use [`SdkClient::from_sdk_config`] to provide a non-default configuration.
pub async fn default_builder() -> MultipartUploadBuilder<SdkClient> {
    let config = aws_config::load_from_env().await;
    let client = SdkClient::from_sdk_config(config);
    MultipartUploadBuilder::new(client)
}

// https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html
mod config {
    pub(crate) const AWS_MAX_OBJECT_SIZE: usize = 5 * 1024 * 1024 * 1024 * 1024;
    pub(crate) const AWS_MIN_PART_SIZE: usize = 5 * 1024 * 1024;
    pub(crate) const AWS_MAX_PART_SIZE: usize = 5 * 1024 * 1024 * 1024;
    pub(crate) const AWS_MAX_PART_COUNT: usize = 10000;
    pub(crate) const DEFAULT_MAX_OBJECT_SIZE: usize = 5 * 1024 * 1024 * 1024;
    pub(crate) const DEFAULT_MAX_PART_SIZE: usize = 10 * 1024 * 1024;
}
