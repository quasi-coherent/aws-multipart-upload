//! # aws-multipart-upload
//!
//! `aws-multipart-upload` is a crate that wraps the official [`aws-sdk-s3`]
//! operations related to S3 multipart uploads into an implementation of `Sink`
//! from the `futures` crate.
//!
//! ## `Upload`
//!
//! The core type--the `Sink` itself--is [`Upload`].  It needs an S3 client,
//! represented by the trait [`UploadClient`], and a way to encode the item type
//! of the sink.  This crate provides suitable types of the latter, found in the
//! [`codec`] module, but any [`Encoder`] is acceptable.
//!
//! The `Upload` type captures the lifecycle of a multipart upload: writing to
//! parts, uploading parts, and completing the upload after enough parts have
//! been written.  Internally, this works by writing to an in-memory buffer
//! until the configured buffer size is reached. This triggers a flush to the
//! inner `AsyncWrite`, which is a temporary file.  The AWS SDK defines the
//! upload part request to take a [`ByteStream`] and a retryable `ByteStream`
//! can be created from a file, so this implementation is convenient.
//!
//! [`aws_sdk_s3`]: https://docs.rs/aws-sdk-s3/1.79.0/aws_sdk_s3/
//! [`Upload`]: crate::types::upload::Upload
//! [`UploadClient`]: crate::client::UploadClient
//! [`codec`]: crate::codec
//! [`Encoder`]: https://docs.rs/tokio-util/0.7.14/tokio_util/codec/trait.Encoder.html
//! [documentation]: crate::types::upload::Upload
pub mod client;
#[doc(inline)]
pub use self::client::{aws::AwsClient, UploadClient};

pub mod codec;
pub mod types;

pub mod upload;
#[doc(inline)]
pub use self::upload::Upload;

mod aws_ops {
    pub use aws_sdk_s3::operation::complete_multipart_upload as complete;
    pub use aws_sdk_s3::operation::create_multipart_upload as create;
    pub use aws_sdk_s3::operation::upload_part;
}

/// The minimum part size in an AWS multipart upload is 5MiB.
pub const AWS_MIN_PART_SIZE: usize = 5 * 1024 * 1024;
/// The maximum total size is 5 TiB.
pub const AWS_MAX_UPLOAD_SIZE: usize = 5 * 1024 * 1024 * 1024 * 1024;
/// The maximum total number of parts is 10,000.
pub const AWS_MAX_UPLOAD_PARTS: usize = 10000;

/// A builder for the `Upload` sink.
pub struct UploadBuilder<T, E> {
    client: T,
    codec: E,
    config: UploadConfig,
}

impl<T, E> UploadBuilder<T, E>
where
    T: UploadClient,
    E: Default,
{
    /// Return the builder given an upload client.
    pub fn from_client(client: T) -> Self {
        Self {
            client,
            codec: E::default(),
            config: UploadConfig::default(),
        }
    }

    /// Set the encoder type for the upload.
    pub fn with_encoder(mut self, codec: E) -> Self {
        self.codec = codec;
        self
    }

    /// Set configuration for the upload.
    pub fn set_config(mut self, config: UploadConfig) -> Self {
        self.config = config;
        self
    }

    /// Returns the [`Upload`] sink with an upload started.
    ///
    /// [`Upload`]: crate::types::upload::Upload
    pub async fn build<I>(self, bucket: &str, key: &str) -> Result<Upload<E>, AwsError>
    where
        T: UploadClient + Send + Sync + 'static,
        E: tokio_util::codec::Encoder<I>,
    {
        let addr: types::UploadAddress = (bucket, key).into();
        let sink = Upload::new(self.client, self.codec, addr, self.config).await?;

        Ok(sink)
    }
}

/// Errors that can be produced by the API.
#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum AwsError {
    #[error("error creating upload {0}")]
    Create(#[from] aws_sdk_s3::error::SdkError<aws_ops::create::CreateMultipartUploadError>),
    #[error("error uploading part {0}")]
    Upload(#[from] aws_sdk_s3::error::SdkError<aws_ops::upload_part::UploadPartError>),
    #[error("error completing upload {0}")]
    Complete(#[from] aws_sdk_s3::error::SdkError<aws_ops::complete::CompleteMultipartUploadError>),
    #[error("error creating bytestream {0}")]
    ByteStream(#[from] aws_sdk_s3::primitives::ByteStreamError),
    #[error("io error {0}")]
    Io(#[from] std::io::Error),
    #[error("error with tempfile {0}")]
    TmpFile(#[from] async_tempfile::Error),
    #[error("encoding error {0}")]
    Codec(String),
    #[error("missing required field {0}")]
    Missing(&'static str),
    #[error("user defined error {0}")]
    Custom(String),
    #[error(transparent)]
    DynStd(#[from] Box<dyn std::error::Error + Send + Sync + 'static>),
}

impl From<AwsError> for std::io::Error {
    fn from(v: AwsError) -> Self {
        Self::new(std::io::ErrorKind::Other, v)
    }
}

/// Configuration for the AWS multipart upload.
#[derive(Debug, Clone, Copy)]
pub struct UploadConfig {
    /// The minimum size in bytes of one part.
    /// A part upload will be triggered after an incoming write exceeds this.
    pub min_part_size: usize,
    /// The size in bytes of the internal buffer for writing parts to.
    /// Exceeding this triggers a write to the inner temp file.  The default is
    /// 8KiB.
    pub buffer_size: Option<usize>,
    // Not in use currently.
    _max_upload_size: Option<usize>,
    // Not in use currently.
    _max_upload_parts: Option<usize>,
}

impl UploadConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_min_part_size(mut self, size: usize) -> Self {
        self.min_part_size = size;
        self
    }

    pub fn with_buffer_size(mut self, size: usize) -> Self {
        self.buffer_size = Some(size);
        self
    }
}

impl Default for UploadConfig {
    fn default() -> Self {
        Self {
            min_part_size: AWS_MIN_PART_SIZE,
            buffer_size: None,
            _max_upload_size: None,
            _max_upload_parts: None,
        }
    }
}
