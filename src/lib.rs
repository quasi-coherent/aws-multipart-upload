pub mod client;
pub use self::client::aws::AwsClient;

pub mod codec;

pub mod types;
pub use self::types::api as api_types;
pub use self::types::upload::Upload;
pub use self::types::upload_forever::UploadForever;

pub mod testing {
    pub use super::client::fs::AsyncTempFileClient;
    pub use super::client::hashmap::HashMapClient;
}

mod aws_ops {
    pub use aws_sdk_s3::operation::complete_multipart_upload as complete;
    pub use aws_sdk_s3::operation::create_multipart_upload as create;
    pub use aws_sdk_s3::operation::upload_part as upload;
}

/// The minimum part size in an AWS multipart upload is 5MiB.
pub const AWS_MIN_PART_SIZE: usize = 5 * 1024 * 1024;
/// The maximum total size is 5 TiB.
pub const AWS_MAX_UPLOAD_SIZE: usize = 5 * 1024 * 1024 * 1024 * 1024;
/// The maximum total number of parts is 10,000.
pub const AWS_MAX_UPLOAD_PARTS: usize = 10000;
/// The default upload size is 100MiB.
pub const AWS_DEFAULT_TARGET_UPLOAD_SIZE: usize = 100 * 1024 * 1024;

#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum AwsError {
    #[error("error creating upload {0}")]
    Create(#[from] aws_sdk_s3::error::SdkError<aws_ops::create::CreateMultipartUploadError>),
    #[error("error uploading part {0}")]
    Upload(#[from] aws_sdk_s3::error::SdkError<aws_ops::upload::UploadPartError>),
    #[error("error completing upload {0}")]
    Complete(#[from] aws_sdk_s3::error::SdkError<aws_ops::complete::CompleteMultipartUploadError>),
    #[error("error creating bytestream {0}")]
    ByteStream(#[from] aws_sdk_s3::primitives::ByteStreamError),
    #[error("io error {0}")]
    Io(#[from] std::io::Error),
    #[error("missing required field {0}")]
    Missing(&'static str),
    #[error("encoding error {0}")]
    Codec(String),
    #[error("error formatting timestamp for s3 address {0}")]
    AddrFmt(#[from] chrono::format::ParseError),
    #[error("unable to produce the next upload destination")]
    UploadForever,
    #[error("ser/de error {0}")]
    Serde(String),
    #[error("user defined error {0}")]
    Custom(String),
    #[error(transparent)]
    DynStd(Box<dyn std::error::Error + Send + Sync + 'static>),
}

impl From<AwsError> for std::io::Error {
    fn from(v: AwsError) -> Self {
        Self::new(std::io::ErrorKind::Other, v)
    }
}

/// A builder for the `Upload` or `UploadForever` sinks.
pub struct UploadBuilder<C, E, U> {
    ctrl: C,
    codec: E,
    client: U,
}

impl<C, E, U> UploadBuilder<C, E, U> {
    pub fn new(client: U, ctrl: C, codec: E) -> Self
    where
        C: types::UploadControl,
        U: types::UploadClient,
    {
        Self {
            client,
            ctrl,
            codec,
        }
    }

    /// `init_upload` takes the fixed bucket/key and returns the one-time upload
    /// sink `Upload` in a future.
    pub async fn init_upload<I>(self, bucket: String, key: String) -> Result<Upload<E>, AwsError>
    where
        C: types::UploadControl + Send + Sync + 'static,
        E: tokio_util::codec::Encoder<I>,
        U: types::UploadClient + Send + Sync + 'static,
    {
        let addr = api_types::UploadAddress::new(bucket, key);
        let params = self.client.new_upload(&addr).await?;
        let sink = Upload::new(self.client, self.ctrl, self.codec, params);
        Ok(sink)
    }

    /// `init_upload_forever` takes the supplied iterator of S3 addresses and
    /// returns the self-driving sink `UploadForever` in a future.
    pub async fn init_upload_forever<I, T>(
        self,
        addr: T,
    ) -> Result<UploadForever<C, E, T, U>, AwsError>
    where
        C: types::UploadControl + Send + Sync + 'static,
        E: tokio_util::codec::Encoder<I> + Clone,
        T: Iterator<Item = api_types::UploadAddress>,
        U: types::UploadClient + Send + Sync + 'static,
    {
        let sink = UploadForever::new(self.client, self.ctrl, self.codec, addr).await?;
        Ok(sink)
    }
}

/// Default parameters for part/object uploads.
#[derive(Debug, Clone)]
pub struct DefaultControl {
    target_part_size: usize,
    target_upload_size: usize,
    target_num_parts: Option<usize>,
}

impl DefaultControl {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_target_part_size(mut self, n: usize) -> Self {
        self.target_part_size = n;
        self
    }

    pub fn set_target_upload_size(mut self, n: usize) -> Self {
        self.target_upload_size = n;
        self
    }

    pub fn set_target_num_parts(mut self, n: usize) -> Self {
        self.target_num_parts = Some(n);
        self
    }
}

impl Default for DefaultControl {
    fn default() -> Self {
        Self {
            target_part_size: AWS_MIN_PART_SIZE,
            target_upload_size: AWS_DEFAULT_TARGET_UPLOAD_SIZE,
            target_num_parts: None,
        }
    }
}

impl self::types::UploadControl for DefaultControl {
    fn target_part_size(&self) -> usize {
        self.target_part_size
    }

    fn is_upload_ready(&self, upload_size: usize, num_parts: usize) -> bool {
        upload_size >= self.target_upload_size
            || num_parts >= self.target_num_parts.unwrap_or_default()
    }
}
