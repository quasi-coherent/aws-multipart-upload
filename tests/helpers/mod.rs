pub mod client;
pub use self::client::{CheckJsonlines, CheckRowCount, TestClient};

pub mod message;
pub use self::message::{TestItem, TestItemStream};

use aws_multipart_upload::{Upload, UploadBuilder, UploadClient, UploadConfig};
use std::{str::FromStr, sync::LazyLock};
use tokio_util::codec::Encoder;

pub static TRACER: LazyLock<()> = LazyLock::new(|| {
    let level = std::env::var("LOG_LEVEL")
        .map(|l| tracing::Level::from_str(l.as_str()).unwrap())
        .unwrap_or(tracing::Level::INFO);
    tracing_subscriber::fmt().with_max_level(level).init()
});

#[derive(Debug)]
pub struct TestUpload<T, E> {
    client: T,
    codec: E,
    part_size: usize,
    buf_size: Option<usize>,
}

impl<T, E> TestUpload<T, E>
where
    T: UploadClient + Send + Sync + 'static,
    E: Encoder<TestItem> + Default,
{
    pub fn new(client: T) -> Self {
        Self {
            client,
            codec: E::default(),
            part_size: 512,
            buf_size: None,
        }
    }

    pub fn with_part_size(mut self, size: usize) -> Self {
        self.part_size = size;
        self
    }

    pub fn with_buf_size(mut self, size: usize) -> Self {
        self.buf_size = Some(size);
        self
    }

    pub async fn build(self) -> Upload<E> {
        let mut config = UploadConfig::new().with_min_part_size(self.part_size);
        if let Some(size) = self.buf_size {
            config = config.with_buffer_size(size);
        }

        UploadBuilder::from_client(self.client)
            .with_encoder(self.codec)
            .set_config(config)
            .build("doesnot", "matter")
            .await
            .map_err(|e| tracing::error!(error = ?e, "error creating sink"))
            .unwrap()
    }
}
