pub mod client;
pub use self::client::TestUploadClient;

pub mod ctrl;
use self::ctrl::TestControl;

pub mod message;
pub use self::message::{TestItem, TestItemStream};

use aws_multipart_upload::{api_types::UploadAddress, codec::CsvCodec};
use aws_multipart_upload::{AwsError, Upload, UploadBuilder, UploadForever};
use std::{str::FromStr, sync::LazyLock};

pub static TRACER: LazyLock<()> = LazyLock::new(|| {
    let level = std::env::var("LOG_LEVEL")
        .map(|l| tracing::Level::from_str(l.as_str()).unwrap())
        .unwrap_or(tracing::Level::INFO);
    tracing_subscriber::fmt().with_max_level(level).init()
});

#[derive(Debug)]
pub struct TestCsvUpload {
    client: TestUploadClient,
    codec: CsvCodec,
    ctrl: TestControl,
}

impl TestCsvUpload {
    pub fn new() -> Self {
        Self {
            client: TestUploadClient::default(),
            codec: CsvCodec::default(),
            ctrl: TestControl::default(),
        }
    }

    pub fn num_items(mut self, item_count: usize) -> Self {
        self.client.1 = Some(item_count);
        self
    }

    pub fn with_part_size(mut self, n: usize) -> Self {
        self.ctrl.part_size = n;
        self
    }

    pub fn with_upload_size(mut self, n: usize) -> Self {
        self.ctrl.upload_size = n;
        self
    }

    pub fn csv_headers(mut self) -> Self {
        self.codec.has_headers = true;
        self
    }

    pub async fn init_upload(self) -> Result<Upload<CsvCodec>, AwsError> {
        let builder = UploadBuilder::new(self.client, self.ctrl, self.codec);
        let sink = builder
            .init_upload::<TestItem>("doesnt".to_string(), "matter".to_string())
            .await?;
        Ok(sink)
    }

    pub async fn init_upload_forever<T>(
        self,
        upload_addr: T,
    ) -> Result<UploadForever<TestControl, CsvCodec, T, TestUploadClient>, AwsError>
    where
        T: Iterator<Item = UploadAddress>,
    {
        let builder = UploadBuilder::new(self.client, self.ctrl, self.codec);
        let sink = builder
            .init_upload_forever::<TestItem, T>(upload_addr)
            .await?;
        Ok(sink)
    }
}
