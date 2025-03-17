pub mod client;
pub use self::client::{CheckJsonlines, CheckRowCount, TestClient};

pub mod ctrl;
use self::ctrl::TestControl;

pub mod message;
pub use self::message::{TestItem, TestItemStream};

use aws_multipart_upload::api_types::UploadAddress;
use aws_multipart_upload::{types::UploadClient, AwsError, Upload, UploadBuilder, UploadForever};
use std::{str::FromStr, sync::LazyLock};
use tokio_util::codec::Encoder;

pub static TRACER: LazyLock<()> = LazyLock::new(|| {
    let level = std::env::var("LOG_LEVEL")
        .map(|l| tracing::Level::from_str(l.as_str()).unwrap())
        .unwrap_or(tracing::Level::INFO);
    tracing_subscriber::fmt().with_max_level(level).init()
});

#[derive(Debug)]
pub struct TestUpload<U, E> {
    client: U,
    codec: E,
    ctrl: TestControl,
}

impl<U: Default, E: Default> Default for TestUpload<U, E> {
    fn default() -> Self {
        Self {
            client: U::default(),
            codec: E::default(),
            ctrl: TestControl::default(),
        }
    }
}

impl<U, E> TestUpload<U, E>
where
    U: UploadClient + Send + Sync + 'static,
    E: Encoder<TestItem> + Default,
{
    pub fn from_client(client: U) -> Self {
        Self {
            client,
            codec: E::default(),
            ctrl: TestControl::default(),
        }
    }

    pub fn with_codec(mut self, codec: E) -> Self {
        self.codec = codec;
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

    pub async fn init_upload(self) -> Result<Upload<E>, AwsError> {
        let builder = UploadBuilder::new(self.client, self.ctrl, self.codec);
        let sink = builder
            .init_upload::<TestItem>("doesnt".to_string(), "matter".to_string())
            .await?;
        Ok(sink)
    }

    pub async fn init_upload_forever<T>(
        self,
        upload_addr: T,
    ) -> Result<UploadForever<TestControl, E, T, U>, AwsError>
    where
        E: Clone,
        T: Iterator<Item = UploadAddress>,
    {
        let builder = UploadBuilder::new(self.client, self.ctrl, self.codec);
        let sink = builder
            .init_upload_forever::<TestItem, T>(upload_addr)
            .await?;
        Ok(sink)
    }
}
