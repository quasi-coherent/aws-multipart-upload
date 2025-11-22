use aws_multipart_upload::aws_config as config;
use aws_multipart_upload::codec::{CsvBuilder, CsvEncoder};
use aws_multipart_upload::error::Error;
use aws_multipart_upload::prelude::*;
use aws_multipart_upload::request::CompletedUpload;
use aws_multipart_upload::uri::{KeyPrefix, NewObjectUri};
use aws_multipart_upload::{ByteSize, SdkClient, Status, UploadBuilder};
use chrono::{DateTime, Utc};
use futures::{Stream, StreamExt, stream};
use serde::{Deserialize, Serialize};
use tracing_subscriber::filter::{EnvFilter, LevelFilter};
use tracing_subscriber::{fmt, prelude::*};

const BUCKET: &str = "test-bucket-use2";
const PREFIX: &str = "example/prefix";

#[tokio::main]
async fn main() {
    let filter = EnvFilter::builder()
        .with_default_directive(LevelFilter::WARN.into())
        .parse("aws_multipart_upload=trace")
        .unwrap();

    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(filter)
        .init();

    let app = ExampleApp::default();
    let uploader = app.uploader().await;

    let completed = ExampleApp::input_stream()
        .take(10000)
        .assemble(uploader)
        .await
        .inspect_err(|e| println!("{e}"))
        .unwrap();

    let uri = completed.uri;
    let etag = completed.etag;
    println!("upload to {uri} finished, object etag: {etag}");
}

struct ExampleApp {
    endpoint_url: String,
    upload_mib: u64,
    part_mib: u64,
}

impl Default for ExampleApp {
    fn default() -> Self {
        Self {
            endpoint_url: "http://127.0.0.1:9090".into(),
            upload_mib: 10,
            part_mib: 6,
        }
    }
}

impl ExampleApp {
    async fn uploader(
        &self,
    ) -> impl FusedMultipartWrite<UserLogin, Ret = Status, Error = Error, Output = CompletedUpload>
    {
        let client = self.sdk_client().await;
        let iter = self.uri_iter();

        UploadBuilder::new(client)
            .max_active_tasks(15)
            .upload_size(ByteSize::mib(self.upload_mib))
            .part_size(ByteSize::mib(self.part_mib))
            .with_encoding(CsvBuilder)
            .with_uri_iter(iter)
            .build::<UserLogin, CsvEncoder>()
    }

    fn uri_iter(&self) -> NewObjectUri {
        let iter = std::iter::repeat_with(|| KeyPrefix::from(PREFIX)).map_key(BUCKET, |prefix| {
            let now = Utc::now();
            let us = now.timestamp_micros();
            let pfx = now.format("%Y/%m/%d/%H").to_string();
            let root = format!("{pfx}/{us}.csv");
            prefix.to_key(&root)
        });
        NewObjectUri::uri_iter(iter)
    }

    async fn sdk_client(&self) -> SdkClient {
        let loader = config::from_env()
            .region("us-east-2")
            .app_name(config::AppName::new("example-app").unwrap())
            .endpoint_url(&self.endpoint_url);
        SdkClient::from_config(loader).await
    }

    fn input_stream() -> impl Stream<Item = UserLogin> {
        stream::iter(0..).map(|n| UserLogin {
            user_id: n % 50,
            display_name: format!("user_{}", n % 50),
            timestamp: Utc::now(),
            outcome: if n % 24 == 0 {
                Outcome::Deny
            } else {
                Outcome::Success
            },
        })
    }
}

/// An item in a message stream we wish to archive in S3.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct UserLogin {
    user_id: u64,
    display_name: String,
    timestamp: DateTime<Utc>,
    outcome: Outcome,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum Outcome {
    Success,
    Deny,
}
