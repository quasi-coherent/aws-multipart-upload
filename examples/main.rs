use aws_multipart_upload::aws_config as config;
use aws_multipart_upload::codec::{JsonLinesEncoder, JsonLinesEncoderBuilder};
use aws_multipart_upload::error::Error;
use aws_multipart_upload::prelude::*;
use aws_multipart_upload::request::CompletedUpload;
use aws_multipart_upload::uri::{Bucket, KeyPrefix};
use aws_multipart_upload::{FusedMultipartWrite, NewObjectUri, SdkClient, UploadClient};
use chrono::{DateTime, Utc};
use futures::{Stream, StreamExt, stream};
use serde::{Deserialize, Serialize};

const BUCKET: &str = "test-bucket-use2";
const PREFIX: &str = "example/prefix";
const PART_BYTES: usize = 5 * 1024 * 1024 * 512;
const UPLOAD_BYTES: usize = 10 * 1024 * 1024 * 1024;

#[tokio::main]
async fn main() {
    let client = sdk_client().await;
    // let uploader = LoginUploader {
    //     encoder: JsonLinesEncoder::new(None),
    // };
}

struct ExampleApp {}

impl ExampleApp {
    fn new(sdk: SdkClient) -> impl FusedMultipartWrite<UserLogin, Output = CompletedUpload> {
        let iter = std::iter::repeat_with(|| (Bucket::from(BUCKET), KeyPrefix::from(PREFIX)))
            .map_key(|prefix| prefix.to_key(&Utc::now().timestamp_millis().to_string()));

        let client = UploadClient::new(sdk);

        aws_multipart_upload::part_buffer(20)
            .into_uploader_with_uri(&client, NewObjectUri::uri_iter(iter))
            .with_part_encoder(JsonLinesEncoderBuilder::default(), None)
    }
}

async fn sdk_client() -> SdkClient {
    let loader = config::from_env()
        .region("us-east-2")
        .app_name(config::AppName::new("example-app").unwrap())
        .endpoint_url("http://localhost:9090");

    SdkClient::from_config(loader).await
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

fn user_login() -> impl Stream<Item = UserLogin> {
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
