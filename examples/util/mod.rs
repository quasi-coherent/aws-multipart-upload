use aws_multipart_upload::SdkClient;
use aws_multipart_upload::uri::{KeyPrefix, ObjectUri, ObjectUriIterExt as _};
use chrono::{DateTime, Utc};
use futures::{Stream, StreamExt, stream};
use serde::{Deserialize, Serialize};
use tracing_subscriber::filter::{EnvFilter, LevelFilter};
use tracing_subscriber::fmt;
use tracing_subscriber::prelude::*;

pub const BUCKET: &str = "test-bucket-use2";
pub const PREFIX: &str = "example/prefix";
pub const ENDPOINT: &str = "http://127.0.0.1:9090";

#[derive(Debug, Clone)]
pub struct Config {
    pub upload_mib: u64,
    pub part_mib: u64,
    pub num_uploads: usize,
    pub max_tasks: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self { upload_mib: 20, part_mib: 5, num_uploads: 3, max_tasks: 15 }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Outcome {
    Success,
    Deny,
}

/// An item in a message stream we wish to archive in S3.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserLogin {
    user_id: u64,
    display_name: String,
    timestamp: DateTime<Utc>,
    outcome: Outcome,
}

impl UserLogin {
    pub fn stream() -> impl Stream<Item = UserLogin> {
        stream::iter(0..).map(|n| UserLogin {
            user_id: n % 50,
            display_name: format!("user_{}", n % 50),
            timestamp: Utc::now(),
            outcome: if n % 24 == 0 { Outcome::Deny } else { Outcome::Success },
        })
    }
}

impl std::fmt::Display for Outcome {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Success => write!(f, "SUCCESS"),
            Self::Deny => write!(f, "DENY"),
        }
    }
}

pub fn init_tracer() {
    let filter = EnvFilter::builder()
        .with_default_directive(LevelFilter::INFO.into())
        .parse("aws_multipart_upload=trace,csv=info,jsonlines=info")
        .unwrap();

    tracing_subscriber::registry().with(fmt::layer()).with(filter).init();
}

pub async fn sdk_client() -> SdkClient {
    let loader = aws_config::from_env()
        .region("us-east-2")
        .app_name(aws_config::AppName::new("example-app").unwrap())
        .endpoint_url(ENDPOINT);

    SdkClient::from_config(loader).await
}

/// Used to produce the next destination for an upload when one finishes.
/// In this example we `take(self.num_uploads)` from this iterator to
/// make it finite.
pub fn iter_uri(
    limit: usize,
    v: &'static str,
    ext: &'static str,
) -> impl Iterator<Item = ObjectUri> {
    std::iter::repeat_with(|| KeyPrefix::from(PREFIX))
        .map_key(BUCKET, move |prefix| {
            let now = Utc::now();
            let us = now.timestamp_micros();
            let pfx = now.format("%Y/%m/%d/%H");
            let root = format!("{pfx}/{v}/{us}.{ext}").to_string();
            prefix.to_key(&root)
        })
        .take(limit)
}
