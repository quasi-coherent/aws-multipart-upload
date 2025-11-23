use aws_multipart_upload::SdkClient;
use aws_multipart_upload::uri::{KeyPrefix, ObjectUri, ObjectUriIterExt as _};
use chrono::Utc;
use tracing_subscriber::filter::{EnvFilter, LevelFilter};
use tracing_subscriber::{fmt, prelude::*};

mod user_stream;
pub use user_stream::*;

#[cfg(feature = "csv")]
pub mod csv;

pub mod jsonlines;

const BUCKET: &str = "test-bucket-use2";
const PREFIX: &str = "example/prefix";
const ENDPOINT: &str = "http://127.0.0.1:9090";

#[derive(Debug, Clone)]
pub struct Config {
    upload_mib: u64,
    part_mib: u64,
    num_uploads: usize,
    max_tasks: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            upload_mib: 25,
            part_mib: 5,
            num_uploads: 3,
            max_tasks: 15,
        }
    }
}

pub fn init_tracer() {
    let filter = EnvFilter::builder()
        .with_default_directive(LevelFilter::WARN.into())
        .parse("aws_multipart_upload=trace")
        .unwrap();

    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(filter)
        .init();
}

async fn sdk_client() -> SdkClient {
    let loader = aws_config::from_env()
        .region("us-east-2")
        .app_name(aws_config::AppName::new("example-app").unwrap())
        .endpoint_url(ENDPOINT);

    SdkClient::from_config(loader).await
}

/// Used to produce the next destination for an upload when one finishes.
/// In this example we `take(self.num_uploads)` from this iterator to
/// make it finite.
fn iter_uri(limit: usize, v: &'static str, ext: &'static str) -> impl Iterator<Item = ObjectUri> {
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
