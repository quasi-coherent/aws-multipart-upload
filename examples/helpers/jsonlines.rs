use aws_multipart_upload::codec::JsonLinesEncoder;
use aws_multipart_upload::{ByteSize, Upload, UploadBuilder};

use super::{Config, iter_uri, sdk_client};

#[derive(Debug, Clone, Copy)]
pub struct JsonLinesExample;

impl JsonLinesExample {
    pub async fn upload(config: Config) -> Upload<JsonLinesEncoder> {
        let iter = iter_uri(config.num_uploads, "jsonlines", "jsonl");
        let client = sdk_client().await;

        UploadBuilder::new(client)
            .with_upload_size(ByteSize::mib(config.upload_mib))
            .with_part_size(ByteSize::mib(config.part_mib))
            .with_encoder(JsonLinesEncoder::new())
            .with_uri_iter(iter)
            .with_capacity(config.max_tasks)
            .build()
    }
}
