use super::{Config, iter_uri, sdk_client};

use aws_multipart_upload::codec::JsonLinesEncoder;
use aws_multipart_upload::{ByteSize, MultipartUpload, UploadBuilder};

#[derive(Debug, Clone, Copy)]
pub struct JsonLinesExample;

impl JsonLinesExample {
    pub async fn upload(config: Config) -> MultipartUpload<JsonLinesEncoder> {
        let iter = iter_uri(config.num_uploads, "jsonlines", "jsonl");
        let client = sdk_client().await;

        UploadBuilder::new(client)
            .max_active_tasks(config.max_tasks)
            .upload_size(ByteSize::mib(config.upload_mib))
            .part_size(ByteSize::mib(config.part_mib))
            .with_encoder(JsonLinesEncoder::new())
            .with_uri_iter(iter)
            .build()
    }
}
