use aws_multipart_upload::codec::CsvEncoder;
use aws_multipart_upload::{ByteSize, Upload, UploadBuilder};

use super::{Config, iter_uri, sdk_client};

#[derive(Debug, Clone, Copy)]
pub struct CsvExample;

impl CsvExample {
    pub async fn upload(config: Config) -> Upload<CsvEncoder> {
        let iter = iter_uri(config.num_uploads, "csv", "csv");
        let client = sdk_client().await;

        UploadBuilder::new(client)
            .with_upload_size(ByteSize::mib(config.upload_mib))
            .with_part_size(ByteSize::mib(config.part_mib))
            .with_encoder(CsvEncoder::default().with_header())
            .with_uri_iter(iter)
            .with_capacity(config.max_tasks)
            .build()
    }
}
