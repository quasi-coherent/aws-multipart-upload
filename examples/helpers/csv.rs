use super::{Config, iter_uri, sdk_client};

use aws_multipart_upload::codec::CsvEncoder;
use aws_multipart_upload::{ByteSize, MultipartUpload, UploadBuilder};

#[derive(Debug, Clone, Copy)]
pub struct CsvExample;

impl CsvExample {
    pub async fn upload(config: Config) -> MultipartUpload<CsvEncoder> {
        let iter = iter_uri(config.num_uploads, "csv", "csv");
        let client = sdk_client().await;

        UploadBuilder::new(client)
            .max_active_tasks(config.max_tasks)
            .upload_size(ByteSize::mib(config.upload_mib))
            .part_size(ByteSize::mib(config.part_mib))
            .with_encoder(CsvEncoder::default().with_header())
            .with_uri_iter(iter)
            .build()
    }
}
