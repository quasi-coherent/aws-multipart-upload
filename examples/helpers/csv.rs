use super::{Config, UserLogin, iter_uri, sdk_client};

use aws_multipart_upload::codec::{CsvBuilder, CsvEncoder};
use aws_multipart_upload::{ByteSize, MultipartUpload, UploadBuilder};

#[derive(Debug, Clone, Copy)]
pub struct CsvExample;

impl CsvExample {
    pub async fn upload(config: Config) -> MultipartUpload<UserLogin, CsvEncoder> {
        let iter = iter_uri(config.num_uploads, "csv", "csv");
        let client = sdk_client().await;

        UploadBuilder::new(client)
            .max_active_tasks(config.max_tasks)
            .upload_size(ByteSize::mib(config.upload_mib))
            .part_size(ByteSize::mib(config.part_mib))
            .with_encoding(CsvBuilder)
            .with_uri_iter(iter)
            .build::<UserLogin, CsvEncoder>()
    }
}
