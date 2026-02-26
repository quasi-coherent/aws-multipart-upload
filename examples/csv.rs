use aws_multipart_upload::encoder::CsvEncoder;
use aws_multipart_upload::stream::UploadStreamExt as _;
use aws_multipart_upload::{ByteSize, UploadBuilder};
use futures::TryStreamExt as _;

pub mod util;
use util::*;

#[tokio::main]
async fn main() {
    init_tracer();
    let config = Config::default();
    let client = sdk_client().await;
    let iter = iter_uri(config.num_uploads, "csvlines", "csv");

    let upl = UploadBuilder::new(client)
        .with_upload_size(ByteSize::mib(config.upload_mib))
        .with_part_size(ByteSize::mib(config.part_mib))
        .with_capacity(config.max_tasks)
        .with_uri_iter(iter)
        .build_encoded(CsvEncoder::new());

    if let Err(e) = UserLogin::stream()
        .try_upload(upl)
        .try_for_each(|out| async move {
            tracing::info!(uploaded = ?out, "completed upload");
            Ok(())
        })
        .await
    {
        tracing::error!(error = %e, "error found in upload");
        std::process::exit(1);
    }
}
