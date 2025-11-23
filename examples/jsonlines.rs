use aws_multipart_upload::write::UploadStreamExt as _;
use futures::StreamExt as _;

pub mod helpers;
pub use helpers::*;

#[tokio::main]
async fn main() {
    init_tracer();
    let config = Config::default();
    let upload = jsonlines::JsonLinesExample::upload(config).await;

    UserLogin::stream()
        .into_upload(upload)
        .for_each(|res| async move {
            match res {
                Ok(completed) => println!("successfully completed upload: {completed:?}"),
                Err(e) => println!("error in multipart upload: {e}"),
            }
        })
        .await
}
