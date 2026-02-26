use aws_multipart_upload::stream::UploadStreamExt as _;
use futures::TryStreamExt as _;

pub mod helpers;
pub use helpers::*;

#[tokio::main]
async fn main() {
    init_tracer();
    let config = Config::default();
    let upload = jsonlines::JsonLinesExample::upload(config).await;

    if let Err(e) = UserLogin::stream()
        .try_upload(upload)
        .try_for_each(|completed| async move {
            tracing::info!(?completed, "completed upload");
            Ok(())
        })
        .await
    {
        tracing::error!(error = %e, "error found in upload");
        std::process::exit(1);
    }
}
