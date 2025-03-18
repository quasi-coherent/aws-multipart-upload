pub mod helpers;
use self::helpers::{
    CheckJsonlines, CheckRowCount, TestClient, TestItemStream, TestUpload, TRACER,
};

use aws_multipart_upload::{
    client::UploadClientExt as _,
    codec::{CsvCodec, JsonlinesCodec},
};
use futures::StreamExt as _;

#[tokio::test(flavor = "multi_thread")]
async fn upload_csv_num_items() {
    let _ = &*TRACER;

    let client = TestClient::new().with_callback(CheckRowCount(100));
    let upload = TestUpload::<_, CsvCodec>::new(client).build().await;

    let res = TestItemStream::take_items(100)
        .map(Ok)
        .forward(upload)
        .await;

    assert!(res.is_ok())
}

#[tokio::test(flavor = "multi_thread")]
async fn upload_jsonlines_num_items() {
    let _ = &*TRACER;

    let client = TestClient::new().with_callback(CheckJsonlines(100));
    let upload = TestUpload::<_, JsonlinesCodec>::new(client).build().await;

    let res = TestItemStream::take_items(100)
        .map(Ok)
        .forward(upload)
        .await;

    assert!(res.is_ok())
}
