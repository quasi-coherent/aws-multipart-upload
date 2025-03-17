#![allow(unused_imports, dead_code)]
mod helpers;
use self::helpers::{
    CheckJsonlines, CheckRowCount, TestClient, TestItemStream, TestUpload, TRACER,
};

use aws_multipart_upload::{
    client::UploadClientExt as _,
    codec::{CsvCodec, JsonlinesCodec},
    types::iter_addr::Timestamped,
};
use futures::StreamExt as _;

#[tokio::test(flavor = "multi_thread")]
async fn upload_csv_num_items() {
    let _ = &*TRACER;

    let client = TestClient::new().with_callback(CheckRowCount(100));
    let s = TestUpload::<_, CsvCodec>::from_client(client)
        .init_upload()
        .await;
    assert!(s.is_ok());

    let upload_sink = s.unwrap();
    let res = TestItemStream::take_items(100)
        .map(Ok)
        .forward(upload_sink)
        .await;
    assert!(res.is_ok())
}

#[tokio::test(flavor = "multi_thread")]
async fn upload_jsonlines_num_items() {
    let _ = &*TRACER;

    let client = TestClient::new().with_callback(CheckJsonlines(100));
    let s = TestUpload::<_, JsonlinesCodec>::from_client(client)
        .init_upload()
        .await;
    assert!(s.is_ok());

    let upload_sink = s.unwrap();
    let res = TestItemStream::take_items(100)
        .map(Ok)
        .forward(upload_sink)
        .await;
    if let Err(ref e) = res {
        tracing::error!(err = ?e, "error in sink");
    }
    assert!(res.is_ok())
}

#[tokio::test(flavor = "multi_thread")]
async fn upload_forever_jsonlines_bytes() {
    let _ = &*TRACER;

    let iter_addr = Timestamped::new("my-bucket", "%Y/%m/%d%h%m%s.json").unwrap();
    let s = TestUpload::<TestClient, JsonlinesCodec>::default()
        .init_upload_forever(iter_addr)
        .await;
    assert!(s.is_ok());

    let upload_sink = s.unwrap();
    let res = TestItemStream::take_bytes(5 * 1024 * 1024)
        .map(Ok)
        .forward(upload_sink)
        .await;
    assert!(res.is_ok())
}
