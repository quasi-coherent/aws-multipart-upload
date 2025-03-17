#![allow(unused_imports, dead_code)]
mod helpers;
use self::helpers::{TestCsvUpload, TestItemStream, TRACER};

use futures::StreamExt as _;

#[tokio::test]
async fn test_upload_num_items() {
    let _ = &*TRACER;

    let s = TestCsvUpload::new().num_items(100).init_upload().await;
    assert!(s.is_ok());

    let upload_sink = s.unwrap();
    let res = TestItemStream::take_items(100)
        .map(Ok)
        .forward(upload_sink)
        .await;
    tracing::warn!("{res:?}");
    assert!(res.is_ok())
}
