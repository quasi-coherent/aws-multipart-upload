use aws_multipart_upload::client::OnComplete;
use aws_multipart_upload::types::UploadClient;
use aws_multipart_upload::{api_types::*, testing::HashMapClient, AwsError};
use futures::future::BoxFuture;

use super::TestItem;

#[derive(Debug, Default)]
pub struct TestClient(pub HashMapClient);

impl TestClient {
    pub fn new() -> Self {
        Self::default()
    }
}

#[derive(Clone, Debug)]
pub struct CheckRowCount(pub usize);

impl OnComplete<TestClient> for CheckRowCount {
    /// Callback on a CSV upload to check that the number of rows is correct.
    fn on_upload_complete<'a, 'c: 'a>(
        &'c self,
        client: &'a TestClient,
        _: EntityTag,
    ) -> BoxFuture<'a, Result<(), AwsError>> {
        Box::pin(async move {
            let count = self.0;
            let store = client.0.clone_inner().await;
            let mut item_count = 0;
            for (_, part) in store.into_iter() {
                let de = String::from_utf8(part).unwrap();
                let rs: Vec<String> = de.lines().map(|s| s.to_string()).collect();
                item_count += rs.len();
            }
            if item_count != count {
                Err(AwsError::Custom(format!(
                    "incorrect item count: got {item_count}, expected {count}"
                )))
            } else {
                Ok(())
            }
        })
    }
}

#[derive(Debug, Clone)]
pub struct CheckJsonlines(pub usize);

impl OnComplete<TestClient> for CheckJsonlines {
    fn on_upload_complete<'a, 'c: 'a>(
        &'c self,
        client: &'a TestClient,
        _: EntityTag,
    ) -> BoxFuture<'a, Result<(), AwsError>> {
        Box::pin(async move {
            let count = self.0;
            let store = client.0.clone_inner().await;
            let mut item_count = 0;
            for (_, part) in store.into_iter() {
                let de = String::from_utf8(part).unwrap();
                let rs: Result<Vec<TestItem>, _> =
                    de.lines().map(|s| serde_json::from_str(s)).collect();
                let Ok(items) = rs else {
                    tracing::error!(error = ?rs.unwrap_err(), "error deserializing part");
                    continue;
                };
                item_count += items.len();
            }
            if item_count != count {
                Err(AwsError::Custom(format!(
                    "incorrect item count: got {item_count}, expected {count}"
                )))
            } else {
                Ok(())
            }
        })
    }
}

impl UploadClient for TestClient {
    fn new_upload<'a, 'client: 'a>(
        &'client self,
        addr: &'a UploadAddress,
    ) -> BoxFuture<'a, Result<UploadRequestParams, AwsError>> {
        self.0.new_upload(addr)
    }

    fn upload_part<'a, 'client: 'a>(
        &'client self,
        params: &'a UploadRequestParams,
        part_number: i32,
        part: aws_sdk_s3::primitives::ByteStream,
    ) -> BoxFuture<'a, Result<EntityTag, AwsError>> {
        self.0.upload_part(params, part_number, part)
    }

    fn complete_upload<'a, 'client: 'a>(
        &'client self,
        params: &'a UploadRequestParams,
        parts: &'a UploadedParts,
    ) -> BoxFuture<'a, Result<EntityTag, AwsError>> {
        self.0.complete_upload(params, parts)
    }
}
