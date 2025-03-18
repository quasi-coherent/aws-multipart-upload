use aws_multipart_upload::client::{hashmap::HashMapClient, OnUploadAction};
use aws_multipart_upload::types::{EntityTag, UploadAddress, UploadParams, UploadedParts};
use aws_multipart_upload::{AwsError, UploadClient};
use futures::future::{ready, BoxFuture};

use super::TestItem;

#[derive(Debug, Default)]
pub struct TestClient(pub HashMapClient);

impl TestClient {
    pub fn new() -> Self {
        Self::default()
    }
}

impl UploadClient for TestClient {
    fn new_upload<'a, 'client: 'a>(
        &'client self,
        addr: &'a UploadAddress,
    ) -> BoxFuture<'a, Result<UploadParams, AwsError>> {
        self.0.new_upload(addr)
    }

    fn upload_part<'a, 'client: 'a>(
        &'client self,
        params: &'a UploadParams,
        part_number: i32,
        part: aws_sdk_s3::primitives::ByteStream,
    ) -> BoxFuture<'a, Result<EntityTag, AwsError>> {
        self.0.upload_part(params, part_number, part)
    }

    fn complete_upload<'a, 'client: 'a>(
        &'client self,
        params: &'a UploadParams,
        parts: &'a UploadedParts,
    ) -> BoxFuture<'a, Result<EntityTag, AwsError>> {
        self.0.complete_upload(params, parts)
    }
}

// Assert on the `OnUploadAction` implementation's result.
// It checks the number of items written.
#[derive(Clone, Debug)]
pub struct CheckRowCount(pub usize);
// An `OnUploadAction` to check serialization and row count.
#[derive(Debug, Clone)]
pub struct CheckJsonlines(pub usize);

impl OnUploadAction<TestClient> for CheckRowCount {
    fn on_upload_part<'a, 'c: 'a>(
        &'c self,
        _: &'a TestClient,
        _: UploadParams,
        _: EntityTag,
    ) -> BoxFuture<'a, Result<(), AwsError>> {
        Box::pin(ready(Ok(())))
    }

    fn on_upload_complete<'a, 'c: 'a>(
        &'c self,
        client: &'a TestClient,
        _: UploadParams,
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

impl OnUploadAction<TestClient> for CheckJsonlines {
    fn on_upload_part<'a, 'c: 'a>(
        &'c self,
        _: &'a TestClient,
        _: UploadParams,
        _: EntityTag,
    ) -> BoxFuture<'a, Result<(), AwsError>> {
        Box::pin(ready(Ok(())))
    }

    fn on_upload_complete<'a, 'c: 'a>(
        &'c self,
        client: &'a TestClient,
        _: UploadParams,
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
