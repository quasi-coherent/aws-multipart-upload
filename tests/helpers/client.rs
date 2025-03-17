use aws_multipart_upload::types::UploadClient;
use aws_multipart_upload::{api_types::*, testing::HashMapClient, AwsError};
use futures::future::{ready, BoxFuture};

use super::TestItem;

#[derive(Debug, Default)]
pub struct TestUploadClient(pub HashMapClient, pub Option<usize>);

impl TestUploadClient {
    pub fn new() -> Self {
        Self::default()
    }
}

impl UploadClient for TestUploadClient {
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

    fn on_upload_complete<'a, 'client: 'a>(
        &'client self,
        _: &'a EntityTag,
    ) -> BoxFuture<'a, Result<(), AwsError>> {
        let Some(count) = self.1 else {
            return Box::pin(ready(Ok(())));
        };

        Box::pin({
            let store = self.0.clone_inner();
            let values = store.into_values();
            let mut item_count = 0;
            for val in values {
                let de = serde_json::from_slice::<Vec<TestItem>>(&val);
                let Ok(items) = de else {
                    tracing::error!(error = ?de.unwrap_err(), "error deserializing part");
                    continue;
                };
                item_count += items.len();
            }
            ready(if item_count != count {
                Err(AwsError::Custom(format!(
                    "incorrect item count: got {item_count}, expected {count}"
                )))
            } else {
                Ok(())
            })
        })
    }
}
