use aws_sdk_s3::primitives::ByteStream;
use futures::future::{ready, BoxFuture};
use std::collections::HashMap;
use tokio::sync::Mutex;

use crate::{
    client::UploadClient,
    types::{EntityTag, UploadAddress, UploadId, UploadParams, UploadedParts},
    AwsError,
};

/// For testing, a client that writes a part `n` with data `bytes` as the entry
/// `(n: i32, bytes: Vec<u8>)` in a hash map.
#[derive(Debug)]
pub struct HashMapClient {
    store: Mutex<HashMap<i32, Vec<u8>>>,
}

impl Default for HashMapClient {
    fn default() -> Self {
        Self {
            store: Mutex::new(HashMap::new()),
        }
    }
}

impl HashMapClient {
    pub fn new() -> Self {
        Self {
            store: Mutex::new(HashMap::new()),
        }
    }

    pub async fn clone_inner(&self) -> HashMap<i32, Vec<u8>> {
        let map = self.store.lock().await;
        map.clone()
    }
}

impl UploadClient for HashMapClient {
    fn upload_part<'a, 'c: 'a>(
        &'c self,
        params: &'a UploadParams,
        part_number: i32,
        part: ByteStream,
    ) -> BoxFuture<'a, Result<EntityTag, AwsError>> {
        Box::pin(async move {
            let etag = EntityTag::new(format!("{}_{}", params.key(), part_number));
            let vec = part.collect().await.map(|data| data.to_vec())?;

            let mut map = self.store.lock().await;
            let _ = map.entry(part_number).or_insert(vec);

            Ok(etag)
        })
    }

    // This is not meaningful for this client.
    fn new_upload<'a, 'c: 'a>(
        &'c self,
        addr: &'a UploadAddress,
    ) -> BoxFuture<'a, Result<UploadParams, AwsError>> {
        let upload_id = UploadId::from(addr.key().to_string());
        Box::pin(ready(Ok(UploadParams::new(upload_id, addr.clone()))))
    }

    // This is not meaningful for this client.
    fn complete_upload<'a, 'c: 'a>(
        &'c self,
        params: &'a UploadParams,
        parts: &'a UploadedParts,
    ) -> BoxFuture<'a, Result<EntityTag, AwsError>> {
        let etag = EntityTag::from(format!("{}_{}", params.key(), parts.last_part_number()));
        Box::pin(ready(Ok(etag)))
    }
}
