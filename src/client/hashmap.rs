use aws_sdk_s3::primitives::ByteStream;
use futures::future::{ready, BoxFuture};
use std::collections::HashMap;
use std::sync::RwLock;

use crate::{
    types::{api::*, UploadClient},
    AwsError,
};

/// For testing, a client that writes a part `n` with data `bytes` as the entry
/// `(n, bytes)` in a hash map.
#[derive(Debug, Default)]
pub struct HashMapClient {
    store: RwLock<HashMap<i32, Vec<u8>>>,
}

impl HashMapClient {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn into_inner(self) -> HashMap<i32, Vec<u8>> {
        self.store.into_inner().unwrap()
    }

    pub fn clone_inner(&self) -> HashMap<i32, Vec<u8>> {
        self.store.read().unwrap().clone()
    }
}

impl UploadClient for HashMapClient {
    fn upload_part<'a, 'c: 'a>(
        &'c self,
        params: &'a UploadRequestParams,
        part_number: i32,
        part: ByteStream,
    ) -> BoxFuture<'a, Result<EntityTag, AwsError>> {
        Box::pin(async move {
            let etag = EntityTag::new(format!("{}_{}", params.key(), part_number));
            let vec = part.collect().await.map(|data| data.to_vec())?;

            let lock = self.store.read().unwrap();
            if lock.get(&part_number).is_some() {
                return Ok(etag);
            }
            let mut lock = self.store.write().unwrap();
            let _ = lock.entry(part_number).or_insert(vec);

            Ok(etag)
        })
    }

    // This is not meaningful for this client.
    fn new_upload<'a, 'c: 'a>(
        &'c self,
        addr: &'a UploadAddress,
    ) -> BoxFuture<'a, Result<UploadRequestParams, AwsError>> {
        let upload_id = UploadId::from(addr.key().to_string());
        Box::pin(ready(Ok(UploadRequestParams::new(upload_id, addr.clone()))))
    }

    // This is not meaningful for this client.
    fn complete_upload<'a, 'c: 'a>(
        &'c self,
        params: &'a UploadRequestParams,
        parts: &'a UploadedParts,
    ) -> BoxFuture<'a, Result<EntityTag, AwsError>> {
        let etag = EntityTag::from(format!("{}_{}", params.key(), parts.last_completed()));
        Box::pin(ready(Ok(etag)))
    }
}
