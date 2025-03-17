use async_tempfile::{Error as TmpFileError, TempFile};
use aws_sdk_s3::primitives::ByteStream;
use futures::{future, future::BoxFuture};
use tokio::io::AsyncWriteExt as _;
use tokio::sync::Mutex;

use crate::{
    types::{api::*, UploadClient},
    AwsError,
};

impl From<TmpFileError> for AwsError {
    fn from(value: TmpFileError) -> Self {
        match value {
            TmpFileError::Io(e) => AwsError::from(e),
            e => AwsError::DynStd(e.to_string().into()),
        }
    }
}

/// Another upload client for testing that writes to a temp file.
#[derive(Debug)]
pub struct AsyncTempFileClient {
    file: Mutex<Option<TempFile>>,
}

impl Default for AsyncTempFileClient {
    fn default() -> Self {
        Self {
            file: Mutex::new(None),
        }
    }
}

impl AsyncTempFileClient {
    pub fn new() -> Self {
        Self::default()
    }

    async fn set(&self) -> Result<String, AwsError> {
        let inner = TempFile::new().await?;
        let path = inner
            .file_path()
            .to_str()
            .map(str::to_string)
            .ok_or_else(|| AwsError::Missing("upload_id"))?;
        let mut f = self.file.lock().await;
        *f = Some(inner);
        Ok(path)
    }

    async fn write(&self, buf: &[u8]) -> Result<(), AwsError> {
        let mut inner = self.file.lock().await;
        let f = inner
            .as_deref_mut()
            .ok_or_else(|| AwsError::Missing("no file set for `AsyncTempFileClient::write`"))?;
        f.write_all(buf).await?;
        f.flush().await?;
        Ok(())
    }
}

impl UploadClient for AsyncTempFileClient {
    // Make the `upload_id` the temp file's path, so we're really initializing
    // `AsyncTempFileClient` here.
    fn new_upload<'a, 'client: 'a>(
        &'client self,
        addr: &'a UploadAddress,
    ) -> BoxFuture<'a, Result<UploadRequestParams, AwsError>> {
        Box::pin(async move {
            let upload_id: UploadId = self.set().await?.into();
            Ok(UploadRequestParams::new(upload_id, addr.clone()))
        })
    }

    fn upload_part<'a, 'client: 'a>(
        &'client self,
        params: &'a UploadRequestParams,
        part_number: i32,
        part: ByteStream,
    ) -> BoxFuture<'a, Result<EntityTag, AwsError>> {
        Box::pin(async move {
            let filepath = params.upload_id();
            let etag = EntityTag::new(format!("{filepath}_{part_number}"));
            let bytevec = part.collect().await.map(|data| data.to_vec())?;
            self.write(&bytevec).await?;
            Ok(etag)
        })
    }

    fn complete_upload<'a, 'client: 'a>(
        &'client self,
        params: &'a UploadRequestParams,
        parts: &'a UploadedParts,
    ) -> BoxFuture<'a, Result<EntityTag, AwsError>> {
        let etag = EntityTag::from(format!("{}_{}", params.upload_id(), parts.last_completed()));
        Box::pin(future::ready(Ok(etag)))
    }
}
