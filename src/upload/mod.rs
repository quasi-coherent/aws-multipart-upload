//! This module contains the core multipart upload implementations.
use crate::client::{SendCompleteUpload, SendRequest, SendUploadPart, UploadClient};
use crate::codec::EncodedPart;
use crate::error::{Error as UploadError, ErrorRepr, Result};
use crate::sdk::api::CreateRequest;
use crate::sdk::{CompletedUpload, ObjectUri, PartBody, UploadData};

use futures::Stream;
use multipart_write::FusedMultipartWrite;
use multipart_write::prelude::*;
use multipart_write::stream::{FeedMultipartWrite, MultipartStreamExt};
use std::fmt::{self, Debug, Formatter};
use std::marker::PhantomData;
use std::pin::Pin;

mod encoded;
pub use encoded::Encoded;

mod multipart_upload;
pub use multipart_upload::MultipartUpload;

mod part_buf;
pub use part_buf::PartBuf;

pub mod state;
use state::*;

pub mod uri;

/// Extension trait for combining streams and multipart uploads.
pub trait MultipartUploadStreamExt: Stream {
    /// Transform a stream into results of using the provided encoder `E` to
    /// create repeated [`PartBody`].
    ///
    /// The closure `F` is a checkpoint, deciding when to pause writing items to
    /// a part and convert it to a `PartBody`.
    fn encode_parts<E, F>(self, encoder: E, f: F) -> FeedMultipartWrite<Self, Encoded<E>, F>
    where
        E: EncodedPart<Self::Item> + Unpin,
        F: FnMut(PartProgress) -> bool,
        Self: Sized,
    {
        let encoded = Encoded::new(encoder);
        self.feed_multipart_write(encoded, f)
    }

    /// Transform a stream whose item type is `PartBody` into a stream of
    /// [`CompletedUpload`], the response returned when requesting an active
    /// multipart upload be completed, by forwarding it to a `MultipartWrite`r.
    ///
    /// The supplied closure evaluates on the return type of the writer and
    /// determines when the writer is to complete.
    ///
    /// # Fuse behavior
    ///
    /// This adapter requires that the writer implement `FusedMultipartWrite`.
    /// This is so that in case the writer has terminated we can end the stream
    /// gracefully.
    ///
    /// In the case of [`MultipartUpload`], which needs an active upload to not
    /// be terminated, a combinator such as [`bootstrapped`] can keep the writer
    /// from terminating after completing uploads, but only if a new one can be
    /// started automatically.  The [`ObjectUriIterator`] trait provides the
    /// only required parameter to create a new upload--the `ObjectUri`--so this
    /// is useful in that case.
    ///
    /// [`bootstrapped`]: multipart_upload::MultipartUploadExt::bootstrapped
    /// [`ObjectUriIterator`]: uri::ObjectUriIterator
    fn upload_as_parts<Wr, F>(self, upload: Wr, f: F) -> FeedMultipartWrite<Self, Wr, F>
    where
        Self: Stream<Item = PartBody> + Sized,
        Wr: FusedMultipartWrite<PartBody, Output = CompletedUpload, Error = UploadError>,
        F: FnMut(Wr::Ret) -> bool,
    {
        self.feed_multipart_write(upload, f)
    }
}

/// A builder for multipart upload types.
pub struct MultipartUploadBuilder<C> {
    client: C,
    target_upload: TargetUpload,
    target_part: TargetPart,
}

impl<C: SendRequest + Send + Sync + 'static> MultipartUploadBuilder<C> {
    /// Initialize a `MultipartUploadBuilder` from a client implementation.
    pub fn new(client: C) -> Self {
        MultipartUploadBuilder {
            client,
            target_upload: TargetUpload::default(),
            target_part: TargetPart::default(),
        }
    }

    /// Set target values for the upload.
    pub fn target_upload(mut self, target_upload: TargetUpload) -> Self {
        self.target_upload = target_upload;
        self
    }

    /// Set target values for the parts in an upload.
    pub fn target_part(mut self, target_part: TargetPart) -> Self {
        self.target_part = target_part;
        self
    }

    /// Returns a multipart upload that is not active because it has no upload
    /// ID and target object URI.
    ///
    /// Set the upload with [`with_upload_data`] or use a `MultipartWrite`
    /// combinator like [`bootstrapped`] to activate or keep the writer active
    /// indefinitely.
    pub fn build_inactive_upload(self) -> MultipartUpload<PartBuf> {
        let client = UploadClient::new(self.client);
        let buf = PartBuf::with_capacity(self.target_upload.capacity);
        MultipartUpload::new_inactive(buf, client)
    }

    pub fn build_uploader<E, Item>(self, encoder: E) -> MultipartUploader<E, Item> {
        let encoded = Encoded::new(encoder);
        let client = UploadClient::new(self.client);
        let buf = PartBuf::with_capacity(self.target_upload.capacity);
        let upload = MultipartUpload::new_inactive(buf, client.clone());
        MultipartUploader {
            encoded,
            upload,
            client,
            target_upload: self.target_upload,
            target_part: self.target_part,
            upload_state: UploadState::default(),
            part_state: PartState::default(),
            _it: std::marker::PhantomData,
        }
    }
}

/// `MultipartUploader` is a type wrapping a particular encoder for `Item` and a
/// `MultipartUpload` for uploading the parts that the encoder builds from it,
/// and exposes methods to perform the full lifecycle of a multipart upload.
pub struct MultipartUploader<E, Item> {
    encoded: Encoded<E>,
    upload: MultipartUpload<PartBuf>,
    client: UploadClient,
    target_upload: TargetUpload,
    target_part: TargetPart,
    upload_state: UploadState,
    part_state: PartState,
    _it: PhantomData<Item>,
}

impl<E, Item> MultipartUploader<E, Item>
where
    E: EncodedPart<Item> + Unpin,
{
    /// Get the current state of the in-progress part.
    pub fn current_part(&self) -> PartState {
        self.part_state
    }

    /// Get the current state of the in-progress upload.
    pub fn current_upload(&self) -> UploadState {
        self.upload_state
    }

    /// Returns whether the current in-progress part has reached the target
    /// size this uploader was configured with.
    pub fn is_part_complete(&mut self) -> bool {
        self.target_part
            .part_complete(self.part_state.total_bytes, self.part_state.total_items)
    }

    /// Returns whether the current in-progress upload has reached the target
    /// size this uploader was configured with.
    pub fn is_upload_complete(&mut self) -> bool {
        self.target_upload.upload_complete(
            self.upload_state.total_bytes,
            self.upload_state.total_parts,
            self.upload_state.last_part_bytes,
        )
    }

    /// Returns whether there is an upload in-progress.
    ///
    /// If this returns `false` then it is permitted to create a new upload on
    /// this type with [`start_new_upload`].
    pub fn has_active_upload(&self) -> bool {
        self.upload.is_terminated()
    }

    /// Start a new upload with the destination object URI, returning the
    /// previous upload data.
    ///
    /// # Errors
    ///
    /// This returns an error if there is already an active upload or if there
    /// was an error found in the response.
    pub async fn start_new_upload(&mut self, uri: ObjectUri) -> Result<UploadData> {
        if self.has_active_upload() {
            return Err(ErrorRepr::UploadStillActive)?;
        }
        let req = CreateRequest::new(uri);
        let data = self.client.send_create_upload_request(req).await?;
        let old = Pin::new(&mut self.upload).reactivate(data)?;
        Ok(old)
    }

    /// Write an encoded item, returning the progress of the current part.
    ///
    /// # Errors
    ///
    /// Returns an error if the encoding resulted in an error.
    pub async fn send_encoded(&mut self, it: Item) -> Result<PartProgress> {
        let progress = self.encoded.send(it).await?;
        self.part_state.update(progress.total_bytes);
        Ok(progress)
    }

    /// Flush the encoder to return a completed part.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization of the part upload body was not
    /// successful.
    pub async fn build_part_body(&mut self) -> Result<PartBody> {
        self.encoded.flush().await?;
        let body = self.encoded.complete().await?;
        self.part_state = PartState::default();
        Ok(body)
    }

    /// Create a new part upload request and write it to the uploader.
    ///
    /// # Errors
    ///
    /// This returns an error if the upload is no longer available.
    pub async fn upload_part(&mut self, body: PartBody) -> Result<UploadProgress> {
        let progress = self.upload.send(body).await?;
        self.upload_state.update(progress.last_part_bytes);
        Ok(progress)
    }

    /// Finishes all pending part uploads.
    ///
    /// # Errors
    ///
    /// This returns an error if the upload is no longer available or if a part
    /// upload resolves to an error.  In this case, the pending buffer is
    /// preserved and `finish_pending` can be called again, but it is not
    /// possible to recover the original part.
    pub async fn finish_pending(&mut self) -> Result<()> {
        self.upload.flush().await?;
        Ok(())
    }

    /// Complete the multipart upload, returning the data from the AWS response.
    ///
    /// This closes the uploader and any method call after will return an error
    /// until a new upload is added to it.
    ///
    /// # Errors
    ///
    /// This returns an error if the upload is no longer available or if the
    /// complete upload request resolved to an error.
    pub async fn complete_upload(&mut self) -> Result<CompletedUpload> {
        self.upload.flush().await?;
        let completed = self.upload.complete().await?;
        self.upload_state = UploadState::default();
        Ok(completed)
    }
}

impl<E: Debug, Item> Debug for MultipartUploader<E, Item> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("MultipartUploader")
            .field("encoded", &self.encoded)
            .field("upload", &self.upload)
            .field("client", &self.client)
            .field("target_upload", &self.target_upload)
            .field("target_part", &self.target_part)
            .field("upload_state", &self.upload_state)
            .field("part_state", &self.part_state)
            .finish()
    }
}
