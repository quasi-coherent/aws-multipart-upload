# aws-multipart-upload

WIP rough implementation of two `futures::Sink`s for doing an AWS S3 multipart upload with the
official [SDK](https://docs.rs/aws-sdk-s3/latest/aws_sdk_s3/index.html):

* `Upload`: For the finite case, send items to `Upload` until they are exhausted or the (optional)
  target upload size is reached.  Flushing or closing the sink completes the upload.  Attempting to
  send more items after an upload is an error.  For this reason, it's probably not a good idea to
  configure it with a target upload size, unless it's really a "maximum upload size" and a very safe
  upper bound on the total bytes.
* `UploadForever`: For the infinite case, `UploadForever` uses an iterator of S3 addresses to
  continuously build and upload parts, complete uploads when the target upload size is achieved, and
  then start new uploads from the `next` address iterator.  If `next` returns `None`, this is an
  error.

TODO(qcoh):
* Configuration of the object in S3.
* Configuration of internal buffers.
* Better builder interface.
* Improve `Upload` API to be more... intuitive?
* Can an `AsyncWrite` be used as the internal buffer?  `tokio::fs::File`?
