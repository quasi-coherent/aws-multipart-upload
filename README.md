<h1 align="center">aws-multipart-upload</h1>
<br />
<div align="center">
  <a href="https://crates.io/crates/aws-multipart-upload">
    <img src="https://img.shields.io/crates/v/aws-multipart-upload.svg?style=flat-square"
    alt="Crates.io version" /></a>
  <a href="https://docs.rs/aws-multipart-upload">
    <img src="https://img.shields.io/badge/docs-latest-blue.svg?style=flat-square" alt="docs.rs docs" /></a>
</div>

## Description

A crate in alpha with a `futures::Sink` for doing AWS S3 multipart uploads with the official [SDK][sdk].

Current:
* `Upload`: For the finite case, send items to `Upload` until they are exhausted or the (optional)
  target upload size is reached.  Flushing or closing the sink completes the upload.  Attempting to
  send more items after an upload is an error.  For this reason, it's probably not a good idea to
  configure it with a target upload size, unless it's really a "maximum upload size" and a very safe
  upper bound on the total bytes.
  - The `Upload` sink can also start a new upload to address the write-after-upload error by calling
    `poll_new_upload`.

[sdk]: https://docs.rs/aws-sdk-s3/latest/aws_sdk_s3/index.html
