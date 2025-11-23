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

A high-level API for building and working with AWS S3 multipart uploads using the official [SDK] for
Rust.

## Motivation

Making an AWS S3 multipart upload is a fairly involved multi-stage process:

1. Send a request to create the multipart upload; preserve the ID of the upload from the response.
2. Build parts for the upload, which generally follow the pattern:
   - Repeatedly write to some buffer of bytes, keeping track of how many bytes have been written.
     AWS imposes a minimum and maximum size for a part.
   - It is critical also to keep track of the part number.  When a part should be sent, collect the
     bytes along with part number, upload ID, and object URI into the request object.  A successful
     response contains the entity tag of the part.  This must be stored with the exact part number
     that was used.
3. Repeat until the upload should be completed, which typically involves tracking another counter for
   bytes written.  The request to complete the upload needs the ID, object URI, and the complete
   collection of part number paired with entity tag.  Send the request; an empty 200 response means
   it succeeded.

The official AWS Rust SDK is generated code that exposes request builders that can be initialized
and sent from a client, including the several mentioned above, but there isn't much beyond that.

The `aws-multipart-upload` crate aims to simplify this process and do so with abstractions that
integrate cleanly with the parts of the Rust ecosystem one is likely to be using, or that one would
like to be using, when performing multipart uploads.

## Example

Add the crate to your Cargo.toml:

```toml
aws-multipart-upload = "0.1.0-rc4"
```

The feature flag `"csv"` enables a "part encoder"--the component responsible for writing items to a
part--built from a [`csv`][csv-docsio] writer.  Part encoders for writing jsonlines and for writing
arbitrary lines of text are available as well.

This example shows a stream of `serde_json::Value`s being written as comma-separated values to a
multipart upload.  This is a future and awaiting the future runs the stream to completion by writing
and uploading parts behind the scenes, completing the upload when the stream is exhausted.

```rust
use aws_multipart_upload::{ByteSize, SdkClient, UploadBuilder};
use aws_multipart_upload::codec::{CsvBuilder, CsvEncoder};
use aws_multipart_upload::write::UploadStreamExt as _;
use futures::stream::{self, StreamExt as _};
use serde_json::{Value, json};

/// Default aws-sdk-s3 client:
let client = SdkClient::defaults().await;

/// Use `UploadBuilder` to build a multipart uploader:
let upl = UploadBuilder::new(client)
    .with_encoding(CsvBuilder)
    .with_part_size(ByteSize::mib(10))
    .with_uri(("example-bucket-us-east-1", "destination/key.csv"))
    .build::<Value, CsvEncoder>();

/// Consume a stream of `Value`s by forwarding it to `upl`,
/// and poll for completion:
let values = stream::iter(0..).map(|n| json!({"n": n, "n_sq": n * n}));
let completed = values
    .take(100000)
    .collect_upload(upl)
    .await
    .unwrap();

println!("object uploaded: {}", completed.uri);
```

[SDK]: https://awslabs.github.io/aws-sdk-rust/
[csv-docsio]: https://docs.rs/csv/latest/csv/
