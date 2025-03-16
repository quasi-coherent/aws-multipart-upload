use chrono::format::{Item, StrftimeItems};
use chrono::Utc;

use super::api::UploadAddress;
use crate::AwsError;

/// Iterator of S3 addresses where the prefix is derived from the current time
/// and a format string.
#[derive(Debug, Clone)]
pub struct Timestamped<'a> {
    fmt_items: Vec<Item<'a>>,
    bucket: String,
    prefix: Option<String>,
}

impl<'a> Timestamped<'a> {
    /// This generator of upload destinations has a fixed key but the value is
    /// the current time, formatted according to `fmt`, which is an `srtftime`
    /// style format string.
    ///
    /// The optional `prefix` is a directory-style string prefix to prepend to
    /// the `key` of `UploadAddress` this upload is going to.
    /// For example,
    ///
    /// ```rust
    /// let mut ts = Timestamped::new("my-bucket", "%Y/%d")
    ///     .unwrap()
    ///     .with_prefix("a/b/c");
    /// let addr = ts.next();
    /// println!("{addr:?}");
    /// // Prints `Some(UploadAddress { bucket: "my-bucket", key: "a/b/c/2025/02" })`.
    /// ```
    pub fn new(bucket: &str, fmt: &'a str) -> Result<Self, AwsError> {
        let fmt_items = StrftimeItems::new(fmt).parse()?;
        Ok(Self {
            fmt_items,
            bucket: bucket.to_string(),
            prefix: None,
        })
    }

    pub fn with_prefix(mut self, prefix: &str) -> Self {
        self.prefix = Some(prefix.to_string());
        self
    }

    fn now_formatted(&self) -> String {
        let now = Utc::now();
        now.format_with_items(self.fmt_items.as_slice().iter())
            .to_string()
    }

    fn upload_addr(&self) -> UploadAddress {
        let mut buf = String::new();
        let now = self.now_formatted();
        let Some(ref pfx) = self.prefix else {
            buf.push_str(&now);
            return UploadAddress::new(self.bucket.to_string(), buf);
        };
        if !pfx.ends_with("/") {
            buf.push_str(&format!("{}/{}", pfx, now));
        } else {
            buf.push_str(&format!("{}{}", pfx, now));
        };

        UploadAddress::new(self.bucket.to_string(), buf)
    }
}

impl Iterator for Timestamped<'_> {
    type Item = UploadAddress;

    fn next(&mut self) -> Option<Self::Item> {
        Some(self.upload_addr())
    }
}
