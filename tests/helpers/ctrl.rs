use aws_multipart_upload::types::UploadControl;

#[derive(Debug, Clone)]
pub struct TestControl {
    pub part_size: usize,
    pub upload_size: usize,
}

impl Default for TestControl {
    fn default() -> Self {
        Self {
            part_size: 512,
            upload_size: 5 * 1024 * 1024,
        }
    }
}

impl TestControl {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_part_size(mut self, n: usize) -> Self {
        self.part_size = n;
        self
    }

    pub fn with_upload_size(mut self, n: usize) -> Self {
        self.upload_size = n;
        self
    }
}

impl UploadControl for TestControl {
    fn target_part_size(&self) -> usize {
        self.part_size
    }

    fn is_upload_ready(&self, upload_size: usize, _: usize) -> bool {
        self.upload_size <= upload_size
    }
}
