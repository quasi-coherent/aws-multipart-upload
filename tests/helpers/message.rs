use futures::{stream, Stream, StreamExt as _};
use rand::{rngs::SmallRng, Rng as _, SeedableRng};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TestEnum {
    Left,
    Right,
}

/// A test item for the upload sink.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TestItem {
    a: String,
    b: i32,
    c: Option<bool>,
    d: TestEnum,
}

impl TestItem {
    pub fn new() -> Self {
        let mut rng = SmallRng::from_rng(&mut rand::rng());
        let a: String = std::iter::repeat(())
            .map(|_| rng.sample(rand::distr::Alphanumeric))
            .take(10)
            .map(char::from)
            .collect();
        let b = rng.random::<i32>();
        let c = if rng.random_bool(0.5) {
            None
        } else {
            let x = rng.random_bool(0.5);
            Some(x)
        };
        let d = if rng.random_bool(0.8) {
            TestEnum::Left
        } else {
            TestEnum::Right
        };
        Self { a, b, c, d }
    }
}

#[derive(Debug, Clone)]
pub struct TestItemStream;

impl TestItemStream {
    pub fn take_items(n: usize) -> impl Stream<Item = TestItem> {
        stream::iter(0..n).map(|_| TestItem::new())
    }

    #[allow(dead_code)]
    pub fn take_bytes(n: usize) -> impl Stream<Item = TestItem> {
        stream::unfold(0, move |bytes| async move {
            if bytes >= n {
                None
            } else {
                let item = TestItem::new();
                let n_ = serde_json::to_vec(&item).unwrap().as_slice().len();
                Some((item, bytes + n_))
            }
        })
    }
}
