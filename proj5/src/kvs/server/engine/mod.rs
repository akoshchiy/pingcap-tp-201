pub mod sled_eng;
pub mod store;

use crate::kvs::err::Result;
use futures::future::BoxFuture;
use std::future::Future;

pub trait KvsEngine: Clone + Send + Sync + 'static {
    fn get(&self, key: String) -> BoxFuture<Result<Option<String>>>;

    fn set(&self, key: String, value: String) -> BoxFuture<Result<()>>;

    fn remove(&self, key: String) -> BoxFuture<Result<()>>;
}
