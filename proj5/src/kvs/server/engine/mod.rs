pub mod sled_eng;
pub mod store;

use crate::kvs::err::Result;
use std::future::Future;
use futures::future::BoxFuture;

pub trait KvsEngine: Clone + Send + 'static {
    fn get(&self, key: String) -> BoxFuture<Result<Option<String>>>;

    fn set(&self, key: String, value: String) -> BoxFuture<Result<()>>;

    fn remove(&self, key: String) -> BoxFuture<Result<()>>;
}
