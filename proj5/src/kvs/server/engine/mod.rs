pub mod sled_eng;
pub mod store;

use std::future::Future;
use crate::kvs::err::Result;

pub trait KvsEngine: Clone + Send + 'static {
    fn get(&self, key: String) -> Box<dyn Future<Output=Result<Option<String>>>>;

    fn set(&self, key: String, value: String) -> Box<dyn Future<Output=Result<()>>>;

    fn remove(&self, key: String) -> Box<dyn Future<Output=Result<()>>>;
}
