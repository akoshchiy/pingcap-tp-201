pub mod sled_eng;
pub mod store;

use crate::kvs::err::Result;

pub trait KvsEngine: Clone + Send + 'static {
    fn get(&self, key: String) -> Result<Option<String>>;

    fn set(&self, key: String, value: String) -> Result<()>;

    fn remove(&self, key: String) -> Result<()>;
}
