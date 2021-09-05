pub mod sled_eng;
pub mod store;

use crate::kvs::err::Result;

pub trait KvsEngine {
    fn get(&mut self, key: String) -> Result<Option<String>>;

    fn set(&mut self, key: String, value: String) -> Result<()>;

    fn remove(&mut self, key: String) -> Result<()>;
}
