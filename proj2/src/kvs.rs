use std::collections::HashMap;
use std::error::Error;
use std::path::PathBuf;

pub struct KvStore {
    store: HashMap<String, String>,
}

#[derive(Debug)]
pub enum KvError {
}

pub type Result<T> = std::result::Result<T, KvError>;

impl KvStore {

    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        unimplemented!()
    }

    pub fn get(&self, key: String) -> Result<Option<String>> {
        // self.store.get(&key).cloned()
        unimplemented!()
    }

    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        // self.store.insert(key, value);
        unimplemented!()
    }

    pub fn remove(&mut self, key: String) -> Result<()> {
        // self.store.remove(&key);
        unimplemented!()
    }
}

// impl Default for KvStore {
//     fn default() -> Self {
//         Self::new()
//     }
// }
