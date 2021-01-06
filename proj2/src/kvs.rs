
use std::collections::HashMap;
use std::path::PathBuf;
use thiserror::Error;
use std::fs::File;
use std::io::{Write, Error};

use crate::wal::LogWriter;

pub struct KvStore {
    store: HashMap<String, String>,
    log: LogWriter,
}

#[derive(Error, Debug)]
pub enum KvError {
    #[error("file error: {file}")]
    FileError { file: String, source: Error }
}

pub type Result<T> = std::result::Result<T, KvError>;

impl KvStore {

    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let buf = path.into();
        let buf_path = buf.as_path();

        LogWriter::open(buf_path).map(|log| {
            KvStore {
                store: log.read_all(),
                log,
            }
        })
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
