use crate::kvs::LogEntry;
use std::error::Error;
use thiserror::Error;
use std::str::Utf8Error;
use std::string::FromUtf8Error;

#[derive(Error, Debug)]
pub enum KvError {
    #[error("parse file_id error: {path}")]
    ParseFileId { path: String },

    #[error("walk_dir error: {path} source -> {source}")]
    Dir {
        path: String,
        source: walkdir::Error,
    },

    #[error(transparent)]
    Io(std::io::Error),

    #[error("deserialize entry failed at pos: {pos}, source -> {source}")]
    DeserializeEntry { pos: u32, source: bson::de::Error },

    #[error("serialize entry failed: {entry:?}, source -> {source}")]
    SerializeEntry {
        entry: LogEntry,
        source: bson::ser::Error,
    },

    #[error("Key not found")]
    KeyNotFound,

    #[error("sled error, key: {key}, source -> {source}")]
    Sled {
        key: String,
        source: sled::Error,
    },

    #[error("ut8 conversion error, key: {key}, source -> {source}")]
    Ut8Conversion {
        key: String,
        source: FromUtf8Error,
    },
}

pub type Result<T> = std::result::Result<T, KvError>;
