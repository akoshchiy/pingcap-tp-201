use std::error::Error;
use thiserror::Error;
use crate::kvs::io::{LogFrame, LogEntry};

#[derive(Error, Debug)]
pub enum KvError {

    #[error("parse file_id error: {path}")]
    ParseFileId { path: String },

    #[error("walk_dir error: {path} source -> {source}")]
    Dir { path: String, source: walkdir::Error },

    #[error(transparent)]
    Io(std::io::Error),

    #[error("deserialize entry failed at pos: {pos}, source -> {source}")]
    DeserializeEntry { pos: u32, source: bson::de::Error },

    #[error("serialize entry failed: {entry:?}, source -> {source}")]
    SerializeEntry { entry: LogEntry, source: bson::ser::Error },

    #[error("noop")]
    Noop,
}

pub type Result<T> = std::result::Result<T, KvError>;
