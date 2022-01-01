use crate::kvs::LogEntry;
use std::error::Error;
use std::str::Utf8Error;
use std::string::FromUtf8Error;
use thiserror::Error;
use tokio::sync::oneshot::error::RecvError;

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
    SledAccess { key: String, source: sled::Error },

    #[error(transparent)]
    Sled(sled::Error),

    #[error("ut8 conversion error, key: {key}, source -> {source}")]
    Ut8Conversion { key: String, source: FromUtf8Error },

    #[error(transparent)]
    BsonSerialize(bson::ser::Error),

    #[error(transparent)]
    BsonDeserialize(bson::de::Error),

    #[error("server error: {msg}")]
    Server { msg: String },

    #[error("server unexpected result: {val}")]
    UnexpectedResult { val: String },

    #[error("pool build error: {msg}")]
    PoolBuild { msg: String },

    #[error(transparent)]
    OneshotRecv(RecvError),
}

pub type Result<T> = std::result::Result<T, KvError>;

impl From<std::io::Error> for KvError {
    fn from(e: std::io::Error) -> Self {
        KvError::Io(e)
    }
}

impl From<bson::ser::Error> for KvError {
    fn from(e: bson::ser::Error) -> Self {
        KvError::BsonSerialize(e)
    }
}

impl From<bson::de::Error> for KvError {
    fn from(e: bson::de::Error) -> Self {
        KvError::BsonDeserialize(e)
    }
}
