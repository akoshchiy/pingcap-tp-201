use std::error::Error;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum KvError {
    #[error("file error: {file}")]
    FileError {
        file: String,
    },

    #[error("parse file_id error: {path}")]
    ParseFileIdError {
        path: String,
    },

    #[error("walk_dir error: {path}")]
    WalkDirError {
        path: String,
    },

    #[error("io error")]
    IoError {
        source: std::io::Error,
    },

    #[error("noop")]
    Noop,
}

pub type Result<T> = std::result::Result<T, KvError>;
