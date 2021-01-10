use std::path::PathBuf;
use std::fs::File;
use thiserror::Error as ThisError;


use crate::kvs;
use crate::wal::FileId::{Append, Compact};
use crate::kvs::KvError;
use std::collections::HashMap;
use std::process::id;

mod reader;
mod appender;
mod iterator;
mod dir;

pub use file::FileId;
use std::num::ParseIntError;
use std::backtrace::Backtrace;


pub type Result<T> = std::result::Result<T, Error>;

#[derive(ThisError, Debug)]
pub enum Error {
    #[error("waldir error: {}", path)]
    WalkDirError {
        path: String,
        source: walkdir::Error,
        backtrace: Backtrace,
    },
    #[error("wal io error: {}", path)]
    FileIoError {
        path: String,
        source: std::io::Error,
        backtrace: Backtrace,
    },
    #[error("wal file_name parse failed: {}", name)]
    FileNameParseError {
        name: String,
        source: Option<ParseIntError>,
        backtrace: Backtrace,
    },
}

pub enum Command {
    Set,
    Remove,
}


pub struct Entry {
    pub key: String,
    pub cmd: Command,
    pub value_offset: usize,
    pub value_size: usize,
}