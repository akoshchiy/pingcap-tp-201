use std::path::PathBuf;
use std::fs::File;

use crate::kvs;

mod reader;
mod appender;
mod iterator;
mod dir;

// use crate::wal::reader::LogReader;
// pub(crate) use crate::wal::appender::LogAppender;

pub enum Command {
    Set,
    Remove,
}

pub struct Entry {
    pub file_id: usize,
    pub key: String,
    pub cmd: Command,
    pub value_offset: usize,
    pub value_size: usize,
}

fn open_file(path: impl Into<PathBuf>) -> kvs::Result<File> {
    let buf = path.into();
    let buf_path = buf.as_path();

    File::open(buf_path).map_err(|err| {
        kvs::KvError::FileError {
            file: path.display().to_string(),
            source: e,
        }
    })
}

