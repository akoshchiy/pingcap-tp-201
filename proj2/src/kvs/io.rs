use std::io::BufReader;
use std::fs::File;

use super::err;

use super::err::Result as Result;

pub(super) enum LogEntry {
    Set { key: String, val: String },
    Remove { key: String },
}

pub(super) struct LogReader {
    reader: BufReader<File>,
    pos: u32,
}

impl LogReader {
    pub fn read_next(&mut self) -> Result<LogEntry> {
        unimplemented!()
    }

    pub(super) fn read_pos(&mut self, pos: u32) -> Result<LogEntry> {
        unimplemented!()
    }
}