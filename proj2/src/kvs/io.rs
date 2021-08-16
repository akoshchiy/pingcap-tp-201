use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom};

use super::err;

use super::err::Result;
use crate::kvs::err::KvError;
use crate::kvs::err::KvError::{Noop, IoError};
use std::convert::TryInto;
use std::path::Path;

use serde::{Deserialize, Serialize};

const FRAME_HEADER_SIZE: usize = 4;

#[derive(Serialize, Deserialize)]
#[serde(tag = "cmd")]
pub(super) enum LogEntry {
    Set { key: String, val: String },
    Remove { key: String },
}

pub(super) struct LogReader<R> {
    reader: BufReader<R>,
    pos: u32,
}

pub(super) struct LogFrame {
    entry: LogEntry,
    offset: u32,
    size: u32,
}

impl<R: Read + Seek> LogReader<R> {
    pub(super) fn new(reader: R) -> LogReader<R> {
        LogReader {
            reader: BufReader::new(reader),
            pos: 0,
        }
    }

    pub(super) fn read_next(&mut self) -> Result<LogFrame> {
        let frame = self.read_pos(self.pos)?;
        self.pos += frame.size;
        Ok(frame)
    }

    pub(super) fn read_pos(&mut self, pos: u32) -> Result<LogFrame> {
        self.reader.seek(SeekFrom::Start(pos as u64));
        let size = self.read_size()?;

        let mut vec: Vec<u8> = Vec::with_capacity(size as usize);
        let read_res = self.reader.read_exact(vec.as_mut_slice());

        if read_res.is_err() {
            return Err(IoError { source: read_res.err().unwrap() });
        }

        let entry_res = bincode::deserialize::<LogEntry>(vec.as_slice());

        match entry_res {
            Ok(entry) => Ok(LogFrame {
                size: (FRAME_HEADER_SIZE + vec.len()) as u32,
                offset: pos,
                entry,
            }),
            Err(_) => Err(Noop),
        }
    }

    pub(super) fn pos(&self) -> u32 {
        self.pos
    }

    fn read_size(&mut self) -> Result<u32> {
        let mut buf = [0u8; FRAME_HEADER_SIZE];
        let res = self.reader.read_exact(&mut buf);
        match res {
            Ok(_) => Ok(u32::from_be_bytes(buf)),
            Err(e) => Err(IoError { source: e })
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::kvs::io::LogReader;
    use std::fs::{File, ReadDir};

    #[test]
    fn test_reader() {
        // LogReader::new()
        // bytebuffer::B
        //
        // LogReader::new()
    }
}
