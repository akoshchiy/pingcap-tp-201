use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom, Write, IoSliceMut};

use super::err;

use super::err::Result;
use crate::kvs::err::KvError;
use crate::kvs::err::KvError::{IoError, Noop};
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
        let mut vec: Vec<u8> = vec![0; size as usize];
        let read_res = self.reader.read_exact(vec.as_mut_slice());

        if read_res.is_err() {
            return Err(IoError {
                source: read_res.err().unwrap(),
            });
        }

        let entry_res = bson::from_slice(vec.as_slice());

        match entry_res {
            Ok(entry) => Ok(LogFrame {
                size: vec.len() as u32,
                offset: pos,
                entry,
            }),
            Err(err) => Err(Noop),
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
            Err(e) => Err(IoError { source: e }),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::kvs::io::{LogEntry, LogReader};
    use std::borrow::Borrow;
    use std::fs::{File, ReadDir};
    use std::io::Cursor;

    #[test]
    fn test_reader() {
        let expected_entry = LogEntry::Set {
            key: "key1".to_string(),
            val: "val".to_string(),
        };
        let buf = serialize_entry(&expected_entry);
        let entry_size = buf.len() as u32;

        let mut reader = LogReader::new(Cursor::new(buf));

        let res = reader.read_next();
        // assert_eq!(res.is_ok(), true);

        let frame = res.unwrap();
        assert_eq!(frame.offset, 0);
        assert_eq!(frame.size + 4, entry_size);

        if let LogEntry::Set { key, val } = frame.entry {
            assert_eq!(key, "key1");
            assert_eq!(val, "val");
        } else {
            unreachable!()
        }
    }

    fn serialize_entry(entry: &LogEntry) -> Vec<u8> {
        let mut entry_bytes = bson::to_vec(&entry).unwrap();
        let size = entry_bytes.len() as u32;
        let mut size_bytes: [u8; 4] = size.to_be_bytes();
        let mut buf = Vec::with_capacity((size + 4) as usize);
        buf.extend_from_slice(&size_bytes);
        buf.extend_from_slice(entry_bytes.as_slice());
        buf
    }
}
