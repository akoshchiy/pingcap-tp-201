
use super::err::Result;
use crate::kvs::err::KvError::{IoError, Noop};
use std::convert::TryInto;
use std::path::Path;

use serde::{Deserialize, Serialize};
use std::io::{SeekFrom, BufReader, Seek, Read, Write, BufWriter};

const FRAME_HEADER_SIZE: usize = 4;

#[derive(Serialize, Deserialize)]
#[serde(tag = "cmd")]
pub(super) enum LogEntry {
    Set { key: String, val: String },
    Remove { key: String },
}

pub(super) struct LogFrame {
    entry: LogEntry,
    offset: u32,
    size: u32,
}

pub(super) struct LogReader<R: Read + Seek> {
    reader: BufReader<R>,
    pos: u32,
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
        let seek_res = self.reader.seek(SeekFrom::Start(pos as u64));
        if seek_res.is_err() {
            return Err(Noop);
        }

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

pub(super) struct LogWriter<W: Write> {
    writer: BufWriter<W>,
}

impl<W: Write> LogWriter<W> {
    pub(super) fn new(writer: W) -> LogWriter<W> {
        LogWriter {
            writer: BufWriter::new(writer),
        }
    }

    pub(super) fn write(&mut self, entry: &LogEntry) -> Result<()> {
        let entry_buf = match bson::to_vec(entry) {
            Ok(entry_buf) => entry_buf,
            Err(err) => return Err(Noop),
        };

        let len = entry_buf.len() as u32;

        if let Err(e) = self.write_size(len) {
            return Err(Noop);
        }

        let write_res = self.writer.write(entry_buf.as_slice());
        match write_res {
            Ok(_) => (),
            Err(e) => return Err(Noop),
        }

        match self.writer.flush() {
            Ok(_) => Ok(()),
            Err(e) => return Err(Noop),
        }
    }

    fn write_size(&mut self, size: u32) -> Result<()> {
        let mut buf: [u8; FRAME_HEADER_SIZE] = size.to_be_bytes();
        let res = self.writer.write(&mut buf);
        match res {
            Ok(_) => Ok(()),
            Err(e) => Err(Noop),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::kvs::io::{LogEntry, LogReader, LogWriter};
    use std::borrow::Borrow;
    use std::io::{Cursor, Write, Read};

    struct WriteBuffer {
        buf: Vec<u8>,
    }

    impl Write for WriteBuffer {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            &self.buf.extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl WriteBuffer {
        fn new() -> WriteBuffer {
            WriteBuffer { buf: Vec::new() }
        }

        fn buf(&self) -> &[u8] {
            self.buf.as_slice()
        }
    }

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

    #[test]
    fn test_writer() {
        let entry = LogEntry::Set {
            key: "key1".to_string(),
            val: "val".to_string(),
        };

        let mut write_buf = WriteBuffer::new();
        {
            let mut writer = LogWriter::new(&mut write_buf);
            let res = writer.write(&entry);
        }

        let buf = write_buf.buf();

        let result_entry = deserialize_entry(buf);

        if let LogEntry::Set { key, val } = result_entry {
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

    fn deserialize_entry(buf: &[u8]) -> LogEntry {
        let mut cursor = Cursor::new(buf);

        let mut size_buf = [0u8; 4];
        cursor.read_exact(&mut size_buf).unwrap();

        let size = u32::from_be_bytes(size_buf);

        let mut entry_buf = vec![0u8; size as usize];
        cursor.read_exact(&mut entry_buf).unwrap();

        bson::from_slice(entry_buf.as_slice()).unwrap()
    }
}
