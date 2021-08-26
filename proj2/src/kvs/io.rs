use super::err::Result;
use crate::kvs::err::KvError::{Io, Noop, DeserializeEntry, SerializeEntry};
use std::convert::TryInto;
use std::path::Path;

use serde::{Deserialize, Serialize};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write, ErrorKind};
use std::fs::ReadDir;
use crate::kvs::err::KvError;

const FRAME_HEADER_SIZE: usize = 4;

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "cmd")]
pub enum LogEntry {
    Set { key: String, val: String },
    Remove { key: String },
}

#[derive(Debug)]
pub(super) struct LogFrame {
    pub entry: LogEntry,
    pub offset: u32,
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

    pub(super) fn read_next(&mut self) -> Result<Option<LogFrame>> {
        match self.read_pos(self.pos) {
            Ok(f) => Ok(Some(f)),
            Err(e) => {
                match e {
                    KvError::Io(io_err) => {
                        match io_err.kind() {
                            ErrorKind::UnexpectedEof => Ok(None),
                            _ => Err(Io(io_err)),
                        }
                    },
                    _ => Err(e)
                }
            }
        }
    }

    pub(super) fn read_pos(&mut self, pos: u32) -> Result<LogFrame> {
        self.seek_pos(pos)?;

        let size = self.read_size()?;

        let mut vec: Vec<u8> = vec![0; size as usize];
        self.read_exact(vec.as_mut_slice())?;

        self.pos = pos + FRAME_HEADER_SIZE as u32 + size;

        let entry_res = bson::from_slice(vec.as_slice());
        match entry_res {
            Ok(entry) => Ok(LogFrame {
                offset: pos,
                entry,
            }),
            Err(err) => Err(DeserializeEntry {
                pos,
                source: err,
            }),
        }
    }

    pub(super) fn pos(&self) -> u32 {
        self.pos
    }

    fn read_size(&mut self) -> Result<u32> {
        let mut buf = [0u8; FRAME_HEADER_SIZE];
        self.read_exact(&mut buf)
            .map(|_| u32::from_be_bytes(buf))
    }

    fn seek_pos(&mut self, pos: u32) -> Result<()> {
        let res = self.reader.seek(SeekFrom::Start(pos as u64));
        match res {
            Ok(_) => Ok(()),
            Err(e) => Err(Io(e)),
        }
    }

    fn read_exact(&mut self, buf: &mut [u8]) -> Result<()> {
        self.reader
            .read_exact(buf)
            .map_err(|e| Io(e))
    }
}

pub(super) struct LogWriter<W: Write> {
    writer: BufWriter<W>,
    pos: u32,
}

impl<W: Write> LogWriter<W> {
    pub(super) fn new(writer: W) -> LogWriter<W> {
        LogWriter {
            writer: BufWriter::new(writer),
            pos: 0,
        }
    }

    pub(super) fn write(&mut self, entry: LogEntry) -> Result<()> {
        let entry_buf = bson::to_vec(&entry)
            .map_err(|e| SerializeEntry { entry, source: e })?;

        let len = entry_buf.len() as u32;

        self.write_size(len)?;

        self.writer
            .write(entry_buf.as_slice())
            .map_err(|e| Io(e))?;

        self.writer
            .flush()
            .map_err(|e| Io(e))?;

        self.pos += FRAME_HEADER_SIZE as u32 + len;

        Ok(())
    }

    pub(super) fn pos(&self) -> u32 {
        self.pos
    }

    fn write_size(&mut self, size: u32) -> Result<()> {
        let mut buf: [u8; FRAME_HEADER_SIZE] = size.to_be_bytes();
        let res = self.writer.write(&mut buf);
        match res {
            Ok(_) => Ok(()),
            Err(e) => Err(Io(e)),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::kvs::io::{LogEntry, LogReader, LogWriter};
    use std::borrow::Borrow;
    use std::io::{Cursor, Read, Write};

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

        let frame = res.unwrap().unwrap();
        assert_eq!(frame.offset, 0);

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
            let res = writer.write(entry);
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
