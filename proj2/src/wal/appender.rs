use std::fs::File;
use crate::wal::dir::{FileId, WalDir};
use std::io::{Seek, Write};

pub(crate) struct LogAppender<'a> {
    dir: &'a WalDir
}

impl LogAppender {
    pub fn append(&mut self, key: String, value: String) {

        let file = self.dir.get_append_file().unwrap();

    }
}

struct AppendFrame {
    size: u32,
    data: Vec<u8>
}

struct AppendEntry {
    key: Vec<u8>,


}

