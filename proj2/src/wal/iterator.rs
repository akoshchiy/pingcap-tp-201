use std::fs::File;
use crate::wal::Entry;
use std::io::Read;
use crate::wal::dir::WalDir;

pub(crate) struct WalIterator {
    index: usize,
    file: File,
    dir: WalDir
}

impl WalIterator {

    pub(crate) fn next(&mut self) -> Entry {
        // self.file.read()
        unimplemented!("not yet")
    }
}
