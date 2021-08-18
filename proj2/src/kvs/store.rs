use super::err::Result;
use crate::kvs::file::{FileId, extract_files};
use std::collections::HashMap;

use super::file;
use std::path::{PathBuf, Path};
use crate::kvs::err::KvError::Noop;
use crate::kvs::io::{LogReader, LogWriter};
use std::fs::{File, OpenOptions};
use std::str::pattern::Pattern;

pub struct KvStore {
    mem_table: HashMap<String, TableEntry>,
    readers: HashMap<u32, LogReader<File>>,
    writer: LogWriter<File>,
    uncompacted_count: usize,
}

struct TableEntry {
    file_id: FileId,
    offset: u32,
    len: u32,
}

impl KvStore {
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let path = path.into();

        let file_extract_res = extract_files(path.as_path());

        let file_extract = match file_extract_res {
            Ok(file_extract) => file_extract,
            Err(e) => return Err(Noop),
        };

        let readers = prepare_readers(&file_extract.files, path.as_path())?;
        let writer = open_writer(&file_extract.write_file, path.as_path())?;

        let mut store = KvStore {
            mem_table: HashMap::new(),
            readers,
            writer,
            uncompacted_count: 0
        };

        store.fill_mem_table();

        Ok(store)
    }

    fn fill_mem_table(&mut self) {
        let table = &mut self.mem_table;
    }

    pub fn get(&self, key: String) -> Result<Option<String>> {
        // self.store.get(&key).cloned()
        unimplemented!()
    }

    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        // self.store.insert(key, value);
        unimplemented!()
    }

    pub fn remove(&mut self, key: String) -> Result<()> {
        // self.store.remove(&key);
        unimplemented!()
    }
}

fn prepare_readers(files: &[FileId], path: &Path) -> Result<HashMap<u32, LogReader<File>>> {
    let mut map = HashMap::new();
    for file in files {
        match open_reader(file, path) {
            Ok(reader) => {
                map.insert(file.version, reader);
            },
            Err => return Err(Noop)
        };
    }
    Ok(map)
}

fn open_reader(file_id: &FileId, root_path: &Path) -> Result<LogReader<File>> {
    let file_str: String = file_id.into();
    let file_path = root_path.join(Path::new(&file_str)).as_path();
    match File::open(file_path) {
        Ok(f) => Ok(LogReader::new(f)),
        Err(e) => Noop
    }
}

fn open_writer(file_id: &FileId, root_path: &Path) -> Result<LogWriter<File>> {
    let file_str: String = file_id.into();
    let file_path = root_path.join(Path::new(&file_str)).as_path();

    let file_res = OpenOptions::new()
        .write(true)
        .create(true)
        .open(file_path);

    match file_res {
        Ok(f) => Ok(LogWriter::new(f)),
        Err(e) => Noop
    }
}