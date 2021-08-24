use super::err::Result;
use crate::kvs::file::{extract_files, FileExtract, FileId};
use std::collections::{BTreeMap, HashMap};

use super::file;
use crate::kvs::err::KvError::Noop;
use crate::kvs::io::{LogReader, LogWriter};
use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};
use std::process::exit;

pub struct KvStore {
    mem_table: HashMap<String, TableEntry>,
    readers: BTreeMap<FileId, LogReader<File>>,
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

        let readers = prepare_readers(&file_extract, path.as_path())?;
        let writer = prepare_writer(&file_extract, path.as_path())?;

        let mut store = KvStore {
            mem_table: HashMap::new(),
            readers,
            writer,
            uncompacted_count: 0,
        };

        store.fill_mem_table();

        Ok(store)
    }

    fn fill_mem_table(&mut self) {
        let table = &mut self.mem_table;
        let readers = &mut self.readers;

        for pair in readers {
            let file_id = *pair.0;
            let reader = pair.1;
            loop {
                let frame = match reader.read_next() {
                    Ok(frame) => frame,
                    Err(e) => return,
                };
            }
        }
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

fn prepare_readers(
    extract: &FileExtract,
    path: &Path,
) -> Result<BTreeMap<FileId, LogReader<File>>> {
    let mut readers = BTreeMap::new();

    let mut files = Vec::with_capacity(2);

    if !extract.compact_files.is_empty() {
        let last_compact_file = &extract.compact_files[extract.compact_files.len() - 1];
        files.push(last_compact_file);
    }

    if !extract.append_files.is_empty() {
        let last_append_file = &extract.append_files[extract.append_files.len() - 1];
        files.push(last_append_file);
    }

    for file in files {
        match open_reader(file, path) {
            Ok(reader) => {
                readers.insert(file.clone(), reader);
            }
            Err(_) => return Err(Noop),
        };
    }
    Ok(readers)
}

fn prepare_writer(extract: &FileExtract, path: &Path) -> Result<LogWriter<File>> {
    let first_append_file = FileId::Append(extract.last_version + 1);

    let file_id = extract
        .append_files
        .get(extract.append_files.len() - 1)
        .unwrap_or(&first_append_file);

    open_writer(file_id, path)
}

fn open_reader(file_id: &FileId, root_path: &Path) -> Result<LogReader<File>> {
    let file_str: String = file_id.into();
    let file_path = root_path.join(Path::new(&file_str));
    match File::open(file_path.as_path()) {
        Ok(f) => Ok(LogReader::new(f)),
        Err(e) => Err(Noop),
    }
}

fn open_writer(file_id: &FileId, root_path: &Path) -> Result<LogWriter<File>> {
    let file_str: String = file_id.into();
    let file_path = root_path.join(Path::new(&file_str));

    let file_res = OpenOptions::new()
        .write(true)
        .create(true)
        .open(file_path.as_path());

    match file_res {
        Ok(f) => Ok(LogWriter::new(f)),
        Err(e) => Err(Noop),
    }
}
