use crate::kvs::err::Result;
use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};

use super::file;
use crate::kvs::err::KvError;
use crate::kvs::err::KvError::Io;
use crate::kvs::server::engine::store::file::{extract_files, FileExtract, FileId};
use crate::kvs::server::engine::store::io::{LogEntry, LogReader, LogWriter};
use crate::kvs::server::engine::KvsEngine;
use crossbeam_skiplist::SkipMap;
use slog::Logger;
use std::fs::{File, OpenOptions};
use std::future::Future;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::process::exit;
use std::sync::atomic::AtomicU32;
use std::sync::{Arc, Mutex};

const DUPLICATE_COUNT_THRESHOLD: u32 = 1000;

pub struct KvStore {
    root_path: PathBuf,
    mem_table: Arc<SkipMap<String, TableEntry>>,
    readers: RefCell<BTreeMap<FileId, LogReader<File>>>,
    writer: Arc<SharedKvStoreWriter>,
}

struct SharedKvStoreWriter(Mutex<KvStoreWriter>);

struct KvStoreWriter {
    current_file: FileId,
    writer: LogWriter<File>,
    duplicate_count: u32,
}

#[derive(Debug, Copy, Clone)]
struct TableEntry {
    file_id: FileId,
    offset: u32,
}

impl KvStore {
    pub fn open(path: impl Into<PathBuf>, thread_size: usize) -> Result<KvStore> {
        let path = path.into();

        let file_extract = extract_files(path.as_path())?;
        let writer = prepare_writer(&file_extract, path.as_path())?;
        let mut readers = prepare_readers(&file_extract, path.as_path())?;
        let table = prepare_table(&mut readers)?;

        Ok(KvStore {
            root_path: path,
            mem_table: Arc::new(table),
            readers: RefCell::new(readers),
            writer: Arc::new(SharedKvStoreWriter(Mutex::new(writer))),
        })
    }

    fn compact(&self, writer: &mut KvStoreWriter) -> Result<()> {
        let file_id = FileId::Compact(writer.current_file.version());
        self.write_compact_file(&file_id);

        let mut reader = open_reader(&file_id, &self.root_path)?;

        fill_table_from(&self.mem_table, file_id, &mut reader)?;

        for kv in self.readers.borrow().deref() {
            remove_file(kv.0, &self.root_path);
        }

        let mut readers = self.readers.borrow_mut();

        readers.clear();
        readers.insert(file_id, reader);

        let append_file_id = FileId::Append(file_id.version() + 1);

        let log_writer = open_writer(&append_file_id, &self.root_path)?;

        writer.writer = log_writer;
        writer.current_file = append_file_id;
        writer.duplicate_count = 0;

        Ok(())
    }

    fn write_compact_file(&self, file_id: &FileId) -> Result<()> {
        let mut writer = open_writer(file_id, self.root_path.as_path())?;

        for pair in self.mem_table.iter() {
            let val = match read_entry(&self.root_path, &self.readers, *pair.value())? {
                Some(val) => val,
                None => continue,
            };
            let entry = LogEntry::Set {
                key: (*pair.key()).clone(),
                val,
            };
            writer.write(entry);
        }

        Ok(())
    }
}

impl Clone for KvStore {
    fn clone(&self) -> Self {
        KvStore {
            root_path: self.root_path.clone(),
            mem_table: self.mem_table.clone(),
            readers: RefCell::new(BTreeMap::new()),
            writer: self.writer.clone(),
        }
    }
}

impl KvsEngine for KvStore {
    fn get(&self, key: String) -> Box<dyn Future<Output=Result<Option<String>>>> {
        // let entry = match self.mem_table.get(&key) {
        //     Some(entry) => entry,
        //     None => return Ok(None),
        // };
        // read_entry(&self.root_path, &self.readers, *entry.value())
        unimplemented!()
    }

    fn set(&self, key: String, value: String) -> Box<dyn Future<Output=Result<()>>> {
        // let mut writer = self.writer.0.lock().unwrap();
        //
        // let offset = writer.writer.pos();
        //
        // writer.writer.write(LogEntry::Set {
        //     key: key.clone(),
        //     val: value,
        // })?;
        //
        // if self.mem_table.contains_key(&key) {
        //     writer.duplicate_count += 1;
        // }
        //
        // self.mem_table.insert(
        //     key,
        //     TableEntry {
        //         file_id: writer.current_file,
        //         offset,
        //     },
        // );
        //
        // if writer.duplicate_count >= DUPLICATE_COUNT_THRESHOLD {
        //     self.compact(&mut writer)?;
        // }
        //
        // Ok(())
        unimplemented!()
    }

    fn remove(&self, key: String) -> Box<dyn Future<Output=Result<()>>> {
        // let mut writer = self.writer.0.lock().unwrap();
        //
        // writer.writer.write(LogEntry::Remove { key: key.clone() })?;
        //
        // writer.duplicate_count += 1;
        //
        // let res = self
        //     .mem_table
        //     .remove(&key)
        //     .map(|e| ())
        //     .ok_or(KvError::KeyNotFound);
        //
        // if writer.duplicate_count >= DUPLICATE_COUNT_THRESHOLD {
        //     self.compact(&mut writer)?;
        // }
        //
        // res
        unimplemented!()
    }
}

fn read_entry(
    root: &Path,
    readers_cell: &RefCell<BTreeMap<FileId, LogReader<File>>>,
    entry: TableEntry,
) -> Result<Option<String>> {
    let mut readers = readers_cell.borrow_mut();

    if !readers.contains_key(&entry.file_id) {
        let reader = open_reader(&entry.file_id, root)?;
        readers.insert(entry.file_id, reader);
    }

    let mut reader = readers.get_mut(&entry.file_id).unwrap();

    reader
        .read_pos(entry.offset)
        .map(|frame| match frame.entry {
            LogEntry::Set { val, .. } => Some(val),
            _ => None,
        })
}

fn prepare_table(
    readers: &mut BTreeMap<FileId, LogReader<File>>,
) -> Result<SkipMap<String, TableEntry>> {
    let table = SkipMap::new();
    for pair in readers {
        fill_table_from(&table, *pair.0, pair.1)?;
    }
    Ok(table)
}

fn fill_table_from(
    table: &SkipMap<String, TableEntry>,
    file_id: FileId,
    reader: &mut LogReader<File>,
) -> Result<()> {
    loop {
        let frame = match reader.read_next()? {
            Some(frame) => frame,
            None => return Ok(()),
        };
        match frame.entry {
            LogEntry::Set { key, .. } => {
                table.insert(
                    key,
                    TableEntry {
                        file_id,
                        offset: frame.offset,
                    },
                );
            }
            LogEntry::Remove { key } => {
                table.remove(&key);
            }
        };
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
            Err(e) => return Err(e),
        };
    }
    Ok(readers)
}

fn prepare_writer(extract: &FileExtract, path: &Path) -> Result<KvStoreWriter> {
    let file_id = extract
        .append_files
        .get(extract.append_files.len() - 1)
        .unwrap();

    open_writer(file_id, path).map(|w| KvStoreWriter {
        current_file: *file_id,
        writer: w,
        duplicate_count: 0,
    })
}

fn open_reader(file_id: &FileId, root_path: &Path) -> Result<LogReader<File>> {
    let file_str: String = file_id.into();
    let file_path = root_path.join(Path::new(&file_str));
    match File::open(file_path.as_path()) {
        Ok(f) => Ok(LogReader::new(f)),
        Err(e) => Err(Io(e)),
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
        Err(e) => Err(Io(e)),
    }
}

fn remove_file(file_id: &FileId, root_path: &Path) -> Result<()> {
    let file_str: String = file_id.into();
    let file_path = root_path.join(Path::new(&file_str));

    std::fs::remove_file(file_path.as_path()).map_err(|e| Io(e))
}

#[cfg(test)]
mod tests {
    use crate::kvs::server::engine::store::kv_store::KvStore;
    use crate::kvs::KvsEngine;
    use tempfile::TempDir;

    #[test]
    fn test_get_non_existent_key() {
        let temp_dir = TempDir::new().unwrap();
        let mut store = KvStore::open(temp_dir.into_path(), 1).unwrap();
        let result = store.get("key1".to_owned()).unwrap();
        assert_eq!(result.is_none(), true);
    }
}
