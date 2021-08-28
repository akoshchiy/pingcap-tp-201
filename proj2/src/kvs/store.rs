use super::err::Result;
use crate::kvs::file::{extract_files, FileExtract, FileId};
use std::collections::{BTreeMap, HashMap};

use super::file;
use crate::kvs::err::KvError::{Noop, Io};
use crate::kvs::io::{LogReader, LogWriter, LogFrame, LogEntry};
use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};
use std::process::exit;
use crate::kvs::err::KvError;

const DUPLICATE_COUNT_THRESHOLD: u32 = 1000;

pub struct KvStore {
    root_path: PathBuf,
    mem_table: HashMap<String, TableEntry>,
    readers: BTreeMap<FileId, LogReader<File>>,
    writer: (FileId, LogWriter<File>),
    duplicate_count: u32,
}

#[derive(Debug, Copy, Clone)]
struct TableEntry {
    file_id: FileId,
    offset: u32,
}

impl KvStore {
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let path = path.into();

        let file_extract = extract_files(path.as_path())?;
        let writer = prepare_writer(&file_extract, path.as_path())?;
        let mut readers = prepare_readers(&file_extract, path.as_path())?;
        let table = prepare_table(&mut readers)?;

        Ok(KvStore {
            root_path: path,
            mem_table: table,
            readers,
            writer,
            duplicate_count: 0,
        })
    }

    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        let entry = match self.mem_table.get(&key) {
            Some(entry) => entry,
            None => return Ok(None),
        };
        read_entry(&mut self.readers, *entry)
    }

    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let offset = self.writer.1.pos();

        self.writer.1.write(LogEntry::Set { key: key.clone(), val: value })?;

        if self.mem_table.contains_key(&key) {
            self.duplicate_count += 1;
        }

        self.mem_table.insert(key, TableEntry {
            file_id: self.writer.0,
            offset,
        });

        if self.duplicate_count >= DUPLICATE_COUNT_THRESHOLD {
            self.compact()?;
        }

        Ok(())
    }

    pub fn remove(&mut self, key: String) -> Result<()> {
        self.writer.1.write(LogEntry::Remove { key: key.clone() })?;

        self.duplicate_count += 1;

        let res = self.mem_table.remove(&key)
            .map(|e| ())
            .ok_or(KvError::KeyNotFound);

        if self.duplicate_count >= DUPLICATE_COUNT_THRESHOLD {
            self.compact()?;
        }

        res
    }

    fn compact(&mut self) -> Result<()> {
        let file_id = FileId::Compact(self.writer.0.version());
        self.write_compact_file(&file_id);

        let mut reader = open_reader(&file_id, &self.root_path)?;

        fill_table_from(
            &mut self.mem_table,
            file_id,
            &mut reader
        )?;

        for kv in &self.readers {
            remove_file(kv.0, &self.root_path);
        }

        self.readers.clear();
        self.readers.insert(file_id, reader);

        let append_file_id = FileId::Append(file_id.version() + 1);

        let writer = open_writer(&append_file_id, &self.root_path)?;
        self.writer = (append_file_id, writer);

        let append_reader = open_reader(&append_file_id, &self.root_path)?;
        self.readers.insert(append_file_id, append_reader);

        self.duplicate_count = 0;

        Ok(())
    }

    fn write_compact_file(&mut self, file_id: &FileId) -> Result<()> {
        let mut writer = open_writer(file_id, self.root_path.as_path())?;

        for pair in &mut self.mem_table {
            let val = match read_entry(&mut self.readers, *pair.1)? {
                Some(val) => val,
                None => continue,
            };
            let entry = LogEntry::Set { key: (*pair.0).clone(), val };
            writer.write(entry);
        };

        Ok(())
    }
}

fn read_entry(
    readers: &mut BTreeMap<FileId, LogReader<File>>,
    entry: TableEntry
) -> Result<Option<String>> {

    let mut reader = match readers.get_mut(&entry.file_id) {
        Some(reader) => reader,
        None => return Ok(None),
    };

    reader.read_pos(entry.offset)
        .map(|frame| {
            match frame.entry {
                LogEntry::Set { val, .. } => Some(val),
                _ => None,
            }
        })
}

fn prepare_table(readers: &mut BTreeMap<FileId, LogReader<File>>) -> Result<HashMap<String, TableEntry>> {
    let mut table = HashMap::new();
    for pair in readers {
        fill_table_from(&mut table, *pair.0, pair.1)?;
    }
    Ok(table)
}

fn fill_table_from(
    table: &mut HashMap<String, TableEntry>,
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
                table.insert(key, TableEntry {
                    file_id,
                    offset: frame.offset,
                });
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
            Err(_) => return Err(Noop),
        };
    }
    Ok(readers)
}

fn prepare_writer(extract: &FileExtract, path: &Path) -> Result<(FileId, LogWriter<File>)> {
    let file_id = extract.append_files
        .get(extract.append_files.len() - 1)
        .unwrap();

    open_writer(file_id, path)
        .map(|w| (*file_id, w))
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

    std::fs::remove_file(file_path.as_path())
        .map_err(|e| Io(e))
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;
    use crate::kvs::KvStore;

    #[test]
    fn test_get_non_existent_key() {
        let temp_dir = TempDir::new().unwrap();
        let mut store = KvStore::open(temp_dir.into_path()).unwrap();
        let result = store.get("key1".to_owned()).unwrap();
        assert_eq!(result.is_none(), true);
    }
}
