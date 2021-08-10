use std::path::{Path, PathBuf};
use std::fs::{File, read, OpenOptions};
use std::collections::HashMap;

use walkdir::WalkDir;

use crate::wal::{open_file, FileId, Error};
use crate::wal::file::FileId::Compact;
use crate::wal::Result;
use crate::wal::Error::{FileIoError, FileNameParseError, WalkDirError};
use std::backtrace::Backtrace;
use crate::wal::dir::FileId::{Append, Compact};
use std::num::ParseIntError;

const MAX_FILE_LEN: u64 = 1024 * 1024; // 1mb

#[derive(Copy, Clone)]
pub enum FileId {
    Append(u16),
    Compact(u16),
}

impl FileId {
    const WAL_APPEND: &'static str = "wal_append_";
    const WAL_COMPACT: &'static str = "wal_compact_";

    pub fn parse(file_name: String) -> Result<FileId> {
        let parts: Vec<String> = file_name.split("_").collect();
        let last_part = parts.get(parts.len() - 1);

        let idx = match last_part {
            Some(val) => {
                match val.parse::<u16>() {
                    Ok(idx) => idx,
                    Err(e) => Err(FileId::file_name_err(file_name, Option::Some(e)))
                }
            }
            None => return Err(FileId::file_name_err(file_name, Option::Some(e)))
        };

        if file_name.starts_with(FileId::WAL_APPEND) {
            return Ok(FileId::Append(idx));
        }

        if file_name.starts_with(FileId::WAL_COMPACT) {
            return Ok(FileId::Compact(idx));
        }

        Err(FileId::file_name_err(file_name.clone(), Option::None))
    }

    fn file_name_err(name: String, source: Option<ParseIntError>) -> Error {
        FileNameParseError {
            name,
            source,
            backtrace: Backtrace::capture(),
        }
    }

    pub fn is_append(&self) -> bool {
        match self {
            Append(_id) => true,
            _ => false,
        }
    }

    pub fn to_file_name(&self) -> String {
        match self {
            Append(id) => format!("wal_append_{}", id),
            Compact(id) => format!("wal_compact_{}", id),
        }
    }
}

pub struct WalDir {
    dir_path: PathBuf,
    read_files: HashMap<FileId, File>,
    append_file: (FileId, File),
}

impl WalDir {
    pub fn open(path: &Path) -> Result<WalDir> {
        let file_ids_res = WalDir::walk_dir(&path);

        if file_ids_res.is_err() {
            return Err(file_ids_res.err().unwrap());
        }

        let mut read_files = HashMap::new();

        let file_ids = file_ids_res.unwrap();

        for file_id in &file_ids {
            let file_res = WalDir::open_file(&path, false, file_id);
            if file_res.is_err() {
                return Err(file_res.err().unwrap());
            }
            read_files[file_id] = file_res.unwrap();
        }

        let append_file_idx = WalDir::find_append_file_id(&file_ids);
        let append_file_id = FileId::Append(append_file_idx);

        let append_file_res = WalDir::open_file(&path, true, &append_file_id);
        if append_file_res.is_err() {
            return Err(append_file_res.err().unwrap());
        }

        Ok(WalDir {
            dir_path: path.into(),
            read_files,
            append_file: (append_file_id, append_file_res.unwrap()),
        })
    }

    fn walk_dir(path: &Path) -> Result<Vec<FileId>> {
        let entries = WalkDir::new(path).into_iter();

        let mut file_ids = Vec::new();

        for entry in entries {
            if entry.is_err() {
                return Err(
                    WalkDirError {
                        path: path.display().to_string(),
                        source: entry.err().unwrap(),
                        backtrace: Backtrace::capture(),
                    }
                );
            }
            let file_id_res = FileId::parse(
                entry.unwrap().file_name().into()
            );
            if file_id_res.is_err() {
                return Err(file_id_res.err().unwrap());
            }
            file_ids.push(file_id_res.unwrap());
        }

        Ok(file_ids)
    }

    pub fn get_append_file(&mut self) -> Result<&File> {
        let len = self.append_file.metadata()
            .map(|meta| { meta.len() })
            .unwrap_or(0);
        if len >= MAX_FILE_LEN {
            let res = self.open_append_file();
            if res.is_err() {
                return res.map(|f| { &f });
            }
        }
        Ok(&self.append_file.1)
    }

    pub fn delete_read_file(&self, id: FileId) {}

    pub fn get_read_file(&self, id: FileId) -> Option<&File> {
        self.read_files[id]
    }

    pub fn new_compacted_file(&self) -> Result<&File> {
        let idx = self.current_compact_idx();
        let file_id = FileId::Compact(idx + 1);

        let file_res = self.create_file(&file_id)
            .map(|f| { &f });

        if file_res.is_err() {
            return file_res;
        }
        self.read_files[file_id] = file_res.unwrap();
        file_res
    }

    fn current_compact_idx(&self) -> u16 {
        self.read_files
            .iter()
            .filter(|pair| {
                match pair.0 {
                    Compact(_idx) => true,
                    _ => false,
                }
            })
            .max_by(|pair| { pair.0.0 })
            .map_or(0, |pair| { pair.0.0 })
    }

    fn find_append_file_idx(file_ids: &[FileId]) -> u16 {
        file_ids.into_iter()
            .filter(|id| {
                match id {
                    Append(_id) => true,
                    _ => false,
                }
            })
            .max_by(|id| { id.0 })
            .map(|id| { id.0 })
            .unwrap_or(1)
    }

    fn open_append_file(&mut self) -> Result<File> {
        let file_id = FileId::Append(self.append_file.0.0 + 1);
        let append_res = self.open_file(true, &file_id);
        if append_res.is_err() {
            return append_res;
        }
        self.append_file = (file_id.clone(), append_res.unwrap());

        let read_res = self.open_file(false, &file_id);
        if read_res.is_err() {
            return read_res;
        }
        self.read_files[file_id] = read_res.unwrap();

        return read_res;
    }

    fn open_file(path: &Path, append: bool, id: &FileId) -> Result<File> {
        let mut buf = PathBuf::new();
        buf.push(path);
        buf.push(id.to_file_name());

        OpenOptions::new()
            .read(true)
            .append(append)
            .create(true)
            .open(buf.as_path())
            .map_err(|err| {
                Error::FileIoError {
                    path: path.display().to_string(),
                    source: e,
                    backtrace: Backtrace::capture(),
                }
            })
    }
}

