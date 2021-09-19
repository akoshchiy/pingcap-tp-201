use std::path::{Path, PathBuf};
use walkdir::{DirEntry, WalkDir};

use crate::kvs::err::KvError::{Dir, ParseFileId};
use crate::kvs::err::Result;
use std::panic::panic_any;

#[derive(Debug, Clone, Copy, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub(super) enum FileId {
    Compact(u32),
    Append(u32),
    Temp(u32),
}

impl FileId {
    pub fn parse(path: &str) -> Result<FileId> {
        let err = ParseFileId {
            path: path.to_string(),
        };
        let split: Vec<_> = path.split("_").collect();

        if split.len() != 2 {
            return Result::Err(err);
        }

        let ver = match split[1].parse::<u32>() {
            Ok(v) => v,
            Err(_) => return Err(err),
        };

        match split[0] {
            "c" => Ok(FileId::Compact(ver)),
            "a" => Ok(FileId::Append(ver)),
            "t" => Ok(FileId::Temp(ver)),
            _ => Err(err),
        }
    }

    pub fn is_append(&self) -> bool {
        match self {
            FileId::Append(_) => true,
            _ => false,
        }
    }

    pub fn is_compacted(&self) -> bool {
        match self {
            FileId::Compact(_) => true,
            _ => false,
        }
    }

    pub fn is_temp(&self) -> bool {
        match self {
            FileId::Temp(_) => true,
            _ => false,
        }
    }

    pub fn version(&self) -> u32 {
        match self {
            FileId::Append(v) => *v,
            FileId::Compact(v) => *v,
            FileId::Temp(v) => *v,
        }
    }

    fn to_string(&self) -> String {
        match self {
            FileId::Append(v) => format!("a_{}", v),
            FileId::Compact(v) => format!("c_{}", v),
            FileId::Temp(v) => format!("t_{}", v),
        }
    }
}

impl From<&FileId> for String {
    fn from(file_id: &FileId) -> Self {
        file_id.to_string()
    }
}

impl From<FileId> for String {
    fn from(file_id: FileId) -> Self {
        file_id.to_string()
    }
}

pub(super) struct FileExtract {
    pub compact_files: Vec<FileId>,
    pub append_files: Vec<FileId>,
    pub temp_files: Vec<FileId>,
    pub last_version: u32,
}

pub(super) fn extract_files(path: impl AsRef<Path>) -> Result<FileExtract> {
    let path_ref = path.as_ref();

    let entries = WalkDir::new(path_ref).into_iter();

    let mut append_files = Vec::new();
    let mut compact_files = Vec::new();
    let mut temp_files = Vec::new();

    let mut last_version: u32 = 0;

    for entry_res in entries {
        let entry = match entry_res {
            Ok(entry) => entry,
            Err(e) => {
                return Err(Dir {
                    path: path_ref.display().to_string(),
                    source: e,
                });
            }
        };

        let file_name = entry.file_name().to_str().unwrap();

        if entry.file_type().is_dir() {
            continue;
        }

        let file_id = FileId::parse(file_name)?;

        match file_id {
            FileId::Append(_) => append_files.push(file_id),
            FileId::Compact(_) => compact_files.push(file_id),
            FileId::Temp(_) => temp_files.push(file_id),
        }

        let ver = file_id.version();
        if ver > last_version {
            last_version = ver;
        }
    }

    if append_files.is_empty() {
        append_files.push(FileId::Append(last_version + 1));
    }

    append_files.sort();
    compact_files.sort();
    temp_files.sort();

    Ok(FileExtract {
        compact_files,
        append_files,
        temp_files,
        last_version,
    })
}

#[cfg(test)]
mod tests {
    use crate::kvs::server::engine::store::file::{extract_files, FileId};
    use std::collections::{HashMap, HashSet};
    use std::fs::File;
    use tempfile::{NamedTempFile, TempDir};

    #[test]
    fn test_file_id_parse() {
        let res1 = FileId::parse("a_1");
        assert_eq!(res1.is_err(), false);
        let file_id = res1.unwrap();
        assert_eq!(file_id.is_compacted(), false);
        assert_eq!(file_id.version(), 1);

        let res2 = FileId::parse("c_2");
        assert_eq!(res2.is_err(), false);
        let file_id_2 = res2.unwrap();
        assert_eq!(file_id_2.is_compacted(), true);
        assert_eq!(file_id_2.version(), 2);
    }

    #[test]
    fn test_file_id_to_string() {
        let s1: String = FileId::Append(12).into();
        assert_eq!("a_12".to_string(), s1);

        let s2: String = FileId::Compact(10).into();
        assert_eq!("c_10".to_string(), s2);
    }

    #[test]
    fn test_extract_empty_dir() {
        let temp_dir = tempfile::Builder::new()
            .prefix("temporary-dir")
            .tempdir()
            .unwrap();

        let res = extract_files(temp_dir.path());

        assert_eq!(res.is_ok(), true);
        let file_extract = res.unwrap();

        assert_eq!(file_extract.append_files.len(), 1);
        assert_eq!(file_extract.temp_files.len(), 0);
        assert_eq!(file_extract.compact_files.len(), 0);
    }

    #[test]
    fn test_extract_dir() {
        let dir = tempfile::Builder::new()
            .prefix("temporary-dir")
            .rand_bytes(5)
            .tempdir()
            .unwrap();

        let file_path_a_1 = dir.path().join("a_1");
        let file_path_c_1 = dir.path().join("c_1");
        let file_path_a_2 = dir.path().join("a_2");

        let file_a_1 = File::create(file_path_a_1).unwrap();
        let file_c_1 = File::create(file_path_c_1).unwrap();
        let file_a_2 = File::create(file_path_a_2).unwrap();

        let res = extract_files(dir.path());
        // assert_eq!(res.is_ok(), true);

        let file_extract = res.unwrap();
        assert_eq!(file_extract.append_files.len(), 2);

        assert_eq!(file_extract.append_files[0].is_append(), true);
        assert_eq!(file_extract.append_files[0].version(), 1);

        assert_eq!(file_extract.append_files[1].is_append(), true);
        assert_eq!(file_extract.append_files[1].version(), 2);

        assert_eq!(file_extract.compact_files.len(), 1);
        assert_eq!(file_extract.compact_files[0].is_compacted(), true);
        assert_eq!(file_extract.compact_files[0].version(), 1);

        assert_eq!(file_extract.last_version, 2);
    }

    #[test]
    fn test_file_id_as_key_map() {
        let mut map: HashMap<FileId, usize> = HashMap::new();

        map.insert(FileId::Append(1), 1);
        map.insert(FileId::Append(2), 2);
        map.insert(FileId::Compact(3), 3);

        assert_eq!(map.get(&FileId::Append(1)).unwrap(), &1);
        assert_eq!(map.get(&FileId::Append(2)).unwrap(), &2);
        assert_eq!(map.get(&FileId::Compact(3)).unwrap(), &3);
    }
}
