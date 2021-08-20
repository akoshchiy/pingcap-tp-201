use std::path::{Path, PathBuf};
use walkdir::{DirEntry, WalkDir};

use super::err::Result;
use crate::kvs::err::KvError::{ParseFileIdError, WalkDirError};
use std::panic::panic_any;


#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub(super) enum FileId {
    Compact(u32),
    Append(u32),
    Temp(u32),
}

impl FileId {
    pub fn parse(path: &str) -> Result<FileId> {
        let err = ParseFileIdError {
            path: path.to_string(),
        };
        let split: Vec<_> = path.split("_").collect();

        if split.len() != 2 {
            return Result::Err(err);
        }

        let ver = match split[1].parse::<u32>() {
            Ok(v) => v,
            Err(_) => return Err(err)
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
    pub files: Vec<FileId>,
    pub write_file: FileId,
}

pub(super) fn extract_files(path: &Path) -> Result<FileExtract> {
    let entries = WalkDir::new(path).into_iter();

    let mut file_ids = Vec::new();

    for entry_res in entries {
        let entry = match entry_res {
            Ok(entry) => entry,
            Err(_) => return Err(WalkDirError {
                path: path.display().to_string(),
            }),
        };

        let file_name = entry.file_name().to_str().unwrap();

        if entry.file_type().is_dir() {
            continue;
        }

        let file_id = match FileId::parse(file_name) {
            Ok(file_id) => file_id,
            Err(e) => return Err(e),
        };

        file_ids.push(file_id);
    }

    let default_file = FileId::Append(1);

    let last_file = file_ids
        .iter()
        .filter(|f| f.is_append())
        .max_by(|a, b| a.cmp(&b))
        .unwrap_or(&default_file)
        .clone();

    if file_ids.is_empty() {
        file_ids.push(default_file);
    }

    Ok(FileExtract {
        files: file_ids,
        write_file: last_file,
    })
}

#[cfg(test)]
mod tests {
    use crate::kvs::file::{extract_files, FileId};
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
        assert_eq!(file_extract.files.len(), 1);

        assert_eq!(file_extract.files[0].is_compacted(), false);
        assert_eq!(file_extract.files[0].version(), 1);

        assert_eq!(file_extract.write_file.version(), 1);
        assert_eq!(file_extract.write_file.is_compacted(), false);
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
        assert_eq!(res.is_ok(), true);

        let file_extract = res.unwrap();
        assert_eq!(file_extract.files.len(), 3);

        assert_eq!(file_extract.files[0].is_compacted(), false);
        assert_eq!(file_extract.files[0].version(), 1);

        assert_eq!(file_extract.files[1].is_compacted(), false);
        assert_eq!(file_extract.files[1].version(), 2);

        assert_eq!(file_extract.files[2].is_compacted(), true);
        assert_eq!(file_extract.files[2].version(), 1);

        assert_eq!(file_extract.write_file.is_compacted(), false);
        assert_eq!(file_extract.write_file.version(), 2);
    }

    #[test]
    fn test_file_id_ord() {
        let mut file_ids = vec![
            FileId::Append(2),
            FileId::Compact(2),
            FileId::Temp(2),
            FileId::Temp(1),
            FileId::Compact(1),
            FileId::Append(1),
        ];

        file_ids.sort();



        assert_eq!(6, file_ids.len());


    }
}
