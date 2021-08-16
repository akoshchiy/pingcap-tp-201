use std::path::{Path, PathBuf};
use walkdir::{DirEntry, WalkDir};

use super::err::Result;
use crate::kvs::err::KvError::{ParseFileIdError, WalkDirError};
use std::panic::panic_any;

#[derive(Clone, Debug)]
pub(super) struct FileId {
    version: u32,
    compacted: bool,
}

impl FileId {
    fn new(ver: u32, compacted: bool) -> FileId {
        FileId {
            version: ver,
            compacted,
        }
    }

    fn parse(path: &str) -> Result<FileId> {
        let err = ParseFileIdError {
            path: path.to_string(),
        };
        let split: Vec<_> = path.split("_").collect();
        if split.len() != 3 {
            return Result::Err(err);
        }
        if split[0] != "log" {
            return Result::Err(err);
        }
        let compacted = split[1] == "c";
        let ver = split[2].parse::<u32>();

        match ver {
            Ok(id) => Result::Ok(FileId {
                version: id,
                compacted,
            }),
            Err(_) => Result::Err(err),
        }
    }

    fn inc_version(&self) -> FileId {
        FileId {
            version: self.version + 1,
            compacted: self.compacted,
        }
    }

    fn as_compacted(&self) -> FileId {
        FileId {
            version: self.version,
            compacted: true,
        }
    }
}

impl From<FileId> for String {
    fn from(file_id: FileId) -> Self {
        let c = if file_id.compacted { "c" } else { "a" };
        format!("log_{}_{}", c, file_id.version)
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
        if entry_res.is_err() {
            return Err(WalkDirError {
                path: path.display().to_string(),
            });
        }
        let entry = entry_res.unwrap();

        let file_name = entry.file_name().to_str().unwrap();

        if entry.file_type().is_dir() {
            continue;
        }

        let file_id_res = FileId::parse(file_name);
        if file_id_res.is_err() {
            return Err(file_id_res.err().unwrap());
        }
        file_ids.push(file_id_res?);
    }

    let default_file = FileId::new(1, false);

    let last_file = file_ids
        .iter()
        .filter(|f| !f.compacted)
        .max_by(|a, b| a.version.cmp(&b.version))
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
        let res1 = FileId::parse("log_a_1");
        assert_eq!(res1.is_err(), false);
        let file_id = res1.unwrap();
        assert_eq!(file_id.compacted, false);
        assert_eq!(file_id.version, 1);

        let res2 = FileId::parse("log_c_2");
        assert_eq!(res2.is_err(), false);
        let file_id_2 = res2.unwrap();
        assert_eq!(file_id_2.compacted, true);
        assert_eq!(file_id_2.version, 2);
    }

    #[test]
    fn test_file_id_to_string() {
        let s1: String = FileId::new(12, false).into();
        assert_eq!("log_a_12".to_string(), s1);

        let s2: String = FileId::new(10, true).into();
        assert_eq!("log_c_10".to_string(), s2);
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

        assert_eq!(file_extract.files[0].compacted, false);
        assert_eq!(file_extract.files[0].version, 1);

        assert_eq!(file_extract.write_file.version, 1);
        assert_eq!(file_extract.write_file.compacted, false);
    }

    #[test]
    fn test_extract_dir() {
        let dir = tempfile::Builder::new()
            .prefix("temporary-dir")
            .rand_bytes(5)
            .tempdir()
            .unwrap();

        let file_path_a_1 = dir.path().join("log_a_1");
        let file_path_c_1 = dir.path().join("log_c_1");
        let file_path_a_2 = dir.path().join("log_a_2");

        let file_a_1 = File::create(file_path_a_1).unwrap();
        let file_c_1 = File::create(file_path_c_1).unwrap();
        let file_a_2 = File::create(file_path_a_2).unwrap();

        let res = extract_files(dir.path());
        assert_eq!(res.is_ok(), true);

        let file_extract = res.unwrap();
        assert_eq!(file_extract.files.len(), 3);

        assert_eq!(file_extract.files[0].compacted, false);
        assert_eq!(file_extract.files[0].version, 1);

        assert_eq!(file_extract.files[1].compacted, false);
        assert_eq!(file_extract.files[1].version, 2);

        assert_eq!(file_extract.files[2].compacted, true);
        assert_eq!(file_extract.files[2].version, 1);

        assert_eq!(file_extract.write_file.compacted, false);
        assert_eq!(file_extract.write_file.version, 2);
    }
}
