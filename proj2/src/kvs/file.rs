use std::path::PathBuf;
use walkdir::WalkDir;

use super::err::Result as Result;
use crate::kvs::err::KvError::{WalkDirError, ParseFileIdError};
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
        let err = ParseFileIdError { path: path.to_string() };
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
            Err(_) => Result::Err(err)
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

struct FileExtract {
    files: Vec<FileId>,
    write_file: FileId,
}

fn extract_files(path: PathBuf) -> Result<FileExtract> {
    let entries = WalkDir::new(path.as_path()).into_iter();

    let mut file_ids = Vec::new();

    for entry in entries {
        if entry.is_err() {
            return Err(
                WalkDirError {
                    path: path.as_path().display().to_string()
                }
            );
        }
        let file_id_res = FileId::parse(
            entry.unwrap().file_name().to_str().unwrap()
        );
        if file_id_res.is_err() {
            return Err(file_id_res.err().unwrap());
        }
        file_ids.push(file_id_res.unwrap());
    }

    let default_file = FileId::new(1, false);

    let last_file = file_ids
        .iter()
        .filter(|f| { !f.compacted })
        .max_by(|a, b| { a.version.cmp(&b.version) })
        .unwrap_or(&default_file)
        .clone();

    Ok(FileExtract {
        files: file_ids,
        write_file: last_file,
    })
}

#[cfg(test)]
mod tests {
    use crate::kvs::file::FileId;
    use std::fs::File;

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
}