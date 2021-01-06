use std::path::{Path, PathBuf};
use walkdir::WalkDir;
use std::fs::File;

const MAX_FILE_LEN: u64 = 10 * 1024 * 1024;

pub struct WalDir {
    dir_path: Path,
    files: Vec<String>,
    current_idx: usize,
    current_file: File,
}

impl WalDir {
    pub fn open(path: &Path) -> WalDir {
        let entries = WalkDir::new(path).into_iter();
        entries.map(|res| {
            res.and_then(|entry| {
                entry.file_name(
                    entry.metadata().map(|meta| { meta. })
            })
        })
    }

    pub fn current_file(&mut self) -> &File {
        let len = self.current_file.metadata()
            .map(|meta| { meta.len() })
            .unwrap_or(0);

        if len >= MAX_FILE_LEN {
            self.create_file();
        }

        &self.current_file
    }

    fn create_file(&mut self) {
        PathBuf::new()
            // .


    }
}

