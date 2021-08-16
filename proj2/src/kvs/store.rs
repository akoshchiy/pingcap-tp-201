use super::err::Result;
use crate::kvs::file::FileId;
use std::collections::HashMap;
use std::path::PathBuf;

pub struct KvStore {
    mem_table: HashMap<String, TableEntry>,
    // readers: HashMap<u32, BufReader<File>>,
    // writer: BufWriter<File>,
    uncompacted_count: usize,
}

struct TableEntry {
    file_id: FileId,
    offset: u32,
    len: u32,
}

impl KvStore {
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        // let files_res = extract_files(path);
        // if files_res.is_err() {
        //     return Err(files_res.into_err());
        // }
        //
        // let last_file = files_res.unwrap()
        //     .iter()
        //     .filter(|f| { !f.compacted })
        //     .max_by(|a, b| { a.version.cmp(&b.version) })
        //     .unwrap_or(&FileId::new());

        unimplemented!()

        // Ok(KvStore {
        //     mem_table: HashMap::new(),
        //     readers,
        // })
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

// impl Default for KvStore {
//     fn default() -> Self {
//         Self::new()
//     }
// }
