use crate::kvs::KvsEngine;
use crate::kvs::Result;
use sled::Db;

pub struct SledKvsEngine {}

impl SledKvsEngine {
    pub fn new(db: Db) -> SledKvsEngine {
        unimplemented!()
    }
}

impl KvsEngine for SledKvsEngine {
    fn get(&mut self, key: String) -> Result<Option<String>> {
        todo!()
    }

    fn set(&mut self, key: String, value: String) -> Result<()> {
        todo!()
    }

    fn remove(&mut self, key: String) -> Result<()> {
        todo!()
    }
}
