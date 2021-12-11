use crate::kvs::err::KvError::{KeyNotFound, Sled, SledAccess, Ut8Conversion};
use crate::kvs::Result;
use crate::kvs::{KvError, KvsEngine};
use sled::Db;

#[derive(Clone)]
pub struct SledKvsEngine {
    db: Db,
}

impl SledKvsEngine {
    pub fn new(db: Db) -> SledKvsEngine {
        SledKvsEngine { db }
    }

    fn flush(&self) -> Result<()> {
        self.db
            .flush()
            .map(|_| ())
            .map_err(|err| KvError::Sled(err))
    }
}

impl Drop for SledKvsEngine {
    fn drop(&mut self) {
        self.flush();
    }
}

impl KvsEngine for SledKvsEngine {
    fn get(&self, key: String) -> Result<Option<String>> {
        let buf_opt = match self.db.get(key.as_bytes()) {
            Ok(res) => res,
            Err(e) => return Err(SledAccess { key, source: e }),
        };

        let res = match buf_opt {
            Some(buf) => match String::from_utf8(buf.to_vec()) {
                Ok(val) => Some(val),
                Err(e) => return Err(Ut8Conversion { key, source: e }),
            },
            None => None,
        };

        Ok(res)
    }

    fn set(&self, key: String, value: String) -> Result<()> {
        self.db
            .insert(key.as_bytes(), value.as_bytes())
            .map(|_| ())
            .map_err(|err| SledAccess { key, source: err })?;
        self.flush()
    }

    fn remove(&self, key: String) -> Result<()> {
        let res = self.db.remove(key.as_bytes());
        match res {
            Ok(val) => match val {
                Some(_) => Ok(()),
                None => Err(KeyNotFound),
            },
            Err(e) => Err(SledAccess { key, source: e }),
        }?;
        self.flush()
    }
}
