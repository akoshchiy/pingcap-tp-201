use crate::kvs::err::KvError::{KeyNotFound, Sled, SledAccess, Ut8Conversion};
use crate::kvs::thread_pool::ThreadPool;
use crate::kvs::Result;
use crate::kvs::{KvError, KvsEngine};
use futures::future::BoxFuture;
use futures::{FutureExt, TryFutureExt};
use sled::Db;
use std::future::Future;
use std::ops::Deref;
use tokio::sync::oneshot::channel;

#[derive(Clone)]
pub struct SledKvsEngine<P: ThreadPool> {
    db: Db,
    pool: P,
}

impl<P: ThreadPool> SledKvsEngine<P> {
    pub fn new(db: Db, threads: u32) -> Result<SledKvsEngine<P>> {
        let pool = P::new(threads)?;
        Ok(SledKvsEngine { db, pool })
    }
}

impl<P: ThreadPool> Drop for SledKvsEngine<P> {
    fn drop(&mut self) {
        flush(&self.db);
    }
}

impl<P: ThreadPool> KvsEngine for SledKvsEngine<P> {
    fn get(&self, key: String) -> BoxFuture<Result<Option<String>>> {
        let (sender, receiver) = channel::<Result<Option<String>>>();

        let db = self.db.clone();

        self.pool.spawn(move || {
            let buf_opt = match db.get(key.as_bytes()) {
                Ok(res) => res,
                Err(e) => {
                    sender.send(Err(SledAccess { key, source: e })).unwrap();
                    return;
                }
            };

            let res = match buf_opt {
                Some(buf) => match String::from_utf8(buf.to_vec()) {
                    Ok(val) => Some(val),
                    Err(e) => {
                        sender.send(Err(Ut8Conversion { key, source: e })).unwrap();
                        return;
                    }
                },
                None => None,
            };
            sender.send(Ok(res)).unwrap();
        });
        receiver.map(|res| res.unwrap()).boxed()
    }

    fn set(&self, key: String, value: String) -> BoxFuture<Result<()>> {
        let (sender, receiver) = channel::<Result<()>>();

        let db = self.db.clone();

        self.pool.spawn(move || {
            let res = db
                .insert(key.as_bytes(), value.as_bytes())
                .map(|_| ())
                .map_err(|err| SledAccess { key, source: err })
                .and_then(|_| flush(&db));
            sender.send(res).unwrap();
        });

        receiver.map(|res| res.unwrap()).boxed()
    }

    fn remove(&self, key: String) -> BoxFuture<Result<()>> {
        let (sender, receiver) = channel::<Result<()>>();

        let db = self.db.clone();

        self.pool.spawn(move || {
            let res = match db.remove(key.as_bytes()) {
                Ok(val) => match val {
                    Some(_) => Ok(()),
                    None => Err(KeyNotFound),
                },
                Err(e) => Err(SledAccess { key, source: e }),
            }
            .and_then(|_| flush(&db));

            sender.send(res).unwrap();
        });

        receiver.map(|res| res.unwrap()).boxed()
    }
}

fn flush(db: &Db) -> Result<()> {
    db.flush().map(|_| ()).map_err(|err| KvError::Sled(err))
}
