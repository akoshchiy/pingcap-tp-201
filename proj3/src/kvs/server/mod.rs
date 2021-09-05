use crate::kvs::KvsEngine;
use std::net::IpAddr;
use crate::kvs::Result;

pub mod engine;

pub struct KvsServer<E: KvsEngine> {
    engine: E,
    // addr: IpAddr,
}

impl<E: KvsEngine> KvsServer<E> {
    pub fn new(engine: E) -> KvsServer<E> {
        KvsServer {
            engine
        }
    }

    pub fn listen(&self, addr: IpAddr) -> Result<()> {
        unimplemented!()
    }
}


