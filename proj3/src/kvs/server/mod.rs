use crate::kvs::KvsEngine;
use crate::kvs::Result;
use std::net::IpAddr;
use std::str::FromStr;


mod addr;

pub mod engine;

pub use addr::ServerAddr;
pub use addr::AddrError;

pub struct KvsServer<E: KvsEngine> {
    engine: E,
    // addr: IpAddr,
}

impl<E: KvsEngine> KvsServer<E> {
    pub fn new(engine: E) -> KvsServer<E> {
        KvsServer { engine }
    }

    pub fn listen(&self, addr: ServerAddr) -> Result<()> {
        unimplemented!()
    }
}
