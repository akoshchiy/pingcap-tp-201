use crate::kvs::net::Command;
use crate::kvs::net::{read, write, CommandResult};
use crate::kvs::server::conn_handler::ConnectionHandler;
use crate::kvs::thread_pool::ThreadPool;
use crate::kvs::{KvsEngine, Result};
use slog::{error, info, o, trace, Logger};
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::ptr::write_bytes;

pub struct KvsServer<E: KvsEngine, P: ThreadPool> {
    engine: E,
    pool: P,
    log: Logger,
}

impl<E: KvsEngine, P: ThreadPool> KvsServer<E, P> {
    pub fn new(engine: E, pool: P, log: Logger) -> KvsServer<E, P> {
        KvsServer { engine, pool, log }
    }

    pub fn listen(&self, addr: SocketAddr) -> Result<()> {
        let listener = TcpListener::bind(addr)?;

        for stream in listener.incoming() {
            if stream.is_err() {
                let err = stream.err().unwrap();
                error!(&self.log, "incoming connection err: {}", err);
                continue;
            }

            let conn_engine = self.engine.clone();
            let conn_log = self.log.new(o!());

            self.pool.spawn(move || {
                let handler = ConnectionHandler::new(conn_engine, conn_log);
                handler.handle(stream.unwrap());
            });
        }

        Ok(())
    }
}
