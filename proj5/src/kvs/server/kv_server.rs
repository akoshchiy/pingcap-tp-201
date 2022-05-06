use crate::kvs::net::Command;
use crate::kvs::net::{read, write, CommandResult};
use crate::kvs::server::conn_handler::ConnectionHandler;
use crate::kvs::thread_pool::ThreadPool;
use crate::kvs::{KvsEngine, Result};
use slog::{error, info, o, trace, Logger};
use std::io::{Read, Write};
use std::net::SocketAddr;
use std::ptr::write_bytes;
use tokio::io::AsyncReadExt;
use tokio::net::TcpListener;

pub struct KvsServer<E: KvsEngine> {
    engine: E,
    log: Logger,
}

impl<E: KvsEngine> KvsServer<E> {
    pub fn new(engine: E, log: Logger) -> KvsServer<E> {
        KvsServer { engine, log }
    }

    pub async fn listen(&self, addr: SocketAddr) -> Result<()> {
        let listener = TcpListener::bind(addr).await?;
        loop {
            let (stream, _) = listener.accept().await?;

            let conn_engine = self.engine.clone();
            let conn_log = self.log.new(o!());

            tokio::spawn(async move {
                let handler = ConnectionHandler::new(conn_engine, conn_log);
                handler.handle(stream).await;
            });
        }
    }
}
