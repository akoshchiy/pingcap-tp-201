use crate::kvs::net::{read, read_async, write, write_async, Command, CommandResult};
use crate::kvs::KvsEngine;
use crate::kvs::Result;
use crossbeam::channel::internal::SelectHandle;
use slog::{info, Logger};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

pub struct ConnectionHandler<E: KvsEngine> {
    engine: E,
    log: Logger,
}

impl<E: KvsEngine> ConnectionHandler<E> {
    pub fn new(engine: E, log: Logger) -> ConnectionHandler<E> {
        ConnectionHandler { engine, log }
    }

    pub async fn handle(&self, mut stream: TcpStream) -> Result<()> {
        let addr = stream.peer_addr()?;
        info!(self.log, "connection from: {}", addr.ip());

        let cmd: Command = read_async(&mut stream).await?;

        match cmd {
            Command::Set { key, val } => self.handle_set(key, val, &mut stream).await,
            Command::Get { key } => self.handle_get(key, &mut stream).await,
            Command::Remove { key } => self.handle_remove(key, &mut stream).await,
        }
    }

    async fn handle_set(&self, key: String, val: String, stream: &mut TcpStream) -> Result<()> {
        let result = self.engine.set(key, val).await;
        match result {
            Ok(_) => write_async(stream, &CommandResult::Ok).await,
            Err(e) => write_async(stream, &CommandResult::Err(e.to_string())).await,
        }
    }

    async fn handle_get(&self, key: String, stream: &mut TcpStream) -> Result<()> {
        let res = self.engine.get(key).await;
        match res {
            Ok(val) => match val {
                Some(v) => write_async(stream, &CommandResult::OkVal(v)).await,
                None => write_async(stream, &CommandResult::Ok).await,
            },
            Err(e) => write_async(stream, &CommandResult::Err(e.to_string())).await,
        }
    }

    async fn handle_remove(&self, key: String, stream: &mut TcpStream) -> Result<()> {
        let result = self.engine.remove(key).await;
        match result {
            Ok(_) => write_async(stream, &CommandResult::Ok).await,
            Err(e) => write_async(stream, &CommandResult::Err(e.to_string())).await,
        }
    }
}
